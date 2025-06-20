package disk

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"stress/config"
	"stress/utils"
	"os"
	"sync"
	"os"
	"syscall/time"
	"time"
)

// 小型緩衝區池 - 只用於區塊級別的操作
var smallBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4*1024) // 4KB 緩衝區
	},
}

)

// 可預測的資料產生器 - 基於位置產生資料，不需要記憶體儲存
type predictableDataGenerator struct {
	seed int64
}

func newPredictableDataGenerator() *predictableDataGenerator {
	return &predictableDataGenerator{
		seed: time.Now().UnixNano(),
	}
}

// 根據檔案位置產生可預測的資料
func (pdg *predictableDataGenerator) generateBlock(offset int64, size int) []byte {
	block := make([]byte, size)
	
	// 使用 offset 作為種子的一部分，確保相同位置總是產生相同資料
	localSeed := pdg.seed + offset
	rng := rand.New(rand.NewSource(localSeed))
	
	for i := range block {
		block[i] = byte(rng.Intn(256))
	}
	
	return block
}

// 驗證區塊資料是否符合預期
func (pdg *predictableDataGenerator) verifyBlock(offset int64, data []byte) bool {
	expected := pdg.generateBlock(offset, len(data))
	
	for i, b := range data {
		if b != expected[i] {
			return false
		}
	}
	return true
}

// RunDiskStressTest 優化版本 - 直接硬碟操作
func RunDiskStressTest(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, testConfig DiskTestConfig, perfStats *config.PerformanceStats, debug bool, duration string) {
	defer wg.Done()

	// 產生主要 PID
	mainPID := int(time.Now().UnixNano() % 1000000 + 1000)
	utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] started on %d mount points (optimized direct I/O)", mainPID, len(testConfig.MountPoints)), false)

	// 驗證和設定預設值
	if len(testConfig.MountPoints) == 0 {
		utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] no mount points specified, using current directory '.'", mainPID), true)
		testConfig.MountPoints = []string{"."}
	}
	if testConfig.FileSize <= 0 {
		utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] invalid FileSize %d, using default 10MB", mainPID, testConfig.FileSize), true)
		testConfig.FileSize = 10 * 1024 * 1024
	}
	if testConfig.BlockSize <= 0 {
		utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] invalid BlockSize %d, using default 4KB", mainPID, testConfig.BlockSize), true)
		testConfig.BlockSize = 4 * 1024
	}

	// 計算統計間隔
	dur, err := time.ParseDuration(duration)
	if err != nil {
		errorMsg := fmt.Sprintf("Disk test [PID: %d] invalid duration format: %v", mainPID, err)
		errorChan <- fmt.Sprintf("Invalid duration format: %v", err)
		utils.LogMessage(errorMsg, false)
		return
	}
	durSeconds := int64(dur.Seconds())
	var statsInterval int64
	if durSeconds <= 60 {
		statsInterval = 10
	} else {
		statsInterval = durSeconds / 60
	}
	ticker := time.NewTicker(time.Duration(statsInterval) * time.Second)
	defer ticker.Stop()

	var testModes []string
	if testConfig.TestMode == "both" {
		testModes = []string{"sequential", "random"}
	} else if testConfig.TestMode == "sequential" || testConfig.TestMode == "random" {
		testModes = []string{testConfig.TestMode}
	} else {
		errorMsg := fmt.Sprintf("Disk test [PID: %d] invalid test mode: %s", mainPID, testConfig.TestMode)
		errorChan <- fmt.Sprintf("Invalid test mode: %s", testConfig.TestMode)
		utils.LogMessage(errorMsg, false)
		return
	}

	// 限制並發 goroutines
	maxConcurrent := runtime.NumCPU() * 2
	if len(testConfig.MountPoints)*len(testModes) < maxConcurrent {
		maxConcurrent = len(testConfig.MountPoints) * len(testModes)
	}
	semaphore := make(chan struct{}, maxConcurrent)

	mountWg := &sync.WaitGroup{}
	for i, mountPoint := range testConfig.MountPoints {
		mountWg.Add(1)
		go func(mp string, index int) {
			defer mountWg.Done()

			// 驗證掛載點
			if info, err := os.Stat(mp); err != nil {
				errorMsg := fmt.Sprintf("Disk test [PID: %d] mount point %s not accessible: %v", mainPID, mp, err)
				errorChan <- fmt.Sprintf("Mount point %s not accessible", mp)
				utils.LogMessage(errorMsg, false)
				return
			} else if !info.IsDir() {
				errorMsg := fmt.Sprintf("Disk test [PID: %d] mount point %s is not a directory", mainPID, mp)
				errorChan <- fmt.Sprintf("Mount point %s is not a directory", mp)
				utils.LogMessage(errorMsg, false)
				return
			}

			// 檢查寫入權限
			tempFilePath := filepath.Join(mp, fmt.Sprintf(".writetest_%d", time.Now().UnixNano()))
			tempFile, err := os.Create(tempFilePath)
			if err != nil {
				errorMsg := fmt.Sprintf("Disk test [PID: %d] mount point %s is not writable: %v", mainPID, mp, err)
				errorChan <- fmt.Sprintf("Mount point %s is not writable", mp)
				utils.LogMessage(errorMsg, false)
				return
			}
			tempFile.Close()
			os.Remove(tempFilePath)

			// 檢查硬碟空間
			var stat syscall.Statfs_t
			requiredSpace := uint64(testConfig.FileSize) * 2
			if err := syscall.Statfs(mp, &stat); err == nil {
				availableBytes := stat.Bavail * uint64(stat.Bsize)
				if availableBytes < requiredSpace {
					errorMsg := fmt.Sprintf("Disk test [PID: %d] insufficient disk space on %s", mainPID, mp)
					errorChan <- fmt.Sprintf("Insufficient disk space on %s", mp)
					utils.LogMessage(errorMsg, false)
					return
				}
			}

			utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] starting test on mount point %s", mainPID, mp), false)

			modeWg := &sync.WaitGroup{}
			for j, mode := range testModes {
				modeWg.Add(1)
				subPID := mainPID + (index*10+j+1)*100
				go func(currentMode string, pid int) {
					defer modeWg.Done()
					
					// 取得信號量
					semaphore <- struct{}{}
					defer func() { <-semaphore }()

					filePath := filepath.Join(mp, fmt.Sprintf("stress_test_%s_%d.dat", currentMode, rand.Intn(10000)))
					
					// 建立資料產生器
					dataGen := newPredictableDataGenerator()

					if debug {
						utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] starting optimized disk test for %s (mode: %s)", mainPID, mp, currentMode), true)
					}

					iteration := 0
					for {
						select {
						case <-stop:
							os.Remove(filePath)
							utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] received stop signal for %s (mode: %s)", mainPID, mp, currentMode), false)
							return
						default:
							iteration++

							// 執行直接硬碟寫入
							writeStart := time.Now()
							writeErr := performDirectDiskWrite(mainPID, filePath, dataGen, currentMode, testConfig.FileSize, testConfig.BlockSize, debug)
							writeDuration := time.Since(writeStart)

							if writeErr != nil {
								errorMsg := fmt.Sprintf("Disk test [PID: %d] write error on %s (mode: %s)", mainPID, mp, currentMode)
								errorChan <- fmt.Sprintf("Disk write error on %s (mode: %s)", mp, currentMode)
								utils.LogMessage(errorMsg, false)
								if debug {
									utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] write error details: %v", mainPID, writeErr), true)
								}
								os.Remove(filePath)
								time.Sleep(2 * time.Second)
								continue
							}

							// 執行直接硬碟讀取和驗證
							readStart := time.Now()
							readErr := performDirectDiskReadAndVerify(mainPID, filePath, dataGen, currentMode, testConfig.FileSize, testConfig.BlockSize, debug)
							readDuration := time.Since(readStart)

							if readErr != nil {
								errorMsg := fmt.Sprintf("Disk test [PID: %d] read/verify error on %s (mode: %s)", mainPID, mp, currentMode)
								errorChan <- fmt.Sprintf("Disk read/verify error on %s (mode: %s)", mp, currentMode)
								utils.LogMessage(errorMsg, false)
								if debug {
									utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] read/verify error details: %v", mainPID, readErr), true)
								}
								os.Remove(filePath)
								time.Sleep(2 * time.Second)
								continue
							}

							// 在 ticker 時更新統計
							select {
							case <-ticker.C:
								writeSpeedMBps := float64(0)
								if writeDuration.Seconds() > 0 {
									writeSpeedMBps = float64(testConfig.FileSize) / writeDuration.Seconds() / (1024 * 1024)
								}
								readSpeedMBps := float64(0)
								if readDuration.Seconds() > 0 {
									readSpeedMBps = float64(testConfig.FileSize) / readDuration.Seconds() / (1024 * 1024)
								}

								if debug {
									utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] write on %s: %.2f MB/s", mainPID, mp, writeSpeedMBps), true)
									utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] read/verify on %s: %.2f MB/s", mainPID, mp, readSpeedMBps), true)
								}

								// 更新效能統計
								perfStats.Lock()
								diskPerfKey := fmt.Sprintf("%s|%s|%d", mp, currentMode, testConfig.BlockSize)
								found := false
								for i, dp := range perfStats.Disk {
									existingKey := fmt.Sprintf("%s|%s|%d", dp.MountPoint, dp.Mode, dp.BlockSize)
									if existingKey == diskPerfKey {
										if readSpeedMBps > dp.ReadSpeed {
											perfStats.Disk[i].ReadSpeed = readSpeedMBps
										}
										if writeSpeedMBps > dp.WriteSpeed {
											perfStats.Disk[i].WriteSpeed = writeSpeedMBps
										}
										perfStats.Disk[i].WriteCount++
										perfStats.Disk[i].ReadCount++
										found = true
										break
									}
								}
								if !found {
									newPerf := config.DiskPerformance{
										MountPoint: mp,
										Mode:       currentMode,
										BlockSize:  testConfig.BlockSize,
										ReadSpeed:  readSpeedMBps,
										WriteSpeed: writeSpeedMBps,
										WriteCount: 1,
										ReadCount:  1,
									}
									perfStats.Disk = append(perfStats.Disk, newPerf)
								}
								perfStats.Unlock()
							default:
								// 繼續而不更新統計
							}

							time.Sleep(150 * time.Millisecond)

							// 定期觸發 GC
							if iteration%50 == 0 {
								runtime.GC()
							}
						}
					}
				}(mode, subPID)
			}
			modeWg.Wait()
		}(mountPoint, i)
	}
	mountWg.Wait()
	utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] completed", mainPID), false)
}

// performDirectDiskWrite 直接寫入硬碟，不使用大記憶體緩衝區
func performDirectDiskWrite(mainPID int, filePath string, dataGen *predictableDataGenerator, mode string, fileSize, blockSize int64, debug bool) error {
	// 移除現有檔案
	if _, statErr := os.Stat(filePath); statErr == nil {
		if rmErr := os.Remove(filePath); rmErr != nil {
			return fmt.Errorf("failed to remove existing file: %w", rmErr)
		}
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// 從緩衝區池取得小緩衝區
	buffer := smallBufferPool.Get().([]byte)
	defer smallBufferPool.Put(buffer)

	totalBlocks := fileSize / blockSize
	if fileSize%blockSize > 0 {
		totalBlocks++
	}

	if mode == "sequential" {
		// 順序寫入
		for blockIdx := int64(0); blockIdx < totalBlocks; blockIdx++ {
			offset := blockIdx * blockSize
			currentBlockSize := blockSize
			if offset+currentBlockSize > fileSize {
				currentBlockSize = fileSize - offset
			}

			// 分批寫入區塊，避免大記憶體分配
			for bytesWritten := int64(0); bytesWritten < currentBlockSize; {
				chunkSize := int64(len(buffer))
				if bytesWritten+chunkSize > currentBlockSize {
					chunkSize = currentBlockSize - bytesWritten
				}

				// 產生這個區塊的資料
				chunkData := dataGen.generateBlock(offset+bytesWritten, int(chunkSize))
				
				n, writeErr := file.Write(chunkData)
				if writeErr != nil {
					return fmt.Errorf("write error at offset %d: %w", offset+bytesWritten, writeErr)
				}
				if int64(n) != chunkSize {
					return fmt.Errorf("short write at offset %d", offset+bytesWritten)
				}
				
				bytesWritten += int64(n)
			}
		}
	} else {
		// 隨機寫入
		// 先建立檔案大小
		if err := file.Truncate(fileSize); err != nil {
			return fmt.Errorf("failed to truncate file: %w", err)
		}

		// 建立隨機區塊順序
		blockOrder := make([]int64, totalBlocks)
		for i := int64(0); i < totalBlocks; i++ {
			blockOrder[i] = i
		}
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		rng.Shuffle(int(totalBlocks), func(i, j int) {
			blockOrder[i], blockOrder[j] = blockOrder[j], blockOrder[i]
		})

		// 隨機寫入區塊
		for _, blockIdx := range blockOrder {
			offset := blockIdx * blockSize
			currentBlockSize := blockSize
			if offset+currentBlockSize > fileSize {
				currentBlockSize = fileSize - offset
			}

			// 分批寫入區塊
			for bytesWritten := int64(0); bytesWritten < currentBlockSize; {
				chunkSize := int64(len(buffer))
				if bytesWritten+chunkSize > currentBlockSize {
					chunkSize = currentBlockSize - bytesWritten
				}

				// 產生這個位置的資料
				chunkData := dataGen.generateBlock(offset+bytesWritten, int(chunkSize))
				
				n, writeErr := file.WriteAt(chunkData, offset+bytesWritten)
				if writeErr != nil {
					return fmt.Errorf("write error at offset %d: %w", offset+bytesWritten, writeErr)
				}
				if int64(n) != chunkSize {
					return fmt.Errorf("short write at offset %d", offset+bytesWritten)
				}
				
				bytesWritten += int64(n)
			}
		}
	}

	// 同步到硬碟
	if syncErr := file.Sync(); syncErr != nil {
		return fmt.Errorf("failed to sync file: %w", syncErr)
	}

	return nil
}

// performDirectDiskReadAndVerify 直接從硬碟讀取並驗證，不使用大記憶體緩衝區
func performDirectDiskReadAndVerify(mainPID int, filePath string, dataGen *predictableDataGenerator, mode string, fileSize, blockSize int64, debug bool) error {
	fileInfo, statErr := os.Stat(filePath)
	if statErr != nil {
		return fmt.Errorf("file not found: %w", statErr)
	}

	if fileInfo.Size() != fileSize {
		return fmt.Errorf("file size mismatch: expected %d, got %d", fileSize, fileInfo.Size())
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// 從緩衝區池取得小緩衝區
	buffer := smallBufferPool.Get().([]byte)
	defer smallBufferPool.Put(buffer)

	totalBlocks := fileSize / blockSize
	if fileSize%blockSize > 0 {
		totalBlocks++
	}

	if mode == "sequential" {
		// 順序讀取和驗證
		for blockIdx := int64(0); blockIdx < totalBlocks; blockIdx++ {
			offset := blockIdx * blockSize
			currentBlockSize := blockSize
			if offset+currentBlockSize > fileSize {
				currentBlockSize = fileSize - offset
			}

			// 分批讀取和驗證區塊
			for bytesRead := int64(0); bytesRead < currentBlockSize; {
				chunkSize := int64(len(buffer))
				if bytesRead+chunkSize > currentBlockSize {
					chunkSize = currentBlockSize - bytesRead
				}

				n, readErr := file.Read(buffer[:chunkSize])
				if readErr != nil && readErr != io.EOF {
					return fmt.Errorf("read error at offset %d: %w", offset+bytesRead, readErr)
				}
				if int64(n) != chunkSize && readErr != io.EOF {
					return fmt.Errorf("short read at offset %d", offset+bytesRead)
				}

				// 驗證資料
				if !dataGen.verifyBlock(offset+bytesRead, buffer[:n]) {
					return fmt.Errorf("data verification failed at offset %d", offset+bytesRead)
				}
				
				bytesRead += int64(n)
			}
		}
	} else {
		// 隨機讀取和驗證
		// 建立隨機區塊順序
		blockOrder := make([]int64, totalBlocks)
		for i := int64(0); i < totalBlocks; i++ {
			blockOrder[i] = i
		}
		rng := rand.New(rand.NewSource(time.Now().UnixNano() + 1))
		rng.Shuffle(int(totalBlocks), func(i, j int) {
			blockOrder[i], blockOrder[j] = blockOrder[j], blockOrder[i]
		})

		// 隨機讀取和驗證區塊
		for _, blockIdx := range blockOrder {
			offset := blockIdx * blockSize
			currentBlockSize := blockSize
			if offset+currentBlockSize > fileSize {
				currentBlockSize = fileSize - offset
			}

			// 分批讀取和驗證區塊
			for bytesRead := int64(0); bytesRead < currentBlockSize; {
				chunkSize := int64(len(buffer))
				if bytesRead+chunkSize > currentBlockSize {
					chunkSize = currentBlockSize - bytesRead
				}

				n, readErr := file.ReadAt(buffer[:chunkSize], offset+bytesRead)
				if readErr != nil && readErr != io.EOF {
					return fmt.Errorf("read error at offset %d: %w", offset+bytesRead, readErr)
				}
				if int64(n) != chunkSize && readErr != io.EOF {
					return fmt.Errorf("short read at offset %d", offset+bytesRead)
				}

				// 驗證資料
				if !dataGen.verifyBlock(offset+bytesRead, buffer[:n]) {
					return fmt.Errorf("data verification failed at offset %d", offset+bytesRead)
				}
				
				bytesRead += int64(n)
			}
		}
	}

	return nil
}
