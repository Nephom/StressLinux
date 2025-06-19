package disk

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"stress/config"
	"stress/utils"
	"sync"
	"syscall"
	"time"
)

// Buffer pool for reusing memory buffers
var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 64*1024) // 64KB buffer
	},
}

// Shared data pool to avoid duplicating random data
type sharedDataManager struct {
	data []byte
	mu   sync.RWMutex
}

func (sdm *sharedDataManager) getData(size int64) []byte {
	sdm.mu.RLock()
	if sdm.data != nil && int64(len(sdm.data)) == size {
		defer sdm.mu.RUnlock()
		return sdm.data
	}
	sdm.mu.RUnlock()

	sdm.mu.Lock()
	defer sdm.mu.Unlock()
	
	// Double check after acquiring write lock
	if sdm.data != nil && int64(len(sdm.data)) == size {
		return sdm.data
	}

	// Generate new shared data
	sdm.data = make([]byte, size)
	if _, err := rand.Read(sdm.data); err != nil {
		// If random generation fails, use a pattern
		for i := range sdm.data {
			sdm.data[i] = byte(i % 256)
		}
	}
	return sdm.data
}

var globalSharedData = &sharedDataManager{}

// RunDiskStressTest runs the disk stress test with optimized memory usage and dynamic stats interval
func RunDiskStressTest(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, testConfig DiskTestConfig, perfStats *config.PerformanceStats, debug bool, duration string) {
	defer wg.Done()

	// Generate main PID for this test
	mainPID := rand.Intn(1000000) + 1000
	utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] started on %d mount points", mainPID, len(testConfig.MountPoints)), false)

	// Validate and set defaults
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
	if testConfig.FileSize < testConfig.BlockSize && testConfig.TestMode != "sequential" {
		utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] warning: FileSize (%s) is smaller than BlockSize (%s) for random/both mode", mainPID, utils.FormatSize(testConfig.FileSize), utils.FormatSize(testConfig.BlockSize)), true)
	}

	// Calculate stats interval based on duration
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
		errorMsg := fmt.Sprintf("Disk test [PID: %d] invalid test mode: %s. Use 'sequential', 'random', or 'both'", mainPID, testConfig.TestMode)
		errorChan <- fmt.Sprintf("Invalid test mode: %s", testConfig.TestMode)
		utils.LogMessage(errorMsg, false)
		return
	}

	// Generate shared random data once
	if debug {
		utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] generating shared random data of size %s...", mainPID, utils.FormatSize(testConfig.FileSize)), true)
	}
	sharedData := globalSharedData.getData(testConfig.FileSize)
	if debug {
		utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] shared random data ready", mainPID), true)
	}

	// Limit concurrent goroutines to prevent memory exhaustion
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

			// Validate mount point
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

			// Check write permission
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

			// Check disk space
			var stat syscall.Statfs_t
			requiredSpace := uint64(testConfig.FileSize) * 2
			if err := syscall.Statfs(mp, &stat); err == nil {
				availableBytes := stat.Bavail * uint64(stat.Bsize)
				if availableBytes < requiredSpace {
					errorMsg := fmt.Sprintf("Disk test [PID: %d] insufficient disk space on %s: required approx %s, available %s", mainPID, mp, utils.FormatSize(int64(requiredSpace)), utils.FormatSize(int64(availableBytes)))
					errorChan <- fmt.Sprintf("Insufficient disk space on %s", mp)
					utils.LogMessage(errorMsg, false)
					return
				}
				if debug {
					utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] disk space check on %s: OK (Available: %s, Required: approx %s)", mainPID, mp, utils.FormatSize(int64(availableBytes)), utils.FormatSize(int64(requiredSpace))), true)
				}
			} else {
				utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] warning: Could not check disk space on %s: %v. Proceeding anyway", mainPID, mp, err), true)
			}

			utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] starting test on mount point %s", mainPID, mp), false)

			modeWg := &sync.WaitGroup{}
			for j, mode := range testModes {
				modeWg.Add(1)
				subPID := mainPID + (index*10+j+1)*100
				go func(currentMode string, pid int) {
					defer modeWg.Done()
					
					// Acquire semaphore
					semaphore <- struct{}{}
					defer func() { <-semaphore }()

					filePath := filepath.Join(mp, fmt.Sprintf("stress_test_%s_%d.dat", currentMode, rand.Intn(10000)))

					if debug {
						utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] starting disk test for %s (mode: %s, file: %s, size: %s, block: %s)", mainPID, mp, currentMode, filepath.Base(filePath), utils.FormatSize(testConfig.FileSize), utils.FormatSize(testConfig.BlockSize)), true)
					}

					iteration := 0
					for {
						select {
						case <-stop:
							os.Remove(filePath)
							utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] received stop signal for %s (mode: %s)", mainPID, mp, currentMode), false)
							stopSubprocesses(mainPID, mp, currentMode, pid, debug)
							return
						default:
							iteration++
							if debug {
								utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] mount %s, mode %s, iteration %d: Starting cycle", mainPID, mp, currentMode, iteration), true)
							}

							// Perform disk write
							if debug {
								utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] mount %s, mode %s, iteration %d: Performing write...", mainPID, mp, currentMode, iteration), true)
							}
							writeStart := time.Now()
							writeErr := performDiskWriteOptimized(mainPID, filePath, sharedData, currentMode, testConfig.BlockSize, debug)
							writeDuration := time.Since(writeStart)

							if writeErr != nil {
								errorMsg := fmt.Sprintf("Disk test [PID: %d] write error on %s (mode: %s, file: %s, iter: %d)", mainPID, mp, currentMode, filepath.Base(filePath), iteration)
								errorChan <- fmt.Sprintf("Disk write error on %s (mode: %s)", mp, currentMode)
								utils.LogMessage(errorMsg, false)
								if debug {
									utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] write error details: %v", mainPID, writeErr), true)
								}
								os.Remove(filePath)
								time.Sleep(2 * time.Second)
								continue
							}

							// Perform disk read and verify
							if debug {
								utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] mount %s, mode %s, iteration %d: Performing read and verify...", mainPID, mp, currentMode, iteration), true)
							}
							readStart := time.Now()
							readErr := performDiskReadAndVerifyOptimized(mainPID, filePath, sharedData, currentMode, testConfig.BlockSize, debug)
							readDuration := time.Since(readStart)

							if readErr != nil {
								errorMsg := fmt.Sprintf("Disk test [PID: %d] read/verify error on %s (mode: %s, file: %s, iter: %d)", mainPID, mp, currentMode, filepath.Base(filePath), iteration)
								errorChan <- fmt.Sprintf("Disk read/verify error on %s (mode: %s)", mp, currentMode)
								utils.LogMessage(errorMsg, false)
								if debug {
									utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] read/verify error details: %v", mainPID, readErr), true)
								}
								os.Remove(filePath)
								time.Sleep(2 * time.Second)
								continue
							}

							// Update stats only on ticker
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
									utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] write on %s (mode: %s, iter: %d): %.2f MB/s (%s in %v)", mainPID, mp, currentMode, iteration, writeSpeedMBps, utils.FormatSize(testConfig.FileSize), writeDuration), true)
									utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] read/verify on %s (mode: %s, iter: %d): %.2f MB/s (%s in %v)", mainPID, mp, currentMode, iteration, readSpeedMBps, utils.FormatSize(testConfig.FileSize), readDuration), true)
								}

								// Update performance stats
								perfStats.Lock()
								diskPerfKey := fmt.Sprintf("%s|%s|%d", mp, currentMode, testConfig.BlockSize)
								found := false
								for i, dp := range perfStats.Disk {
									existingKey := fmt.Sprintf("%s|%s|%d", dp.MountPoint, dp.Mode, dp.BlockSize)
									if existingKey == diskPerfKey {
										if readSpeedMBps > dp.ReadSpeed {
											perfStats.Disk[i].ReadSpeed = readSpeedMBps
											if debug {
												utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] updated best read speed for %s: %.2f MB/s", mainPID, diskPerfKey, readSpeedMBps), true)
											}
										}
										if writeSpeedMBps > dp.WriteSpeed {
											perfStats.Disk[i].WriteSpeed = writeSpeedMBps
											if debug {
												utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] updated best write speed for %s: %.2f MB/s", mainPID, diskPerfKey, writeSpeedMBps), true)
											}
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
									if debug {
										utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] added initial perf record for %s: Read=%.2f MB/s, Write=%.2f MB/s", mainPID, diskPerfKey, readSpeedMBps, writeSpeedMBps), true)
									}
								}
								perfStats.Unlock()
							default:
								// Continue without updating stats
							}

							if debug {
								utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] mount %s, mode %s, iteration %d: Cycle completed successfully", mainPID, mp, currentMode, iteration), true)
							}
							time.Sleep(150 * time.Millisecond)

							// Trigger GC periodically to free memory
							if iteration%100 == 0 {
								runtime.GC()
							}
						}
					}
				}(mode, subPID)
			}
			modeWg.Wait()
			if debug {
				utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] all test modes finished for mount point %s", mainPID, mp), true)
			}
		}(mountPoint, i)
	}
	mountWg.Wait()
	utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] completed", mainPID), false)
}

// stopSubprocesses simulates stopping subprocesses for a test mode
func stopSubprocesses(mainPID int, mountPoint, mode string, subPID int, debug bool) {
	utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] subprocess %s [PID: %d] stopping on %s...", mainPID, mode, subPID, mountPoint), false)
	time.Sleep(100 * time.Millisecond)
	if debug {
		utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] subprocess %s [PID: %d] stopped on %s", mainPID, mode, subPID, mountPoint), true)
	}
}

// performDiskWriteOptimized writes data to a file with optimized memory usage
func performDiskWriteOptimized(mainPID int, filePath string, data []byte, mode string, blockSize int64, debug bool) error {
	if _, statErr := os.Stat(filePath); statErr == nil {
		if rmErr := os.Remove(filePath); rmErr != nil {
			return fmt.Errorf("failed to remove existing file (%s): %w", filePath, rmErr)
		}
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("failed to stat file before write (%s): %w", filePath, statErr)
	}

	if len(data) == 0 {
		return fmt.Errorf("attempt to write empty data to file %s", filePath)
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to open/create file for writing (%s): %w", filePath, err)
	}
	defer file.Close()

	totalSize := int64(len(data))
	var totalBytesWritten int64

	if mode == "sequential" {
		// Write in chunks to reduce memory pressure
		chunkSize := int64(64 * 1024) // 64KB chunks
		if chunkSize > totalSize {
			chunkSize = totalSize
		}

		for offset := int64(0); offset < totalSize; offset += chunkSize {
			end := offset + chunkSize
			if end > totalSize {
				end = totalSize
			}

			n, writeErr := file.Write(data[offset:end])
			totalBytesWritten += int64(n)
			if writeErr != nil {
				return fmt.Errorf("sequential write error on %s at offset %d after writing %d bytes: %w", filePath, offset, n, writeErr)
			}
			if int64(n) != (end - offset) {
				return fmt.Errorf("sequential write short write on %s at offset %d: wrote %d bytes, expected %d", filePath, offset, n, end-offset)
			}
		}
	} else {
		if err := file.Truncate(totalSize); err != nil {
			return fmt.Errorf("failed to truncate file to required size %d for (%s): %w", totalSize, filePath, err)
		}

		blocks := totalSize / blockSize
		if totalSize%blockSize > 0 {
			blocks++
		}

		blockOrder := make([]int64, blocks)
		for i := int64(0); i < blocks; i++ {
			blockOrder[i] = i
		}
		source := rand.NewSource(time.Now().UnixNano())
		rng := rand.New(source)
		rng.Shuffle(int(blocks), func(i, j int) {
			blockOrder[i], blockOrder[j] = blockOrder[j], blockOrder[i]
		})

		for _, blockIdx := range blockOrder {
			start := blockIdx * blockSize
			end := start + blockSize
			if end > totalSize {
				end = totalSize
			}
			chunkSize := end - start
			if chunkSize <= 0 {
				continue
			}

			if start >= int64(len(data)) || end > int64(len(data)) {
				return fmt.Errorf("internal logic error: calculated range [%d:%d] exceeds data length %d", start, end, len(data))
			}

			n, writeErr := file.WriteAt(data[start:end], start)
			totalBytesWritten += int64(n)
			if writeErr != nil {
				return fmt.Errorf("random write error on %s at offset %d after writing %d bytes for this chunk: %w", filePath, start, n, writeErr)
			}
			if int64(n) != chunkSize {
				return fmt.Errorf("random write short write on %s at offset %d: wrote %d bytes, expected %d", filePath, start, n, chunkSize)
			}
		}

		if totalBytesWritten != totalSize {
			return fmt.Errorf("random write total bytes written mismatch: wrote %d bytes, expected %d for %s", totalBytesWritten, totalSize, filePath)
		}
	}

	if syncErr := file.Sync(); syncErr != nil {
		return fmt.Errorf("failed to sync file (%s) after writing %d bytes: %w", filePath, totalBytesWritten, syncErr)
	}

	fileInfo, statErr := os.Stat(filePath)
	if statErr != nil {
		return fmt.Errorf("final file verification failed after close (%s): cannot stat file: %w", filePath, statErr)
	}

	if fileInfo.Size() != totalSize {
		return fmt.Errorf("final file size verification failed: expected %d bytes, got %d bytes for file %s", totalSize, fileInfo.Size(), filePath)
	}

	return nil
}

// performDiskReadAndVerifyOptimized reads and verifies file data with optimized memory usage
func performDiskReadAndVerifyOptimized(mainPID int, filePath string, originalData []byte, mode string, blockSize int64, debug bool) error {
	expectedSize := int64(len(originalData))

	fileInfo, statErr := os.Stat(filePath)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return fmt.Errorf("file not found for reading (%s): %w", filePath, statErr)
		}
		return fmt.Errorf("file not accessible for reading (%s): cannot stat file: %w", filePath, statErr)
	}

	totalSize := fileInfo.Size()
	if totalSize != expectedSize {
		return fmt.Errorf("file size mismatch before reading %s: expected %d bytes, found %d bytes", filePath, expectedSize, totalSize)
	}
	if expectedSize == 0 {
		return nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file for reading (%s): %w", filePath, err)
	}
	defer file.Close()

	// Use chunked reading to reduce memory usage
	chunkSize := int64(64 * 1024) // 64KB chunks
	if mode == "sequential" {
		buffer := bufferPool.Get().([]byte)
		defer bufferPool.Put(buffer)

		for offset := int64(0); offset < totalSize; {
			readSize := chunkSize
			if offset+readSize > totalSize {
				readSize = totalSize - offset
			}
			if readSize > int64(len(buffer)) {
				// Need larger buffer for this chunk
				largeBuffer := make([]byte, readSize)
				n, readErr := file.Read(largeBuffer)
				if readErr != nil && readErr != io.EOF {
					return fmt.Errorf("sequential read error on %s at offset %d: %w", filePath, offset, readErr)
				}
				if int64(n) != readSize && readErr != io.EOF {
					return fmt.Errorf("sequential read short read on %s at offset %d: read %d bytes, expected %d", filePath, offset, n, readSize)
				}
				
				// Verify chunk
				if !bytes.Equal(originalData[offset:offset+int64(n)], largeBuffer[:n]) {
					return fmt.Errorf("data verification failed for file %s at offset %d", filePath, offset)
				}
				offset += int64(n)
			} else {
				n, readErr := file.Read(buffer[:readSize])
				if readErr != nil && readErr != io.EOF {
					return fmt.Errorf("sequential read error on %s at offset %d: %w", filePath, offset, readErr)
				}
				if int64(n) != readSize && readErr != io.EOF {
					return fmt.Errorf("sequential read short read on %s at offset %d: read %d bytes, expected %d", filePath, offset, n, readSize)
				}
				
				// Verify chunk
				if !bytes.Equal(originalData[offset:offset+int64(n)], buffer[:n]) {
					return fmt.Errorf("data verification failed for file %s at offset %d", filePath, offset)
				}
				offset += int64(n)
			}
		}
	} else {
		// Random mode - read in blocks
		blocks := totalSize / blockSize
		if totalSize%blockSize > 0 {
			blocks++
		}

		blockOrder := make([]int64, blocks)
		for i := int64(0); i < blocks; i++ {
			blockOrder[i] = i
		}
		source := rand.NewSource(time.Now().UnixNano() + 1)
		rng := rand.New(source)
		rng.Shuffle(int(blocks), func(i, j int) {
			blockOrder[i], blockOrder[j] = blockOrder[j], blockOrder[i]
		})

		buffer := bufferPool.Get().([]byte)
		defer bufferPool.Put(buffer)

		for _, blockIdx := range blockOrder {
			start := blockIdx * blockSize
			end := start + blockSize
			if end > totalSize {
				end = totalSize
			}
			chunkSize := end - start
			if chunkSize <= 0 {
				continue
			}

			var readBuffer []byte
			if chunkSize <= int64(len(buffer)) {
				readBuffer = buffer[:chunkSize]
			} else {
				readBuffer = make([]byte, chunkSize)
			}

			n, readErr := file.ReadAt(readBuffer, start)
			if int64(n) != chunkSize {
				return fmt.Errorf("random read short read on %s at offset %d: read %d bytes, expected %d (error: %v)", filePath, start, n, chunkSize, readErr)
			}
			if readErr != nil && readErr != io.EOF {
				return fmt.Errorf("random read error on %s at offset %d after reading %d bytes for this chunk: %w", filePath, start, n, readErr)
			}

			// Verify chunk
			if !bytes.Equal(originalData[start:end], readBuffer) {
				return fmt.Errorf("data verification failed for file %s at offset %d", filePath, start)
			}
		}
	}

	return nil
}
