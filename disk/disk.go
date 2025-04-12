// disk/disk.go
package disk

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"stress/config"
	"stress/utils"
	"sync"
	"syscall"
	"time"
)

// RunDiskStressTest runs the disk stress test
func RunDiskStressTest(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, testConfig DiskTestConfig, perfStats *config.PerformanceStats, debug bool) {
	defer wg.Done()

	if len(testConfig.MountPoints) == 0 {
		utils.LogMessage("No mount points specified, using current directory '.'", true)
		testConfig.MountPoints = []string{"."}
	}
	if testConfig.FileSize <= 0 {
		utils.LogMessage(fmt.Sprintf("Invalid FileSize %d, using default 10MB", testConfig.FileSize), true)
		testConfig.FileSize = 10 * 1024 * 1024
	}
	if testConfig.BlockSize <= 0 {
		utils.LogMessage(fmt.Sprintf("Invalid BlockSize %d, using default 4KB", testConfig.BlockSize), true)
		testConfig.BlockSize = 4 * 1024
	}
	if testConfig.FileSize < testConfig.BlockSize && testConfig.TestMode != "sequential" {
		utils.LogMessage(fmt.Sprintf("Warning: FileSize (%s) is smaller than BlockSize (%s) for random/both mode.",
			utils.FormatSize(testConfig.FileSize), utils.FormatSize(testConfig.BlockSize)), true)
	}

	var testModes []string
	if testConfig.TestMode == "both" {
		testModes = []string{"sequential", "random"}
	} else if testConfig.TestMode == "sequential" || testConfig.TestMode == "random" {
		testModes = []string{testConfig.TestMode}
	} else {
		errorMsg := fmt.Sprintf("Invalid test mode: %s. Use 'sequential', 'random', or 'both'.", testConfig.TestMode)
		errorChan <- errorMsg
		utils.LogMessage(errorMsg, true)
		return
	}

	mountWg := &sync.WaitGroup{}
	for _, mountPoint := range testConfig.MountPoints {
		mountWg.Add(1)
		go func(mp string) {
			defer mountWg.Done()

			if info, err := os.Stat(mp); err != nil {
				errorMsg := fmt.Sprintf("Mount point %s not accessible: %v", mp, err)
				errorChan <- errorMsg
				utils.LogMessage(errorMsg, true)
				return
			} else if !info.IsDir() {
				errorMsg := fmt.Sprintf("Mount point %s is not a directory", mp)
				errorChan <- errorMsg
				utils.LogMessage(errorMsg, true)
				return
			}

			tempFilePath := filepath.Join(mp, fmt.Sprintf(".writetest_%d", time.Now().UnixNano()))
			tempFile, err := os.Create(tempFilePath)
			if err != nil {
				errorMsg := fmt.Sprintf("Mount point %s is not writable: %v", mp, err)
				errorChan <- errorMsg
				utils.LogMessage(errorMsg, true)
				return
			}
			tempFile.Close()
			os.Remove(tempFilePath)

			var stat syscall.Statfs_t
			requiredSpace := uint64(testConfig.FileSize) * 2
			if err := syscall.Statfs(mp, &stat); err == nil {
				availableBytes := stat.Bavail * uint64(stat.Bsize)
				if availableBytes < requiredSpace {
					errorMsg := fmt.Sprintf("Insufficient disk space on %s: required approx %s, available %s",
						mp, utils.FormatSize(int64(requiredSpace)), utils.FormatSize(int64(availableBytes)))
					errorChan <- errorMsg
					utils.LogMessage(errorMsg, true)
					return
				}
				utils.LogMessage(fmt.Sprintf("Disk space check on %s: OK (Available: %s, Required: approx %s)", mp,
					utils.FormatSize(int64(availableBytes)), utils.FormatSize(int64(requiredSpace))), debug)
			} else {
				utils.LogMessage(fmt.Sprintf("Warning: Could not check disk space on %s: %v. Proceeding anyway.", mp, err), true)
			}

			utils.LogMessage(fmt.Sprintf("Generating %s of random data for tests on %s...", utils.FormatSize(testConfig.FileSize), mp), debug)
			data := make([]byte, testConfig.FileSize)
			if _, err := rand.Read(data); err != nil {
				errorMsg := fmt.Sprintf("Failed to generate random data for %s: %v", mp, err)
				errorChan <- errorMsg
				utils.LogMessage(errorMsg, true)
				return
			}
			utils.LogMessage(fmt.Sprintf("Random data generated for %s.", mp), debug)

			modeWg := &sync.WaitGroup{}
			for _, mode := range testModes {
				modeWg.Add(1)
				go func(currentMode string) {
					defer modeWg.Done()
					filePath := filepath.Join(mp, fmt.Sprintf("stress_test_%s_%d.dat", currentMode, rand.Intn(10000)))

					utils.LogMessage(fmt.Sprintf("Starting disk test goroutine for mount point: %s (mode: %s, file: %s, size: %s, block: %s)",
						mp, currentMode, filepath.Base(filePath), utils.FormatSize(testConfig.FileSize), utils.FormatSize(testConfig.BlockSize)), debug)

					iteration := 0
					for {
						iteration++
						utils.LogMessage(fmt.Sprintf("Mount %s, Mode %s, Iteration %d: Starting cycle.", mp, currentMode, iteration), debug)

						select {
						case <-stop:
							os.Remove(filePath)
							utils.LogMessage(fmt.Sprintf("Disk test stopped on %s (mode: %s, file: %s)", mp, currentMode, filepath.Base(filePath)), debug)
							return
						default:
							utils.LogMessage(fmt.Sprintf("Mount %s, Mode %s, Iteration %d: Performing write...", mp, currentMode, iteration), debug)
							writeStart := time.Now()
							writeErr := performDiskWrite(filePath, data, currentMode, testConfig.BlockSize)
							writeDuration := time.Since(writeStart)

							if writeErr != nil {
								errorMsg := fmt.Sprintf("Disk write error on %s (mode: %s, file: %s, iter: %d, duration: %v): %v", mp, currentMode, filepath.Base(filePath), iteration, writeDuration, writeErr)
								errorChan <- errorMsg
								utils.LogMessage(errorMsg, true)
								os.Remove(filePath)
								time.Sleep(2 * time.Second)
								continue
							}

							writeSpeedMBps := float64(0)
							if writeDuration.Seconds() > 0 {
								writeSpeedMBps = float64(testConfig.FileSize) / writeDuration.Seconds() / (1024 * 1024)
							}
							utils.LogMessage(fmt.Sprintf("Disk write on %s (mode: %s, iter: %d): %.2f MB/s (%s in %v)",
								mp, currentMode, iteration, writeSpeedMBps, utils.FormatSize(testConfig.FileSize), writeDuration), debug)

							utils.LogMessage(fmt.Sprintf("Mount %s, Mode %s, Iteration %d: Performing read and verify...", mp, currentMode, iteration), debug)
							readStart := time.Now()
							readErr := performDiskReadAndVerify(filePath, data, currentMode, testConfig.BlockSize)
							readDuration := time.Since(readStart)

							if readErr != nil {
								errorMsg := fmt.Sprintf("Disk read/verify error on %s (mode: %s, file: %s, iter: %d, duration: %v): %v", mp, currentMode, filepath.Base(filePath), iteration, readDuration, readErr)
								errorChan <- errorMsg
								utils.LogMessage(errorMsg, true)
								os.Remove(filePath)
								time.Sleep(2 * time.Second)
								continue
							}

							readSpeedMBps := float64(0)
							if readDuration.Seconds() > 0 {
								readSpeedMBps = float64(testConfig.FileSize) / readDuration.Seconds() / (1024 * 1024)
							}
							utils.LogMessage(fmt.Sprintf("Disk read/verify on %s (mode: %s, iter: %d): %.2f MB/s (%s in %v)",
								mp, currentMode, iteration, readSpeedMBps, utils.FormatSize(testConfig.FileSize), readDuration), debug)

							// 在 RunDiskStressTest 中
							perfStats.Lock()
							diskPerfKey := fmt.Sprintf("%s|%s|%d", mp, currentMode, testConfig.BlockSize)
							found := false
							for i, dp := range perfStats.Disk {
								existingKey := fmt.Sprintf("%s|%s|%d", dp.MountPoint, dp.Mode, dp.BlockSize)
								if existingKey == diskPerfKey {
									if readSpeedMBps > dp.ReadSpeed {
										perfStats.Disk[i].ReadSpeed = readSpeedMBps
										utils.LogMessage(fmt.Sprintf("Updated best read speed for %s: %.2f MB/s", diskPerfKey, readSpeedMBps), debug)
									}
									if writeSpeedMBps > dp.WriteSpeed {
										perfStats.Disk[i].WriteSpeed = writeSpeedMBps
										utils.LogMessage(fmt.Sprintf("Updated best write speed for %s: %.2f MB/s", diskPerfKey, writeSpeedMBps), debug)
									}
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
								}
								perfStats.Disk = append(perfStats.Disk, newPerf)
								utils.LogMessage(fmt.Sprintf("Added initial perf record for %s: Read=%.2f MB/s, Write=%.2f MB/s", diskPerfKey, readSpeedMBps, writeSpeedMBps), debug)
							}
							perfStats.Unlock()


							utils.LogMessage(fmt.Sprintf("Mount %s, Mode %s, Iteration %d: Cycle completed successfully. Sleeping.", mp, currentMode, iteration), debug)
							time.Sleep(150 * time.Millisecond)
						}
					}
				}(mode)
			}
			modeWg.Wait()
			utils.LogMessage(fmt.Sprintf("All test modes finished or stopped for mount point %s.", mp), debug)
		}(mountPoint)
	}

	mountWg.Wait()
	utils.LogMessage("All mount point test goroutines have finished.", debug)
}

// performDiskWrite writes data to a file
func performDiskWrite(filePath string, data []byte, mode string, blockSize int64) (err error) {
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

	totalSize := int64(len(data))
	var totalBytesWritten int64 = 0

	if mode == "sequential" {
		n, writeErr := file.Write(data)
		totalBytesWritten = int64(n)
		if writeErr != nil {
			file.Close()
			return fmt.Errorf("sequential write error on %s after writing %d bytes: %w", filePath, totalBytesWritten, writeErr)
		}
		if totalBytesWritten != totalSize {
			file.Close()
			return fmt.Errorf("sequential write short write on %s: wrote %d bytes, expected %d", filePath, totalBytesWritten, totalSize)
		}
	} else {
		if err := file.Truncate(totalSize); err != nil {
			file.Close()
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
				file.Close()
				return fmt.Errorf("internal logic error: calculated range [%d:%d] exceeds data length %d", start, end, len(data))
			}

			n, writeErr := file.WriteAt(data[start:end], start)
			totalBytesWritten += int64(n)
			if writeErr != nil {
				file.Close()
				return fmt.Errorf("random write error on %s at offset %d after writing %d bytes for this chunk: %w", filePath, start, n, writeErr)
			}
			if int64(n) != chunkSize {
				file.Close()
				return fmt.Errorf("random write short write on %s at offset %d: wrote %d bytes, expected %d", filePath, start, n, chunkSize)
			}
		}

		if totalBytesWritten != totalSize {
			file.Close()
			return fmt.Errorf("random write total bytes written mismatch: wrote %d bytes, expected %d for %s", totalBytesWritten, totalSize, filePath)
		}
	}

	if syncErr := file.Sync(); syncErr != nil {
		file.Close()
		return fmt.Errorf("failed to sync file (%s) after writing %d bytes: %w", filePath, totalBytesWritten, syncErr)
	}

	if closeErr := file.Close(); closeErr != nil {
		return fmt.Errorf("failed to close file (%s) after writing and sync: %w", filePath, closeErr)
	}

	fileInfo, statErr := os.Stat(filePath)
	if statErr != nil {
		return fmt.Errorf("final file verification failed after close (%s): cannot stat file: %w", filePath, statErr)
	}

	if fileInfo.Size() != totalSize {
		return fmt.Errorf("final file size verification failed: expected %d bytes, got %d bytes for file %s",
			totalSize, fileInfo.Size(), filePath)
	}

	return nil
}

// performDiskReadAndVerify reads and verifies file data
func performDiskReadAndVerify(filePath string, originalData []byte, mode string, blockSize int64) error {
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

	readData := make([]byte, totalSize)

	if mode == "sequential" {
		n, readErr := io.ReadFull(file, readData)
		if readErr != nil {
			if readErr == io.ErrUnexpectedEOF {
				return fmt.Errorf("sequential read error on %s: file was shorter than expected (read %d bytes, expected %d). Inconsistency detected: %w", filePath, n, totalSize, readErr)
			}
			return fmt.Errorf("sequential read error on %s after reading %d bytes: %w", filePath, n, readErr)
		}
		if int64(n) != totalSize {
			return fmt.Errorf("internal inconsistency: ReadFull returned nil error but read %d bytes, expected %d for %s", n, totalSize, filePath)
		}
	} else {
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

		var totalBytesRead int64 = 0
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

			if start >= int64(len(readData)) || end > int64(len(readData)) {
				return fmt.Errorf("internal logic error during random read: calculated range [%d:%d] exceeds buffer length %d", start, end, len(readData))
			}

			n, readErr := file.ReadAt(readData[start:end], start)
			totalBytesRead += int64(n)

			if int64(n) != chunkSize {
				return fmt.Errorf("random read short read on %s at offset %d: read %d bytes, expected %d (error: %v)", filePath, start, n, chunkSize, readErr)
			}

			if readErr != nil && readErr != io.EOF {
				return fmt.Errorf("random read error on %s at offset %d after reading %d bytes for this chunk: %w", filePath, start, n, readErr)
			}
		}
	}

	if !bytes.Equal(originalData, readData) {
		mismatchPos := int64(-1)
		var originalByte, readByte byte
		limit := len(originalData)
		if len(readData) < limit {
			limit = len(readData)
		}

		for i := 0; i < limit; i++ {
			if originalData[i] != readData[i] {
				mismatchPos = int64(i)
				originalByte = originalData[i]
				readByte = readData[i]
				break
			}
		}
		if mismatchPos == -1 && len(originalData) != len(readData) {
			mismatchPos = int64(limit)
			if len(originalData) > limit {
				originalByte = originalData[limit]
			}
		}

		return fmt.Errorf("data verification failed for file %s: read data does not match original data (lengths: original=%d, read=%d). First mismatch at byte %d (original: %d[0x%X], read: %d[0x%X])",
			filePath, len(originalData), len(readData), mismatchPos, originalByte, originalByte, readByte, readByte)
	}

	return nil
}
