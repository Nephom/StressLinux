package rawdisk

import (
    "bytes"
    "fmt"
    "io"
    "math/rand"
    "os"
    "stress/config"
    "stress/utils"
    "sync"
    "syscall"
    "time"
    "unsafe"
)

// RunRawDiskStressTest runs stress tests on raw disk devices
func RunRawDiskStressTest(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, testConfig RawDiskTestConfig, perfStats *config.PerformanceStats, debug bool) {
    defer wg.Done()

    if len(testConfig.DevicePaths) == 0 {
        errorMsg := "No raw disk devices specified for testing"
        errorChan <- errorMsg
        utils.LogMessage(errorMsg, true)
        return
    }
    if testConfig.TestSize <= 0 {
        utils.LogMessage(fmt.Sprintf("Invalid TestSize %d, using default 100MB", testConfig.TestSize), true)
        testConfig.TestSize = 100 * 1024 * 1024
    }
    if testConfig.BlockSize <= 0 {
        utils.LogMessage(fmt.Sprintf("Invalid BlockSize %d, using default 4KB", testConfig.BlockSize), true)
        testConfig.BlockSize = 4 * 1024
    }
    if testConfig.StartOffset < 0 {
        utils.LogMessage(fmt.Sprintf("Invalid StartOffset %d, using default 1GB", testConfig.StartOffset), true)
        testConfig.StartOffset = 1 * 1024 * 1024 * 1024
    }
    if testConfig.TestSize < testConfig.BlockSize && testConfig.TestMode != "sequential" {
        utils.LogMessage(fmt.Sprintf("Warning: TestSize (%s) is smaller than BlockSize (%s) for random/both mode.",
            utils.FormatSize(testConfig.TestSize), utils.FormatSize(testConfig.BlockSize)), true)
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

    deviceWg := &sync.WaitGroup{}
    for _, devicePath := range testConfig.DevicePaths {
        deviceWg.Add(1)
        go func(dp string) {
            defer deviceWg.Done()

            info, err := os.Stat(dp)
            if err != nil {
                errorMsg := fmt.Sprintf("Device %s not accessible: %v", dp, err)
                errorChan <- errorMsg
                utils.LogMessage(errorMsg, true)
                return
            }

            mode := info.Mode()
            if mode&os.ModeDevice == 0 {
                errorMsg := fmt.Sprintf("%s is not a device", dp)
                errorChan <- errorMsg
                utils.LogMessage(errorMsg, true)
                return
            }

            device, err := os.OpenFile(dp, os.O_RDWR, 0)
            if err != nil {
                errorMsg := fmt.Sprintf("Cannot open device %s for writing: %v (you may need root privileges)", dp, err)
                errorChan <- errorMsg
                utils.LogMessage(errorMsg, true)
                return
            }
            device.Close()

            device, err = os.OpenFile(dp, os.O_RDONLY, 0)
            if err != nil {
                errorMsg := fmt.Sprintf("Cannot open device %s for reading: %v", dp, err)
                errorChan <- errorMsg
                utils.LogMessage(errorMsg, true)
                return
            }

            var deviceSize int64
            fd := device.Fd()
            deviceSize, err = getDeviceSize(fd)
            if err != nil {
                errorMsg := fmt.Sprintf("Failed to get size of device %s: %v", dp, err)
                errorChan <- errorMsg
                utils.LogMessage(errorMsg, true)
                device.Close()
                return
            }
            device.Close()

            if testConfig.StartOffset+testConfig.TestSize > deviceSize {
                errorMsg := fmt.Sprintf("Test area exceeds device size on %s: device size is %s, requested offset %s plus test size %s",
                    dp, utils.FormatSize(deviceSize), utils.FormatSize(testConfig.StartOffset), utils.FormatSize(testConfig.TestSize))
                errorChan <- errorMsg
                utils.LogMessage(errorMsg, true)
                return
            }

            utils.LogMessage(fmt.Sprintf("Device %s checked: OK (Size: %s, Start Offset: %s, Test Size: %s)",
                dp, utils.FormatSize(deviceSize), utils.FormatSize(testConfig.StartOffset), utils.FormatSize(testConfig.TestSize)), debug)

            utils.LogMessage(fmt.Sprintf("Generating %s of random data for tests on %s...", utils.FormatSize(testConfig.TestSize), dp), debug)
            data := make([]byte, testConfig.TestSize)
            if _, err := rand.Read(data); err != nil {
                errorMsg := fmt.Sprintf("Failed to generate random data for %s: %v", dp, err)
                errorChan <- errorMsg
                utils.LogMessage(errorMsg, true)
                return
            }
            utils.LogMessage(fmt.Sprintf("Random data generated for %s.", dp), debug)

            modeWg := &sync.WaitGroup{}
            for _, mode := range testModes {
                modeWg.Add(1)
                go func(currentMode string) {
                    defer modeWg.Done()

                    utils.LogMessage(fmt.Sprintf("Starting raw disk test goroutine for device: %s (mode: %s, size: %s, block: %s, offset: %s)",
                        dp, currentMode, utils.FormatSize(testConfig.TestSize), utils.FormatSize(testConfig.BlockSize),
                        utils.FormatSize(testConfig.StartOffset)), debug)

                    iteration := 0
                    for {
                        iteration++
                        utils.LogMessage(fmt.Sprintf("Device %s, Mode %s, Iteration %d: Starting cycle.", dp, currentMode, iteration), debug)

                        select {
                        case <-stop:
                            utils.LogMessage(fmt.Sprintf("Raw disk test stopped on %s (mode: %s)", dp, currentMode), debug)
                            return
                        default:
                            utils.LogMessage(fmt.Sprintf("Device %s, Mode %s, Iteration %d: Performing write...", dp, currentMode, iteration), debug)
                            writeStart := time.Now()
                            writeErr := performRawDiskWrite(dp, data, currentMode, testConfig.BlockSize, testConfig.StartOffset)
                            writeDuration := time.Since(writeStart)

                            if writeErr != nil {
                                errorMsg := fmt.Sprintf("Raw disk write error on %s (mode: %s, iter: %d, duration: %v): %v", dp, currentMode, iteration, writeDuration, writeErr)
                                errorChan <- errorMsg
                                utils.LogMessage(errorMsg, true)
                                time.Sleep(2 * time.Second)
                                continue
                            }

                            writeSpeedMBps := float64(0)
                            if writeDuration.Seconds() > 0 {
                                writeSpeedMBps = float64(testConfig.TestSize) / writeDuration.Seconds() / (1024 * 1024)
                            }
                            utils.LogMessage(fmt.Sprintf("Raw disk write on %s (mode: %s, iter: %d): %.2f MB/s (%s in %v)",
                                dp, currentMode, iteration, writeSpeedMBps, utils.FormatSize(testConfig.TestSize), writeDuration), debug)

                            utils.LogMessage(fmt.Sprintf("Device %s, Mode %s, Iteration %d: Performing read and verify...", dp, currentMode, iteration), debug)
                            readStart := time.Now()
                            readErr := performRawDiskReadAndVerify(dp, data, currentMode, testConfig.BlockSize, testConfig.StartOffset)
                            readDuration := time.Since(readStart)

                            if readErr != nil {
                                errorMsg := fmt.Sprintf("Raw disk read/verify error on %s (mode: %s, iter: %d, duration: %v): %v", dp, currentMode, iteration, readDuration, readErr)
                                errorChan <- errorMsg
                                utils.LogMessage(errorMsg, true)
                                time.Sleep(2 * time.Second)
                                continue
                            }

                            readSpeedMBps := float64(0)
                            if readDuration.Seconds() > 0 {
                                readSpeedMBps = float64(testConfig.TestSize) / readDuration.Seconds() / (1024 * 1024)
                            }
                            utils.LogMessage(fmt.Sprintf("Raw disk read/verify on %s (mode: %s, iter: %d): %.2f MB/s (%s in %v)",
                                dp, currentMode, iteration, readSpeedMBps, utils.FormatSize(testConfig.TestSize), readDuration), debug)

                            perfStats.Lock()
                            diskPerfKey := fmt.Sprintf("raw:%s|%s|%d", dp, currentMode, testConfig.BlockSize)
                            found := false
                            for i, rdp := range perfStats.RawDisk {
                                existingKey := fmt.Sprintf("raw:%s|%s|%d", rdp.DevicePath, rdp.Mode, rdp.BlockSize)
                                if existingKey == diskPerfKey {
                                    if readSpeedMBps > rdp.ReadSpeed {
                                        perfStats.RawDisk[i].ReadSpeed = readSpeedMBps
                                        utils.LogMessage(fmt.Sprintf("Updated best read speed for %s: %.2f MB/s", diskPerfKey, readSpeedMBps), debug)
                                    }
                                    if writeSpeedMBps > rdp.WriteSpeed {
                                        perfStats.RawDisk[i].WriteSpeed = writeSpeedMBps
                                        utils.LogMessage(fmt.Sprintf("Updated best write speed for %s: %.2f MB/s", diskPerfKey, writeSpeedMBps), debug)
                                    }
                                    perfStats.RawDisk[i].WriteCount++
                                    perfStats.RawDisk[i].ReadCount++
                                    found = true
                                    break
                                }
                            }

                            if !found {
                                newPerf := config.RawDiskPerformance{
                                    DevicePath: dp,
                                    Mode:       currentMode,
                                    BlockSize:  testConfig.BlockSize,
                                    ReadSpeed:  readSpeedMBps,
                                    WriteSpeed: writeSpeedMBps,
                                    WriteCount: 1,
                                    ReadCount:  1,
                                }
                                perfStats.RawDisk = append(perfStats.RawDisk, newPerf)
                                utils.LogMessage(fmt.Sprintf("Added initial perf record for %s: Read=%.2f MB/s, Write=%.2f MB/s, Operations: Write=1, Read=1", diskPerfKey, readSpeedMBps, writeSpeedMBps), debug)
                            }
                            perfStats.Unlock()

                            utils.LogMessage(fmt.Sprintf("Device %s, Mode %s, Iteration %d: Cycle completed successfully. Sleeping.", dp, currentMode, iteration), debug)
                            time.Sleep(150 * time.Millisecond)
                        }
                    }
                }(mode)
            }
            modeWg.Wait()
            utils.LogMessage(fmt.Sprintf("All test modes finished or stopped for device %s.", dp), debug)
        }(devicePath)
    }

    deviceWg.Wait()
    utils.LogMessage("All raw disk device test goroutines have finished.", debug)
}

// getDeviceSize gets the size of a block device using ioctl
func getDeviceSize(fd uintptr) (int64, error) {
    const BLKGETSIZE64 = 0x80081272
    var size int64
    _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, BLKGETSIZE64, uintptr(unsafe.Pointer(&size)))
    if errno != 0 {
        return 0, errno
    }
    return size, nil
}

// performRawDiskWrite writes data to a raw disk device
func performRawDiskWrite(devicePath string, data []byte, mode string, blockSize int64, startOffset int64) error {
    if len(data) == 0 {
        return fmt.Errorf("attempt to write empty data to device %s", devicePath)
    }

    device, err := os.OpenFile(devicePath, os.O_WRONLY, 0)
    if err != nil {
        return fmt.Errorf("failed to open device for writing (%s): %w", devicePath, err)
    }
    defer device.Close()

    totalSize := int64(len(data))
    var totalBytesWritten int64 = 0

    if mode == "sequential" {
        n, writeErr := device.WriteAt(data, startOffset)
        totalBytesWritten = int64(n)
        if writeErr != nil {
            return fmt.Errorf("sequential write error on %s after writing %d bytes at offset %d: %w",
                devicePath, totalBytesWritten, startOffset, writeErr)
        }
        if totalBytesWritten != totalSize {
            return fmt.Errorf("sequential write short write on %s: wrote %d bytes, expected %d",
                devicePath, totalBytesWritten, totalSize)
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
        source := rand.NewSource(time.Now().UnixNano())
        rng := rand.New(source)
        rng.Shuffle(int(blocks), func(i, j int) {
            blockOrder[i], blockOrder[j] = blockOrder[j], blockOrder[i]
        })

        for _, blockIdx := range blockOrder {
            dataStart := blockIdx * blockSize
            dataEnd := dataStart + blockSize
            if dataEnd > totalSize {
                dataEnd = totalSize
            }
            chunkSize := dataEnd - dataStart
            if chunkSize <= 0 {
                continue
            }

            if dataStart >= int64(len(data)) || dataEnd > int64(len(data)) {
                return fmt.Errorf("internal logic error: calculated range [%d:%d] exceeds data length %d",
                    dataStart, dataEnd, len(data))
            }

            deviceOffset := startOffset + dataStart
            n, writeErr := device.WriteAt(data[dataStart:dataEnd], deviceOffset)
            totalBytesWritten += int64(n)
            if writeErr != nil {
                return fmt.Errorf("random write error on %s at offset %d after writing %d bytes for this chunk: %w",
                    devicePath, deviceOffset, n, writeErr)
            }
            if int64(n) != chunkSize {
                return fmt.Errorf("random write short write on %s at offset %d: wrote %d bytes, expected %d",
                    devicePath, deviceOffset, n, chunkSize)
            }
        }

        if totalBytesWritten != totalSize {
            return fmt.Errorf("random write total bytes written mismatch: wrote %d bytes, expected %d for %s",
                totalBytesWritten, totalSize, devicePath)
        }
    }

    if err := device.Sync(); err != nil {
        return fmt.Errorf("failed to sync device (%s) after writing %d bytes: %w",
            devicePath, totalBytesWritten, err)
    }

    return nil
}

// performRawDiskReadAndVerify reads and verifies data from a raw disk device
func performRawDiskReadAndVerify(devicePath string, originalData []byte, mode string, blockSize int64, startOffset int64) error {
    expectedSize := int64(len(originalData))
    if expectedSize == 0 {
        return nil
    }

    device, err := os.Open(devicePath)
    if err != nil {
        return fmt.Errorf("failed to open device for reading (%s): %w", devicePath, err)
    }
    defer device.Close()

    readData := make([]byte, expectedSize)

    if mode == "sequential" {
        n, readErr := device.ReadAt(readData, startOffset)
        if readErr != nil && readErr != io.EOF {
            return fmt.Errorf("sequential read error on %s after reading %d bytes at offset %d: %w",
                devicePath, n, startOffset, readErr)
        }
        if int64(n) != expectedSize {
            return fmt.Errorf("sequential read short read on %s: read %d bytes, expected %d",
                devicePath, n, expectedSize)
        }
    } else {
        blocks := expectedSize / blockSize
        if expectedSize%blockSize > 0 {
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
            dataStart := blockIdx * blockSize
            dataEnd := dataStart + blockSize
            if dataEnd > expectedSize {
                dataEnd = expectedSize
            }
            chunkSize := dataEnd - dataStart
            if chunkSize <= 0 {
                continue
            }

            if dataStart >= int64(len(readData)) || dataEnd > int64(len(readData)) {
                return fmt.Errorf("internal logic error during random read: calculated range [%d:%d] exceeds buffer length %d",
                    dataStart, dataEnd, len(readData))
            }

            deviceOffset := startOffset + dataStart
            n, readErr := device.ReadAt(readData[dataStart:dataEnd], deviceOffset)
            totalBytesRead += int64(n)

            if int64(n) != chunkSize {
                return fmt.Errorf("random read short read on %s at offset %d: read %d bytes, expected %d (error: %v)",
                    devicePath, deviceOffset, n, chunkSize, readErr)
            }

            if readErr != nil && readErr != io.EOF {
                return fmt.Errorf("random read error on %s at offset %d after reading %d bytes for this chunk: %w",
                    devicePath, deviceOffset, n, readErr)
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

        return fmt.Errorf("data verification failed for device %s: read data does not match original data "+
            "(lengths: original=%d, read=%d). First mismatch at byte %d (original: %d[0x%X], read: %d[0x%X])",
            devicePath, len(originalData), len(readData), mismatchPos, originalByte, originalByte, readByte, readByte)
    }

    return nil
}
