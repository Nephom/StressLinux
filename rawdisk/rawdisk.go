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
        utils.LogMessage(fmt.Sprintf("Warning: TestSize (%s) is smaller than BlockSize (%s) for random mode.",
            utils.FormatSize(testConfig.TestSize), utils.FormatSize(testConfig.BlockSize)), true)
    }

    var testModes []string
    if testConfig.TestMode == "both" {
        errorMsg := "Test mode 'both' is not supported. Please specify 'sequential' or 'random'."
        errorChan <- errorMsg
        utils.LogMessage(errorMsg, true)
        return
    } else if testConfig.TestMode == "sequential" || testConfig.TestMode == "random" {
        testModes = []string{testConfig.TestMode}
    } else {
        errorMsg := fmt.Sprintf("Invalid test mode: %s. Use 'sequential' or 'random'.", testConfig.TestMode)
        errorChan <- errorMsg
        utils.LogMessage(errorMsg, true)
        return
    }

    deviceWg := &sync.WaitGroup{}
    for _, devicePath := range testConfig.DevicePaths {
        deviceWg.Add(1)
        go func(dp string) {
            defer deviceWg.Done()

            // Check if device exists and is a block device
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

            // Check if device can be opened with O_DIRECT
            device, err := os.OpenFile(dp, os.O_RDWR|syscall.O_DIRECT, 0)
            if err != nil {
                errorMsg := fmt.Sprintf("Cannot open device %s for writing with O_DIRECT: %v (you may need root privileges)", dp, err)
                errorChan <- errorMsg
                utils.LogMessage(errorMsg, true)
                return
            }
            device.Close()

            device, err = os.OpenFile(dp, os.O_RDONLY|syscall.O_DIRECT, 0)
            if err != nil {
                errorMsg := fmt.Sprintf("Cannot open device %s for reading with O_DIRECT: %v", dp, err)
                errorChan <- errorMsg
                utils.LogMessage(errorMsg, true)
                return
            }

            // Get device size
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

            // Check if test area fits within device
            if testConfig.StartOffset+testConfig.TestSize > deviceSize {
                errorMsg := fmt.Sprintf("Test area exceeds device size on %s: device size is %s, requested offset %s plus test size %s",
                    dp, utils.FormatSize(deviceSize), utils.FormatSize(testConfig.StartOffset), utils.FormatSize(testConfig.TestSize))
                errorChan <- errorMsg
                utils.LogMessage(errorMsg, true)
                return
            }

            utils.LogMessage(fmt.Sprintf("Device %s checked: OK (Size: %s, Start Offset: %s, Test Size: %s)",
                dp, utils.FormatSize(deviceSize), utils.FormatSize(testConfig.StartOffset), utils.FormatSize(testConfig.TestSize)), debug)

            modeWg := &sync.WaitGroup{}
            for _, mode := range testModes {
                modeWg.Add(1)
                go func(currentMode string) {
                    defer modeWg.Done()

                    // Generate mode-specific data with proper alignment
                    utils.LogMessage(fmt.Sprintf("Generating %s of random data for tests on %s (mode: %s)...",
                        utils.FormatSize(testConfig.TestSize), dp, currentMode), debug)

                    // Ensure 4KB alignment for O_DIRECT
                    alignedSize := ((testConfig.TestSize + 4095) / 4096) * 4096

                    // Allocate memory with additional space for alignment adjustment
                    rawBuffer := make([]byte, alignedSize+4096)

                    // Calculate alignment offset - fixed type conversion
                    addr := uintptr(unsafe.Pointer(&rawBuffer[0]))
                    alignmentOffset := uintptr(4096) - (addr % uintptr(4096))
                    if alignmentOffset == uintptr(4096) {
                        alignmentOffset = 0
                    }

                    // Create properly aligned slice from the buffer
                    data := rawBuffer[alignmentOffset : alignmentOffset+uintptr(alignedSize)]

                    // Fill with random data
                    if _, err := rand.Read(data[:testConfig.TestSize]); err != nil {
                        errorMsg := fmt.Sprintf("Failed to generate random data for %s (mode: %s): %v", dp, currentMode, err)
                        errorChan <- errorMsg
                        utils.LogMessage(errorMsg, true)
                        return
                    }
                    utils.LogMessage(fmt.Sprintf("Random data generated for %s (mode: %s).", dp, currentMode), debug)

                    utils.LogMessage(fmt.Sprintf("Starting raw disk test goroutine for device: %s (mode: %s, size: %s, block: %s, offset: %s)",
                        dp, currentMode, utils.FormatSize(testConfig.TestSize), utils.FormatSize(testConfig.BlockSize),
                        utils.FormatSize(testConfig.StartOffset)), debug)

                    seed := time.Now().UnixNano()
                    iteration := 0
                    for {
                        iteration++
                        utils.LogMessage(fmt.Sprintf("Device %s, Mode %s, Iteration %d: Starting cycle.", dp, currentMode, iteration), debug)

                        select {
                        case <-stop:
                            utils.LogMessage(fmt.Sprintf("Raw disk test stopped on %s (mode: %s)", dp, currentMode), debug)
                            return
                        default:
                            // Perform write operation
                            utils.LogMessage(fmt.Sprintf("Device %s, Mode %s, Iteration %d: Performing write...", dp, currentMode, iteration), debug)
                            writeStart := time.Now()
                            writeErr := performRawDiskWrite(dp, data[:testConfig.TestSize], currentMode, testConfig.BlockSize, testConfig.StartOffset, seed)
                            writeDuration := time.Since(writeStart)

                            if writeErr != nil {
                                errorMsg := fmt.Sprintf("Raw disk write error on %s (mode: %s, iter: %d, duration: %v): %v",
                                    dp, currentMode, iteration, writeDuration, writeErr)
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

                            // Ensure data is actually written to disk before reading
                            time.Sleep(50 * time.Millisecond)

                            // Perform read and verify operation
                            utils.LogMessage(fmt.Sprintf("Device %s, Mode %s, Iteration %d: Performing read and verify...", dp, currentMode, iteration), debug)
                            readStart := time.Now()
                            readErr := performRawDiskReadAndVerify(dp, data[:testConfig.TestSize], currentMode, testConfig.BlockSize, testConfig.StartOffset, seed)
                            readDuration := time.Since(readStart)

                            if readErr != nil {
                                errorMsg := fmt.Sprintf("Raw disk read/verify error on %s (mode: %s, iter: %d, duration: %v): %v",
                                    dp, currentMode, iteration, readDuration, readErr)
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

                            // Update performance statistics
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
                                utils.LogMessage(fmt.Sprintf("Added initial perf record for %s: Read=%.2f MB/s, Write=%.2f MB/s, Operations: Write=1, Read=1",
                                    diskPerfKey, readSpeedMBps, writeSpeedMBps), debug)
                            }
                            perfStats.Unlock()

                            utils.LogMessage(fmt.Sprintf("Device %s, Mode %s, Iteration %d: Cycle completed successfully. Sleeping.",
                                dp, currentMode, iteration), debug)
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

// alignedBuffer allocates a buffer aligned to 4KB for O_DIRECT operations
func alignedBuffer(size int64) ([]byte, []byte) {
    // Use fixed sizes to avoid type conversion issues
    alignSize := 4096
    alignedSize := ((size + int64(alignSize) - 1) / int64(alignSize)) * int64(alignSize)

    // Allocate with extra space for alignment adjustment
    rawBuffer := make([]byte, alignedSize+int64(alignSize))

    // Calculate alignment offset using proper type conversions
    addr := uintptr(unsafe.Pointer(&rawBuffer[0]))
    offset := uintptr(alignSize) - (addr % uintptr(alignSize))
    if offset == uintptr(alignSize) {
        offset = 0
    }

    // Return both the raw buffer (to prevent GC) and the aligned slice
    return rawBuffer, rawBuffer[offset : offset+uintptr(alignedSize)]
}

// performRawDiskWrite writes data to a raw disk device
func performRawDiskWrite(devicePath string, data []byte, mode string, blockSize int64, startOffset int64, seed int64) error {
    if len(data) == 0 {
        return fmt.Errorf("attempt to write empty data to device %s", devicePath)
    }

    device, err := os.OpenFile(devicePath, os.O_WRONLY|syscall.O_DIRECT, 0)
    if err != nil {
        return fmt.Errorf("failed to open device for writing (%s): %w", devicePath, err)
    }
    defer device.Close()

    totalSize := int64(len(data))
    var totalBytesWritten int64 = 0

    if mode == "sequential" {
        // For sequential mode, write the entire buffer at once
        rawBuffer, alignedData := alignedBuffer(totalSize)
        _ = rawBuffer // Keep reference to prevent GC

        copy(alignedData, data)

        n, writeErr := device.WriteAt(alignedData, startOffset)
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
        // For random mode, write blocks in random order
        blocks := totalSize / blockSize
        if totalSize%blockSize > 0 {
            blocks++
        }

        blockOrder := make([]int64, blocks)
        for i := int64(0); i < blocks; i++ {
            blockOrder[i] = i
        }
        rng := rand.New(rand.NewSource(seed))
        rng.Shuffle(int(blocks), func(i, j int) {
            blockOrder[i], blockOrder[j] = blockOrder[j], blockOrder[i]
        })

        // Allocate a single aligned buffer for block operations
        rawBlockBuffer, alignedBlockBuffer := alignedBuffer(blockSize)
        _ = rawBlockBuffer // Keep reference to prevent GC

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

            // Copy data to aligned buffer
            copy(alignedBlockBuffer[:chunkSize], data[dataStart:dataEnd])

            deviceOffset := startOffset + dataStart
            n, writeErr := device.WriteAt(alignedBlockBuffer[:chunkSize], deviceOffset)
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

    // Ensure data is flushed to disk
    if err := device.Sync(); err != nil {
        return fmt.Errorf("failed to sync device (%s) after writing %d bytes: %w",
            devicePath, totalBytesWritten, err)
    }

    // Call fsync explicitly for extra safety
    if err := syscall.Fsync(int(device.Fd())); err != nil {
        return fmt.Errorf("failed to fsync device (%s) after writing %d bytes: %w",
            devicePath, totalBytesWritten, err)
    }

    return nil
}

// performRawDiskReadAndVerify reads and verifies data from a raw disk device
func performRawDiskReadAndVerify(devicePath string, originalData []byte, mode string, blockSize int64, startOffset int64, seed int64) error {
    expectedSize := int64(len(originalData))
    if expectedSize == 0 {
        return nil
    }

    device, err := os.OpenFile(devicePath, os.O_RDONLY|syscall.O_DIRECT, 0)
    if err != nil {
        return fmt.Errorf("failed to open device for reading (%s): %w", devicePath, err)
    }
    defer device.Close()

    if mode == "sequential" {
        // For sequential mode, read the entire buffer at once with proper alignment
        rawBuffer, readData := alignedBuffer(expectedSize)
        _ = rawBuffer // Keep reference to prevent GC

        n, readErr := device.ReadAt(readData[:expectedSize], startOffset)
        if readErr != nil && readErr != io.EOF {
            return fmt.Errorf("sequential read error on %s after reading %d bytes at offset %d: %w",
                devicePath, n, startOffset, readErr)
        }
        if int64(n) != expectedSize {
            return fmt.Errorf("sequential read short read on %s: read %d bytes, expected %d",
                devicePath, n, expectedSize)
        }

        // Compare read data with original data
        if !bytes.Equal(originalData, readData[:expectedSize]) {
            return identifyMismatch(devicePath, originalData, readData[:expectedSize], "sequential")
        }
    } else {
        // For random mode, read blocks in the same random order as they were written
        blocks := expectedSize / blockSize
        if expectedSize%blockSize > 0 {
            blocks++
        }

        blockOrder := make([]int64, blocks)
        for i := int64(0); i < blocks; i++ {
            blockOrder[i] = i
        }
        rng := rand.New(rand.NewSource(seed))
        rng.Shuffle(int(blocks), func(i, j int) {
            blockOrder[i], blockOrder[j] = blockOrder[j], blockOrder[i]
        })

        // Allocate an aligned result buffer for the entire data
        rawResultBuffer, resultBuffer := alignedBuffer(expectedSize)
        _ = rawResultBuffer // Keep reference to prevent GC

        // And a buffer for reading individual blocks
        rawBlockBuffer, blockBuffer := alignedBuffer(blockSize)
        _ = rawBlockBuffer // Keep reference to prevent GC

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

            deviceOffset := startOffset + dataStart
            n, readErr := device.ReadAt(blockBuffer[:chunkSize], deviceOffset)
            totalBytesRead += int64(n)

            if int64(n) != chunkSize {
                return fmt.Errorf("random read short read on %s at offset %d: read %d bytes, expected %d (error: %v)",
                    devicePath, deviceOffset, n, chunkSize, readErr)
            }

            if readErr != nil && readErr != io.EOF {
                return fmt.Errorf("random read error on %s at offset %d after reading %d bytes for this chunk: %w",
                    devicePath, deviceOffset, n, readErr)
            }

            // Copy block data to the result buffer at the correct position
            copy(resultBuffer[dataStart:dataEnd], blockBuffer[:chunkSize])

            // Also verify each block immediately
            expectedBlock := originalData[dataStart:dataEnd]
            if !bytes.Equal(expectedBlock, blockBuffer[:chunkSize]) {
                return fmt.Errorf("data verification failed for block %d on device %s",
                    blockIdx, devicePath)
            }
        }

        if totalBytesRead != expectedSize {
            return fmt.Errorf("random read total bytes mismatch on %s: read %d bytes, expected %d",
                devicePath, totalBytesRead, expectedSize)
        }

        // Double-check the entire buffer as well
        if !bytes.Equal(originalData, resultBuffer[:expectedSize]) {
            return identifyMismatch(devicePath, originalData, resultBuffer[:expectedSize], "random (full buffer)")
        }
    }

    return nil
}

// identifyMismatch provides detailed information about data verification failures
func identifyMismatch(devicePath string, originalData, readData []byte, mode string) error {
    mismatchPos := int64(-1)
    var originalByte, readByte byte
    limit := len(originalData)
    if len(readData) < limit {
        limit = len(readData)
    }

    // Find the first mismatch
    for i := 0; i < limit; i++ {
        if originalData[i] != readData[i] {
            mismatchPos = int64(i)
            originalByte = originalData[i]
            readByte = readData[i]
            break
        }
    }

    // If no direct mismatch but lengths differ
    if mismatchPos == -1 && len(originalData) != len(readData) {
        mismatchPos = int64(limit)
        if len(originalData) > limit {
            originalByte = originalData[limit]
        }
    }

    // Count total mismatches for better diagnosis
    mismatchCount := 0
    for i := 0; i < limit; i++ {
        if originalData[i] != readData[i] {
            mismatchCount++
        }
    }

    // Create context around the mismatch
    var contextStart, contextEnd int
    if mismatchPos >= 0 {
        contextStart = int(mismatchPos) - 16
        if contextStart < 0 {
            contextStart = 0
        }
        contextEnd = int(mismatchPos) + 16
        if contextEnd > limit {
            contextEnd = limit
        }
    }

    contextMsg := ""
    if contextStart < contextEnd {
        originalContext := fmt.Sprintf("Original[%d:%d]: % X", contextStart, contextEnd, originalData[contextStart:contextEnd])
        readContext := fmt.Sprintf("Read[%d:%d]: % X", contextStart, contextEnd, readData[contextStart:contextEnd])
        contextMsg = "\n" + originalContext + "\n" + readContext
    }

    return fmt.Errorf("data verification failed for device %s in %s mode: "+
        "read data does not match original data "+
        "(lengths: original=%d, read=%d, mismatches=%d). First mismatch at byte %d "+
        "(original: %d[0x%X], read: %d[0x%X])%s",
        devicePath, mode, len(originalData), len(readData), mismatchCount, mismatchPos,
        originalByte, originalByte, readByte, readByte, contextMsg)
}
