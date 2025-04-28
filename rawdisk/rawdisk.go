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
    "sync/atomic"
    "syscall"
    "time"
    "unsafe"
)

// Device access synchronization
var deviceMutexes = sync.Map{}

// getDeviceMutex returns a mutex for a specific device
func getDeviceMutex(devicePath string) *sync.Mutex {
    mutex, ok := deviceMutexes.Load(devicePath)
    if !ok {
        mutex = &sync.Mutex{}
        deviceMutexes.Store(devicePath, mutex)
    }
    return mutex.(*sync.Mutex)
}

// RunRawDiskStressTest runs stress tests on raw disk devices with optimized memory usage and dynamic stats interval
func RunRawDiskStressTest(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, testConfig RawDiskTestConfig, perfStats *config.PerformanceStats, debug bool, duration string) {
    defer wg.Done()

    // Validate and set defaults
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

    // Calculate stats interval based on duration
    dur, err := time.ParseDuration(duration)
    if err != nil {
        errorMsg := fmt.Sprintf("Invalid duration format: %v", err)
        errorChan <- errorMsg
        utils.LogMessage(errorMsg, true)
        return
    }
    durSeconds := int64(dur.Seconds())
    var statsInterval int64
    if durSeconds <= 60 {
        statsInterval = 10 // 10 seconds for short durations
    } else {
        statsInterval = durSeconds / 60 // e.g., 3600s -> 60s, 86400s -> 1440s
    }
    ticker := time.NewTicker(time.Duration(statsInterval) * time.Second)
    defer ticker.Stop()

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

    // Create a buffer pool to reduce memory allocations
    bufferPool := &sync.Pool{
        New: func() interface{} {
            rawBuffer, alignedBuffer := alignedBuffer(testConfig.TestSize)
            return &struct {
                raw    []byte
                buffer []byte
            }{raw: rawBuffer, buffer: alignedBuffer}
        },
    }

    // Create a block buffer pool for random mode operations
    blockBufferPool := &sync.Pool{
        New: func() interface{} {
            rawBuffer, alignedBuffer := alignedBuffer(testConfig.BlockSize)
            return &struct {
                raw    []byte
                buffer []byte
            }{raw: rawBuffer, buffer: alignedBuffer}
        },
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

            if debug {
                utils.LogMessage(fmt.Sprintf("Device %s checked: OK (Size: %s, Start Offset: %s, Test Size: %s)",
                    dp, utils.FormatSize(deviceSize), utils.FormatSize(testConfig.StartOffset), utils.FormatSize(testConfig.TestSize)), true)
            }

            // Generate random data once per device - get from buffer pool
            if debug {
                utils.LogMessage(fmt.Sprintf("Generating %s of random data for tests on %s...", utils.FormatSize(testConfig.TestSize), dp), true)
            }
            
            // Get a buffer from the pool instead of allocating a new one
            bufObj := bufferPool.Get().(*struct {
                raw    []byte
                buffer []byte
            })
            data := bufObj.buffer[:testConfig.TestSize]
            
            if _, err := rand.Read(data); err != nil {
                errorMsg := fmt.Sprintf("Failed to generate random data for %s: %v", dp, err)
                errorChan <- errorMsg
                utils.LogMessage(errorMsg, true)
                bufferPool.Put(bufObj) // Return buffer to pool
                return
            }
            
            if debug {
                utils.LogMessage(fmt.Sprintf("Random data generated for %s.", dp), true)
            }

            modeWg := &sync.WaitGroup{}
            for _, mode := range testModes {
                modeWg.Add(1)
                go func(currentMode string) {
                    defer modeWg.Done()
                    defer bufferPool.Put(bufObj) // Return buffer to pool when done

                    if debug {
                        utils.LogMessage(fmt.Sprintf("Starting raw disk test for %s (mode: %s, size: %s, block: %s, offset: %s)",
                            dp, currentMode, utils.FormatSize(testConfig.TestSize), utils.FormatSize(testConfig.BlockSize), utils.FormatSize(testConfig.StartOffset)), true)
                    }

                    seed := time.Now().UnixNano()
                    iteration := 0
                    var lastStatsUpdate time.Time
                    var writeSpeed, readSpeed atomic.Value
                    writeSpeed.Store(float64(0))
                    readSpeed.Store(float64(0))

                    for {
                        select {
                        case <-stop:
                            if debug {
                                utils.LogMessage(fmt.Sprintf("Raw disk test stopped on %s (mode: %s)", dp, currentMode), true)
                            }
                            return
                        default:
                            iteration++
                            seedForThisIteration := seed + int64(iteration) // Unique seed per iteration
                            
                            if debug {
                                utils.LogMessage(fmt.Sprintf("Device %s, Mode %s, Iteration %d: Starting cycle.", dp, currentMode, iteration), true)
                            }

                            // Obtain device-specific mutex before operations
                            deviceMutex := getDeviceMutex(dp)
                            deviceMutex.Lock()

                            // Perform write operation
                            if debug {
                                utils.LogMessage(fmt.Sprintf("Device %s, Mode %s, Iteration %d: Performing write...", dp, currentMode, iteration), true)
                            }
                            writeStart := time.Now()
                            writeErr := performRawDiskWrite(dp, data, currentMode, testConfig.BlockSize, testConfig.StartOffset, seedForThisIteration, blockBufferPool)
                            writeDuration := time.Since(writeStart)

                            if writeErr != nil {
                                errorMsg := fmt.Sprintf("Raw disk write error on %s (mode: %s, iter: %d, duration: %v): %v",
                                    dp, currentMode, iteration, writeDuration, writeErr)
                                errorChan <- errorMsg
                                utils.LogMessage(errorMsg, true)
                                deviceMutex.Unlock()
                                time.Sleep(2 * time.Second)
                                continue
                            }

                            // Ensure data is properly synced to disk before reading
                            syncStart := time.Now()
                            syncErr := syncDevice(dp)
                            if syncErr != nil {
                                errorMsg := fmt.Sprintf("Device sync error on %s: %v", dp, syncErr)
                                errorChan <- errorMsg
                                utils.LogMessage(errorMsg, true)
                            }
                            syncDuration := time.Since(syncStart)
                            if debug && syncDuration > 100*time.Millisecond {
                                utils.LogMessage(fmt.Sprintf("Device sync took %v on %s", syncDuration, dp), true)
                            }

                            // Perform read and verify operation
                            if debug {
                                utils.LogMessage(fmt.Sprintf("Device %s, Mode %s, Iteration %d: Performing read and verify...", dp, currentMode, iteration), true)
                            }
                            readStart := time.Now()
                            readErr := performRawDiskReadAndVerify(dp, data, currentMode, testConfig.BlockSize, testConfig.StartOffset, seedForThisIteration, blockBufferPool)
                            readDuration := time.Since(readStart)

                            // Release device mutex
                            deviceMutex.Unlock()

                            if readErr != nil {
                                errorMsg := fmt.Sprintf("Raw disk read/verify error on %s (mode: %s, iter: %d, duration: %v): %v",
                                    dp, currentMode, iteration, readDuration, readErr)
                                errorChan <- errorMsg
                                utils.LogMessage(errorMsg, true)
                                time.Sleep(2 * time.Second)
                                continue
                            }

                            // Calculate speeds for this iteration
                            currentWriteSpeedMBps := float64(0)
                            if writeDuration.Seconds() > 0 {
                                currentWriteSpeedMBps = float64(testConfig.TestSize) / writeDuration.Seconds() / (1024 * 1024)
                                writeSpeed.Store(currentWriteSpeedMBps)
                            }
                            currentReadSpeedMBps := float64(0)
                            if readDuration.Seconds() > 0 {
                                currentReadSpeedMBps = float64(testConfig.TestSize) / readDuration.Seconds() / (1024 * 1024)
                                readSpeed.Store(currentReadSpeedMBps)
                            }

                            // Update stats on ticker or when ticker channel has data
                            select {
                            case <-ticker.C:
                                lastStatsUpdate = time.Now()
                                
                                if debug {
                                    utils.LogMessage(fmt.Sprintf("Raw disk write on %s (mode: %s, iter: %d): %.2f MB/s (%s in %v)",
                                        dp, currentMode, iteration, currentWriteSpeedMBps, utils.FormatSize(testConfig.TestSize), writeDuration), true)
                                    utils.LogMessage(fmt.Sprintf("Raw disk read/verify on %s (mode: %s, iter: %d): %.2f MB/s (%s in %v)",
                                        dp, currentMode, iteration, currentReadSpeedMBps, utils.FormatSize(testConfig.TestSize), readDuration), true)
                                }

                                // Update performance statistics
                                updatePerfStats(perfStats, dp, currentMode, testConfig.BlockSize, currentWriteSpeedMBps, currentReadSpeedMBps, debug)
                            default:
                                // If it's been more than statsInterval*2 seconds since last update, force an update
                                if time.Since(lastStatsUpdate) > time.Duration(statsInterval*2)*time.Second {
                                    lastStatsUpdate = time.Now()
                                    updatePerfStats(perfStats, dp, currentMode, testConfig.BlockSize, currentWriteSpeedMBps, currentReadSpeedMBps, debug)
                                }
                            }

                            if debug {
                                utils.LogMessage(fmt.Sprintf("Device %s, Mode %s, Iteration %d: Cycle completed successfully.", dp, currentMode, iteration), true)
                            }

                            // Add a small pause between iterations to reduce CPU usage and give other goroutines a chance
                            time.Sleep(100 * time.Millisecond)
                        }
                    }
                }(mode)
            }
            modeWg.Wait()
            if debug {
                utils.LogMessage(fmt.Sprintf("All test modes finished for device %s.", dp), true)
            }
        }(devicePath)
    }
    deviceWg.Wait()
    if debug {
        utils.LogMessage("All raw disk device test goroutines have finished.", true)
    }
}

// updatePerfStats updates performance statistics
func updatePerfStats(perfStats *config.PerformanceStats, devicePath, mode string, blockSize int64, 
                    writeSpeedMBps, readSpeedMBps float64, debug bool) {
    perfStats.Lock()
    defer perfStats.Unlock()
    
    diskPerfKey := fmt.Sprintf("raw:%s|%s|%d", devicePath, mode, blockSize)
    for i, rdp := range perfStats.RawDisk {
        existingKey := fmt.Sprintf("raw:%s|%s|%d", rdp.DevicePath, rdp.Mode, rdp.BlockSize)
        if existingKey == diskPerfKey {
            if readSpeedMBps > rdp.ReadSpeed {
                perfStats.RawDisk[i].ReadSpeed = readSpeedMBps
                if debug {
                    utils.LogMessage(fmt.Sprintf("Updated best read speed for %s: %.2f MB/s", diskPerfKey, readSpeedMBps), true)
                }
            }
            if writeSpeedMBps > rdp.WriteSpeed {
                perfStats.RawDisk[i].WriteSpeed = writeSpeedMBps
                if debug {
                    utils.LogMessage(fmt.Sprintf("Updated best write speed for %s: %.2f MB/s", diskPerfKey, writeSpeedMBps), true)
                }
            }
            perfStats.RawDisk[i].WriteCount++
            perfStats.RawDisk[i].ReadCount++
            return
        }
    }
    
    newPerf := config.RawDiskPerformance{
        DevicePath: devicePath,
        Mode:       mode,
        BlockSize:  blockSize,
        ReadSpeed:  readSpeedMBps,
        WriteSpeed: writeSpeedMBps,
        WriteCount: 1,
        ReadCount:  1,
    }
    perfStats.RawDisk = append(perfStats.RawDisk, newPerf)
    if debug {
        utils.LogMessage(fmt.Sprintf("Added initial perf record for %s: Read=%.2f MB/s, Write=%.2f MB/s", diskPerfKey, readSpeedMBps, writeSpeedMBps), true)
    }
}

// syncDevice ensures all data is flushed to disk
func syncDevice(devicePath string) error {
    device, err := os.OpenFile(devicePath, os.O_WRONLY, 0)
    if err != nil {
        return fmt.Errorf("failed to open device for sync (%s): %w", devicePath, err)
    }
    defer device.Close()

    if err := device.Sync(); err != nil {
        return fmt.Errorf("failed to sync device (%s): %w", devicePath, err)
    }
    if err := syscall.Fsync(int(device.Fd())); err != nil {
        return fmt.Errorf("failed to fsync device (%s): %w", devicePath, err)
    }
    return nil
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
    alignSize := 4096
    alignedSize := ((size + int64(alignSize) - 1) / int64(alignSize)) * int64(alignSize)
    rawBuffer := make([]byte, alignedSize+int64(alignSize))
    addr := uintptr(unsafe.Pointer(&rawBuffer[0]))
    offset := uintptr(alignSize) - (addr % uintptr(alignSize))
    if offset == uintptr(alignSize) {
        offset = 0
    }
    return rawBuffer, rawBuffer[offset : offset+uintptr(alignedSize)]
}

// performRawDiskWrite writes data to a raw disk device
func performRawDiskWrite(
    devicePath string, 
    data []byte, 
    mode string, 
    blockSize int64, 
    startOffset int64, 
    seed int64, 
    blockBufferPool *sync.Pool,
) error {
    if len(data) == 0 {
        return fmt.Errorf("attempt to write empty data to device %s", devicePath)
    }

    device, err := os.OpenFile(devicePath, os.O_WRONLY|syscall.O_DIRECT, 0)
    if err != nil {
        return fmt.Errorf("failed to open device for writing (%s): %w", devicePath, err)
    }
    defer device.Close()

    totalSize := int64(len(data))
    var totalBytesWritten int64

    if mode == "sequential" {
        // For sequential mode, we use a buffer large enough for the entire data
        rawBuffer, alignedData := alignedBuffer(totalSize)
        _ = rawBuffer // Keep reference to prevent GC
        copy(alignedData, data)
        n, writeErr := device.WriteAt(alignedData[:totalSize], startOffset)
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
        // For random mode, calculate blocks and create random order
        blocks := totalSize / blockSize
        if totalSize%blockSize > 0 {
            blocks++
        }
        blockOrder := make([]int64, blocks)
        for i := int64(0); i < blocks; i++ {
            blockOrder[i] = i
        }
        
        // Use deterministic randomization with the seed provided
        rng := rand.New(rand.NewSource(seed))
        rng.Shuffle(int(blocks), func(i, j int) {
            blockOrder[i], blockOrder[j] = blockOrder[j], blockOrder[i]
        })

        // Get a block buffer from the pool
        bufObj := blockBufferPool.Get().(*struct {
            raw    []byte
            buffer []byte
        })
        alignedBlockBuffer := bufObj.buffer[:blockSize]
        defer blockBufferPool.Put(bufObj) // Return to pool when done

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

            // Clear buffer before reuse
            for i := range alignedBlockBuffer[:chunkSize] {
                alignedBlockBuffer[i] = 0
            }
            
            // Copy data to the aligned buffer
            copy(alignedBlockBuffer[:chunkSize], data[dataStart:dataEnd])
            
            // Write to disk
            deviceOffset := startOffset + dataStart
            n, writeErr := device.WriteAt(alignedBlockBuffer[:chunkSize], deviceOffset)
            totalBytesWritten += int64(n)
            
            if writeErr != nil {
                return fmt.Errorf("random write error on %s at offset %d after writing %d bytes: %w", 
                    devicePath, deviceOffset, n, writeErr)
            }
            if int64(n) != chunkSize {
                return fmt.Errorf("random write short write on %s at offset %d: wrote %d bytes, expected %d", 
                    devicePath, deviceOffset, n, chunkSize)
            }
        }
        
        if totalBytesWritten != totalSize {
            return fmt.Errorf("random write total bytes mismatch: wrote %d bytes, expected %d for %s", 
                totalBytesWritten, totalSize, devicePath)
        }
    }

    // Ensure data is synced to disk
    if err := device.Sync(); err != nil {
        return fmt.Errorf("failed to sync device (%s) after writing %d bytes: %w", 
            devicePath, totalBytesWritten, err)
    }
    if err := syscall.Fsync(int(device.Fd())); err != nil {
        return fmt.Errorf("failed to fsync device (%s) after writing %d bytes: %w", 
            devicePath, totalBytesWritten, err)
    }

    return nil
}

// performRawDiskReadAndVerify reads and verifies data from a raw disk device
func performRawDiskReadAndVerify(
    devicePath string, 
    originalData []byte, 
    mode string, 
    blockSize int64, 
    startOffset int64, 
    seed int64, 
    blockBufferPool *sync.Pool,
) error {
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
        // For sequential mode, read the entire data at once
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
        
        // Verify the data
        if !bytes.Equal(originalData, readData[:expectedSize]) {
            return identifyMismatch(devicePath, originalData, readData[:expectedSize], "sequential")
        }
    } else {
        // For random mode, calculate blocks and read them in the same order as they were written
        blocks := expectedSize / blockSize
        if expectedSize%blockSize > 0 {
            blocks++
        }
        blockOrder := make([]int64, blocks)
        for i := int64(0); i < blocks; i++ {
            blockOrder[i] = i
        }
        
        // Use deterministic randomization with the same seed used for writing
        rng := rand.New(rand.NewSource(seed))
        rng.Shuffle(int(blocks), func(i, j int) {
            blockOrder[i], blockOrder[j] = blockOrder[j], blockOrder[i]
        })

        // Get a block buffer from the pool
        bufObj := blockBufferPool.Get().(*struct {
            raw    []byte
            buffer []byte
        })
        blockBuffer := bufObj.buffer[:blockSize]
        defer blockBufferPool.Put(bufObj)

        var totalBytesRead int64
        
        // Read and verify each block individually
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
            
            // Clear buffer before reuse
            for i := range blockBuffer[:chunkSize] {
                blockBuffer[i] = 0
            }
            
            deviceOffset := startOffset + dataStart
            n, readErr := device.ReadAt(blockBuffer[:chunkSize], deviceOffset)
            totalBytesRead += int64(n)
            
            if int64(n) != chunkSize {
                return fmt.Errorf("random read short read on %s at offset %d: read %d bytes, expected %d (error: %v)", 
                    devicePath, deviceOffset, n, chunkSize, readErr)
            }
            if readErr != nil && readErr != io.EOF {
                return fmt.Errorf("random read error on %s at offset %d after reading %d bytes: %w", 
                    devicePath, deviceOffset, n, readErr)
            }
            
            // Verify this block immediately
            if !bytes.Equal(originalData[dataStart:dataEnd], blockBuffer[:chunkSize]) {
                return fmt.Errorf("data verification failed for block %d at offset %d on device %s", 
                    blockIdx, deviceOffset, devicePath)
            }
        }
        
        if totalBytesRead != expectedSize {
            return fmt.Errorf("random read total bytes mismatch on %s: read %d bytes, expected %d", 
                devicePath, totalBytesRead, expectedSize)
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
    
    // If no mismatch found but lengths differ
    if mismatchPos == -1 && len(originalData) != len(readData) {
        mismatchPos = int64(limit)
        if len(originalData) > limit {
            originalByte = originalData[limit]
        }
    }
    
    // Count total mismatches (limited to avoid performance impact)
    mismatchCount := 0
    maxMismatchesToCount := 1000
    for i := 0; i < limit && mismatchCount < maxMismatchesToCount; i++ {
        if originalData[i] != readData[i] {
            mismatchCount++
        }
    }
    
    // If we hit the limit, indicate there could be more mismatches
    mismatchCountStr := fmt.Sprintf("%d", mismatchCount)
    if mismatchCount == maxMismatchesToCount {
        mismatchCountStr = fmt.Sprintf("%d+", mismatchCount)
    }
    
    // Prepare context around the first mismatch for debugging
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
    return fmt.Errorf("data verification failed for device %s in %s mode: read data does not match original data (lengths: original=%d, read=%d, mismatches=%s). First mismatch at byte %d (original: %d[0x%X], read: %d[0x%X])%s",
        devicePath, mode, len(originalData), len(readData), mismatchCountStr, mismatchPos, originalByte, originalByte, readByte, readByte, contextMsg)
}
