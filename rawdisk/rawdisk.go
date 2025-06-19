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

// 全局 buffer pools，避免重複創建
var (
    globalBufferPool      *sync.Pool
    globalBlockBufferPool *sync.Pool
    poolOnce              sync.Once
)

// 初始化全局 buffer pools
func initBufferPools(testSize, blockSize int64) {
    poolOnce.Do(func() {
        globalBufferPool = &sync.Pool{
            New: func() interface{} {
                rawBuffer, alignedBuffer := alignedBuffer(testSize)
                return &struct {
                    raw    []byte
                    buffer []byte
                }{raw: rawBuffer, buffer: alignedBuffer}
            },
        }
        
        globalBlockBufferPool = &sync.Pool{
            New: func() interface{} {
                rawBuffer, alignedBuffer := alignedBuffer(blockSize)
                return &struct {
                    raw    []byte
                    buffer []byte
                }{raw: rawBuffer, buffer: alignedBuffer}
            },
        }
    })
}

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

// 優化的 buffer 結構
type BufferManager struct {
    testBuffer  []byte
    blockBuffer []byte
    testBufObj  interface{}
    blockBufObj interface{}
}

// 創建 buffer manager
func NewBufferManager(testSize, blockSize int64) *BufferManager {
    initBufferPools(testSize, blockSize)
    
    testBufObj := globalBufferPool.Get().(*struct {
        raw    []byte
        buffer []byte
    })
    
    blockBufObj := globalBlockBufferPool.Get().(*struct {
        raw    []byte
        buffer []byte
    })
    
    return &BufferManager{
        testBuffer:  testBufObj.buffer[:testSize],
        blockBuffer: blockBufObj.buffer[:blockSize],
        testBufObj:  testBufObj,
        blockBufObj: blockBufObj,
    }
}

// 釋放 buffer
func (bm *BufferManager) Release() {
    if bm.testBufObj != nil {
        globalBufferPool.Put(bm.testBufObj)
    }
    if bm.blockBufObj != nil {
        globalBlockBufferPool.Put(bm.blockBufObj)
    }
}

// RunRawDiskStressTest 優化後的主函數
func RunRawDiskStressTest(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, testConfig RawDiskTestConfig, perfStats *config.PerformanceStats, debug bool, duration string) {
    defer wg.Done()

    // Generate main PID for this test
    mainPID := rand.Intn(1000000) + 1000
    utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] started on %d devices", mainPID, len(testConfig.DevicePaths)), false)

    // Validate and set defaults
    if len(testConfig.DevicePaths) == 0 {
        errorMsg := fmt.Sprintf("Raw disk test [PID: %d] no raw disk devices specified for testing", mainPID)
        errorChan <- "No raw disk devices specified"
        utils.LogMessage(errorMsg, false)
        return
    }
    if testConfig.TestSize <= 0 {
        utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] invalid TestSize %d, using default 100MB", mainPID, testConfig.TestSize), true)
        testConfig.TestSize = 100 * 1024 * 1024
    }
    if testConfig.BlockSize <= 0 {
        utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] invalid BlockSize %d, using default 4KB", mainPID, testConfig.BlockSize), true)
        testConfig.BlockSize = 4 * 1024
    }

    // 限制記憶體使用：如果測試大小超過 256MB，使用分塊處理
    const maxTestSize = 256 * 1024 * 1024
    if testConfig.TestSize > maxTestSize {
        utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] TestSize %s exceeds limit, reducing to %s", mainPID, utils.FormatSize(testConfig.TestSize), utils.FormatSize(maxTestSize)), true)
        testConfig.TestSize = maxTestSize
    }

    // Calculate stats interval
    dur, err := time.ParseDuration(duration)
    if err != nil {
        errorMsg := fmt.Sprintf("Raw disk test [PID: %d] invalid duration format: %v", mainPID, err)
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
        errorMsg := fmt.Sprintf("Raw disk test [PID: %d] test mode 'both' is not supported. Please specify 'sequential' or 'random'", mainPID)
        errorChan <- "Test mode 'both' is not supported"
        utils.LogMessage(errorMsg, false)
        return
    } else if testConfig.TestMode == "sequential" || testConfig.TestMode == "random" {
        testModes = []string{testConfig.TestMode}
    } else {
        errorMsg := fmt.Sprintf("Raw disk test [PID: %d] invalid test mode: %s. Use 'sequential' or 'random'", mainPID, testConfig.TestMode)
        errorChan <- fmt.Sprintf("Invalid test mode: %s", testConfig.TestMode)
        utils.LogMessage(errorMsg, false)
        return
    }

    // 限制並發數量，避免創建過多 goroutine
    const maxConcurrentDevices = 4
    semaphore := make(chan struct{}, maxConcurrentDevices)
    
    deviceWg := &sync.WaitGroup{}
    for i, devicePath := range testConfig.DevicePaths {
        deviceWg.Add(1)
        
        // 控制並發數量
        semaphore <- struct{}{}
        
        go func(dp string, index int) {
            defer func() {
                deviceWg.Done()
                <-semaphore // 釋放信號量
            }()

            // 設備驗證邏輯保持不變...
            if !validateDevice(dp, testConfig, mainPID, errorChan, debug) {
                return
            }

            // 創建 buffer manager
            bufferManager := NewBufferManager(testConfig.TestSize, testConfig.BlockSize)
            defer bufferManager.Release()

            // 生成隨機數據
            if debug {
                utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] generating %s of random data for tests on %s...", mainPID, utils.FormatSize(testConfig.TestSize), dp), true)
            }
            
            if _, err := rand.Read(bufferManager.testBuffer); err != nil {
                errorMsg := fmt.Sprintf("Raw disk test [PID: %d] failed to generate random data for %s: %v", mainPID, dp, err)
                errorChan <- fmt.Sprintf("Failed to generate random data for device %s", dp)
                utils.LogMessage(errorMsg, false)
                return
            }

            utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] starting test on %s", mainPID, dp), false)

            // 串行執行測試模式，避免並發問題
            for j, mode := range testModes {
                subPID := mainPID + (index*10 + j + 1)*100
                
                if debug {
                    utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] starting raw disk test for %s (mode: %s)", mainPID, dp, mode), true)
                }

                // 執行測試循環
                if err := runTestLoop(dp, mode, bufferManager.testBuffer, testConfig, stop, ticker, statsInterval, perfStats, debug, mainPID, subPID); err != nil {
                    errorChan <- fmt.Sprintf("Test error on %s: %v", dp, err)
                    return
                }
            }
        }(devicePath, i)
    }
    
    deviceWg.Wait()
    utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] completed", mainPID), false)
}

// 設備驗證函數
func validateDevice(devicePath string, testConfig RawDiskTestConfig, mainPID int, errorChan chan string, debug bool) bool {
    // 檢查設備是否存在
    info, err := os.Stat(devicePath)
    if err != nil {
        errorMsg := fmt.Sprintf("Raw disk test [PID: %d] device %s not accessible: %v", mainPID, devicePath, err)
        errorChan <- fmt.Sprintf("Device %s not accessible", devicePath)
        utils.LogMessage(errorMsg, false)
        return false
    }
    
    mode := info.Mode()
    if mode&os.ModeDevice == 0 {
        errorMsg := fmt.Sprintf("Raw disk test [PID: %d] %s is not a device", mainPID, devicePath)
        errorChan <- fmt.Sprintf("%s is not a device", devicePath)
        utils.LogMessage(errorMsg, false)
        return false
    }

    // 測試設備是否可以打開
    device, err := os.OpenFile(devicePath, os.O_RDWR|syscall.O_DIRECT, 0)
    if err != nil {
        errorMsg := fmt.Sprintf("Raw disk test [PID: %d] cannot open device %s: %v", mainPID, devicePath, err)
        errorChan <- fmt.Sprintf("Cannot open device %s", devicePath)
        utils.LogMessage(errorMsg, false)
        return false
    }
    
    // 獲取設備大小
    deviceSize, err := getDeviceSize(device.Fd())
    device.Close()
    
    if err != nil {
        errorMsg := fmt.Sprintf("Raw disk test [PID: %d] failed to get size of device %s: %v", mainPID, devicePath, err)
        errorChan <- fmt.Sprintf("Failed to get size of device %s", devicePath)
        utils.LogMessage(errorMsg, false)
        return false
    }

    // 檢查測試區域是否在設備範圍內
    if testConfig.StartOffset+testConfig.TestSize > deviceSize {
        errorMsg := fmt.Sprintf("Raw disk test [PID: %d] test area exceeds device size on %s", mainPID, devicePath)
        errorChan <- fmt.Sprintf("Test area exceeds device size on %s", devicePath)
        utils.LogMessage(errorMsg, false)
        return false
    }

    if debug {
        utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] device %s validated successfully", mainPID, devicePath), true)
    }
    
    return true
}

// 測試循環函數
func runTestLoop(devicePath, mode string, data []byte, testConfig RawDiskTestConfig, stop chan struct{}, ticker *time.Ticker, statsInterval int64, perfStats *config.PerformanceStats, debug bool, mainPID, subPID int) error {
    seed := time.Now().UnixNano()
    iteration := 0
    var lastStatsUpdate time.Time
    
    // 創建專用的 block buffer
    blockBufObj := globalBlockBufferPool.Get().(*struct {
        raw    []byte
        buffer []byte
    })
    defer globalBlockBufferPool.Put(blockBufObj)
    
    for {
        select {
        case <-stop:
            utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] received stop signal for %s (mode: %s)", mainPID, devicePath, mode), false)
            return nil
        default:
            iteration++
            seedForThisIteration := seed + int64(iteration)
            
            if debug {
                utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] device %s, mode %s, iteration %d: Starting cycle", mainPID, devicePath, mode, iteration), true)
            }

            // 獲取設備鎖
            deviceMutex := getDeviceMutex(devicePath)
            deviceMutex.Lock()

            // 執行寫入操作
            writeStart := time.Now()
            writeErr := performRawDiskWriteOptimized(mainPID, devicePath, data, mode, testConfig.BlockSize, testConfig.StartOffset, seedForThisIteration, blockBufObj.buffer, debug)
            writeDuration := time.Since(writeStart)

            if writeErr != nil {
                deviceMutex.Unlock()
                if debug {
                    utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] write error: %v", mainPID, writeErr), true)
                }
                time.Sleep(2 * time.Second)
                continue
            }

            // 同步設備
            if err := syncDevice(devicePath); err != nil && debug {
                utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] sync error: %v", mainPID, err), true)
            }

            // 執行讀取和驗證
            readStart := time.Now()
            readErr := performRawDiskReadAndVerifyOptimized(mainPID, devicePath, data, mode, testConfig.BlockSize, testConfig.StartOffset, seedForThisIteration, blockBufObj.buffer, debug)
            readDuration := time.Since(readStart)

            deviceMutex.Unlock()

            if readErr != nil {
                if debug {
                    utils.LogMessage(fmt.Sprintf("Raw disk test [PID: %d] read error: %v", mainPID, readErr), true)
                }
                time.Sleep(2 * time.Second)
                continue
            }

            // 計算速度
            var currentWriteSpeedMBps, currentReadSpeedMBps float64
            if writeDuration.Seconds() > 0 {
                currentWriteSpeedMBps = float64(testConfig.TestSize) / writeDuration.Seconds() / (1024 * 1024)
            }
            if readDuration.Seconds() > 0 {
                currentReadSpeedMBps = float64(testConfig.TestSize) / readDuration.Seconds() / (1024 * 1024)
            }

            // 更新統計
            select {
            case <-ticker.C:
                lastStatsUpdate = time.Now()
                updatePerfStats(perfStats, devicePath, mode, testConfig.BlockSize, currentWriteSpeedMBps, currentReadSpeedMBps, debug, mainPID)
            default:
                if time.Since(lastStatsUpdate) > time.Duration(statsInterval*2)*time.Second {
                    lastStatsUpdate = time.Now()
                    updatePerfStats(perfStats, devicePath, mode, testConfig.BlockSize, currentWriteSpeedMBps, currentReadSpeedMBps, debug, mainPID)
                }
            }

            // 短暫休息，避免過度消耗 CPU
            time.Sleep(100 * time.Millisecond)
        }
    }
}

// 優化的寫入函數 - 重用 buffer
func performRawDiskWriteOptimized(mainPID int, devicePath string, data []byte, mode string, blockSize int64, startOffset int64, seed int64, blockBuffer []byte, debug bool) error {
    if len(data) == 0 {
        return fmt.Errorf("attempt to write empty data to device %s", devicePath)
    }

    device, err := os.OpenFile(devicePath, os.O_WRONLY|syscall.O_DIRECT, 0)
    if err != nil {
        return fmt.Errorf("failed to open device for writing (%s): %w", devicePath, err)
    }
    defer device.Close()

    totalSize := int64(len(data))

    if mode == "sequential" {
        // 直接寫入，不需要額外的 buffer
        n, writeErr := device.WriteAt(data, startOffset)
        if writeErr != nil {
            return fmt.Errorf("sequential write error: %w", writeErr)
        }
        if int64(n) != totalSize {
            return fmt.Errorf("sequential write incomplete: wrote %d, expected %d", n, totalSize)
        }
    } else {
        // 隨機模式使用提供的 blockBuffer
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

            // 清空並複製數據到 aligned buffer
            for i := range blockBuffer[:chunkSize] {
                blockBuffer[i] = 0
            }
            copy(blockBuffer[:chunkSize], data[dataStart:dataEnd])

            deviceOffset := startOffset + dataStart
            n, writeErr := device.WriteAt(blockBuffer[:chunkSize], deviceOffset)
            if writeErr != nil {
                return fmt.Errorf("random write error at offset %d: %w", deviceOffset, writeErr)
            }
            if int64(n) != chunkSize {
                return fmt.Errorf("random write incomplete at offset %d: wrote %d, expected %d", deviceOffset, n, chunkSize)
            }
        }
    }

    return device.Sync()
}

// 優化的讀取驗證函數 - 重用 buffer
func performRawDiskReadAndVerifyOptimized(mainPID int, devicePath string, originalData []byte, mode string, blockSize int64, startOffset int64, seed int64, blockBuffer []byte, debug bool) error {
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
        // 為 sequential 模式創建一個對齊的 buffer
        rawBuffer, readBuffer := alignedBuffer(expectedSize)
        _ = rawBuffer
        defer func() {
            // 明確釋放大 buffer
            rawBuffer = nil
            readBuffer = nil
        }()

        n, readErr := device.ReadAt(readBuffer[:expectedSize], startOffset)
        if readErr != nil && readErr != io.EOF {
            return fmt.Errorf("sequential read error: %w", readErr)
        }
        if int64(n) != expectedSize {
            return fmt.Errorf("sequential read incomplete: read %d, expected %d", n, expectedSize)
        }

        if !bytes.Equal(originalData, readBuffer[:expectedSize]) {
            return fmt.Errorf("data verification failed for sequential read")
        }
    } else {
        // 隨機模式使用提供的 blockBuffer
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

            // 清空 buffer
            for i := range blockBuffer[:chunkSize] {
                blockBuffer[i] = 0
            }

            deviceOffset := startOffset + dataStart
            n, readErr := device.ReadAt(blockBuffer[:chunkSize], deviceOffset)
            if readErr != nil && readErr != io.EOF {
                return fmt.Errorf("random read error at offset %d: %w", deviceOffset, readErr)
            }
            if int64(n) != chunkSize {
                return fmt.Errorf("random read incomplete at offset %d: read %d, expected %d", deviceOffset, n, chunkSize)
            }

            if !bytes.Equal(originalData[dataStart:dataEnd], blockBuffer[:chunkSize]) {
                return fmt.Errorf("data verification failed for block %d at offset %d", blockIdx, deviceOffset)
            }
        }
    }

    return nil
}

// 其他輔助函數保持不變...
func syncDevice(devicePath string) error {
    device, err := os.OpenFile(devicePath, os.O_WRONLY, 0)
    if err != nil {
        return fmt.Errorf("failed to open device for sync (%s): %w", devicePath, err)
    }
    defer device.Close()

    if err := device.Sync(); err != nil {
        return fmt.Errorf("failed to sync device (%s): %w", devicePath, err)
    }
    return nil
}

func getDeviceSize(fd uintptr) (int64, error) {
    const BLKGETSIZE64 = 0x80081272
    var size int64
    _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, BLKGETSIZE64, uintptr(unsafe.Pointer(&size)))
    if errno != 0 {
        return 0, errno
    }
    return size, nil
}

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

func updatePerfStats(perfStats *config.PerformanceStats, devicePath, mode string, blockSize int64, writeSpeedMBps, readSpeedMBps float64, debug bool, mainPID int) {
    perfStats.Lock()
    defer perfStats.Unlock()

    diskPerfKey := fmt.Sprintf("raw:%s|%s|%d", devicePath, mode, blockSize)
    for i, rdp := range perfStats.RawDisk {
        existingKey := fmt.Sprintf("raw:%s|%s|%d", rdp.DevicePath, rdp.Mode, rdp.BlockSize)
        if existingKey == diskPerfKey {
            if readSpeedMBps > rdp.ReadSpeed {
                perfStats.RawDisk[i].ReadSpeed = readSpeedMBps
            }
            if writeSpeedMBps > rdp.WriteSpeed {
                perfStats.RawDisk[i].WriteSpeed = writeSpeedMBps
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
}
