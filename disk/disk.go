package disk

import (
    "context"
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

// Small buffer pool - used for block-level operations
var smallBufferPool = sync.Pool{
    New: func() interface{} {
        return make([]byte, 4*1024) // 4KB buffer
    },
}

// Predictable data generator - generates data based on position, no memory storage needed
type predictableDataGenerator struct {
    seed int64
}

func newPredictableDataGenerator() *predictableDataGenerator {
    return &predictableDataGenerator{
        seed: time.Now().UnixNano(),
    }
}

// Generate predictable data based on file position
func (pdg *predictableDataGenerator) generateBlock(offset int64, size int) []byte {
    block := make([]byte, size)
    localSeed := pdg.seed + offset
    rng := rand.New(rand.NewSource(localSeed))
    for i := range block {
        block[i] = byte(rng.Intn(256))
    }
    return block
}

// Verify block data matches expected
func (pdg *predictableDataGenerator) verifyBlock(offset int64, data []byte) bool {
    expected := pdg.generateBlock(offset, len(data))
    for i, b := range data {
        if b != expected[i] {
            return false
        }
    }
    return true
}

// RunDiskStressTest - supports multiple files and dual I/O modes (direct and async)
func RunDiskStressTest(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, testConfig DiskTestConfig, perfStats *config.PerformanceStats, debug bool, duration string) {
    defer wg.Done()

    // Generate main PID
    mainPID := int(time.Now().UnixNano()%1000000 + 1000)

    // Create context bound to stop channel
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Monitor stop channel
    go func() {
        <-stop
        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] received stop signal, initiating shutdown", mainPID), false)
        cancel()
    }()

    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] started on %d mount points", mainPID, len(testConfig.MountPoints)), false)

    // Validate and set defaults
    if len(testConfig.MountPoints) == 0 {
        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] no mount points specified, using current directory '.'", mainPID), false)
        testConfig.MountPoints = []string{"."}
    }
    if testConfig.FileSize <= 0 {
        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] invalid FileSize %d, using default 10MB", mainPID, testConfig.FileSize), false)
        testConfig.FileSize = 10 * 1024 * 1024
    }
    if testConfig.BlockSize <= 0 {
        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] invalid BlockSize %d, using default 4KB", mainPID, testConfig.BlockSize), false)
        testConfig.BlockSize = 4 * 1024
    }
    if testConfig.NumFiles <= 0 {
        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] invalid NumFiles %d, using default 4", mainPID, testConfig.NumFiles), false)
        testConfig.NumFiles = 4
    }

    if debug {
        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] configuration: FileSize=%d bytes, BlockSize=%d bytes, NumFiles=%d", mainPID, testConfig.FileSize, testConfig.BlockSize, testConfig.NumFiles), true)
    }

    // Define I/O modes (fixed to both)
    ioModes := []string{"direct", "async"}

    // Check total space requirements for all mount points
    var testModes []string
    if testConfig.TestMode == "both" {
        testModes = []string{"sequential", "random"}
    } else if testConfig.TestMode == "sequential" || testConfig.TestMode == "random" {
        testModes = []string{testConfig.TestMode}
    } else {
        errorMsg := fmt.Sprintf("Disk test [PID: %d] invalid test mode: %s", mainPID, testConfig.TestMode)
        errorChan <- fmt.Sprintf("Invalid test mode: %s", testConfig.TestMode)
        utils.LogMessage(errorMsg, true)
        return
    }
    totalRequiredSpace := uint64(testConfig.FileSize) * uint64(testConfig.NumFiles) * uint64(len(testModes)) * uint64(len(ioModes))
    for _, mp := range testConfig.MountPoints {
        var stat syscall.Statfs_t
        if err := syscall.Statfs(mp, &stat); err != nil {
            errorMsg := fmt.Sprintf("Disk test [PID: %d] cannot stat mount point %s: %v", mainPID, mp, err)
            errorChan <- fmt.Sprintf("Cannot stat mount point %s: %v", mp, err)
            utils.LogMessage(errorMsg, true)
            return
        }
        availableBytes := stat.Bavail * uint64(stat.Bsize)
        if availableBytes < totalRequiredSpace {
            errorMsg := fmt.Sprintf("Disk test [PID: %d] insufficient disk space on %s: need %d bytes, available %d bytes", mainPID, mp, totalRequiredSpace, availableBytes)
            utils.LogMessage(errorMsg, true)
            for {
                select {
                case <-ctx.Done():
                    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] received stop signal, exiting due to insufficient space on %s", mainPID, mp), false)
                    return
                default:
                    errorChan <- fmt.Sprintf("Insufficient disk space on %s: need %d bytes, available %d bytes", mp, totalRequiredSpace, availableBytes)
                    time.Sleep(1 * time.Second)
                }
            }
        }
        if debug {
            utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] mount point %s has %d bytes available, need %d bytes", mainPID, mp, availableBytes, totalRequiredSpace), true)
        }
    }

    // Calculate GC interval
    dur, err := time.ParseDuration(duration)
    if err != nil {
        errorMsg := fmt.Sprintf("Disk test [PID: %d] invalid duration format: %v", mainPID, err)
        errorChan <- fmt.Sprintf("Invalid duration format: %v", err)
        utils.LogMessage(errorMsg, true)
        return
    }
    durSeconds := int64(dur.Seconds())
    var gcInterval int64
    if durSeconds <= 60 {
        gcInterval = 10
    } else {
        gcInterval = durSeconds / 60
    }
    ticker := time.NewTicker(time.Duration(gcInterval) * time.Second)
    defer ticker.Stop()

    if debug {
        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] GC interval set to %d seconds", mainPID, gcInterval), true)
    }

    // Limit concurrent goroutines
    maxConcurrent := runtime.NumCPU() * 2
    if len(testConfig.MountPoints)*len(testModes)*len(ioModes)*testConfig.NumFiles < maxConcurrent {
        maxConcurrent = len(testConfig.MountPoints) * len(testModes) * len(ioModes) * testConfig.NumFiles
    }
    semaphore := make(chan struct{}, maxConcurrent)

    if debug {
        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] max concurrent goroutines set to %d", mainPID, maxConcurrent), true)
    }

    // Store all test files for cleanup
    var fileListMu sync.Mutex
    fileList := make(map[string]struct{})

    // Initialize performance stats for each mount point, mode, and ioMode
    var perfWg sync.WaitGroup
    perfWg.Add(1)
    go func() {
        defer perfWg.Done()
        for _, mp := range testConfig.MountPoints {
            for _, mode := range testModes {
                for _, ioMode := range ioModes {
                    perfStats.Lock()
                    diskPerfKey := fmt.Sprintf("%s|%s|%s|%d", mp, mode, ioMode, testConfig.BlockSize)
                    found := false
                    for _, dp := range perfStats.Disk {
                        existingKey := fmt.Sprintf("%s|%s|%s|%d", dp.MountPoint, dp.Mode, dp.IOMode, dp.BlockSize)
                        if existingKey == diskPerfKey {
                            found = true
                            break
                        }
                    }
                    if !found {
                        perfStats.Disk = append(perfStats.Disk, config.DiskPerformance{
                            MountPoint: mp,
                            Mode:       mode,
                            IOMode:     ioMode,
                            BlockSize:  testConfig.BlockSize,
                        })
                        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] added performance entry for %s (mode: %s, io: %s, blockSize: %d)", mainPID, mp, mode, ioMode, testConfig.BlockSize), false)
                    }
                    perfStats.Unlock()
                }
            }
        }
    }()

    // Cleanup goroutine
    cleanupWg := &sync.WaitGroup{}
    cleanupWg.Add(1)
    go func() {
        defer cleanupWg.Done()
        <-ctx.Done()
        fileListMu.Lock()
        for filePath := range fileList {
            if err := os.Remove(filePath); err != nil {
                if !os.IsNotExist(err) {
                    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] failed to delete file %s: %v", mainPID, filePath, err), true)
                } else if debug {
                    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] file %s already deleted during cleanup", mainPID, filePath), true)
                }
            } else if debug {
                utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] deleted file %s during cleanup", mainPID, filePath), true)
            }
            delete(fileList, filePath)
        }
        fileListMu.Unlock()
        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] all test files cleaned up", mainPID), false)
    }()

    mountWg := &sync.WaitGroup{}
    for i, mountPoint := range testConfig.MountPoints {
        mountWg.Add(1)
        go func(mp string, index int) {
            defer mountWg.Done()

            // Validate mount point
            if info, err := os.Stat(mp); err != nil {
                errorMsg := fmt.Sprintf("Disk test [PID: %d] mount point %s not accessible: %v", mainPID, mp, err)
                errorChan <- fmt.Sprintf("Cannot stat mount point %s: %v", mp, err)
                utils.LogMessage(errorMsg, true)
                return
            } else if !info.IsDir() {
                errorMsg := fmt.Sprintf("Disk test [PID: %d] mount point %s is not a directory", mainPID, mp)
                errorChan <- fmt.Sprintf("Mount point %s is not a directory", mp)
                utils.LogMessage(errorMsg, true)
                return
            }

            // Check write permissions
            tempFilePath := filepath.Join(mp, fmt.Sprintf(".writetest_%d", time.Now().UnixNano()))
            tempFile, err := os.Create(tempFilePath)
            if err != nil {
                errorMsg := fmt.Sprintf("Disk test [PID: %d] mount point %s is not writable: %v", mainPID, mp, err)
                errorChan <- fmt.Sprintf("Mount point %s is not writable", mp)
                utils.LogMessage(errorMsg, true)
                return
            }
            tempFile.Close()
            if err := os.Remove(tempFilePath); err != nil && debug {
                utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] failed to delete temp file %s: %v", mainPID, tempFilePath, err), true)
            }

            utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] starting test on mount point %s", mainPID, mp), false)

            modeWg := &sync.WaitGroup{}
            for j, mode := range testModes {
                for k, ioMode := range ioModes {
                    modeWg.Add(1)
                    subPID := mainPID + (index*100+j*10+k+1)*100
                    go func(currentMode, currentIOMode string, pid int) {
                        defer modeWg.Done()

                        // Acquire semaphore
                        semaphore <- struct{}{}
                        defer func() { <-semaphore }()

                        // Create file list
                        filePaths := make([]string, testConfig.NumFiles)
                        for m := 0; m < testConfig.NumFiles; m++ {
                            filePaths[m] = filepath.Join(mp, fmt.Sprintf("stress_test_%s_%s_%d_%d.dat", currentMode, currentIOMode, rand.Intn(10000), m))
                            fileListMu.Lock()
                            fileList[filePaths[m]] = struct{}{}
                            fileListMu.Unlock()
                            if debug {
                                utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] created test file path %s for %s (mode: %s, io: %s)", mainPID, filePaths[m], mp, currentMode, currentIOMode), true)
                            }
                        }

                        if len(filePaths) == 0 {
                            errorMsg := fmt.Sprintf("Disk test [PID: %d] no file paths generated for %s (mode: %s, io: %s, numFiles: %d)", mainPID, mp, currentMode, currentIOMode, testConfig.NumFiles)
                            errorChan <- errorMsg
                            utils.LogMessage(errorMsg, true)
                            return
                        }

                        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] starting %s disk test for %s (mode: %s, %d files, file size: %d bytes)", mainPID, currentIOMode, mp, currentMode, len(filePaths), testConfig.FileSize), false)

                        // Create data generator
                        dataGen := newPredictableDataGenerator()

                        iteration := 0
                        for {
                            select {
                            case <-ctx.Done():
                                // Clean up file list
                                for _, filePath := range filePaths {
                                    fileListMu.Lock()
                                    delete(fileList, filePath)
                                    fileListMu.Unlock()
                                    if debug {
                                        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] removed file %s from cleanup list on shutdown (mode: %s, io: %s)", mainPID, filePath, currentMode, currentIOMode), true)
                                    }
                                }
                                utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] stopped for %s (mode: %s, io: %s, iteration: %d)", mainPID, mp, currentMode, currentIOMode, iteration), false)
                                return
                            default:
                                iteration++

                                var operationErr error

                                if currentIOMode == "async" {
                                    select {
                                    case <-ctx.Done():
                                        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] async operation canceled before starting for %s (mode: %s, io: %s, iteration: %d)", mainPID, mp, currentMode, currentIOMode, iteration), false)
                                        return
                                    default:
                                        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] starting async operations for %d files in %s (mode: %s, iteration: %d)", mainPID, len(filePaths), mp, currentMode, iteration), false)
                                        operationErr = performAsyncDiskOperations(ctx, mainPID, filePaths, dataGen, currentMode, testConfig.FileSize, testConfig.BlockSize, debug, errorChan)
                                    }
                                } else {
                                    // Direct I/O
                                    for _, filePath := range filePaths {
                                        select {
                                        case <-ctx.Done():
                                            utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] direct operation canceled for %s (mode: %s, io: %s, file: %s, iteration: %d)", mainPID, mp, currentMode, currentIOMode, filePath, iteration), false)
                                            return
                                        default:
                                            // Perform direct disk write
                                            utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] starting direct write to %s (mode: %s, io: %s, size: %d bytes, iteration: %d)", mainPID, filePath, currentMode, currentIOMode, testConfig.FileSize, iteration), false)
                                            writeErr := performDirectDiskWrite(ctx, mainPID, filePath, dataGen, currentMode, testConfig.FileSize, testConfig.BlockSize, debug)
                                            if writeErr != nil {
                                                if writeErr == ctx.Err() {
                                                    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] write canceled for %s (mode: %s, io: %s, file: %s, iteration: %d)", mainPID, mp, currentMode, currentIOMode, filePath, iteration), false)
                                                    return
                                                }
                                                errorMsg := fmt.Sprintf("Disk test [PID: %d] write error on %s (mode: %s, io: %s, file: %s, iteration: %d): %v", mainPID, mp, currentMode, currentIOMode, filePath, iteration, writeErr)
                                                errorChan <- fmt.Sprintf("Disk write error on %s (mode: %s, io: %s)", mp, currentMode, currentIOMode)
                                                utils.LogMessage(errorMsg, true)
                                                operationErr = writeErr
                                                break
                                            }
                                            utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] write completed successfully for %s (mode: %s, io: %s, file: %s, iteration: %d)", mainPID, filePath, currentMode, currentIOMode, filePath, iteration), false)

                                            // Perform direct disk read and verify
                                            utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] starting direct read/verify for %s (mode: %s, io: %s, size: %d bytes, iteration: %d)", mainPID, filePath, currentMode, currentIOMode, testConfig.FileSize, iteration), false)
                                            readErr := performDirectDiskReadAndVerify(ctx, mainPID, filePath, dataGen, currentMode, testConfig.FileSize, testConfig.BlockSize, debug)
                                            if readErr != nil && readErr != ctx.Err() {
                                                errorMsg := fmt.Sprintf("Disk test [PID: %d] read/verify error on %s (mode: %s, io: %s, file: %s, iteration: %d): %v", mainPID, mp, currentMode, currentIOMode, filePath, iteration, readErr)
                                                errorChan <- fmt.Sprintf("Disk read/verify error on %s (mode: %s, io: %s)", mp, currentMode, currentIOMode)
                                                utils.LogMessage(errorMsg, true)
                                                operationErr = readErr
                                                break
                                            }
                                            if readErr == nil {
                                                utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] read/verify completed successfully for %s (mode: %s, io: %s, file: %s, iteration: %d)", mainPID, filePath, currentMode, currentIOMode, filePath, iteration), false)
                                            }
                                        }
                                    }
                                }

                                if operationErr != nil {
                                    time.Sleep(2 * time.Second)
                                    continue
                                }

                                time.Sleep(10 * time.Millisecond)

                                // Trigger GC periodically
                                select {
                                case <-ticker.C:
                                    if iteration%50 == 0 {
                                        if debug {
                                            utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] triggering GC at iteration %d for %s (mode: %s, io: %s)", mainPID, iteration, mp, currentMode, currentIOMode), true)
                                        }
                                        runtime.GC()
                                    }
                                default:
                                }
                            }
                        }
                    }(mode, ioMode, subPID)
                }
            }
            modeWg.Wait()
            utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] completed all modes for mount point %s", mainPID, mp), false)
        }(mountPoint, i)
    }
    mountWg.Wait()

    // Wait for cleanup and performance stats completion
    cancel()
    perfWg.Wait()
    cleanupWg.Wait()
    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] completed all operations and cleanup", mainPID), false)
}

// performDirectDiskWrite writes directly to disk without large memory buffers
func performDirectDiskWrite(ctx context.Context, mainPID int, filePath string, dataGen *predictableDataGenerator, mode string, fileSize, blockSize int64, debug bool) error {
    if _, statErr := os.Stat(filePath); statErr == nil {
        if debug {
            utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] removing existing file %s before write (mode: %s)", mainPID, filePath, mode), true)
        }
        if rmErr := os.Remove(filePath); rmErr != nil {
            return fmt.Errorf("failed to remove existing file %s: %w", filePath, rmErr)
        }
    }

    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] creating file %s for write (mode: %s, size: %d bytes, block size: %d bytes)", mainPID, filePath, mode, fileSize, blockSize), false)
    file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
    if err != nil {
        return fmt.Errorf("failed to create file %s: %w", filePath, err)
    }
    defer file.Close()

    buffer := smallBufferPool.Get().([]byte)
    defer smallBufferPool.Put(buffer)

    totalBlocks := fileSize / blockSize
    if fileSize%blockSize != 0 {
        totalBlocks++
    }

    if mode == "sequential" {
        for blockIdx := int64(0); blockIdx < totalBlocks; blockIdx++ {
            select {
            case <-ctx.Done():
                utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] write canceled for %s at block %d (mode: %s)", mainPID, filePath, blockIdx, mode), false)
                return ctx.Err()
            default:
                offset := blockIdx * blockSize
                currentBlockSize := blockSize
                if offset+currentBlockSize > fileSize {
                    currentBlockSize = fileSize - offset
                }

                if debug && blockIdx%100 == 0 {
                    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] writing block %d/%d to %s at offset %d (mode: %s, block size: %d bytes)", mainPID, blockIdx, totalBlocks, filePath, offset, mode, currentBlockSize), true)
                }

                for bytesWritten := int64(0); bytesWritten < currentBlockSize; {
                    chunkSize := int64(len(buffer))
                    if bytesWritten+chunkSize > currentBlockSize {
                        chunkSize = currentBlockSize - bytesWritten
                    }

                    chunkData := dataGen.generateBlock(offset+bytesWritten, int(chunkSize))
                    n, writeErr := file.Write(chunkData)
                    if writeErr != nil {
                        return fmt.Errorf("write error at offset %d in %s: %w", offset+bytesWritten, filePath, writeErr)
                    }
                    if int64(n) != chunkSize {
                        return fmt.Errorf("short write at offset %d in %s: wrote %d, expected %d", offset+bytesWritten, filePath, n, chunkSize)
                    }
                    bytesWritten += int64(n)
                }
            }
        }
    } else {
        if err := file.Truncate(fileSize); err != nil {
            return fmt.Errorf("failed to truncate file %s to %d bytes: %w", filePath, fileSize, err)
        }
        if debug {
            utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] truncated file %s to %d bytes for random write", mainPID, filePath, fileSize), true)
        }

        blockOrder := make([]int64, totalBlocks)
        for i := int64(0); i < totalBlocks; i++ {
            blockOrder[i] = i
        }
        rng := rand.New(rand.NewSource(time.Now().UnixNano()))
        rng.Shuffle(int(totalBlocks), func(i, j int) {
            blockOrder[i], blockOrder[j] = blockOrder[j], blockOrder[i]
        })

        for blockNum, blockIdx := range blockOrder {
            select {
            case <-ctx.Done():
                utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] random write canceled for %s at block %d (mode: %s)", mainPID, filePath, blockIdx, mode), false)
                return ctx.Err()
            default:
                offset := blockIdx * blockSize
                currentBlockSize := blockSize
                if offset+currentBlockSize > fileSize {
                    currentBlockSize = fileSize - offset
                }

                if debug && blockNum%100 == 0 {
                    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] writing random block %d/%d to %s at offset %d (mode: %s, block size: %d bytes)", mainPID, blockNum, totalBlocks, filePath, offset, mode, currentBlockSize), true)
                }

                for bytesWritten := int64(0); bytesWritten < currentBlockSize; {
                    chunkSize := int64(len(buffer))
                    if bytesWritten+chunkSize > currentBlockSize {
                        chunkSize = currentBlockSize - bytesWritten
                    }

                    chunkData := dataGen.generateBlock(offset+bytesWritten, int(chunkSize))
                    n, writeErr := file.WriteAt(chunkData, offset+bytesWritten)
                    if writeErr != nil {
                        return fmt.Errorf("write error at offset %d in %s: %v", offset+bytesWritten, filePath, writeErr)
                    }
                    if int64(n) != chunkSize {
                        return fmt.Errorf("short write at offset %d in %s: wrote %d, expected %d", offset+bytesWritten, filePath, n, chunkSize)
                    }
                    bytesWritten += int64(n)
                }
            }
        }
    }

    if debug {
        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] syncing file %s after write (mode: %s)", mainPID, filePath, mode), true)
    }
    if err := file.Sync(); err != nil {
        return fmt.Errorf("failed to sync file %s: %w", filePath, err)
    }

    return nil
}

// performDirectDiskReadAndVerify reads and verifies directly from disk
func performDirectDiskReadAndVerify(ctx context.Context, mainPID int, filePath string, dataGen *predictableDataGenerator, mode string, fileSize, blockSize int64, debug bool) error {
    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] checking file %s for read/verify (mode: %s, size: %d bytes, block size: %d bytes)", mainPID, filePath, mode, fileSize, blockSize), false)
    fileInfo, err := os.Stat(filePath)
    if err != nil {
        return fmt.Errorf("file %s not found: %w", filePath, err)
    }

    if fileInfo.Size() != fileSize {
        return fmt.Errorf("file size mismatch for %s: expected %d, got %d", filePath, fileSize, fileInfo.Size())
    }

    file, err := os.Open(filePath)
    if err != nil {
        return fmt.Errorf("failed to open file %s: %w", filePath, err)
    }
    defer file.Close()

    buffer := smallBufferPool.Get().([]byte)
    defer smallBufferPool.Put(buffer)

    totalBlocks := fileSize / blockSize
    if fileSize%blockSize != 0 {
        totalBlocks++
    }

    if mode == "sequential" {
        for blockIdx := int64(0); blockIdx < totalBlocks; blockIdx++ {
            select {
            case <-ctx.Done():
                utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] read canceled for %s at block %d (mode: %s)", mainPID, filePath, blockIdx, mode), false)
                return ctx.Err()
            default:
                offset := blockIdx * blockSize
                currentBlockSize := blockSize
                if offset+currentBlockSize > fileSize {
                    currentBlockSize = fileSize - offset
                }

                if debug && blockIdx%100 == 0 {
                    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] reading block %d/%d from %s at offset %d (mode: %s, block size: %d bytes)", mainPID, blockIdx, totalBlocks, filePath, offset, mode, currentBlockSize), true)
                }

                for bytesRead := int64(0); bytesRead < currentBlockSize; {
                    chunkSize := int64(len(buffer))
                    if bytesRead+chunkSize > currentBlockSize {
                        chunkSize = currentBlockSize - bytesRead
                    }

                    n, readErr := file.Read(buffer[:chunkSize])
                    if readErr != nil && readErr != io.EOF {
                        return fmt.Errorf("read error at offset %d in %s: %w", offset+bytesRead, filePath, readErr)
                    }
                    if int64(n) != chunkSize && readErr != io.EOF {
                        return fmt.Errorf("short read at offset %d in %s: read %d, expected %d", offset+bytesRead, filePath, n, chunkSize)
                    }

                    if !dataGen.verifyBlock(offset+bytesRead, buffer[:n]) {
                        return fmt.Errorf("data verification failed at offset %d in %s", offset+bytesRead, filePath)
                    }
                    bytesRead += int64(n)
                }
            }
        }
    } else {
        blockOrder := make([]int64, totalBlocks)
        for i := int64(0); i < totalBlocks; i++ {
            blockOrder[i] = i
        }
        rng := rand.New(rand.NewSource(time.Now().UnixNano() + 1))
        rng.Shuffle(int(totalBlocks), func(i, j int) {
            blockOrder[i], blockOrder[j] = blockOrder[j], blockOrder[i]
        })

        for blockNum, blockIdx := range blockOrder {
            select {
            case <-ctx.Done():
                utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] random read canceled for %s at block %d (mode: %s)", mainPID, filePath, blockIdx, mode), false)
                return ctx.Err()
            default:
                offset := blockIdx * blockSize
                currentBlockSize := blockSize
                if offset+currentBlockSize > fileSize {
                    currentBlockSize = fileSize - offset
                }

                if debug && blockNum%100 == 0 {
                    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] reading random block %d/%d from %s at offset %d (mode: %s, block size: %d bytes)", mainPID, blockNum, totalBlocks, filePath, offset, mode, currentBlockSize), true)
                }

                for bytesRead := int64(0); bytesRead < currentBlockSize; {
                    chunkSize := int64(len(buffer))
                    if bytesRead+chunkSize > currentBlockSize {
                        chunkSize = currentBlockSize - bytesRead
                    }

                    n, readErr := file.ReadAt(buffer[:chunkSize], offset+bytesRead)
                    if readErr != nil && readErr != io.EOF {
                        return fmt.Errorf("read error at offset %d in %s: %w", offset+bytesRead, filePath, readErr)
                    }
                    if int64(n) != chunkSize && readErr != io.EOF {
                        return fmt.Errorf("short read at offset %d in %s: read %d, expected %d", offset+bytesRead, filePath, n, chunkSize)
                    }

                    if !dataGen.verifyBlock(offset+bytesRead, buffer[:n]) {
                        return fmt.Errorf("data verification failed at offset %d in %s", offset+bytesRead, filePath)
                    }
                    bytesRead += int64(n)
                }
            }
        }
    }

    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] read/verify completed successfully for %s (mode: %s)", mainPID, filePath, mode), false)
    return nil
}

// performAsyncDiskOperations performs async disk operations
func performAsyncDiskOperations(ctx context.Context, mainPID int, filePaths []string, dataGen *predictableDataGenerator, mode string, fileSize, blockSize int64, debug bool, errorChan chan string) error {
    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] starting async operations for %d files (mode: %s, file size: %d bytes, block size: %d bytes)", mainPID, len(filePaths), mode, fileSize, blockSize), false)

    var wg sync.WaitGroup
    operationChan := make(chan struct{}, len(filePaths)) // Limit concurrency
    errorList := make(chan error, len(filePaths))

    for _, path := range filePaths {
        select {
        case <-ctx.Done():
            utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] async operations canceled before starting file %s (mode: %s)", mainPID, path, mode), false)
            return ctx.Err()
        default:
            operationChan <- struct{}{} // Acquire semaphore
            wg.Add(1)
            go func(filePath string) {
                defer wg.Done()
                defer func() { <-operationChan }()

                if debug {
                    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] async operation file: %s (mode: %s)", mainPID, filePath, mode), true)
                }

                // Write operation
                writeErr := performDirectDiskWrite(ctx, mainPID, filePath, dataGen, mode, fileSize, blockSize, debug)
                if writeErr != nil {
                    if writeErr != ctx.Err() {
                        errorMsg := fmt.Sprintf("Disk test [PID: %d] async write error on %s (mode: %s): %v", mainPID, filePath, mode, writeErr)
                        errorChan <- fmt.Sprintf("Disk async write error on %s (mode: %s)", filePath, mode)
                        utils.LogMessage(errorMsg, true)
                        errorList <- writeErr
                    } else {
                        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] async write canceled for %s (mode: %s)", mainPID, filePath, mode), false)
                    }
                    return
                }
                utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] async write completed successfully for %s (mode: %s)", mainPID, filePath, mode), false)

                // Read and verify operation
                readErr := performDirectDiskReadAndVerify(ctx, mainPID, filePath, dataGen, mode, fileSize, blockSize, debug)
                if readErr != nil {
                    if readErr != ctx.Err() {
                        errorMsg := fmt.Sprintf("Disk test [PID: %d] async read/verify error on %s (mode: %s): %v", mainPID, filePath, mode, readErr)
                        errorChan <- fmt.Sprintf("Disk async read/verify error on %s (mode: %s)", filePath, mode)
                        utils.LogMessage(errorMsg, true)
                        errorList <- readErr
                    } else {
                        utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] async read canceled for %s (mode: %s)", mainPID, filePath, mode), false)
                    }
                    return
                }
                utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] async read/verify completed successfully for %s (mode: %s)", mainPID, filePath, mode), false)
            }(path)
        }
    }

    // Wait for all operations to complete or cancel
    go func() {
        wg.Wait()
        close(errorList)
    }()

    // Collect errors
    for e := range errorList {
        if e != nil {
            return e // Return first non-nil error
        }
    }

    utils.LogMessage(fmt.Sprintf("Disk test [PID: %d] completed async operations for %d files (mode: %s)", mainPID, len(filePaths), mode), false)
    return nil
}
