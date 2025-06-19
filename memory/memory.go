package memory

import (
    "fmt"
    "math"
    "math/rand"
    "runtime"
    "stress/config"
    "stress/utils"
    "sync"
    "syscall"
    "time"
    "unsafe"
)

func LogMemoryAddress(arrIdx, elemIdx int, arrays [][]int64) string {
    if arrIdx < 0 || arrIdx >= len(arrays) || elemIdx < 0 || elemIdx >= len(arrays[arrIdx]) {
        return "0x0"
    }
    baseAddr := uintptr(unsafe.Pointer(&arrays[arrIdx][0]))
    if elemIdx > 0 && uintptr(elemIdx) > (^uintptr(0))/8 {
        return "0x0"
    }
    offset := uintptr(elemIdx) * 8
    if baseAddr > ^uintptr(0)-offset {
        return "0x0"
    }
    return fmt.Sprintf("0x%x", baseAddr+offset)
}

func RunMemoryStressTest(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, config MemoryConfig, perfStats_ *config.PerformanceStats) {
    defer wg.Done()

    // 生成主程序 PID（模擬）
    mainPID := rand.Intn(1000000) + 1000
    utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] started", mainPID), false)

    threadRNG := rand.New(rand.NewSource(time.Now().UnixNano()))
    var checkpointDataLock sync.Mutex
    checkpointData := make(map[int]map[int]int64)

    type ErrorRecord struct {
        Timestamp   time.Time
        ArrIdx      int
        ElemIdx     int
        Actual      int64
        Expected    int64
        Address     string
        RepairTried bool
    }
    var errorRecords []ErrorRecord
    var errorRecordsLock sync.Mutex
    var damagedLocations map[string]int
    var damagedLocationsLock sync.Mutex
    damagedLocations = make(map[string]int)

    totalMem, _ := getSystemMemory()
    var targetMemBytes uint64
    if config.UsagePercent < 0 {
        config.UsagePercent = 0
    } else if config.UsagePercent > 1.0 {
        config.UsagePercent = 1.0
    }
    if totalMem > math.MaxUint64/100 {
        targetMemBytes = totalMem / 100 * uint64(config.UsagePercent*100)
    } else {
        targetMemBytes = uint64(float64(totalMem) * config.UsagePercent)
    }
    utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] targeting %.2f%% of total system memory (%.2f GB)", mainPID, config.UsagePercent*100, float64(targetMemBytes)/(1024*1024*1024)), false)

    const speedTestSize = 50_000_000
    speedTestArray := make([]int64, speedTestSize)
    utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] measuring initial memory speeds...", mainPID), false)

    writeStart := time.Now()
    writeCount := uint64(0)
    validationErrors := 0
    for i := 0; i < speedTestSize; i++ {
        speedTestArray[i] = int64(i * 2)
        if writeCount < math.MaxUint64 {
            writeCount++
        }
    }
    writeDuration := time.Since(writeStart)
    bytesWritten := speedTestSize * 8
    writeSpeedMBps := float64(bytesWritten) / writeDuration.Seconds() / (1024 * 1024)

    var sum int64 = 0
    readCount := uint64(0)
    readStart := time.Now()
    for i := 0; i < speedTestSize; i++ {
        val := speedTestArray[i]
        expectedVal := int64(i * 2)
        if val != expectedVal {
            validationErrors++
            if validationErrors < 10 && config.Debug {
                utils.LogMessage(fmt.Sprintf("Data validation error at index %d: got %d, expected %d", i, val, expectedVal), true)
            }
        }
        sum += val
        if readCount < math.MaxUint64 {
            readCount++
        }
    }
    readDuration := time.Since(readStart)
    bytesRead := speedTestSize * 8
    readSpeedMBps := float64(bytesRead) / readDuration.Seconds() / (1024 * 1024)

    if validationErrors > 0 {
        utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] WARNING: Found %d validation errors during sequential read test!", mainPID, validationErrors), false)
        errorChan <- fmt.Sprintf("Memory data corruption detected: %d errors in sequential read test", validationErrors)
    } else if config.Debug {
        utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] sequential read validation: PASSED", mainPID), true)
    }

    randIndices := make([]int, 5_000_000)
    for i := range randIndices {
        randIndices[i] = threadRNG.Intn(speedTestSize)
    }
    modifications := make(map[int]int64)
    randomValidationErrors := 0
    randomCount := uint64(0)
    randomStart := time.Now()
    for _, idx := range randIndices {
        originalVal := speedTestArray[idx]
        expectedVal := int64(0)
        if modifiedVal, exists := modifications[idx]; exists {
            expectedVal = modifiedVal
            if originalVal != expectedVal {
                randomValidationErrors++
                if randomValidationErrors < 10 && config.Debug {
                    utils.LogMessage(fmt.Sprintf("Random access validation error at index %d: got %d, expected %d", idx, originalVal, expectedVal), true)
                }
            }
        } else {
            expectedVal = int64(idx * 2)
            if originalVal != expectedVal {
                randomValidationErrors++
                if randomValidationErrors < 10 && config.Debug {
                    utils.LogMessage(fmt.Sprintf("Initial pattern validation error at index %d: got %d, expected %d", idx, originalVal, expectedVal), true)
                }
            }
        }
        newVal := originalVal ^ 0x1
        speedTestArray[idx] = newVal
        modifications[idx] = newVal
        if randomCount < math.MaxUint64 {
            randomCount++
        }
    }
    randomDuration := time.Since(randomStart)
    randomBytes := int64(len(randIndices) * 16)
    randomSpeedMBps := float64(randomBytes) / randomDuration.Seconds() / (1024 * 1024)

    if randomValidationErrors > 0 {
        utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] WARNING: Found %d validation errors during random access test!", mainPID, randomValidationErrors), false)
        errorChan <- fmt.Sprintf("Memory data corruption detected: %d errors in random access test", randomValidationErrors)
    } else if config.Debug {
        utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] random access validation: PASSED", mainPID), true)
    }

    if config.Debug {
        utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] speed results: Sequential write: %.2f MB/s, operations: %d; Sequential read: %.2f MB/s, operations: %d; Random access: %.2f MB/s, operations: %d", mainPID, writeSpeedMBps, writeCount, readSpeedMBps, readCount, randomSpeedMBps, randomCount), true)
    }

    speedTestArray = nil
    modifications = nil
    runtime.GC()

    const arraySize = 10_000_000
    const bytesPerEntry = 8
    var arraysNeeded int
    totalBytesNeeded := arraySize * bytesPerEntry
    if totalBytesNeeded <= 0 || targetMemBytes < uint64(totalBytesNeeded) {
        arraysNeeded = 1
    } else {
        arraysNeededUint64 := targetMemBytes / uint64(totalBytesNeeded)
        if arraysNeededUint64 > uint64(math.MaxInt32) {
            arraysNeeded = math.MaxInt32
        } else {
            arraysNeeded = int(arraysNeededUint64)
        }
    }
    if arraysNeeded < 1 {
        arraysNeeded = 1
    }
    utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] starting allocation for %d arrays of %d elements each", mainPID, arraysNeeded, arraySize), false)

    var arrays [][]int64
    allocStart := time.Now()
    var bytesAllocated uint64 = 0
    seed := time.Now().UnixNano()
    arraySeedValues := make([]int64, 0, arraysNeeded)
    const checkpointInterval = 100_000

    for i := 0; i < arraysNeeded; i++ {
        select {
        case <-stop:
            utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] allocation interrupted by stop signal", mainPID), false)
            stopSubprocesses(mainPID, config.Debug)
            return
        default:
        }
        var arr []int64
        func() {
            defer func() {
                if r := recover(); r != nil {
                    utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] recovered from allocation panic: %v", mainPID, r), true)
                }
            }()
            if arraySize <= 0 || arraySize > math.MaxInt32/8 {
                utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] array size too large for safe allocation", mainPID), true)
                return
            }
            arr = make([]int64, arraySize)
            arraySeed := seed + int64(i*1000)
            arraySeedValues = append(arraySeedValues, arraySeed)
            arrayRNG := rand.New(rand.NewSource(arraySeed))
            for j := range arr {
                if j%checkpointInterval == 0 {
                    arr[j] = int64(j) ^ arraySeed
                    checkpointDataLock.Lock()
                    if checkpointData[i] == nil {
                        checkpointData[i] = make(map[int]int64)
                    }
                    checkpointData[i][j] = arr[j]
                    checkpointDataLock.Unlock()
                } else {
                    arr[j] = arrayRNG.Int63()
                }
            }
        }()
        if arr != nil {
            arrays = append(arrays, arr)
            if bytesAllocated <= math.MaxUint64-uint64(arraySize*bytesPerEntry) {
                bytesAllocated += uint64(arraySize * bytesPerEntry)
            }
            if (i+1)%10 == 0 {
                allocPercent := float64(bytesAllocated) * 100 / float64(totalMem)
                utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] allocation progress: %d/%d arrays (%.2f%% of system memory)", mainPID, i+1, arraysNeeded, allocPercent), false)
            }
        } else {
            utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] failed to allocate memory array, continuing with what we have", mainPID), true)
            break
        }
        if bytesAllocated >= targetMemBytes {
            utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] reached target memory allocation", mainPID), false)
            break
        }
        if i%100 == 0 {
            runtime.GC()
        }
    }

    allocDuration := time.Since(allocStart)
    allocSpeedMBps := float64(bytesAllocated) / allocDuration.Seconds() / (1024 * 1024)
    memoryUsagePercent := float64(bytesAllocated) * 100 / float64(totalMem)
    utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] allocated: %.2f GB out of %.2f GB total (%.2f%% of system memory), speed: %.2f MB/s", mainPID, float64(bytesAllocated)/(1024*1024*1024), float64(totalMem)/(1024*1024*1024), memoryUsagePercent, allocSpeedMBps), false)

    perfStats_.Lock()
    perfStats_.Memory.WriteSpeed = writeSpeedMBps
    perfStats_.Memory.ReadSpeed = readSpeedMBps
    perfStats_.Memory.RandomAccessSpeed = randomSpeedMBps
    perfStats_.Memory.UsagePercent = memoryUsagePercent
    perfStats_.Memory.WriteCount = writeCount
    perfStats_.Memory.ReadCount = readCount
    perfStats_.Memory.RandomAccessCount = randomCount
    perfStats_.Memory.MinReadSpeed = readSpeedMBps
    perfStats_.Memory.MaxReadSpeed = readSpeedMBps
    perfStats_.Memory.SumReadSpeed = readSpeedMBps
    perfStats_.Memory.ReadSpeedCount = 1
    perfStats_.Memory.MinWriteSpeed = writeSpeedMBps
    perfStats_.Memory.MaxWriteSpeed = writeSpeedMBps
    perfStats_.Memory.SumWriteSpeed = writeSpeedMBps
    perfStats_.Memory.WriteSpeedCount = 1
    perfStats_.Memory.MinRandomAccessSpeed = randomSpeedMBps
    perfStats_.Memory.MaxRandomAccessSpeed = randomSpeedMBps
    perfStats_.Memory.SumRandomAccessSpeed = randomSpeedMBps
    perfStats_.Memory.RandomAccessCount = 1
    perfStats_.Unlock()

    if memoryUsagePercent < config.UsagePercent*75.0 {
        errorChan <- fmt.Sprintf("Could only allocate %.2f%% of system memory, wanted %.2f%%", memoryUsagePercent, config.UsagePercent*100)
    }

    validationTicker := time.NewTicker(30 * time.Second)
    defer validationTicker.Stop()
    performanceUpdateTicker := time.NewTicker(5 * time.Second)
    defer performanceUpdateTicker.Stop()

    memValidationErrors := uint64(0)
    operationsSinceLastValidation := uint64(0)
    operationCount := uint64(0)
    lastPerformanceUpdate := time.Now()
    utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] starting stress test...", mainPID), false)

    for {
        select {
        case <-performanceUpdateTicker.C:
            now := time.Now()
            duration := now.Sub(lastPerformanceUpdate)
            if operationCount > 10000 {
                bytesProcessed := int64(operationCount * 16)
                currentSpeedMBps := float64(bytesProcessed) / duration.Seconds() / (1024 * 1024)
                const smallTestSize = 1_000_000
                testArray := make([]int64, smallTestSize)
                writeStart := time.Now()
                for i := 0; i < smallTestSize; i++ {
                    testArray[i] = int64(i)
                }
                writeDuration := time.Since(writeStart)
                bytesWritten := smallTestSize * 8
                writeSpeedMBps := float64(bytesWritten) / writeDuration.Seconds() / (1024 * 1024)
                readStart := time.Now()
                var sum int64
                for i := 0; i < smallTestSize; i++ {
                    sum += testArray[i]
                }
                readDuration := time.Since(readStart)
                bytesRead := smallTestSize * 8
                readSpeedMBps := float64(bytesRead) / readDuration.Seconds() / (1024 * 1024)
                perfStats_.Lock()
                perfStats_.Memory.RandomAccessSpeed = currentSpeedMBps
                perfStats_.Memory.ReadSpeed = readSpeedMBps
                perfStats_.Memory.WriteSpeed = writeSpeedMBps
                if currentSpeedMBps < perfStats_.Memory.MinRandomAccessSpeed || perfStats_.Memory.MinRandomAccessSpeed == 0 {
                    perfStats_.Memory.MinRandomAccessSpeed = currentSpeedMBps
                }
                if currentSpeedMBps > perfStats_.Memory.MaxRandomAccessSpeed {
                    perfStats_.Memory.MaxRandomAccessSpeed = currentSpeedMBps
                }
                perfStats_.Memory.SumRandomAccessSpeed += currentSpeedMBps
                if readSpeedMBps < perfStats_.Memory.MinReadSpeed || perfStats_.Memory.MinReadSpeed == 0 {
                    perfStats_.Memory.MinReadSpeed = readSpeedMBps
                }
                if readSpeedMBps > perfStats_.Memory.MaxReadSpeed {
                    perfStats_.Memory.MaxReadSpeed = readSpeedMBps
                }
                perfStats_.Memory.SumReadSpeed += readSpeedMBps
                if writeSpeedMBps < perfStats_.Memory.MinWriteSpeed || perfStats_.Memory.MinWriteSpeed == 0 {
                    perfStats_.Memory.MinWriteSpeed = writeSpeedMBps
                }
                if writeSpeedMBps > perfStats_.Memory.MaxWriteSpeed {
                    perfStats_.Memory.MaxWriteSpeed = writeSpeedMBps
                }
                perfStats_.Memory.SumWriteSpeed += writeSpeedMBps
                if perfStats_.Memory.RandomAccessCount < math.MaxInt64 {
                    perfStats_.Memory.RandomAccessCount++
                }
                if perfStats_.Memory.ReadSpeedCount < math.MaxInt64 {
                    perfStats_.Memory.ReadSpeedCount++
                }
                if perfStats_.Memory.WriteSpeedCount < math.MaxInt64 {
                    perfStats_.Memory.WriteSpeedCount++
                }
                perfStats_.Unlock()
                if config.Debug {
                    utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] performance update: Random Access Speed %.2f MB/s, Read Speed %.2f MB/s, Write Speed %.2f MB/s", mainPID, currentSpeedMBps, readSpeedMBps, writeSpeedMBps), true)
                }
                testArray = nil
                runtime.GC()
            }
            operationCount = 0
            lastPerformanceUpdate = now

        case <-validationTicker.C:
            validationStart := time.Now()
            localErrors := uint64(0)
            for arrIdx := range arrays {
                if arrIdx >= len(arraySeedValues) {
                    continue
                }
                checkpointDataLock.Lock()
                for elemIdx, expectedVal := range checkpointData[arrIdx] {
                    actualVal := arrays[arrIdx][elemIdx]
                    address := LogMemoryAddress(arrIdx, elemIdx, arrays)
                    damagedLocationsLock.Lock()
                    if damagedLocations[address] >= 3 {
                        damagedLocationsLock.Unlock()
                        continue
                    }
                    damagedLocationsLock.Unlock()
                    if actualVal != expectedVal {
                        if localErrors < math.MaxUint64 {
                            localErrors++
                        }
                        damagedLocationsLock.Lock()
                        if damagedLocations[address] < math.MaxInt32 {
                            damagedLocations[address]++
                        }
                        damagedLocationsLock.Unlock()
                        errorRecordsLock.Lock()
                        errorRecords = append(errorRecords, ErrorRecord{
                            Timestamp:   time.Now(),
                            ArrIdx:      arrIdx,
                            ElemIdx:     elemIdx,
                            Actual:      actualVal,
                            Expected:    expectedVal,
                            Address:     address,
                            RepairTried: false,
                        })
                        errorRecordsLock.Unlock()
                        arrays[arrIdx][elemIdx] = expectedVal
                        if config.Debug {
                            utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] checkpoint validation error at arr[%d][%d]: got %d, expected %d, address: %s, repair attempted", mainPID, arrIdx, elemIdx, actualVal, expectedVal, address), true)
                        }
                        if arrays[arrIdx][elemIdx] != expectedVal {
                            if config.Debug {
                                utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] repair failed at arr[%d][%d], address: %s", mainPID, arrIdx, elemIdx, address), true)
                            }
                        }
                    }
                }
                checkpointDataLock.Unlock()
            }
            if memValidationErrors <= math.MaxUint64-localErrors {
                memValidationErrors += localErrors
            }
            validationDuration := time.Since(validationStart)
            if localErrors > 0 {
                utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] validation found %d errors! Total errors so far: %d", mainPID, localErrors, memValidationErrors), false)
            } else if config.Debug {
                utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] validation PASSED (checked %d arrays in %.2f seconds)", mainPID, len(arrays), validationDuration.Seconds()), true)
            }
            operationsSinceLastValidation = 0

        case <-stop:
            utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] received stop signal", mainPID), false)
            stopSubprocesses(mainPID, config.Debug)
            if memValidationErrors > 0 {
                utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] total validation errors: %d", mainPID, memValidationErrors), false)
                if config.Debug {
                    errorRecordsLock.Lock()
                    utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] error records: %+v", mainPID, errorRecords), true)
                    errorRecordsLock.Unlock()
                    damagedLocationsLock.Lock()
                    utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] damaged locations (error count): %+v", mainPID, damagedLocations), true)
                    damagedLocationsLock.Unlock()
                }
            } else {
                utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] data validation: All checks PASSED", mainPID), false)
            }
            utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] stopped", mainPID), false)
            return

        default:
            for i := 0; i < 1000 && operationsSinceLastValidation < 5000; i++ {
                select {
                case <-stop:
                    utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] interrupted during operation", mainPID), false)
                    stopSubprocesses(mainPID, config.Debug)
                    return
                default:
                }
                if len(arrays) == 0 {
                    break
                }
                arrIdx := threadRNG.Intn(len(arrays))
                if arrIdx >= len(arrays) || len(arrays[arrIdx]) == 0 {
                    continue
                }
                elemIdx := threadRNG.Intn(len(arrays[arrIdx]))
                address := LogMemoryAddress(arrIdx, elemIdx, arrays)
                damagedLocationsLock.Lock()
                if damagedLocations[address] >= 3 {
                    damagedLocationsLock.Unlock()
                    continue
                }
                damagedLocationsLock.Unlock()
                val := arrays[arrIdx][elemIdx]
                newVal := val ^ 0xFF
                arrays[arrIdx][elemIdx] = newVal
                if elemIdx%checkpointInterval == 0 {
                    checkpointDataLock.Lock()
                    if checkpointData[arrIdx] == nil {
                        checkpointData[arrIdx] = make(map[int]int64)
                    }
                    checkpointData[arrIdx][elemIdx] = newVal
                    checkpointDataLock.Unlock()
                }
                perfStats_.Lock()
                if perfStats_.Memory.RandomAccessCount < math.MaxUint64 {
                    perfStats_.Memory.RandomAccessCount++
                }
                perfStats_.Unlock()
                if operationCount < math.MaxUint64 {
                    operationCount++
                }
                if operationsSinceLastValidation < math.MaxUint64 {
                    operationsSinceLastValidation++
                }
            }
            if threadRNG.Intn(10_000) == 0 {
                runtime.GC()
            }
            runtime.Gosched()
        }
    }
}

// stopSubprocesses 模擬停止子程序並記錄日誌
func stopSubprocesses(mainPID int, debug bool) {
    // 模擬三個子程序：分配、讀寫、驗證
    subprocesses := []string{"allocation", "readwrite", "validation"}
    for i, name := range subprocesses {
        subPID := mainPID + (i+1)*100 // 生成唯一子程序 PID
        utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] subprocess %s stopping...", mainPID, name, subPID), false)
        time.Sleep(100 * time.Millisecond) // 模擬停止延遲
        if debug {
            utils.LogMessage(fmt.Sprintf("Memory test [PID: %d] subprocess %s [PID: %d] stopped", mainPID, name, subPID), true)
        }
    }
}

func getSystemMemory() (total, free uint64) {
    var info syscall.Sysinfo_t
    err := syscall.Sysinfo(&info)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("Failed to get system memory info: %v", err), true)
    }
    total = info.Totalram
    free = info.Freeram
    return
}
