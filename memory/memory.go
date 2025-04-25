package memory

import (
    "fmt"
    "math/rand"
    "runtime"
    "stress/config"
    "stress/utils"
    "sync"
    "syscall"
    "time"
	"unsafe"
)

// RunMemoryStressTest runs the memory stress test
func LogMemoryAddress(arrIdx, elemIdx int, arrays [][]int64) string {
    baseAddr := uintptr(unsafe.Pointer(&arrays[arrIdx][0]))
    offset := uintptr(elemIdx) * 8
    return fmt.Sprintf("0x%x", baseAddr+offset)
}

func RunMemoryStressTest(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, config MemoryConfig, perfStats_ *config.PerformanceStats) {
    defer wg.Done()

    // 執行緒專屬隨機數生成器
    threadRNG := rand.New(rand.NewSource(time.Now().UnixNano()))

    // 保護 checkpointData 的鎖
    var checkpointDataLock sync.Mutex
    checkpointData := make(map[int]map[int]int64)

    // 錯誤記錄和損壞位置追蹤
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
    var damagedLocations map[string]int // 記錄損壞次數
    var damagedLocationsLock sync.Mutex
    damagedLocations = make(map[string]int)

    totalMem, _ := getSystemMemory()
    targetMemBytes := uint64(float64(totalMem) * config.UsagePercent)
    if config.Debug {
        utils.LogMessage(fmt.Sprintf("Memory test targeting %.2f%% of total system memory (%.2f GB)",
            config.UsagePercent*100, float64(targetMemBytes)/(1024*1024*1024)), config.Debug)
    }

    // Sequential memory test
    const speedTestSize = 50_000_000
    speedTestArray := make([]int64, speedTestSize)

    if config.Debug {
        utils.LogMessage("Measuring sequential memory write speed...", config.Debug)
    }
    writeStart := time.Now()
    writeCount := uint64(0)
    validationErrors := 0
    for i := 0; i < speedTestSize; i++ {
        speedTestArray[i] = int64(i * 2)
        writeCount++
    }
    writeDuration := time.Since(writeStart)
    bytesWritten := speedTestSize * 8
    writeSpeedMBps := float64(bytesWritten) / writeDuration.Seconds() / (1024 * 1024)

    if config.Debug {
        utils.LogMessage("Measuring sequential memory read speed...", config.Debug)
    }
    var sum int64 = 0
    readCount := uint64(0)
    readStart := time.Now()
    for i := 0; i < speedTestSize; i++ {
        val := speedTestArray[i]
        expectedVal := int64(i * 2)
        if val != expectedVal {
            validationErrors++
            if validationErrors < 10 && config.Debug {
                utils.LogMessage(fmt.Sprintf("Data validation error at index %d: got %d, expected %d",
                    i, val, expectedVal), config.Debug)
            }
        }
        sum += val
        readCount++
    }
    readDuration := time.Since(readStart)
    bytesRead := speedTestSize * 8
    readSpeedMBps := float64(bytesRead) / readDuration.Seconds() / (1024 * 1024)

    if validationErrors > 0 && config.Debug {
        utils.LogMessage(fmt.Sprintf("WARNING: Found %d validation errors during sequential read test!",
            validationErrors), config.Debug)
        errorChan <- fmt.Sprintf("Memory data corruption detected: %d errors in sequential read test",
            validationErrors)
    } else if config.Debug {
        utils.LogMessage("Sequential data validation: PASSED", config.Debug)
    }

    // Random memory access test
    if config.Debug {
        utils.LogMessage("Measuring random memory access speed...", config.Debug)
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
                    utils.LogMessage(fmt.Sprintf("Random access validation error at index %d: got %d, expected %d",
                        idx, originalVal, expectedVal), config.Debug)
                }
            }
        } else {
            expectedVal = int64(idx * 2)
            if originalVal != expectedVal {
                randomValidationErrors++
                if randomValidationErrors < 10 && config.Debug {
                    utils.LogMessage(fmt.Sprintf("Initial pattern validation error at index %d: got %d, expected %d",
                        idx, originalVal, expectedVal), config.Debug)
                }
            }
        }
        newVal := originalVal ^ 0x1
        speedTestArray[idx] = newVal
        modifications[idx] = newVal
        randomCount++
    }
    randomDuration := time.Since(randomStart)
    randomBytes := int64(len(randIndices) * 16)
    randomSpeedMBps := float64(randomBytes) / randomDuration.Seconds() / (1024 * 1024)

    if randomValidationErrors > 0 && config.Debug {
        utils.LogMessage(fmt.Sprintf("WARNING: Found %d validation errors during random access test!",
            randomValidationErrors), config.Debug)
        errorChan <- fmt.Sprintf("Memory data corruption detected: %d errors in random access test",
            randomValidationErrors)
    } else if config.Debug {
        utils.LogMessage("Random access data validation: PASSED", config.Debug)
    }

    if config.Debug {
        utils.LogMessage(fmt.Sprintf("Memory speed results:"), config.Debug)
        utils.LogMessage(fmt.Sprintf("  - Sequential write: %.2f MB/s, operations: %d", writeSpeedMBps, writeCount), config.Debug)
        utils.LogMessage(fmt.Sprintf("  - Sequential read:  %.2f MB/s, operations: %d", readSpeedMBps, readCount), config.Debug)
        utils.LogMessage(fmt.Sprintf("  - Random access:    %.2f MB/s, operations: %d", randomSpeedMBps, randomCount), config.Debug)
    }

    // Clean up
    speedTestArray = nil
    modifications = nil
    runtime.GC()

    // Long-running memory stress test
    const arraySize = 10_000_000
    const bytesPerEntry = 8
    arraysNeeded := int(targetMemBytes / (arraySize * bytesPerEntry))
    if arraysNeeded < 1 {
        arraysNeeded = 1
    }
    if config.Debug {
        utils.LogMessage(fmt.Sprintf("Memory test allocating %d arrays of %d elements each", arraysNeeded, arraySize), config.Debug)
    }

    var arrays [][]int64
    allocStart := time.Now()
    var bytesAllocated uint64 = 0
    seed := time.Now().UnixNano()
    arraySeedValues := make([]int64, 0, arraysNeeded)
    const checkpointInterval = 100_000

    for i := 0; i < arraysNeeded; i++ {
        var arr []int64
        func() {
            defer func() {
                if r := recover(); r != nil && config.Debug {
                    utils.LogMessage(fmt.Sprintf("Recovered from allocation panic: %v", r), config.Debug)
                }
            }()
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
            bytesAllocated += arraySize * bytesPerEntry
            if config.Debug && (i+1)%10 == 0 {
                allocPercent := float64(bytesAllocated) * 100 / float64(totalMem)
                utils.LogMessage(fmt.Sprintf("Memory allocation progress: %d/%d arrays (%.2f%% of system memory)",
                    i+1, arraysNeeded, allocPercent), config.Debug)
            }
        } else {
            if config.Debug {
                utils.LogMessage("Failed to allocate memory array, continuing with what we have", config.Debug)
            }
            break
        }
        if bytesAllocated >= targetMemBytes {
            if config.Debug {
                utils.LogMessage("Reached target memory allocation", config.Debug)
            }
            break
        }
        if i%100 == 0 {
            runtime.GC()
        }
    }

    allocDuration := time.Since(allocStart)
    allocSpeedMBps := float64(bytesAllocated) / allocDuration.Seconds() / (1024 * 1024)
    memoryUsagePercent := float64(bytesAllocated) * 100 / float64(totalMem)

    if config.Debug {
        utils.LogMessage(fmt.Sprintf("Memory allocated: %.2f GB out of %.2f GB total (%.2f%% of system memory)",
            float64(bytesAllocated)/(1024*1024*1024),
            float64(totalMem)/(1024*1024*1024),
            memoryUsagePercent), config.Debug)
        utils.LogMessage(fmt.Sprintf("Memory bulk allocation speed: %.2f MB/s", allocSpeedMBps), config.Debug)
    }

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
        errorChan <- fmt.Sprintf("Could only allocate %.2f%% of system memory, wanted %.2f%%",
            memoryUsagePercent, config.UsagePercent*100)
    }

    validationTicker := time.NewTicker(30 * time.Second)
    defer validationTicker.Stop()
    performanceUpdateTicker := time.NewTicker(10 * time.Second)
    defer performanceUpdateTicker.Stop()

    memValidationErrors := uint64(0)
    operationsSinceLastValidation := uint64(0)
    operationCount := uint64(0)
    lastPerformanceUpdate := time.Now()

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
                if currentSpeedMBps < perfStats_.Memory.MinRandomAccessSpeed || perfStats_.Memory.MinRandomAccessSpeed == 0 {
                    perfStats_.Memory.MinRandomAccessSpeed = currentSpeedMBps
                }
                if currentSpeedMBps > perfStats_.Memory.MaxRandomAccessSpeed {
                    perfStats_.Memory.MaxRandomAccessSpeed = currentSpeedMBps
                }
                perfStats_.Memory.SumRandomAccessSpeed += currentSpeedMBps
                perfStats_.Memory.RandomAccessCount++
                perfStats_.Memory.ReadSpeed = readSpeedMBps
                if readSpeedMBps < perfStats_.Memory.MinReadSpeed || perfStats_.Memory.MinReadSpeed == 0 {
                    perfStats_.Memory.MinReadSpeed = readSpeedMBps
                }
                if readSpeedMBps > perfStats_.Memory.MaxReadSpeed {
                    perfStats_.Memory.MaxReadSpeed = readSpeedMBps
                }
                perfStats_.Memory.SumReadSpeed += readSpeedMBps
                perfStats_.Memory.ReadSpeedCount++
                perfStats_.Memory.WriteSpeed = writeSpeedMBps
                if writeSpeedMBps < perfStats_.Memory.MinWriteSpeed || perfStats_.Memory.MinWriteSpeed == 0 {
                    perfStats_.Memory.MinWriteSpeed = writeSpeedMBps
                }
                if writeSpeedMBps > perfStats_.Memory.MaxWriteSpeed {
                    perfStats_.Memory.MaxWriteSpeed = writeSpeedMBps
                }
                perfStats_.Memory.SumWriteSpeed += writeSpeedMBps
                perfStats_.Memory.WriteSpeedCount++
                perfStats_.Unlock()
                if config.Debug {
                    utils.LogMessage(fmt.Sprintf("Performance update: Random Access Speed %.2f MB/s, Read Speed %.2f MB/s, Write Speed %.2f MB/s",
                        currentSpeedMBps, readSpeedMBps, writeSpeedMBps), config.Debug)
                }
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
                    if damagedLocations[address] >= 3 { // 連續3次錯誤則跳過
                        damagedLocationsLock.Unlock()
                        continue
                    }
                    damagedLocationsLock.Unlock()

                    if actualVal != expectedVal {
                        localErrors++
                        damagedLocationsLock.Lock()
                        damagedLocations[address]++
                        damagedLocationsLock.Unlock()

                        errorRecordsLock.Lock()
                        errorRecords = append(errorRecords, ErrorRecord{
                            Timestamp: time.Now(),
                            ArrIdx:    arrIdx,
                            ElemIdx:   elemIdx,
                            Actual:    actualVal,
                            Expected:  expectedVal,
                            Address:   address,
                            RepairTried: false,
                        })
                        errorRecordsLock.Unlock()

                        // 嘗試修復
                        arrays[arrIdx][elemIdx] = expectedVal
                        if config.Debug {
                            utils.LogMessage(fmt.Sprintf("Checkpoint validation error at arr[%d][%d]: got %d, expected %d, address: %s, repair attempted",
                                arrIdx, elemIdx, actualVal, expectedVal, address), config.Debug)
                        }

                        // 再次驗證
                        if arrays[arrIdx][elemIdx] != expectedVal {
                            if config.Debug {
                                utils.LogMessage(fmt.Sprintf("Repair failed at arr[%d][%d], address: %s",
                                    arrIdx, elemIdx, address), config.Debug)
                            }
                        }
                    }
                }
                checkpointDataLock.Unlock()
            }

            memValidationErrors += localErrors
            validationDuration := time.Since(validationStart)

            if localErrors > 0 {
                errorMsg := fmt.Sprintf("Memory validation found %d errors! Total errors so far: %d", localErrors, memValidationErrors)
                if config.Debug {
                    utils.LogMessage(errorMsg, config.Debug)
                }
                errorChan <- errorMsg
            } else if config.Debug {
                utils.LogMessage(fmt.Sprintf("Memory validation PASSED (checked %d arrays in %.2f seconds)", len(arrays), validationDuration.Seconds()), config.Debug)
            }

            operationsSinceLastValidation = 0

        case <-stop:
            if config.Debug {
                utils.LogMessage("Memory test stopped.", config.Debug)
                if memValidationErrors > 0 {
                    utils.LogMessage(fmt.Sprintf("Total memory validation errors during test: %d", memValidationErrors), config.Debug)
                    errorRecordsLock.Lock()
                    utils.LogMessage(fmt.Sprintf("Error records: %+v", errorRecords), config.Debug)
                    errorRecordsLock.Unlock()
                    damagedLocationsLock.Lock()
                    utils.LogMessage(fmt.Sprintf("Damaged locations (error count): %+v", damagedLocations), config.Debug)
                    damagedLocationsLock.Unlock()
                } else {
                    utils.LogMessage("Memory data validation: All checks PASSED", config.Debug)
                }
            }
            return

        default:
            for i := 0; i < 1000 && operationsSinceLastValidation < 5000; i++ {
                if len(arrays) == 0 {
                    break
                }
                arrIdx := threadRNG.Intn(len(arrays))
                elemIdx := threadRNG.Intn(arraySize)
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
                perfStats_.Memory.RandomAccessCount++
                perfStats_.Unlock()

                operationCount++
                operationsSinceLastValidation++
            }

            if threadRNG.Intn(10_000) == 0 {
                runtime.GC()
            }
            time.Sleep(time.Millisecond)
        }
    }
}

// getSystemMemory retrieves total and free system memory
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
