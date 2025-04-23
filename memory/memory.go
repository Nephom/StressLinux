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
)

// RunMemoryStressTest runs the memory stress test
func RunMemoryStressTest(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, config MemoryConfig, perfStats *config.PerformanceStats) {
    defer wg.Done()

    totalMem, _ := getSystemMemory()
    targetMemBytes := uint64(float64(totalMem) * config.UsagePercent)

    if config.Debug {
        utils.LogMessage(fmt.Sprintf("Memory test targeting %.2f%% of total system memory (%.2f GB)",
            config.UsagePercent*100, float64(targetMemBytes)/(1024*1024*1024)), config.Debug)
    }

    // Sequential memory test with validation
    const speedTestSize = 50_000_000
    var speedTestArray []int64
    speedTestArray = make([]int64, speedTestSize)

    // Write test with data pattern
    if config.Debug {
        utils.LogMessage("Measuring sequential memory write speed with validation pattern...", config.Debug)
    }

    writeStart := time.Now()
    writeCount := uint64(0)
    validationErrors := 0
    
    for i := 0; i < speedTestSize; i++ {
        speedTestArray[i] = int64(i * 2) // Using a simple pattern (i*2) for validation
        writeCount++
    }
    writeDuration := time.Since(writeStart)
    bytesWritten := speedTestSize * 8
    writeSpeedMBps := float64(bytesWritten) / writeDuration.Seconds() / (1024 * 1024)

    // Read test with validation
    if config.Debug {
        utils.LogMessage("Measuring sequential memory read speed with data validation...", config.Debug)
    }

    var sum int64 = 0
    readCount := uint64(0)
    readStart := time.Now()
    for i := 0; i < speedTestSize; i++ {
        val := speedTestArray[i]
        expectedVal := int64(i * 2)
        
        // Validate read data matches expected pattern
        if val != expectedVal {
            validationErrors++
            if validationErrors < 10 && config.Debug { // Limit error messages
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
        utils.LogMessage("Sequential data validation: PASSED (all values matched expected pattern)", config.Debug)
    }

    // Random memory access test with validation
    if config.Debug {
        utils.LogMessage("Measuring random memory access speed with data validation...", config.Debug)
    }

    randIndices := make([]int, 5_000_000)
    for i := range randIndices {
        randIndices[i] = rand.Intn(speedTestSize)
    }

    // Track modifications for validation
    modifications := make(map[int]int64)
    randomValidationErrors := 0
    randomCount := uint64(0)
    randomStart := time.Now()
    
    for _, idx := range randIndices {
        originalVal := speedTestArray[idx]
        expectedVal := int64(0)
        
        // If we've modified this index before, check it matches our expected value
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
            // First access to this index, should be our initial pattern
            expectedVal = int64(idx * 2)
            if originalVal != expectedVal {
                randomValidationErrors++
                if randomValidationErrors < 10 && config.Debug {
                    utils.LogMessage(fmt.Sprintf("Initial pattern validation error at index %d: got %d, expected %d", 
                        idx, originalVal, expectedVal), config.Debug)
                }
            }
        }
        
        // Modify the value with XOR (reversible operation)
        newVal := originalVal ^ 0x1
        speedTestArray[idx] = newVal
        // Store the expected value after modification
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
        utils.LogMessage("Random access data validation: PASSED (all values matched expected pattern)", config.Debug)
    }

    if config.Debug {
        utils.LogMessage(fmt.Sprintf("Memory speed results:"), config.Debug)
        utils.LogMessage(fmt.Sprintf("  - Sequential write: %.2f MB/s, operations: %d", writeSpeedMBps, writeCount), config.Debug)
        utils.LogMessage(fmt.Sprintf("  - Sequential read:  %.2f MB/s, operations: %d", readSpeedMBps, readCount), config.Debug)
        utils.LogMessage(fmt.Sprintf("  - Random access:    %.2f MB/s, operations: %d", randomSpeedMBps, randomCount), config.Debug)
    }

    // Clean up for memory allocation test
    speedTestArray = nil
    modifications = nil
    runtime.GC()

    // Long-running memory stress test setup
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
    // Store a validation pattern for periodic integrity checks
    var validationPatterns [][]int64
    allocStart := time.Now()
    var bytesAllocated uint64 = 0

    for i := 0; i < arraysNeeded; i++ {
        var arr []int64
        var pattern []int64
        
        func() {
            defer func() {
                if r := recover(); r != nil && config.Debug {
                    utils.LogMessage(fmt.Sprintf("Recovered from allocation panic: %v", r), config.Debug)
                }
            }()

            arr = make([]int64, arraySize)
            pattern = make([]int64, arraySize)
            
            // Initialize with a validation pattern - every 10th value is its index
            for j := range arr {
                // Store a deterministic but semi-random pattern
                value := rand.Int63()
                arr[j] = value
                pattern[j] = value
                
                // Special validation values every 10th element
                if j%10 == 0 {
                    arr[j] = int64(j)
                    pattern[j] = int64(j)
                }
            }
        }()

        if arr != nil {
            arrays = append(arrays, arr)
            validationPatterns = append(validationPatterns, pattern)
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

    perfStats.Lock()
    perfStats.Memory.WriteSpeed = writeSpeedMBps
    perfStats.Memory.ReadSpeed = readSpeedMBps
    perfStats.Memory.RandomAccessSpeed = randomSpeedMBps
    perfStats.Memory.UsagePercent = memoryUsagePercent
    perfStats.Memory.WriteCount = writeCount
    perfStats.Memory.ReadCount = readCount
    perfStats.Memory.RandomAccessCount = randomCount
    perfStats.Unlock()

    if memoryUsagePercent < config.UsagePercent*75.0 {
        errorChan <- fmt.Sprintf("Could only allocate %.2f%% of system memory, wanted %.2f%%",
            memoryUsagePercent, config.UsagePercent*100)
    }

    // Add periodic full validation
    validationTicker := time.NewTicker(30 * time.Second)
    defer validationTicker.Stop()
    
    // Track modifications for each array in the ongoing test
    memValidationErrors := uint64(0)
    operationsSinceLastValidation := uint64(0)
    
    // Track memory operations
    modificationsMade := make(map[int]map[int]int64)
    for i := range arrays {
        modificationsMade[i] = make(map[int]int64)
    }

    for {
        select {
        case <-validationTicker.C:
            // Perform full memory validation
            if config.Debug {
                utils.LogMessage(fmt.Sprintf("Running full memory validation after %d operations...", 
                    operationsSinceLastValidation), config.Debug)
            }
            
            validationStart := time.Now()
            localErrors := uint64(0)
            
            // Check all arrays
            for arrIdx := range arrays {
                if arrIdx >= len(validationPatterns) {
                    continue
                }
                
                // Sample every 100th element to reduce validation overhead
                for elemIdx := 0; elemIdx < len(arrays[arrIdx]); elemIdx += 100 {
                    expectedVal := validationPatterns[arrIdx][elemIdx]
                    
                    // If we modified this value, get the expected modified value
                    if modVal, exists := modificationsMade[arrIdx][elemIdx]; exists {
                        expectedVal = modVal
                    }
                    
                    actualVal := arrays[arrIdx][elemIdx]
                    if actualVal != expectedVal {
                        localErrors++
                        if localErrors < 10 && config.Debug {
                            utils.LogMessage(fmt.Sprintf("Memory validation error at arr[%d][%d]: got %d, expected %d", 
                                arrIdx, elemIdx, actualVal, expectedVal), config.Debug)
                        }
                    }
                }
                
                // Every 10th array, validate special values (indices divisible by 10)
                if arrIdx%10 == 0 {
                    for elemIdx := 0; elemIdx < len(arrays[arrIdx]); elemIdx += 10 {
                        if elemIdx%10 == 0 {
                            expectedVal := int64(elemIdx) // Special pattern: value equals index
                            // If we modified this value, get the expected modified value
                            if modVal, exists := modificationsMade[arrIdx][elemIdx]; exists {
                                expectedVal = modVal
                            }
                            
                            actualVal := arrays[arrIdx][elemIdx]
                            if actualVal != expectedVal {
                                localErrors++
                                if localErrors < 10 && config.Debug {
                                    utils.LogMessage(fmt.Sprintf("Special value validation error at arr[%d][%d]: got %d, expected %d", 
                                        arrIdx, elemIdx, actualVal, expectedVal), config.Debug)
                                }
                            }
                        }
                    }
                }
            }
            
            memValidationErrors += localErrors
            validationDuration := time.Since(validationStart)
            
            if localErrors > 0 {
                errorMsg := fmt.Sprintf("Memory validation found %d errors! Total errors so far: %d", 
                    localErrors, memValidationErrors)
                if config.Debug {
                    utils.LogMessage(errorMsg, config.Debug)
                }
                errorChan <- errorMsg
            } else if config.Debug {
                utils.LogMessage(fmt.Sprintf("Memory validation PASSED (checked %d arrays in %.2f seconds)", 
                    len(arrays), validationDuration.Seconds()), config.Debug)
            }
            
            operationsSinceLastValidation = 0
            
        case <-stop:
            if config.Debug {
                utils.LogMessage("Memory test stopped.", config.Debug)
                if memValidationErrors > 0 {
                    utils.LogMessage(fmt.Sprintf("Total memory validation errors during test: %d", 
                        memValidationErrors), config.Debug)
                } else {
                    utils.LogMessage("Memory data validation: All checks PASSED", config.Debug)
                }
            }
            return
            
        default:
            for i := 0; i < 1000; i++ {
                if len(arrays) == 0 {
                    break
                }

                arrIdx := rand.Intn(len(arrays))
                elemIdx := rand.Intn(arraySize)
                val := arrays[arrIdx][elemIdx]
                newVal := val ^ 0xFF
                arrays[arrIdx][elemIdx] = newVal
                
                // Track this modification for validation
                modificationsMade[arrIdx][elemIdx] = newVal
                
                perfStats.Lock()
                perfStats.Memory.RandomAccessCount++
                perfStats.Unlock()
                
                operationsSinceLastValidation++
            }

            if rand.Intn(10_000) == 0 {
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
