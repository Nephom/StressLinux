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

    const speedTestSize = 50_000_000
    var speedTestArray []int64
    speedTestArray = make([]int64, speedTestSize)

    if config.Debug {
        utils.LogMessage("Measuring sequential memory write speed...", config.Debug)
    }

    writeStart := time.Now()
    writeCount := uint64(0)
    for i := 0; i < speedTestSize; i++ {
        speedTestArray[i] = int64(i)
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
        sum += speedTestArray[i]
        readCount++
    }
    readDuration := time.Since(readStart)
    bytesRead := speedTestSize * 8
    readSpeedMBps := float64(bytesRead) / readDuration.Seconds() / (1024 * 1024)

    if config.Debug {
        utils.LogMessage("Measuring random memory access speed...", config.Debug)
    }

    randIndices := make([]int, 5_000_000)
    for i := range randIndices {
        randIndices[i] = rand.Intn(speedTestSize)
    }

    randomCount := uint64(0)
    randomStart := time.Now()
    for _, idx := range randIndices {
        val := speedTestArray[idx]
        speedTestArray[idx] = val ^ 0x1
        randomCount++
    }
    randomDuration := time.Since(randomStart)
    randomBytes := int64(len(randIndices) * 16)
    randomSpeedMBps := float64(randomBytes) / randomDuration.Seconds() / (1024 * 1024)

    if config.Debug {
        utils.LogMessage(fmt.Sprintf("Memory speed results:"), config.Debug)
        utils.LogMessage(fmt.Sprintf("  - Sequential write: %.2f MB/s, operations: %d", writeSpeedMBps, writeCount), config.Debug)
        utils.LogMessage(fmt.Sprintf("  - Sequential read:  %.2f MB/s, operations: %d", readSpeedMBps, readCount), config.Debug)
        utils.LogMessage(fmt.Sprintf("  - Random access:    %.2f MB/s, operations: %d", randomSpeedMBps, randomCount), config.Debug)
    }

    speedTestArray = nil
    runtime.GC()

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

    for i := 0; i < arraysNeeded; i++ {
        var arr []int64
        func() {
            defer func() {
                if r := recover(); r != nil && config.Debug {
                    utils.LogMessage(fmt.Sprintf("Recovered from allocation panic: %v", r), config.Debug)
                }
            }()

            arr = make([]int64, arraySize)
            for j := range arr {
                arr[j] = rand.Int63()
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

    for {
        select {
        case <-stop:
            if config.Debug {
                utils.LogMessage("Memory test stopped.", config.Debug)
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
                arrays[arrIdx][elemIdx] = val ^ 0xFF
                perfStats.Lock()
                perfStats.Memory.RandomAccessCount++
                perfStats.Unlock()
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
