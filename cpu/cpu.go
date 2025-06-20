package cpu

import (
    "crypto"
    "crypto/aes"
    "crypto/cipher"
    "crypto/ecdsa"
    "crypto/ed25519"
    "crypto/elliptic"
    "crypto/rand"
    "crypto/rsa"
    "crypto/sha256"
    "crypto/sha512"
    "bytes"
    "context"
    "fmt"
    "math"
    "os"
    "runtime"
    "stress/config"
    "stress/utils"
    "sync"
    "sync/atomic"
    "time"
    "strings"

    "golang.org/x/sys/unix"
    "golang.org/x/crypto/blake2b"
    "golang.org/x/crypto/chacha20poly1305"
    "golang.org/x/crypto/pbkdf2"
)

// adjustBatchSize adjusts the batch size based on the load level
func adjustBatchSize(baseBatchSize int, loadLevel string) int {
    adjusted := baseBatchSize
    switch strings.ToLower(loadLevel) {
    case "high", "2":
        adjusted = baseBatchSize * 2
    case "low", "1":
        adjusted = baseBatchSize / 100
    case "default", "0", "":
        adjusted = baseBatchSize
    default:
        utils.LogMessage(fmt.Sprintf("Invalid CPU load level: %s, using Default", loadLevel), true)
        adjusted = baseBatchSize
    }
    if adjusted < 100 {
        adjusted = 100
    }
    return adjusted
}

// adjustCycleDuration adjusts the cycle duration based on the load level
func adjustCycleDuration(loadLevel string) time.Duration {
    baseDuration := 50 * time.Millisecond
    switch strings.ToLower(loadLevel) {
    case "high", "2":
        return baseDuration * 2
    case "low", "1":
        return baseDuration / 2
    case "default", "0", "":
        return baseDuration
    default:
        utils.LogMessage(fmt.Sprintf("Invalid CPU load level: %s, using Default cycle duration", loadLevel), true)
        return baseDuration
    }
}

// RunCPUStressTests runs CPU stress tests across specified cores
func RunCPUStressTests(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, testConfig CPUConfig, perfStats *config.PerformanceStats) {
    defer wg.Done()

    // Use actual process ID
    pid := os.Getpid()
    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] started on %d cores", pid, testConfig.NumCores), false)

    // Set GOMAXPROCS to limit maximum parallelism
    if testConfig.NumCores > 0 {
        oldGOMAXPROCS := runtime.GOMAXPROCS(testConfig.NumCores)
        if testConfig.Debug {
            utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] set GOMAXPROCS to %d (was %d)", pid, testConfig.NumCores, oldGOMAXPROCS), true)
        }
    }

    // Get cache information
    cacheInfo, err := utils.GetCacheInfo()
    if err != nil {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] failed to get cache info: %v, using defaults", pid, err), false)
    } else if testConfig.Debug {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] L1 Cache Size: %.2f KB", pid, float64(cacheInfo.L1Size)/1024), true)
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] L2 Cache Size: %.2f KB", pid, float64(cacheInfo.L2Size)/1024), true)
        if cacheInfo.L3Size > 0 {
            utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] L3 Cache Size: %.2f MB", pid, float64(cacheInfo.L3Size)/(1024*1024)), true)
        } else {
            utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] L3 Cache: Not present", pid), true)
        }
    }

    // Determine the list of CPU cores to use
    allCPUs := testConfig.CPUList
    if len(allCPUs) == 0 {
        allCPUs = make([]int, testConfig.NumCores)
        for i := 0; i < testConfig.NumCores; i++ {
            allCPUs[i] = i
        }
    }

    if testConfig.NumCores > len(allCPUs) {
        testConfig.NumCores = len(allCPUs)
        if testConfig.Debug {
            utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] adjusted NumCores to %d to match CPU list length", pid, testConfig.NumCores), true)
        }
    } else if testConfig.NumCores > 0 {
        allCPUs = allCPUs[:testConfig.NumCores]
    }

    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] running on %d cores (CPUs: %v)", pid, len(allCPUs), allCPUs), false)

    var innerWg sync.WaitGroup
    innerWg.Add(len(allCPUs))
    for _, cpuID := range allCPUs {
        go func(id int) {
            defer innerWg.Done()
            runAllTestsPerCore(pid, stop, errorChan, id, perfStats, testConfig.Debug, testConfig.LoadLevel)
        }(cpuID)
    }
    innerWg.Wait()

    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] completed", pid), false)
    runtime.GC()
}

// getTestNames returns test names
func getTestNames(tests []struct {
    name   string
    fn     func(int, <-chan struct{}, chan<- string, int, *config.PerformanceStats, bool, string, time.Duration)
    weight float64
}) []string {
    names := make([]string, len(tests))
    for i, test := range tests {
        names[i] = test.name
    }
    return names
}

// runAllTestsPerCore runs all test types sequentially in one goroutine per core
func runAllTestsPerCore(mainPID int, stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string) {
    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] starting stress worker on CPU %d", mainPID, cpuID), false)

    runtime.LockOSThread()
    defer runtime.UnlockOSThread()

    // Create a local context that can be cancelled immediately when stop signal is received
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Start a goroutine to watch for stop signal and cancel context immediately
    go func() {
        select {
        case <-stop:
            utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] Stress test is timeout, exit! (CPU %d)", mainPID, cpuID), false)
            cancel() // This will immediately cancel all running operations
            return
        case <-ctx.Done():
            return
        }
    }()

    cpuset := unix.CPUSet{}
    cpuset.Set(cpuID)
    err := unix.SchedSetaffinity(0, &cpuset)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] failed to set CPU affinity for CPU %d: %v", mainPID, cpuID, err), false)
    } else if debug {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] successfully set CPU affinity for CPU %d", mainPID, cpuID), true)
        if debug {
            var actualSet unix.CPUSet
            err := unix.SchedGetaffinity(0, &actualSet)
            if err != nil {
                utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] failed to get CPU affinity for CPU %d: %v", mainPID, cpuID, err), true)
            } else {
                utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] actual CPU affinity for CPU %d: %v", mainPID, cpuID, actualSet), true)
            }
        }
    }

    // Define all available tests
    allTests := map[string]struct {
        fn     func(int, <-chan struct{}, chan<- string, int, *config.PerformanceStats, bool, string, time.Duration)
        weight float64
    }{
        "integer": {runIntegerComputationParallel, 0.2},
        "float":   {runFloatComputationParallel, 0.2},
        "vector":  {runVectorComputationParallel, 0.2},
        "cache":   {runCacheStressParallel, 0.1},
        "branch":  {runBranchPredictionParallel, 0.15},
        "crypto":  {runCryptoStressParallel, 0.15},
    }

    // Define test configurations for each load level
    type testConfig struct {
        name   string
        weight float64 // Only used for non-high load levels
        interval time.Duration // Interval after each test
    }

    loadLevelConfigs := map[string][]testConfig{
        "high": {
            {"integer", 0.0, 0},
            {"float", 0.0, 0},
            {"vector", 0.0, 0},
            {"cache", 0.0, 0},
            {"branch", 0.0, 0},
            {"crypto", 0.0, 0},
        },
        "default": {
            {"float", 0.2, 50 * time.Millisecond},
            {"vector", 0.2, 50 * time.Millisecond},
            {"cache", 0.2, 50 * time.Millisecond},
            {"branch", 0.2, 50 * time.Millisecond},
            {"crypto", 0.2, 50 * time.Millisecond},
        },
        "low": {
            {"integer", 0.5, 100 * time.Millisecond},
            {"float", 0.5, 100 * time.Millisecond},
        },
    }

    // Select tests based on load level
    configKey := strings.ToLower(loadLevel)
    switch configKey {
    case "high", "2":
        configKey = "high"
    case "low", "1":
        configKey = "low"
    case "default", "0", "":
        configKey = "default"
    default:
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] invalid CPU load level: %s, using Default", mainPID, loadLevel), false)
        configKey = "default"
    }

    tests, exists := loadLevelConfigs[configKey]
    if !exists {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] no config for load level %s, using Default", mainPID, configKey), false)
        tests = loadLevelConfigs["default"]
    }

    var selectedTests []struct {
        name   string
        fn     func(int, <-chan struct{}, chan<- string, int, *config.PerformanceStats, bool, string, time.Duration)
        weight float64
        interval time.Duration
    }

    totalWeight := 0.0
    for _, test := range tests {
        if configKey != "high" {
            totalWeight += test.weight
        }
    }

    for _, test := range tests {
        testFn, ok := allTests[test.name]
        if !ok {
            utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] test %s not found, skipping", mainPID, test.name), false)
            continue
        }
        weight := test.weight
        if configKey == "high" {
            weight = 0.0 // No weight for high load
        }
        selectedTests = append(selectedTests, struct {
            name   string
            fn     func(int, <-chan struct{}, chan<- string, int, *config.PerformanceStats, bool, string, time.Duration)
            weight float64
            interval time.Duration
        }{test.name, testFn.fn, weight, test.interval})
    }

    if len(selectedTests) == 0 {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] no valid tests selected for CPU %d, exiting", mainPID, cpuID), false)
        return
    }

    cycleDuration := adjustCycleDuration(loadLevel)
    if debug {
        testNames := make([]string, len(selectedTests))
        for i, test := range selectedTests {
            testNames[i] = test.name
        }
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] CPU %d: Load level %s, running %d tests: %v, expected cycle time: %v",
            mainPID, cpuID, configKey, len(selectedTests), testNames, cycleDuration), true)
    }

    // Main test execution loop with immediate stop capability
    for {
        select {
        case <-ctx.Done():
            utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] Stress test is timeout, exit! Stopping CPU %d immediately", mainPID, cpuID), false)
            stopSubprocesses(mainPID, cpuID, debug)
            return
        default:
            start := time.Now()
            
            // Execute all tests in the cycle, but check for cancellation frequently
            for i, test := range selectedTests {
                select {
                case <-ctx.Done():
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] Stress test is timeout, exit! Interrupted CPU %d during test %s", mainPID, cpuID, test.name), false)
                    stopSubprocesses(mainPID, cpuID, debug)
                    return
                default:
                    // Calculate test duration
                    testDuration := 100 * time.Millisecond // Fixed base duration
                    if configKey != "high" {
                        testDuration = time.Duration(float64(cycleDuration) * test.weight)
                    }
                    
                    // Create a channel to signal test completion
                    testDone := make(chan struct{})
                    
                    // Run the test in a goroutine so we can cancel it immediately
                    go func() {
                        defer close(testDone)
                        test.fn(mainPID, ctx.Done(), errorChan, cpuID, perfStats, debug, loadLevel, testDuration)
                    }()
                    
                    // Wait for either test completion or cancellation
                    select {
                    case <-ctx.Done():
                        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] Stress test is timeout, exit! Cancelling test %s on CPU %d", mainPID, test.name, cpuID), false)
                        stopSubprocesses(mainPID, cpuID, debug)
                        return
                    case <-testDone:
                        // Test completed normally
                    }
                    
                    // Add interval (except for last test in cycle or high load)
                    if test.interval > 0 && (i < len(selectedTests)-1 || configKey != "high") {
                        select {
                        case <-ctx.Done():
                            utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] Stress test is timeout, exit! Cancelled during interval on CPU %d", mainPID, cpuID), false)
                            stopSubprocesses(mainPID, cpuID, debug)
                            return
                        case <-time.After(test.interval):
                            if debug {
                                utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] CPU %d: Applied interval of %v after test %s",
                                    mainPID, cpuID, test.interval, test.name), true)
                            }
                        }
                    }
                }
            }
            
            if debug {
                utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] CPU %d: Actual cycle time: %v", mainPID, cpuID, time.Since(start)), true)
            }
            
            // Check for cancellation before GC
            select {
            case <-ctx.Done():
                utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] Stress test is timeout, exit! Stopping CPU %d after cycle completion", mainPID, cpuID), false)
                stopSubprocesses(mainPID, cpuID, debug)
                return
            default:
                runtime.GC()
            }
        }
    }
}

func stopSubprocesses(mainPID, cpuID int, debug bool) {
    subprocesses := []string{"integer", "float", "vector", "cache", "branch", "crypto"}
    for i, name := range subprocesses {
        subPID := mainPID + (i+1)*100
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] subprocess %s [PID: %d] stopping on CPU %d...", mainPID, name, subPID, cpuID), false)
        time.Sleep(100 * time.Millisecond)
        if debug {
            utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] subprocess %s [PID: %d] stopped on CPU %d", mainPID, name, subPID, cpuID), true)
        }
    }
}

// runIntegerComputationParallel performs intensive integer operations
func runIntegerComputationParallel(mainPID int, stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)
    baseBatchSize := 1000000
    batchSize := adjustBatchSize(baseBatchSize, loadLevel)
    primes := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53}

    var expectedResult int64
    var errorDetected bool

    for time.Since(startTime) < duration {
        select {
        case <-stop:
            perfStats.Lock()
            perfStats.CPU.IntegerCount += operationCount
            perfStats.Unlock()
            if debug {
                utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] integer test stopped on CPU %d", mainPID, cpuID), true)
            }
            return
        default:
            for i := 0; i < batchSize; i++ {
                var result int64 = 1
                for _, prime := range primes {
                    result = (result * int64(prime)) % (1<<31 - 1)
                    result = result ^ (result >> 3)
                    result = result + int64(prime)
                }
                if i == 0 {
                    expectedResult = result
                } else if !errorDetected && result != expectedResult {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] integer computation error on CPU %d", mainPID, cpuID), false)
                    if debug {
                        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] integer computation error on CPU %d: Expected %d, got %d", mainPID, cpuID, expectedResult, result), true)
                    }
                    errorChan <- fmt.Sprintf("Integer computation error on CPU %d", cpuID)
                    errorDetected = true
                }
                operationCount++
            }
        }
    }

    perfStats.Lock()
    perfStats.CPU.IntegerCount += operationCount
    perfStats.Unlock()

    if debug {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] CPU %d integer operations: %d", mainPID, cpuID, operationCount), true)
    }
}

// runFloatComputationParallel performs intensive floating-point operations
func runFloatComputationParallel(mainPID int, stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)
    baseBatchSize := 500000
    batchSize := adjustBatchSize(baseBatchSize, loadLevel)
    constants := []float64{3.14159, 2.71828, 1.41421, 1.73205, 2.23606, 2.44949, 2.64575}

    var expectedResult float64
    var errorDetected bool
    var epsilon float64 = 1e-10

    for time.Since(startTime) < duration {
        select {
        case <-stop:
            perfStats.Lock()
            perfStats.CPU.FloatCount += operationCount
            perfStats.Unlock()
            if debug {
                utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] float test stopped on CPU %d", mainPID, cpuID), true)
            }
            return
        default:
            for i := 0; i < batchSize; i++ {
                var result float64 = 1.0
                for _, c := range constants {
                    result = result * math.Sin(c) + math.Cos(result)
                    result = math.Sqrt(math.Abs(result)) + math.Log(1 + math.Abs(result))
                    result = math.Pow(result, 0.5) * c
                }
                if i == 0 {
                    expectedResult = result
                } else if !errorDetected && math.Abs(result-expectedResult) > epsilon {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] float computation error on CPU %d", mainPID, cpuID), false)
                    if debug {
                        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] float computation error on CPU %d: Expected %.10f, got %.10f", mainPID, cpuID, expectedResult, result), true)
                    }
                    errorChan <- fmt.Sprintf("Float computation error on CPU %d", cpuID)
                    errorDetected = true
                }
                operationCount++
            }
        }
    }

    perfStats.Lock()
    perfStats.CPU.FloatCount += operationCount
    perfStats.Unlock()

    if debug {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] CPU %d float operations: %d", mainPID, cpuID, operationCount), true)
    }
}

// runVectorComputationParallel performs SIMD-like vector operations
func runVectorComputationParallel(mainPID int, stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)
    const vectorSize = 1024
    baseBatchSize := 10000
    batchSize := adjustBatchSize(baseBatchSize, loadLevel)

    vecA := make([]float64, vectorSize)
    vecB := make([]float64, vectorSize)
    vecC := make([]float64, vectorSize)

    for i := 0; i < vectorSize; i++ {
        vecA[i] = float64(i%17) * 0.5
        vecB[i] = float64(i%19) * 0.75
    }

    var expectedChecksum float64
    var errorDetected bool
    var epsilon float64 = 1e-10

    for time.Since(startTime) < duration {
        select {
        case <-stop:
            perfStats.Lock()
            perfStats.CPU.VectorCount += operationCount
            perfStats.Unlock()
            if debug {
                utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] vector test stopped on CPU %d", mainPID, cpuID), true)
            }
            return
        default:
            for b := 0; b < batchSize; b++ {
                dotProduct := 0.0
                for i := 0; i < vectorSize; i++ {
                    vecC[i] = vecA[i] + vecB[i]
                    vecC[i] = vecC[i] * (vecA[i] * vecB[i])
                    vecC[i] = math.Sqrt(math.Abs(vecC[i]))
                    dotProduct += vecA[i] * vecB[i]
                }
                checksum := dotProduct
                if b == 0 {
                    expectedChecksum = checksum
                } else if !errorDetected && math.Abs(checksum-expectedChecksum) > epsilon {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] vector computation error on CPU %d", mainPID, cpuID), false)
                    if debug {
                        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] vector computation error on CPU %d: Expected checksum %.10f, got %.10f", mainPID, cpuID, expectedChecksum, checksum), true)
                    }
                    errorChan <- fmt.Sprintf("Vector computation error on CPU %d", cpuID)
                    errorDetected = true
                }
                operationCount++
            }
        }
    }

    perfStats.Lock()
    perfStats.CPU.VectorCount += operationCount
    perfStats.Unlock()

    if debug {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] CPU %d vector operations: %d", mainPID, cpuID, operationCount), true)
    }
}

// runCacheStressParallel performs cache stress testing
func runCacheStressParallel(mainPID int, stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)
    baseBatchSize := 1000000
    batchSize := adjustBatchSize(baseBatchSize, loadLevel)
    cacheInfo, err := utils.GetCacheInfo()
    if err != nil {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] failed to get cache info: %v, using defaults", mainPID, err), false)
    }

    var targetCacheSize int64
    var targetCacheLevel string
    if cacheInfo.L3Size > 0 {
        targetCacheSize = cacheInfo.L3Size
        targetCacheLevel = "L3"
    } else {
        targetCacheSize = cacheInfo.L2Size
        targetCacheLevel = "L2"
    }

    // 調整 cache 使用策略：每個 CPU 核心只使用部分 cache
    // 假設系統有 N 個核心，每個核心使用 cache 的 1/N 部分，再加上安全係數 0.5
    numCPUs := runtime.NumCPU()
    cachePortionPerCPU := float64(targetCacheSize) / float64(numCPUs) * 0.5
    arraySize := int(cachePortionPerCPU / 8) // 每個 int64 佔 8 bytes
    
    // 設定最小和最大限制
    minArraySize := 1024
    maxArraySize := int(float64(targetCacheSize) * 0.3 / 8) // 最多使用 30% 的總 cache
    
    if arraySize < minArraySize {
        arraySize = minArraySize
    } else if arraySize > maxArraySize {
        arraySize = maxArraySize
    }
    
    dataSizeBytes := int64(arraySize) * 8

    if debug {
        sizeMB := float64(dataSizeBytes) / (1024 * 1024)
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] stressing %s Cache on CPU %d with array size: %.2f MB (portion: %.2f%% of total cache)", 
            mainPID, targetCacheLevel, cpuID, sizeMB, (float64(dataSizeBytes)/float64(targetCacheSize))*100), true)
    }

    // 初始化資料陣列
    data := make([]int64, arraySize)
    for i := range data {
        data[i] = int64(i)
    }

    rng := utils.NewRand(int64(cpuID))
    const pageSize = 4096 / 8 // 4KB page size in int64 units
    pageCount := arraySize / pageSize
    if pageCount < 1 {
        pageCount = 1
    }

    // 預熱階段：循序存取確保資料載入到 cache
    var sum int64
    for i := 0; i < arraySize; i++ {
        sum += data[i]
        data[i] = sum ^ int64(i)
    }

    // 計算動態批次大小
    sampleStart := time.Now()
    for i := 0; i < 1000; i++ {
        pageIdx := rng.Intn(pageCount)
        startIdx := pageIdx * pageSize
        endIdx := startIdx + pageSize
        if endIdx > arraySize {
            endIdx = arraySize
        }
        
        for j := startIdx; j < endIdx; j++ {
            sum += data[j]
            // 增加計算密度以提高 cache 壓力
            for k := 0; k < 5; k++ {
                sum ^= data[j]
                sum += int64(k)
            }
            data[j] = sum
        }
    }

    sampleElapsed := time.Since(sampleStart)
    if sampleElapsed > 0 {
        batchTime := sampleElapsed / 1000
        targetBatches := int(duration / batchTime)
        if targetBatches > 0 {
            batchSize = (batchSize * targetBatches) / 1000
        }
        if batchSize < 100 {
            batchSize = 100
        }
    }

    // 主要測試迴圈 - 僅進行讀寫操作，不進行驗證
    for time.Since(startTime) < duration {
        select {
        case <-stop:
            perfStats.Lock()
            perfStats.CPU.CacheCount += operationCount
            perfStats.Unlock()
            if debug {
                utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] cache test stopped on CPU %d", mainPID, cpuID), true)
            }
            return
        default:
            for i := 0; i < batchSize; i++ {
                // 隨機選擇一個記憶體頁面
                pageIdx := rng.Intn(pageCount)
                startIdx := pageIdx * pageSize
                endIdx := startIdx + pageSize
                if endIdx > arraySize {
                    endIdx = arraySize
                }
                
                // 在該頁面內進行密集的讀寫操作
                for j := startIdx; j < endIdx; j++ {
                    // 讀取操作
                    sum += data[j]
                    
                    // 增加計算複雜度以提高 cache 壓力
                    for k := 0; k < 10; k++ {
                        sum ^= data[j]
                        sum += int64(j * k)
                    }
                    
                    // 寫入操作
                    data[j] = sum
                }
            }
            operationCount += uint64(batchSize)
        }
    }

    perfStats.Lock()
    perfStats.CPU.CacheCount += operationCount
    perfStats.Unlock()

    if debug {
        finalSizeMB := float64(dataSizeBytes) / (1024 * 1024)
        message := fmt.Sprintf("CPU test [PID: %d] CPU %d completed cache operations: %d (%.2f MB processed)", 
            mainPID, cpuID, operationCount, finalSizeMB)
        utils.LogMessage(message, true)  // 寫入日誌
        fmt.Println(message)             // 直接輸出到 console
    }
}

// runBranchPredictionParallel performs branch prediction stress testing
func NewSimpleRNG(seed int64) *SimpleRNG {
	return &SimpleRNG{seed: uint64(seed)}
}

func (r *SimpleRNG) Intn(n int) int {
	r.seed = r.seed*1103515245 + 12345
	return int(r.seed % uint64(n))
}

func runBranchPredictionParallel(mainPID int, stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string, duration time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			if debug {
				errorMsg := fmt.Sprintf("CPU test [PID: %d] CRASH on CPU %d: %v", mainPID, cpuID, r)
				utils.LogMessage(errorMsg, true)
				select {
				case errorChan <- errorMsg:
				default:
				}
			}
		}
	}()

	startTime := time.Now()
	var operationCount uint64
	baseBatchSize := 1000000
	batchSize := adjustBatchSize(baseBatchSize, loadLevel)
	
	// 使用簡單的隨機數生成器或適配你的 utils.Rand
	var rng RandomGenerator
	if utilsRand := utils.NewRand(int64(cpuID)); utilsRand != nil {
		// 如果 utils.Rand 實現了 Intn(int) int 方法，可以直接使用
		if r, ok := interface{}(utilsRand).(RandomGenerator); ok {
			rng = r
		} else {
			rng = NewSimpleRNG(int64(cpuID))
		}
	} else {
		rng = NewSimpleRNG(int64(cpuID))
	}
	
	// 增加驗證用的結果結構
	var testResult BranchTestResult
	var errorCount uint64
	
	// 更複雜的分支預測模式
	patterns := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
	patternIndex := 0
	
	// 動態調整負載
	lastCheckTime := startTime
	performanceCounter := uint64(0)
	
	for time.Since(startTime) < duration {
		select {
		case <-stop:
			updatePerfStats(perfStats, operationCount, &testResult, errorCount)
			if debug && errorCount > 0 {
				utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] branch test stopped on CPU %d with %d errors", mainPID, cpuID, errorCount), true)
			}
			return
		default:
			// 動態批次處理
			currentBatch := batchSize
			if time.Since(lastCheckTime) > 100*time.Millisecond {
				// 根據系統負載動態調整
				if loadLevel == "extreme" {
					currentBatch = int(float64(batchSize) * (1.0 + 0.5*math.Sin(float64(time.Since(startTime).Nanoseconds())/1e9)))
				}
				lastCheckTime = time.Now()
			}
			
			// 執行更複雜的分支預測測試
			for i := 0; i < currentBatch; i++ {
				x := rng.Intn(10000) // 增加隨機範圍
				pattern := patterns[patternIndex%len(patterns)]
				
				// 驗證前的預期值
				expectedResult := calculateExpectedResult(x, pattern)
				
				// 實際計算
				actualResult, err := performComplexBranchOperation(x, pattern, &testResult)
				
				// 驗證結果
				if err != nil {
					atomic.AddUint64(&errorCount, 1)
					if debug {
						select {
						case errorChan <- fmt.Sprintf("CPU test [PID: %d] CPU %d calculation error: %v", mainPID, cpuID, err):
						default:
						}
					}
				} else if actualResult != expectedResult {
					atomic.AddUint64(&errorCount, 1)
					if debug {
						select {
						case errorChan <- fmt.Sprintf("CPU test [PID: %d] CPU %d verification failed: expected %d, got %d", mainPID, cpuID, expectedResult, actualResult):
						default:
						}
					}
				}
				
				// 增加分支預測混亂度
				if performanceCounter%1000 == 0 {
					patternIndex = (patternIndex + rng.Intn(len(patterns))) % len(patterns)
				}
				
				atomic.AddUint64(&operationCount, 1)
				performanceCounter++
				
				// 防止過度佔用CPU，允許其他goroutine運行
				if performanceCounter%10000 == 0 {
					runtime.Gosched()
				}
			}
			
			// 額外的分支預測挑戰：不規則模式
			if operationCount%50000 == 0 {
				performIrregularBranchPattern(rng, &testResult, &operationCount)
			}
		}
	}

	updatePerfStats(perfStats, operationCount, &testResult, errorCount)
	
	if debug && errorCount > 0 {
		utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] CPU %d completed with %d operations and %d errors", mainPID, cpuID, operationCount, errorCount), true)
	}
}

// 計算預期結果用於驗證
func calculateExpectedResult(x, pattern int) int64 {
	var result int64
	
	switch {
	case x%pattern == 0:
		if x > 0 {
			result = int64(x * x)
		} else {
			result = int64(x)
		}
	case x%(pattern-1) == 0:
		result = int64(x << 2) // 左移2位
	case x%(pattern-2) == 0:
		if pattern-2 > 0 {
			result = int64(x / (pattern - 2))
		} else {
			result = int64(x)
		}
	case x%2 == 0:
		result = int64(x ^ (x >> 1))
	default:
		result = int64((x * 3) + 1)
	}
	
	return result
}

// 執行複雜的分支操作
func performComplexBranchOperation(x, pattern int, testResult *BranchTestResult) (int64, error) {
	var result int64
	
	// 防止除零錯誤
	if pattern == 0 {
		return 0, fmt.Errorf("pattern cannot be zero")
	}
	
	switch {
	case x%pattern == 0:
		if x > 0 {
			result = int64(x * x)
			atomic.AddUint64(&testResult.MultiplyCount, 1)
		} else {
			result = int64(x)
		}
		atomic.AddInt64(&testResult.Sum, result)
		
	case x%(pattern-1) == 0:
		result = int64(x << 2) // 左移2位相當於乘以4
		atomic.AddUint64(&testResult.ShiftCount, 1)
		atomic.AddInt64(&testResult.Sum, result)
		
	case x%(pattern-2) == 0:
		if pattern-2 > 0 {
			result = int64(x / (pattern - 2))
			atomic.AddUint64(&testResult.DivideCount, 1)
		} else {
			result = int64(x)
		}
		atomic.AddInt64(&testResult.Sum, result)
		
	case x%2 == 0:
		result = int64(x ^ (x >> 1))
		atomic.AddInt64(&testResult.XorResult, result)
		atomic.AddUint64(&testResult.ModuloCount, 1)
		
	default:
		// 複雜的分支：產生不可預測的模式
		if x > 5000 {
			result = int64((x * 3) + 1)
		} else if x > 1000 {
			result = int64((x * 5) - 2)
		} else if x > 100 {
			result = int64(x * x / 10)
			if x*x < 0 { // 溢出檢查
				return 0, fmt.Errorf("integer overflow detected")
			}
		} else {
			result = int64((x * 3) + 1)
		}
		atomic.AddInt64(&testResult.Sum, result)
	}
	
	return result, nil
}

// 執行不規則分支模式以增加預測難度
func performIrregularBranchPattern(rng RandomGenerator, testResult *BranchTestResult, operationCount *uint64) {
	// 建立隨機但複雜的分支模式
	for i := 0; i < 1000; i++ {
		x := rng.Intn(1000)
		
		// 故意設計難以預測的分支
		switch {
		case isPrime(x) && x%4 == 1:
			atomic.AddInt64(&testResult.Sum, int64(x*x))
		case !isPrime(x) && x%3 == 0:
			atomic.AddInt64(&testResult.XorResult, int64(x)^0xFF)
		case x > 500 && x%7 != 0:
			atomic.AddInt64(&testResult.Sum, int64(x>>1))
		case x < 100 || (x > 200 && x < 300):
			atomic.AddInt64(&testResult.XorResult, int64(x<<1))
		default:
			atomic.AddInt64(&testResult.Sum, int64(x*3+1))
		}
		
		atomic.AddUint64(operationCount, 1)
	}
}

// 簡單的質數檢查（用於增加分支複雜度）
func isPrime(n int) bool {
	if n < 2 {
		return false
	}
	if n == 2 {
		return true
	}
	if n%2 == 0 {
		return false
	}
	
	for i := 3; i*i <= n; i += 2 {
		if n%i == 0 {
			return false
		}
	}
	return true
}

// 更新性能統計
func updatePerfStats(perfStats *config.PerformanceStats, operationCount uint64, testResult *BranchTestResult, errorCount uint64) {
	perfStats.Lock()
	defer perfStats.Unlock()

	perfStats.CPU.BranchCount += operationCount
}

// runCryptoStressParallel 執行高強度加密壓力測試
func runCryptoStressParallel(mainPID int, stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)
    const blockSize = 2 * 1024 * 1024 // 增加到2MB提升壓力
    baseBatchSize := 20000 // 增加批次大小
    batchSize := adjustBatchSize(baseBatchSize, loadLevel)

    rng := utils.NewRand(int64(cpuID))
    data := make([]byte, blockSize)
    _, err := rng.Read(data)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] random data generation failed on CPU %d: %v", mainPID, cpuID, err), false)
        errorChan <- fmt.Sprintf("Random data generation failed on CPU %d", cpuID)
        return
    }

    // RSA 密鑰對 (提升到4096位元增加運算負荷)
    rsaKey, err := rsa.GenerateKey(rand.Reader, 4096)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] RSA key generation failed on CPU %d: %v", mainPID, cpuID, err), false)
        errorChan <- fmt.Sprintf("RSA key generation failed on CPU %d", cpuID)
        return
    }

    // AES-256 設定
    aesKey := make([]byte, 32)
    _, err = rng.Read(aesKey)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] AES key generation failed on CPU %d: %v", mainPID, cpuID, err), false)
        errorChan <- fmt.Sprintf("AES key generation failed on CPU %d", cpuID)
        return
    }
    aesCipher, err := aes.NewCipher(aesKey)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] AES cipher creation failed on CPU %d: %v", mainPID, cpuID, err), false)
        errorChan <- fmt.Sprintf("AES cipher creation failed on CPU %d", cpuID)
        return
    }
    gcm, err := cipher.NewGCM(aesCipher)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] GCM creation failed on CPU %d: %v", mainPID, cpuID, err), false)
        errorChan <- fmt.Sprintf("GCM creation failed on CPU %d", cpuID)
        return
    }
    nonce := make([]byte, gcm.NonceSize())

    // ECDSA P-384 (比P-256更耗運算資源)
    ecdsaKey, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] ECDSA key generation failed on CPU %d: %v", mainPID, cpuID, err), false)
        errorChan <- fmt.Sprintf("ECDSA key generation failed on CPU %d", cpuID)
        return
    }

    // Ed25519 密鑰對
    ed25519Pub, ed25519Priv, err := ed25519.GenerateKey(rand.Reader)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] Ed25519 key generation failed on CPU %d: %v", mainPID, cpuID, err), false)
        errorChan <- fmt.Sprintf("Ed25519 key generation failed on CPU %d", cpuID)
        return
    }

    // ChaCha20Poly1305 設定
    chachaKey := make([]byte, 32)
    _, err = rng.Read(chachaKey)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] ChaCha20 key generation failed on CPU %d: %v", mainPID, cpuID, err), false)
        errorChan <- fmt.Sprintf("ChaCha20 key generation failed on CPU %d", cpuID)
        return
    }
    chachaCipher, err := chacha20poly1305.New(chachaKey)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] ChaCha20Poly1305 creation failed on CPU %d: %v", mainPID, cpuID, err), false)
        errorChan <- fmt.Sprintf("ChaCha20Poly1305 creation failed on CPU %d", cpuID)
        return
    }

    // 驗證用的預期值
    var expectedSHA256Hash [32]byte
    var expectedSHA512Hash [64]byte
    var expectedBlake2bHash [64]byte
    var errorDetected bool
    validationData := make([]byte, 4096) // 專用於驗證的資料
    copy(validationData, data[:4096])

    for time.Since(startTime) < duration {
        select {
        case <-stop:
            perfStats.Lock()
            perfStats.CPU.CryptoCount += operationCount
            perfStats.Unlock()
            if debug {
                utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] crypto test stopped on CPU %d", mainPID, cpuID), true)
            }
            return
        default:
            var sha256Hash [32]byte
            var sha512Hash [64]byte
            var blake2bHash [64]byte

            for i := 0; i < batchSize && !errorDetected; i++ {
                // 1. 多種雜湊算法並行運算增加負荷
                sha256Hash = sha256.Sum256(data)
                sha512Hash = sha512.Sum512(data)

                blake2b, err := blake2b.New512(nil)
                if err != nil {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] Blake2b creation failed on CPU %d: %v", mainPID, cpuID, err), false)
                    errorChan <- fmt.Sprintf("Blake2b creation failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }
                blake2b.Write(data)
                copy(blake2bHash[:], blake2b.Sum(nil))

                // 2. RSA 簽名與驗證
                rsaHash := sha256.Sum256(data[:2048])
                rsaSignature, err := rsa.SignPKCS1v15(rand.Reader, rsaKey, crypto.SHA256, rsaHash[:])
                if err != nil {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] RSA sign failed on CPU %d: %v", mainPID, cpuID, err), false)
                    errorChan <- fmt.Sprintf("RSA sign failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }
                // RSA 驗證
                err = rsa.VerifyPKCS1v15(&rsaKey.PublicKey, crypto.SHA256, rsaHash[:], rsaSignature)
                if err != nil {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] RSA verify failed on CPU %d: %v", mainPID, cpuID, err), false)
                    errorChan <- fmt.Sprintf("RSA verify failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }

                // 3. AES-GCM 加密與解密
                _, err = rng.Read(nonce)
                if err != nil {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] nonce generation failed on CPU %d: %v", mainPID, cpuID, err), false)
                    errorChan <- fmt.Sprintf("Nonce generation failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }
                ciphertext := gcm.Seal(nil, nonce, data[:2048], nil)
                if len(ciphertext) == 0 {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] AES encryption failed on CPU %d", mainPID, cpuID), false)
                    errorChan <- fmt.Sprintf("AES encryption failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }
                // AES 解密驗證
                plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
                if err != nil || !bytes.Equal(plaintext, data[:2048]) {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] AES decryption verification failed on CPU %d", mainPID, cpuID), false)
                    errorChan <- fmt.Sprintf("AES decryption verification failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }

                // 4. ChaCha20Poly1305 加密與解密
                chachaNonce := make([]byte, chachaCipher.NonceSize())
                _, err = rng.Read(chachaNonce)
                if err != nil {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] ChaCha nonce generation failed on CPU %d: %v", mainPID, cpuID, err), false)
                    errorChan <- fmt.Sprintf("ChaCha nonce generation failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }
                chachaCiphertext := chachaCipher.Seal(nil, chachaNonce, data[:2048], nil)
                chachaPlaintext, err := chachaCipher.Open(nil, chachaNonce, chachaCiphertext, nil)
                if err != nil || !bytes.Equal(chachaPlaintext, data[:2048]) {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] ChaCha20 encryption/decryption failed on CPU %d", mainPID, cpuID), false)
                    errorChan <- fmt.Sprintf("ChaCha20 encryption/decryption failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }

                // 5. ECDSA 簽名與驗證
                ecdsaHash := sha256.Sum256(data[:1024])
                ecdsaR, ecdsaS, err := ecdsa.Sign(rand.Reader, ecdsaKey, ecdsaHash[:])
                if err != nil {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] ECDSA sign failed on CPU %d: %v", mainPID, cpuID, err), false)
                    errorChan <- fmt.Sprintf("ECDSA sign failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }
                // ECDSA 驗證
                if !ecdsa.Verify(&ecdsaKey.PublicKey, ecdsaHash[:], ecdsaR, ecdsaS) {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] ECDSA verify failed on CPU %d", mainPID, cpuID), false)
                    errorChan <- fmt.Sprintf("ECDSA verify failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }

                // 6. Ed25519 簽名與驗證
                ed25519Signature := ed25519.Sign(ed25519Priv, data[:512])
                if !ed25519.Verify(ed25519Pub, data[:512], ed25519Signature) {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] Ed25519 verify failed on CPU %d", mainPID, cpuID), false)
                    errorChan <- fmt.Sprintf("Ed25519 verify failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }

                // 7. PBKDF2 密鑰衍生 (高 CPU 消耗)
                salt := make([]byte, 16)
                _, err = rng.Read(salt)
                if err != nil {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] salt generation failed on CPU %d: %v", mainPID, cpuID, err), false)
                    errorChan <- fmt.Sprintf("Salt generation failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }
                derivedKey := pbkdf2.Key(data[:32], salt, 10000, 32, sha256.New)
                if len(derivedKey) != 32 {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] PBKDF2 key derivation failed on CPU %d", mainPID, cpuID), false)
                    errorChan <- fmt.Sprintf("PBKDF2 key derivation failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }

                // 8. 更新資料以增加變化
                data[i%blockSize] ^= sha256Hash[0]
                data[(i*2)%blockSize] ^= sha512Hash[0]
                data[(i*3)%blockSize] ^= blake2bHash[0]

                operationCount++
            }

            // 驗證機制：使用固定資料進行一致性檢查
            if operationCount == uint64(batchSize) && !errorDetected {
                expectedSHA256Hash = sha256.Sum256(validationData)
                expectedSHA512Hash = sha512.Sum512(validationData)
                blake2bHasher, _ := blake2b.New512(nil)
                blake2bHasher.Write(validationData)
                copy(expectedBlake2bHash[:], blake2bHasher.Sum(nil))
            } else if operationCount > uint64(batchSize) && !errorDetected {
                // 定期驗證固定資料的雜湊值是否一致
                currentSHA256 := sha256.Sum256(validationData)
                currentSHA512 := sha512.Sum512(validationData)
                blake2bHasher, _ := blake2b.New512(nil)
                blake2bHasher.Write(validationData)
                var currentBlake2b [64]byte
                copy(currentBlake2b[:], blake2bHasher.Sum(nil))

                if currentSHA256 != expectedSHA256Hash || currentSHA512 != expectedSHA512Hash || currentBlake2b != expectedBlake2bHash {
                    utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] crypto consistency verification failed on CPU %d", mainPID, cpuID), false)
                    if debug {
                        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] hash consistency check failed on CPU %d", mainPID, cpuID), true)
                    }
                    errorChan <- fmt.Sprintf("Crypto consistency verification failed on CPU %d", cpuID)
                    errorDetected = true
                }
            }

            if errorDetected {
                break
            }
        }
    }

    perfStats.Lock()
    perfStats.CPU.CryptoCount += operationCount
    perfStats.Unlock()

    if debug {
        utils.LogMessage(fmt.Sprintf("CPU test [PID: %d] CPU %d crypto operations completed: %d", mainPID, cpuID, operationCount), true)
    }
}
