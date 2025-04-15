package cpu

import (
    "crypto"
    "crypto/aes"
    "crypto/cipher"
    "crypto/ecdsa"
    "crypto/elliptic"
    "crypto/rand"
    "crypto/rsa"
    "crypto/sha256"
    "fmt"
    "math"
    "runtime"
    "stress/config"
    "stress/utils"
    "sync"
    "time"
    "strings"

    "golang.org/x/sys/unix"
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
    // Ensure batchSize is at least 100 to maintain test accuracy
    if adjusted < 100 {
        adjusted = 100
    }
    return adjusted
}

// RunCPUStressTests runs CPU stress tests across specified cores
func RunCPUStressTests(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, testConfig CPUConfig, perfStats *config.PerformanceStats) {
    defer wg.Done()

    // Set GOMAXPROCS to limit maximum parallelism
    if testConfig.NumCores > 0 {
        oldGOMAXPROCS := runtime.GOMAXPROCS(testConfig.NumCores)
        if testConfig.Debug {
            utils.LogMessage(fmt.Sprintf("Set GOMAXPROCS to %d (was %d)", testConfig.NumCores, oldGOMAXPROCS), testConfig.Debug)
        }
    }

    // Get cache information
    cacheInfo, err := utils.GetCacheInfo()
    if err != nil {
        utils.LogMessage(fmt.Sprintf("Failed to get cache info: %v, using defaults", err), testConfig.Debug)
    }

    utils.LogMessage(fmt.Sprintf("L1 Cache Size: %.2f KB", float64(cacheInfo.L1Size)/1024), testConfig.Debug)
    utils.LogMessage(fmt.Sprintf("L2 Cache Size: %.2f KB", float64(cacheInfo.L2Size)/1024), testConfig.Debug)
    if cacheInfo.L3Size > 0 {
        utils.LogMessage(fmt.Sprintf("L3 Cache Size: %.2f MB", float64(cacheInfo.L3Size)/(1024*1024)), testConfig.Debug)
    } else {
        utils.LogMessage("L3 Cache: Not present", testConfig.Debug)
    }

    perfStats.Lock()
    if perfStats.CPU.CoreGFLOPS == nil {
        perfStats.CPU.CoreGFLOPS = make(map[int]float64)
    }
    perfStats.CPU.CacheInfo = cacheInfo
    perfStats.Unlock()

    // Determine the list of CPU cores to use
    allCPUs := testConfig.CPUList
    if len(allCPUs) == 0 {
        allCPUs = make([]int, testConfig.NumCores)
        for i := 0; i < testConfig.NumCores; i++ {
            allCPUs[i] = i
        }
    }

    // Ensure NumCores does not exceed CPUList length
    if testConfig.NumCores > len(allCPUs) {
        testConfig.NumCores = len(allCPUs)
        if testConfig.Debug {
            utils.LogMessage(fmt.Sprintf("Adjusted NumCores to %d to match CPU list length", testConfig.NumCores), testConfig.Debug)
        }
    } else if testConfig.NumCores > 0 {
        allCPUs = allCPUs[:testConfig.NumCores]
    }

    perfStats.Lock()
    perfStats.CPU.NumCores = len(allCPUs)
    perfStats.Unlock()

    utils.LogMessage(fmt.Sprintf("Running CPU tests on %d cores (CPUs: %v)", len(allCPUs), allCPUs), testConfig.Debug)

    var innerWg sync.WaitGroup
    for _, cpuID := range allCPUs {
        innerWg.Add(1)
        go func(id int) {
            defer innerWg.Done()
            runAllTestsPerCore(stop, errorChan, id, perfStats, testConfig.Debug, testConfig.LoadLevel)
        }(cpuID)
    }
    innerWg.Wait()
}

// runAllTestsPerCore runs all test types sequentially in one goroutine per core
func runAllTestsPerCore(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string) {
    if debug {
        utils.LogMessage(fmt.Sprintf("Starting stress worker on CPU %d", cpuID), debug)
    }

    runtime.LockOSThread()
    cpuset := unix.CPUSet{}
    cpuset.Set(cpuID)
    err := unix.SchedSetaffinity(0, &cpuset)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("Failed to set CPU affinity for CPU %d: %v (may require root privileges)", cpuID, err), true)
    } else if debug {
        utils.LogMessage(fmt.Sprintf("Successfully set CPU affinity for CPU %d", cpuID), debug)
    }

    if debug {
        var actualSet unix.CPUSet
        err := unix.SchedGetaffinity(0, &actualSet)
        if err != nil {
            utils.LogMessage(fmt.Sprintf("Failed to get CPU affinity for CPU %d: %v", cpuID, err), debug)
        } else {
            utils.LogMessage(fmt.Sprintf("Actual CPU affinity for CPU %d: %v", cpuID, actualSet), debug)
        }
    }

    // 定義所有可用的測試項目及其參考權重
    allTests := map[string]struct {
        fn     func(<-chan struct{}, chan<- string, int, *config.PerformanceStats, bool, string, time.Duration)
        weight float64
    }{
        "integer": {runIntegerComputationParallel, 0.2},
        "float":   {runFloatComputationParallel, 0.2},
        "vector":  {runVectorComputationParallel, 0.2},
        "cache":   {runCacheStressParallel, 0.1},
        "branch":  {runBranchPredictionParallel, 0.15},
        "crypto":  {runCryptoStressParallel, 0.15},
    }

    // 定義每個 loadLevel 的測試項目和權重
    type testConfig struct {
        name   string
        weight float64
    }

    loadLevelConfigs := map[string][]testConfig{
        "high": {
            {"integer", 0.1},
            {"float", 0.2},
            {"vector", 0.2},
            {"cache", 0.2},
            {"branch", 0.15},
            {"crypto", 0.15},
        },
        "low": {
            {"integer", 0.2},
            {"float", 0.3},
        },
        "default": {
            {"float", 0.2},
            {"vector", 0.2},
            {"cache", 0.2},
            {"branch", 0.2},
            {"crypto", 0.2},
        },
    }

    // 選擇測試項目
    var selectedTests []struct {
        name   string
        fn     func(<-chan struct{}, chan<- string, int, *config.PerformanceStats, bool, string, time.Duration)
        weight float64
    }

    // 根據 loadLevel 選擇配置
    configKey := strings.ToLower(loadLevel)
    switch configKey {
    case "high", "2":
        configKey = "high"
    case "low", "1":
        configKey = "low"
    case "default", "0", "":
        configKey = "default"
    default:
        utils.LogMessage(fmt.Sprintf("Invalid CPU load level: %s, using Default", loadLevel), true)
        configKey = "default"
    }

    // 從配置中構建 selectedTests
    tests, exists := loadLevelConfigs[configKey]
    if !exists {
        utils.LogMessage(fmt.Sprintf("No config for load level %s, using Default", configKey), true)
        tests = loadLevelConfigs["default"]
    }

    // 計算權重總和
    totalWeight := 0.0
    for _, test := range tests {
        totalWeight += test.weight
    }

    // 僅對 high 正規化權重
    normalize := configKey == "high" && totalWeight != 0 && totalWeight != 1.0

    for _, test := range tests {
        testFn, ok := allTests[test.name]
        if !ok {
            utils.LogMessage(fmt.Sprintf("Test %s not found in allTests, skipping", test.name), true)
            continue
        }
        weight := test.weight
        if normalize {
            weight = test.weight / totalWeight // 僅 high 正規化
        }
        selectedTests = append(selectedTests, struct {
            name   string
            fn     func(<-chan struct{}, chan<- string, int, *config.PerformanceStats, bool, string, time.Duration)
            weight float64
        }{test.name, testFn.fn, weight})
    }

    if len(selectedTests) == 0 {
        utils.LogMessage(fmt.Sprintf("No valid tests selected for CPU %d, exiting", cpuID), true)
        runtime.UnlockOSThread()
        return
    }

    // 記錄選定的測試、權重和預計循環時間
    cycleDuration := 50 * time.Millisecond
    if debug {
        usedTime := time.Duration(float64(cycleDuration) * totalWeight)
        if normalize {
            usedTime = cycleDuration // high 正規化後用滿
        }
        utils.LogMessage(fmt.Sprintf("CPU %d: Load level %s, running %d tests: %v, expected cycle time: %v", cpuID, configKey, len(selectedTests), getTestNames(selectedTests), usedTime), debug)
    }

    for {
        select {
        case <-stop:
            if debug {
                utils.LogMessage(fmt.Sprintf("Stress tests on CPU %d completed", cpuID), debug)
            }
            runtime.UnlockOSThread()
            return
        default:
            start := time.Now()
            for _, test := range selectedTests {
                testBudget := time.Duration(float64(cycleDuration) * test.weight)
                test.fn(stop, errorChan, cpuID, perfStats, debug, loadLevel, testBudget)
            }
            // 為 low 和 default 添加閒置時間
            if configKey != "high" && totalWeight < 1.0 {
                idleTime := time.Duration(float64(cycleDuration) * (1.0 - totalWeight))
                time.Sleep(idleTime)
            }
            if debug {
                utils.LogMessage(fmt.Sprintf("CPU %d: Actual cycle time: %v", cpuID, time.Since(start)), debug)
            }
        }
    }
}

// 輔助函數：取得測試名稱和權重
func getTestNames(tests []struct {
    name   string
    fn     func(<-chan struct{}, chan<- string, int, *config.PerformanceStats, bool, string, time.Duration)
    weight float64
}) []string {
    names := make([]string, len(tests))
    for i, test := range tests {
        names[i] = fmt.Sprintf("%s(%.2f)", test.name, test.weight)
    }
    return names
}

// runIntegerComputationParallel performs intensive integer operations
func runIntegerComputationParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string, duration time.Duration) {
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
            return
        default:
            for i := 0; i < batchSize; i++ {
                var result int64 = 1
                for _, prime := range primes {
                    result = (result * int64(prime)) % (1<<31 - 1)
                    result = result ^ (result >> 3)
                    result = result + int64(prime)
                }

                if i == 0 && operationCount == 0 {
                    expectedResult = result
                } else if !errorDetected && result != expectedResult {
                    errorChan <- fmt.Sprintf("Integer computation error on CPU %d: Expected %d, got %d", cpuID, expectedResult, result)
                    errorDetected = true
                }
                operationCount++
            }
        }
    }

    elapsed := time.Since(startTime)
    if elapsed > 0 {
        opsPerSecond := float64(operationCount) / elapsed.Seconds()
        const opsPerFlop = 8
        gflopsEquivalent := (opsPerSecond * opsPerFlop) / 1e9

        perfStats.Lock()
        perfStats.CPU.IntegerOPS = (perfStats.CPU.IntegerOPS + opsPerSecond/1e9) / 2
        perfStats.CPU.CoreGFLOPS[cpuID] = (perfStats.CPU.CoreGFLOPS[cpuID] + gflopsEquivalent) / 2
        perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflopsEquivalent) / 2
        perfStats.CPU.IntegerCount += operationCount
        perfStats.Unlock()

        if debug {
            utils.LogMessage(fmt.Sprintf("CPU %d integer perf: %.2f GOPS (%.2f GFLOPS equiv), operations: %d", cpuID, opsPerSecond/1e9, gflopsEquivalent, operationCount), debug)
        }
    }
}

// runFloatComputationParallel performs intensive floating-point operations
func runFloatComputationParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string, duration time.Duration) {
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
            return
        default:
            for i := 0; i < batchSize; i++ {
                var result float64 = 1.0
                for _, c := range constants {
                    result = result * math.Sin(c) + math.Cos(result)
                    result = math.Sqrt(math.Abs(result)) + math.Log(1 + math.Abs(result))
                    result = math.Pow(result, 0.5) * c
                }

                if i == 0 && operationCount == 0 {
                    expectedResult = result
                } else if !errorDetected && math.Abs(result-expectedResult) > epsilon {
                    errorChan <- fmt.Sprintf("Float computation error on CPU %d: Expected %.10f, got %.10f", cpuID, expectedResult, result)
                    errorDetected = true
                }
                operationCount++
            }
        }
    }

    elapsed := time.Since(startTime)
    if elapsed > 0 {
        opsPerSecond := float64(operationCount) / elapsed.Seconds()
        const flopsPerOp = 12
        gflops := (opsPerSecond * flopsPerOp) / 1e9

        perfStats.Lock()
        perfStats.CPU.FloatOPS = (perfStats.CPU.FloatOPS + opsPerSecond/1e9) / 2
        perfStats.CPU.CoreGFLOPS[cpuID] = (perfStats.CPU.CoreGFLOPS[cpuID] + gflops) / 2
        perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflops) / 2
        perfStats.CPU.FloatCount += operationCount
        perfStats.Unlock()

        if debug {
            utils.LogMessage(fmt.Sprintf("CPU %d float perf: %.2f GFLOPS, operations: %d", cpuID, gflops, operationCount), debug)
        }
    }
}

// runVectorComputationParallel performs SIMD-like vector operations
func runVectorComputationParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string, duration time.Duration) {
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
                if b == 0 && operationCount == 0 {
                    expectedChecksum = checksum
                } else if !errorDetected && math.Abs(checksum-expectedChecksum) > epsilon {
                    errorChan <- fmt.Sprintf("Vector computation error on CPU %d: Expected checksum %.10f, got %.10f", cpuID, expectedChecksum, checksum)
                    errorDetected = true
                }
                operationCount++
            }
        }
    }

    elapsed := time.Since(startTime)
    if elapsed > 0 {
        opsPerSecond := float64(operationCount) / elapsed.Seconds()
        flopsPerVectorOp := float64(vectorSize * 5)
        gflops := (opsPerSecond * flopsPerVectorOp) / 1e9

        perfStats.Lock()
        perfStats.CPU.VectorOPS = (perfStats.CPU.VectorOPS + opsPerSecond/1e9) / 2
        perfStats.CPU.CoreGFLOPS[cpuID] = (perfStats.CPU.CoreGFLOPS[cpuID] + gflops) / 2
        perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflops) / 2
        perfStats.CPU.VectorCount += operationCount
        perfStats.Unlock()

        if debug {
            utils.LogMessage(fmt.Sprintf("CPU %d vector perf: %.2f GFLOPS, operations: %d", cpuID, gflops, operationCount), debug)
        }
    }
}

// runCacheStressParallel performs cache stress testing targeting a specific cache level
func runCacheStressParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)

    baseBatchSize := 1000000
    batchSize := adjustBatchSize(baseBatchSize, loadLevel)
    sampleStart := time.Now()
    perfStats.Lock()
    cacheInfo := perfStats.CPU.CacheInfo
    perfStats.Unlock()

    var targetCacheSize int64
    var targetCacheLevel string
    if cacheInfo.L3Size > 0 {
        targetCacheSize = cacheInfo.L3Size
        targetCacheLevel = "L3"
    } else {
        targetCacheSize = cacheInfo.L2Size
        targetCacheLevel = "L2"
    }

    arraySize := int((float64(targetCacheSize) * 0.75) / 8)
    if arraySize < 1024 {
        arraySize = 1024
    }
    dataSizeBytes := int64(arraySize) * 8

    if debug {
        sizeMB := float64(dataSizeBytes) / (1024 * 1024)
        utils.LogMessage(fmt.Sprintf("Stressing %s Cache on CPU %d with array size: %.2f MB", targetCacheLevel, cpuID, sizeMB), debug)
    }

    data := make([]int64, arraySize)
    for i := range data {
        data[i] = int64(i)
    }

    rng := utils.NewRand(int64(cpuID))
    const pageSize = 4096 / 8
    pageCount := arraySize / pageSize
    if pageCount < 1 {
        pageCount = 1
    }

    var sum int64
    for i := 0; i < 1000; i++ {
        pageIdx := rng.Intn(pageCount)
        startIdx := pageIdx * pageSize
        for j := 0; j < pageSize; j++ {
            idx := startIdx + j
            if idx >= arraySize {
                break
            }
            sum += data[idx]
            data[idx] ^= sum
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

    var expectedSum int64
    var errorDetected bool

    for time.Since(startTime) < duration {
        select {
        case <-stop:
            return
        default:
            for i := 0; i < batchSize; i++ {
                pageIdx := rng.Intn(pageCount)
                startIdx := pageIdx * pageSize
                for j := 0; j < pageSize; j++ {
                    idx := startIdx + j
                    if idx >= arraySize {
                        break
                    }
                    sum += data[idx]
                    for k := 0; k < 10; k++ {
                        sum ^= data[idx]
                        sum += int64(math.Sqrt(float64(sum)))
                    }
                    data[idx] = sum
                }
            }

            operationCount += uint64(batchSize)

            if operationCount == uint64(batchSize) {
                expectedSum = sum
            } else if !errorDetected && sum != expectedSum {
                errorChan <- fmt.Sprintf("Cache stress error on CPU %d: Expected sum %d, got %d", cpuID, expectedSum, sum)
                errorDetected = true
            }
        }
    }

    elapsed := time.Since(startTime)
    if elapsed > 0 {
        opsPerSecond := float64(operationCount) / elapsed.Seconds()
        const opsPerFlop = 2
        gflopsEquivalent := (opsPerSecond * opsPerFlop) / 1e9

        perfStats.Lock()
        perfStats.CPU.CoreGFLOPS[cpuID] = (perfStats.CPU.CoreGFLOPS[cpuID] + gflopsEquivalent) / 2
        perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflopsEquivalent) / 2
        perfStats.CPU.CacheCount += operationCount
        perfStats.Unlock()

        if debug {
            utils.LogMessage(fmt.Sprintf("CPU %d cache perf: %.2f GOPS (%.2f GFLOPS equiv), operations: %d", cpuID, opsPerSecond/1e9, gflopsEquivalent, operationCount), debug)
        }
    }
}

// runBranchPredictionParallel performs branch prediction stress testing
func runBranchPredictionParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)

    baseBatchSize := 1000000
    batchSize := adjustBatchSize(baseBatchSize, loadLevel)
    rng := utils.NewRand(int64(cpuID))

    for time.Since(startTime) < duration {
        select {
        case <-stop:
            return
        default:
            for i := 0; i < batchSize; i++ {
                x := rng.Intn(100)
                var result int64
                if x%7 == 0 {
                    result += int64(x * x)
                } else if x%5 == 0 {
                    result += int64(x * 2)
                } else if x%3 == 0 {
                    result -= int64(x)
                } else {
                    result ^= int64(x)
                }
                _ = result
            }

            operationCount += uint64(batchSize)
        }
    }

    elapsed := time.Since(startTime)
    if elapsed > 0 {
        opsPerSecond := float64(operationCount) / elapsed.Seconds()
        const opsPerFlop = 4
        gflopsEquivalent := (opsPerSecond * opsPerFlop) / 1e9

        perfStats.Lock()
        perfStats.CPU.CoreGFLOPS[cpuID] = (perfStats.CPU.CoreGFLOPS[cpuID] + gflopsEquivalent) / 2
        perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflopsEquivalent) / 2
        perfStats.CPU.BranchCount += operationCount
        perfStats.Unlock()

        if debug {
            utils.LogMessage(fmt.Sprintf("CPU %d branch perf: %.2f GOPS (%.2f GFLOPS equiv), operations: %d", cpuID, opsPerSecond/1e9, gflopsEquivalent, operationCount), debug)
        }
    }
}

// runCryptoStressParallel performs cryptographic stress testing
func runCryptoStressParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadLevel string, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)

    const blockSize = 1024 * 1024
    baseBatchSize := 10000
    batchSize := adjustBatchSize(baseBatchSize, loadLevel)

    rng := utils.NewRand(int64(cpuID))
    data := make([]byte, blockSize)
    _, err := rng.Read(data)
    if err != nil {
        errorChan <- fmt.Sprintf("Random data generation failed on CPU %d: %v", cpuID, err)
        return
    }

    rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
    if err != nil {
        errorChan <- fmt.Sprintf("RSA key generation failed on CPU %d: %v", cpuID, err)
        return
    }

    aesKey := make([]byte, 32)
    _, err = rng.Read(aesKey)
    if err != nil {
        errorChan <- fmt.Sprintf("AES key generation failed on CPU %d: %v", cpuID, err)
        return
    }
    aesCipher, err := aes.NewCipher(aesKey)
    if err != nil {
        errorChan <- fmt.Sprintf("AES cipher creation failed on CPU %d: %v", cpuID, err)
        return
    }
    gcm, err := cipher.NewGCM(aesCipher)
    if err != nil {
        errorChan <- fmt.Sprintf("GCM creation failed on CPU %d: %v", cpuID, err)
        return
    }
    nonce := make([]byte, gcm.NonceSize())
    _, err = rng.Read(nonce)
    if err != nil {
        errorChan <- fmt.Sprintf("Nonce generation failed on CPU %d: %v", cpuID, err)
        return
    }

    ecdsaKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
    if err != nil {
        errorChan <- fmt.Sprintf("ECDSA key generation failed on CPU %d: %v", cpuID, err)
        return
    }

    var expectedHash [32]byte
    var errorDetected bool

    for time.Since(startTime) < duration {
        select {
        case <-stop:
            return
        default:
            var hash [32]byte
            for i := 0; i < batchSize; i++ {
                hash = sha256.Sum256(data)
                rsaHash := sha256.Sum256(data)
                _, err := rsa.SignPKCS1v15(rand.Reader, rsaKey, crypto.SHA256, rsaHash[:])
                if err != nil {
                    errorChan <- fmt.Sprintf("RSA sign failed on CPU %d: %v", cpuID, err)
                    errorDetected = true
                    break
                }

                ciphertext := gcm.Seal(nil, nonce, data[:1024], nil)
                if len(ciphertext) == 0 {
                    errorChan <- fmt.Sprintf("AES encryption failed on CPU %d", cpuID)
                    errorDetected = true
                    break
                }

                ecdsaHash := sha256.Sum256(data)
                _, _, err = ecdsa.Sign(rand.Reader, ecdsaKey, ecdsaHash[:])
                if err != nil {
                    errorChan <- fmt.Sprintf("ECDSA sign failed on CPU %d: %v", cpuID, err)
                    errorDetected = true
                    break
                }

                data[0] = hash[0]
            }

            operationCount += uint64(batchSize)

            if operationCount == uint64(batchSize) {
                expectedHash = hash
            } else if !errorDetected && hash != expectedHash {
                errorChan <- fmt.Sprintf("Crypto stress error on CPU %d: SHA-256 hash mismatch", cpuID)
                errorDetected = true
            }
        }
    }

    elapsed := time.Since(startTime)
    if elapsed > 0 {
        opsPerSecond := float64(operationCount) / elapsed.Seconds()
        const opsPerFlop = 5000
        gflopsEquivalent := (opsPerSecond * opsPerFlop) / 1e9

        perfStats.Lock()
        perfStats.CPU.CoreGFLOPS[cpuID] = (perfStats.CPU.CoreGFLOPS[cpuID] + gflopsEquivalent) / 2
        perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflopsEquivalent) / 2
        perfStats.CPU.CryptoCount += operationCount
        perfStats.Unlock()

        if debug {
            utils.LogMessage(fmt.Sprintf("CPU %d crypto perf: %.2f GOPS (%.2f GFLOPS equiv), operations: %d", cpuID, opsPerSecond/1e9, gflopsEquivalent, operationCount), debug)
        }
    }
}
