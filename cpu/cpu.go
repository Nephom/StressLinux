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

    "golang.org/x/sys/unix"
)

// RunCPUStressTests runs CPU stress tests across specified cores with target load
func RunCPUStressTests(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, testConfig CPUConfig, perfStats *config.PerformanceStats) {
    defer wg.Done()

    // Get NUMA information
    numaInfo, err := utils.GetNUMAInfo()
    if err != nil {
        utils.LogMessage(fmt.Sprintf("Failed to get NUMA info: %v, falling back to single node", err), testConfig.Debug)
    }

    if testConfig.Debug {
        utils.LogMessage(fmt.Sprintf("Detected %d NUMA nodes", numaInfo.NumNodes), testConfig.Debug)
        for i, cpus := range numaInfo.NodeCPUs {
            if len(cpus) > 0 {
                utils.LogMessage(fmt.Sprintf("NUMA node %d has CPUs: %v", i, cpus), testConfig.Debug)
            }
        }
    }

    // Get total available CPUs across all NUMA nodes
    var allCPUs []int
    for _, cpus := range numaInfo.NodeCPUs {
        allCPUs = append(allCPUs, cpus...)
    }

    if len(allCPUs) == 0 {
        // Fallback if no NUMA info available
        for i := 0; i < runtime.NumCPU(); i++ {
            allCPUs = append(allCPUs, i)
        }
    }

    // Restrict number of cores
    if testConfig.NumCores > 0 && testConfig.NumCores < len(allCPUs) {
        allCPUs = allCPUs[:testConfig.NumCores]
    }

    // Validate load percentage
    loadPercent := testConfig.LoadPercent
    if loadPercent <= 0 || loadPercent > 1 {
        loadPercent = 1.0 // Default to 100% load
    }

    // Initialize CoreGFLOPS map if nil
    perfStats.Lock()
    if perfStats.CPU.CoreGFLOPS == nil {
        perfStats.CPU.CoreGFLOPS = make(map[int]float64)
    }
    perfStats.CPU.NumCores = len(allCPUs)
    perfStats.Unlock()

    utils.LogMessage(fmt.Sprintf("Running CPU tests on %d cores at %.0f%% load", len(allCPUs), loadPercent*100), testConfig.Debug)

    // Run one goroutine per core, handling all test types
    var innerWg sync.WaitGroup
    for _, cpuID := range allCPUs {
        innerWg.Add(1)
        go func(id int) {
            defer innerWg.Done()
            runAllTestsPerCore(stop, errorChan, id, perfStats, testConfig.Debug, loadPercent)
        }(cpuID)
    }
    innerWg.Wait()
}

// runAllTestsPerCore runs all test types sequentially in one goroutine per core
func runAllTestsPerCore(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadPercent float64) {
    if debug {
        utils.LogMessage(fmt.Sprintf("Starting stress worker on CPU %d with %.0f%% load", cpuID, loadPercent*100), debug)
    }

    // Lock to OS thread and set CPU affinity
    runtime.LockOSThread()
    cpuset := unix.CPUSet{}
    cpuset.Set(cpuID)
    err := unix.SchedSetaffinity(0, &cpuset)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("Failed to set CPU affinity for CPU %d: %v (may require root privileges)", cpuID, err), true)
    } else if debug {
        utils.LogMessage(fmt.Sprintf("Successfully set CPU affinity for CPU %d", cpuID), debug)
    }

    // Test weights (approximate proportion of time for each test)
    testFuncs := []struct {
        name   string
        fn     func(<-chan struct{}, chan<- string, int, *config.PerformanceStats, bool, float64, time.Duration)
        weight float64
    }{
        {"integer", runIntegerComputationParallel, 0.2},
        {"float", runFloatComputationParallel, 0.2},
        {"vector", runVectorComputationParallel, 0.2},
        {"cache", runCacheStressParallel, 0.15},
        {"branch", runBranchPredictionParallel, 0.15},
        {"goroutine", runGoroutineStressParallel, 0.05},
        {"crypto", runCryptoStressParallel, 0.05},
    }

    cycleDuration := 100 * time.Millisecond // 縮短到 100ms

    for {
        select {
        case <-stop:
            if debug {
                utils.LogMessage(fmt.Sprintf("Stress tests on CPU %d completed", cpuID), debug)
            }
            runtime.UnlockOSThread()
            return
        default:
            cycleStart := time.Now()

            // Run each test for its weighted duration
            for _, test := range testFuncs {
                testBudget := time.Duration(float64(cycleDuration) * test.weight)
                test.fn(stop, errorChan, cpuID, perfStats, debug, loadPercent, testBudget)
            }

            // Ensure cycle alignment
            elapsed := time.Since(cycleStart)
            if elapsed < cycleDuration {
                time.Sleep(cycleDuration - elapsed)
            }
        }
    }
}

// runIntegerComputationParallel performs intensive integer operations
func runIntegerComputationParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadPercent float64, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)

    const batchSize = 1000000
    primes := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53}

    var expectedResult int64
    var errorDetected bool

    for time.Since(startTime) < duration {
        select {
        case <-stop:
            return
        default:
            workStart := time.Now()
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

            // Load control within test
            workDuration := time.Since(workStart)
            if loadPercent < 1.0 && loadPercent > 0.0 {
                sleepDuration := time.Duration(float64(workDuration) * (1/loadPercent - 1))
                if sleepDuration > 0 {
                    time.Sleep(sleepDuration)
                }
            }

            elapsed := time.Since(startTime)
            if elapsed > 500*time.Millisecond {
                opsPerSecond := float64(operationCount) / elapsed.Seconds()
                const opsPerFlop = 8
                gflopsEquivalent := (opsPerSecond * opsPerFlop) / 1e9

                perfStats.Lock()
                perfStats.CPU.IntegerOPS = (perfStats.CPU.IntegerOPS + opsPerSecond/1e9) / 2
                perfStats.CPU.CoreGFLOPS[cpuID] = (perfStats.CPU.CoreGFLOPS[cpuID] + gflopsEquivalent) / 2
                perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflopsEquivalent) / 2
                perfStats.Unlock()

                if debug {
                    utils.LogMessage(fmt.Sprintf("CPU %d integer perf: %.2f GOPS (%.2f GFLOPS equiv)", cpuID, opsPerSecond/1e9, gflopsEquivalent), debug)
                }
            }
        }
    }
}

// runFloatComputationParallel performs intensive floating-point operations
func runFloatComputationParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadPercent float64, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)

    const batchSize = 500000
    constants := []float64{3.14159, 2.71828, 1.41421, 1.73205, 2.23606, 2.44949, 2.64575}

    var expectedResult float64
    var errorDetected bool
    var epsilon float64 = 1e-10

    for time.Since(startTime) < duration {
        select {
        case <-stop:
            return
        default:
            workStart := time.Now()
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

            workDuration := time.Since(workStart)
            if loadPercent < 1.0 && loadPercent > 0.0 {
                sleepDuration := time.Duration(float64(workDuration) * (1/loadPercent - 1))
                if sleepDuration > 0 {
                    time.Sleep(sleepDuration)
                }
            }

            elapsed := time.Since(startTime)
            if elapsed > 500*time.Millisecond {
                opsPerSecond := float64(operationCount) / elapsed.Seconds()
                const flopsPerOp = 12
                gflops := (opsPerSecond * flopsPerOp) / 1e9

                perfStats.Lock()
                perfStats.CPU.FloatOPS = (perfStats.CPU.FloatOPS + opsPerSecond/1e9) / 2
                perfStats.CPU.CoreGFLOPS[cpuID] = (perfStats.CPU.CoreGFLOPS[cpuID] + gflops) / 2
                perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflops) / 2
                perfStats.Unlock()

                if debug {
                    utils.LogMessage(fmt.Sprintf("CPU %d float perf: %.2f GFLOPS", cpuID, gflops), debug)
                }
            }
        }
    }
}

// runVectorComputationParallel performs SIMD-like vector operations
func runVectorComputationParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadPercent float64, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)

    const vectorSize = 1024
    const batchSize = 1000

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
            workStart := time.Now()
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

            workDuration := time.Since(workStart)
            if loadPercent < 1.0 && loadPercent > 0.0 {
                sleepDuration := time.Duration(float64(workDuration) * (1/loadPercent - 1))
                if sleepDuration > 0 {
                    time.Sleep(sleepDuration)
                }
            }

            elapsed := time.Since(startTime)
            if elapsed > 500*time.Millisecond {
                opsPerSecond := float64(operationCount) / elapsed.Seconds()
                flopsPerVectorOp := float64(vectorSize * 5)
                gflops := (opsPerSecond * flopsPerVectorOp) / 1e9

                perfStats.Lock()
                perfStats.CPU.VectorOPS = (perfStats.CPU.VectorOPS + opsPerSecond/1e9) / 2
                perfStats.CPU.CoreGFLOPS[cpuID] = (perfStats.CPU.CoreGFLOPS[cpuID] + gflops) / 2
                perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflops) / 2
                perfStats.Unlock()

                if debug {
                    utils.LogMessage(fmt.Sprintf("CPU %d vector perf: %.2f GFLOPS", cpuID, gflops), debug)
                }
            }
        }
    }
}

// runCacheStressParallel performs cache stress testing
func runCacheStressParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadPercent float64, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)

    const arraySize = 32 * 1024 * 1024
    const batchSize = 1000000

    data := make([]int64, arraySize)
    for i := range data {
        data[i] = int64(i)
    }

    rng := utils.NewRand(int64(cpuID))
    indices := make([]int, batchSize)
    for i := 0; i < batchSize; i++ {
        indices[i] = rng.Intn(arraySize)
    }

    var expectedSum int64
    var errorDetected bool

    for time.Since(startTime) < duration {
        select {
        case <-stop:
            return
        default:
            workStart := time.Now()
            var sum int64
            for _, idx := range indices {
                sum += data[idx]
                data[idx] ^= sum
            }

            operationCount += uint64(batchSize)

            if operationCount == uint64(batchSize) {
                expectedSum = sum
            } else if !errorDetected && sum != expectedSum {
                errorChan <- fmt.Sprintf("Cache stress error on CPU %d: Expected sum %d, got %d", cpuID, expectedSum, sum)
                errorDetected = true
            }

            workDuration := time.Since(workStart)
            if loadPercent < 1.0 && loadPercent > 0.0 {
                sleepDuration := time.Duration(float64(workDuration) * (1/loadPercent - 1))
                if sleepDuration > 0 {
                    time.Sleep(sleepDuration)
                }
            }

            elapsed := time.Since(startTime)
            if elapsed > 500*time.Millisecond {
                opsPerSecond := float64(operationCount) / elapsed.Seconds()
                const opsPerFlop = 2
                gflopsEquivalent := (opsPerSecond * opsPerFlop) / 1e9

                perfStats.Lock()
                perfStats.CPU.CoreGFLOPS[cpuID] = (perfStats.CPU.CoreGFLOPS[cpuID] + gflopsEquivalent) / 2
                perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflopsEquivalent) / 2
                perfStats.Unlock()

                if debug {
                    utils.LogMessage(fmt.Sprintf("CPU %d cache perf: %.2f GOPS (%.2f GFLOPS equiv)", cpuID, opsPerSecond/1e9, gflopsEquivalent), debug)
                }
            }
        }
    }
}

// runCryptoStressParallel performs cryptographic stress testing
func runCryptoStressParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadPercent float64, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)

    const blockSize = 1024 * 1024
    const batchSize = 100

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
            workStart := time.Now()
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

            workDuration := time.Since(workStart)
            if loadPercent < 1.0 && loadPercent > 0.0 {
                sleepDuration := time.Duration(float64(workDuration) * (1/loadPercent - 1))
                if sleepDuration > 0 {
                    time.Sleep(sleepDuration)
                }
            }

            elapsed := time.Since(startTime)
            if elapsed > 500*time.Millisecond {
                opsPerSecond := float64(operationCount) / elapsed.Seconds()
                const opsPerFlop = 5000
                gflopsEquivalent := (opsPerSecond * opsPerFlop) / 1e9

                perfStats.Lock()
                perfStats.CPU.CoreGFLOPS[cpuID] = (perfStats.CPU.CoreGFLOPS[cpuID] + gflopsEquivalent) / 2
                perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflopsEquivalent) / 2
                perfStats.Unlock()

                if debug {
                    utils.LogMessage(fmt.Sprintf("CPU %d crypto perf: %.2f GOPS (%.2f GFLOPS equiv)", cpuID, opsPerSecond/1e9, gflopsEquivalent), debug)
                }
            }
        }
    }
}

// runBranchPredictionParallel performs branch prediction stress testing
func runBranchPredictionParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadPercent float64, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)

    const batchSize = 1000000
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
                _ = result // 避免未使用警告
            }

            operationCount += uint64(batchSize)

            elapsed := time.Since(startTime)
            if elapsed > 500*time.Millisecond {
                opsPerSecond := float64(operationCount) / elapsed.Seconds()
                const opsPerFlop = 4
                gflopsEquivalent := (opsPerSecond * opsPerFlop) / 1e9

                perfStats.Lock()
                perfStats.CPU.CoreGFLOPS[cpuID] = (perfStats.CPU.CoreGFLOPS[cpuID] + gflopsEquivalent) / 2
                perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflopsEquivalent) / 2
                perfStats.Unlock()

                if debug {
                    utils.LogMessage(fmt.Sprintf("CPU %d branch perf: %.2f GOPS (%.2f GFLOPS equiv)", cpuID, opsPerSecond/1e9, gflopsEquivalent), debug)
                }
            }
        }
    }
}

// runGoroutineStressParallel performs goroutine blocking/waking stress testing
func runGoroutineStressParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool, loadPercent float64, duration time.Duration) {
    startTime := time.Now()
    operationCount := uint64(0)

    const numGoroutines = 100 // 減少到 100
    const batchSize = 10000

    ch := make(chan int, numGoroutines)
    var wg sync.WaitGroup

    for i := 0; i < numGoroutines; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for {
                select {
                case <-stop:
                    return
                case n := <-ch:
                    _ = n
                }
            }
        }()
    }

    for time.Since(startTime) < duration {
        select {
        case <-stop:
            close(ch)
            wg.Wait()
            return
        default:
            for i := 0; i < batchSize; i++ {
                ch <- i
            }

            operationCount += uint64(batchSize)

            elapsed := time.Since(startTime)
            if elapsed > 500*time.Millisecond {
                opsPerSecond := float64(operationCount) / elapsed.Seconds()
                const opsPerFlop = 1
                gflopsEquivalent := (opsPerSecond * opsPerFlop) / 1e9

                perfStats.Lock()
                perfStats.CPU.CoreGFLOPS[cpuID] = (perfStats.CPU.CoreGFLOPS[cpuID] + gflopsEquivalent) / 2
                perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflopsEquivalent) / 2
                perfStats.Unlock()

                if debug {
                    utils.LogMessage(fmt.Sprintf("CPU %d goroutine perf: %.2f GOPS (%.2f GFLOPS equiv)", cpuID, opsPerSecond/1e9, gflopsEquivalent), debug)
                }
            }
        }
    }

    close(ch)
    wg.Wait()
}
