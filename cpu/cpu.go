// cpu/cpu.go
package cpu

import (
    "fmt"
    "math"
    "runtime"
    "stress/config"
    "stress/utils"
    "sync"
    "time"
)

// RunCPUStressTests runs CPU stress tests across all available cores
func RunCPUStressTests(wg *sync.WaitGroup, stop chan struct{}, errorChan chan string, config CPUConfig, perfStats *config.PerformanceStats) {
    defer wg.Done()

    // Get NUMA information
    numaInfo, err := utils.GetNUMAInfo()
    if err != nil {
        utils.LogMessage(fmt.Sprintf("Failed to get NUMA info: %v, falling back to single node", err), config.Debug)
    }

    if config.Debug {
        utils.LogMessage(fmt.Sprintf("Detected %d NUMA nodes", numaInfo.NumNodes), config.Debug)
        for i, cpus := range numaInfo.NodeCPUs {
            if len(cpus) > 0 {
                utils.LogMessage(fmt.Sprintf("NUMA node %d has CPUs: %v", i, cpus), config.Debug)
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

    utils.LogMessage(fmt.Sprintf("Running parallel CPU tests on %d cores", len(allCPUs)), config.Debug)

    // Run integer computations
    wg.Add(1)
    go func() {
        defer wg.Done()
        var innerWg sync.WaitGroup

        for _, cpuID := range allCPUs {
            innerWg.Add(1)
            go func(id int) {
                defer innerWg.Done()
                runtime.LockOSThread()
                runIntegerComputationParallel(stop, errorChan, id, perfStats, config.Debug)
            }(cpuID)
        }

        innerWg.Wait()
    }()

    // Run floating-point computations
    wg.Add(1)
    go func() {
        defer wg.Done()
        var innerWg sync.WaitGroup

        for _, cpuID := range allCPUs {
            innerWg.Add(1)
            go func(id int) {
                defer innerWg.Done()
                runtime.LockOSThread()
                runFloatComputationParallel(stop, errorChan, id, perfStats, config.Debug)
            }(cpuID)
        }

        innerWg.Wait()
    }()

    // Run vector computations
    wg.Add(1)
    go func() {
        defer wg.Done()
        var innerWg sync.WaitGroup

        for _, cpuID := range allCPUs {
            innerWg.Add(1)
            go func(id int) {
                defer innerWg.Done()
                runtime.LockOSThread()
                runVectorComputationParallel(stop, errorChan, id, perfStats, config.Debug)
            }(cpuID)
        }

        innerWg.Wait()
    }()
}

// runIntegerComputationParallel performs intensive integer operations
func runIntegerComputationParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool) {
    if debug {
        utils.LogMessage(fmt.Sprintf("Starting integer computation worker on CPU %d", cpuID), debug)
    }

    startTime := time.Now()
    lastUpdateTime := startTime
    operationCount := uint64(0)

    const batchSize = 10000000
    const updateInterval = 2 * time.Second
    primes := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53}

    var expectedResult int64 = 0
    var errorDetected bool = false

    for {
        select {
        case <-stop:
            if debug {
                elapsed := time.Since(startTime)
                opsPerSecond := float64(operationCount) / elapsed.Seconds()
                utils.LogMessage(fmt.Sprintf("Integer computation on CPU %d completed: %d operations (%.2f ops/sec)",
                    cpuID, operationCount, opsPerSecond), debug)
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

                if i == 0 && operationCount == 0 {
                    expectedResult = result
                } else if !errorDetected && result != expectedResult {
                    errorMsg := fmt.Sprintf("Integer computation error on CPU %d: Expected %d, got %d", cpuID, expectedResult, result)
                    errorChan <- errorMsg
                    errorDetected = true
                }

                operationCount++
            }

            now := time.Now()
            if now.Sub(lastUpdateTime) >= updateInterval {
                elapsed := now.Sub(startTime)
                opsPerSecond := float64(operationCount) / elapsed.Seconds()
                const opsPerFlop = 8
                gflopsEquivalent := (opsPerSecond * opsPerFlop) / 1e9

                perfStats.Lock() // 改用導出的方法
                perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflopsEquivalent) / 2
                perfStats.Unlock() // 改用導出的方法

                if debug {
                    utils.LogMessage(fmt.Sprintf("CPU %d integer perf: %.2f GOPS (%.2f GFLOPS equiv)",
                        cpuID, opsPerSecond/1e9, gflopsEquivalent), debug)
                }

                lastUpdateTime = now
            }
        }
    }
}

// runFloatComputationParallel performs intensive floating-point operations
func runFloatComputationParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool) {
    if debug {
        utils.LogMessage(fmt.Sprintf("Starting float computation worker on CPU %d", cpuID), debug)
    }

    startTime := time.Now()
    lastUpdateTime := startTime
    operationCount := uint64(0)

    const batchSize = 5000000
    const updateInterval = 2 * time.Second
    constants := []float64{3.14159, 2.71828, 1.41421, 1.73205, 2.23606, 2.44949, 2.64575}

    var expectedResult float64 = 0
    var errorDetected bool = false
    var epsilon float64 = 1e-10

    for {
        select {
        case <-stop:
            if debug {
                elapsed := time.Since(startTime)
                opsPerSecond := float64(operationCount) / elapsed.Seconds()
                utils.LogMessage(fmt.Sprintf("Float computation on CPU %d completed: %d operations (%.2f ops/sec)",
                    cpuID, operationCount, opsPerSecond), debug)
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

                if i == 0 && operationCount == 0 {
                    expectedResult = result
                } else if !errorDetected && math.Abs(result-expectedResult) > epsilon {
                    errorMsg := fmt.Sprintf("Float computation error on CPU %d: Expected %.10f, got %.10f, diff %.10f",
                        cpuID, expectedResult, result, math.Abs(result-expectedResult))
                    errorChan <- errorMsg
                    errorDetected = true
                }

                operationCount++
            }

            now := time.Now()
            if now.Sub(lastUpdateTime) >= updateInterval {
                elapsed := now.Sub(startTime)
                opsPerSecond := float64(operationCount) / elapsed.Seconds()
                const flopsPerOp = 12
                gflops := (opsPerSecond * flopsPerOp) / 1e9

                perfStats.Lock() // 改用導出的方法
                perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflops) / 2
                perfStats.Unlock() // 改用導出的方法

                if debug {
                    utils.LogMessage(fmt.Sprintf("CPU %d float perf: %.2f GFLOPS", cpuID, gflops), debug)
                }

                lastUpdateTime = now
            }
        }
    }
}

// runVectorComputationParallel performs SIMD-like vector operations
func runVectorComputationParallel(stop <-chan struct{}, errorChan chan<- string, cpuID int, perfStats *config.PerformanceStats, debug bool) {
    if debug {
        utils.LogMessage(fmt.Sprintf("Starting vector computation worker on CPU %d", cpuID), debug)
    }

    startTime := time.Now()
    lastUpdateTime := startTime
    operationCount := uint64(0)

    const vectorSize = 1024
    const batchSize = 1000
    const updateInterval = 2 * time.Second

    vecA := make([]float64, vectorSize)
    vecB := make([]float64, vectorSize)
    vecC := make([]float64, vectorSize)

    for i := 0; i < vectorSize; i++ {
        vecA[i] = float64(i%17) * 0.5
        vecB[i] = float64(i%19) * 0.75
    }

    var checksum float64 = 0
    var expectedChecksum float64 = 0
    var errorDetected bool = false
    var epsilon float64 = 1e-10

    for {
        select {
        case <-stop:
            if debug {
                elapsed := time.Since(startTime)
                opsPerSecond := float64(operationCount) / elapsed.Seconds()
                utils.LogMessage(fmt.Sprintf("Vector computation on CPU %d completed: %d operations (%.2f ops/sec)",
                    cpuID, operationCount, opsPerSecond), debug)
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

                checksum = dotProduct

                if b == 0 && operationCount == 0 {
                    expectedChecksum = checksum
                } else if !errorDetected && math.Abs(checksum-expectedChecksum) > epsilon {
                    errorMsg := fmt.Sprintf("Vector computation error on CPU %d: Expected checksum %.10f, got %.10f",
                        cpuID, expectedChecksum, checksum)
                    errorChan <- errorMsg
                    errorDetected = true
                }

                operationCount++
            }

            now := time.Now()
            if now.Sub(lastUpdateTime) >= updateInterval {
                elapsed := now.Sub(startTime)
                opsPerSecond := float64(operationCount) / elapsed.Seconds()
                flopsPerVectorOp := float64(vectorSize * 5)
                gflops := (opsPerSecond * flopsPerVectorOp) / 1e9

                perfStats.Lock() // 改用導出的方法
                perfStats.CPU.GFLOPS = (perfStats.CPU.GFLOPS + gflops) / 2
                perfStats.Unlock() // 改用導出的方法

                if debug {
                    utils.LogMessage(fmt.Sprintf("CPU %d vector perf: %.2f GFLOPS", cpuID, gflops), debug)
                }

                lastUpdateTime = now
            }
        }
    }
}
