package main

import (
    "flag"
    "fmt"
    "os"
    "runtime"
    "strings"
    "sync"
    "time"
    "math/rand"

    cfg "stress/config"
    "stress/cpu"
    "stress/disk"
    "stress/memory"
    "stress/rawdisk"
    "stress/utils"
    "stress/systeminfo"
)

type stringSliceFlag []string

func (i *stringSliceFlag) String() string {
    return fmt.Sprintf("%v", *i)
}

func (i *stringSliceFlag) Set(value string) error {
    *i = append(*i, value)
    return nil
}

func main() {
    var mountPoints string
    var fileSize string
    var testMode string
    var blockSizes string
    var rawDiskPaths stringSliceFlag
    var rawDiskStartOffset string
    var duration string
    var testCPU bool
    var cpuCores int
    var cpuLoad string
    var memoryPercent float64
    var debugFlag bool
    var showHelp bool
    var printSystemInfo bool
    var numaNode int

    flag.StringVar(&mountPoints, "l", "", "Comma separated mount points to test (e.g. /mnt/disk1,/mnt/disk2)")
    flag.Var(&rawDiskPaths, "disk", "Raw disk devices to test (e.g., /dev/sdb, /dev/nvme0n1)")
    flag.StringVar(&fileSize, "size", "10MB", "Size of test files for disk tests or test size for raw disk tests (supports K, M, G units)")
    flag.StringVar(&testMode, "mode", "both", "Test mode for disk and raw disk: sequential, random, or both")
    flag.StringVar(&blockSizes, "block", "4K", "Comma separated block sizes for disk and raw disk operations (supports K, M, G units)")
    flag.StringVar(&rawDiskStartOffset, "diskoffset", "1G", "Start offset from the beginning of the raw device (e.g., 1G, 100M, supports K, M, G units)")
    flag.StringVar(&duration, "duration", "10m", "Test duration (e.g. 30s, 5m, 1h)")
    flag.BoolVar(&testCPU, "cpu", false, "Enable CPU testing")
    flag.IntVar(&cpuCores, "cpu-cores", 0, "Number of CPU cores to stress (0 means all cores)")
    flag.StringVar(&cpuLoad, "cpu-load", "Default", "CPU load level: High(2), Low(1), or Default(0)")
    flag.IntVar(&numaNode, "numa", -1, "NUMA node to stress (e.g., 0 or 1; default -1 means all nodes)")
    flag.Float64Var(&memoryPercent, "memory", 0, "Memory testing percentage (0.1-9.9 for 1%-99% of total memory, e.g., 1.5 for 15%)")
    flag.BoolVar(&debugFlag, "d", false, "Enable debug mode")
    flag.BoolVar(&showHelp, "h", false, "Show help")
    flag.BoolVar(&printSystemInfo, "print", false, "Print available system resources for stress testing (alias: -list)")
    flag.BoolVar(&printSystemInfo, "list", false, "Alias for -print")
    flag.Parse()

    // 檢查 -memory 參數範圍
    if memoryPercent != 0 {
        if memoryPercent < 0.1 || memoryPercent > 9.9 {
            utils.LogMessage(fmt.Sprintf("Error: -memory must be between 0.1 and 9.9, got %.1f", memoryPercent), true)
            os.Exit(1)
        }
    }

    // 解析 RawDisk 參數
	var fileSizeBytes, rawDiskStartOffsetBytes int64
    if fileSize != "" {
        size, err := utils.ParseSize(fileSize)
        if err != nil {
            utils.LogMessage(fmt.Sprintf("Error: Invalid -size '%s': %v", fileSize, err), true)
            os.Exit(1)
        }
        fileSizeBytes = size
    } else {
        fileSizeBytes = 10 * 1024 * 1024 // 10MB default
    }

    if rawDiskStartOffset != "" {
        offset, err := utils.ParseSize(rawDiskStartOffset)
        if err != nil {
            utils.LogMessage(fmt.Sprintf("Error: Invalid -diskoffset '%s': %v", rawDiskStartOffset, err), true)
            os.Exit(1)
        }
        rawDiskStartOffsetBytes = offset
    } else {
        rawDiskStartOffsetBytes = 1024 * 1024 * 1024 // 1GB default
    }

	// 如果有原始磁碟，檢查每個磁碟的大小
    rawDiskSizes := make(map[string]int64)
    if len(rawDiskPaths) > 0 {
        for _, devicePath := range rawDiskPaths {
            size, err := utils.GetDiskSize(devicePath)
            if err != nil {
                utils.LogMessage(fmt.Sprintf("Error: Failed to get size for device %s: %v", devicePath, err), true)
                os.Exit(1)
            }
            rawDiskSizes[devicePath] = size
            utils.LogMessage(fmt.Sprintf("Detected size for device %s: %s", devicePath, utils.FormatSize(size)), true)
        }
    }

    if printSystemInfo {
        info := systeminfo.GetSystemInfo()
        systeminfo.PrintSystemInfo(info)
        return
    }

    if (!testCPU && memoryPercent == 0 && mountPoints == "" && len(rawDiskPaths) == 0) || showHelp {
        fmt.Println("System Stress Test Tool")
        fmt.Println("Usage: stress [options]")
        fmt.Println("\nOptions:")
        flag.PrintDefaults()
        fmt.Println("\nNotes:")
        fmt.Println("- At least one of -cpu, -memory, -l, -disk must be specified.")
        fmt.Println("- Use -cpu-load to adjust CPU test intensity: 'High(2)', 'Low(1)', or 'Default(0)'.")
        fmt.Println("- Use -print or -list to view available system resources.")
        return
    }

    configuration, err := cfg.LoadConfig()
    if err != nil {
        fmt.Printf("[Ignore] Failed to load config.json, using default settings: %v\n", err)
    }

    debug := debugFlag || configuration.Debug

    testDuration, err := time.ParseDuration(duration)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("Invalid duration format: %s, using default 10 minutes", duration), true)
        testDuration = 10 * time.Minute
    }

    utils.LogMessage(fmt.Sprintf("Starting stress test for %v...", testDuration), true)
    utils.LogMessage(fmt.Sprintf("Debug mode: %v", debug), debug)

    perfStats := &cfg.PerformanceStats{
        CPU: cfg.CPUPerformance{
            CoreGFLOPS: make(map[int]float64),
        },
        Memory:  cfg.MemoryPerformance{},
        Disk:    []cfg.DiskPerformance{},
        RawDisk: []cfg.RawDiskPerformance{},
    }

    var wg sync.WaitGroup
    stop := make(chan struct{})
    errorChan := make(chan string, 100)

    results := cfg.TestResult{
        CPU:  "PASS",
        DIMM: "PASS",
        HDD:  "PASS",
    }

    errorDetails := make(map[string][]string)

    // Memory test with percentage-based configuration
    if memoryPercent > 0 {
        memUsagePercent := memoryPercent / 10.0
        if memUsagePercent > 0.95 {
            memUsagePercent = 0.95
            utils.LogMessage("Memory usage capped at 95% for system stability", true)
        }

        memConfig := memory.MemoryConfig{
            UsagePercent: memUsagePercent,
            Debug:        debug,
        }
        utils.LogMessage(fmt.Sprintf("Starting memory stress test with %.1f%% of total memory...", memUsagePercent*100), debug)
        wg.Add(1)
        go memory.RunMemoryStressTest(&wg, stop, errorChan, memConfig, perfStats)
    }

    var testModes []string
    switch testMode {
    case "sequential":
        testModes = []string{"sequential"}
    case "random":
        testModes = []string{"random"}
    case "both", "":
        testModes = []string{"sequential", "random"}
    default:
        utils.LogMessage(fmt.Sprintf("Invalid test mode: %s, using both sequential and random", testMode), true)
        testModes = []string{"sequential", "random"}
    }

    var blockSizeList []int64
    if blockSizes != "" {
        for _, bsStr := range strings.Split(blockSizes, ",") {
            bs, err := utils.ParseSize(bsStr)
            if err != nil {
                utils.LogMessage(fmt.Sprintf("Invalid block size: %v, skipping", err), debug)
                continue
            }
            blockSizeList = append(blockSizeList, bs)
        }
    }
    if len(blockSizeList) == 0 {
        blockSizeList = append(blockSizeList, 4*1024)
    }

	// Mount point disk tests
    var mounts []string
    if mountPoints != "" {
        mounts = strings.Split(mountPoints, ",")
        utils.LogMessage(fmt.Sprintf("Starting disk tests on mount points: %v", mounts), debug)

        for _, mode := range testModes {
            for _, bs := range blockSizeList {
                bsDisplay := utils.FormatSize(bs)
                utils.LogMessage(fmt.Sprintf("Starting %s disk test with file size %s and block size %s on %v...",
                    mode, utils.FormatSize(fileSizeBytes), bsDisplay, mounts), debug)

                diskConfig := disk.DiskTestConfig{
                    MountPoints: mounts,
                    FileSize:    fileSizeBytes,
                    TestMode:    mode,
                    BlockSize:   bs,
                }

                wg.Add(1)
                go disk.RunDiskStressTest(&wg, stop, errorChan, diskConfig, perfStats, debug)
            }
        }
    }

	// Raw disk tests
    if len(rawDiskPaths) > 0 {
        if os.Geteuid() != 0 {
            utils.LogMessage("Warning: Raw disk tests (-disk) typically require root privileges.", true)
            utils.LogMessage("You may not have sufficient permissions to perform these tests.", true)
        }

        for _, mode := range testModes {
            for _, bs := range blockSizeList {
                for _, devicePath := range rawDiskPaths {
                    // 獲取磁碟大小
                    diskSize, ok := rawDiskSizes[devicePath]
                    if !ok {
                        utils.LogMessage(fmt.Sprintf("Error: Size not found for device %s", devicePath), true)
                        continue
                    }

                    // 計算可用大小（考慮 StartOffset）
                    availableSize := diskSize - rawDiskStartOffsetBytes
                    if availableSize <= 0 {
                        utils.LogMessage(fmt.Sprintf("Error: Start offset %s exceeds size %s for device %s",
                            utils.FormatSize(rawDiskStartOffsetBytes), utils.FormatSize(diskSize), devicePath), true)
                        continue
                    }

                    // 限制 TestSize 不超過可用大小
                    testSize := fileSizeBytes
                    if testSize > availableSize {
                        testSize = availableSize
                        utils.LogMessage(fmt.Sprintf("Warning: Test size %s exceeds available size %s for device %s, capping at %s",
                            utils.FormatSize(fileSizeBytes), utils.FormatSize(availableSize), devicePath, utils.FormatSize(testSize)), true)
                    }

                    rawDiskTestConfig := rawdisk.RawDiskTestConfig{
                        DevicePaths: []string{devicePath}, // 單獨測試每個設備
                        TestSize:    testSize,
                        BlockSize:   bs,
                        TestMode:    mode,
                        StartOffset: rawDiskStartOffsetBytes,
                    }

                    utils.LogMessage(fmt.Sprintf("Starting raw disk test on device %s: Test size: %s, Block size: %s, Mode: %s, Start offset: %s",
                        devicePath, utils.FormatSize(testSize), utils.FormatSize(bs), mode, utils.FormatSize(rawDiskStartOffsetBytes)), debug)

                    wg.Add(1)
                    go rawdisk.RunRawDiskStressTest(&wg, stop, errorChan, rawDiskTestConfig, perfStats, debug)
                }
            }
        }
    }

    // CPU test
    if testCPU {
        rand.Seed(time.Now().UnixNano())

        numaInfo, err := utils.GetNUMAInfo()
        if err != nil {
            utils.LogMessage(fmt.Sprintf("Failed to get NUMA info: %v", err), debug)
            numaNode = -1
        }

        numCores := cpuCores
        selectedCPUs := []int{}

        if numaNode >= 0 && numaInfo.NumNodes > 0 && numaNode < numaInfo.NumNodes {
            selectedCPUs = numaInfo.NodeCPUs[numaNode]
            if len(selectedCPUs) == 0 {
                utils.LogMessage(fmt.Sprintf("NUMA node %d has no CPUs, falling back to all cores", numaNode), true)
                numaNode = -1
            } else {
                utils.LogMessage(fmt.Sprintf("NUMA node %d has CPUs: %v", numaNode, selectedCPUs), debug)
                if numCores > 0 && numCores > len(selectedCPUs) {
                    utils.LogMessage(fmt.Sprintf("Error: Requested %d cores, but NUMA node %d only has %d cores. Falling back to all cores.", cpuCores, numaNode, len(selectedCPUs)), true)
                    numaNode = -1
                }
            }
        }

        if numaNode < 0 || numaInfo.NumNodes == 0 {
            totalCores := runtime.NumCPU()
            allCPUs := make([]int, totalCores)
            for i := 0; i < totalCores; i++ {
                allCPUs[i] = i
            }

            if numCores == 0 {
                numCores = totalCores
                selectedCPUs = allCPUs
                utils.LogMessage(fmt.Sprintf("No CPU cores specified, using all %d cores: %v", numCores, selectedCPUs), debug)
            } else if numCores > totalCores {
                numCores = totalCores
                selectedCPUs = allCPUs
                utils.LogMessage(fmt.Sprintf("Requested %d cores, but only %d available. Using %d cores: %v", cpuCores, totalCores, numCores, selectedCPUs), true)
            } else {
                selectedCPUs = randomSelectCores(allCPUs, numCores)
                utils.LogMessage(fmt.Sprintf("Randomly selected %d cores: %v", numCores, selectedCPUs), debug)
            }
        } else {
            if numCores == 0 {
                numCores = len(selectedCPUs)
                utils.LogMessage(fmt.Sprintf("No CPU cores specified, using all %d cores in NUMA node %d: %v", numCores, numaNode, selectedCPUs), debug)
            } else {
                selectedCPUs = randomSelectCores(selectedCPUs, numCores)
                utils.LogMessage(fmt.Sprintf("Randomly selected %d cores in NUMA node %d: %v", numCores, numaNode, selectedCPUs), debug)
            }
        }

        testConfig := cpu.CPUConfig{
            NumCores:   numCores,
            Debug:      debug,
            CPUList:    selectedCPUs,
            LoadLevel:  cpuLoad,
        }
        utils.LogMessage(fmt.Sprintf("Starting CPU stress tests using %d cores (CPUs: %v) with load level: %s...", testConfig.NumCores, testConfig.CPUList, testConfig.LoadLevel), debug)
        wg.Add(1)
        go cpu.RunCPUStressTests(&wg, stop, errorChan, testConfig, perfStats)
    }

    go func() {
        for err := range errorChan {
            if err == "" {
                continue
            }

            switch {
            case strings.Contains(err, "Integer") || strings.Contains(err, "Float") || strings.Contains(err, "Vector"):
                results.CPU = "FAIL"
                errorDetails["CPU"] = append(errorDetails["CPU"], err)
            case strings.Contains(err, "Memory"):
                results.DIMM = "FAIL"
                errorDetails["DIMM"] = append(errorDetails["DIMM"], err)
            case strings.Contains(err, "Disk") || strings.Contains(err, "RawDisk"):
                results.HDD = "FAIL"
                errorDetails["HDD"] = append(errorDetails["HDD"], err)
            }

            utils.LogMessage(fmt.Sprintf("Error detected: %s", err), debug)
        }
    }()

    progressTicker := time.NewTicker(30 * time.Second)
    go func() {
        for {
            select {
            case <-progressTicker.C:
                perfStats.Lock()
                cpuGFLOPS := perfStats.CPU.GFLOPS
                memRead := perfStats.Memory.ReadSpeed
                memWrite := perfStats.Memory.WriteSpeed
                var bestDiskRead, bestDiskWrite float64
                var bestDiskMount string
                for _, disk := range perfStats.Disk {
                    if disk.ReadSpeed > bestDiskRead {
                        bestDiskRead = disk.ReadSpeed
                        bestDiskMount = disk.MountPoint
                    }
                    if disk.WriteSpeed > bestDiskWrite {
                        bestDiskWrite = disk.WriteSpeed
                    }
                }
                var bestRawDiskRead, bestRawDiskWrite float64
                var bestRawDiskDevice string
                for _, rawDisk := range perfStats.RawDisk {
                    if rawDisk.ReadSpeed > bestRawDiskRead {
                        bestRawDiskRead = rawDisk.ReadSpeed
                        bestRawDiskDevice = rawDisk.DevicePath
                    }
                    if rawDisk.WriteSpeed > bestRawDiskWrite {
                        bestRawDiskWrite = rawDisk.WriteSpeed
                    }
                }
                perfStats.Unlock()

                var progressMsg string
                if testCPU {
                    progressMsg = fmt.Sprintf("Progress update - CPU: %.2f GFLOPS (approximate value, not exact)", cpuGFLOPS)
                } else {
                    progressMsg = "Progress update"
                }

                if memoryPercent > 0 {
                    progressMsg += fmt.Sprintf(", Memory: R=%.2f MB/s W=%.2f MB/s", memRead, memWrite)
                }
                if len(mounts) > 0 {
                    progressMsg += fmt.Sprintf(", Disk(%s): R=%.2f MB/s W=%.2f MB/s",
                        bestDiskMount, bestDiskRead, bestDiskWrite)
                }
                if len(rawDiskPaths) > 0 {
                    progressMsg += fmt.Sprintf(", RawDisk(%s): R=%.2f MB/s W=%.2f MB/s",
                        bestRawDiskDevice, bestRawDiskRead, bestRawDiskWrite)
                }
                utils.LogMessage(progressMsg, true)

            case <-stop:
                progressTicker.Stop()
                return
            }
        }
    }()

    startTime := time.Now()
    time.Sleep(testDuration)
    close(stop)
    wg.Wait()
    close(errorChan)

    elapsedTime := time.Since(startTime)

    utils.LogMessage("=== PERFORMANCE RESULTS ===", true)
    var totalOperations uint64
    if testCPU {
        cpuTotal := perfStats.CPU.IntegerCount + perfStats.CPU.FloatCount + perfStats.CPU.VectorCount +
                    perfStats.CPU.CacheCount + perfStats.CPU.BranchCount + perfStats.CPU.CryptoCount
        utils.LogMessage(fmt.Sprintf("CPU Performance: %.2f GFLOPS (Load Level: %s)", perfStats.CPU.GFLOPS, cpuLoad), true)
        utils.LogMessage(fmt.Sprintf("CPU Operations: Integer=%d, Float=%d, Vector=%d, Cache=%d, Branch=%d, Crypto=%d, Total=%d",
            perfStats.CPU.IntegerCount, perfStats.CPU.FloatCount, perfStats.CPU.VectorCount,
            perfStats.CPU.CacheCount, perfStats.CPU.BranchCount, perfStats.CPU.CryptoCount, cpuTotal), true)
        totalOperations += cpuTotal
    }
    if memoryPercent > 0 {
        memTotal := perfStats.Memory.WriteCount + perfStats.Memory.ReadCount + perfStats.Memory.RandomAccessCount
        utils.LogMessage(fmt.Sprintf("Memory Performance - Read: %.2f MB/s, Write: %.2f MB/s, Random Access: %.2f MB/s",
            perfStats.Memory.ReadSpeed, perfStats.Memory.WriteSpeed, perfStats.Memory.RandomAccessSpeed), true)
        utils.LogMessage(fmt.Sprintf("Memory Operations: Write=%d, Read=%d, Random Access=%d, Total=%d",
            perfStats.Memory.WriteCount, perfStats.Memory.ReadCount, perfStats.Memory.RandomAccessCount, memTotal), true)
        totalOperations += memTotal
    }
    if len(mounts) > 0 {
        var diskTotal uint64
        utils.LogMessage("Disk Performance:", true)
        for _, disk := range perfStats.Disk {
            diskOps := disk.WriteCount + disk.ReadCount
            utils.LogMessage(fmt.Sprintf("  Mount: %s, Mode: %s, Block: %s - Read: %.2f MB/s, Write: %.2f MB/s, Operations: Write=%d, Read=%d, Total=%d",
                disk.MountPoint, disk.Mode, utils.FormatSize(disk.BlockSize), disk.ReadSpeed, disk.WriteSpeed,
                disk.WriteCount, disk.ReadCount, diskOps), true)
            diskTotal += diskOps
        }
        utils.LogMessage(fmt.Sprintf("Disk Total Operations: %d", diskTotal), true)
        totalOperations += diskTotal
    }
    if len(rawDiskPaths) > 0 {
        var rawDiskTotal uint64
        printRawDiskPerformanceResults(perfStats)
        for _, rawDisk := range perfStats.RawDisk {
            rawDiskOps := rawDisk.WriteCount + rawDisk.ReadCount
            rawDiskTotal += rawDiskOps
        }
        utils.LogMessage(fmt.Sprintf("RawDisk Total Operations: %d", rawDiskTotal), true)
        totalOperations += rawDiskTotal
    }

    utils.LogMessage(fmt.Sprintf("Total Operations Across All Tests: %d", totalOperations), true)

    resultStr := fmt.Sprintf("Stress Test Summary - Duration: %s", elapsedTime.Round(time.Second))
    if testCPU {
        resultStr += fmt.Sprintf(" | CPU: %s", results.CPU)
    }
    if memoryPercent > 0 {
        resultStr += fmt.Sprintf(" | DIMM: %s", results.DIMM)
    }
    if len(mounts) > 0 || len(rawDiskPaths) > 0 {
        resultStr += fmt.Sprintf(" | HDD: %s", results.HDD)
    }

    for component, errors := range errorDetails {
        if len(errors) > 0 {
            resultStr += fmt.Sprintf("\n%s FAIL reason: %s", component, errors[0])
            if len(errors) > 1 {
                resultStr += fmt.Sprintf(" (and %d more errors)", len(errors)-1)
            }
        }
    }

    utils.LogMessage(resultStr, true)
    utils.LogMessage("Stress test completed!", true)
}

// 整合 printRawDiskPerformanceResults 函數
func printRawDiskPerformanceResults(perfStats *cfg.PerformanceStats) {
    if len(perfStats.RawDisk) == 0 {
        return
    }

    utils.LogMessage("\nRaw Disk Performance Results:", true)
    utils.LogMessage("-----------------------------", true)

    deviceResults := make(map[string][]cfg.RawDiskPerformance)
    for _, result := range perfStats.RawDisk {
        deviceResults[result.DevicePath] = append(deviceResults[result.DevicePath], result)
    }

    for device, results := range deviceResults {
        utils.LogMessage(fmt.Sprintf("\nDevice: %s", device), true)
        utils.LogMessage(fmt.Sprintf("%-12s %-10s %-12s %-15s %-15s %-15s", "Mode", "Block Size", "Block Size", "Read Speed", "Write Speed", "Operations"), true)
        utils.LogMessage(fmt.Sprintf("%-12s %-10s %-12s %-15s %-15s %-15s", "----", "----------", "----------", "----------", "-----------", "----------"), true)

        for _, result := range results {
            opsTotal := result.WriteCount + result.ReadCount
            utils.LogMessage(fmt.Sprintf("%-12s %-10d %-12s %-15.2f %-15.2f %-15d",
                result.Mode,
                result.BlockSize,
                utils.FormatSize(result.BlockSize),
                result.ReadSpeed,
                result.WriteSpeed,
                opsTotal), true)
        }
    }
}

// randomSelectCores 從給定的核心列表中隨機選擇指定數量的核心
func randomSelectCores(cpus []int, count int) []int {
    if count >= len(cpus) {
        return append([]int{}, cpus...)
    }

    available := append([]int{}, cpus...)
    selected := make([]int, 0, count)

    for i := 0; i < count; i++ {
        if len(available) == 0 {
            break
        }
        idx := rand.Intn(len(available))
        selected = append(selected, available[idx])
        available = append(available[:idx], available[idx+1:]...)
    }

    return selected
}
