package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "os"
    "regexp"
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
    var scanFlag bool
    var numaNode int
    var serverIPs string

    flag.StringVar(&mountPoints, "l", "", "Comma-separated mount points to test (e.g. /mnt/disk1,/mnt/disk2)")
    flag.Var(&rawDiskPaths, "disk", "Raw disk devices to test (e.g., /dev/sdb, /dev/nvme0n1)")
    flag.StringVar(&fileSize, "size", "", "Size of test files for disk tests or test size for raw disk tests (supports K, M, G units)")
    flag.StringVar(&testMode, "mode", "", "Test mode for disk and raw disk: sequential or random")
    flag.StringVar(&blockSizes, "block", "", "Comma-separated block sizes for disk and raw disk operations (supports K, M, G units)")
    flag.StringVar(&rawDiskStartOffset, "diskoffset", "", "Start offset from the beginning of the raw device (e.g., 1G, 100M, supports K, M, G units)")
    flag.StringVar(&duration, "duration", "10m", "Test duration (e.g. 30s, 5m, 1h)")
    flag.BoolVar(&testCPU, "cpu", false, "Enable CPU testing")
    flag.IntVar(&cpuCores, "cpu-cores", 0, "Number of CPU cores to stress (0 means all cores)")
    flag.StringVar(&cpuLoad, "cpu-load", "", "CPU load level: High(2), Low(1), or Default(0)")
    flag.IntVar(&numaNode, "numa", -1, "NUMA node to stress (e.g., 0 or 1; default -1 means all nodes)")
    flag.Float64Var(&memoryPercent, "memory", 0, "Memory testing percentage (0.1-9.9 for 1%-99% of total memory, e.g., 1.5 for 15%)")
    flag.BoolVar(&debugFlag, "d", false, "Enable debug mode")
    flag.BoolVar(&showHelp, "h", false, "Show help")
    flag.BoolVar(&printSystemInfo, "print", false, "Print available system resources for stress testing (alias: -list)")
    flag.BoolVar(&printSystemInfo, "list", false, "Alias for -print")
    flag.BoolVar(&scanFlag, "scan", false, "Scan system resources and update config.json")
    flag.Parse()

    // Check if any command-line parameters are provided
    hasFlags := debugFlag || testCPU || cpuCores != 0 || cpuLoad != "" || memoryPercent != 0 ||
        mountPoints != "" || len(rawDiskPaths) > 0 || fileSize != "" || rawDiskStartOffset != "" ||
        blockSizes != "" || testMode != "" || numaNode != -1 || showHelp || printSystemInfo || scanFlag

    var configuration cfg.Config
    var debug bool

    // Load config.json only if no command-line parameters are provided
    if !hasFlags {
        var err error
        configuration, err = cfg.LoadConfig()
        if err != nil {
            fmt.Printf("[Ignore] Failed to load config.json, using default settings: %v\n", err)
        } else {
            debug = configuration.Debug
            testCPU = configuration.CPU
            cpuCores = configuration.Cores
            cpuLoad = configuration.Load
            if configuration.Memory {
                memoryPercent = configuration.MEMPercent
            }
            mountPoints = configuration.Mountpoint
            if configuration.RAWDisk != "" {
                rawDiskPaths = strings.Split(configuration.RAWDisk, ",")
            }
            fileSize = configuration.Size
            rawDiskStartOffset = configuration.Offset
            blockSizes = configuration.Block
            testMode = configuration.Mode
            numaNode = configuration.NUMANode
        }
    } else {
        debug = debugFlag
    }

    if showHelp {
        fmt.Println("System Stress Test Tool v2")
        fmt.Println("Usage: stress [options]")
        fmt.Println("\nOptions:")
        flag.PrintDefaults()
        fmt.Println("\nNotes:")
        fmt.Println("- At least one of -cpu, -memory, -l, -disk, or -net must be specified for stress testing.")
        fmt.Println("- Use -cpu-load to adjust CPU test intensity: 'High(2)', 'Low(1)', or 'Default(0)'.")
        fmt.Println("- Use -print or -list to view available system resources.")
        fmt.Println("- Use -scan to update config.json with system resources.")
        fmt.Println("- Raw disk tests (-disk) require 'sequential' or 'random' mode; 'both' is not supported.")
        fmt.Println("")
        fmt.Println("- Due to all tests will need memory size, so if you run all tests, the memory usage must be 80% as well.")
        return
    }

    if printSystemInfo {
        info := systeminfo.GetSystemInfo()
        systeminfo.PrintSystemInfo(info)
        return
    }

    if scanFlag {
        info := systeminfo.GetSystemInfo()

        coreCount := 0
        reCores := regexp.MustCompile(`Cores: (\d+)`)
        if match := reCores.FindStringSubmatch(info.CPUInfo); len(match) > 1 {
            fmt.Sscanf(match[1], "%d", &coreCount)
        } else {
            coreCount = runtime.NumCPU()
            utils.LogMessage("Warning: Could not parse core count from CPUInfo, using runtime.NumCPU()", true)
        }

        numaNode := -1
        reNuma := regexp.MustCompile(`Numa Node: (\d+)`)
        if match := reNuma.FindStringSubmatch(info.CPUInfo); len(match) > 1 {
            fmt.Sscanf(match[1], "%d", &numaNode)
        } else {
            numaNode = -1
            utils.LogMessage("Warning: Could not parse core count from CPUInfo, using all Numa Node", true)
        }

        memPercent := 0.0
        reMem := regexp.MustCompile(`\(([\d.]+)\)`)
        if match := reMem.FindStringSubmatch(info.MemoryInfo); len(match) > 1 {
            fmt.Sscanf(match[1], "%f", &memPercent)
        } else {
            memPercent = 9.0
            utils.LogMessage("Warning: Could not parse memory stress percent from MemoryInfo, defaulting to 9.0 (90%)", true)
        }

        mountPointsStr := ""
        reMounts := regexp.MustCompile(`Disk Mount Points Available for Stress: (.*)`)
        if match := reMounts.FindStringSubmatch(info.DiskMounts); len(match) > 1 {
            mountPointsStr = match[1]
        } else if info.DiskMounts != "Disk Mount Points: None available, please check df by yourself!" {
            utils.LogMessage("Warning: Could not parse mount points from DiskMounts, defaulting to empty", true)
        }

        rawDisksStr := ""
        reRawDisks := regexp.MustCompile(`Raw Disks Available for Stress: (.*)`)
        if match := reRawDisks.FindStringSubmatch(info.RawDisks); len(match) > 1 {
            rawDisksStr = match[1]
        } else if info.RawDisks != "Raw Disks: None available" {
            utils.LogMessage("Warning: Could not parse raw disks from RawDisks, defaulting to empty", true)
        }

        config := cfg.Config{
            Debug:      false,
            CPU:        true,
            Cores:      coreCount,
            Load:       "Default",
            Memory:     true,
            MEMPercent: memPercent,
            Mountpoint: mountPointsStr,
            RAWDisk:    rawDisksStr,
            Size:       "10M",
            Offset:     "1G",
            Block:      "4K",
            Mode:       "sequential",
            NUMANode:   numaNode,
        }

        if config.Memory && config.MEMPercent <= 0 {
            utils.LogMessage("Error: MEMPercent must be between 0.1 and 9.9 when Memory is enabled", true)
            os.Exit(1)
        }

        if config.RAWDisk != "" && config.Offset == "" {
            utils.LogMessage("Error: Offset must be specified when RAWDisk is not empty", true)
            os.Exit(1)
        }

        type ConfigWithDescription struct {
            Debug          bool    `json:"debug"`
            DebugDesc      string  `json:"debug_description"`
            CPU            bool    `json:"CPU"`
            CPUDesc        string  `json:"CPU_description"`
            Cores          int     `json:"Cores"`
            CoresDesc      string  `json:"Cores_description"`
            Load           string  `json:"Load"`
            LoadDesc       string  `json:"Load_description"`
            Memory         bool    `json:"Memory"`
            MemoryDesc     string  `json:"Memory_description"`
            MEMPercent     float64 `json:"MEMPercent"`
            MEMPercentDesc string  `json:"MEMPercent_description"`
            Mountpoint     string  `json:"Mountpoint"`
            MountpointDesc string  `json:"Mountpoint_description"`
            RAWDisk        string  `json:"RAWDisk"`
            RAWDiskDesc    string  `json:"RAWDisk_description"`
            Size           string  `json:"Size"`
            SizeDesc       string  `json:"Size_description"`
            Offset         string  `json:"Offset"`
            OffsetDesc     string  `json:"Offset_description"`
            Block          string  `json:"Block"`
            BlockDesc      string  `json:"Block_description"`
            Mode           string  `json:"Mode"`
            ModeDesc       string  `json:"Mode_description"`
            NUMANode       int     `json:"NUMA"`
            NUMANodeDesc   string  `json:"NUMA_description"`
        }

        configWithDesc := ConfigWithDescription{
            Debug:          config.Debug,
            DebugDesc:      "Enable debug mode",
            CPU:            config.CPU,
            CPUDesc:        "Enable CPU stress testing",
            Cores:          config.Cores,
            CoresDesc:      "Number of CPU cores to stress (0 means all cores)",
            Load:           config.Load,
            LoadDesc:       "CPU load level: High(2), Low(1), or Default(0)",
            Memory:         config.Memory,
            MemoryDesc:     "Enable memory stress testing",
            MEMPercent:     config.MEMPercent,
            MEMPercentDesc: "Memory testing percentage (0.1-9.0 for 1%-90% of total memory, e.g., 1.5 for 15%)",
            Mountpoint:     config.Mountpoint,
            MountpointDesc: "Comma-separated mount points to test (e.g., /mnt/disk1,/mnt/disk2)",
            RAWDisk:        config.RAWDisk,
            RAWDiskDesc:    "Raw disk devices to test (e.g., /dev/sdb, /dev/nvme0n1)",
            Size:           config.Size,
            SizeDesc:       "Size of test files for disk tests or test size for raw disk tests (supports K, M, G units)",
            Offset:         config.Offset,
            OffsetDesc:     "Start offset from the beginning of the raw device (e.g., 1G, 100M, supports K, M, G units)",
            Block:          config.Block,
            BlockDesc:      "Comma-separated block sizes for disk and raw disk operations (supports K, M, G units)",
            Mode:           config.Mode,
            ModeDesc:       "Test mode for disk and random disk: sequential or random",
            NUMANode:       config.NUMANode,
            NUMANodeDesc:   "Test mode for NUMA Node, the -1 as default to test all nodes",
        }

        configData, err := json.MarshalIndent(configWithDesc, "", "    ")
        if err != nil {
            utils.LogMessage(fmt.Sprintf("Error: Failed to marshal config: %v", err), true)
            os.Exit(1)
        }
        err = os.WriteFile("config.json", configData, 0644)
        if err != nil {
            utils.LogMessage(fmt.Sprintf("Error: Failed to write config.json: %v", err), true)
            os.Exit(1)
        }

        utils.LogMessage("Successfully scanned system resources and updated config.json", false)
        return
    }

    // Validate parameters
    if memoryPercent != 0 {
        if memoryPercent < 0.1 || memoryPercent > 9.9 {
            utils.LogMessage(fmt.Sprintf("Error: -memory must be between 0.1 and 9.9, got %.1f", memoryPercent), true)
            os.Exit(1)
        }
    }

    if len(rawDiskPaths) > 0 && rawDiskStartOffset == "" {
        utils.LogMessage("Error: -diskoffset must be specified when -disk is provided", true)
        os.Exit(1)
    }

    // Set final default values
    if fileSize == "" {
        fileSize = "10MB"
    }
    if blockSizes == "" {
        blockSizes = "4K"
    }
    if testMode == "" {
        testMode = "sequential"
    }
    if cpuLoad == "" {
        cpuLoad = "Default"
    }

    var testModes []string
    if len(rawDiskPaths) > 0 && testMode == "both" {
        utils.LogMessage("Invalid test mode: both, using sequential", true)
        testModes = []string{"sequential"}
    } else {
        switch testMode {
        case "sequential":
            testModes = []string{"sequential"}
        case "random":
            testModes = []string{"random"}
        case "both":
            testModes = []string{"sequential", "random"}
        default:
            utils.LogMessage(fmt.Sprintf("Invalid test mode: %s, using sequential", testMode), true)
            testModes = []string{"sequential"}
        }
    }

    // Parse unified parameters
    var fileSizeBytes, rawDiskStartOffsetBytes int64
    if fileSize != "" {
        size, err := utils.ParseSize(fileSize)
        if err != nil {
            utils.LogMessage(fmt.Sprintf("Error: Invalid -size '%s': %v", fileSize, err), true)
            os.Exit(1)
        }
        fileSizeBytes = size
    } else {
        fileSizeBytes = 10 * 1024 * 1024
    }

    if rawDiskStartOffset != "" {
        offset, err := utils.ParseSize(rawDiskStartOffset)
        if err != nil {
            utils.LogMessage(fmt.Sprintf("Error: Invalid -diskoffset '%s': %v", rawDiskStartOffset, err), true)
            os.Exit(1)
        }
        rawDiskStartOffsetBytes = offset
    } else {
        rawDiskStartOffsetBytes = 1024 * 1024 * 1024
    }

    // Check if any stress test is requested
    if !testCPU && memoryPercent == 0 && mountPoints == "" && len(rawDiskPaths) == 0 && serverIPs == "" {
        fmt.Println("Error: At least one of -cpu, -memory, -l, -disk, or -net must be specified for stress testing.")
        fmt.Println("Use -h to see all options.")
        os.Exit(1)
    }

    testDuration, err := time.ParseDuration(duration)
    if err != nil {
        utils.LogMessage(fmt.Sprintf("Invalid duration format: %s, using default 10 minutes", duration), true)
        testDuration = 10 * time.Minute
    }

    utils.LogMessage(fmt.Sprintf("Starting stress test for %v...", testDuration), false)
    utils.LogMessage(fmt.Sprintf("Debug mode: %v", debug), false)

    // Initialize perfStats
    perfStats := &cfg.PerformanceStats{
        CPU: cfg.CPUPerformance{
            IntegerCount: 0,
            FloatCount:   0,
            VectorCount:  0,
            CacheCount:   0,
            BranchCount:  0,
            CryptoCount:  0,
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
        Mountpoint: make(map[string]string),
        RawDisk:    make(map[string]string),
    }

    mounts := []string{}
    if mountPoints != "" {
        for _, mount := range strings.Split(mountPoints, ",") {
            if mount != "" { // Filter empty strings
                mounts = append(mounts, mount)
                results.Mountpoint[mount] = "PASS"
            }
        }
    }

    for _, disk := range rawDiskPaths {
        results.RawDisk[disk] = "PASS"
    }

    errorDetails := make(map[string][]string)

    // Memory test
    if memoryPercent > 0 {
        memUsagePercent := memoryPercent / 10.0
        if memUsagePercent > 0.9 {
            memUsagePercent = 0.9
            utils.LogMessage("Memory recommended usage capped at 90% for system stability", false)
        }

        memConfig := memory.MemoryConfig{
            UsagePercent: memUsagePercent,
            Debug:        debug,
        }
        utils.LogMessage(fmt.Sprintf("Starting memory stress test with %.1f%% of total memory...", memUsagePercent*100), false)
        wg.Add(1)
        go memory.RunMemoryStressTest(&wg, stop, errorChan, memConfig, perfStats)
    }

    // Parse block sizes
    var blockSizeList []int64
    if blockSizes != "" {
        for _, bsStr := range strings.Split(blockSizes, ",") {
            bs, err := utils.ParseSize(bsStr)
            if err != nil {
                utils.LogMessage(fmt.Sprintf("Invalid block size: %v, skipping", err), true)
                continue
            }
            blockSizeList = append(blockSizeList, bs)
        }
    }
    if len(blockSizeList) == 0 {
        blockSizeList = append(blockSizeList, 4*1024)
    }

    // Mount point disk tests
    if mountPoints != "" {
        mounts = strings.Split(mountPoints, ",")
        utils.LogMessage(fmt.Sprintf("Starting disk tests on mount points: %v", mounts), false)

        for _, mode := range testModes {
            for _, bs := range blockSizeList {
                bsDisplay := utils.FormatSize(bs)
                utils.LogMessage(fmt.Sprintf("Starting %s disk test with file size %s and block size %s on %v...",
                    mode, utils.FormatSize(fileSizeBytes), bsDisplay, mounts), false)

                diskConfig := disk.DiskTestConfig{
                    MountPoints: mounts,
                    FileSize:    fileSizeBytes,
                    TestMode:    mode,
                    BlockSize:   bs,
                    NumFiles:    4, // Explicitly set to align with disk.go default
                }

                wg.Add(1)
                go disk.RunDiskStressTest(&wg, stop, errorChan, diskConfig, perfStats, debug, duration)
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
                rawDiskTestConfig := rawdisk.RawDiskTestConfig{
                    DevicePaths: rawDiskPaths,
                    TestSize:    fileSizeBytes,
                    BlockSize:   bs,
                    TestMode:    mode,
                    StartOffset: rawDiskStartOffsetBytes,
                }

                utils.LogMessage(fmt.Sprintf("Starting raw disk tests on %d devices: %v",
                    len(rawDiskPaths), rawDiskPaths), false)
                utils.LogMessage(fmt.Sprintf("Test size: %s, Block size: %s, Mode: %s, Start offset: %s",
                    utils.FormatSize(fileSizeBytes),
                    utils.FormatSize(bs),
                    mode,
                    utils.FormatSize(rawDiskStartOffsetBytes)), false)

                wg.Add(1)
                go rawdisk.RunRawDiskStressTest(&wg, stop, errorChan, rawDiskTestConfig, perfStats, debug, duration)
            }
        }
    }

    // CPU test
    if testCPU {
        rand.Seed(time.Now().UnixNano())

        numaInfo, err := utils.GetNUMAInfo()
        if err != nil {
            utils.LogMessage(fmt.Sprintf("Failed to get NUMA info: %v", err), true)
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
                utils.LogMessage(fmt.Sprintf("NUMA node %d has CPUs: %v", numaNode, selectedCPUs), false)
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
                utils.LogMessage(fmt.Sprintf("No CPU cores specified, using all %d cores: %v", numCores, selectedCPUs), false)
            } else if numCores > totalCores {
                numCores = totalCores
                selectedCPUs = allCPUs
                utils.LogMessage(fmt.Sprintf("Requested %d cores, but only %d available. Using %d cores: %v", cpuCores, totalCores, numCores, selectedCPUs), true)
            } else {
                selectedCPUs = randomSelectCores(allCPUs, numCores)
                utils.LogMessage(fmt.Sprintf("Randomly selected %d cores: %v", numCores, selectedCPUs), false)
            }
        } else {
            if numCores == 0 {
                numCores = len(selectedCPUs)
                utils.LogMessage(fmt.Sprintf("No CPU cores specified, using all %d cores in NUMA node %d: %v", numCores, numaNode, selectedCPUs), false)
            } else {
                selectedCPUs = randomSelectCores(selectedCPUs, numCores)
                utils.LogMessage(fmt.Sprintf("Randomly selected %d cores in NUMA node %d: %v", numCores, numaNode, selectedCPUs), false)
            }
        }

        testConfig := cpu.CPUConfig{
            NumCores:   numCores,
            Debug:      debug,
            CPUList:    selectedCPUs,
            LoadLevel:  cpuLoad,
        }
        utils.LogMessage(fmt.Sprintf("Starting CPU stress tests using %d cores (CPUs: %v) with load level: %s...", testConfig.NumCores, testConfig.CPUList, testConfig.LoadLevel), false)
        wg.Add(1)
        go cpu.RunCPUStressTests(&wg, stop, errorChan, testConfig, perfStats)
    }

    // Error handling and progress updates
    go func() {
        for err := range errorChan {
            if err == "" {
                continue
            }
            // Skip context canceled errors, treat as normal shutdown
            if strings.Contains(strings.ToLower(err), "context canceled") {
                if debug {
                    utils.LogMessage(fmt.Sprintf("Shutdown signal received: %s", err), true)
                }
                continue
            }
            if strings.Contains(err, "Test mode 'both' is not supported") {
                fmt.Println(err)
                os.Exit(1)
            }
            switch {
            case strings.Contains(err, "Integer") || strings.Contains(err, "Float") ||
                strings.Contains(err, "Vector") || strings.Contains(err, "Cache") ||
                strings.Contains(err, "Crypto"):
                results.CPU = "FAIL"
                errorDetails["CPU"] = append(errorDetails["CPU"], err)
            case strings.Contains(err, "Memory"):
                results.DIMM = "FAIL"
                errorDetails["DIMM"] = append(errorDetails["DIMM"], err)
            case strings.Contains(err, "Disk"):
                for _, mount := range mounts {
                    if strings.Contains(err, mount) {
                        results.Mountpoint[mount] = "FAIL"
                        errorDetails["Mountpoint_"+mount] = append(errorDetails["Mountpoint_"+mount], err)
                    }
                }
            case strings.Contains(err, "RawDisk"):
                for _, disk := range rawDiskPaths {
                    if strings.Contains(err, disk) {
                        results.RawDisk[disk] = "FAIL"
                        errorDetails["RawDisk_"+disk] = append(errorDetails["RawDisk_"+disk], err)
                    }
                }
            }
            utils.LogMessage(fmt.Sprintf("Error detected: %s", err), true)
        }
    }()

    // Progress updates
    progressTicker := time.NewTicker(120 * time.Second)
    // Record previous counts
    var lastIntegerCount, lastFloatCount, lastVectorCount, lastCacheCount, lastBranchCount, lastCryptoCount uint64
    go func() {
        for {
            select {
            case <-progressTicker.C:
                perfStats.Lock()
                // Calculate deltas
                deltaInteger := perfStats.CPU.IntegerCount - lastIntegerCount
                deltaFloat := perfStats.CPU.FloatCount - lastFloatCount
                deltaVector := perfStats.CPU.VectorCount - lastVectorCount
                deltaCache := perfStats.CPU.CacheCount - lastCacheCount
                deltaBranch := perfStats.CPU.BranchCount - lastBranchCount
                deltaCrypto := perfStats.CPU.CryptoCount - lastCryptoCount

                // Update last counts
                lastIntegerCount = perfStats.CPU.IntegerCount
                lastFloatCount = perfStats.CPU.FloatCount
                lastVectorCount = perfStats.CPU.VectorCount
                lastCacheCount = perfStats.CPU.CacheCount
                lastBranchCount = perfStats.CPU.BranchCount
                lastCryptoCount = perfStats.CPU.CryptoCount

                progressMsg := "Progress update"
                if testCPU {
                    progressMsg += fmt.Sprintf(", CPU Operations: Integer=%s, Float=%s, Vector=%s, Cache=%s, Branch=%s, Crypto=%s",
                        utils.FormatCount(deltaInteger), utils.FormatCount(deltaFloat), utils.FormatCount(deltaVector),
                        utils.FormatCount(deltaCache), utils.FormatCount(deltaBranch), utils.FormatCount(deltaCrypto))
                }
                if memoryPercent > 0 {
                    progressMsg += fmt.Sprintf(", Memory: R=%.2f MB/s W=%.2f MB/s Rand=%.2f MB/s",
                        perfStats.Memory.ReadSpeed, perfStats.Memory.WriteSpeed, perfStats.Memory.RandomAccessSpeed)
                }
                if len(rawDiskPaths) > 0 {
                    var totalRawDiskOps uint64
                    var bestRawDiskDevice string
                    for _, rawDisk := range perfStats.RawDisk {
                        ops := rawDisk.WriteCount + rawDisk.ReadCount
                        if ops > totalRawDiskOps {
                            totalRawDiskOps = ops
                            bestRawDiskDevice = rawDisk.DevicePath
                        }
                    }
                    progressMsg += fmt.Sprintf(", RawDisk(%s): Total Operations=%d",
                        bestRawDiskDevice, totalRawDiskOps)
                }
                perfStats.Unlock()
                utils.LogMessage(progressMsg, false)

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

    utils.LogMessage("=== PERFORMANCE RESULTS ===", false)
    var totalOperations uint64
    if testCPU {
        perfStats.Lock()
        cpuTotal := perfStats.CPU.IntegerCount + perfStats.CPU.FloatCount + perfStats.CPU.VectorCount +
            perfStats.CPU.CacheCount + perfStats.CPU.BranchCount + perfStats.CPU.CryptoCount
        utils.LogMessage(fmt.Sprintf("CPU Operations: Integer=%d, Float=%d, Vector=%d, Cache=%d, Branch=%d, Crypto=%d, Total=%d",
            perfStats.CPU.IntegerCount, perfStats.CPU.FloatCount, perfStats.CPU.VectorCount,
            perfStats.CPU.CacheCount, perfStats.CPU.BranchCount, perfStats.CPU.CryptoCount, cpuTotal), false)
        perfStats.Unlock()
        totalOperations += cpuTotal
    }
    if memoryPercent > 0 {
        memTotal := perfStats.Memory.WriteCount + perfStats.Memory.ReadCount + perfStats.Memory.RandomAccessCount
        perfStats.Lock()
        var avgReadSpeed, avgWriteSpeed, avgRandomAccessSpeed float64
        if perfStats.Memory.ReadSpeedCount > 0 {
            avgReadSpeed = perfStats.Memory.SumReadSpeed / float64(perfStats.Memory.ReadSpeedCount)
        }
        if perfStats.Memory.WriteSpeedCount > 0 {
            avgWriteSpeed = perfStats.Memory.SumWriteSpeed / float64(perfStats.Memory.WriteSpeedCount)
        }
        if perfStats.Memory.RandomAccessCount > 0 {
            avgRandomAccessSpeed = perfStats.Memory.SumRandomAccessSpeed / float64(perfStats.Memory.RandomAccessCount)
        }
        utils.LogMessage(fmt.Sprintf("Memory Performance - Read: %.2f MB/s (Min=%.2f, Max=%.2f, Avg=%.2f), Write: %.2f MB/s (Min=%.2f, Max=%.2f, Avg=%.2f), Random Access: %.2f MB/s (Min=%.2f, Max=%.2f, Avg=%.2f)",
            perfStats.Memory.ReadSpeed, perfStats.Memory.MinReadSpeed, perfStats.Memory.MaxReadSpeed, avgReadSpeed,
            perfStats.Memory.WriteSpeed, perfStats.Memory.MinWriteSpeed, perfStats.Memory.MaxWriteSpeed, avgWriteSpeed,
            perfStats.Memory.RandomAccessSpeed, perfStats.Memory.MinRandomAccessSpeed, perfStats.Memory.MaxRandomAccessSpeed, avgRandomAccessSpeed), false)
        utils.LogMessage(fmt.Sprintf("Memory Operations: Write=%d, Read=%d, Random Access=%d, Total=%d",
            perfStats.Memory.WriteCount, perfStats.Memory.ReadCount, perfStats.Memory.RandomAccessCount, memTotal), false)
        perfStats.Unlock()
        totalOperations += memTotal
    }
    if len(mounts) > 0 {
        utils.LogMessage("Mountpoint test:", false)
        for _, disk := range perfStats.Disk {
            utils.LogMessage(fmt.Sprintf("  Mount: %s, Mode: %s, Block: %s",
                disk.MountPoint, disk.Mode, utils.FormatSize(disk.BlockSize)), false)
        }
    }
    if len(rawDiskPaths) > 0 {
        var rawDiskTotal uint64
        utils.LogMessage("Raw Disk Operations:", false)
        for _, rawDisk := range perfStats.RawDisk {
            rawDiskOps := rawDisk.WriteCount + rawDisk.ReadCount
            utils.LogMessage(fmt.Sprintf("  RawDisk: %s, Mode: %s, Block: %s - Operations: Write=%d, Read=%d, Total=%d",
                rawDisk.DevicePath, rawDisk.Mode, utils.FormatSize(rawDisk.BlockSize),
                rawDisk.WriteCount, rawDisk.ReadCount, rawDiskOps), false)
            rawDiskTotal += rawDiskOps
        }
        utils.LogMessage(fmt.Sprintf("RawDisk Total Operations: %d", rawDiskTotal), false)
        totalOperations += rawDiskTotal
    }

    utils.LogMessage(fmt.Sprintf("Total Operations Across All Tests without Disk: %d", totalOperations), false)

    resultStr := fmt.Sprintf("Stress Test Summary - Duration: %s", elapsedTime.Round(time.Second))
    if testCPU {
        resultStr += fmt.Sprintf(" | CPU: %s", results.CPU)
    }
    if memoryPercent > 0 {
        resultStr += fmt.Sprintf(" | DIMM: %s", results.DIMM)
    }
    for mount, status := range results.Mountpoint {
        resultStr += fmt.Sprintf(" | Mountpoint(%s): %s", mount, status)
    }
    for disk, status := range results.RawDisk {
        resultStr += fmt.Sprintf(" | RawDisk(%s): %s", disk, status)
    }

    for component, errors := range errorDetails {
        if len(errors) > 0 {
            resultStr += fmt.Sprintf("\n%s FAIL reason: %s", component, errors[0])
            if len(errors) > 1 {
                resultStr += fmt.Sprintf(" (and %d more errors)", len(errors)-1)
            }
        }
    }

    utils.LogMessage(resultStr, false)
    utils.LogMessage("Stress test completed!", false)

    // Clear perfStats to free memory
    perfStats.Lock()
    perfStats.CPU.IntegerCount = 0
    perfStats.CPU.FloatCount = 0
    perfStats.CPU.VectorCount = 0
    perfStats.CPU.CacheCount = 0
    perfStats.CPU.BranchCount = 0
    perfStats.CPU.CryptoCount = 0
    perfStats.Disk = nil
    perfStats.RawDisk = nil
    perfStats.Unlock()
}

// randomSelectCores 使用 Fisher-Yates 洗牌演算法隨機選擇指定數量的 CPU 核心
func randomSelectCores(cpus []int, count int) []int {
    if count >= len(cpus) {
        return append([]int{}, cpus...)
    }

    // 複製輸入切片以避免修改原始資料
    result := make([]int, len(cpus))
    copy(result, cpus)

    // Fisher-Yates 洗牌，只洗牌前 count 個元素
    for i := 0; i < count && i < len(result); i++ {
        j := i + rand.Intn(len(result)-i)
        result[i], result[j] = result[j], result[i]
    }

    // 返回前 count 個元素
    return result[:count]
}
