package main

import (
        "flag"
        "fmt"
        "os"
        "os/exec"
        "runtime"
        "strings"
        "sync"
	"syscall"
        "time"
	"path/filepath"

        cfg "stress/config"
        "stress/cpu"
        "stress/disk"
        "stress/memory"
        "stress/rawdisk"
        "stress/utils"

	gcpu "github.com/shirou/gopsutil/v4/cpu"
	gdisk "github.com/shirou/gopsutil/v4/disk"
	gmem "github.com/shirou/gopsutil/v4/mem"
)

type stringSliceFlag []string

func (i *stringSliceFlag) String() string {
    return fmt.Sprintf("%v", *i)
}

func (i *stringSliceFlag) Set(value string) error {
    *i = append(*i, value)
    return nil
}

func getSystemInfo() {
    // CPU 資訊
    cpuInfo, err := gcpu.Info()
    if err != nil || len(cpuInfo) == 0 {
        fmt.Println("CPU Info: Unable to retrieve CPU information")
    } else {
        fmt.Printf("CPU Info: Model: %s, Cores: %d, Frequency: %.2f MHz\n",
            cpuInfo[0].ModelName, cpuInfo[0].Cores, cpuInfo[0].Mhz)
    }

    // 記憶體資訊
    vm, err := gmem.VirtualMemory()
    if err != nil {
        fmt.Println("Memory Info: Unable to retrieve memory information")
    } else {
        availablePercent := float64(vm.Available) / float64(vm.Total) * 100
        stressPercent := availablePercent / 10.0
        if stressPercent > 9.5 {
            stressPercent = 9.5
        }
        fmt.Printf("Memory Info: Available for stress: %.1f%% (%.1f)\n", availablePercent, stressPercent)
    }

    // 系統分區資訊
    partitions, err := gdisk.Partitions(true)
    if err != nil {
        fmt.Println("Disk Mount Points: Unable to retrieve mount points")
        partitions = []gdisk.PartitionStat{}
    }

    // 檢測 ZFS Pool 使用的實體裝置
    zfsDisks := make(map[string]bool)
    zfsMounts := make(map[string]bool)

    if out, err := exec.Command("zpool", "status").Output(); err == nil {
        lines := strings.Split(string(out), "\n")
        for _, line := range lines {
            fields := strings.Fields(line)
            if len(fields) > 0 {
                dev := fields[0]
                if strings.HasPrefix(dev, "/dev/") || strings.HasPrefix(dev, "ata-") || strings.HasPrefix(dev, "wwn-") {
                    fullPath := dev
                    if !strings.HasPrefix(dev, "/dev/") {
                        fullPath = "/dev/disk/by-id/" + dev
                    }
                    resolved, err := filepath.EvalSymlinks(fullPath)
                    if err == nil {
                        baseDevice := strings.TrimRight(resolved, "0123456789")
                        zfsDisks[baseDevice] = true
                    }
                }
            }
        }
    } else {
        fmt.Println("ZFS Info: Unable to retrieve zpool status")
    }

    for _, p := range partitions {
        if strings.ToLower(p.Fstype) == "zfs" {
            zfsMounts[p.Mountpoint] = true
        }
    }

    // 檢測 LVM
    lvmDisks := make(map[string]bool)
    lvmMounts := make(map[string]bool)
    for _, p := range partitions {
        if strings.HasPrefix(p.Device, "/dev/mapper/") {
            lvmMounts[p.Mountpoint] = true
            if strings.HasPrefix(p.Device, "/dev/dm-") {
                lvmDisks["/dev/sdb"] = true // TODO: 動態解析 dm 對應的實體裝置
            }
        }
    }

    // 檢測 MD RAID
    mdDisks := make(map[string]bool)
    mdMounts := make(map[string]bool)
    if mdstat, err := os.ReadFile("/proc/mdstat"); err == nil {
        lines := strings.Split(string(mdstat), "\n")
        for _, line := range lines {
            if strings.Contains(line, "md") && strings.Contains(line, ":") {
                parts := strings.Fields(line)
                for _, part := range parts {
                    if strings.HasPrefix(part, "sd") || strings.HasPrefix(part, "nvme") {
                        dev := "/dev/" + strings.TrimSuffix(part, "[0-9]")
                        mdDisks[dev] = true
                    }
                }
            }
        }
        for _, p := range partitions {
            if strings.HasPrefix(p.Device, "/dev/md") {
                mdMounts[p.Mountpoint] = true
            }
        }
    }

    // 可用掛載點
    nonSystemMounts := []string{}
    for _, p := range partitions {
        if strings.HasPrefix(p.Mountpoint, "/boot") ||
            strings.HasPrefix(p.Mountpoint, "/var") ||
            p.Mountpoint == "/" ||
            strings.HasPrefix(p.Mountpoint, "/snap") ||
            strings.HasPrefix(p.Mountpoint, "/dev") ||
            zfsMounts[p.Mountpoint] ||
            lvmMounts[p.Mountpoint] ||
            mdMounts[p.Mountpoint] {
            continue
        }
        usage, err := gdisk.Usage(p.Mountpoint)
        if err == nil && usage.Free > 1*1024*1024*1024 {
            nonSystemMounts = append(nonSystemMounts, p.Mountpoint)
        }
    }

    if len(nonSystemMounts) > 0 {
        fmt.Printf("Disk Mount Points Available for Stress: %s\n", strings.Join(nonSystemMounts, ", "))
    } else {
        fmt.Println("Disk Mount Points: None available, please check df by yourself!")
    }

    // Raw Disk 偵測
    rawDisks := []string{}
    sdDisks, _ := filepath.Glob("/dev/sd[a-z]")
    nvmeDisks, _ := filepath.Glob("/dev/nvme[0-9]n[0-9]")
    possibleDisks := append(sdDisks, nvmeDisks...)

    for _, diskPath := range possibleDisks {
        if _, err := os.Stat(diskPath); err != nil {
            continue
        }
        if zfsDisks[diskPath] || lvmDisks[diskPath] || mdDisks[diskPath] {
            continue
        }
        isSystemDisk := false
        for _, p := range partitions {
            if strings.HasPrefix(p.Device, diskPath) && p.Mountpoint == "/" {
                isSystemDisk = true
                break
            }
        }
        if !isSystemDisk {
            if stat, err := os.Stat(diskPath); err == nil {
                if stat.Mode()&os.ModeDevice != 0 {
                    var sysStat syscall.Stat_t
                    if err := syscall.Stat(diskPath, &sysStat); err == nil {
                        if sysStat.Rdev&0x7 == 0 {
                            rawDisks = append(rawDisks, diskPath)
                        }
                    }
                }
            }
        }
    }

    if len(rawDisks) > 0 {
        fmt.Printf("Raw Disks Available for Stress: %s\n", strings.Join(rawDisks, ", "))
    } else {
        fmt.Println("Raw Disks: None available")
    }
}


func main() {
        var mountPoints string
        var fileSize string
        var testMode string
        var blockSizes string
        var rawDiskPaths stringSliceFlag
        var duration string
        var testCPU bool
        var memoryPercent float64
        var debugFlag bool
        var showHelp bool
	var printSystemInfo bool

        // 為 raw disk 添加新的 flag
        rawDiskTestSize := flag.Int64("disksize", 100*1024*1024, "Size in bytes to test on each raw disk device")
        rawDiskBlockSize := flag.Int64("diskblock", 4*1024, "Block size in bytes for raw disk device testing")
        rawDiskTestMode := flag.String("diskmode", "both", "Test mode for raw disk device: 'sequential', 'random', or 'both'")
        rawDiskStartOffset := flag.Int64("diskoffset", 1024*1024*1024, "Start offset in bytes from the beginning of the raw device")

        flag.StringVar(&mountPoints, "l", "", "Comma separated mount points to test (e.g. /mnt/disk1,/mnt/disk2)")
        flag.Var(&rawDiskPaths, "disk", "Raw disk devices to test (e.g., /dev/sdb, /dev/nvme0n1)")
        flag.StringVar(&fileSize, "size", "10MB", "Size of test files (supports K, M, G units)")
        flag.StringVar(&testMode, "mode", "both", "Test mode: sequential, random, or both")
        flag.StringVar(&blockSizes, "block", "4K", "Comma separated block sizes for disk operations (supports K, M, G units)")
        flag.StringVar(&duration, "duration", "10m", "Test duration (e.g. 30s, 5m, 1h)")
        flag.BoolVar(&testCPU, "cpu", false, "Enable CPU testing")
        flag.Float64Var(&memoryPercent, "memory", 0, "Memory testing percentage (1-9 for 10%-90%, 1.5 for 15%, etc)")
        flag.BoolVar(&debugFlag, "d", false, "Enable debug mode")
        flag.BoolVar(&showHelp, "h", false, "Show help")
	flag.BoolVar(&printSystemInfo, "print", false, "Print available system resources for stress testing (alias: -list)")
	flag.BoolVar(&printSystemInfo, "list", false, "Alias for -print")
        flag.Parse()

	if printSystemInfo {
		fmt.Println("=== System Resources Available for Stress Testing ===")
		getSystemInfo()
		return
        }

        if (!testCPU && memoryPercent == 0 && mountPoints == "" && len(rawDiskPaths) == 0) || showHelp {
                fmt.Println("System Stress Test Tool")
                fmt.Println("Usage: stress [options]")
                fmt.Println("\nOptions:")
                flag.PrintDefaults()
                fmt.Println("\nAt least one of -cpu, -memory, -l, -disk must be specified.")
		fmt.Println("Use -print or -list to view available system resources.")
                return
        }

        configuration, err := cfg.LoadConfig()
        if err != nil {
                fmt.Printf("Failed to load config.json, using default settings: %v\n", err)
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
                CPU:     cfg.CPUPerformance{GFLOPS: 0.0},
                Memory:  cfg.MemoryPerformance{},
                Disk:    []cfg.DiskPerformance{},
                RawDisk: []cfg.RawDiskPerformance{}, // 初始化 RawDisk
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
		// Convert memory percentage (e.g., 7 means 70%, 1.5 means 15%)
		memUsagePercent := memoryPercent / 10.0
		if memUsagePercent > 0.95 {
			memUsagePercent = 0.95 // Cap at 95% for safety
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

	// Mount point disk tests
        var mounts []string
        if mountPoints != "" {
                mounts = strings.Split(mountPoints, ",")
                utils.LogMessage(fmt.Sprintf("Starting disk tests on mount points: %v", mounts), debug)
        }

        fileSizeBytes, err := utils.ParseSize(fileSize)
        if err != nil {
                utils.LogMessage(fmt.Sprintf("Invalid file size: %v, using default 10MB", err), true)
                fileSizeBytes = 10 * 1024 * 1024
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

        if len(mounts) > 0 {
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

                var rawTestModes []string
                switch *rawDiskTestMode {
                case "sequential":
                        rawTestModes = []string{"sequential"}
                case "random":
                        rawTestModes = []string{"random"}
                case "both":
                        rawTestModes = []string{"sequential", "random"}
                default:
                        utils.LogMessage(fmt.Sprintf("Invalid raw disk test mode: %s, using both", *rawDiskTestMode), true)
                        rawTestModes = []string{"sequential", "random"}
                }

                for _, mode := range rawTestModes {
                        rawDiskTestConfig := rawdisk.RawDiskTestConfig{
                                DevicePaths: rawDiskPaths,
                                TestSize:    *rawDiskTestSize,
                                BlockSize:   *rawDiskBlockSize,
                                TestMode:    mode,
                                StartOffset: *rawDiskStartOffset,
                        }

                        utils.LogMessage(fmt.Sprintf("Starting raw disk tests on %d devices: %v",
                                len(rawDiskPaths), rawDiskPaths), debug)
                        utils.LogMessage(fmt.Sprintf("Test size: %s, Block size: %s, Mode: %s, Start offset: %s",
                                utils.FormatSize(*rawDiskTestSize),
                                utils.FormatSize(*rawDiskBlockSize),
                                mode,
                                utils.FormatSize(*rawDiskStartOffset)), debug)

                        wg.Add(1)
                        go rawdisk.RunRawDiskStressTest(&wg, stop, errorChan, rawDiskTestConfig, perfStats, debug)
                }
        }

        if testCPU {
                cpuConfig := cpu.CPUConfig{
                        NumCores: runtime.NumCPU(),
                        Debug:    debug,
                }
                utils.LogMessage(fmt.Sprintf("Starting CPU stress tests using %d cores...", cpuConfig.NumCores), debug)
                wg.Add(1)
                go cpu.RunCPUStressTests(&wg, stop, errorChan, cpuConfig, perfStats)
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
                        case strings.Contains(err, "Disk") || strings.Contains(err, "RawDisk"): // 添加 RawDisk 錯誤檢測
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
                                for _, rawDisk := range perfStats.RawDisk { // 添加 raw disk 進度更新
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
        if testCPU {
                utils.LogMessage(fmt.Sprintf("CPU Performance: %.2f GFLOPS", perfStats.CPU.GFLOPS), true)
        }
        if memoryPercent > 0 {
                utils.LogMessage(fmt.Sprintf("Memory Performance - Read: %.2f MB/s, Write: %.2f MB/s",
                        perfStats.Memory.ReadSpeed, perfStats.Memory.WriteSpeed), true)
        }
        if len(mounts) > 0 {
                utils.LogMessage("Disk Performance:", true)
                for _, disk := range perfStats.Disk {
                        utils.LogMessage(fmt.Sprintf("  Mount: %s, Mode: %s, Block: %s - Read: %.2f MB/s, Write: %.2f MB/s",
                                disk.MountPoint, disk.Mode, utils.FormatSize(disk.BlockSize), disk.ReadSpeed, disk.WriteSpeed), true)
                }
        }
        // 添加 raw disk 性能結果輸出
        if len(rawDiskPaths) > 0 {
                printRawDiskPerformanceResults(perfStats)
        }

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

        // Group by device path
        deviceResults := make(map[string][]cfg.RawDiskPerformance)
        for _, result := range perfStats.RawDisk {
                deviceResults[result.DevicePath] = append(deviceResults[result.DevicePath], result)
        }

        for device, results := range deviceResults {
                utils.LogMessage(fmt.Sprintf("\nDevice: %s", device), true)
                utils.LogMessage(fmt.Sprintf("%-12s %-10s %-12s %-15s %-15s", "Mode", "Block Size", "Block Size", "Read Speed", "Write Speed"), true)
                utils.LogMessage(fmt.Sprintf("%-12s %-10s %-12s %-15s %-15s", "----", "----------", "----------", "----------", "-----------"), true)

                for _, result := range results {
                        utils.LogMessage(fmt.Sprintf("%-12s %-10d %-12s %-15.2f %-15.2f",
                                result.Mode,
                                result.BlockSize,
                                utils.FormatSize(result.BlockSize),
                                result.ReadSpeed,
                                result.WriteSpeed), true)
                }
        }
}
