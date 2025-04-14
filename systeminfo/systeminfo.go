package systeminfo

import (
        "fmt"
        "os"
        "os/exec"
        "path/filepath"
        "runtime" // 新增，用於獲取總 CPU 核心數
        "strconv"
        "strings"
        "syscall"

        gcpu "github.com/shirou/gopsutil/v4/cpu"
        gdisk "github.com/shirou/gopsutil/v4/disk"
        gmem "github.com/shirou/gopsutil/v4/mem"
        gnet "github.com/shirou/gopsutil/v4/net"
        "stress/utils" // 新增，導入 utils 包以獲取 NUMA 信息
)

// SystemInfo holds information about CPU, memory, disk, and raw disk availability.
type SystemInfo struct {
        CPUInfo       string
        MemoryInfo    string
        DiskMounts    string
        RawDisks      string
        NUMAInfo      string // 新增字段，存儲格式化的 NUMA 信息
        DetailedInfo  DetailedSystemInfo
}

// DetailedSystemInfo holds detailed system information.
type DetailedSystemInfo struct {
        CPUDetails    string
        MemoryDetails string
        DiskDetails   string
        NetworkDetails string
}

// GetSystemInfo retrieves system resource information for stress testing.
func GetSystemInfo() SystemInfo {
        var info SystemInfo

        // CPU information
        cpuInfo, err := gcpu.Info()
        if err != nil || len(cpuInfo) == 0 {
                info.CPUInfo = "CPU Info: Unable to retrieve CPU information"
        } else {
                // Get total number of cores
                totalCores, _ := gcpu.Counts(true)
                info.CPUInfo = fmt.Sprintf("CPU Info: Model: %s, Cores: %d, Frequency: %.2f MHz",
                        cpuInfo[0].ModelName, totalCores, cpuInfo[0].Mhz)
        }

        // Memory information
        vm, err := gmem.VirtualMemory()
        if err != nil {
                info.MemoryInfo = "Memory Info: Unable to retrieve memory information"
        } else {
                availablePercent := float64(vm.Available) / float64(vm.Total) * 100
                stressPercent := availablePercent / 10.0
                if stressPercent > 9.5 {
                        stressPercent = 9.5
                }
                info.MemoryInfo = fmt.Sprintf("Memory Info: Available for stress: %.1f%% (%.1f)",
                        availablePercent, stressPercent)
        }

        // Disk partitions information
        partitions, err := gdisk.Partitions(true)
        if err != nil {
                info.DiskMounts = "Disk Mount Points: Unable to retrieve mount points"
                partitions = []gdisk.PartitionStat{}
        }

        // Detect ZFS Pool devices
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
                info.DiskMounts = "ZFS Info: Unable to retrieve zpool status"
        }

        for _, p := range partitions {
                if strings.ToLower(p.Fstype) == "zfs" {
                        zfsMounts[p.Mountpoint] = true
                }
        }

        // Detect LVM
        lvmDisks := make(map[string]bool)
        lvmMounts := make(map[string]bool)
        for _, p := range partitions {
                if strings.HasPrefix(p.Device, "/dev/mapper/") {
                        lvmMounts[p.Mountpoint] = true
                        if strings.HasPrefix(p.Device, "/dev/dm-") {
                                lvmDisks["/dev/sdb"] = true // TODO: Dynamically resolve dm to physical device
                        }
                }
        }

        // Detect MD RAID
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

        // Available mount points
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
                info.DiskMounts = fmt.Sprintf("Disk Mount Points Available for Stress: %s",
                        strings.Join(nonSystemMounts, ", "))
        } else {
                info.DiskMounts = "Disk Mount Points: None available, please check df by yourself!"
        }

        // Raw disk detection
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
                info.RawDisks = fmt.Sprintf("Raw Disks Available for Stress: %s",
                        strings.Join(rawDisks, ", "))
        } else {
                info.RawDisks = "Raw Disks: None available"
        }

        // NUMA information
        numaInfo, err := utils.GetNUMAInfo()
        if err != nil {
                info.NUMAInfo = fmt.Sprintf("NUMA Info: Failed to retrieve NUMA information: %v", err)
        } else {
                var numaDetails []string
                numaDetails = append(numaDetails, fmt.Sprintf("NUMA Nodes: %d", numaInfo.NumNodes))
                for i, cpus := range numaInfo.NodeCPUs {
                        if len(cpus) > 0 {
                                numaDetails = append(numaDetails, fmt.Sprintf("Node %d CPUs: %v", i, cpus))
                        }
                }
                numaDetails = append(numaDetails, fmt.Sprintf("Total CPU cores: %d", runtime.NumCPU()))
                info.NUMAInfo = strings.Join(numaDetails, "\n")
        }

        // Get detailed system information
        info.DetailedInfo = getDetailedSystemInfo()

        return info
}

// getDetailedSystemInfo collects detailed system information.
func getDetailedSystemInfo() DetailedSystemInfo {
        var details DetailedSystemInfo

        // Detailed CPU information
        cpuInfo, err := gcpu.Info()
        if err == nil && len(cpuInfo) > 0 {
                // Get total number of cores
                totalCores, _ := gcpu.Counts(true)
                
                details.CPUDetails = fmt.Sprintf(
                        "[CPU]: Model: %s, Cores: %d, Frequency: %.2f MHz, Cache: %d KB",
                        cpuInfo[0].ModelName, totalCores, cpuInfo[0].Mhz, cpuInfo[0].CacheSize)
        } else {
                details.CPUDetails = "[CPU]: Unable to retrieve detailed CPU information"
        }

        var memoryDetails []string
        // Detailed Memory information
        vm, err := gmem.VirtualMemory()
        if err == nil {
                memoryDetails = append(memoryDetails, fmt.Sprintf(
                        "System Memory: Total: %.2f GB, Available: %.2f GB, Used: %.2f GB, Free: %.2f GB, Percent Used: %.1f%%",
                        float64(vm.Total)/1024/1024/1024, float64(vm.Available)/1024/1024/1024,
                        float64(vm.Used)/1024/1024/1024, float64(vm.Free)/1024/1024/1024, vm.UsedPercent))
        }

        // Detailed memory device info from dmidecode
        if output, err := exec.Command("dmidecode", "-t", "17").CombinedOutput(); err == nil {
                lines := strings.Split(string(output), "\n")
                var currentDevice map[string]string
                var devices []map[string]string

                for _, line := range lines {
                        line = strings.TrimSpace(line)
                        if line == "" {
                                continue
                        }
                        if strings.HasPrefix(line, "Memory Device") {
                                if currentDevice != nil && currentDevice["Size"] != "No Module Installed" {
                                        devices = append(devices, currentDevice)
                                }
                                currentDevice = make(map[string]string)
                        } else if currentDevice != nil && strings.Contains(line, ": ") {
                                parts := strings.SplitN(line, ": ", 2)
                                if len(parts) == 2 {
                                        key := strings.TrimSpace(parts[0])
                                        value := strings.TrimSpace(parts[1])
                                        currentDevice[key] = value
                                }
                        }
                }
                // Append the last device if valid
                if currentDevice != nil && currentDevice["Size"] != "No Module Installed" {
                        devices = append(devices, currentDevice)
                }

                if len(devices) > 0 {
                        for i, device := range devices {
                                manufacturer := device["Manufacturer"]
                                if manufacturer == "" || manufacturer == "Not Specified" {
                                        manufacturer = "Unknown"
                                }
                                partNumber := device["Part Number"]
                                if partNumber == "" || partNumber == "Not Specified" {
                                        partNumber = "Unknown"
                                }
                                serialNumber := device["Serial Number"]
                                if serialNumber == "" || serialNumber == "Not Specified" {
                                        serialNumber = "Unknown"
                                }
                                size := device["Size"]
                                if size == "" || size == "Not Specified" {
                                        size = "Unknown"
                                }
                                memType := device["Type"]
                                if memType == "" || memType == "Not Specified" {
                                        memType = "Unknown"
                                }
                                speed := device["Speed"]
                                if speed == "" || speed == "Not Specified" || speed == "Unknown" {
                                        speed = "Unknown"
                                }
                                configuredSpeed := device["Configured Memory Speed"]
                                if configuredSpeed == "" || configuredSpeed == "Not Specified" || configuredSpeed == "Unknown" {
                                        configuredSpeed = "Unknown"
                                }
                                rank := device["Rank"]
                                if rank == "" || rank == "Not Specified" {
                                        rank = "Unknown"
                                }

                                memoryDetails = append(memoryDetails, fmt.Sprintf(
                                        "[Memory] %d: %s %s(%s) %s | %s | %s(Current: %s) | Rank %s",
                                        i, manufacturer, partNumber, serialNumber, size, memType, speed, configuredSpeed, rank))
                        }
                }
        } else {
                memoryDetails = append(memoryDetails, fmt.Sprintf(
                        "[Memory]: Unable to retrieve device information (requires dmidecode, try running with sudo): %v",
                        err))
        }

        if len(memoryDetails) > 0 {
                details.MemoryDetails = strings.Join(memoryDetails, "\n")
        } else {
                details.MemoryDetails = "[Memory]: Unable to retrieve memory information"
        }

        // Detailed Disk information (Physical Disks)
        var diskDetails []string
        sdDisks, _ := filepath.Glob("/dev/sd[a-z]")
        nvmeDisks, _ := filepath.Glob("/dev/nvme[0-9]n[0-9]")
        possibleDisks := append(sdDisks, nvmeDisks...)

        for _, diskPath := range possibleDisks {
                // Get disk model and size using lsblk
                cmd := exec.Command("lsblk", "-d", "-n", "-o", "MODEL,SIZE", diskPath)
                output, err := cmd.CombinedOutput() // Capture both stdout and stderr
                if err != nil {
                        // Log error for debugging
                        fmt.Fprintf(os.Stderr, "lsblk failed for %s: %v, output: %s\n", diskPath, err, string(output))
                        continue
                }

                // Parse lsblk output
                lines := strings.Split(strings.TrimSpace(string(output)), "\n")
                if len(lines) == 0 {
                        continue
                }

                // Split MODEL and SIZE
                fields := strings.Fields(lines[0])
                if len(fields) < 1 {
                        continue
                }

                // Model may be empty, handle it gracefully
                model := "Unknown Model"
                if len(fields) > 1 {
                        model = strings.TrimSpace(strings.Join(fields[:len(fields)-1], " "))
                        if model == "" {
                                model = "Unknown Model"
                        }
                }

                // Parse size (e.g., "447.1G" or "1T")
                sizeStr := strings.TrimSpace(fields[len(fields)-1])
                var sizeGB float64
                if strings.HasSuffix(sizeStr, "T") {
                        size, err := strconv.ParseFloat(strings.TrimSuffix(sizeStr, "T"), 64)
                        if err != nil {
                                continue
                        }
                        sizeGB = size * 1000
                } else if strings.HasSuffix(sizeStr, "G") {
                        size, err := strconv.ParseFloat(strings.TrimSuffix(sizeStr, "G"), 64)
                        if err != nil {
                                continue
                        }
                        sizeGB = size
                } else if strings.HasSuffix(sizeStr, "M") {
                        size, err := strconv.ParseFloat(strings.TrimSuffix(sizeStr, "M"), 64)
                        if err != nil {
                                continue
                        }
                        sizeGB = size / 1000
                } else {
                        continue // Skip if size format is unknown
                }

                diskDetails = append(diskDetails, fmt.Sprintf(
                        "[Disk]: %s | Model: %s | Size: %.2f GB",
                        diskPath, model, sizeGB))
        }

        if len(diskDetails) > 0 {
                details.DiskDetails = strings.Join(diskDetails, "\n")
        } else {
                details.DiskDetails = "[Disk]: Unable to retrieve physical disk information (requires lsblk, try running with sudo)"
        }

        // Detailed Network information
        netInterfaces, err := gnet.Interfaces()
        if err == nil {
                var netDetails []string
                for _, ni := range netInterfaces {
                        var addrs []string
                        for _, addr := range ni.Addrs {
                                addrs = append(addrs, addr.Addr)
                        }
                        netDetails = append(netDetails, fmt.Sprintf(
                                "Interface: %s, MAC: %s, IPs: %s, Flags: %v",
                                ni.Name, ni.HardwareAddr, strings.Join(addrs, ", "), ni.Flags))
                }
                details.NetworkDetails = strings.Join(netDetails, "\n")
        } else {
                details.NetworkDetails = "[Network]: Unable to retrieve network information"
        }

        return details
}

// PrintSystemInfo outputs the system information to the console.
func PrintSystemInfo(info SystemInfo) {
        fmt.Println("\n=== System Information ===")
        fmt.Println("CPU Details:")
        fmt.Println(info.DetailedInfo.CPUDetails)
        fmt.Println("\nMemory Details:")
        fmt.Println(info.DetailedInfo.MemoryDetails)
        fmt.Println("\nDisk Details:")
        fmt.Println(info.DetailedInfo.DiskDetails)
        fmt.Println("\nNetwork Details:")
        fmt.Println(info.DetailedInfo.NetworkDetails)
        fmt.Println("\n\n=== System Resources Available for Stress Testing ===")
        fmt.Println(info.CPUInfo)
        fmt.Println(info.NUMAInfo)
        fmt.Println(info.MemoryInfo)
        fmt.Println(info.DiskMounts)
        fmt.Println(info.RawDisks)
}
