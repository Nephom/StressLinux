package utils

import (
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "path/filepath"
    "runtime"
    "strconv"
    "strings"
    "time"
    "math/rand"
    "stress/config"
    "syscall"
    "unsafe"
    "os/exec"
)

// Logger function to handle both console output and file logging
func LogMessage(message string, debug bool) error {
    timestamp := time.Now().Format("2006-01-02 15:04:05")
    logEntry := fmt.Sprintf("%s | %s", timestamp, message)

    // Check if stress.log already exists
    fileInfo, err := os.Stat("stress.log")
    var creationTime string

    if os.IsNotExist(err) {
        creationTime = timestamp
        f, err := os.Create("stress.log")
        if err != nil {
            return fmt.Errorf("failed to create stress.log: %v", err)
        }
        defer f.Close()
        _, err = f.WriteString(fmt.Sprintf("Log file created at: %s\n", creationTime))
        if err != nil {
            return fmt.Errorf("failed to write creation time: %v", err)
        }
    } else {
        creationTime = fileInfo.ModTime().Format("2006-01-02 15:04:05")
    }

    f, err := os.OpenFile("stress.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return fmt.Errorf("failed to open stress.log: %v", err)
    }
    defer f.Close()

    logger := log.New(f, "", 0)
    logger.Println(logEntry)

    if debug {
        fmt.Println(logEntry)
    }

    return nil
}

// FormatSize converts bytes to human-readable string (KB, MB, GB)
func FormatSize(size int64) string {
    const (
        KB = 1024
        MB = KB * 1024
        GB = MB * 1024
    )

    if size >= GB {
        return fmt.Sprintf("%.2fGB", float64(size)/float64(GB))
    }
    if size >= MB {
        return fmt.Sprintf("%.2fMB", float64(size)/float64(MB))
    }
    if size >= KB {
        return fmt.Sprintf("%.2fKB", float64(size)/float64(KB))
    }

    return fmt.Sprintf("%dB", size)
}

// ParseSize parses size string with units (e.g., 4K, 64K, 1G)
func ParseSize(sizeStr string) (int64, error) {
    sizeStr = strings.ToUpper(sizeStr)
    var multiplier int64 = 1

    if strings.HasSuffix(sizeStr, "K") {
        multiplier = 1024
        sizeStr = sizeStr[:len(sizeStr)-1]
    } else if strings.HasSuffix(sizeStr, "KB") {
        multiplier = 1024
        sizeStr = sizeStr[:len(sizeStr)-2]
    } else if strings.HasSuffix(sizeStr, "M") {
        multiplier = 1024 * 1024
        sizeStr = sizeStr[:len(sizeStr)-1]
    } else if strings.HasSuffix(sizeStr, "MB") {
        multiplier = 1024 * 1024
        sizeStr = sizeStr[:len(sizeStr)-2]
    } else if strings.HasSuffix(sizeStr, "G") {
        multiplier = 1024 * 1024 * 1024
        sizeStr = sizeStr[:len(sizeStr)-1]
    } else if strings.HasSuffix(sizeStr, "GB") {
        multiplier = 1024 * 1024 * 1024
        sizeStr = sizeStr[:len(sizeStr)-2]
    }

    size, err := strconv.ParseInt(sizeStr, 10, 64)
    if err != nil {
        return 0, err
    }

    return size * multiplier, nil
}

// NUMAInfo holds information about NUMA nodes
type NUMAInfo struct {
    NumNodes int
    NodeCPUs [][]int
}

// GetNUMAInfo retrieves NUMA node information (Linux-specific)
func GetNUMAInfo() (NUMAInfo, error) {
    info := NUMAInfo{
        NumNodes: 1,
        NodeCPUs: make([][]int, 0),
    }

    nodeDir := "/sys/devices/system/node"
    if _, err := os.Stat(nodeDir); os.IsNotExist(err) {
        cpus := make([]int, runtime.NumCPU())
        for i := 0; i < runtime.NumCPU(); i++ {
            cpus[i] = i
        }
        info.NodeCPUs = append(info.NodeCPUs, cpus)
        return info, nil
    }

    files, err := os.ReadDir(nodeDir)
    if err != nil {
        return info, err
    }

    for _, file := range files {
        if !file.IsDir() || !strings.HasPrefix(file.Name(), "node") {
            continue
        }

        nodeID, err := strconv.Atoi(strings.TrimPrefix(file.Name(), "node"))
        if err != nil {
            continue
        }

        if nodeID >= len(info.NodeCPUs) {
            newSize := nodeID + 1
            if len(info.NodeCPUs) < newSize {
                oldSize := len(info.NodeCPUs)
                info.NodeCPUs = append(info.NodeCPUs, make([][]int, newSize-oldSize)...)
            }
        }

        cpuList, err := os.ReadFile(filepath.Join(nodeDir, file.Name(), "cpulist"))
        if err != nil {
            continue
        }

        cpus := make([]int, 0)
        for _, segment := range strings.Split(strings.TrimSpace(string(cpuList)), ",") {
            if strings.Contains(segment, "-") {
                parts := strings.Split(segment, "-")
                if len(parts) != 2 {
                    continue
                }
                start, err := strconv.Atoi(parts[0])
                if err != nil {
                    continue
                }
                end, err := strconv.Atoi(parts[1])
                if err != nil {
                    continue
                }
                for i := start; i <= end; i++ {
                    cpus = append(cpus, i)
                }
            } else {
                cpu, err := strconv.Atoi(segment)
                if err != nil {
                    continue
                }
                cpus = append(cpus, cpu)
            }
        }

        info.NodeCPUs[nodeID] = cpus
    }

    info.NumNodes = 0
    for i := range info.NodeCPUs {
        if len(info.NodeCPUs[i]) > 0 {
            info.NumNodes = i + 1
        }
    }

    return info, nil
}

// NewRand creates a new random number generator with the given seed
func NewRand(seed int64) *rand.Rand {
    return rand.New(rand.NewSource(seed))
}

// getCacheInfo retrieves L1, L2, and L3 cache sizes from the system
func GetCacheInfo() (config.CacheInfo, error) {
    cacheInfo := config.CacheInfo{}
    cacheDir := "/sys/devices/system/cpu/cpu0/cache"

    for i := 0; i <= 3; i++ {
        levelPath := filepath.Join(cacheDir, fmt.Sprintf("index%d/level", i))
        sizePath := filepath.Join(cacheDir, fmt.Sprintf("index%d/size", i))
        typePath := filepath.Join(cacheDir, fmt.Sprintf("index%d/type", i))

        levelData, err := ioutil.ReadFile(levelPath)
        if err != nil {
            continue
        }
        level, err := strconv.Atoi(strings.TrimSpace(string(levelData)))
        if err != nil {
            continue
        }

        typeData, err := ioutil.ReadFile(typePath)
        if err != nil {
            continue
        }
        cacheType := strings.TrimSpace(string(typeData))
        if cacheType != "Data" && cacheType != "Unified" {
            continue
        }

        sizeData, err := ioutil.ReadFile(sizePath)
        if err != nil {
            continue
        }
        sizeStr := strings.TrimSpace(string(sizeData))
        size, err := ParseCacheSize(sizeStr)
        if err != nil {
            continue
        }

        switch level {
        case 1:
            cacheInfo.L1Size = size
        case 2:
            cacheInfo.L2Size = size
        case 3:
            cacheInfo.L3Size = size
        }
    }

    if cacheInfo.L1Size == 0 {
        cacheInfo.L1Size = 32 * 1024
    }
    if cacheInfo.L2Size == 0 {
        cacheInfo.L2Size = 256 * 1024
    }
    if cacheInfo.L3Size == 0 {
        cacheInfo.L3Size = 0
    }

    return cacheInfo, nil
}

// ParseCacheSize converts cache size string (e.g., "32K", "4M") to bytes
func ParseCacheSize(sizeStr string) (int64, error) {
    sizeStr = strings.TrimSpace(sizeStr)
    if len(sizeStr) == 0 {
        return 0, fmt.Errorf("empty cache size string")
    }

    unit := sizeStr[len(sizeStr)-1:]
    valueStr := sizeStr[:len(sizeStr)-1]
    value, err := strconv.ParseInt(valueStr, 10, 64)
    if err != nil {
        return 0, fmt.Errorf("invalid cache size value: %v", err)
    }

    switch strings.ToUpper(unit) {
    case "K":
        return value * 1024, nil
    case "M":
        return value * 1024 * 1024, nil
    case "G":
        return value * 1024 * 1024 * 1024, nil
    default:
        return 0, fmt.Errorf("unknown cache size unit: %s", unit)
    }
}

// GetDiskSize returns the total size of the specified disk device (in bytes)
func GetDiskSize(devicePath string) (int64, error) {
    file, err := os.OpenFile(devicePath, os.O_RDONLY, 0)
    if err != nil {
        return 0, fmt.Errorf("failed to open device %s: %v", devicePath, err)
    }
    defer file.Close()

    var size int64
    _, _, errno := syscall.Syscall(
        syscall.SYS_IOCTL,
        file.Fd(),
        0x80081272, // BLKGETSIZE64
        uintptr(unsafe.Pointer(&size)),
    )
    if errno != 0 {
        return 0, fmt.Errorf("ioctl BLKGETSIZE64 failed for %s: %v", devicePath, errno)
    }

    return size, nil
}

// GetNICSpeed retrieves the speed of the network interface in Mbps
func GetNICSpeed(iface string) (int64, error) {
    speedPath := fmt.Sprintf("/sys/class/net/%s/speed", iface)
    data, err := os.ReadFile(speedPath)
    if err != nil {
        return 0, fmt.Errorf("failed to read NIC speed for %s: %v", iface, err)
    }
    speed, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
    if err != nil {
        return 0, fmt.Errorf("invalid NIC speed value for %s: %v", iface, err)
    }
    return speed, nil
}

// GetInterfaceForIP finds the network interface associated with the given IP
func GetInterfaceForIP(ip string) (string, error) {
    ifaces, err := os.ReadDir("/sys/class/net")
    if err != nil {
        return "", fmt.Errorf("failed to list network interfaces: %v", err)
    }

    for _, iface := range ifaces {
        if iface.IsDir() {
            addrPath := fmt.Sprintf("/sys/class/net/%s/address", iface.Name())
            data, err := os.ReadFile(addrPath)
            if err != nil {
                continue
            }
            mac := strings.TrimSpace(string(data))

            cmd := exec.Command("ip", "addr", "show", iface.Name())
            output, err := cmd.Output()
            if err != nil {
                continue
            }
            if strings.Contains(string(output), ip) {
                return iface.Name(), nil
            }
        }
    }
    return "", fmt.Errorf("no interface found for IP %s", ip)
}

// EnableBBR enables TCP BBR congestion control if not already enabled
func EnableBBR() error {
    // Check if BBR module is loaded
    _, err := os.Stat("/sys/module/tcp_bbr")
    if os.IsNotExist(err) {
        cmd := exec.Command("modprobe", "tcp_bbr")
        if err := cmd.Run(); err != nil {
            return fmt.Errorf("failed to load tcp_bbr module: %v", err)
        }
    }

    // Check current congestion control
    data, err := os.ReadFile("/proc/sys/net/ipv4/tcp_congestion_control")
    if err != nil {
        return fmt.Errorf("failed to read tcp_congestion_control: %v", err)
    }
    if strings.TrimSpace(string(data)) == "bbr" {
        return nil
    }

    // Enable BBR
    cmd := exec.Command("sysctl", "-w", "net.ipv4.tcp_congestion_control=bbr")
    if err := cmd.Run(); err != nil {
        return fmt.Errorf("failed to enable BBR: %v", err)
    }
    return nil
}

// NetworkConfig defines desired network optimization settings
type NetworkConfig struct {
    SysctlSettings map[string]string
    MTU            int
    FileDescriptors string
}

// DefaultNetworkConfig returns the recommended settings for high-speed networks
func DefaultNetworkConfig() NetworkConfig {
    return NetworkConfig{
        SysctlSettings: map[string]string{
            "net.core.rmem_max":                "67108864",
            "net.core.wmem_max":                "67108864",
            "net.ipv4.tcp_rmem":                "4096 87380 67108864",
            "net.ipv4.tcp_wmem":                "4096 65536 67108864",
            "net.core.netdev_max_backlog":      "10000",
            "net.core.somaxconn":               "65535",
            "net.ipv4.tcp_slow_start_after_idle": "0",
        },
        MTU:            9000,
        FileDescriptors: "1048576",
    }
}

// ConfigureNetwork applies network optimizations if not already set
func ConfigureNetwork(ips []string, debug bool) error {
    config := DefaultNetworkConfig()
    var errors []string

    // Configure sysctl settings
    for key, desired := range config.SysctlSettings {
        current, err := getSysctl(key)
        if err != nil {
            errors = append(errors, fmt.Sprintf("failed to read %s: %v", key, err))
            continue
        }
        if strings.TrimSpace(current) != desired {
            cmd := exec.Command("sysctl", "-w", fmt.Sprintf("%s=%s", key, desired))
            if err := cmd.Run(); err != nil {
                errors = append(errors, fmt.Sprintf("failed to set %s to %s: %v", key, desired, err))
            } else {
                LogMessage(fmt.Sprintf("INFO: Set %s to %s", key, desired), debug)
            }
        } else {
            LogMessage(fmt.Sprintf("INFO: %s already set to %s", key, desired), debug)
        }
    }

    // Configure MTU for interfaces
    for _, ip := range ips {
        iface, err := GetInterfaceForIP(ip)
        if err != nil {
            errors = append(errors, fmt.Sprintf("failed to find interface for IP %s: %v", ip, err))
            continue
        }

        currentMTU, err := getMTU(iface)
        if err != nil {
            errors = append(errors, fmt.Sprintf("failed to read MTU for %s: %v", iface, err))
            continue
        }
        if currentMTU != config.MTU {
            cmd := exec.Command("ip", "link", "set", iface, "mtu", strconv.Itoa(config.MTU))
            if err := cmd.Run(); err != nil {
                errors = append(errors, fmt.Sprintf("failed to set MTU %d for %s: %v", config.MTU, iface, err))
            } else {
                LogMessage(fmt.Sprintf("INFO: Set MTU to %d for %s", config.MTU, iface), debug)
            }
        } else {
            LogMessage(fmt.Sprintf("INFO: MTU already set to %d for %s", config.MTU, iface), debug)
        }

        // Check NIC speed and enable BBR if ≥100Gbps
        speed, err := GetNICSpeed(iface)
        if err != nil {
            errors = append(errors, fmt.Sprintf("failed to get NIC speed for %s: %v", iface, err))
            continue
        }
        LogMessage(fmt.Sprintf("INFO: NIC %s speed: %d Mbps", iface, speed), debug)
        if speed >= 100000 { // 100Gbps = 100000 Mbps
            LogMessage(fmt.Sprintf("INFO: High-speed NIC detected for %s, enabling BBR", iface), debug)
            if err := EnableBBR(); err != nil {
                errors = append(errors, fmt.Sprintf("failed to enable BBR for %s: %v", iface, err))
            } else {
                LogMessage(fmt.Sprintf("INFO: TCP BBR enabled for %s", iface), debug)
            }
        }
    }

    // Configure file descriptors
    currentSoft, currentHard, err := getFileDescriptors()
    desiredFD := config.FileDescriptors
    if err != nil {
        errors = append(errors, fmt.Sprintf("failed to read file descriptors: %v", err))
    } else if currentSoft != desiredFD || currentHard != desiredFD {
        err := setFileDescriptors(desiredFD)
        if err != nil {
            errors = append(errors, fmt.Sprintf("failed to set file descriptors to %s: %v", desiredFD, err))
        } else {
            LogMessage(fmt.Sprintf("INFO: Set file descriptors to %s", desiredFD), debug)
        }
    } else {
        LogMessage(fmt.Sprintf("INFO: File descriptors already set to %s", desiredFD), debug)
    }

    if len(errors) > 0 {
        return fmt.Errorf("network configuration errors: %s", strings.Join(errors, "; "))
    }
    return nil
}

// getSysctl retrieves the current value of a sysctl parameter
func getSysctl(key string) (string, error) {
    data, err := os.ReadFile(fmt.Sprintf("/proc/sys/%s", strings.ReplaceAll(key, ".", "/")))
    if err != nil {
        return "", err
    }
    return strings.TrimSpace(string(data)), nil
}

// getMTU retrieves the current MTU for a network interface
func getMTU(iface string) (int, error) {
    cmd := exec.Command("ip", "link", "show", iface)
    output, err := cmd.Output()
    if err != nil {
        return 0, err
    }
    lines := strings.Split(string(output), "\n")
    for _, line := range lines {
        if strings.Contains(line, "mtu") {
            fields := strings.Fields(line)
            for _, field := range fields {
                if strings.HasPrefix(field, "mtu") {
                    mtuStr := strings.TrimPrefix(field, "mtu")
                    return strconv.Atoi(mtuStr)
                }
            }
        }
    }
    return 0, fmt.Errorf("MTU not found for %s", iface)
}

// getFileDescriptors retrieves the current soft and hard file descriptor limits
func getFileDescriptors() (string, string, error) {
    cmd := exec.Command("ulimit", "-Sn")
    softOut, err := cmd.Output()
    if err != nil {
        return "", "", err
    }
    soft := strings.TrimSpace(string(softOut))

    cmd = exec.Command("ulimit", "-Hn")
    hardOut, err := cmd.Output()
    if err != nil {
        return "", "", err
    }
    hard := strings.TrimSpace(string(hardOut))

    return soft, hard, nil
}

// setFileDescriptors sets the soft and hard file descriptor limits
func setFileDescriptors(limit string) error {
    // Write to /etc/security/limits.conf
    entry := fmt.Sprintf("* soft nofile %s\n* hard nofile %s\n", limit, limit)
    if err := os.WriteFile("/etc/security/limits.conf", []byte(entry), 0644); err != nil {
        return fmt.Errorf("failed to write limits.conf: %v", err)
    }

    // Apply immediately
    cmd := exec.Command("ulimit", "-Sn", limit)
    if err := cmd.Run(); err != nil {
        return fmt.Errorf("failed to set soft limit: %v", err)
    }
    cmd = exec.Command("ulimit", "-Hn", limit)
    if err := cmd.Run(); err != nil {
        return fmt.Errorf("failed to set hard limit: %v", err)
    }
    return nil
}
