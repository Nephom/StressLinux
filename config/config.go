// config/config.go
package config

import (
    "encoding/json"
    "os"
)

// LoadConfig loads configuration from config.json
func LoadConfig() (Config, error) {
    var config Config
    // Set default values
    config.Debug = false
    config.CPU = false
    config.Cores = 0
    config.Load = "Default"
    config.Memory = false
    config.MEMPercent = 0
    config.Mountpoint = ""
    config.RAWDisk = ""
    config.Size = "10M"
    config.Offset = "1G"
    config.Block = "4K"
    config.Mode = "both"
	config.NUMANode = -1

    data, err := os.ReadFile("config.json")
    if err != nil {
        return config, err
    }

	type ConfigWithDescription struct {
        Debug      bool    `json:"debug"`
        CPU        bool    `json:"CPU"`
        Cores      int     `json:"Cores"`
        Load       string  `json:"Load"`
        Memory     bool    `json:"Memory"`
        MEMPercent float64 `json:"MEMPercent"`
        Mountpoint string  `json:"Mountpoint"`
        RAWDisk    string  `json:"RAWDisk"`
        Size       string  `json:"Size"`
        Offset     string  `json:"Offset"`
        Block      string  `json:"Block"`
        Mode       string  `json:"Mode"`
        NUMANode   int     `json:"NUMANode"`
        DebugDesc      string `json:"debug_description,omitempty"`
        CPUDesc        string `json:"CPU_description,omitempty"`
        CoresDesc      string `json:"Cores_description,omitempty"`
        LoadDesc       string `json:"Load_description,omitempty"`
        MemoryDesc     string `json:"Memory_description,omitempty"`
        MEMPercentDesc string `json:"MEMPercent_description,omitempty"`
        MountpointDesc string `json:"Mountpoint_description,omitempty"`
        RAWDiskDesc    string `json:"RAWDisk_description,omitempty"`
        SizeDesc       string `json:"Size_description,omitempty"`
        OffsetDesc     string `json:"Offset_description,omitempty"`
        BlockDesc      string `json:"Block_description,omitempty"`
        ModeDesc       string `json:"Mode_description,omitempty"`
        NUMANodeDesc   string `json:"NUMANode_description,omitempty"`
	}

    var temp ConfigWithDescription
    err = json.Unmarshal(data, &temp)
    if err != nil {
        // 嘗試解析不帶說明的舊格式
        err = json.Unmarshal(data, &config)
        if err != nil {
            return config, err
        }
        return config, nil
    }

    // 轉換為 Config 結構體
    config = Config{
        Debug:      temp.Debug,
        CPU:        temp.CPU,
        Cores:      temp.Cores,
        Load:       temp.Load,
        Memory:     temp.Memory,
        MEMPercent: temp.MEMPercent,
        Mountpoint: temp.Mountpoint,
        RAWDisk:    temp.RAWDisk,
        Size:       temp.Size,
        Offset:     temp.Offset,
        Block:      temp.Block,
        Mode:       temp.Mode,
		NUMANode:   temp.NUMANode,
    }

    return config, nil
}
