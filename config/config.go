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

    data, err := os.ReadFile("config.json")
    if err != nil {
        return config, err
    }

    err = json.Unmarshal(data, &config)
    return config, err
}
