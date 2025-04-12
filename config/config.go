// config/config.go
package config

import (
	"encoding/json"
	"os"
)

// LoadConfig loads configuration from config.json
func LoadConfig() (Config, error) {
	var config Config
	config.Debug = false // Default value

	data, err := os.ReadFile("config.json")
	if err != nil {
		return config, err
	}

	err = json.Unmarshal(data, &config)
	return config, err
}
