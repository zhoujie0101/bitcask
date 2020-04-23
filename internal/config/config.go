package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type Config struct {
	MaxDatafileSize int    `json:max_datafile_size`
	MaxKeySize      uint32 `json:max_key_size`
	MaxValueSize    uint64 `json:max_value_size`
	Sync            bool   `json:sync`
}

//Load config from file
func Load(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Save config to specific file
func (c *Config) Save(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}
