package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	MQTTAddr      string        `yaml:"mqtt_addr"`
	MQTTTLSAddr   string        `yaml:"mqtt_tls_addr"`
	MetricsAddr   string        `yaml:"metrics_addr"`
	AdminAddr     string        `yaml:"admin_addr"`
	TopicsDir     string        `yaml:"topics_dir"`
	MetaDir       string        `yaml:"meta_dir"`
	UsersFile     string        `yaml:"users_file"`
	TLSCert       string        `yaml:"tls_cert"`
	TLSKey        string        `yaml:"tls_key"`
	TLSCA         string        `yaml:"tls_ca"`
	DedupCapacity int           `yaml:"dedup_capacity"`
	DedupTTL      time.Duration `yaml:"dedup_ttl"`
	AdminUser     string        `yaml:"admin_user"`
	AdminPassword string        `yaml:"admin_password"`
	AuthRequired  bool          `yaml:"auth_required"`
}

func Default() Config {
	return Config{
		MQTTAddr:      ":1883",
		MQTTTLSAddr:   ":8883",
		MetricsAddr:   ":9090",
		AdminAddr:     ":8080",
		TopicsDir:     "storage/topics",
		MetaDir:       "storage/meta",
		UsersFile:     "config/users.yaml",
		DedupCapacity: 100_000,
		DedupTTL:      10 * time.Minute,
		AdminUser:     "admin",
		AdminPassword: "admin",
		AuthRequired:  true,
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
