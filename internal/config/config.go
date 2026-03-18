package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/caioricciuti/pato-lake/internal/license"
	"gopkg.in/yaml.v3"
)

type Config struct {
	// Server
	Port    int
	DevMode bool
	AppURL  string

	// Database (SQLite for app metadata)
	DatabasePath string

	// DuckDB
	DuckDBPath        string // persistent DuckDB file (empty = in-memory)
	DuckDBMemoryLimit string // e.g. "4GB"
	DuckDBThreads     int    // 0 = auto
	DuckDBExtensions  []string

	// Security
	AppSecretKey   string
	SessionMaxAge  int // seconds, default 7 days
	AllowedOrigins []string

	// Admin bootstrap
	AdminUsername string
	AdminPassword string

	// License
	LicenseJSON string
}

type serverConfigFile struct {
	Port              int      `yaml:"port"`
	AppURL            string   `yaml:"app_url"`
	DatabasePath      string   `yaml:"database_path"`
	DuckDBPath        string   `yaml:"duckdb_path"`
	DuckDBMemoryLimit string   `yaml:"duckdb_memory_limit"`
	DuckDBThreads     int      `yaml:"duckdb_threads"`
	DuckDBExtensions  []string `yaml:"duckdb_extensions"`
	AppSecretKey      string   `yaml:"app_secret_key"`
	AllowedOrigins    []string `yaml:"allowed_origins"`
	AdminUsername     string   `yaml:"admin_username"`
	AdminPassword     string   `yaml:"admin_password"`
}

// DefaultServerConfigPath returns the platform-specific default config path.
func DefaultServerConfigPath() string {
	switch runtime.GOOS {
	case "darwin":
		home, _ := os.UserHomeDir()
		return filepath.Join(home, ".config", "patolake", "server.yaml")
	default:
		return "/etc/patolake/server.yaml"
	}
}

// Load creates a Config by merging: config file -> env vars -> defaults.
func Load(configPath string) *Config {
	cfg := &Config{
		Port:           3488,
		DatabasePath:   "./data/patolake.db",
		DuckDBPath:     "./data/duck.db",
		AppSecretKey:   DefaultAppSecretKey,
		SessionMaxAge:  7 * 24 * 60 * 60,
		AdminUsername:  "admin",
		DuckDBExtensions: []string{
			"httpfs", "parquet", "json", "icu",
		},
	}

	// 1. Load from config file
	if configPath != "" {
		if err := loadServerConfigFile(configPath, cfg); err != nil {
			if !os.IsNotExist(err) {
				slog.Warn("Failed to load config file", "path", configPath, "error", err)
			}
		} else {
			slog.Info("Loaded config file", "path", configPath)
		}
	} else {
		defaultPath := DefaultServerConfigPath()
		if err := loadServerConfigFile(defaultPath, cfg); err == nil {
			slog.Info("Loaded config file", "path", defaultPath)
		}
	}

	// 2. Override with environment variables
	if v := os.Getenv("PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.Port = p
		}
	}
	if v := os.Getenv("APP_URL"); v != "" {
		cfg.AppURL = trimQuotes(v)
	}
	if v := os.Getenv("DATABASE_PATH"); v != "" {
		cfg.DatabasePath = v
	}
	if v := os.Getenv("DUCKDB_PATH"); v != "" {
		cfg.DuckDBPath = v
	}
	if v := os.Getenv("DUCKDB_MEMORY_LIMIT"); v != "" {
		cfg.DuckDBMemoryLimit = v
	}
	if v := os.Getenv("DUCKDB_THREADS"); v != "" {
		if t, err := strconv.Atoi(v); err == nil {
			cfg.DuckDBThreads = t
		}
	}
	if v := os.Getenv("APP_SECRET_KEY"); v != "" {
		cfg.AppSecretKey = trimQuotes(v)
	}
	if v := os.Getenv("ALLOWED_ORIGINS"); v != "" {
		cfg.AllowedOrigins = nil
		for _, o := range strings.Split(v, ",") {
			if trimmed := strings.TrimSpace(o); trimmed != "" {
				cfg.AllowedOrigins = append(cfg.AllowedOrigins, trimmed)
			}
		}
	}
	if v := os.Getenv("ADMIN_USERNAME"); v != "" {
		cfg.AdminUsername = v
	}
	if v := os.Getenv("ADMIN_PASSWORD"); v != "" {
		cfg.AdminPassword = v
	}

	// Derived defaults
	if cfg.AppURL == "" {
		cfg.AppURL = "http://localhost:" + strconv.Itoa(cfg.Port)
	}

	cfg.DevMode = os.Getenv("NODE_ENV") != "production"

	return cfg
}

func loadServerConfigFile(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var fc serverConfigFile
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return err
	}

	if fc.Port != 0 {
		cfg.Port = fc.Port
	}
	if fc.AppURL != "" {
		cfg.AppURL = fc.AppURL
	}
	if fc.DatabasePath != "" {
		cfg.DatabasePath = fc.DatabasePath
	}
	if fc.DuckDBPath != "" {
		cfg.DuckDBPath = fc.DuckDBPath
	}
	if fc.DuckDBMemoryLimit != "" {
		cfg.DuckDBMemoryLimit = fc.DuckDBMemoryLimit
	}
	if fc.DuckDBThreads != 0 {
		cfg.DuckDBThreads = fc.DuckDBThreads
	}
	if len(fc.DuckDBExtensions) > 0 {
		cfg.DuckDBExtensions = fc.DuckDBExtensions
	}
	if fc.AppSecretKey != "" {
		cfg.AppSecretKey = fc.AppSecretKey
	}
	if len(fc.AllowedOrigins) > 0 {
		cfg.AllowedOrigins = fc.AllowedOrigins
	}
	if fc.AdminUsername != "" {
		cfg.AdminUsername = fc.AdminUsername
	}
	if fc.AdminPassword != "" {
		cfg.AdminPassword = fc.AdminPassword
	}

	return nil
}

// GenerateServerTemplate returns a YAML config template.
func GenerateServerTemplate() string {
	return `# Patolake Server Configuration
#
# Place this file at:
#   macOS: ~/.config/patolake/server.yaml
#   Linux: /etc/patolake/server.yaml
#
# All settings can also be set via environment variables.
# Priority: env vars > config file > defaults

# HTTP port (default: 3488)
port: 3488

# Public URL of the server
# app_url: https://patolake.yourcompany.com

# SQLite database path for app metadata (default: ./data/patolake.db)
# database_path: /var/lib/patolake/patolake.db

# DuckDB persistent storage path (empty = in-memory only)
# duckdb_path: /var/lib/patolake/duck.db

# DuckDB resource limits
# duckdb_memory_limit: 4GB
# duckdb_threads: 4

# DuckDB extensions to auto-load
# duckdb_extensions:
#   - httpfs
#   - parquet
#   - json
#   - icu
#   - postgres_scanner
#   - mysql_scanner
#   - spatial

# Admin bootstrap credentials
# admin_username: admin
# admin_password: changeme

# Secret key for session encryption (CHANGE THIS in production)
# app_secret_key: your-random-secret-here

# Allowed CORS origins
# allowed_origins:
#   - https://patolake.yourcompany.com
`
}

func (c *Config) IsProduction() bool {
	return !c.DevMode
}

func (c *Config) IsPro() bool {
	info := license.ValidateLicense(c.LicenseJSON)
	return info.Valid && strings.EqualFold(strings.TrimSpace(info.Edition), "pro")
}

func trimQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
