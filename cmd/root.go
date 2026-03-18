package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "patolake",
	Short: "Patolake - DuckDB data platform",
	Long:  "Patolake is a self-hosted DuckDB data platform. Query anything: files, S3, Parquet, CSV, JSON, remote databases — all in one.",
}

func init() {
	loadEnvFile(".env")
}

func Execute() {
	if len(os.Args) == 1 {
		rootCmd.SetArgs([]string{"server"})
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// loadEnvFile reads a .env file and sets environment variables.
// Existing env vars are NOT overwritten (real env takes precedence).
// Silently does nothing if the file doesn't exist.
func loadEnvFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		// Strip surrounding quotes
		if len(val) >= 2 && ((val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'')) {
			val = val[1 : len(val)-1]
		}
		// Don't overwrite existing env vars
		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
}
