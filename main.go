package main

import (
	"github.com/caioricciuti/pato-lake/cmd"
	"github.com/caioricciuti/pato-lake/internal/version"
)

var (
	Version   = "dev"
	Commit    = "none"
	BuildDate = "unknown"
)

func main() {
	version.Set(Version, Commit, BuildDate)
	cmd.FrontendFS = frontendFS()
	cmd.Execute()
}
