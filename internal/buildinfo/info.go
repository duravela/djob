// Package buildinfo identifies the executable used for a deployment or drill.
package buildinfo

import (
	"encoding/json"
	"io"
	"runtime"
	"runtime/debug"
)

// Release builds set these values through linker flags using the source commit.
var (
	Version   = "devel"
	Revision  string
	BuildTime string
)

type Info struct {
	Version   string `json:"version"`
	Revision  string `json:"revision"`
	BuildTime string `json:"build_time"`
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	Dirty     bool   `json:"dirty"`
}

func Current() Info {
	i := Info{Version: Version, Revision: Revision, BuildTime: BuildTime, GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH}
	if build, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range build.Settings {
			switch setting.Key {
			case "vcs.revision":
				if i.Revision == "" {
					i.Revision = setting.Value
				}
			case "vcs.time":
				if i.BuildTime == "" {
					i.BuildTime = setting.Value
				}
			case "vcs.modified":
				i.Dirty = setting.Value == "true"
			}
		}
	}
	return i
}

func Write(w io.Writer) error { return json.NewEncoder(w).Encode(Current()) }
