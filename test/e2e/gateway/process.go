package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// The scratch test image includes this helper. It observes PID 1 in the target
// container, not the helper process, and never runs in a release executable.
func processStats() error {
	fds, err := os.ReadDir("/proc/1/fd")
	if err != nil {
		return err
	}
	data, err := os.ReadFile("/proc/1/status")
	if err != nil {
		return err
	}
	values := map[string]uint64{"open_files": uint64(len(fds))}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || (fields[0] != "VmRSS:" && fields[0] != "Threads:") {
			continue
		}
		n, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return err
		}
		if fields[0] == "VmRSS:" {
			values["resident_bytes"] = n * 1024
		} else {
			values["threads"] = n
		}
	}
	if values["resident_bytes"] == 0 || values["threads"] == 0 {
		return fmt.Errorf("missing PID 1 resource counters")
	}
	return json.NewEncoder(os.Stdout).Encode(values)
}

func processHighWater() error {
	data, err := os.ReadFile("/proc/1/status")
	if err != nil {
		return err
	}
	values := map[string]uint64{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "VmHWM:" && fields[2] == "kB" {
			n, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
			values["peak_resident_bytes"] = n * 1024
		}
	}
	for name, file := range map[string]string{"cgroup_peak_bytes": "memory.peak", "cgroup_limit_bytes": "memory.max"} {
		data, err := os.ReadFile("/sys/fs/cgroup/" + file)
		if err != nil {
			return err
		}
		n, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		if err != nil {
			return err
		}
		values[name] = n
	}
	data, err = os.ReadFile("/sys/fs/cgroup/memory.events")
	if err != nil {
		return err
	}
	foundOOMCounter := false
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "oom_kill" {
			foundOOMCounter = true
			values["oom_kills"], err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		}
	}
	if !foundOOMCounter || values["peak_resident_bytes"] == 0 || values["cgroup_limit_bytes"] == 0 || values["cgroup_peak_bytes"] == 0 {
		return fmt.Errorf("missing memory high-water counters")
	}
	return json.NewEncoder(os.Stdout).Encode(values)
}
