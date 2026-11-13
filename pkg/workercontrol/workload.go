package workercontrol

import "reflect"

// Only these lists permit platform-added entries. Mounts are identified by
// path: one volume may legitimately be mounted more than once. Positional lists
// (arguments, capabilities) and the configured pull-Secret list are wholly owned.
func workloadListKey(field string) string {
	switch field {
	case "containers", "env", "volumes":
		return "name"
	case "topologySpreadConstraints":
		return "topologyKey"
	case "volumeMounts":
		return "mountPath"
	}
	return ""
}

// Compare owned fields, tolerating platform additions but rejecting duplicate
// owned entries. In particular, a later duplicate environment value must not
// shadow the deployment identity that admission validated.
func contains(actual, desired any) bool { return containsField(actual, desired, "") }

func containsField(actual, desired any, field string) bool {
	switch wanted := desired.(type) {
	case map[string]any:
		got, ok := actual.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range wanted {
			if !containsField(got[key], value, key) {
				return false
			}
		}
		return true
	case []any:
		got, ok := actual.([]any)
		if !ok {
			return false
		}
		key := workloadListKey(field)
		if key == "" || len(wanted) == 0 {
			return reflect.DeepEqual(actual, desired)
		}
		for _, value := range wanted {
			id, matches, matching := text(object(value)[key]), 0, false
			if id == "" {
				return false
			}
			for _, candidate := range got {
				if text(object(candidate)[key]) == id {
					matches++
					matching = contains(candidate, value)
				}
			}
			if matches != 1 || !matching {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(actual, desired)
	}
}

func workloadPod(spec any) map[string]any { return object(object(object(spec)["template"])["spec"]) }

func containsDeploymentSpec(actual, desired any) bool {
	// The optional pull-Secret setting also owns its absence. Clearing it must
	// remove the previously configured reference, rather than retain stale access.
	return contains(actual, desired) && reflect.DeepEqual(workloadPod(actual)["imagePullSecrets"], workloadPod(desired)["imagePullSecrets"])
}

func mergeDeploymentSpec(actual, desired any) any {
	if _, present := workloadPod(desired)["imagePullSecrets"]; !present {
		delete(workloadPod(actual), "imagePullSecrets")
	}
	return mergeWorkloadField(actual, desired, "")
}

// The GET response is private to this operation. Overlay owned fields there so
// resource-version CAS protects both djob's changes and preserved platform data.
func mergeWorkloadField(actual, desired any, field string) any {
	switch wanted := desired.(type) {
	case map[string]any:
		if field == "seccompProfile" {
			return desired
		}
		got := object(actual)
		if got == nil {
			got = make(map[string]any)
		}
		for key, value := range wanted {
			got[key] = mergeWorkloadField(got[key], value, key)
		}
		return got
	case []any:
		key := workloadListKey(field)
		if key == "" || len(wanted) == 0 {
			return desired
		}
		entries := make(map[string]any, len(wanted))
		for _, value := range wanted {
			entries[text(object(value)[key])] = value
		}
		used := make(map[string]bool, len(wanted))
		got, _ := actual.([]any)
		merged := make([]any, 0, len(got)+len(wanted))
		for _, existing := range got {
			id := text(object(existing)[key])
			value, owned := entries[id]
			if !owned {
				merged = append(merged, existing)
				continue
			}
			if used[id] {
				continue
			}
			used[id] = true
			// Environment values and mounts are complete owned entries. This
			// removes incompatible valueFrom/subPath fields on drift repair.
			// Container maps and same-kind volume sources retain platform fields.
			if field == "containers" || field == "volumes" && sameVolumeSource(existing, value) {
				value = mergeWorkloadField(existing, value, "")
			}
			merged = append(merged, value)
		}
		for _, value := range wanted {
			if !used[text(object(value)[key])] {
				merged = append(merged, value)
			}
		}
		return merged
	default:
		return desired
	}
}

func sameVolumeSource(actual, desired any) bool {
	for _, kind := range []string{"secret", "emptyDir"} {
		if object(object(desired)[kind]) != nil && object(object(actual)[kind]) != nil {
			return true
		}
	}
	return false
}
