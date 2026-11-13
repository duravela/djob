package e2e_test

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// The qualification driver acts as an explicit operator. Production defaults do
// not acquire a retention policy. Only already verified, known-successful runs
// age out; their deletion audits and request identities remain durable.
func (h *harness) retireSoakHistory(report *soakReport) {
	cutoff := time.Now().UTC().Add(-report.Config.HistoryWindow)
	for _, tenant := range []string{"alpha", "beta"} {
		var token string
		for {
			page := h.request(0, "POST", "/api/jobs/v1/list", tenant+"-reader", object{"kind": "RUN", "page_size": 32, "page_token": token}, 200)
			records, _ := page["records"].([]any)
			for _, value := range records {
				record, _ := value.(map[string]any)
				run, _ := record["run"].(map[string]any)
				if run["status"] != "succeeded" {
					continue
				}
				at, err := time.Parse(time.RFC3339Nano, fmt.Sprint(run["last_transition"]))
				if err != nil {
					h.t.Fatal(err)
				}
				if at.After(cutoff) {
					continue
				}
				id, step := fmt.Sprint(run["id"]), fmt.Sprint(run["step"])
				h.soakMaintenance(report, tenant, "purge", object{"run_id": id, "expected_step": step, "finished_before": cutoff.Format(time.RFC3339Nano), "batch_size": 128, "reason": "qualification payload history window elapsed"}, at)
				var deletion object
				poll(h.t, "retention cleanup completes", func() (bool, error) {
					result := h.request(0, "POST", "/api/jobs/v1/get", tenant+"-reader", object{"reference": object{"kind": "DELETION", "id": id}}, 200)
					deletion, _ = result["deletion"].(map[string]any)
					return deletion["cleanup_complete"] == true, nil
				})
				floor, err := time.Parse(time.RFC3339Nano, fmt.Sprint(deletion["last_cleanup_at"]))
				if err != nil {
					h.t.Fatal(err)
				}
				h.soakMaintenance(report, tenant, "compact_receipts", object{"run_id": id, "expected_step": step, "batch_size": 128, "reason": "qualification historical receipt payload cleanup"}, floor)
				poll(h.t, "retention receipt compaction completes", func() (bool, error) {
					result := h.request(0, "POST", "/api/jobs/v1/get", tenant+"-reader", object{"reference": object{"kind": "RECEIPT_COMPACTION", "id": id}}, 200)
					progress, _ := result["receipt_compaction"].(map[string]any)
					return progress["complete"] == true, nil
				})
				report.Purged++
				report.Compacted++
				if receipt, ok := report.replays[tenant]; ok && receipt.runID == id {
					reply := report.requestMutation(0, tenant+"-writer", receipt.request)
					if !reflect.DeepEqual(reply, receipt.reply) {
						h.t.Fatalf("compaction changed the original launch acknowledgement: before=%v after=%v", receipt.reply, reply)
					}
					h.request(0, "POST", "/api/jobs/v1/get", tenant+"-reader", object{"reference": object{"kind": "RUN", "id": id}}, 404)
					delete(report.replays, tenant)
					report.Replayed++
				}
			}
			token, _ = page["next_page_token"].(string)
			if token == "" {
				break
			}
		}
		// Materialization and configuration receipts have a separate lifetime
		// from run history. Capture a finite horizon while calendars stay live.
		if !report.Config.CompactConfigurationReceipts {
			continue
		}
		id := fmt.Sprintf("configuration-history-%d", report.ConfigurationSweeps)
		h.soakMaintenance(report, tenant, "compact_configuration_receipts", object{"compaction_id": id, "batch_size": 128, "reason": "qualification historical configuration receipt cleanup"}, time.Time{})
		var progress object
		poll(h.t, "configuration receipt compaction completes", func() (bool, error) {
			result := h.request(0, "POST", "/api/jobs/v1/get", tenant+"-reader", object{"reference": object{"kind": "CONFIGURATION_RECEIPT_COMPACTION", "id": id}}, 200)
			progress, _ = result["configuration_receipt_compaction"].(map[string]any)
			return progress["complete"] == true, nil
		})
		count, err := strconv.ParseUint(fmt.Sprint(progress["receipts_compacted"]), 10, 64)
		if err != nil {
			h.t.Fatal(err)
		}
		report.ConfigurationSweeps++
		report.ConfigurationReceipts += count
		if receipt, ok := report.configurationReplays[tenant]; ok {
			reply := report.requestMutation(0, "platform", receipt.request)
			if !reflect.DeepEqual(reply, receipt.reply) {
				h.t.Fatalf("configuration compaction changed the original acknowledgement: before=%v after=%v", receipt.reply, reply)
			}
			delete(report.configurationReplays, tenant)
			report.ConfigurationReplays++
		}
	}
}

func (h *harness) soakMaintenance(report *soakReport, tenant, operation string, value object, floor time.Time) {
	meta := h.meta()
	at := time.Now().UTC()
	if floor.After(at) {
		at = floor
	}
	meta["at"] = at.Format(time.RFC3339Nano)
	report.requestMutation(0, "platform", object{"tenant_id": tenant, "command": meta, operation: value})
}

type soakReplay struct {
	runID          string
	request, reply object
}

func (h *harness) auditSoakHistory(report *soakReport, unknown map[string]int) {
	successes, deleted, compacted := 0, 0, 0
	configurationSweeps := 0
	var configurationReceipts uint64
	for _, tenant := range []string{"alpha", "beta"} {
		scheduled, uncertain, succeeded := 0, 0, 0
		for _, kind := range []string{"RUN", "DELETION", "RECEIPT_COMPACTION", "CONFIGURATION_RECEIPT_COMPACTION"} {
			var token string
			for {
				page := h.request(1, "POST", "/api/jobs/v1/list", tenant+"-reader", object{"kind": kind, "page_size": 32, "page_token": token}, 200)
				records, _ := page["records"].([]any)
				for _, value := range records {
					record, _ := value.(map[string]any)
					if kind == "CONFIGURATION_RECEIPT_COMPACTION" {
						progress, _ := record["configuration_receipt_compaction"].(map[string]any)
						if progress["complete"] != true {
							h.t.Fatalf("unfinished configuration compaction: %v", progress)
						}
						count, err := strconv.ParseUint(fmt.Sprint(progress["receipts_compacted"]), 10, 64)
						if err != nil {
							h.t.Fatal(err)
						}
						configurationReceipts += count
						configurationSweeps++
						continue
					}
					if kind == "RECEIPT_COMPACTION" {
						progress, _ := record["receipt_compaction"].(map[string]any)
						if progress["complete"] != true {
							h.t.Fatalf("unfinished receipt compaction: %v", progress)
						}
						compacted++
						continue
					}
					field := "run"
					if kind == "DELETION" {
						field = "deletion"
					}
					run, _ := record[field].(map[string]any)
					if kind == "DELETION" {
						if run["cleanup_complete"] != true || run["status"] != "succeeded" {
							h.t.Fatalf("retention removed unverified history: %v", run)
						}
						deleted++
					}
					switch run["status"] {
					case "succeeded":
						if run["trigger_id"] == "calendar" {
							scheduled++
						} else {
							succeeded++
						}
					case "unknown":
						if kind != "RUN" || !strings.HasPrefix(fmt.Sprint(run["id"]), "offline-") || run["trigger_id"] != "" {
							h.t.Fatalf("unexpected uncertainty: %v", run)
						}
						uncertain++
					default:
						h.t.Fatalf("unfinished or failed soak run: %v", run)
					}
				}
				token, _ = page["next_page_token"].(string)
				if token == "" {
					break
				}
			}
		}
		if scheduled == 0 || uncertain != unknown[tenant] {
			h.t.Fatalf("%s scheduled=%d uncertain=%d expected=%d", tenant, scheduled, uncertain, unknown[tenant])
		}
		report.Scheduled += scheduled
		successes += succeeded
	}
	if report.Succeeded != successes || report.Purged != deleted || report.Compacted != compacted {
		h.t.Fatalf("durable counts differ: successes=%d/%d deleted=%d/%d compacted=%d/%d", report.Succeeded, successes, report.Purged, deleted, report.Compacted, compacted)
	}
	if report.Config.HistoryWindow > 0 && (deleted == 0 || report.Replayed != 2) {
		h.t.Fatalf("history window did not exercise cleanup and exact request replay: deleted=%d replayed=%d", deleted, report.Replayed)
	}
	if configurationSweeps != report.ConfigurationSweeps || configurationReceipts != report.ConfigurationReceipts {
		h.t.Fatalf("configuration compaction audit differs: sweeps=%d/%d receipts=%d/%d", configurationSweeps, report.ConfigurationSweeps, configurationReceipts, report.ConfigurationReceipts)
	}
	if report.Config.CompactConfigurationReceipts && (configurationReceipts == 0 || report.ConfigurationReplays != 2) {
		h.t.Fatalf("configuration compaction and exact replay were not exercised: receipts=%d replayed=%d", configurationReceipts, report.ConfigurationReplays)
	}
}
