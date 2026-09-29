#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Ozone auto-balancer: assessment loop plus optional RM/DN sanity check.

On a fixed interval, runs:
    ozone admin containerbalancer assessment --json
parses the JSON, writes a human-readable report, and decides whether balancing
is actionable. When the assessment gives a go-ahead (actionable verdict) and
sanity_check is configured, queries Prometheus using PromQL and thresholds from
the JSON config and appends a SANITY CHECK section to that report.

The JSON config file is the SINGLE SOURCE OF TRUTH for all user-configurable
values. This script adds NO configuration defaults of its own: every required
field must be present in the config, otherwise a configuration error is raised
(exit 2). Metric names, PromQL, and thresholds for sanity checks live only in
JSON — not in this script.

Assessment config fields (all required; use null where noted to omit a CLI flag):
  threshold, include_nodes, exclude_nodes, limit, minimum_drift,
  min_eligible_datanodes, min_source_nodes, min_target_nodes, assessment_interval

Optional sanity_check: null disables Prometheus checks; otherwise an object with
prometheus_url, query_timeout_seconds, counter_rate_window, and a checks array.
Each check: group (rm|dn), label, query, kind, threshold, description, relevance.
  kind gauge_max              instant query; max(series) vs threshold
  kind counter_increase_sum   same; empty successful result counts as 0
Use {{counter_rate_window}} in query strings for PromQL range windows.

Loop: assessment -> optional sanity on actionable reports -> sleep -> repeat.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

OZONE_ASSESSMENT_CMD = ["ozone", "admin", "containerbalancer", "assessment", "--json"]
DRIFT_KEY = "driftPercentage"
VERDICT_ACTIONABLE = "actionable"
SANITY_SECTION_MARKER = "SANITY CHECK"

REQUIRED_KEYS = (
    "threshold",
    "include_nodes",
    "exclude_nodes",
    "limit",
    "minimum_drift",
    "min_eligible_datanodes",
    "min_source_nodes",
    "min_target_nodes",
    "assessment_interval",
    "sanity_check",
)

CHECK_KINDS = ("gauge_max", "counter_increase_sum")
CHECK_GROUPS = ("rm", "dn")
COUNTER_WINDOW_PLACEHOLDER = "{{counter_rate_window}}"

_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(value):
    """'5h' / '30m' / '90s' / '1d' / '1h30m' / bare seconds -> total seconds (int)."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)
    if not isinstance(value, str):
        raise ValueError(f"interval must be a string like '5h' (got {value!r})")
    text = value.strip().lower()
    if re.fullmatch(r"\d+", text):
        return int(text)
    tokens = re.findall(r"(\d+)\s*([smhd])", text)
    if not tokens or re.sub(r"(\d+)\s*([smhd])", "", text).strip():
        raise ValueError(f"invalid interval {value!r}; use forms like '5h', '30m', '1h30m'")
    return sum(int(n) * _UNIT_SECONDS[u] for n, u in tokens)


def human_bytes(num):
    try:
        num = float(num)
    except (TypeError, ValueError):
        return str(num)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(num) < 1024.0 or unit == "PB":
            return f"{int(num)} B" if unit == "B" else f"{num:.2f} {unit}"
        num /= 1024.0
    return f"{num:.2f} PB"


def now_utc():
    return datetime.now(timezone.utc)


def die(code, message):
    sys.stderr.write(f"ERROR: {message}\n")
    sys.exit(code)


def fmt_num(value):
    if value is None:
        return "(not available)"
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.2f}"


def render_kv(label, value, width=42):
    return f"  {label:<{width}} {value}"


def render_sub(label, value, width=40):
    return f"    {label:<{width}} {value}"


def _require(container, key, ctx):
    if key not in container:
        die(2, f"Missing required config field: {ctx}.{key}")
    return container[key]


def _non_negative_number(value, ctx):
    if isinstance(value, bool):
        die(2, f"{ctx} must be a non-negative number.")
    try:
        num = float(value)
    except (TypeError, ValueError):
        die(2, f"{ctx} must be a non-negative number (got {value!r}).")
    if num < 0:
        die(2, f"{ctx} must be non-negative.")
    return num


def _pos_number(value, ctx):
    if isinstance(value, bool):
        die(2, f"{ctx} must be a positive number.")
    try:
        num = float(value)
    except (TypeError, ValueError):
        die(2, f"{ctx} must be a positive number (got {value!r}).")
    if num <= 0:
        die(2, f"{ctx} must be positive.")
    return num


def load_sanity_check(raw):
    if raw is None:
        return None
    if not isinstance(raw, dict):
        die(2, "sanity_check must be null or a JSON object.")

    prometheus_url = _require(raw, "prometheus_url", "sanity_check")
    if not isinstance(prometheus_url, str) or not prometheus_url.strip():
        die(2, "sanity_check.prometheus_url must be a non-empty string.")
    raw["prometheus_url"] = prometheus_url.rstrip("/")

    raw["query_timeout_seconds"] = _pos_number(
        _require(raw, "query_timeout_seconds", "sanity_check"),
        "sanity_check.query_timeout_seconds")

    window = _require(raw, "counter_rate_window", "sanity_check")
    if not isinstance(window, str) or not re.fullmatch(r"\d+[smhdwy]", window.strip()):
        die(2, "sanity_check.counter_rate_window must be a PromQL duration like '5m'.")
    raw["counter_rate_window"] = window.strip()

    checks = _require(raw, "checks", "sanity_check")
    if not isinstance(checks, list) or not checks:
        die(2, "sanity_check.checks must be a non-empty array.")

    parsed_checks = []
    for i, item in enumerate(checks):
        ctx = f"sanity_check.checks[{i}]"
        if not isinstance(item, dict):
            die(2, f"{ctx} must be a JSON object.")
        group = _require(item, "group", ctx)
        if group not in CHECK_GROUPS:
            die(2, f"{ctx}.group must be one of: {', '.join(CHECK_GROUPS)}.")
        label = _require(item, "label", ctx)
        if not isinstance(label, str) or not label.strip():
            die(2, f"{ctx}.label must be a non-empty string.")
        query = _require(item, "query", ctx)
        if not isinstance(query, str) or not query.strip():
            die(2, f"{ctx}.query must be a non-empty string.")
        kind = _require(item, "kind", ctx)
        if kind not in CHECK_KINDS:
            die(2, f"{ctx}.kind must be one of: {', '.join(CHECK_KINDS)}.")
        threshold = _non_negative_number(_require(item, "threshold", ctx), f"{ctx}.threshold")
        description = _require(item, "description", ctx)
        relevance = _require(item, "relevance", ctx)
        if not isinstance(description, str) or not description.strip():
            die(2, f"{ctx}.description must be a non-empty string.")
        if not isinstance(relevance, str) or not relevance.strip():
            die(2, f"{ctx}.relevance must be a non-empty string.")
        parsed_checks.append({
            "group": group,
            "label": label.strip(),
            "query": query.strip(),
            "kind": kind,
            "threshold": threshold,
            "description": description.strip(),
            "relevance": relevance.strip(),
        })
    raw["checks"] = parsed_checks
    return raw


def load_config(path):
    if not os.path.isfile(path):
        die(2, f"Config file not found: {path}")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    except json.JSONDecodeError as exc:
        die(2, f"Config file is not valid JSON: {exc}")
    if not isinstance(cfg, dict):
        die(2, "Config file must contain a JSON object.")

    missing = [k for k in REQUIRED_KEYS if k not in cfg]
    if missing:
        die(2, "Missing required config field(s): " + ", ".join(missing)
            + ". The config file must define: " + ", ".join(REQUIRED_KEYS) + ".")

    if cfg["threshold"] is not None:
        if isinstance(cfg["threshold"], bool):
            die(2, "threshold must be a number or null.")
        try:
            cfg["threshold"] = float(cfg["threshold"])
        except (TypeError, ValueError):
            die(2, f"threshold must be a number or null (got {cfg['threshold']!r}).")
        if not (0.0 <= cfg["threshold"] < 100.0):
            die(2, "threshold must be in the range [0.0, 100.0).")

    if cfg["limit"] is not None:
        if isinstance(cfg["limit"], bool) or not float(cfg["limit"]).is_integer():
            die(2, f"limit must be an integer or null (got {cfg['limit']!r}).")
        cfg["limit"] = int(cfg["limit"])
        if cfg["limit"] < 1:
            die(2, "limit must be at least 1.")

    cfg["include_nodes"] = normalise_node_list(cfg["include_nodes"], "include_nodes")
    cfg["exclude_nodes"] = normalise_node_list(cfg["exclude_nodes"], "exclude_nodes")
    cfg["minimum_drift"] = parse_percent(cfg["minimum_drift"], "minimum_drift")
    cfg["min_eligible_datanodes"] = parse_non_negative_int(cfg["min_eligible_datanodes"],
                                                           "min_eligible_datanodes")
    cfg["min_source_nodes"] = parse_non_negative_int(cfg["min_source_nodes"], "min_source_nodes")
    cfg["min_target_nodes"] = parse_non_negative_int(cfg["min_target_nodes"], "min_target_nodes")

    try:
        cfg["assessment_interval_s"] = parse_duration(cfg["assessment_interval"])
    except ValueError as exc:
        die(2, str(exc))
    if cfg["assessment_interval_s"] <= 0:
        die(2, "assessment_interval must be positive.")

    cfg["sanity_check"] = load_sanity_check(cfg["sanity_check"])
    return cfg


def normalise_node_list(value, field):
    if value is None:
        return None
    if isinstance(value, list):
        items = [str(v).strip() for v in value if str(v).strip()]
    elif isinstance(value, str):
        items = [s.strip() for s in value.split(",") if s.strip()]
    else:
        die(2, f"{field} must be null, a CSV string, or a list of strings.")
    return ",".join(items) if items else None


def parse_non_negative_int(value, field):
    if isinstance(value, bool):
        die(2, f"{field} must be a non-negative integer.")
    try:
        num = float(value)
    except (TypeError, ValueError):
        die(2, f"{field} must be a non-negative integer (got {value!r}).")
    if not num.is_integer():
        die(2, f"{field} must be a non-negative integer (got {value!r}).")
    num = int(num)
    if num < 0:
        die(2, f"{field} must be non-negative.")
    return num


def parse_percent(value, field):
    if isinstance(value, bool):
        die(2, f"{field} must be a percentage number.")
    if isinstance(value, str) and value.strip().endswith("%"):
        value = value.strip()[:-1]
    try:
        num = float(value)
    except (TypeError, ValueError):
        die(2, f"{field} must be a percentage number (got {value!r}).")
    if not (0.0 <= num <= 100.0):
        die(2, f"{field} must be in the range [0, 100].")
    return num


def build_command(cfg):
    cmd = list(OZONE_ASSESSMENT_CMD)
    if cfg["threshold"] is not None:
        cmd += ["--threshold", str(cfg["threshold"])]
    if cfg["limit"] is not None:
        cmd += ["--limit", str(cfg["limit"])]
    if cfg["include_nodes"]:
        cmd += ["--include-datanodes", cfg["include_nodes"]]
    if cfg["exclude_nodes"]:
        cmd += ["--exclude-datanodes", cfg["exclude_nodes"]]
    return cmd


def run_command(cmd):
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError:
        return None, "", "'ozone' not found on PATH."
    except OSError as exc:
        return None, "", f"Failed to execute assessment command: {exc}"
    return proc.returncode, proc.stdout, proc.stderr


LABELS = [
    ("totalEligibleDatanodes",           "Total eligible datanodes",             None),
    ("thresholdPercentage",              "Threshold",                            "pct"),
    ("clusterAvgUtilizationPercentage",  "Cluster average utilization",          "pct"),
    ("driftPercentage",                  "Drift (max util - min util)",          "pct"),
    ("maxUtilizationPercentage",         "Max datanode utilization",             "pct"),
    ("minUtilizationPercentage",         "Min datanode utilization",             "pct"),
    ("upperLimitPercentage",             "Upper utilization limit",              "pct"),
    ("lowerLimitPercentage",             "Lower utilization limit",              "pct"),
    ("bytesToMove",                      "Total bytes to move",                  "bytes"),
    ("movementRatioPercentage",          "Movement ratio (of cluster capacity)", "pct"),
]


def fmt_value(raw, kind):
    if raw is None:
        return "(not reported)"
    if kind == "pct":
        return f"{raw}%"
    if kind == "bytes":
        return f"{human_bytes(raw)} ({raw} bytes)"
    return str(raw)


def render_node_group(title, group):
    lines = [f"{title}:"]
    if not isinstance(group, dict):
        lines.append(render_kv("(no data)", ""))
        return lines
    lines.append(render_kv("Count", group.get("count", "(not reported)")))
    nodes = group.get("nodes") or []
    if not nodes:
        lines.append(render_kv("Nodes", "(none)"))
    else:
        lines.append("  Nodes:")
        for node in nodes:
            host = node.get("hostname", "(unknown)")
            util = node.get("utilizationPercentage", "?")
            lines.append(f"      - {host:<40} {util}%")
    return lines


def render_config(cfg):
    def show(v):
        return "null (flag omitted)" if v is None else v
    lines = [
        "CONFIGURATION USED (from JSON)",
        render_kv("Threshold", "null (flag omitted)" if cfg["threshold"] is None
                  else f"{cfg['threshold']}%"),
        render_kv("Limit (nodes listed)", show(cfg["limit"])),
        render_kv("Include datanodes", show(cfg["include_nodes"])),
        render_kv("Exclude datanodes", show(cfg["exclude_nodes"])),
        render_kv("Minimum drift", f"{cfg['minimum_drift']}%"),
        render_kv("Min eligible datanodes", cfg["min_eligible_datanodes"]),
        render_kv("Min source nodes", cfg["min_source_nodes"]),
        render_kv("Min target nodes", cfg["min_target_nodes"]),
        render_kv("Assessment interval", cfg["assessment_interval"]),
        render_kv("Sanity check (Prometheus)", "disabled" if cfg["sanity_check"] is None
                  else cfg["sanity_check"]["prometheus_url"]),
    ]
    return lines


def _json_int_field(data, key):
    raw = data.get(key)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    if not float(raw).is_integer():
        return None
    return int(raw)


def _node_group_count(data, key):
    group = data.get(key)
    if not isinstance(group, dict):
        return None
    return _json_int_field(group, "count")


def build_decision(data, cfg):
    eligible = _json_int_field(data, "totalEligibleDatanodes")
    source_count = _node_group_count(data, "sourceNodes")
    target_count = _node_group_count(data, "targetNodes")
    bytes_to_move = _json_int_field(data, "bytesToMove")
    drift = data.get(DRIFT_KEY)
    drift_ok = isinstance(drift, (int, float)) and not isinstance(drift, bool)

    lines = ["DECISION"]

    if eligible is None:
        lines.append("  Could not read totalEligibleDatanodes from assessment output.")
        return "not-actionable", lines

    if eligible < cfg["min_eligible_datanodes"]:
        lines.append("  Not enough eligible datanodes for balancing "
                     f"({eligible} eligible, minimum {cfg['min_eligible_datanodes']}).")
        return "no-eligible-datanodes", lines

    if source_count is None or target_count is None or bytes_to_move is None:
        lines.append("  Could not read source/target counts or bytesToMove from assessment output.")
        return "not-actionable", lines

    if source_count < cfg["min_source_nodes"]:
        lines.append("  No source nodes (over-utilized datanodes).")
        return "no-source-nodes", lines

    if target_count < cfg["min_target_nodes"]:
        lines.append("  No target nodes (under-utilized datanodes).")
        return "no-target-nodes", lines

    if bytes_to_move <= 0:
        lines.append("  No bytes to move.")
        return "no-bytes-to-move", lines

    if not drift_ok:
        lines.append("  Could not determine cluster drift from the assessment output.")
        return "drift-unknown", lines

    lines.append(render_kv("Drift", f"{drift}%"))
    lines.append(render_kv("Minimum drift threshold", f"{cfg['minimum_drift']}%"))

    if drift < cfg["minimum_drift"]:
        lines += ["", "  No actionable imbalance in cluster."]
        return "no-actionable-imbalance", lines

    lines += ["", "  The cluster has sufficient imbalance "
              f"(drift {drift}% >= minimum {cfg['minimum_drift']}%). "
              "Balancer assessment indicates it is reasonable to proceed."]
    return VERDICT_ACTIONABLE, lines


def write_report(output_dir, started, lines):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"cb_assessment_{started.strftime('%Y%m%d_%H%M%S')}.txt")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return path


def prom_query(sc, promql):
    url = f"{sc['prometheus_url']}/api/v1/query?" + urllib.parse.urlencode({"query": promql})
    try:
        with urllib.request.urlopen(url, timeout=sc["query_timeout_seconds"]) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError, json.JSONDecodeError):
        return False, []
    if payload.get("status") != "success":
        return False, []
    values = []
    for item in payload.get("data", {}).get("result", []):
        raw = item.get("value")
        if isinstance(raw, list) and len(raw) == 2:
            try:
                values.append(float(raw[1]))
            except (TypeError, ValueError):
                continue
    return True, values


def _status(value, threshold):
    if value is None:
        return "UNKNOWN"
    return "PRESSURE" if value > threshold else "PASS"


def _expand_query(sc, query):
    return query.replace(COUNTER_WINDOW_PLACEHOLDER, sc["counter_rate_window"])


def evaluate_check(sc, check):
    promql = _expand_query(sc, check["query"])
    ok, values = prom_query(sc, promql)
    threshold = check["threshold"]
    if not ok:
        value = None
    elif check["kind"] == "counter_increase_sum":
        value = max(values) if values else 0.0
    else:
        value = max(values) if values else None
    return {
        "label": check["label"],
        "value_display": fmt_num(value),
        "description": check["description"],
        "relevance": check["relevance"],
        "threshold": threshold,
        "status": _status(value, threshold),
    }


def overall_pressure(results):
    if any(r["status"] == "PRESSURE" for r in results):
        return "PRESSURE"
    if any(r["status"] == "UNKNOWN" for r in results):
        return "UNKNOWN"
    return "PASS"


def render_metric(result):
    return [
        render_kv(result["label"], result["value_display"]),
        render_sub("Description", result["description"]),
        render_sub("Relevance to balancer", result["relevance"]),
        render_sub("Threshold (max)", fmt_num(result["threshold"])),
        render_sub("Status", result["status"]),
    ]


def build_sanity_lines(sc, rm_results, dn_results, started):
    rm_state = overall_pressure(rm_results)
    dn_state = overall_pressure(dn_results)
    if rm_state == "PRESSURE" or dn_state == "PRESSURE":
        overall = "NOT SAFE TO PROCEED"
    elif rm_state == "UNKNOWN" or dn_state == "UNKNOWN":
        overall = "INCONCLUSIVE (metrics unavailable)"
    else:
        overall = "PASS"

    lines = [
        "",
        "=" * 72,
        SANITY_SECTION_MARKER,
        render_kv("Generated at (UTC)", started.isoformat()),
        render_kv("Prometheus", sc["prometheus_url"]),
        render_kv("Counter window", sc["counter_rate_window"]),
        "",
        "RM REPLICATION/DELETE PRESSURE",
    ]
    for r in rm_results:
        lines += render_metric(r)
    lines += ["", "DATANODE REPLICATION/DELETE PRESSURE"]
    for r in dn_results:
        lines += render_metric(r)
    lines += [
        "",
        "SANITY CHECK RESULT",
        render_kv("RM pressure", rm_state),
        render_kv("DN pressure", dn_state),
        render_kv("Overall", overall),
    ]
    return overall, lines


def run_sanity_check(cfg, report_path):
    """Append sanity section to report_path when assessment was actionable. Never raises."""
    sc = cfg["sanity_check"]
    if sc is None:
        return

    try:
        with open(report_path, "r", encoding="utf-8") as fh:
            text = fh.read()
    except OSError as exc:
        print(f"Could not read report for sanity check {report_path}: {exc}")
        return

    if SANITY_SECTION_MARKER in text:
        print(f"[{now_utc().isoformat()}] sanity check already present -> skip ({report_path})")
        return

    started = now_utc()
    rm_results = []
    dn_results = []
    for check in sc["checks"]:
        result = evaluate_check(sc, check)
        if check["group"] == "rm":
            rm_results.append(result)
        else:
            dn_results.append(result)

    overall, lines = build_sanity_lines(sc, rm_results, dn_results, started)
    try:
        with open(report_path, "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    except OSError as exc:
        print(f"Could not append sanity check to {report_path}: {exc}")
        return
    print(f"[{started.isoformat()}] sanity check {overall} -> {report_path}")


def run_once(cfg, output_dir):
    """Run one assessment. Returns (report_path, verdict). verdict is None on command/parse errors."""
    started = now_utc()
    header = [
        "=" * 72,
        "OZONE CONTAINER BALANCER - ASSESSMENT REPORT",
        render_kv("Generated at (UTC)", started.isoformat()),
        "=" * 72, "",
    ]
    next_due = started + timedelta(seconds=cfg["assessment_interval_s"])
    schedule = [
        "",
        "SCHEDULE",
        render_kv("This assessment ran at (UTC)", started.isoformat()),
        render_kv("Assessment interval", cfg["assessment_interval"]),
        render_kv("Next assessment due at (UTC)", next_due.isoformat()),
    ]

    cmd = build_command(cfg)
    rc, stdout, stderr = run_command(cmd)

    if rc is None or rc != 0:
        lines = header + [
            "RESULT: ERROR - assessment command failed.",
            "",
            render_kv("Command", " ".join(cmd)),
            render_kv("Exit code", "n/a" if rc is None else rc),
            "",
            "STDERR:",
            (stderr.strip() or "(empty)"),
            "",
        ] + render_config(cfg) + schedule
        path = write_report(output_dir, started, lines)
        print(f"[{started.isoformat()}] assessment FAILED -> {path}")
        return path, None

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        lines = header + [
            "RESULT: ERROR - assessment output was not valid JSON.",
            "",
            render_kv("Command", " ".join(cmd)),
            render_kv("Parse error", str(exc)),
            "",
            "RAW OUTPUT (first 4000 chars):",
            stdout[:4000] or "(empty)",
            "",
        ] + render_config(cfg) + schedule
        path = write_report(output_dir, started, lines)
        print(f"[{started.isoformat()}] malformed JSON -> {path}")
        return path, None

    body = ["ASSESSMENT RESULTS"]
    for key, label, kind in LABELS:
        body.append(render_kv(label, fmt_value(data.get(key), kind)))
    body.append("")
    body += render_node_group("Source nodes (over-utilized)", data.get("sourceNodes"))
    body.append("")
    body += render_node_group("Target nodes (under-utilized)", data.get("targetNodes"))
    body.append("")

    verdict, decision = build_decision(data, cfg)
    lines = header + render_config(cfg) + [""] + body + [""] + decision + schedule
    path = write_report(output_dir, started, lines)
    print(f"[{started.isoformat()}] {verdict} -> {path}")
    return path, verdict


def main():
    ap = argparse.ArgumentParser(
        description="Loop container balancer assessment on the configured interval, write "
                    "human-readable reports, and run a Prometheus sanity check when the "
                    "assessment gives a go-ahead. All options come from the JSON config file.")
    ap.add_argument("-c", "--config", default="auto-balancer.config.json",
                    help="Path to the JSON config file (default: ./auto-balancer.config.json)")
    ap.add_argument("-d", "--output-dir", default=os.getcwd(),
                    help="Directory for report files (default: current working directory)")
    args = ap.parse_args()

    cfg = load_config(args.config)
    interval = cfg["assessment_interval_s"]
    sanity_note = ("enabled" if cfg["sanity_check"] is not None
                   else "disabled (sanity_check is null)")

    print(f"Starting auto-balancer loop: interval={cfg['assessment_interval']} "
          f"({interval}s), sanity={sanity_note}, reports -> "
          f"{os.path.abspath(args.output_dir)}. Ctrl-C to stop.")
    try:
        while True:
            path, verdict = run_once(cfg, args.output_dir)
            if verdict == VERDICT_ACTIONABLE:
                run_sanity_check(cfg, path)
            wake = (now_utc() + timedelta(seconds=interval)).isoformat()
            print(f"Sleeping {cfg['assessment_interval']} until next assessment (~{wake}).")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopping auto-balancer loop.")
        sys.exit(0)


if __name__ == "__main__":
    main()
