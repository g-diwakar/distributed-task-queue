#!/usr/bin/env python3
"""
Submit sample jobs to the distributed task queue.
Usage:
    python3 scripts/submit.py               # submit all + poll status
    python3 scripts/submit.py --no-poll     # submit and exit
    python3 scripts/submit.py --type sleep_job --priority high
"""

import json
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime

BASE_URL = "http://localhost:8080"

PRIORITY_MAP = {"low": 1, "medium": 2, "high": 3}

SAMPLE_JOBS = [
    {
        "type": "sleep_job",
        "priority": 2,
        "payload": {"duration_seconds": 3},
        "max_attempts": 3,
    },
    {
        "type": "http_fetch",
        "priority": 3,
        "payload": {"url": "https://httpbin.org/get"},
        "max_attempts": 3,
    },
    {
        "type": "data_transform",
        "priority": 2,
        "payload": {"input": {"name": "  Alice  ", "city": "london", "score": 42}},
        "max_attempts": 2,
    },
    {
        "type": "image_resize",
        "priority": 2,
        "payload": {"width": 1920, "height": 1080},
        "max_attempts": 2,
    },
    {
        "type": "send_email",
        "priority": 1,
        "payload": {"to": "user@example.com", "subject": "Hello from the queue"},
        "max_attempts": 3,
    },
    {
        "type": "fail_job",
        "priority": 1,
        "payload": {},
        "max_attempts": 3,  # exhausts retries → goes to dead letter queue
    },
]

TERMINAL_STATUSES = {"completed", "failed", "dead", "cancelled"}

STATUS_ICONS = {
    "pending":   "⏳",
    "running":   "▶",
    "retrying":  "🔁",
    "completed": "✓",
    "failed":    "✗",
    "dead":      "💀",
    "cancelled": "⊘",
}


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def get(path):
    with urllib.request.urlopen(BASE_URL + path) as resp:
        return json.loads(resp.read())


# ── Actions ───────────────────────────────────────────────────────────────────

def submit_jobs(jobs):
    print(f"Submitting {len(jobs)} job(s) to {BASE_URL}\n")
    ids = []
    for j in jobs:
        result = post("/jobs", j)
        icon = STATUS_ICONS.get(result["status"], "?")
        print(f"  {icon}  [{result['id'][:10]}]  {result['type']:<16}  priority={result['priority']}")
        ids.append(result["id"])
    print()
    return ids


def poll_status(ids, timeout=120):
    print("Polling for results (Ctrl+C to stop)…\n")
    deadline = time.time() + timeout

    while time.time() < deadline:
        jobs = [get(f"/jobs/{jid}") for jid in ids]

        lines = []
        for j in jobs:
            icon = STATUS_ICONS.get(j["status"], "?")
            error = f"  ← {j['error']}" if j.get("error") else ""
            lines.append(f"  {icon}  [{j['id'][:10]}]  {j['type']:<16}  {j['status']}{error}")

        # Clear previous block and reprint
        sys.stdout.write(f"\033[{len(lines)}A" if ids != lines else "")
        for line in lines:
            print(f"\033[K{line}")

        if all(j["status"] in TERMINAL_STATUSES for j in jobs):
            print("\nAll jobs reached a terminal state.")
            return

        time.sleep(1)

    print("\nTimeout reached.")


def list_jobs(status=None):
    path = "/jobs"
    if status:
        path += f"?status={status}"
    jobs = get(path)
    if not jobs:
        print("No jobs found.")
        return
    print(f"{'ID':<12}  {'TYPE':<16}  {'STATUS':<12}  {'PRIORITY':<8}  CREATED")
    print("─" * 70)
    for j in jobs:
        created = datetime.fromisoformat(j["created_at"].replace("Z", "+00:00"))
        print(f"  {j['id'][:10]:<10}  {j['type']:<16}  {j['status']:<12}  {j['priority']:<8}  {created:%H:%M:%S}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    args = sys.argv[1:]
    opts = {
        "no_poll": "--no-poll" in args,
        "list":    "--list"    in args,
        "type":    None,
        "priority": 2,
    }
    if "--type" in args:
        idx = args.index("--type")
        opts["type"] = args[idx + 1]
    if "--priority" in args:
        idx = args.index("--priority")
        opts["priority"] = PRIORITY_MAP.get(args[idx + 1], 2)
    return opts


def main():
    opts = parse_args()

    if opts["list"]:
        list_jobs()
        return

    jobs = SAMPLE_JOBS
    if opts["type"]:
        jobs = [j for j in SAMPLE_JOBS if j["type"] == opts["type"]]
        if not jobs:
            print(f"Unknown type '{opts['type']}'. Available: {[j['type'] for j in SAMPLE_JOBS]}")
            sys.exit(1)
        jobs[0]["priority"] = opts["priority"]

    ids = submit_jobs(jobs)

    if not opts["no_poll"]:
        poll_status(ids)


if __name__ == "__main__":
    try:
        main()
    except urllib.error.URLError as e:
        print(f"Cannot connect to {BASE_URL}: {e}")
        print("Make sure the server is running:  go run ./cmd/server")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nStopped.")
