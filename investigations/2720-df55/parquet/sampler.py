#!/usr/bin/env python3
"""Samples one server pid every second until it exits.

Columns: VmRSS and VmHWM of the server (kB), the server's cumulative CPU
ticks, the host's cumulative busy and total ticks from /proc/stat, the
cumulative CPU ticks of every process whose command line names the co-tenant
marker (another executor's workdir or its store), and the 1-minute load.

usage: sampler.py <pid> <out.tsv> [co-tenant-marker ...]
"""
import os
import sys
import time

pid, out = sys.argv[1], sys.argv[2]
markers = [m.encode() for m in sys.argv[3:]]


def ticks(p):
    with open(f"/proc/{p}/stat") as s:
        f = s.read().rsplit(")", 1)[1].split()
    return int(f[11]) + int(f[12])


def host():
    with open("/proc/stat") as s:
        f = [int(x) for x in s.readline().split()[1:]]
    idle = f[3] + f[4]
    return sum(f) - idle, sum(f)


def cotenant():
    total = 0
    if not markers:
        return total
    for p in os.listdir("/proc"):
        if not p.isdigit():
            continue
        try:
            with open(f"/proc/{p}/cmdline", "rb") as c:
                cmd = c.read()
            if any(m in cmd for m in markers):
                total += ticks(p)
        except (FileNotFoundError, ProcessLookupError, PermissionError, IndexError):
            continue
    return total


with open(out, "w", buffering=1) as f:
    f.write("unix_ts\tvmrss_kb\tvmhwm_kb\tserver_ticks\thost_busy\thost_total\tcotenant_ticks\tload1\n")
    while True:
        try:
            with open(f"/proc/{pid}/status") as s:
                fields = dict(
                    line.split(":", 1) for line in s.read().splitlines() if ":" in line
                )
            st = ticks(pid)
        except (FileNotFoundError, ProcessLookupError):
            break
        rss = fields.get("VmRSS", "").split()
        hwm = fields.get("VmHWM", "").split()
        if not rss:
            break
        busy, total = host()
        with open("/proc/loadavg") as l:
            load1 = l.read().split()[0]
        f.write(
            f"{time.time():.3f}\t{rss[0]}\t{hwm[0] if hwm else ''}\t{st}\t{busy}\t{total}\t{cotenant()}\t{load1}\n"
        )
        time.sleep(1.0)
