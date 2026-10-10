#!/usr/bin/env python3
"""Sample a process's VmRSS/VmHWM and the host load average once a second.

Usage: sampler.py <pid> <out.tsv>. Runs until the process exits.
"""
import os
import sys
import time

pid, out = int(sys.argv[1]), sys.argv[2]
with open(out, "w") as f:
    f.write("unix\tvmrss_kb\tvmhwm_kb\tload1\tmemavailable_kb\n")
    while True:
        try:
            status = open(f"/proc/{pid}/status").read()
        except OSError:
            break
        fields = {}
        for line in status.splitlines():
            k, _, v = line.partition(":")
            fields[k] = v.strip().split(" ")[0]
        load1 = open("/proc/loadavg").read().split()[0]
        avail = "0"
        for line in open("/proc/meminfo"):
            if line.startswith("MemAvailable:"):
                avail = line.split()[1]
        f.write(f"{time.time():.1f}\t{fields.get('VmRSS', '0')}\t{fields.get('VmHWM', '0')}\t{load1}\t{avail}\n")
        f.flush()
        time.sleep(1)
