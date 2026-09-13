#!/usr/bin/env python3
"""Assert the pre-registered bands of LEDGER.md runs 3 to 5 against the raw
result JSON. Exits non-zero on any miss; prints every check with its figure."""
import json, sys, os
R = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
def load(n): return json.load(open(os.path.join(R, n)))
r3a = load("run3a-instr-map.json"); r3b = load("run3b-instr-declared.json")
r4 = load("run4-stock-declared.json"); r5 = load("run5-instr-declared-delay20.json")
fails = []
def check(name, ok, detail):
    print(("MET  " if ok else "MISS ") + name + ": " + detail)
    if not ok: fails.append(name)
def entries(r): return {e["id"]: e for e in r["entries"]}
def cold(e): return e["per_run_accounting"][0]
def phase(acc, p, k="wire_bytes"): return next(x for x in acc["wire_bytes_by_phase"] if x["phase"] == p)[k]
FAST = ["narrow_count_filter","fastpath_minmax_ts","agg_group_severity_dur","cpu_regex_histogram","attr_eq_count","attr_group","dur_sum_threshold","attr_project_limit"]
ATTR = ["attr_eq_count","attr_group","dur_sum_threshold","attr_project_limit"]
for label, r in (("3a-map", r3a), ("3b-declared", r3b)):
    E = entries(r); stored = r["dataset"]["stored_bytes"]
    for sid in FAST:
        a = cold(E[sid])
        check(f"B3.1 {label} {sid} scan GETs==40", phase(a,"scan","get_requests")==40, f"{phase(a,'scan','get_requests')}")
        check(f"B3.1 {label} {sid} scan bytes==stored", phase(a,"scan")==stored, f"{phase(a,'scan')} vs {stored}")
        check(f"B3.1 {label} {sid} whole_opens==40 ranged==0", a["logs_whole_object_opens"]==40 and a["logs_ranged_opens"]==0, f"{a['logs_whole_object_opens']}/{a['logs_ranged_opens']}")
    a = cold(E["selective_limit_planpath"])
    check(f"B3.2 {label} planpath plan bytes==stored", phase(a,"plan")==stored, f"{phase(a,'plan')} vs {stored}")
    check(f"B3.2 {label} planpath scan bytes==0", phase(a,"scan")==0, f"{phase(a,'scan')}")
    check(f"B3.2 {label} planpath plan_init>0", a["scan_timing"]["plan_init_elapsed_ns"]>1000, f"{a['scan_timing']['plan_init_elapsed_ns']} ns")
    check(f"B3.2 {label} planpath whole_opens==0", a["logs_whole_object_opens"]==0, f"{a['logs_whole_object_opens']}")
    for e in r["entries"]:
        if e["median_ms"] >= 20:
            spread = (e["max_ms"]-e["min_ms"])/e["median_ms"]
            print(f"B3.7 {label} {e['id']}: spread {(spread):.2f} (min {e['min_ms']:.1f} med {e['median_ms']:.1f} max {e['max_ms']:.1f})")
Ea, Eb = entries(r3a), entries(r3b)
for sid in ATTR:
    a, b = cold(Ea[sid]), cold(Eb[sid])
    check(f"B3.3 {sid} GETs equal", a["object_store_get_requests"]==b["object_store_get_requests"], f"{a['object_store_get_requests']} vs {b['object_store_get_requests']}")
    check(f"B3.3 {sid} bytes equal", a["object_store_bytes"]==b["object_store_bytes"], f"{a['object_store_bytes']} vs {b['object_store_bytes']}")
    check(f"B3.6 {sid} rows equal", Ea[sid]["rows_returned"]==Eb[sid]["rows_returned"], f"{Ea[sid]['rows_returned']} vs {Eb[sid]['rows_returned']}")
    ratio_pages = a["page_stored_bytes_decoded"]/max(1,b["page_stored_bytes_decoded"])
    ratio_dec = a["scan_timing"]["decode_build_elapsed_ns"]/max(1,b["scan_timing"]["decode_build_elapsed_ns"])
    if sid != "dur_sum_threshold":
        check(f"B3.4 {sid} pages decoded ratio>=3", ratio_pages>=3.0, f"{ratio_pages:.2f} ({a['page_stored_bytes_decoded']} vs {b['page_stored_bytes_decoded']})")
        check(f"B3.5 {sid} decode ratio>=1.5", ratio_dec>=1.5, f"{ratio_dec:.2f} ({a['scan_timing']['decode_build_elapsed_ns']/1e6:.1f} vs {b['scan_timing']['decode_build_elapsed_ns']/1e6:.1f} ms)")
    else:
        print(f"info dur_sum_threshold pages ratio {ratio_pages:.2f} decode ratio {ratio_dec:.2f}")
    print(f"info {sid}: map cold {Ea[sid]['cold_ms']:.1f} med {Ea[sid]['median_ms']:.1f} cpu {a['cpu_ms']} emit {a['scan_timing']['emit_elapsed_ns']/1e6:.0f}ms | declared cold {Eb[sid]['cold_ms']:.1f} med {Eb[sid]['median_ms']:.1f} cpu {b['cpu_ms']} emit {b['scan_timing']['emit_elapsed_ns']/1e6:.0f}ms")
E4 = entries(r4)
for sid, e in Eb.items():
    s = E4[sid]
    if s["median_ms"] >= 10:
        d = abs(e["median_ms"]-s["median_ms"])/s["median_ms"]
        check(f"B4 overhead {sid}", d<=0.15, f"instr med {e['median_ms']:.1f} vs stock {s['median_ms']:.1f} ({d*100:.0f}%)")
    else:
        print(f"info B4 {sid}: instr med {e['median_ms']:.2f} vs stock {s['median_ms']:.2f} (below 10 ms, not judged)")
E5 = entries(r5)
for sid in FAST:
    t = cold(E5[sid])["scan_timing"]
    check(f"B5.1 {sid} open_max in [100,115]ms", 100e6 <= t["open_elapsed_max_ns"] <= 115e6, f"{t['open_elapsed_max_ns']/1e6:.1f} ms")
    check(f"B5.2 {sid} pending==40", t["open_pending_polls"]==40, f"{t['open_pending_polls']}")
    base = cold(Eb[sid])["scan_timing"]["decode_build_elapsed_ns"]
    d = abs(t["decode_build_elapsed_ns"]-base)/max(1,base)
    check(f"B5.3 {sid} decode within 15%", d<=0.15, f"{t['decode_build_elapsed_ns']/1e6:.1f} vs {base/1e6:.1f} ms ({d*100:.0f}%)")
    print(f"info B5 {sid}: cold {E5[sid]['cold_ms']:.1f} ms first_batch {t['first_batch_elapsed_min_ns']/1e6:.1f} stream_max {t['stream_elapsed_max_ns']/1e6:.1f} decode_max {t['decode_build_elapsed_max_ns']/1e6:.1f} GETs {cold(E5[sid])['object_store_get_requests']} unattributed {cold(E5[sid])['get_requests_unattributed']}")
t = cold(E5["selective_limit_planpath"])["scan_timing"]
check("B5.4 planpath plan_init in [100,115]", 100e6 <= t["plan_init_elapsed_ns"] <= 115e6, f"{t['plan_init_elapsed_ns']/1e6:.1f} ms")
check("B5.4 planpath open_max<5ms", t["open_elapsed_max_ns"] < 5e6, f"{t['open_elapsed_max_ns']/1e6:.2f} ms")
for sid, e in E5.items():
    check(f"B5.5 {sid} cold>=100ms", e["cold_ms"]>=100, f"{e['cold_ms']:.1f}")
print(f"\n{len(fails)} misses" if fails else "\nall bands met")
sys.exit(1 if fails else 0)
