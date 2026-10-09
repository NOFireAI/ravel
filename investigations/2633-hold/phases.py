"""Row 1's figure split by phase, and how often handoff_overlap equals fetch_reserved.

usage: phases.py <run-dir> [<run-dir> ...]   (each holds samples.tsv,
bench-report.json and t-bench-end)
"""
import json,sys
C="unix_ts ok allocated active resident sql_reserved fetch_reserved fetch_cache_resident catalog_cache_resident handoff_overlap budget_limit accounted gap vmrss_bytes".split()
for d in sys.argv[1:]:
    rows=[]
    for l in open(d+'/samples.tsv'):
        if l[0] in '#u': continue
        f=l.split('\t')
        if f[1]!='1': continue
        r={c:(float(f[i]) if f[i] else None) for i,c in enumerate(C)}
        if r['accounted'] is None: continue
        rows.append(r)
    be=float(open(d+'/t-bench-end').read()); cs=be-json.load(open(d+'/bench-report.json'))['concurrency']['elapsed_s']
    u=lambda r:r['allocated']-r['accounted']+r['handoff_overlap']
    ser=[r for r in rows if r['unix_ts']<cs]; idle=[r for r in rows if r['unix_ts']>be]
    eq=sum(1 for r in rows if r['handoff_overlap']==r['fetch_reserved'])
    ms=max(ser,key=u); mi=max(idle,key=u)
    print(d, "serial n",len(ser),"max %.3f at t=%+.1f"%(u(ms)/1e9,ms['unix_ts']-cs), "sql %.3f fetch %.3f"%(ms['sql_reserved']/1e9,ms['fetch_reserved']/1e9), "| idle max %.3f"%(u(mi)/1e9), "| overlap==fetch_reserved %d/%d"%(eq,len(rows)))
