import csv,sys,json
rows=[]
for line in open('samples.tsv'):
    if line.startswith('#') or line.startswith('unix_ts'): continue
    f=line.rstrip('\n').split('\t'); rows.append(f)
cols="unix_ts ok allocated active resident sql_reserved fetch_reserved fetch_cache_resident catalog_cache_resident handoff_overlap budget_limit accounted gap vmrss_bytes rssanon_bytes bg_thread".split()
R=[]
for f in rows:
    d={}
    for i,c in enumerate(cols):
        v=f[i]; d[c]=float(v) if v not in ('',) else None
    R.append(d)
t0=float(open('t-server-start').read()); bend=float(open('t-bench-end').read())
r=json.load(open('bench-report.json')); cs=bend-r['concurrency']['elapsed_s']
print("rows",len(R),"first",R[0]['unix_ts'],"last",R[-1]['unix_ts'], "server start",t0,"conc start",cs,"bench end",bend)
live=[d for d in R if d['ok']==1]
print("ok rows",len(live),"non-ok rows",[ (d['unix_ts']) for d in R if d['ok']!=1])
# assertion
bad=[d['unix_ts'] for d in live if d['unix_ts']-R[0]['unix_ts']>=30 and not (d['allocated'] and d['active'] and d['resident'])]
bg=[d['unix_ts'] for d in live if d['bg_thread']!=1]
print("ASSERT nonzero after 30s: violations",len(bad), "; bg!=1 rows:",len(bg))
G=1e9
conc=[d for d in live if cs<=d['unix_ts']<=bend]
print("conc rows",len(conc))
pk=max(conc,key=lambda d:d['gap'])
def show(d,name):
    ret=d['resident']-d['allocated']; unc=d['allocated']-d['accounted']
    print(f"{name}: ts={d['unix_ts']:.3f} t=+{d['unix_ts']-cs:.1f}s res={d['resident']/G:.3f} act={d['active']/G:.3f} alloc={d['allocated']/G:.3f} acc={d['accounted']/G:.3f} gap={d['gap']/G:.3f} ret={ret/G:.3f} unc={unc/G:.3f} sql={d['sql_reserved']/G:.3f} fetch={d['fetch_reserved']/G:.3f} fcache={d['fetch_cache_resident']/G:.3f} ccache={d['catalog_cache_resident']/G:.3f} overlap={d['handoff_overlap']/G:.3f} vmrss={d['vmrss_bytes']/G:.3f}")
show(pk,"PEAKGAP")
for d in [pk]:
    print({k:int(d[k]) for k in cols[2:] if d[k] is not None})
    ret=d['resident']-d['allocated']; unc=d['allocated']-d['accounted']
    print("ret",int(ret),ret/d['gap'],"res-act",int(d['resident']-d['active']),(d['resident']-d['active'])/d['gap'],"act-alloc",int(d['active']-d['allocated']),(d['active']-d['allocated'])/d['gap'],"unc",int(unc),unc/d['gap'])
show(max(conc,key=lambda d:d['resident']),"PEAKRES")
show(max(conc,key=lambda d:d['allocated']-d['accounted']),"PEAKUNC")
show(max(conc,key=lambda d:d['resident']-d['allocated']),"PEAKRET")
show(max(conc,key=lambda d:d['sql_reserved']),"PEAKSQL")
show(max(conc,key=lambda d:d['handoff_overlap']),"PEAKOVERLAP")
idle=[d for d in live if d['unix_ts']-bend>=300]
show(idle[-1],"IDLE-LAST"); print("idle after last query s", idle[-1]['unix_ts']-bend, "n idle>=300",len(idle))
d=idle[-1]; print("idle ret",int(d['resident']-d['allocated']),"alloc-fcache",int(d['allocated']-d['fetch_cache_resident']))
import statistics as st
print("median gap",st.median(d['gap'] for d in conc)/G,"median ret",st.median(d['resident']-d['allocated'] for d in conc)/G,"median unc",st.median(d['allocated']-d['accounted'] for d in conc)/G)
print("min gap conc",min(d['gap'] for d in conc)/G)
print("peak vmrss",max(live,key=lambda d:d['vmrss_bytes'] or 0)['vmrss_bytes']/G, "at t=+%.1f"%(max(live,key=lambda d:d['vmrss_bytes'] or 0)['unix_ts']-cs))
# idle decay trajectory
for d in live:
    if d['unix_ts']>bend and int(d['unix_ts']-bend)%60<5: print(f"  idle +{d['unix_ts']-bend:.0f}s ret={(d['resident']-d['allocated'])/G:.3f} alloc-fc={(d['allocated']-d['fetch_cache_resident'])/G:.3f} res={d['resident']/G:.3f}")
