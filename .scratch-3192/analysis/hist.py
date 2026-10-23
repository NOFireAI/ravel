exec(open('analysis/an.py').read())
import json,collections,sys
seg2sz={int(k):v for k,v in json.load(open('analysis/seg2sz.json')).items()}
sz2seg={v:k for k,v in seg2sz.items()}
def oid(k): s=int(k.split(':')[2]); return f"s{sz2seg[s]}/{k.split(':')[0][:8]}"
ABBR={'get':'get','fifo_admit':'admit','fifo_evict_small':'evict_small','fifo_ghost_drop':'ghost_drop','fifo_hit':'fifo_hit','fifo_evict_main':'evict_main','get_or_fetch':'gof'}
def hist(ev,key):
    out=[]
    for e in ev:
        if e['cache']!='C2' or e['key']!=key or e['op'] in ('sf_leader_recheck','admit'): continue
        if e['op']=='get': out.append(f"get:{e['out']}")
        elif e['op']=='fifo_admit': out.append(f"admit:{e['out']}")
        elif e['op']=='fifo_evict_small': out.append(f"evict_small:{e['out']}"+(f"(freq={re.search(r'freq=(\d+)',e['extra']).group(1)})" if 'freq=' in e['extra'] else ''))
        elif e['op']=='fifo_hit': out.append(f"fifo_hit(freq_after={re.search(r'freq_after=(\d+)',e['extra']).group(1)},{re.search(r'queue=(\w+)',e['extra']).group(1)})")
        elif e['op']=='get_or_fetch': 
            role=re.search(r'role=(\w+)',e['extra']).group(1); out.append(f"gof:{e['out']}/{role}" if (role!='leader' or e['out']!='fetched') else 'fetch')
        else: out.append(f"{e['op']}:{e['out']}")
        out[-1]=f"{e['seq']} "+out[-1]
    return out
mode=sys.argv[1]
for t in (['P-1','P-2'] if mode=='P' else ['Q-1','Q-2']):
    pre,runs=load(f'trace-{t}.tsv')
    # objects touched >1 per run, followers, late served
    for r in runs:
        c=collections.Counter(e['key'] for e in r['ev'] if e['cache']=='C2' and e['op']=='get')
        fol=sum(1 for e in r['ev'] if e['cache']=='C2' and e['op']=='get_or_fetch' and ('follower' in e['extra'] or e['out']!='fetched'))
        acc=collections.Counter(e['key'] for e in r['ev'] if e['cache']=='acct')
        hk=[oid(e['key']) for e in r['ev'] if e['cache']=='C2' and e['op']=='get' and e['out']=='hit']
        mainadm=[oid(e['key']) for e in r['ev'] if e['cache']=='C2' and e['op'] in('fifo_evict_small',) and e['out']=='to_main']
        via=[oid(e['key']) for e in r['ev'] if e['op']=='fifo_admit' and e['out']=='main_via_ghost']
        evm=[e for e in r['ev'] if e['op']=='fifo_evict_main']
        print(f"{t} run{r['n']}: C2 gets={sum(c.values())} distinct={len(c)} max/key={max(c.values())} acct max/key={max(acc.values())} follower_or_lateserve={fol} hits={hk} to_main={mainadm} main_via_ghost={via} evict_main={len(evm)}")
    print()
    r1,r2=runs[0],runs[1]
    keys=[]
    for e in r1['ev']:
        if e['cache']=='C2' and e['op']=='get' and e['key'] not in keys: keys.append(e['key'])
    hitkeys=[e['key'] for e in r2['ev'] if e['cache']=='C2' and e['op']=='get' and e['out']=='hit']
    sel = hitkeys if mode=='P' else keys
    for k in sorted(sel,key=lambda k:sz2seg[int(k.split(':')[2])]):
        print(f"{t} {oid(k)} size={k.split(':')[2]}")
        print("   run1:", '; '.join(hist(r1['ev'],k)))
        print("   run2:", '; '.join(hist(r2['ev'],k)))
    print()
