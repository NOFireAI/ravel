exec(open('analysis/an.py').read())
import json
def sz(k): return 'o'+k.split(':')[2]
out={}
for t in ['P-1','P-2','Q-1','Q-2']:
    pre,runs=load(f'trace-{t}.tsv')
    pra=json.load(open(f'report-{t}.json'))['entries'][0]['per_run_accounting']
    maps=[]
    for r in runs:
        order=[]
        for e in r['ev']:
            if e['cache']=='C2' and e['op']=='get' and e['key'] not in order: order.append(e['key'])
        segs=sorted(pra[r['n']-1]['scan_timing']['segments'],key=lambda s:s['open_start_ns'])
        maps.append({k:(s['segment'],s['partition']) for k,s in zip(order,segs)})
        segorder=[s['segment'] for s in segs]
        hits=[sz(e['key']) for e in r['ev'] if e['cache']=='C2' and e['op']=='get' and e['out']=='hit']
        print(t,'run',r['n'],'segment open order:',' '.join(map(str,segorder)))
        print('   partitions        :',' '.join(str(s['partition']) for s in segs))
    stable=all(m==maps[0] for m in maps)
    print(t,'key->segment mapping stable across 4 runs (open_start order == trace lookup order):',stable)
    m=maps[0]
    print('  seg->size:',' '.join(f"s{v[0]}={sz(k)}" for k,v in sorted(m.items(),key=lambda kv:kv[1][0])))
    out[t]={k:v for k,v in m.items()}
json.dump({t:{k:v for k,v in m.items()} for t,m in out.items()},open('/tmp/s3192/segmap.json','w'))
