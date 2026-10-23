exec(open('analysis/an.py').read())
import json,collections
def sz(k): return int(k.split(':')[2])
votes=collections.defaultdict(collections.Counter)
for t in ['P-1','P-2','Q-1','Q-2']:
    pre,runs=load(f'trace-{t}.tsv')
    pra=json.load(open(f'report-{t}.json'))['entries'][0]['per_run_accounting']
    for r in runs:
        order=[]
        for e in r['ev']:
            if e['cache']=='C2' and e['op']=='get' and e['key'] not in order: order.append(e['key'])
        segs=sorted(pra[r['n']-1]['scan_timing']['segments'],key=lambda s:s['open_start_ns'])
        for k,s in zip(order,segs): votes[s['segment']][sz(k)]+=1
seg2sz={}
for s in sorted(votes):
    (best,n),=votes[s].most_common(1); seg2sz[s]=best
    print(s,best,n,'/16', dict(votes[s]) if n<16 else '')
print('bijective:',len(set(seg2sz.values()))==25)
json.dump(seg2sz,open('analysis/seg2sz.json','w'))
