exec(open('analysis/an.py').read())
import json
seg2sz={int(k):v for k,v in json.load(open('analysis/seg2sz.json')).items()}
sz2seg={v:k for k,v in seg2sz.items()}
for t in ['P-1','P-2','Q-1','Q-2']:
    pre,runs=load(f'trace-{t}.tsv')
    for r in runs:
        order=[];res=[]
        for e in r['ev']:
            if e['cache']=='C2' and e['op']=='get' and e['key'] not in order:
                order.append(e['key']); res.append(('s%d'%sz2seg[int(e['key'].split(':')[2])])+('*' if e['out']=='hit' else ''))
        print(f"{t} run {r['n']}: {' '.join(res)}")
