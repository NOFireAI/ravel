exec(open('analysis/an.py').read())
import json
for t in ['P-1','P-2','Q-1','Q-2']:
    pre,runs=load(f'trace-{t}.tsv')
    rep=json.load(open(f'report-{t}.json'))
    # find per_run_accounting
    def find(o,k):
        if isinstance(o,dict):
            if k in o: return o[k]
            for v in o.values():
                r=find(v,k)
                if r is not None: return r
        if isinstance(o,list):
            for v in o:
                r=find(v,k)
                if r is not None: return r
    pra=find(rep,'per_run_accounting')
    for r in runs:
        ev=r['ev']
        c2h=[e for e in ev if e['cache']=='C2' and e['op']=='get' and e['out']=='hit']
        c2m=[e for e in ev if e['cache']=='C2' and e['op']=='get' and e['out']=='miss']
        unc=[e for e in ev if e['cache']=='acct' and e['out']=='none']
        cath=[e for e in ev if e['cache'].startswith('cat:') and e['out']=='hit']
        catm=[e for e in ev if e['cache'].startswith('cat:') and e['out']=='miss']
        ah=[e for e in ev if e['cache']=='acct' and e['op']=='hit']
        am=[e for e in ev if e['cache']=='acct' and e['op']=='miss']
        hits=len(c2h)+len(cath); miss=len(c2m)-len(unc)+len(catm)
        by=sum(map(bytes_of,c2h))+sum(map(bytes_of,cath))
        p=pra[r['n']-1]
        ok = (hits,miss,by)==(p['cache_hits'],p['cache_misses'],p['cache_bytes'])==(int(r['end']['cache_hits']),int(r['end']['cache_misses']),int(r['end']['cache_bytes']))
        print(t,r['n'],'trace',hits,miss,by,'| report',p['cache_hits'],p['cache_misses'],p['cache_bytes'],'| split C2 h/m/B',len(c2h),len(c2m)-len(unc),sum(map(bytes_of,c2h)),'cat h/m/B',len(cath),len(catm),sum(map(bytes_of,cath)),'acct h/m',len(ah),len(am),'OK' if ok else 'DIFF')
