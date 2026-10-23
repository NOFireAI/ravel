import sys,re,collections,json
def load(fn):
    runs=[];cur=None;pre=[]
    for line in open(fn):
        f=line.rstrip('\n').split('\t')
        seq,cache,op,key,intent,outc,extra=(f+['']*7)[:7]
        ev=dict(seq=int(seq),cache=cache,op=op,key=key,intent=intent,out=outc,extra=extra)
        if op=='RUN': cur=dict(n=int(key),ev=[]);runs.append(cur);continue
        if op=='RUNEND': cur['end']=dict(kv.split('=') for kv in extra.split()); cur=None; continue
        (cur['ev'] if cur else pre).append(ev)
    return pre,runs
def bytes_of(e):
    m=re.search(r'bytes=(\d+)',e['extra']); return int(m.group(1)) if m else 0
