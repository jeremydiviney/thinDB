import json,pathlib,subprocess,sys
sys.path.insert(0,'/home/ubuntu/wayroll-bench')
from ordinary_sql_diagnosis import start,stop,command,UNIT
root=pathlib.Path('/home/ubuntu/wayroll-bench/ordinary-sql-implementation')
mode=sys.argv[1]
dop=int(sys.argv[2])
if mode not in ('profile','timing') or dop not in (12,16):raise SystemExit('bad mode or DOP')
labels=sys.argv[3:] or ['base-simple','noest-simple','plans-simple','crossplans','base-cross','child-cross','detail-cross','expanded-simple','interval-quarter']
for index,label in enumerate(labels):
    for tag in (['baseline','candidate'] if index%2==0 else ['candidate','baseline']):
        archive=root/tag
        output=root/(tag+'-'+str(dop)+'-'+mode+'-'+label)
        output.mkdir(exist_ok=True)
        if (output/'complete.json').exists():continue
        binary=(archive/'thindb-profile').resolve()
        try:
            start(archive,output,(root.parent/'region-matrix-0258e6d/data').resolve(),dop,mode=='profile',trace_joins=mode=='profile')
            with (output/'client.log').open('w') as log:
                subprocess.run(['node',str(root/'replay.cjs'),str(output),label,'1' if mode=='profile' else '5'],stdout=log,stderr=subprocess.STDOUT,timeout=300,check=True)
            state=command(['systemctl','show',UNIT,'-p','MemoryPeak','-p','MemoryCurrent','-p','ControlGroup'])
            (output/'memory.txt').write_text(state)
            cg=dict(line.split('=',1) for line in state.splitlines())['ControlGroup']
            (output/'memory-events.txt').write_text(pathlib.Path('/sys/fs/cgroup'+cg+'/memory.events').read_text())
            (output/'complete.json').write_text(json.dumps({'label':label,'tag':tag,'mode':mode,'dop':dop}))
            print(tag,dop,(output/'client.log').read_text().splitlines()[-1],flush=True)
        finally:stop(binary)
