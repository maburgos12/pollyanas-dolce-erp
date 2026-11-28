#!/usr/bin/env python3
"""Opt-in synthetic Redis/Celery warm test. Never connects to production.

Creates only two labelled containers and one internal network with immutable
local images. The existing task PostgreSQL container is never touched.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

TASK = 'telegram-supervised-publisher-20261127'
REDIS = 'erp_telegram_supervised_20261127_redis'
CELERY = 'erp_telegram_supervised_20261127_celery'
NETWORK = 'erp_telegram_supervised_20261127_net'
REDIS_IMAGE = 'sha256:2e3eb11462c4e3759e6972678dd051d150ff54b1e60714b3c33b701260172a90'
APP_IMAGE = 'sha256:719d113d641a546347375cb25cf08cec2372908185978b76a3725721a35547f6'
LIMIT = 8 * 1024 * 1024


def inside():
    import datetime as dt
    import fcntl
    import importlib.metadata
    import signal
    import socket
    import redis
    from celery import Celery
    import erp_release_preservation as preservation
    import erp_release_snapshot as snapshot

    started = time.monotonic()
    url = 'redis://' + REDIS + ':6379/0'
    app_source = """from celery import Celery
app=Celery('synthetic',broker=%r)
app.conf.update(task_serializer='json',accept_content=['json'],task_ignore_result=True,
 broker_connection_retry_on_startup=True,worker_prefetch_multiplier=1,
 task_acks_late=True,broker_transport_options={'visibility_timeout':7200})
@app.task(name='synthetic.marker')
def marker():
 raise RuntimeError('Future synthetic task must never execute during this test')
""" % url
    Path('/scratch/synthetic.py').write_text(app_source)
    app = Celery('test_driver', broker=url)
    client = redis.Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2)
    deadline = time.monotonic()+30
    while True:
        try:
            assert client.ping()
            break
        except redis.ConnectionError:
            if time.monotonic() > deadline:raise
            time.sleep(.2)
    assert client.dbsize() == 0, 'Synthetic Redis must start empty'
    children = []
    logs = []
    nodes = [queue+'@'+socket.gethostname() for queue in preservation.QUEUES]
    try:
        for queue,node in zip(preservation.QUEUES,nodes):
            log = open('/scratch/'+queue+'.log','wb');logs.append(log)
            process = subprocess.Popen([sys.executable,'-m','celery','-A','synthetic:app','worker',
                '--pool=solo','--concurrency=1','--loglevel=WARNING','--without-gossip','--without-mingle',
                '--without-heartbeat','--queues',queue,'--hostname',node],stdout=log,stderr=log)
            children.append(process)
        deadline = time.monotonic()+75
        while True:
            assert all(p.poll() is None for p in children), 'Worker exited during startup'
            replies=app.control.inspect(destination=nodes,timeout=1).ping()
            if isinstance(replies,dict) and set(replies)==set(nodes):break
            if time.monotonic()>deadline:raise RuntimeError('Seven workers did not become ready')
            time.sleep(.5)
        idle=snapshot.collect_snapshot(client,None,nodes,app=app,deadline=time.monotonic()+20)
        for phase,values in idle['replies'].items():
            assert set(values)==set(nodes)
            assert all(v==({'ok':'pong'} if phase=='ping' else []) for v in values.values())
        eta=dt.datetime.now(dt.timezone.utc)+dt.timedelta(hours=1)
        sent=[app.send_task('synthetic.marker',queue=q,eta=eta).id for q in preservation.QUEUES]
        deadline=time.monotonic()+30
        while client.hlen('unacked')!=7:
            assert time.monotonic()<deadline,'ETA tasks not reserved'
            time.sleep(.2)
        before=snapshot.collect_snapshot(client,None,nodes,app=app,deadline=time.monotonic()+20)
        preservation.validate_before(before)
        assert {r['id'] for r in before['records']}==set(sent)
        assert all(r['location']=='unacked' for r in before['records'])
        assert all(len(v)==1 for v in before['replies']['scheduled'].values())
        stop_started=time.monotonic()
        for p in children:p.send_signal(signal.SIGTERM)
        exit_codes=[]
        for p in children:exit_codes.append(p.wait(timeout=max(1,60-(time.monotonic()-stop_started))))
        assert exit_codes==[0]*7, 'Natural worker exit must be zero'
        natural_stop_seconds=time.monotonic()-stop_started
        after=snapshot.collect_snapshot(client,before,[],deadline=time.monotonic()+20)
        proof=preservation.validate_restored(before,after,scope='all',all_stopped=True)
        assert set(proof['outcomes'])==set(sent)
        assert set(proof['outcomes'].values())=={'restored_unique_exact'}
        assert client.hlen('unacked')==0
        assert sum(client.llen(k) for k in preservation.QUEUE_KEYS)==7
        lock_path='/scratch/deploy.lock'
        fd=os.open(lock_path,os.O_CREAT|os.O_RDWR,0o600)
        bad=os.open('/scratch/wrong.lock',os.O_CREAT|os.O_RDWR,0o600)
        try:
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            env=dict(os.environ,ERP_DEPLOY_LOCK=lock_path)
            script='source /deploy_web_safe.sh; acquire_deployment_lock "$1"'
            inherited=subprocess.run(['bash','-c',script,'fixture',str(fd)],pass_fds=(fd,),env=env,capture_output=True,timeout=5)
            wrong=subprocess.run(['bash','-c',script,'fixture',str(bad)],pass_fds=(bad,),env=env,capture_output=True,timeout=5)
            concurrent=subprocess.run(['bash','-c','source /deploy_web_safe.sh; acquire_deployment_lock'],env=env,capture_output=True,timeout=5)
            assert inherited.returncode==0 and wrong.returncode==2 and concurrent.returncode==1
            locks={'inherited_same_ofd':inherited.returncode,'wrong_inode_rejected':wrong.returncode,'concurrent_blocked':concurrent.returncode}
        finally:
            os.close(bad);os.close(fd)
        receipt={'status':'PASS','kind':'real_synthetic_redis_celery','versions':{k:importlib.metadata.version(k) for k in ('celery','kombu','redis')},
                 'redis_server_version':client.info('server')['redis_version'],
                 'nodes':nodes,'idle_rpc_phases':list(idle['replies']),'before':before,'after':after,'proof':proof,
                 'exit_codes':exit_codes,'natural_stop_seconds':natural_stop_seconds,
                 'deployment_flock':locks,'elapsed_seconds':time.monotonic()-started,
                 'scope_limit':'Transport preservation and natural stop only; no production or complete ERP callback validation'}
        print(json.dumps(receipt),flush=True)
    finally:
        client.close()
        # On failure preserve tails and try a single natural stop for live owned
        # synthetic workers. Outer teardown still records a failed test.
        for p in children:
            if p.poll() is None:p.send_signal(signal.SIGTERM)
        for p in children:
            if p.poll() is None:
                try:p.wait(timeout=10)
                except subprocess.TimeoutExpired:pass
        for log in logs:
            log.close()
        for path in Path('/scratch').glob('*.log'):
            print(path.name+': '+path.read_text(errors='replace')[-4096:],file=sys.stderr)


def external(artifacts, subnet=None):
    root=Path(__file__).resolve().parents[2]
    artifacts=Path(artifacts).resolve();artifacts.mkdir(parents=True,exist_ok=True)
    resources={'task':TASK,'owner':'publisher_host_adapter','created_at':time.time(),
               'data':'synthetic disposable ETA messages only, no PostgreSQL or unique operational data',
               'names':{'redis':REDIS,'celery':CELERY,'network':NETWORK},'images':[REDIS_IMAGE,APP_IMAGE],
               'ports':[],'volumes':[],'mounts':'read-only source; tmpfs scratch/data',
               'created':[],'removed':[],'status':'planned',
               'mac_available_bytes_before':os.statvfs(root).f_bavail*os.statvfs(root).f_frsize,
               'preserved_resources':'Existing task PostgreSQL container and every unrelated Docker resource'}
    record=artifacts/'integration-resources.json'
    if record.exists():raise RuntimeError('Existing resource record must be reviewed, not overwritten')
    def save():
        fd=os.open(record,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
        with os.fdopen(fd,'w') as f:
            json.dump(resources,f,indent=2);f.flush();os.fsync(f.fileno())
    save()  # Ownership before creating any resource.
    def run(args,timeout=30,check=True):
        value=subprocess.run(args,capture_output=True,timeout=timeout)
        if len(value.stdout)+len(value.stderr)>LIMIT:raise RuntimeError('Output cap exceeded')
        if check and value.returncode:raise RuntimeError('Command failed: '+args[0]+' '+args[1]+' '+value.stderr.decode(errors='replace')[-1500:])
        return value
    def owned(kind,identifier):
        value=run(['docker',kind,'inspect',identifier])
        item=json.loads(value.stdout)[0]
        labels=item.get('Labels',{}) if kind=='network' else item['Config']['Labels']
        assert labels.get('codex.task')==TASK,'Refuse unowned resource'
        return item
    try:
        for image in (REDIS_IMAGE,APP_IMAGE):
            assert json.loads(run(['docker','image','inspect',image]).stdout)[0]['Id']==image
        for kind,name in (('container',REDIS),('container',CELERY),('network',NETWORK)):
            assert run(['docker',kind,'inspect',name],check=False).returncode!=0, 'Existing name cannot be reused'
        extra=[]
        if subnet:
            import ipaddress
            proposed=ipaddress.ip_network(subnet)
            ids=run(['docker','network','ls','-q']).stdout.decode().split()
            existing=json.loads(run(['docker','network','inspect',*ids]).stdout)
            for network_record in existing:
                for value in network_record['IPAM'].get('Config') or []:
                    known=ipaddress.ip_network(value['Subnet'])
                    assert known.version!=proposed.version or not known.overlaps(proposed), 'Subnet overlap'
            (artifacts/'network-inventory.json').write_text(json.dumps(existing,indent=2))
            resources['reviewed_explicit_subnet']=subnet;save()
            extra=['--subnet',subnet]
        network=run(['docker','network','create','--internal',*extra,'--label','codex.task='+TASK,NETWORK]).stdout.decode().strip()
        resources['created'].append({'kind':'network','id':network});save()
        assert owned('network',network)['Internal'] is True
        common=['--read-only','--cpus','2','--memory','768m','--pids-limit','128','--network',NETWORK,
                '--label','codex.task='+TASK,'--cap-drop','ALL','--security-opt','no-new-privileges:true']
        redisid=run(['docker','create','--name',REDIS,*common,'--user','1000:1000','--tmpfs','/data:rw,nosuid,nodev,size=134217728,uid=1000,gid=1000',
                    REDIS_IMAGE,'redis-server','--save','','--appendonly','no','--maxmemory','128mb','--maxmemory-policy','noeviction']).stdout.decode().strip()
        resources['created'].append({'kind':'container','id':redisid});save()
        owned('container',redisid);run(['docker','start',redisid])
        celeryid=run(['docker','create','--name',CELERY,*common,'--user','1000:1000','--workdir','/scratch',
            '--tmpfs','/scratch:rw,nosuid,nodev,size=134217728,uid=1000,gid=1000','--tmpfs','/tmp:rw,nosuid,nodev,size=33554432',
            '--mount','type=bind,src='+str(root/'ops/erp-telegram-release')+',dst=/tested,readonly',
            '--mount','type=bind,src='+str(root/'scripts/deploy_web_safe.sh')+',dst=/deploy_web_safe.sh,readonly',
            '--mount','type=bind,src='+str(Path(__file__).resolve())+',dst=/integration.py,readonly',
            '--entrypoint','/usr/bin/env',APP_IMAGE,'-i','PATH=/opt/venv/bin:/usr/local/bin:/usr/bin:/bin',
            'HOME=/scratch','PYTHONPATH=/scratch:/tested','/opt/venv/bin/python','/integration.py','--inside']).stdout.decode().strip()
        resources['created'].append({'kind':'container','id':celeryid});save()
        item=owned('container',celeryid)
        assert item['HostConfig']['ReadonlyRootfs'] and not item['HostConfig']['PortBindings']
        resources['status']='running';save()
        result=run(['docker','start','-a',celeryid],timeout=240,check=False)
        (artifacts/'integration.stdout.json').write_bytes(result.stdout)
        (artifacts/'integration.stderr.log').write_bytes(result.stderr)
        state=owned('container',celeryid)['State']
        resources['celery_terminal_state']=state;save()
        assert result.returncode==0 and state['ExitCode']==0 and not state['OOMKilled'],'Synthetic integration failed; see logs'
        receipt=json.loads(result.stdout)
        assert receipt['status']=='PASS'
        receipt['tested_sources']={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in
            (root/'ops/erp-telegram-release/erp_release_preservation.py',root/'ops/erp-telegram-release/erp_release_snapshot.py',root/'scripts/deploy_web_safe.sh',Path(__file__).resolve())}
        (artifacts/'integration-proof.json').write_text(json.dumps(receipt,indent=2)+'\n')
        resources['status']='passed'
    except BaseException:
        resources['status']='failed'
        raise
    finally:
        # Only exact IDs created above, checked for ownership again before removal.
        for resource in reversed(resources['created']):
            item=owned(resource['kind'],resource['id'])
            if resource['kind']=='container':
                if item['State']['Running']:run(['docker','stop','--timeout','20',resource['id']],timeout=30)
                run(['docker','rm',resource['id']])
            else:run(['docker','network','rm',resource['id']])
            resources['removed'].append(resource);save()
        resources['resources_remaining']=[]
        resources['mac_available_bytes_after']=os.statvfs(root).f_bavail*os.statvfs(root).f_frsize
        resources['mac_available_delta_bytes']=resources['mac_available_bytes_after']-resources['mac_available_bytes_before']
        resources['space_measurement_limit']='Concurrent Mac disk change, not attributable recovered bytes from tmpfs-only test'
        resources['cleanup_verified']=all(run(['docker',r['kind'],'inspect',r['id']],check=False).returncode!=0 for r in resources['created'])
        save()
    print(json.dumps({'status':resources['status'],'cleanup_verified':resources['cleanup_verified'],'evidence':str(artifacts/'integration-proof.json')}))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--inside',action='store_true');parser.add_argument('--execute',action='store_true');parser.add_argument('--artifacts');parser.add_argument('--subnet',help='Explicit subnet only after host route review; Docker overlaps checked')
    args=parser.parse_args()
    if args.inside:inside()
    elif args.execute and args.artifacts:external(args.artifacts,args.subnet)
    else:parser.error('Explicit --execute --artifacts required')
