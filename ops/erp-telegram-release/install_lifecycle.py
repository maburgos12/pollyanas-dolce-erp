"""Install Point lifecycle modules from the exact Telegram-approved published checkout."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import time
import subprocess

from erp_telegram_release import Ledger
from install import replace_file,check_policy
from erp_cleanup_execute import deployment_lock

APP=Path('/opt/pastelerias-erp')
TARGET=Path('/usr/local/libexec/erp-point-decision')
# Audited consumer includes d:/c: and existing Mac-IA hooks; never reconstruct it.
CONSUMER_SHA='47f12c6f5197c142407ce36c485f3676c1a67e569cddb59fcf67ea07580984c8'
UNITS=('erp-point-archive.service','erp-point-archive.timer','erp-cleanup-execute.service','erp-cleanup-execute.timer')


def install(source,target=TARGET,units=Path('/etc/systemd/system'),run=subprocess.run,preflight=None,activate=False):
    consumer=target/'decidir_corte_point.py';original=consumer.read_bytes()
    if hashlib.sha256(original).hexdigest()!=CONSUMER_SHA:
        raise ValueError('consumer_changed_requires_review')
    changes={target/name:(source/name).read_bytes() for name in ('erp_telegram_release.py','erp_telegram_cleanup.py')}
    for path,content in changes.items():compile(content,str(path),'exec')
    for name in UNITS:changes[units/name]=(source.parents[1]/'scripts/ops'/name).read_bytes()
    (preflight or (lambda:check_policy(source)))()
    run(['systemd-analyze','verify',*[str(source.parents[1]/'scripts/ops'/name) for name in UNITS]],check=True)
    before={p:p.read_bytes() if p.exists() else None for p in changes}
    prior={name:{kind:run(['systemctl','is-'+kind,'--quiet',name],check=False).returncode==0 for kind in ('active','enabled')} for name in ('erp-point-archive.timer','erp-cleanup-execute.timer')}
    loaded={name:run(['systemctl','show','--property=LoadState','--value',name],check=True,capture_output=True,text=True).stdout.strip()=='loaded' for name in UNITS}
    backup=target/'lifecycle-backups';backup.mkdir(mode=0o700,exist_ok=True)
    for path,content in before.items():
        if content is not None:
            saved=backup/(path.name+'.'+hashlib.sha256(content).hexdigest())
            if not saved.exists():replace_file(saved,content,0o600)
            if saved.read_bytes()!=content:raise ValueError('backup_mismatch')
    try:
        for name in prior:
            if loaded[name]:run(['systemctl','stop',name],check=True)
        for name in ('erp-point-archive.service','erp-cleanup-execute.service'):
            if not loaded[name]:continue
            active=run(['systemctl','show','--property=ActiveState','--value',name],check=True,capture_output=True,text=True).stdout.strip()
            if active not in {'inactive','failed'}:
                raise ValueError('lifecycle_job_active')
    except Exception:
        for name,states in prior.items():
            if states['active']:run(['systemctl','start',name],check=True)
        raise
    try:
        for path,content in changes.items():replace_file(path,content)
        if consumer.read_bytes()!=original:raise ValueError('consumer_changed')
        run(['systemctl','daemon-reload'],check=True)
        run(['systemctl','restart','erp-point-decision.service'],check=True)
        run(['systemctl','is-active','--quiet','erp-point-decision.service'],check=True)
    except Exception:
        for name in prior:
            if loaded[name]:run(['systemctl','stop',name],check=False)
        for path,content in before.items():
            if content is not None:replace_file(path,content)
        run(['systemctl','daemon-reload'],check=True)
        run(['systemctl','restart','erp-point-decision.service'],check=True)
        run(['systemctl','is-active','--quiet','erp-point-decision.service'],check=True)
        for name,states in prior.items():
            if not loaded[name]:continue
            run(['systemctl','enable' if states['enabled'] else 'disable',name],check=True)
            run(['systemctl','start' if states['active'] else 'stop',name],check=True)
        raise
    if consumer.read_bytes()!=original:raise ValueError('consumer_changed')
    # Activation failures retain compatible modules; never roll back under a started job.
    for name,states in prior.items():
        if activate:run(['systemctl','enable','--now',name],check=True)
        elif states['active']:run(['systemctl','start',name],check=True)


def provision_archive(root):
    if any(p.is_symlink() for p in (root,*root.parents)):raise ValueError('unsafe_archive_root')
    root.mkdir(mode=0o755,parents=True,exist_ok=True)
    if os.geteuid()==0:os.chown(root,0,0)
    root.chmod(0o755)
    for name in ('catalog','catalog/objects','catalog/by-path','staging'):
        path=root/name
        if path.is_symlink():raise ValueError('unsafe_archive_directory')
        path.mkdir(mode=0o755,parents=True,exist_ok=True);path.chmod(0o755)
        if os.geteuid()==0:os.chown(path,0,0)
    for name in ('locks','requests'):
        path=root/name
        if path.is_symlink():raise ValueError('unsafe_archive_directory')
        path.mkdir(mode=0o1777,parents=True,exist_ok=True);path.chmod(0o1777)
        if os.geteuid()==0:os.chown(path,0,0)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--release-nonce',required=True);parser.add_argument('--activate',action='store_true');args=parser.parse_args()
    if os.geteuid()!=0:parser.error('root required')
    source=Path(__file__).resolve().parent
    if source!=APP/'ops/erp-telegram-release':raise ValueError('official_checkout_required')
    releases=Ledger(Path('/var/lib/erp-point-decision/releases'))
    with releases.lock(worker=True), deployment_lock():
        if subprocess.check_output(['git','-C',str(APP),'status','--porcelain']).strip():raise ValueError('dirty_checkout')
        head=subprocess.check_output(['git','-C',str(APP),'rev-parse','HEAD'],text=True).strip()
        if args.activate:
            path=Path('/var/lib/erp-point-archive')/'activation-proof.json'
            info=path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=0 or info.st_mode&0o077:
                raise ValueError('unsafe_activation_proof')
            proof=json.loads(path.read_text())
            if proof.get('revision')!=head or proof.get('successful_recovery_cycles',0)<2 or not 0<=time.time()-proof.get('verified_at',0)<3600:
                raise ValueError('two_recovery_cycles_required')
        row=releases.read(args.release_nonce)
        if row['state']!='published' or row['decision']!='approved' or row['release_sha']!=head:raise ValueError('exact_published_approval_required')
        # A reader deployment is not permission to remove historical sources.
        provision_archive(APP/'storage/pos_bridge/archive')
        for path in ('/var/lib/erp-point-archive','/var/lib/erp-cleanup-execute'):
            Path(path).mkdir(mode=0o700,exist_ok=True)
        install(source,activate=args.activate)
    print('Lifecycle installed from approved revision; no historical files removed.')


if __name__=='__main__':main()
