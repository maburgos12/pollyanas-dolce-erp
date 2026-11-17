"""Two real NAS recovery cycles on a private copy; never remove an operational source."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

from erp_point_archive import APP, STATE, Broker
from erp_cleanup_execute import deployment_lock
from erp_cleanup_prepare import atomic_json
from pos_bridge.services.archive_access import ArchiveStore, ArchivePending


def verify(broker, source, parse, revision, proof_path):
    before=broker.store.snapshot(source)
    with tempfile.TemporaryDirectory(prefix='verification-',dir=broker.store.root) as temporary:
        root=Path(temporary);raw=root/'raw';raw.mkdir()
        original=raw/'verification_category.xls';shutil.copyfile(source,original)
        store=ArchiveStore(root/'archive',raw,wait_seconds=0)
        isolated=Broker(store,broker.nas,root/'state',inventory=lambda **kw:{'entries':[]},parse=parse)
        isolated.state.mkdir()
        semantic=parse(original)
        candidate=dict(relative_path=original.name,**before,**semantic)
        entry=isolated.prepare_entry(candidate)
        cycles=[]
        for _ in range(2):
            # Only this private copy is withdrawn, never the supplied operational source.
            original.unlink()
            try:
                with store.open_source(original):raise ValueError('expected_recovery_request')
            except ArchivePending:pass
            result=isolated.recover_requests()
            if result!=[{'id':entry['id'],'state':'recovered'}] or parse(original)!=semantic or store.snapshot(original)!=before:
                raise ValueError('recovery_cycle_failed')
            cycles.append({'sha256':before['sha256'],'semantic_sha256':semantic['semantic_sha256']})
        if broker.store.snapshot(source)!=before:raise ValueError('source_changed')
        proof=dict(revision=revision,successful_recovery_cycles=len(cycles),cycles=cycles,verified_at=int(time.time()),source_sha256=before['sha256'])
        atomic_json(proof_path,proof)
        return proof


def main():
    if os.geteuid()!=0:raise ValueError('root_required')
    os.umask(0o077)
    with deployment_lock():
        revision=subprocess.check_output(['git','-C',str(APP),'rev-parse','HEAD'],text=True).strip()
        broker=Broker();broker.nas.provision();staging=broker.staging()
        # Parse via the installed worker using only our bounded staging copy.
        def parse(path):
            fd,name=tempfile.mkstemp(prefix='verify-',suffix='.xls',dir=staging);os.close(fd)
            staged=Path(name)
            try:
                shutil.copyfile(path,staged);staged.chmod(0o644)
                return broker._parse(staged)
            finally:staged.unlink()
        source=next((p for p in broker.store.sources_root.glob('*_point_sales_category_*.xls') if p.is_file() and not p.is_symlink() and p.stat().st_size<=100_000_000),None)
        if source is None:raise ValueError('no_category_fixture_available')
        STATE.mkdir(mode=0o700,exist_ok=True)
        with broker.store.path_lock(source.name):
            proof=verify(broker,source,parse,revision,STATE/'activation-proof.json')
        print(json.dumps(proof))


if __name__=='__main__':main()
