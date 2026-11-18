import hashlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'ops/erp-telegram-release'))
import install_lifecycle as i


class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.target=self.root/'target';self.target.mkdir();self.units=self.root/'units';self.units.mkdir()
        self.source=ROOT/'ops/erp-telegram-release';self.consumer=b'preserved Point and Mac-IA hooks\n'
        (self.target/'decidir_corte_point.py').write_bytes(self.consumer)
        for name in ('erp_telegram_release.py','erp_telegram_cleanup.py'):(self.target/name).write_text('# old '+name)
        self.calls=[];self.fail=None;self.active=False;self.loaded=True
        self.hashpatch=patch.object(i,'CONSUMER_SHA',hashlib.sha256(self.consumer).hexdigest());self.hashpatch.start();self.addCleanup(self.hashpatch.stop)
    def run_command(self,args,**kwargs):
        self.calls.append(args)
        if args[1]=='stop' and not self.loaded:raise RuntimeError('unit not loaded')
        if self.fail and self.fail(args):raise RuntimeError('simulated service failure')
        return SimpleNamespace(returncode=1 if args[1].startswith('is-') else 0,stdout=('loaded' if self.loaded else 'not-found') if '--property=LoadState' in args else ('active' if self.active else 'inactive'))
    def install(self,activate=False):i.install(self.source,self.target,self.units,self.run_command,lambda:None,activate)
    def test_preserves_consumer_and_existing_monitors(self):
        self.install()
        self.assertEqual((self.target/'decidir_corte_point.py').read_bytes(),self.consumer)
        for call in self.calls:
            self.assertFalse(any(name in call for name in ('erp-mobile-approval.timer','erp-backup-alerts.timer','erp-telegram-release.timer')))
        self.assertFalse(any(c[1]=='enable' for c in self.calls))
    def test_first_install_skips_stop_for_missing_units(self):
        self.loaded=False;self.install()
        self.assertTrue((self.units/'erp-point-archive.service').is_file())
    def test_changed_consumer_refuses_before_writes(self):
        (self.target/'decidir_corte_point.py').write_text('changed')
        with self.assertRaisesRegex(ValueError,'consumer_changed'):self.install()
        self.assertEqual((self.target/'erp_telegram_cleanup.py').read_text(),'# old erp_telegram_cleanup.py');self.assertFalse(self.calls)
    def test_active_job_refuses_without_module_replacement_or_receiver_restart(self):
        self.active=True
        with self.assertRaisesRegex(ValueError,'job_active'):self.install()
        self.assertEqual((self.target/'erp_telegram_cleanup.py').read_text(),'# old erp_telegram_cleanup.py')
        self.assertFalse(any(c[1]=='restart' for c in self.calls))
    def test_activation_failure_keeps_new_compatible_modules(self):
        self.fail=lambda args:args==['systemctl','enable','--now','erp-cleanup-execute.timer']
        with self.assertRaises(RuntimeError):self.install(True)
        self.assertEqual((self.target/'erp_telegram_cleanup.py').read_bytes(),(self.source/'erp_telegram_cleanup.py').read_bytes())
        self.assertEqual(sum(c[1]=='restart' for c in self.calls),1)
    def test_receiver_failure_restores_previous_modules(self):
        once=[False]
        def fail(args):
            if args==['systemctl','restart','erp-point-decision.service'] and not once[0]:once[0]=True;return True
        self.fail=fail
        with self.assertRaises(RuntimeError):self.install()
        self.assertEqual((self.target/'erp_telegram_cleanup.py').read_text(),'# old erp_telegram_cleanup.py')
        self.assertIn(['systemctl','is-active','--quiet','erp-point-decision.service'],self.calls)
    def test_symlink_archive_parent_rejected(self):
        (self.root/'link').symlink_to(self.target,target_is_directory=True)
        with self.assertRaisesRegex(ValueError,'unsafe_archive'):i.provision_archive(self.root/'link'/'archive')

if __name__=='__main__':unittest.main()
