import importlib.util
from pathlib import Path
import unittest


class InstallerContractTests(unittest.TestCase):
    def test_alias_uses_only_service_key_and_pinned_host_file(self):
        path = Path(__file__).parents[1] / 'ops/install_nas_service_access.py'
        self.assertTrue(path.exists(), 'service access installer is missing')
        spec = importlib.util.spec_from_file_location('nas_access_installer', path)
        installer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(installer)
        config = installer.config_block()
        for required in ('Host qnap-nasdolce-verify', 'HostName 10.77.216.2',
                         'User DolcePasteleria', 'IdentitiesOnly yes', 'IdentityAgent none',
                         'BatchMode yes', 'StrictHostKeyChecking yes',
                         'ClearAllForwardings yes', 'RequestTTY no',
                         'PasswordAuthentication no', 'KbdInteractiveAuthentication no',
                         '/etc/erp-nas-access/id_ed25519', '/etc/erp-nas-access/known_hosts'):
            self.assertIn(required, config)
        self.assertNotIn('StrictHostKeyChecking no', config)


if __name__ == '__main__':
    unittest.main()
