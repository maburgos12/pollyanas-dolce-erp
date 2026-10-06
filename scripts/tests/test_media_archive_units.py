"""Standalone systemd safety contract; no services or database are started."""
import configparser
from pathlib import Path
import shlex
import unittest


UNITS = Path(__file__).resolve().parents[2] / "infra" / "systemd"
MOUNT = r"opt-pastelerias\x2derp-storage-nas-media.mount"


def unit(name):
    parser = configparser.ConfigParser(interpolation=None)
    parser.optionxform = str
    with (UNITS / name).open() as source:
        parser.read_file(source)
    return parser


class MediaArchiveUnitsTests(unittest.TestCase):
    def test_mount_is_read_only_bounded_and_independent_of_boot(self):
        mount = unit(MOUNT)
        path = mount["Mount"]["Where"]
        self.assertEqual(path, "/opt/pastelerias-erp/storage/nas/media")
        self.assertEqual(path.lstrip("/").replace("-", r"\x2d").replace("/", "-") + ".mount", MOUNT)
        self.assertEqual(mount["Mount"]["What"], "//10.77.216.2/ERP_MEDIA_ARCHIVE")
        self.assertEqual(mount["Mount"]["Type"], "cifs")
        options = set(mount["Mount"]["Options"].split(","))
        self.assertTrue({"ro", "seal", "nosuid", "nodev", "noexec", "soft", "nofail",
                         "vers=3.1.1", "echo_interval=5", "uid=1000", "gid=1000",
                         "dir_mode=0550", "file_mode=0440",
                         "credentials=/root/.config/erp-media-archive/reader.credentials"} <= options)
        self.assertNotIn("rw", options)
        self.assertEqual(mount["Mount"]["TimeoutSec"], "30s")
        self.assertEqual(mount["Unit"]["Requires"], "wg-quick@wg-erp-qnap.service")
        self.assertIn("network-online.target", mount["Unit"]["After"].split())
        self.assertNotIn("Install", mount)

    def test_service_is_only_the_explicit_month_pilot(self):
        service = unit("erp-media-archive.service")
        for key in ("Requires", "After"):
            self.assertEqual(set(service["Unit"][key].split()), {"docker.service", MOUNT})
        self.assertEqual(dict(service["Service"]), {
            "Type": "oneshot", "User": "root", "WorkingDirectory": "/opt/pastelerias-erp",
            "UMask": "0077", "TimeoutStartSec": "30min", "ExecStart": service["Service"]["ExecStart"],
        })
        self.assertEqual(shlex.split(service["Service"]["ExecStart"]), [
            "/usr/bin/docker", "compose", "-f", "/opt/pastelerias-erp/docker-compose.yml",
            "exec", "-T", "web", "/usr/bin/timeout", "--signal=TERM", "--kill-after=30s", "1700s",
            "/opt/venv/bin/python", "manage.py", "sync_bitacora_media_archive",
            "--month", "2026-05", "--plan-dir", "/app/storage/media_archive_jobs",
            "--stage-root", "/app/storage/media_archive_export", "--verification-user", "admin",
            "--verification-origin", "http://127.0.0.1:8000",
        ])
        self.assertNotIn("Install", service)

    def test_timer_retries_and_catches_missed_calendar_runs(self):
        timer = unit("erp-media-archive.timer")
        self.assertEqual(dict(timer["Timer"]), {
            "OnBootSec": "5min", "OnCalendar": "hourly", "Persistent": "true",
            "Unit": "erp-media-archive.service",
        })
        self.assertEqual(timer["Install"]["WantedBy"], "timers.target")


if __name__ == "__main__":
    unittest.main()
