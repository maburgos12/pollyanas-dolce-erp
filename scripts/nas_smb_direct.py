"""Read exact backup files over encrypted SMB without the kernel CIFS mount."""
import hashlib
import re
from pathlib import Path
import stat
import subprocess


HOST = "10.77.216.2"
SOURCE = f"//{HOST}/ERP_BACKUPS"
SHARE = "\\\\" + HOST + "\\ERP_BACKUPS"
UNIT = r"opt-pastelerias\x2derp-storage-nas-compartidos-erp_backups.mount"
FILE = re.compile(r"backup_[0-9]{8}_[0-9]{6}\.(?:manifest|sql\.gz|conteos\.tar\.gz|inventory-audit\.tar\.gz|media\.json)")


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _protected_file(path, forbidden_mode):
    info = path.lstat()
    _require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and not info.st_mode & forbidden_mode,
             "configuración SMB no protegida")


def _config():
    def property_value(name):
        return subprocess.check_output(
            ["/usr/bin/systemctl", "show", "-p", name, "--value", UNIT], text=True, timeout=5,
        ).strip()

    path = Path("/etc/systemd/system") / UNIT
    _protected_file(path, 0o022)
    _require(property_value("FragmentPath") == str(path) and not property_value("DropInPaths")
             and property_value("What") == SOURCE, "share SMB inesperado")
    options, = (line[8:] for line in path.read_text().splitlines() if line.startswith("Options="))
    entries = dict(item.partition("=")[::2] for item in options.split(","))
    _require({"ro", "seal"} <= entries.keys() and entries.get("vers") == "3.1.1", "política SMB inesperada")
    credential = Path(entries.get("credentials", ""))
    _require(credential.is_absolute(), "credencial SMB no configurada")
    _protected_file(credential, 0o077)
    try:
        fields = dict(line.strip().split("=", 1) for line in credential.read_text().splitlines()
                      if line.strip() and not line.lstrip().startswith("#"))
        username, password = fields["username"], fields["password"]
        _require(username and password, "credencial SMB incompleta")
        if fields.get("domain"):
            username = fields["domain"] + "\\" + username
        return username, password
    except (ValueError, KeyError, UnicodeError):
        raise ValueError("credencial SMB inválida") from None


class DirectSMBReader:
    def __init__(self):
        try:
            import smbclient

            username, password = _config()
            smbclient.ClientConfig(skip_dfs=True, require_secure_negotiate=True)
            session = smbclient.register_session(HOST, username=username, password=password,
                                                 encrypt=True, connection_timeout=8)
            _require(session.connection.dialect == 0x311 and session.encrypt_data,
                     "sesión SMB no cifrada o de dialecto incorrecto")
            self.client = smbclient
        except Exception:
            # Never forward library diagnostics that may include credentials.
            raise ValueError("No se pudo abrir sesión SMB 3.1.1 cifrada con la credencial existente") from None

    def _read(self, name, size):
        _require(FILE.fullmatch(name), "ruta SMB no permitida")
        try:
            from smbprotocol.open import CreateOptions, FileAttributes

            with self.client.open_file(SHARE + "\\" + name, mode="rb", buffering=0,
                                       create_options=CreateOptions.FILE_OPEN_REPARSE_POINT) as stream:
                _require(not stream.fd.file_attributes & (
                    FileAttributes.FILE_ATTRIBUTE_REPARSE_POINT | FileAttributes.FILE_ATTRIBUTE_DIRECTORY
                ), "archivo SMB no regular")
                chunks = bytearray()
                while len(chunks) < size:
                    data = stream.read(size - len(chunks))
                    if not data:
                        break
                    chunks.extend(data)
                return bytes(chunks)
        except Exception:
            raise ValueError("Archivo exacto inaccesible por SMB directo") from None

    def digest_manifest(self, name):
        contents = self._read(name, 8193)
        _require(0 < len(contents) <= 8192, "tamaño de manifiesto inesperado")
        return hashlib.sha256(contents).hexdigest()

    def readable(self, name):
        return bool(self._read(name, 1))
