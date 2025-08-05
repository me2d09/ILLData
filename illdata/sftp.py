from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Generator, Optional

import paramiko

from .exceptions import IllDataError, NoProposalSelectedError, NotConnectedError

log = logging.getLogger(__name__)


def _join_posix(*parts: str) -> str:
    """Safely join POSIX paths (Paramiko SFTP paths are always POSIX)."""
    p = PurePosixPath(parts[0])
    for part in parts[1:]:
        p = p / part
    return str(p)


@dataclass
class IllSftp:
    """
    Thin SFTP client for the Institut Laue–Langevin (“ILL”) data service,
    implemented with **Paramiko**.

    Example
    -------
    >>> from illdata import IllSftp
    >>> with IllSftp("host", "user", "pass") as s:
    ...     for p in s.proposals():
    ...         print(p)
    ...     s.open_proposal("12345")
    ...     s.download("remote/file.dat", "local/file.dat")
    """

    hostname: str
    username: str
    password: str
    port: int = 22
    known_hosts_path: Optional[str] = None  # optional file for strict host-key checking

    _transport: Optional[paramiko.Transport] = field(default=None, init=False, repr=False)
    _sftp: Optional[paramiko.SFTPClient] = field(default=None, init=False, repr=False)
    _home: str = field(default="", init=False, repr=False)
    _proposal: str = field(default="", init=False, repr=False)
    _propdir: str = field(default="", init=False, repr=False)

    # ------------------------------------------------------------------ context
    def __enter__(self) -> "IllSftp":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.disconnect()
        return None  # do not suppress exceptions

    # ------------------------------------------------------------------ helpers
    @property
    def connected(self) -> bool:
        """Whether an SFTP session is active."""
        return self._sftp is not None

    @property
    def proposal(self) -> str:
        """Currently opened proposal ID (or empty string)."""
        return self._proposal

    # ------------------------------------------------------------------ connect
    def connect(self) -> None:
        """Establish an SSH connection + SFTP session and resolve the *MyData* link."""
        if self.connected:
            return

        try:
            # --- SSH client --------------------------------------------------
            client = paramiko.SSHClient()

            if self.known_hosts_path:
                client.load_host_keys(self.known_hosts_path)
                client.set_missing_host_key_policy(paramiko.RejectPolicy())  # strict check
            else:
                # SECURITY: auto-add unknown host keys (less secure, but compatible)
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            client.connect(
                hostname=self.hostname,
                port=self.port,
                username=self.username,
                password=self.password,
                look_for_keys=False,
                allow_agent=False,
            )

            # keep references so we can close them later
            self._transport = client.get_transport()
            self._sftp = client.open_sftp()

            # resolve MyData symlink
            self._home = self._sftp.readlink("MyData")
            log.info("Connected to %s as %s, home=%s", self.hostname, self.username, self._home)

        except Exception as err:                       # noqa: BLE001
            self.disconnect()
            raise IllDataError(f"Cannot connect to SFTP: {err}") from err    

    def disconnect(self) -> None:
        """Close SFTP session and SSH transport."""
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._transport:
            self._transport.close()
            self._transport = None
        if self.connected is False:
            log.info("Disconnected from %s", self.hostname)

    # ------------------------------------------------------------------ guards
    def _require_sftp(self) -> paramiko.SFTPClient:
        if not self._sftp:
            raise NotConnectedError("Call connect() first or use a with-block.")
        return self._sftp

    def _require_proposal(self) -> None:
        if not self._proposal:
            raise NoProposalSelectedError("Open a proposal first: open_proposal('12345').")

    # ------------------------------------------------------------------ API
    def proposals(self) -> Generator[str, None, None]:
        """Yield IDs of all available proposals (strip ``exp_`` prefix)."""
        sftp = self._require_sftp()
        for name in sftp.listdir(_join_posix(self._home, "byProposal")):
            yield name[4:] if name.startswith("exp_") else name

    def open_proposal(self, value: str) -> None:
        """Select a proposal (e.g. ``'12345'``)."""
        sftp = self._require_sftp()
        self._proposal = value
        self._propdir = sftp.readlink(_join_posix(self._home, "byProposal", "exp_" + value))

    # ------------- list --------------------------------------------------
    def listdir(self, remote_path: str = ".", with_attr: bool = False):
        """
        List files within the current proposal.

        Parameters
        ----------
        remote_path : str
            Path relative to the proposal root (default ``"."``).
        with_attr : bool
            If ``True`` return :class:`paramiko.SFTPAttributes` objects.
        """
        self._require_proposal()
        sftp = self._require_sftp()
        full = _join_posix(self._propdir, remote_path)
        return sftp.listdir_attr(full) if with_attr else sftp.listdir(full)

    def listdir_attr(self, remote_path: str = "."):
        """Alias for :py:meth:`listdir(..., with_attr=True)`."""
        return self.listdir(remote_path, with_attr=True)

    # ------------- file transfer -----------------------------------------
    def download(self, remote_path: str, local_path: str) -> None:
        """Download `remote_path` (inside the proposal) to `local_path`."""
        self._require_proposal()
        sftp = self._require_sftp()

        os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
        sftp.get(_join_posix(self._propdir, remote_path), local_path)
        log.info("Downloaded %s → %s", remote_path, local_path)

