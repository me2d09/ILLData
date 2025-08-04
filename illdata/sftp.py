from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Generator, Optional

import pysftp

from .exceptions import IllDataError, NoProposalSelectedError, NotConnectedError

log = logging.getLogger(__name__)


def _join_posix(*parts: str) -> str:
    """Safely join POSIX‑style paths (SFTP paths are always POSIX)."""
    p = PurePosixPath(parts[0])
    for part in parts[1:]:
        p = p / part
    return str(p)


@dataclass
class IllSftp:
    """
    Thin SFTP client for the Institut Laue–Langevin (“ILL”) data service.

    Example
    -------
    ```python
    >>> with IllSftp("host", "user", "pass") as s:
    ...     for p in s.proposals():
    ...         print(p)
    ...     s.open_proposal("12345")
    ...     s.listdir(".")
    ...     s.download("remote/file.dat", "local/file.dat")
    ```

    Notes
    -----
    Host‑key verification is disabled by default for compatibility.  
    For production environments supply *known_hosts* via :pyattr:`known_hosts_path`.
    """

    hostname: str
    username: str
    password: str
    port: int = 22
    known_hosts_path: Optional[str] = None

    _connection: Optional[pysftp.Connection] = field(default=None, init=False, repr=False)
    _home: str = field(default="", init=False, repr=False)
    _proposal: str = field(default="", init=False, repr=False)
    _propdir: str = field(default="", init=False, repr=False)

    # ---------------------------------------------------------------------
    # Context‑manager helpers
    # ---------------------------------------------------------------------
    def __enter__(self) -> "IllSftp":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self.disconnect()
        except Exception:  # noqa: BLE001
            log.exception("Error while disconnecting")
        # do *not* swallow exceptions from the with‑block
        return None

    # ---------------------------------------------------------------------
    # Properties
    # ---------------------------------------------------------------------
    @property
    def connected(self) -> bool:
        """Return ``True`` if an SFTP connection is active."""
        return self._connection is not None

    @property
    def proposal(self) -> str:
        """Return the currently opened proposal ID, or an empty string."""
        return self._proposal

    # ---------------------------------------------------------------------
    # Connection logic
    # ---------------------------------------------------------------------
    def connect(self) -> None:
        """Open an SFTP connection and resolve the *MyData* symlink."""
        if self._connection is not None:
            return

        try:
            cnopts = pysftp.CnOpts()
            if self.known_hosts_path:
                cnopts.hostkeys.load(self.known_hosts_path)
            else:
                # SECURITY: disable host‑key verification (less secure, but compatible)
                cnopts.hostkeys = None  # type: ignore[assignment]

            self._connection = pysftp.Connection(
                host=self.hostname,
                username=self.username,
                password=self.password,
                port=self.port,
                cnopts=cnopts,
            )
            self._home = self._connection.readlink("MyData")
            log.info("Connected to %s as %s, home=%s", self.hostname, self.username, self._home)
        except Exception as err:  # noqa: BLE001
            self._connection = None
            raise IllDataError(f"Cannot connect to SFTP: {err}") from err

    def disconnect(self) -> None:
        """Close the SFTP connection."""
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                log.info("Disconnected from %s", self.hostname)
                self._connection = None

    # ---------------------------------------------------------------------
    # Internal guards
    # ---------------------------------------------------------------------
    def _require_connection(self) -> pysftp.Connection:
        """Return the current connection or raise :class:`NotConnectedError`."""
        if self._connection is None:
            raise NotConnectedError("Call connect() first or use a with‑block.")
        return self._connection

    def _require_proposal(self) -> None:
        """Ensure that a proposal is opened."""
        if not self._proposal:
            raise NoProposalSelectedError("Open a proposal first: open_proposal('12345').")

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------
    def proposals(self) -> Generator[str, None, None]:
        """Yield IDs of all available proposals (stripping the ``exp_`` prefix)."""
        c = self._require_connection()
        remote_path = _join_posix(self._home, "byProposal")
        for obj in c.listdir(remote_path):
            yield obj[4:] if obj.startswith("exp_") else obj

    def open_proposal(self, value: str) -> None:
        """Select an active *proposal* (e.g. ``'12345'``)."""
        c = self._require_connection()
        self._proposal = value
        c.chdir(_join_posix(self._home, "byProposal"))
        self._propdir = c.readlink("exp_" + value)

    def listdir(self, remote_path: str = ".", with_attr: bool = False):
        """
        List files under the current proposal directory.

        Parameters
        ----------
        remote_path:
            Path relative to the proposal root. Defaults to ``"."``.
        with_attr:
            If ``True`` return :class:`pysftp.SFTPAttributes` objects instead of plain names.
        """
        self._require_connection()
        self._require_proposal()
        c = self._connection  # type: ignore[assignment]
        if with_attr:
            return c.listdir_attr(_join_posix(self._propdir, remote_path))
        return c.listdir(_join_posix(self._propdir, remote_path))

    def listdir_attr(self, remote_path: str = "."):
        """Alias for :py:meth:`listdir` with ``with_attr=True``."""
        return self.listdir(remote_path, with_attr=True)

    def download(self, remote_path: str, local_path: str) -> None:
        """
        Download a file from the current proposal directory.

        Parameters
        ----------
        remote_path:
            Path on the server relative to the proposal root.
        local_path:
            Destination path on the local filesystem.
        """
        self._require_connection()
        self._require_proposal()
        c = self._connection  # type: ignore[assignment]

        # Create the destination folder if it doesn't exist.
        dest_dir = os.path.dirname(os.path.abspath(local_path))
        if dest_dir and not os.path.isdir(dest_dir):
            os.makedirs(dest_dir, exist_ok=True)

        c.get(_join_posix(self._propdir, remote_path), local_path)
        log.info("Downloaded %s -> %s", remote_path, local_path)
