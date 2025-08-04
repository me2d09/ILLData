class IllDataError(Exception):
    """Base exception for the :pymod:`illdata` package."""


class NotConnectedError(IllDataError):
    """Action requires an active SFTP connection."""


class NoProposalSelectedError(IllDataError):
    """Action requires selecting a proposal first (call :py:meth:`IllSftp.open_proposal`)."""
