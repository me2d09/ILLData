import argparse
import os
import sys
from typing import Optional

from .sftp import IllSftp


def _env_default(name: str, fallback: Optional[str] = None) -> Optional[str]:
    return os.environ.get(name, fallback)


def _common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--host", "--hostname", dest="host", required=False,
                   default=_env_default("ILL_HOST"), help="SFTP host")
    p.add_argument("--user", "--username", dest="user", required=False,
                   default=_env_default("ILL_USER"), help="SFTP username")
    p.add_argument("--password", dest="password", required=False,
                   default=_env_default("ILL_PASS"), help="SFTP password")
    p.add_argument("--port", type=int, default=int(_env_default("ILL_PORT", "22")),
                   help="SFTP port (default 22)")
    p.add_argument("--known-hosts", dest="known_hosts",
                   default=_env_default("ILL_KNOWN_HOSTS"),
                   help="Path to a known_hosts file")


def _require_conn_args(args) -> None:
    missing = [k for k in ("host", "user", "password") if not getattr(args, k)]
    if missing:
        raise SystemExit(
            f"Missing arguments: {', '.join(missing)}. "
            "They can also be supplied via ILL_HOST / ILL_USER / ILL_PASS."
        )


def cmd_proposals(args) -> int:
    _require_conn_args(args)
    with IllSftp(args.host, args.user, args.password, port=args.port,
                 known_hosts_path=args.known_hosts) as s:
        for p in s.proposals():
            print(p)
    return 0


def cmd_open(args) -> int:
    _require_conn_args(args)
    with IllSftp(args.host, args.user, args.password, port=args.port,
                 known_hosts_path=args.known_hosts) as s:
        s.open_proposal(args.proposal)
        print(s.proposal)
    return 0


def cmd_ls(args) -> int:
    _require_conn_args(args)
    with IllSftp(args.host, args.user, args.password, port=args.port,
                 known_hosts_path=args.known_hosts) as s:
        s.open_proposal(args.proposal)
        for name in s.listdir(args.path or "."):
            print(name)
    return 0


def cmd_get(args) -> int:
    _require_conn_args(args)
    with IllSftp(args.host, args.user, args.password, port=args.port,
                 known_hosts_path=args.known_hosts) as s:
        s.open_proposal(args.proposal)
        s.download(args.remote, args.local)
    return 0


def cmd_put(args) -> int:
    _require_conn_args(args)
    with IllSftp(args.host, args.user, args.password, port=args.port,
                 known_hosts_path=args.known_hosts) as s:
        s.open_proposal(args.proposal)
        s.upload(args.local, args.remote)
    return 0


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="illdata", description="CLI for working with ILL SFTP")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("proposals", help="List available proposals")
    _common_args(p)
    p.set_defaults(func=cmd_proposals)

    p = sub.add_parser("open", help="Select a proposal (for availability check)")
    _common_args(p)
    p.add_argument("--proposal", required=True, help="Proposal ID (e.g. 12345)")
    p.set_defaults(func=cmd_open)

    p = sub.add_parser("ls", help="List files in a proposal")
    _common_args(p)
    p.add_argument("--proposal", required=True, help="Proposal ID")
    p.add_argument("--path", default=".", help="Path within the proposal (default '.')")
    p.set_defaults(func=cmd_ls)

    p = sub.add_parser("get", help="Download a file")
    _common_args(p)
    p.add_argument("--proposal", required=True, help="Proposal ID")
    p.add_argument("--remote", required=True, help="Remote path within the proposal")
    p.add_argument("--local", required=True, help="Destination path on the local machine")
    p.set_defaults(func=cmd_get)

    p = sub.add_parser("put", help="Upload a file")
    _common_args(p)
    p.add_argument("--proposal", required=True, help="Proposal ID")
    p.add_argument("--local", required=True, help="Local file to upload")
    p.add_argument("--remote", required=True, help="Destination path within the proposal")
    p.set_defaults(func=cmd_put)

    return parser


def main(argv=None) -> int:
    parser = make_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
