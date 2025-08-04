# illdata

Lightweight Python client for working with ILL SFTP (Institut Laue–Langevin). It lets you:

- connect to SFTP,
- list available proposals,
- open a specific proposal,
- list files,
- download files.

> This package also offers logging, and a small CLI.

## Installation (local)

```bash
pip install git+https://github.com/me2d09/ILLData.git

````

## Requirements

* Python >= 3.8
* `pysftp` (installed automatically)

## Quick start (Python)

```python
from illdata import IllSftp

with IllSftp(hostname="host", username="user", password="pass") as ill:
    # list availavble proposals
    for p in ill.proposals():
        print(p)

    ill.open_proposal("12345")
    print(ill.listdir("."))
    ill.download("path/remote/file.dat", "downloads/file.dat")
```

Try it interactively by opening the example notebook: [example.ipynb](./example.ipynb).

## CLI

Install the package, then run:

```bash
illdata proposals --host HOST --user USER --password PASS
illdata open --proposal 12345 --host HOST --user USER --password PASS
illdata ls --proposal 12345 --path . --host HOST --user USER --password PASS
illdata get --proposal 12345 --remote path/on/server.dat --local ./data/file.dat --host HOST --user USER --password PASS
```

You can also provide credentials via environment variables:

* `ILL_HOST`, `ILL_USER`, `ILL_PASS`, (optional) `ILL_PORT`, `ILL_KNOWN_HOSTS`

## Logging

The library uses Python’s standard `logging`. Configure it in your app if you want to see info messages:

```python
import logging
logging.basicConfig(level=logging.INFO)
```

## Security notes

By default, host key verification is **disabled** (for compatibility with older SFTP setups).
For secure use, pass a `known_hosts` file to the CLI (`--known-hosts`) or set `known_hosts_path` in code.

## License

MIT

