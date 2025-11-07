#!/usr/bin/env python3

import os
import sys
from subprocess import run

from testcontainers.postgres import PostgresContainer


def _run_alembic(args: list[str], postgres, **kwargs):
    driver = "psycopg"
    return run(
        ["alembic"] + args,
        env={
            **os.environ,
            "VELA_POSTGRES_URL": postgres.get_connection_url(driver=driver),
        },
        **kwargs,
    )


def main():
    # Work around tox passing `posargs` as single argument
    args = sys.argv[1:]
    if len(args) == 1:
        args = args[0].split(" ")

    with PostgresContainer("postgres:17") as postgres:
        _run_alembic(["upgrade", "head"], postgres, check=True)
        return _run_alembic(args, postgres).returncode


if __name__ == "__main__":
    sys.exit(main())
