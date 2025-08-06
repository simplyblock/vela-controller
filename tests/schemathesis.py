#!/usr/bin/env python3

import logging
import subprocess
import sys
import time
from contextlib import contextmanager
from functools import wraps

import httpx


class RetriesExceededError(Exception):
    pass


def retry(
        func,
        predicate=None,
        retries=0, delay=0.,
        exceptions=(), expected_exceptions=(),
        linear_backoff=0., multiplicative_backoff=1., exponential_backoff=1.,
):
    @wraps(func)
    def wrapper(*args, **kwargs):
        d = delay
        for i in range(retries + 1):
            try:
                result = func(*args, **kwargs)
                if expected_exceptions != ():
                    pass  # We are waiting for a specific exception
                elif predicate is not None and not predicate(result):
                    logging.debug(f'{func.__name__}() returned bad value')
                else:
                    return result
            except expected_exceptions:
                return
            except exceptions:
                logging.debug(f'{func.__name__}() failed, retrying (attempt {i})...', exc_info=True)


            logging.info(f'Retrying {func.__name__}() (attempt {i})...')
            time.sleep(d)
            d = linear_backoff + multiplicative_backoff * delay ** exponential_backoff

        raise RetriesExceededError(f'Retrying {func.__name__}() failed ({retries + 1} attempts)')
    return wrapper


@contextmanager
def podman_compose(file):
    compose_cmd = ['podman-compose', f'--file={file}']
    subprocess.check_output(compose_cmd + ['up', '--detach'])
    try:
        yield
    finally:
        subprocess.check_output(compose_cmd + ['down'])


if __name__ == '__main__':
    url = 'http://localhost:8000/openapi.json'
    with podman_compose('containers/compose.yml'):
        print('Awaiting API...')
        retry(httpx.head, retries=30, delay=1, exceptions=(httpx.ReadError,))(url)
        result = subprocess.run(['schemathesis', 'run', url])
    sys.exit(result.returncode)
