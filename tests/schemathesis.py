#!/usr/bin/env python3

import asyncio
import os
import unittest.mock
from uuid import uuid4

import jwt
import uvicorn
from testcontainers.postgres import PostgresContainer


def serve_app(port, postgres_url, jwt_secret):
    """Serve the FastAPI app asynchronously"""


async def run_schemathesis_tests(base_url, jwt_secret):
    """Run schemathesis tests asynchronously"""
    token = jwt.encode({'sub': str(uuid4())}, jwt_secret, algorithm='HS256')
    process = await asyncio.create_subprocess_exec(
        'schemathesis', 'run', f'{base_url}/openapi.json',
        '--checks', 'all',
        '--max-examples', '10',
        '--header', f'Authorization: Bearer {token}',
        '--wait-for-schema', str(10),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, stderr = await process.communicate()

    print("Schemathesis output:")
    print(stdout.decode())
    if stderr:
        print("Schemathesis errors:")
        print(stderr.decode())

    return process.returncode == 0


async def main():
    jwt_secret = 'secret'
    port = 5000

    with (
            PostgresContainer('postgres:latest', driver='asyncpg') as postgres,
            unittest.mock.patch('kubernetes.config.load_kube_config') as mock_load_config,
            unittest.mock.patch('simplyblock.vela.deployment.create_vela_config'),
            unittest.mock.patch('simplyblock.vela.deployment.get_deployment_status'),
            unittest.mock.patch('simplyblock.vela.deployment.delete_deployment'),
    ):
        mock_load_config.return_value = None

        os.environ['VELA_POSTGRES_URL'] = postgres.get_connection_url()
        os.environ['VELA_JWT_SECRET'] = jwt_secret

        from simplyblock.vela.api import app

        config = uvicorn.Config(app, port=port, log_level="info")
        server = uvicorn.Server(config)
        asyncio.create_task(server.serve())

        await run_schemathesis_tests(f'http://localhost:{port}', jwt_secret)
        await server.shutdown()


if __name__ == '__main__':
    asyncio.run(main())
