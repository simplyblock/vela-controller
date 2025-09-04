#!/usr/bin/env python3

import asyncio
import os
import sys
import unittest.mock
from uuid import uuid4

import jwt
import uvicorn
from testcontainers.postgres import PostgresContainer


def serve_app(port, postgres_url, jwt_secret):
    """Serve the FastAPI app asynchronously"""


async def run_schemathesis_tests(base_url, jwt_secret):
    """Run schemathesis tests asynchronously"""
    token = jwt.encode({
        'sub': str(uuid4()),
        'aal': 'aal2',  # TODO: Dynamically test, see require_mfa
    }, jwt_secret, algorithm='HS256')
    process = await asyncio.create_subprocess_exec(
        'schemathesis', 'run', f'{base_url}/openapi.json',
        '--checks', 'all',
        '--max-examples', '10',
        '--header', f'Authorization: Bearer {token}',
        '--wait-for-schema', str(10),
        '--suppress-health-check', 'filter_too_much',
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
            unittest.mock.patch('simplyblock.vela.deployment.get_deployment_status') as mock_status,
            unittest.mock.patch('simplyblock.vela.deployment.delete_deployment'),
    ):
        mock_load_config.return_value = None

        os.environ['VELA_POSTGRES_URL'] = postgres.get_connection_url()
        os.environ['VELA_JWT_SECRET'] = jwt_secret

        from simplyblock.vela.api import app
        from simplyblock.vela.deployment import DeploymentStatus

        mock_status.return_value = DeploymentStatus(status='ACTIVE_HEALTHY', pods={}, message='')

        config = uvicorn.Config(app, port=port, log_level="info")
        server = uvicorn.Server(config)
        asyncio.create_task(server.serve())

        result = await run_schemathesis_tests(f'http://localhost:{port}', jwt_secret)
        await server.shutdown()
        return result


if __name__ == '__main__':
    sys.exit(0 if asyncio.run(main()) else 1)
