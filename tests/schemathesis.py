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
    token = jwt.encode(
        {
            "sub": str(uuid4()),
            "aal": "aal2",  # TODO: Dynamically test, see require_mfa
        },
        jwt_secret,
        algorithm="HS256",
    )
    process = await asyncio.create_subprocess_exec(
        "schemathesis",
        "run",
        f"{base_url}/openapi.json",
        "--checks=all",
        "--max-examples=10",
        f"--header=Authorization: Bearer {token}",
        "--wait-for-schema=10",
        "--suppress-health-check=filter_too_much",
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
    jwt_secret = "secret"
    port = 5000

    with (
        PostgresContainer("postgres:latest", driver="asyncpg") as postgres,
        unittest.mock.patch(
            "kubernetes_asyncio.config.load_incluster_config",
            new_callable=unittest.mock.AsyncMock,
        ) as mock_load_incluster,
        unittest.mock.patch(
            "kubernetes_asyncio.config.load_kube_config",
            new_callable=unittest.mock.AsyncMock,
        ) as mock_load_config,
        unittest.mock.patch("simplyblock.vela.deployment.create_vela_config"),
        unittest.mock.patch(
            "simplyblock.vela.deployment.get_deployment_status",
            new_callable=unittest.mock.AsyncMock,
        ) as mock_status,
        unittest.mock.patch(
            "simplyblock.vela.deployment.delete_deployment",
            new_callable=unittest.mock.AsyncMock,
        ),
        unittest.mock.patch("keycloak.KeycloakAdmin") as mock_keycloak_admin,
    ):
        mock_load_incluster.return_value = None
        mock_load_config.return_value = None

        # Mock Keycloak admin methods
        mock_keycloak_instance = mock_keycloak_admin.return_value
        mock_keycloak_instance.a_create_user = unittest.mock.AsyncMock(return_value=str(uuid4()))
        mock_keycloak_instance.a_get_user = unittest.mock.AsyncMock(
            return_value={
                "id": str(uuid4()),
                "email": "testuser@example.com",
                "firstName": "Test",
                "lastName": "User",
                "emailVerified": True,
            }
        )
        mock_keycloak_instance.a_get_user_id = unittest.mock.AsyncMock(return_value=str(uuid4()))
        mock_keycloak_instance.a_send_verify_email = unittest.mock.AsyncMock(return_value=None)

        os.environ["VELA_POSTGRES_URL"] = postgres.get_connection_url()
        os.environ["VELA_JWT_SECRET"] = jwt_secret
        os.environ["VELA_PGMETA_CRYPTO_KEY"] = "secret"
        os.environ["VELA_KEYCLOAK_URL"] = "http://example.com"
        os.environ["VELA_KEYCLOAK_CLIENT_ID"] = ""
        os.environ["VELA_KEYCLOAK_CLIENT_SECRET"] = ""

        from simplyblock.vela.api import app
        from simplyblock.vela.deployment import DeploymentStatus

        mock_status.return_value = DeploymentStatus(status="Running")

        config = uvicorn.Config(app, port=port, log_level="info")
        server = uvicorn.Server(config)
        asyncio.create_task(server.serve())

        result = await run_schemathesis_tests(f"http://localhost:{port}", jwt_secret)
        await server.shutdown()
        return result


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(main()) else 1)
