from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Sequence

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from ...exceptions import VelaDeploymentError


@dataclass(frozen=True)
class TLSArtifacts:
    server_cert: str
    server_key: str
    root_ca_cert: str


def _normalize_pem(value: str | bytes) -> str:
    if isinstance(value, bytes):
        text = value.decode("utf-8")
    else:
        text = value
    return text.strip() + "\n"


def _ensure_rsa_private_key(key: object) -> rsa.RSAPrivateKey:
    if isinstance(key, rsa.RSAPrivateKey):
        return key
    raise VelaDeploymentError("Provided private key is not RSA")


def _prepare_alt_names(alt_names: Iterable[str]) -> list[x509.GeneralName]:
    prepared: list[x509.GeneralName] = []
    seen: set[str] = set()

    for raw in alt_names:
        if raw is None:
            continue
        name = str(raw).strip()
        if not name or name in seen:
            continue
        seen.add(name)
        try:
            ip_value = ipaddress.ip_address(name)
        except ValueError:
            prepared.append(x509.DNSName(name))
        else:
            prepared.append(x509.IPAddress(ip_value))
    return prepared


def _load_or_create_ca(
    *,
    ca_common_name: str,
    existing_ca_cert_pem: str | None,
    existing_ca_key_pem: str | None,
    validity_days: int,
    now: datetime,
) -> tuple[x509.Certificate, rsa.RSAPrivateKey, str]:
    if existing_ca_cert_pem or existing_ca_key_pem:
        if not existing_ca_cert_pem or not existing_ca_key_pem:
            raise VelaDeploymentError("Both CA certificate and key must be provided together.")
        try:
            ca_certificate = x509.load_pem_x509_certificate(existing_ca_cert_pem.encode("utf-8"))
        except ValueError as exc:
            raise VelaDeploymentError("Invalid CA certificate PEM payload") from exc
        try:
            ca_private_key = serialization.load_pem_private_key(existing_ca_key_pem.encode("utf-8"), password=None)
        except ValueError as exc:
            raise VelaDeploymentError("Invalid CA key PEM payload") from exc
        rsa_private_key = _ensure_rsa_private_key(ca_private_key)
        return ca_certificate, rsa_private_key, _normalize_pem(existing_ca_cert_pem)

    ca_private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    ca_subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, ca_common_name)])

    ca_builder = (
        x509.CertificateBuilder()
        .subject_name(ca_subject)
        .issuer_name(ca_subject)
        .public_key(ca_private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=validity_days))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
    )
    ca_certificate = ca_builder.sign(private_key=ca_private_key, algorithm=hashes.SHA256())
    ca_cert_pem = _normalize_pem(ca_certificate.public_bytes(serialization.Encoding.PEM))
    return ca_certificate, ca_private_key, ca_cert_pem


def generate_tls_artifacts(
    *,
    server_common_name: str,
    alt_names: Sequence[str] | Iterable[str],
    ca_common_name: str,
    existing_ca_cert_pem: str | None = None,
    existing_ca_key_pem: str | None = None,
    ca_validity_days: int = 365,
    server_validity_days: int = 365,
) -> TLSArtifacts:
    now = datetime.utcnow()
    ca_certificate, ca_private_key, ca_cert_pem = _load_or_create_ca(
        ca_common_name=ca_common_name,
        existing_ca_cert_pem=existing_ca_cert_pem,
        existing_ca_key_pem=existing_ca_key_pem,
        validity_days=ca_validity_days,
        now=now,
    )

    server_private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server_subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, server_common_name)])
    san_entries = _prepare_alt_names(alt_names)

    server_builder = (
        x509.CertificateBuilder()
        .subject_name(server_subject)
        .issuer_name(ca_certificate.subject)
        .public_key(server_private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=server_validity_days))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
    )

    if san_entries:
        server_builder = server_builder.add_extension(x509.SubjectAlternativeName(san_entries), critical=False)

    server_certificate = server_builder.sign(private_key=ca_private_key, algorithm=hashes.SHA256())
    server_key_pem = _normalize_pem(
        server_private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    server_cert_pem = _normalize_pem(server_certificate.public_bytes(serialization.Encoding.PEM))

    return TLSArtifacts(
        server_cert=server_cert_pem,
        server_key=server_key_pem,
        root_ca_cert=ca_cert_pem,
    )


__all__ = ["TLSArtifacts", "generate_tls_artifacts"]
