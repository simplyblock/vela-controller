import base64

from Crypto.Cipher import AES
from Crypto.Hash import MD5
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad, unpad

_SALTED_PREFIX = b"Salted__"


def _evp_bytes_to_key(passphrase: str, salt: bytes) -> tuple[bytes, bytes]:
    """Derive an AES key and IV from a passphrase following OpenSSL's EVP_BytesToKey."""
    derived = b""
    block = b""
    key_len = 32
    iv_len = 16

    while len(derived) < key_len + iv_len:
        block = MD5.new(block + passphrase.encode("utf-8") + salt).digest()
        derived += block

    return derived[:key_len], derived[key_len : key_len + iv_len]


def encrypt_with_passphrase(plaintext: str, passphrase: str) -> str:
    """Encrypt `plaintext` with the supplied passphrase using AES-256-CBC."""
    salt = get_random_bytes(8)
    key, iv = _evp_bytes_to_key(passphrase, salt)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    payload = cipher.encrypt(pad(plaintext.encode("utf-8"), AES.block_size))
    return base64.b64encode(_SALTED_PREFIX + salt + payload).decode("utf-8")


def decrypt_with_passphrase(ciphertext: str, passphrase: str) -> str:
    """Decrypt a ciphertext produced by `encrypt_with_passphrase`."""
    payload = base64.b64decode(ciphertext)
    if not payload.startswith(_SALTED_PREFIX):
        raise ValueError("Invalid ciphertext header.")

    salt = payload[len(_SALTED_PREFIX) : len(_SALTED_PREFIX) + 8]
    encrypted = payload[len(_SALTED_PREFIX) + 8 :]
    key, iv = _evp_bytes_to_key(passphrase, salt)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plaintext = unpad(cipher.decrypt(encrypted), AES.block_size)
    return plaintext.decode("utf-8")


def generate_random_passphrase(bits: int = 64) -> str:
    """Return a base64-encoded random passphrase sized to `bits` (default 64-bit)."""
    if bits % 8 != 0:
        raise ValueError("bits must be a multiple of 8")
    return base64.b64encode(get_random_bytes(bits // 8)).decode("ascii")


def encrypt_with_random_passphrase(plaintext: str, *, bits: int = 64) -> tuple[str, str]:
    """Encrypt `plaintext` and return `(ciphertext, passphrase)` using a random passphrase."""
    passphrase = generate_random_passphrase(bits)
    return encrypt_with_passphrase(plaintext, passphrase), passphrase


def decrypt_with_base64_key(ciphertext: str, key: str) -> str:
    """Decrypt legacy ciphertext that stores a base64-encoded AES key separately."""
    key_bytes = base64.b64decode(key)
    payload = base64.b64decode(ciphertext)
    iv, encrypted = payload[: AES.block_size], payload[AES.block_size :]
    cipher = AES.new(key_bytes, AES.MODE_CBC, iv)
    plaintext = unpad(cipher.decrypt(encrypted), AES.block_size)
    return plaintext.decode("utf-8")
