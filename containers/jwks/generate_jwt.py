import jwt
import datetime

# Load the private key
with open("private.key", "r") as f:
    private_key = f.read()

# JWT payload (customize as needed)
payload = {
    "sub": "testuser",                        # This becomes the Grafana username
    "email": "testuser@example.com",         # This becomes the Grafana email
    "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=10)
}

headers = {
    "kid": "my-test-key",                    # Must match kid in JWKS
    "alg": "RS256"
}

# Encode the token
token = jwt.encode(payload, private_key, algorithm="RS256", headers=headers)

print("Your JWT token:\n")
print(token)
