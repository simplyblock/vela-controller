from jwcrypto import jwk
import json

# Load public key
with open("public.key", "rb") as f:
    key = jwk.JWK.from_pem(f.read())
    key_dict = json.loads(key.export_public())
    key_dict["kid"] = "my-test-key"  # Must match JWT header

# Save JWKS
jwks = {"keys": [key_dict]}
with open("jwks.json", "w") as f:
    json.dump(jwks, f, indent=2)

print("jwks.json generated")
