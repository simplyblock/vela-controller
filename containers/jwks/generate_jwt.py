import jwt
import datetime

# Load the private key
with open("private.key", "r") as f:
    private_key = f.read()

# JWT payload (customize as needed)
payload = {
    "sub": "testuser",                       
    "email": "testuser@example.com",         
    "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=10)
}

headers = {
    "kid": "my-test-key",                    
    "alg": "RS256"
}

# Encode the token
token = jwt.encode(payload, private_key, algorithm="RS256", headers=headers)

print("Your JWT token:\n")
print(token)
