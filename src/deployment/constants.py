grafana_env_patch = [
    {
        "name": "GF_AUTH_JWT_ENABLED",
        "value": "true"
    },
    {
        "name": "GF_AUTH_JWT_HEADER_NAME",
        "value": "X-JWT-Assertion"
    },
    {
        "name": "GF_AUTH_JWT_USERNAME_CLAIM",
        "value": "sub"
    },
    {
        "name": "GF_AUTH_JWT_EMAIL_CLAIM",
        "value": "email"
    }
]

grafana_patch = {
    "spec": {
        "template": {
            "spec": {
                "containers": [
                    {
                        "name": "grafana",
                        "env": grafana_env_patch
                    }
                ]
            }
        }
    }
}
