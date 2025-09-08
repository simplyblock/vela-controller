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
    },
    {
        "name": "GF_AUTH_JWT_AUTO_SIGN_UP",
        "value": "true"
    },
    {
        "name": "GF_USERS_ALLOW_SIGN_UP",
        "value": "false"
    },
    {
        "name": "GF_AUTH_JWT_KEY_FILE",
        "value": "/etc/grafana/jwt-key.pem"
    }
]

grafana_volume_mount_patch = [
    {
        "name": "grafana-jwt-key",
        "mountPath": "/etc/grafana/jwt-key.pem",
        "subPath": "jwt-key.pem",
        "readOnly": True
    }
]

grafana_volume_patch = [
    {
        "name": "grafana-jwt-key",
        "configMap": {
            "name": "grafana-jwt-key",
            "items": [
                {
                    "key": "jwt-key.pem",
                    "path": "jwt-key.pem"
                }
            ]
        }
    }
]

grafana_patch = {
    "spec": {
        "template": {
            "spec": {
                "containers": [
                    {
                        "name": "grafana",
                        "env": grafana_env_patch,
                        "volumeMounts": grafana_volume_mount_patch
                    }
                ],
                "volumes": grafana_volume_patch
            }
        }
    }
}
