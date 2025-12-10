class VelaError(Exception):
    """Base exception for simplyblock Vela domain-specific errors."""


class VelaDeploymentError(VelaError):
    """Deployment-side errors"""


class VelaDeployError(VelaDeploymentError, ExceptionGroup):
    """Errors during vela deployment"""


class VelaKubernetesError(VelaDeploymentError):
    """Error interacting with Kubernetes"""


class VelaCloudflareError(VelaDeploymentError):
    """Error interacting with Cloudflare"""


class VelaGrafanaError(VelaDeploymentError):
    """Error interacting with Grafana"""


class VelaResourceLimitError(VelaError):
    """Error when resource limits are exceeded or illegal"""
