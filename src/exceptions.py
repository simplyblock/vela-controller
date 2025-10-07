class VelaError(Exception):
    """Base exception for simplyblock Vela domain-specific errors."""


class VelaDeploymentError(VelaError):
    """Deployment-side errors"""


class VelaKubernetesError(VelaDeploymentError):
    """Error interacting with Kubernetes"""


class VelaCloudflareError(VelaDeploymentError):
    """Error interacting with Cloudflare"""
