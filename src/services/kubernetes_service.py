from kubernetes import client, config
from kubernetes.client.rest import ApiException
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KubernetesService:
    def __init__(self):
        try:
            # Try to load in-cluster config first (for running in a pod)
            config.load_incluster_config()
        except config.config_exception.ConfigException:
            try:
                # Fall back to kubeconfig file (for local development)
                config.load_kube_config()
            except config.config_exception.ConfigException as e:
                logger.error("Could not configure kubernetes python client")
                raise
        
        self.core_v1 = client.CoreV1Api()

    def check_namespace_status(self, namespace):
        """
        Check if all pods in the namespace are running
        Returns:
            dict: {
                'all_running': bool,
                'status': str,  # 'running', 'pending', 'failed', or 'unknown'
                'pods': list[dict]  # List of pod statuses
            }
        """
        try:
            pods = self.core_v1.list_namespaced_pod(namespace)
            if not pods.items:
                return {
                    'all_running': False,
                    'status': 'pending',
                    'pods': [],
                    'message': 'No pods found in namespace'
                }
            
            all_running = True
            pod_statuses = []
            
            for pod in pods.items:
                pod_status = {
                    'name': pod.metadata.name,
                    'status': 'unknown',
                    'conditions': []
                }
                
                # Check pod phase
                if pod.status.phase == 'Running':
                    # Check all containers are ready
                    container_statuses = pod.status.container_statuses or []
                    ready = all(container.ready for container in container_statuses)
                    pod_status['status'] = 'running' if ready else 'pending'
                    pod_status['containers'] = [{
                        'name': c.name,
                        'ready': c.ready,
                        'restart_count': c.restart_count,
                        'state': c.state.to_dict() if hasattr(c, 'state') else {}
                    } for c in container_statuses]
                else:
                    pod_status['status'] = pod.status.phase.lower()
                
                if pod_status['status'] != 'running':
                    all_running = False
                
                pod_statuses.append(pod_status)
            
            # Determine overall status
            if all_running:
                status = 'running'
            elif any(p['status'] == 'failed' for p in pod_statuses):
                status = 'failed'
            elif any(p['status'] == 'pending' for p in pod_statuses):
                status = 'pending'
            else:
                status = 'unknown'
            
            return {
                'all_running': all_running,
                'status': status,
                'pods': pod_statuses,
                'message': f'Namespace {namespace} status: {status}'
            }
            
        except ApiException as e:
            logger.error(f"Error checking namespace status: {e}")
            return {
                'all_running': False,
                'status': 'error',
                'pods': [],
                'message': f'Error checking namespace status: {str(e)}'
            }
