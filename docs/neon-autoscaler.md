## Neon Autoscaler

### Setup

Autoscaler expects cert-manager to be present in the cluster. Setup instructions are available [here](./manual-deployment.md#cert-manager).


```
kubectl apply -f https://raw.githubusercontent.com/k8snetworkplumbingwg/multus-cni/master/deployments/multus-daemonset-thick.yml
kubectl apply -f https://github.com/neondatabase/autoscaling/releases/latest/download/whereabouts.yaml
```


Install the Neon autoscaler controllers in the `neonvm-system` namespace.

```sh
kubectl create ns neonvm-system
kubectl label namespace neonvm-system  \
  pod-security.kubernetes.io/enforce=privileged --overwrite \
  pod-security.kubernetes.io/audit=privileged --overwrite \
  pod-security.kubernetes.io/warn=privileged --overwrite
```


```sh
kubectl apply -f https://github.com/neondatabase/autoscaling/releases/latest/download/neonvm.yaml
kubectl apply -f https://github.com/neondatabase/autoscaling/releases/latest/download/neonvm-vxlan-controller.yaml
kubectl apply -f https://github.com/neondatabase/autoscaling/releases/latest/download/neonvm-controller.yaml
```

### Usage

Create a sample VM:
```sh
cat <<EOF | kubectl apply -f -
apiVersion: vm.neon.tech/v1
kind: VirtualMachine
metadata:
  name: vm-manohar-dev
spec:
  powerState: Running
  extraNetwork:
    enable: true
  guest:
    rootDisk:
      image: docker.io/manoharbrm/pg16-test:dev9 # TODO: update image
    cpus: { min: 1, use: 1, max: 64 }
    memorySlots: { min: 1, use: 2, max: 256 }
  disks:
    - name: data
      mountPath: /var/lib/postgresql/data
      blockDevice:
        persistentVolumeClaim:
          storageClassName: simplyblock-csi-sc
          accessModes:
            - ReadWriteMany
          resources:
            requests:
              storage: 20Gi
EOF
```

Get the IP address, connect to the VM, and run a pgbench test:

```bash
kubectl run pgbench --rm -it \
  --image=postgres:17 \
  --overrides='
{
  "apiVersion": "v1",
  "metadata": {
    "annotations": {
      "k8s.v1.cni.cncf.io/networks": "neonvm-system/neonvm-overlay-for-pods"
    }
  }
}
' \
  --env PGPASSWORD=manohar_pd -- \
  bash -c "
    pgbench -h 10.100.128.0 -p 5432 -U postgres -i -s 10 postgres &&
    pgbench -h 10.100.128.0 -p 5432 -U postgres -c 4 -j 2 -T 60 -P postgres
  "
```
