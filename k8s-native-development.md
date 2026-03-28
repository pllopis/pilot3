# Development Guide: Kubernetes Native Payload Execution

This document describes how to develop and test the k8s-native payload execution feature locally using kind (Kubernetes in Docker).

## Overview

The k8s-native feature allows pilot to run payloads as native Kubernetes containers in the same pod, rather than as subprocesses. This is useful for running in CVMFS-free environments where the pilot and payload share a pod.

## Architecture

The implementation consists of:

- **pilot/util/k8s.py** - Kubernetes utilities for k8s-native execution
- **pilot/control/payloads/kubernetes_executor.py** - KubernetesExecutor class

The pilot pod must have two containers:
1. **pilot container** - runs the pilot3 code
2. **payload container** - initialized with a dummy image, patched at runtime to run the target workload

A shared emptyDir volume connects both containers.

## Prerequisites

- Docker
- kind
- kubectl

Install kind:
```bash
# macOS
brew install kind

# Linux
curl -Lo kind https://kind.sigs.k8s.io/dl/v0.20.0/kind-$(uname)-amd64
chmod +x kind && mv kind /usr/local/bin/
```

## Development Workflow

### 1. Create kind Cluster

```bash
kind create cluster --name pilot-dev
```

### 2. Clone and Setup pilot-wrapper

```bash
git clone https://github.com/PanDAWMS/pilot-wrapper.git
cd pilot-wrapper
```

Copy the development Dockerfile (provided separately) to this directory, or create one:

```dockerfile
# Development Dockerfile - uses local pilot3 source
FROM docker.io/almalinux:9.6

# ... (full Dockerfile from pilot-wrapper with these changes)

# Instead of downloading pilot3 from GitHub:
COPY pilot3 /pilot3

# Install kubernetes Python client
RUN /opt/pilot/bin/pip install --no-cache-dir kubernetes
```

### 3. Build the Image

```bash
# From pilot-wrapper directory
docker build -t pilot3:dev .
```

### 4. Load Image into kind

```bash
kind load docker-image pilot3:dev --name pilot-dev
```

### 5. Create Pod Manifest

Create `pilot-k8s-native.yaml`:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: pilot-k8s-native
  namespace: default
spec:
  # Init container to set up shared volume structure
  initContainers:
    - name: shared-volume-init
      image: busybox:latest
      command: ['sh', '-c', 'mkdir -p /mnt/wrapper && chmod 777 /mnt/wrapper']
      volumeMounts:
        - name: shared-volume
          mountPath: /mnt/wrapper

  containers:
    - name: pilot
      image: pilot3:dev
      env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: K8S_SHARED_VOLUME_PATH
          value: "/mnt/wrapper"
        - name: K8S_WRAPPER_SCRIPT_PATH
          value: "/mnt/wrapper/launch.sh"
        # For testing - tell pilot to use k8s-native
        - name: PILOT_K8S_NATIVE
          value: "true"
        # Standard pilot env vars (adjust for your test)
        - name: PILOT_NOKILL
          value: "True"
        - name: PILOT_USER
          value: "rubin"
      volumeMounts:
        - name: shared-volume
          mountPath: /mnt/wrapper
        - name: proxy-secret
          mountPath: /proxy
          readOnly: true
        - name: pilot-dir
          mountPath: /pilotdir
      command: ["python3", "/pilot3/pilot.py"]
      args:
        - -q
        - CHSRC_TEST
        - --pilot-user
        - rubin
        - --url
        - https://panda-server.dev.skach.org
        - --port
        - "443"
        - -d
        - --localpy
        - -t
        - --noproxyverification
        - --queuedata-url
        - https://panda-server.dev.skach.org/cache/schedconfig/CHSRC_TEST.all.json

    # The payload container - this is what gets patched
    - name: payload
      image: busybox:latest  # Placeholder - will be patched
      command: ["/bin/sh"]
      args: ["/mnt/wrapper/launch.sh"]
      volumeMounts:
        - name: shared-volume
          mountPath: /mnt/wrapper

  volumes:
    - name: shared-volume
      emptyDir:
        sizeLimit: 1Gi
    - name: proxy-secret
      secret:
        secretName: proxy-secret
        defaultMode: 256
    - name: pilot-dir
      emptyDir: {}

  restartPolicy: Never
```

### 6. Apply the Pod

```bash
kubectl apply -f pilot-k8s-native.yaml

# Watch the logs
kubectl logs -f pilot-k8s-native -c pilot

# Check pod status
kubectl get pod pilot-k8s-native

# If you need to debug
kubectl exec -it pilot-k8s-native -c pilot -- /bin/bash
```

### 7. Iterative Development

To test code changes:

```bash
# 1. Rebuild and reload image
docker build -t pilot3:dev .
kind load docker-image pilot3:dev --name pilot-dev

# 2. Delete and recreate pod
kubectl delete pod pilot-k8s-native
kubectl apply -f pilot-k8s-native.yaml
```

## Testing with kubectl cp (Quick Iteration)

For faster iteration without rebuilding the image:

```bash
# Deploy with pilot-wrapper image
kubectl apply -f pilot-k8s-native.yaml

# After making code changes, copy files directly
kubectl cp /Users/llopis/src/pilot3/pilot/util/k8s.py pilot-dev/pilot-k8s-native:/pilot3/pilot/util/k8s.py
kubectl cp /Users/llopis/src/pilot3/pilot/control/payloads/kubernetes_executor.py pilot-dev/pilot-k8s-native:/pilot3/pilot/control/payloads/kubernetes_executor.py
```

Note: This works for `.py` files but won't update dependencies (like kubernetes package).

## Environment Variables

The k8s-native executor uses these environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `POD_NAME` | Name of the pilot pod | auto-detected |
| `POD_NAMESPACE` | Namespace of the pilot pod | default |
| `K8S_SHARED_VOLUME_PATH` | Path to shared volume | /mnt/wrapper |

## Job Configuration

The k8s-native executor is activated when the `PILOT_K8S_NATIVE` environment variable is set to `"true"` or `"1"` (as shown in the pod manifest above).

Internally, the executor also validates the job using queuedata params:
- `k8s_native_payload` - must be `True` for validation to pass (checked by `validate_k8s_native_requirements()`)
- `k8s_payload_container` - name of payload container (default: "payload")

## Cleanup

```bash
# Delete the pod
kubectl delete pod pilot-k8s-native

# Delete the cluster (when done)
kind delete cluster --name pilot-dev
```

## Troubleshooting

### Pod not starting
```bash
kubectl describe pod pilot-k8s-native
kubectl logs pilot-k8s-native -c pilot
```

### Container not being patched
- Check that the payload container name matches (`k8s_payload_container` param)
- Verify shared volume is mounted in both containers

### Shared volume not accessible
- Check init container ran successfully: `kubectl describe pod pilot-k8s-native`
- Verify volume mounts are correct in both containers

### kubernetes library import failing
- Ensure kubernetes package is installed in the container
- Check `/opt/pilot/bin/pip list | grep kubernetes`