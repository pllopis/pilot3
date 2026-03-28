#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Authors:
# - Pilot Team, pilot@apache.org, 2025

"""Kubernetes utilities for native k8s payload execution."""

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

from pilot.common.errorcodes import ErrorCodes

logger = logging.getLogger(__name__)
errors = ErrorCodes()


class K8sError(Exception):
    """Kubernetes error with error code."""

    def __init__(self, message: str, error_code: int):
        super().__init__(message)
        self.error_code = error_code


@dataclass
class K8sPayloadHandle:
    """Handle for tracking k8s-native payload execution."""

    namespace: str
    pod_name: str
    container_name: str
    image: str
    initial_image: str
    wrapper_script_path: str = None
    job_dir_path: str = None
    start_time: float = None
    command: str = None
    args: str = None


class KubernetesClient:
    """Kubernetes API client wrapper for pilot k8s-native operations."""

    def __init__(self):
        self._core_v1 = None

    def _get_core_v1(self):
        """Get CoreV1Api client, loading config if necessary."""
        global _kubernetes_client
        if self._core_v1 is None:
            try:
                import kubernetes
                from kubernetes import client, config

                _kubernetes_client = kubernetes
                _kubernetes_config = config
            except ImportError:
                raise K8sError(
                    "kubernetes python library is required for k8s-native execution. "
                    "Install with: pip install kubernetes",
                    errors.K8SNATIVESETUPFAIL,
                )
            try:
                _kubernetes_config.load_incluster_config()
            except _kubernetes_config.ConfigException:
                _kubernetes_config.load_kube_config()
            self._core_v1 = client.CoreV1Api()
        return self._core_v1

    @property
    def core_v1(self):
        """Return the CoreV1Api client."""
        return self._get_core_v1()

    def get_pod(self, namespace: str, pod_name: str) -> Optional[Any]:
        """
        Get pod information.

        Args:
            namespace: Pod namespace
            pod_name: Pod name

        Returns:
            Pod dict or None if not found

        Raises:
            K8sError: On API errors
        """
        try:
            from kubernetes.client.exceptions import ApiException

            return self.core_v1.read_namespaced_pod(namespace=namespace, name=pod_name)
        except ApiException as e:
            if e.status == 404:
                return None
            logger.error(f"Failed to get pod {pod_name} in {namespace}: {e}")
            raise K8sError(f"Failed to get pod: {e}", errors.K8SPODNOTFOUND)

    def patch_container_image(
        self, namespace: str, pod_name: str, container_name: str, new_image: str
    ) -> None:
        """
        Patch a container's image in a pod.

        Args:
            namespace: Pod namespace
            pod_name: Pod name
            container_name: Container name to patch
            new_image: New container image URL

        Raises:
            K8sError: On patch failure
        """
        from kubernetes.client.exceptions import ApiException

        patch = [
            {
                "op": "replace",
                "path": f"/spec/containers/{container_name}/image",
                "value": new_image,
            }
        ]
        try:
            self.core_v1.patch_namespaced_pod(
                name=pod_name, namespace=namespace, body=patch
            )
            logger.info(f"Patched container {container_name} to image {new_image}")
        except ApiException as e:
            logger.error(f"Failed to patch container image: {e}")
            raise K8sError(
                f"Failed to patch container image: {e}", errors.K8SPODPATCHFAILED
            )

    def wait_for_container_ready(
        self, namespace: str, pod_name: str, container_name: str, timeout: int = 300
    ) -> bool:
        """
        Wait for container to be ready or running.

        Args:
            namespace: Pod namespace
            pod_name: Pod name
            container_name: Container name
            timeout: Maximum wait time in seconds

        Returns:
            True if container is running, False otherwise

        Raises:
            K8sError: On timeout or API errors
        """
        from kubernetes.client.exceptions import ApiException

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                pod = self.core_v1.read_namespaced_pod(
                    name=pod_name, namespace=namespace
                )
                container_status = None
                for status in pod.status.container_statuses or []:
                    if status.name == container_name:
                        container_status = status
                        break

                if container_status:
                    if container_status.state.running:
                        return True
                    elif container_status.state.terminated:
                        logger.warning(
                            f"Container {container_name} terminated with exit code: "
                            f"{container_status.state.terminated.exit_code}"
                        )
                        return False

                time.sleep(1)
            except ApiException as e:
                logger.warning(f"Error checking pod status: {e}")
                time.sleep(1)

        raise K8sError(
            f"Container {container_name} did not start within {timeout}s",
            errors.K8SCONTAINERSTARTTIMEOUT,
        )

    def wait_for_container_termination(
        self, namespace: str, pod_name: str, container_name: str, timeout: int = 86400
    ) -> tuple[bool, int]:
        """
        Wait for container to terminate.

        Args:
            namespace: Pod namespace
            pod_name: Pod name
            container_name: Container name
            timeout: Maximum wait time in seconds

        Returns:
            Tuple of (terminated: bool, exit_code: int)
        """
        from kubernetes.client.exceptions import ApiException

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                pod = self.core_v1.read_namespaced_pod(
                    name=pod_name, namespace=namespace
                )
                container_status = None
                for status in pod.status.container_statuses or []:
                    if status.name == container_name:
                        container_status = status
                        break

                if container_status and container_status.state.terminated:
                    return True, container_status.state.terminated.exit_code

                time.sleep(1)
            except ApiException as e:
                logger.warning(f"Error checking pod status: {e}")
                time.sleep(1)

        return False, -1

    def stream_container_logs(
        self,
        namespace: str,
        pod_name: str,
        container_name: str,
        follow: bool = True,
        callback=None,
    ) -> None:
        """
        Stream container logs.

        Args:
            namespace: Pod namespace
            pod_name: Pod name
            container_name: Container name
            follow: Follow logs in real-time
            callback: Optional callback function to process each line
        """
        from kubernetes.client.exceptions import ApiException

        try:
            logs = self.core_v1.read_namespaced_pod_log(
                name=pod_name,
                namespace=namespace,
                container=container_name,
                follow=follow,
                _preload_content=False,
            )
            for line in logs:
                decoded_line = line.decode("utf-8", errors="replace").strip()
                if decoded_line:
                    if callback:
                        callback(decoded_line)
                    else:
                        logger.info(f"[{container_name}] {decoded_line}")
        except ApiException as e:
            logger.warning(f"Error streaming logs: {e}")


_k8s_client = None


def get_k8s_client() -> KubernetesClient:
    """Get the singleton KubernetesClient instance."""
    global _k8s_client
    if _k8s_client is None:
        _k8s_client = KubernetesClient()
    return _k8s_client


def in_k8s() -> bool:
    """
    Check if running inside a Kubernetes cluster.

    Returns:
        True if running in k8s, False otherwise
    """
    return os.environ.get("KUBERNETES_SERVICE_HOST") is not None


def get_pod_identity() -> tuple[str, str]:
    """
    Get the current pod name and namespace.

    Returns:
        Tuple of (pod_name, namespace)

    Raises:
        K8sError: If running outside k8s or identity not available
    """
    if not in_k8s():
        raise K8sError("Not running in Kubernetes", errors.K8SPODNOTFOUND)

    pod_name = os.environ.get("HOSTNAME") or os.environ.get("POD_NAME")
    namespace = os.environ.get("POD_NAMESPACE", "default")

    if not pod_name:
        raise K8sError("Pod name not available in environment", errors.K8SPODNOTFOUND)

    return pod_name, namespace


def is_k8s_native_payload(job) -> bool:
    """
    Check if job should run as k8s-native payload.

    Args:
        job: JobData object

    Returns:
        True if job should run as k8s-native payload
    """
    return job.infosys.queuedata.params.get("k8s_native_payload", False)


def get_payload_container_name(job) -> str:
    """
    Get the payload container name from queuedata params.

    Args:
        job: JobData object

    Returns:
        Container name
    """
    return job.infosys.queuedata.params.get("k8s_payload_container", "payload")


def get_shared_volume_path() -> str:
    """
    Get the path to the shared volume mount.

    Returns:
        Shared volume path
    """
    return os.environ.get("K8S_SHARED_VOLUME_PATH", "/mnt/wrapper")


def get_job_dir_path(job_id: str) -> str:
    """
    Get the job-specific directory path in shared volume.

    Args:
        job_id: Job ID

    Returns:
        Job directory path
    """
    return os.path.join(get_shared_volume_path(), f"job-{job_id}")


def generate_wrapper_script(command: str, args: str = "", env: dict = None) -> str:
    """
    Generate the wrapper script content.

    The wrapper script executes the payload command and captures exit code.

    Args:
        command: Command to execute
        args: Arguments for the command
        env: Environment variables to set

    Returns:
        Wrapper script content as string
    """
    env_block = ""
    if env:
        for key, value in env.items():
            env_block += f"export {key}='{value}'\n"

    return f"""#!/bin/bash
# Wrapper script for k8s-native payload execution
# Generated by pilot3

set -e

{env_block}

# Define paths
WRAPPER_PATH="$(dirname "$0")"
JOB_DIR="$(dirname "$WRAPPER_PATH")"
EXIT_CODE_FILE="${{JOB_DIR}}/exit_code"
STDOUT_FILE="${{JOB_DIR}}/stdout.txt"
STDERR_FILE="${{JOB_DIR}}/stderr.txt"

# Write PID for monitoring
echo $$ > "${{JOB_DIR}}/wrapper.pid"

# Execute the payload command
echo "Executing: {command} {args}"
{command} {args} > "$STDOUT_FILE" 2> "$STDERR_FILE"

# Capture exit code
EXIT_CODE=$?
echo $EXIT_CODE > "$EXIT_CODE_FILE"

echo "Payload exited with code: $EXIT_CODE"

exit $EXIT_CODE
"""


def write_wrapper_script(job_dir: str, script_content: str) -> str:
    """
    Write wrapper script to shared volume.

    Args:
        job_dir: Job directory path
        script_content: Wrapper script content

    Returns:
        Path to the wrapper script

    Raises:
        K8sError: On write failure
    """
    wrapper_path = os.path.join(job_dir, "launch.sh")
    try:
        os.makedirs(job_dir, exist_ok=True)
        with open(wrapper_path, "w") as f:
            f.write(script_content)
        os.chmod(wrapper_path, 0o755)
        logger.info(f"Wrote wrapper script to {wrapper_path}")
        return wrapper_path
    except OSError as e:
        logger.error(f"Failed to write wrapper script: {e}")
        raise K8sError(
            f"Failed to write wrapper script: {e}", errors.K8SWRAPPERSCRIPTWRITEFAILED
        )


def read_exit_code(job_dir: str) -> int:
    """
    Read the exit code from the wrapper script output.

    Args:
        job_dir: Job directory path

    Returns:
        Exit code, or -1 if not found
    """
    exit_code_file = os.path.join(job_dir, "exit_code")
    try:
        if os.path.exists(exit_code_file):
            with open(exit_code_file, "r") as f:
                return int(f.read().strip())
    except (OSError, ValueError) as e:
        logger.warning(f"Failed to read exit code: {e}")
    return -1


def read_wrapper_stdout(job_dir: str) -> str:
    """
    Read the stdout from the wrapper script output.

    Args:
        job_dir: Job directory path

    Returns:
        Stdout content, or empty string if not found
    """
    stdout_file = os.path.join(job_dir, "stdout.txt")
    try:
        if os.path.exists(stdout_file):
            with open(stdout_file, "r") as f:
                return f.read()
    except OSError as e:
        logger.warning(f"Failed to read stdout: {e}")
    return ""


def read_wrapper_stderr(job_dir: str) -> str:
    """
    Read the stderr from the wrapper script output.

    Args:
        job_dir: Job directory path

    Returns:
        Stderr content, or empty string if not found
    """
    stderr_file = os.path.join(job_dir, "stderr.txt")
    try:
        if os.path.exists(stderr_file):
            with open(stderr_file, "r") as f:
                return f.read()
    except OSError as e:
        logger.warning(f"Failed to read stderr: {e}")
    return ""


def validate_k8s_native_requirements(job) -> tuple[bool, str]:
    """
    Validate that k8s-native requirements are met.

    Args:
        job: JobData object

    Returns:
        Tuple of (is_valid: bool, error_message: str)
    """
    if not in_k8s():
        return False, "Not running in Kubernetes cluster"

    try:
        pod_name, namespace = get_pod_identity()
    except K8sError as e:
        return False, str(e)

    client = get_k8s_client()
    pod = client.get_pod(namespace, pod_name)

    if not pod:
        return False, f"Pod {pod_name} not found in {namespace}"

    container_names = [c.name for c in pod.spec.containers]
    payload_container = get_payload_container_name(job)

    if payload_container not in container_names:
        return (
            False,
            f"Payload container '{payload_container}' not found in pod. Available: {container_names}",
        )

    shared_volume_path = get_shared_volume_path()
    if not os.path.exists(shared_volume_path):
        return False, f"Shared volume path does not exist: {shared_volume_path}"

    if not os.access(shared_volume_path, os.W_OK):
        return False, f"Shared volume path is not writable: {shared_volume_path}"

    return True, ""
