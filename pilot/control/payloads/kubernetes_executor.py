"""KubernetesExecutor for running payloads as native k8s containers in the same pod.

Note: This executor assumes a single payload is handled at a time. The payload container
is patched for each job and can only run one job at a time.
"""

import logging
import os
import threading
import time
from typing import Any, TextIO

from pilot.common.errorcodes import ErrorCodes
from pilot.control.job import send_state
from pilot.control.payloads.generic import Executor as GenericExecutor
from pilot.info import JobData
from pilot.util.auxiliary import set_pilot_state
from pilot.util.k8s import (
    K8sError,
    get_k8s_client,
    get_pod_identity,
    get_payload_container_name,
    get_shared_volume_path,
    get_job_dir_path,
    generate_wrapper_script,
    read_exit_code,
    read_wrapper_stdout,
    read_wrapper_stderr,
    write_wrapper_script,
    validate_k8s_native_requirements,
)

logger = logging.getLogger(__name__)
errors = ErrorCodes()


class Executor(GenericExecutor):
    """Executor for running payloads as native k8s containers in the same pod."""

    def __init__(
        self, args: object, job: JobData, out: TextIO, err: TextIO, traces: Any
    ):
        """
        Initialize the KubernetesExecutor.

        Args:
            args: Pilot arguments object
            job: JobData object
            out: stdout file object
            err: stderr file object
            traces: traces object
        """
        super().__init__(args, job, out, err, traces)
        self._k8s_handle = None
        self._log_streaming = False
        self._log_thread = None
        self._job_dir = None

    def run_payload(self, job: JobData, cmd: str, out: Any, err: Any) -> Any:
        """
        Execute the payload as a native k8s container.

        This method:
        1. Validates k8s-native requirements
        2. Generates and writes wrapper script to shared volume
        3. Patches payload container with target image
        4. Waits for container to complete
        5. Reads exit code from shared volume
        6. Restores original container image

        Args:
            job: JobData object
            cmd: Command to execute
            out: stdout file object (unused, for API compatibility)
            err: stderr file object (unused, for API compatibility)

        Returns:
            K8sPayloadHandle or None if execution failed
        """
        logger.info("Starting k8s-native payload execution")

        is_valid, error_msg = validate_k8s_native_requirements(job)
        if not is_valid:
            logger.error(f"K8s-native requirements not met: {error_msg}")
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(
                errors.K8SNATIVESETUPFAIL, msg=error_msg
            )
            return None

        try:
            pod_name, namespace = get_pod_identity()
        except K8sError as e:
            logger.error(f"Failed to get pod identity: {e}")
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(
                errors.K8SPODNOTFOUND, msg=str(e)
            )
            return None

        k8s_client = get_k8s_client()
        payload_container = get_payload_container_name(job)

        try:
            pod = k8s_client.get_pod(namespace, pod_name)
        except K8sError as e:
            logger.error(f"Failed to get pod: {e}")
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(
                errors.K8SPODNOTFOUND, msg=str(e)
            )
            return None

        initial_image = None
        for container in pod.spec.containers:
            if container.name == payload_container:
                initial_image = container.image
                break

        if not initial_image:
            logger.error(
                f"Could not find initial image for container {payload_container}"
            )
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(
                errors.K8SCONTAINERNOTFOUND,
                msg=f"Container {payload_container} not found",
            )
            return None

        logger.info(f"Initial container image: {initial_image}")
        logger.info(f"Target payload image: {job.imagename}")

        wrapper_path = None
        # Write wrapper script to fixed path in shared volume (payload container waits for this file)
        shared_volume = get_shared_volume_path()
        fixed_script_dir = os.path.join(shared_volume, "pilot-wrapper")
        self._job_dir = fixed_script_dir
        try:
            env_dict = {
                "JOB_ID": job.jobid,
                "WORKDIR": job.workdir,
                "PANDA_ID": job.jobid,
            }

            wrapper_script_content = generate_wrapper_script(cmd, "", env_dict)
            wrapper_path = write_wrapper_script(fixed_script_dir, wrapper_script_content)
        except K8sError as e:
            logger.error(f"Failed to write wrapper script: {e}")
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(
                e.error_code, msg=str(e)
            )
            return None

        target_image = job.imagename if job.imagename else initial_image

        # Strip singularity container prefixes (docker://, docker-daemon://, etc.)
        # since Kubernetes expects plain image references like "alpine:latest"
        if target_image and "://" in target_image:
            target_image = target_image.split("://", 1)[1]

        try:
            if target_image != initial_image:
                k8s_client.patch_container_image(
                    namespace=namespace,
                    pod_name=pod_name,
                    container_name=payload_container,
                    new_image=target_image,
                )
            else:
                logger.info("Target image same as initial, no patch needed")
        except K8sError as e:
            logger.error(f"Failed to patch container image: {e}")
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(
                errors.K8SIMAGEPATCHFAILED, msg=str(e)
            )
            return None

        self._k8s_handle = {
            "namespace": namespace,
            "pod_name": pod_name,
            "container_name": payload_container,
            "image": target_image,
            "initial_image": initial_image,
            "wrapper_script_path": wrapper_path,
            "job_dir": self._job_dir,
        }

        try:
            k8s_client.wait_for_container_ready(
                namespace=namespace,
                pod_name=pod_name,
                container_name=payload_container,
                timeout=300,
            )
        except K8sError as e:
            logger.error(f"Container failed to start: {e}")
            self._restore_container_image(k8s_client)
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(
                e.error_code, msg=str(e)
            )
            return None

        set_pilot_state(job=job, state="running")

        if self._Executor__args.update_server:
            send_state(job, self._Executor__args, job.state)

        start_time = time.time()
        self._k8s_handle["start_time"] = start_time

        self._start_log_streaming(k8s_client, namespace, pod_name, payload_container)

        terminated, exit_code = k8s_client.wait_for_container_termination(
            namespace=namespace,
            pod_name=pod_name,
            container_name=payload_container,
            timeout=86400,
        )

        self._stop_log_streaming()

        if not terminated:
            logger.warning("Container did not terminate within timeout")
            exit_code = -1

        stdout_content = read_wrapper_stdout(self._job_dir)
        stderr_content = read_wrapper_stderr(self._job_dir)

        if out and stdout_content:
            try:
                out.write(stdout_content.encode())
                out.flush()
            except Exception as e:
                logger.warning(f"Failed to write stdout to file: {e}")

        if err and stderr_content:
            try:
                err.write(stderr_content.encode())
                err.flush()
            except Exception as e:
                logger.warning(f"Failed to write stderr to file: {e}")

        logger.info(f"Payload execution completed with exit code: {exit_code}")
        logger.info(f"Execution time: {time.time() - start_time:.2f} seconds")

        self._restore_container_image(k8s_client)

        return exit_code

    def _restore_container_image(self, k8s_client=None) -> None:
        """Restore the original container image."""
        if not self._k8s_handle:
            return

        if k8s_client is None:
            k8s_client = get_k8s_client()

        try:
            k8s_client.patch_container_image(
                namespace=self._k8s_handle["namespace"],
                pod_name=self._k8s_handle["pod_name"],
                container_name=self._k8s_handle["container_name"],
                new_image=self._k8s_handle["initial_image"],
            )
            logger.info(
                f"Restored container image to {self._k8s_handle['initial_image']}"
            )
        except K8sError as e:
            logger.warning(f"Failed to restore container image: {e}")

    def wait_graceful(self, args: object, proc: Any) -> int:
        """
        Wait for payload process to finish (overridden for k8s-native).

        For k8s-native execution, the 'proc' parameter is actually the exit code
        returned by run_payload().

        Args:
            args: Pilot arguments object
            proc: Exit code (int) from run_payload()

        Returns:
            Exit code (int)
        """
        if proc is None:
            return 0

        if isinstance(proc, dict):
            handle = proc
            exit_code = proc
        else:
            exit_code = proc

        if isinstance(exit_code, dict):
            exit_code = read_exit_code(exit_code.get("job_dir", self._job_dir))

        if exit_code is None:
            exit_code = 0

        return exit_code

    def run(self) -> tuple[int, str]:
        """
        Run the k8s-native payload execution.

        This overrides the generic run() method since k8s-native has a different
        execution model (container patching instead of subprocess execution).

        :return: exit code (int), diagnostics (str).
        """
        diagnostics = ""

        self.pre_setup(self._Executor__job)

        # K8s-native execution requires a container image
        if not self._Executor__job.imagename:
            return errors.K8SNATIVESETUPFAIL, "k8s-native execution requires a container image (--containerImage)"

        # Use jobparams directly — skip trf download and wrapping.
        # The trf (transformation) is an ATLAS concept for non-container execution.
        # With k8s-native, the container image IS the runtime environment,
        # and job.jobparams contains the direct exec string from --exec.
        cmd = self._Executor__job.jobparams
        if not cmd:
            return errors.UNKNOWNPAYLOADFAILURE, "no execution command (--exec) specified"

        self.post_setup(self._Executor__job)

        exit_code = self.run_payload(
            self._Executor__job, cmd, self._Executor__out, self._Executor__err
        )

        if exit_code is None:
            exit_code = errors.K8SNATIVESETUPFAIL
            diagnostics = "k8s-native payload execution failed"

        self.post_payload(self._Executor__job)

        return exit_code, diagnostics

    def _start_log_streaming(
        self, k8s_client, namespace: str, pod_name: str, container_name: str
    ) -> None:
        """Start streaming container logs in a background thread."""
        self._log_streaming = True

        def log_reader():
            try:
                k8s_client.stream_container_logs(
                    namespace=namespace,
                    pod_name=pod_name,
                    container_name=container_name,
                    follow=True,
                    callback=lambda line: logger.info(
                        f"[payload:{container_name}] {line}"
                    ),
                )
            except Exception as e:
                logger.warning(f"Log streaming error: {e}")
            finally:
                self._log_streaming = False

        self._log_thread = threading.Thread(target=log_reader, daemon=True)
        self._log_thread.start()
        logger.info("Started container log streaming")

    def _stop_log_streaming(self) -> None:
        """Stop the log streaming thread."""
        self._log_streaming = False
        if self._log_thread and self._log_thread.is_alive():
            self._log_thread.join(timeout=5)
            logger.info("Stopped container log streaming")
