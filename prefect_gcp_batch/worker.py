"""
Prefect worker for Google Cloud Batch.

Executes Prefect flow runs as single-task Cloud Batch jobs on GCE VMs.
Supports any machine type, Spot VMs with automatic preemption retry,
and Docker container execution.
"""

import asyncio
import re
import shlex
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from uuid import uuid4

from pydantic import Field
from slugify import slugify

from prefect.logging.loggers import PrefectLogAdapter
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from prefect_gcp.credentials import GcpCredentials

try:
    from google.cloud import batch_v1
except ImportError:
    raise ImportError(
        "google-cloud-batch is required. Install it with: "
        "pip install google-cloud-batch"
    )

from google.api_core.exceptions import NotFound

if TYPE_CHECKING:
    import anyio

    from prefect.client.schemas.objects import Flow, FlowRun, WorkPool
    from prefect.client.schemas.responses import DeploymentResponse

_DISALLOWED_GCP_LABEL_CHARACTERS = re.compile(r"[^-a-zA-Z0-9_]+")

# Cloud Batch infrastructure failure exit codes.
# 50001: Spot VM preempted
# 50002: VM terminated by GCE maintenance
# 50003: Task failed due to resource constraints
# 50006: VM preempted during task startup
# 75:    EX_TEMPFAIL — transient infra issue (e.g. gcsfuse mount flake)
_INFRASTRUCTURE_EXIT_CODES = [50001, 50002, 50003, 50006, 75]

# Maximum length for Cloud Batch job IDs.
# Must match [a-z]([a-z0-9-]{0,61}[a-z0-9])? — so 63 chars total.
_MAX_JOB_ID_LENGTH = 63


def _coerce_bool(value: object, default: bool = False) -> bool:
    """Coerce a template-resolved value to bool.

    Prefect template resolution can turn booleans into strings like
    ``"True"`` or ``"False"``.  This normalises them back.
    """
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value) if value is not None else default


class CloudBatchWorkerVariables(BaseVariables):
    """
    Configuration variables for the Cloud Batch work pool.

    These fields appear in the Prefect Cloud work pool UI when creating
    or editing a pool of type ``gcp-batch``.
    """

    region: str = Field(
        description="GCP region for the Cloud Batch job.",
        examples=["us-east1", "us-central1"],
    )
    image: str = Field(
        title="Image URI",
        description=(
            "Docker image URI from Artifact Registry or Container Registry. "
            "The Cloud Batch VM will pull and run this image."
        ),
        examples=["us-central1-docker.pkg.dev/my-project/docker/my-image:latest"],
    )
    credentials: GcpCredentials = Field(
        title="GCP Credentials",
        description="GCP credentials block for the Cloud Batch API.",
    )
    machine_type: str = Field(
        default="e2-standard-4",
        title="Machine Type",
        description=(
            "GCE machine type for the VM. Any valid type works: "
            "e2-standard-4, c3d-highcpu-16, n2d-standard-8, etc."
        ),
    )
    spot: bool = Field(
        default=False,
        title="Spot VM",
        description=(
            "Use Spot VMs (preemptible). Typically 60-85% cheaper than on-demand. "
            "Cloud Batch automatically retries on preemption via lifecycle policies."
        ),
    )
    boot_disk_type: str = Field(
        default="pd-ssd",
        title="Boot Disk Type",
        description="Boot disk type: pd-standard, pd-balanced, or pd-ssd.",
    )
    boot_disk_size_gb: int = Field(
        default=100,
        title="Boot Disk Size (GB)",
        description="Boot disk size in GB. Must accommodate the Docker image and working data.",
    )
    max_retry_count: int = Field(
        default=3,
        title="Max Retry Count",
        description=(
            "Maximum retries on infrastructure failure (Spot preemption, maintenance events). "
            "Only applies to Cloud Batch infrastructure exit codes, not application errors. "
            "Range: 0-10."
        ),
    )
    max_run_duration_hours: int = Field(
        default=24,
        title="Max Run Duration (Hours)",
        description="Task timeout in hours. The task fails with exit code 50005 if exceeded.",
    )
    service_account: Optional[str] = Field(
        default=None,
        title="Service Account",
        description=(
            "Service account email for the VM. "
            "If not set, uses the default compute service account."
        ),
    )
    allowed_zones: Optional[list] = Field(
        default=None,
        title="Allowed Zones",
        description=(
            "Zones within the region to schedule VMs. Spreading across zones "
            "improves Spot availability. If not set, all zones in the region are used."
        ),
        examples=[["us-east1-b", "us-east1-c", "us-east1-d"]],
    )
    vpc_network: Optional[str] = Field(
        default=None,
        title="VPC Network",
        description=(
            "VPC network for the VM. "
            "Format: projects/{project}/global/networks/{network}. "
            "If not set, the default network is used."
        ),
    )
    gcs_volumes: Optional[Dict[str, str]] = Field(
        default=None,
        title="GCS Volume Mounts",
        description=(
            "GCS buckets to mount as volumes on the VM. "
            "Map of bucket name to mount path, e.g. "
            '{"my-bucket": "/mnt/data"}. Uses Cloud Storage FUSE.'
        ),
        examples=[{"my-bucket": "/mnt/data"}],
    )
    gpu_type: Optional[str] = Field(
        default=None,
        title="GPU Type",
        description=(
            "GPU accelerator type to attach to the VM. "
            "e.g. 'nvidia-l4', 'nvidia-tesla-t4'. "
            "If not set, no GPU is attached."
        ),
        examples=["nvidia-l4", "nvidia-tesla-t4"],
    )
    gpu_count: int = Field(
        default=1,
        title="GPU Count",
        description="Number of GPUs to attach (only used when gpu_type is set).",
    )
    install_gpu_drivers: bool = Field(
        default=True,
        title="Install GPU Drivers",
        description=(
            "Let Cloud Batch install NVIDIA GPU drivers automatically. "
            "Only used when gpu_type is set."
        ),
    )
    job_watch_poll_interval: float = Field(
        default=30.0,
        title="Poll Interval (Seconds)",
        description=(
            "Seconds between Cloud Batch API polls while watching a job. "
            "The API has rate limits — avoid setting this too low."
        ),
    )


class CloudBatchWorkerJobConfiguration(BaseJobConfiguration):
    """
    Per-flow-run job configuration for Cloud Batch.

    Built from :class:`CloudBatchWorkerVariables` via the ``job_spec`` template.
    Prefect resolves the template variables before calling
    :meth:`prepare_for_flow_run`.
    """

    region: str = Field(
        description="GCP region for the Cloud Batch job.",
    )
    credentials: Optional[GcpCredentials] = Field(
        default=None,
        title="GCP Credentials",
        description="GCP credentials block for the Cloud Batch API.",
    )
    job_spec: Dict[str, Any] = Field(
        json_schema_extra=dict(
            template={
                "image": "{{ image }}",
                "machine_type": "{{ machine_type }}",
                "spot": "{{ spot }}",
                "boot_disk_type": "{{ boot_disk_type }}",
                "boot_disk_size_gb": "{{ boot_disk_size_gb }}",
                "max_retry_count": "{{ max_retry_count }}",
                "max_run_duration_hours": "{{ max_run_duration_hours }}",
                "service_account": "{{ service_account }}",
                "allowed_zones": "{{ allowed_zones }}",
                "vpc_network": "{{ vpc_network }}",
                "gcs_volumes": "{{ gcs_volumes }}",
                "gpu_type": "{{ gpu_type }}",
                "gpu_count": "{{ gpu_count }}",
                "install_gpu_drivers": "{{ install_gpu_drivers }}",
            }
        ),
    )
    job_watch_poll_interval: float = Field(
        default=30.0,
        title="Poll Interval (Seconds)",
        description="Seconds between Cloud Batch API polls while watching a job.",
    )

    @property
    def project(self) -> str:
        """The GCP project ID from the credentials block."""
        if not self.credentials:
            raise ValueError(
                "No GcpCredentials block configured. Set the 'credentials' "
                "field on your work pool to a GcpCredentials block."
            )
        return self.credentials.project


class CloudBatchWorkerResult(BaseWorkerResult):
    """Result of a completed Cloud Batch job."""


class CloudBatchWorker(
    BaseWorker[
        CloudBatchWorkerJobConfiguration,
        CloudBatchWorkerVariables,
        CloudBatchWorkerResult,
    ]
):
    """Prefect worker that executes flow runs as Google Cloud Batch jobs."""

    type = "gcp-batch"
    job_configuration = CloudBatchWorkerJobConfiguration
    job_configuration_variables = CloudBatchWorkerVariables
    _description = (
        "Execute flow runs on Google Cloud Batch. "
        "Supports any GCE machine type and Spot VMs with automatic preemption retry."
    )
    _display_name = "Google Cloud Batch"
    _documentation_url = "https://github.com/acgourley/prefect-gcp-batch"
    _logo_url = "https://cdn.sanity.io/images/3ugk85nk/production/10424e311932e31c477ac2b9ef3d53cefbaad708-250x250.png"

    async def run(
        self,
        flow_run: "FlowRun",
        configuration: CloudBatchWorkerJobConfiguration,
        task_status: Optional["anyio.abc.TaskStatus"] = None,
    ) -> CloudBatchWorkerResult:
        """
        Create a Cloud Batch job for this flow run and poll until it completes.

        Args:
            flow_run: The flow run to execute.
            configuration: Resolved job configuration (region, job_spec, env, command).
            task_status: If provided, marked as started with the job name
                so Prefect can track and cancel the infrastructure.

        Returns:
            CloudBatchWorkerResult with status_code 0 on success, 1 on failure.
        """
        logger = self.get_flow_run_logger(flow_run)

        client = self._get_async_client(configuration)
        job = self._build_batch_job(configuration)
        job_id = self._generate_job_id(flow_run)

        request = batch_v1.CreateJobRequest(
            parent=f"projects/{configuration.project}/locations/{configuration.region}",
            job=job,
            job_id=job_id,
        )
        created_job = await client.create_job(request)
        logger.info(f"Cloud Batch job created: {created_job.name}")

        if task_status:
            task_status.started(created_job.name)

        final_job = await self._watch_job(
            created_job.name, client, configuration, logger
        )

        status_code = (
            0
            if final_job.status.state == batch_v1.JobStatus.State.SUCCEEDED
            else 1
        )
        return CloudBatchWorkerResult(
            identifier=created_job.name, status_code=status_code
        )

    async def kill_infrastructure(
        self,
        infrastructure_pid: str,
        configuration: CloudBatchWorkerJobConfiguration,
        grace_seconds: int = 30,
    ) -> None:
        """
        Cancel a running Cloud Batch job.

        Called by ``prefect flow-run cancel``. The *infrastructure_pid* is the
        full job name passed to ``task_status.started()`` during :meth:`run`.
        """
        logger = self.logger
        client = self._get_async_client(configuration)
        logger.info(f"Requesting deletion of Cloud Batch job: {infrastructure_pid}")
        await client.delete_job(name=infrastructure_pid)

    def _get_async_client(
        self, configuration: CloudBatchWorkerJobConfiguration
    ) -> batch_v1.BatchServiceAsyncClient:
        """Return an async Cloud Batch client using the credentials block."""
        if not configuration.credentials:
            raise ValueError(
                "No GcpCredentials block configured. Set the 'credentials' "
                "field on your work pool to a GcpCredentials block."
            )
        creds = configuration.credentials.get_credentials_from_service_account()
        return batch_v1.BatchServiceAsyncClient(credentials=creds)

    def _generate_job_id(self, flow_run: "FlowRun") -> str:
        """Generate a unique, GCP-compliant job ID for this flow run.

        Cloud Batch job IDs must:
        - Match ``[a-z]([a-z0-9-]{0,61}[a-z0-9])?``
        - Be at most 63 characters
        """
        suffix = uuid4().hex[:8]
        slug = slugify(flow_run.name, lowercase=True, max_length=50)
        # Ensure slug starts with a letter (slugify may produce leading digits)
        if slug and not slug[0].isalpha():
            slug = f"j-{slug}"
        job_id = f"prefect-{slug}-{suffix}" if slug else f"prefect-{suffix}"
        # Truncate and strip any trailing hyphen left by truncation.
        return job_id[:_MAX_JOB_ID_LENGTH].rstrip("-")

    def _build_batch_job(
        self, configuration: CloudBatchWorkerJobConfiguration
    ) -> batch_v1.Job:
        """Build a single-task Cloud Batch job from the resolved configuration."""
        spec = configuration.job_spec

        # Resolve the command to execute inside the container.
        command = configuration.command
        if isinstance(command, str):
            command = shlex.split(command)

        # Build GCS volume mounts if configured.
        volumes: List[batch_v1.Volume] = []
        gcs_volumes = spec.get("gcs_volumes")
        if gcs_volumes and isinstance(gcs_volumes, dict):
            for bucket, mount_path in gcs_volumes.items():
                volumes.append(
                    batch_v1.Volume(
                        gcs=batch_v1.GCS(remote_path=bucket),
                        mount_path=mount_path,
                    )
                )

        container = batch_v1.Runnable.Container(
            image_uri=spec["image"],
            commands=command,
            volumes=[v.mount_path for v in volumes],
        )

        # Pass Prefect env vars (API URL, API key, flow-run ID, etc.) to the container.
        env_vars = {k: v for k, v in configuration.env.items() if v is not None}
        env = batch_v1.Environment(variables=env_vars)

        runnable = batch_v1.Runnable(container=container, environment=env)

        # Task spec with infrastructure-failure retry policy.
        task_spec = batch_v1.TaskSpec(
            runnables=[runnable],
            volumes=volumes,
            max_retry_count=int(spec.get("max_retry_count", 3)),
            max_run_duration=f"{int(spec.get('max_run_duration_hours', 24)) * 3600}s",
            lifecycle_policies=[
                batch_v1.LifecyclePolicy(
                    action=batch_v1.LifecyclePolicy.Action.RETRY_TASK,
                    action_condition=batch_v1.LifecyclePolicy.ActionCondition(
                        exit_codes=_INFRASTRUCTURE_EXIT_CODES,
                    ),
                ),
            ],
        )

        # Allocation policy: machine type, Spot vs standard, boot disk.
        is_spot = _coerce_bool(spec.get("spot", False))

        instance_policy = batch_v1.AllocationPolicy.InstancePolicy(
            machine_type=spec.get("machine_type", "e2-standard-4"),
            provisioning_model=(
                batch_v1.AllocationPolicy.ProvisioningModel.SPOT
                if is_spot
                else batch_v1.AllocationPolicy.ProvisioningModel.STANDARD
            ),
            boot_disk=batch_v1.AllocationPolicy.Disk(
                type_=spec.get("boot_disk_type", "pd-ssd"),
                size_gb=int(spec.get("boot_disk_size_gb", 100)),
            ),
        )

        # GPU accelerator attachment.
        gpu_type = spec.get("gpu_type")
        # Template resolution can turn None into the string "None"
        if isinstance(gpu_type, str) and gpu_type.lower() == "none":
            gpu_type = None
        install_gpu_drivers = False
        if gpu_type:
            gpu_count = int(spec.get("gpu_count", 1))
            instance_policy.accelerators = [
                batch_v1.AllocationPolicy.Accelerator(
                    type_=gpu_type,
                    count=gpu_count,
                ),
            ]
            install_gpu_drivers = _coerce_bool(spec.get("install_gpu_drivers", True), default=True)

        allocation = batch_v1.AllocationPolicy(
            instances=[
                batch_v1.AllocationPolicy.InstancePolicyOrTemplate(
                    policy=instance_policy,
                    install_gpu_drivers=install_gpu_drivers,
                )
            ],
        )

        # Optional: service account for the VM.
        sa_email = spec.get("service_account")
        if sa_email:
            allocation.service_account = batch_v1.ServiceAccount(email=sa_email)

        # Optional: restrict to specific zones for better Spot availability.
        zones = spec.get("allowed_zones")
        if zones:
            if isinstance(zones, str):
                zones = [zones]
            allocation.location = batch_v1.AllocationPolicy.LocationPolicy(
                allowed_locations=[
                    z if z.startswith("zones/") else f"zones/{z}" for z in zones
                ],
            )

        # Optional: VPC network.
        vpc = spec.get("vpc_network")
        if vpc:
            allocation.network = batch_v1.AllocationPolicy.NetworkPolicy(
                network_interfaces=[
                    batch_v1.AllocationPolicy.NetworkInterface(network=vpc),
                ],
            )

        return batch_v1.Job(
            task_groups=[
                batch_v1.TaskGroup(task_spec=task_spec, task_count=1),
            ],
            allocation_policy=allocation,
            logs_policy=batch_v1.LogsPolicy(
                destination=batch_v1.LogsPolicy.Destination.CLOUD_LOGGING,
            ),
            labels=self._get_sanitized_labels(configuration),
        )

    async def _watch_job(
        self,
        job_name: str,
        client: batch_v1.BatchServiceAsyncClient,
        configuration: CloudBatchWorkerJobConfiguration,
        logger: PrefectLogAdapter,
    ) -> batch_v1.Job:
        """Poll the Cloud Batch API until the job reaches a terminal state."""
        terminal_states = {
            batch_v1.JobStatus.State.SUCCEEDED,
            batch_v1.JobStatus.State.FAILED,
            batch_v1.JobStatus.State.DELETION_IN_PROGRESS,
        }
        last_state = None

        while True:
            try:
                job = await client.get_job(name=job_name)
            except NotFound:
                # Job was deleted (e.g. via kill_infrastructure cancellation).
                logger.info("Cloud Batch job was deleted (cancelled).")
                deleted_job = batch_v1.Job()
                deleted_job.status.state = (
                    batch_v1.JobStatus.State.DELETION_IN_PROGRESS
                )
                return deleted_job

            state = job.status.state

            if state != last_state:
                state_name = batch_v1.JobStatus.State(state).name
                logger.info(f"Cloud Batch job state: {state_name}")
                last_state = state

            if state in terminal_states:
                if state == batch_v1.JobStatus.State.FAILED:
                    await self._log_failed_task_details(job_name, client, logger)
                return job

            await asyncio.sleep(configuration.job_watch_poll_interval)

    async def _log_failed_task_details(
        self,
        job_name: str,
        client: batch_v1.BatchServiceAsyncClient,
        logger: PrefectLogAdapter,
    ) -> None:
        """Fetch and log task-level details when a job fails."""
        try:
            tasks = await client.list_tasks(
                parent=f"{job_name}/taskGroups/group0"
            )
            async for task in tasks:
                if task.status.state != batch_v1.TaskStatus.State.SUCCEEDED:
                    events = [e.description for e in task.status.status_events]
                    logger.error(
                        f"Task {task.name} failed — status events: {events}"
                    )
        except Exception as exc:
            logger.warning(f"Could not fetch task details: {exc}")

    def _get_sanitized_labels(
        self, configuration: CloudBatchWorkerJobConfiguration
    ) -> Dict[str, str]:
        """Sanitize Prefect labels for GCP compliance.

        GCP labels must be lowercase alphanumeric with hyphens and underscores,
        max 63 characters. Prefect labels may contain dots and slashes.
        """
        sanitized = {}
        for key, val in configuration.labels.items():
            clean_key = slugify(
                key,
                lowercase=True,
                replacements=[("/", "_"), (".", "-")],
                max_length=63,
                regex_pattern=_DISALLOWED_GCP_LABEL_CHARACTERS,
            )
            clean_val = slugify(
                val,
                lowercase=True,
                replacements=[("/", "_"), (".", "-")],
                max_length=63,
                regex_pattern=_DISALLOWED_GCP_LABEL_CHARACTERS,
            )
            sanitized[clean_key] = clean_val
        return sanitized
