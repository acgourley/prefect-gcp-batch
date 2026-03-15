"""Unit tests for the Cloud Batch worker.

Focused on logic that can silently break: job spec construction,
template/variable alignment, type coercion from Prefect's template engine,
the async create→poll→result lifecycle, and GCP API constraints (job IDs, labels).
"""

import re
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.cloud import batch_v1
from prefect_gcp.credentials import GcpCredentials

from prefect_gcp_batch.worker import (
    CloudBatchWorker,
    CloudBatchWorkerJobConfiguration,
    CloudBatchWorkerResult,
    CloudBatchWorkerVariables,
    _INFRASTRUCTURE_EXIT_CODES,
    _MAX_JOB_ID_LENGTH,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_JOB_ID_PATTERN = re.compile(r"^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$")


def _make_credentials() -> GcpCredentials:
    """Create a GcpCredentials without triggering google.auth.default()."""
    return GcpCredentials.model_construct(project="my-project")


def _make_configuration(**overrides) -> CloudBatchWorkerJobConfiguration:
    """Build a minimal job configuration for testing."""
    defaults = {
        "region": "us-east1",
        "credentials": _make_credentials(),
        "job_spec": {
            "image": "gcr.io/my-project/my-image:latest",
            "machine_type": "e2-standard-4",
            "spot": False,
            "boot_disk_type": "pd-ssd",
            "boot_disk_size_gb": 100,
            "max_retry_count": 3,
            "max_run_duration_hours": 24,
            "service_account": None,
            "allowed_zones": None,
            "vpc_network": None,
        },
        "job_watch_poll_interval": 0.01,
        "command": "prefect flow-run execute",
        "env": {
            "PREFECT_API_URL": "https://api.prefect.cloud/...",
            "PREFECT_API_KEY": "pnu_abc123",
        },
    }
    defaults.update(overrides)
    return CloudBatchWorkerJobConfiguration.model_construct(**defaults)


def _make_flow_run(name="my-test-flow-run"):
    fr = MagicMock()
    fr.name = name
    fr.id = "00000000-0000-0000-0000-000000000001"
    return fr


def _make_worker() -> CloudBatchWorker:
    worker = CloudBatchWorker.__new__(CloudBatchWorker)
    worker._logger = MagicMock()
    return worker


def _make_batch_job_status(state):
    job = MagicMock()
    job.status.state = state
    job.name = "projects/my-project/locations/us-east1/jobs/prefect-test-abc12345"
    return job


class AsyncIterList:
    """Wraps a list as an async iterator (for mocking list_tasks)."""

    def __init__(self, items):
        self._items = list(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._items:
            raise StopAsyncIteration
        return self._items.pop(0)


# ---------------------------------------------------------------------------
# Template / Variable Alignment
# ---------------------------------------------------------------------------


class TestTemplateAlignment:
    def test_template_keys_match_variable_fields(self):
        """If a variable is added without a template entry (or vice versa),
        that config silently has no effect. This catches it."""
        schema = CloudBatchWorkerJobConfiguration.model_json_schema()
        template_keys = set(schema["properties"]["job_spec"]["template"].keys())

        # Fields on Variables that should flow through the template.
        # Excludes fields handled outside the template (region, credentials,
        # job_watch_poll_interval) and inherited BaseVariables fields.
        base_fields = {"name", "env", "labels", "command"}
        non_template_fields = {"region", "credentials", "job_watch_poll_interval"}
        variable_fields = (
            set(CloudBatchWorkerVariables.model_fields.keys())
            - base_fields
            - non_template_fields
        )

        assert template_keys == variable_fields


# ---------------------------------------------------------------------------
# Build Batch Job
# ---------------------------------------------------------------------------


class TestBuildBatchJob:
    def test_all_options_wired_through(self):
        """Set every optional field and verify it lands in the right place."""
        worker = _make_worker()
        config = _make_configuration(
            job_spec={
                "image": "gcr.io/proj/img:v2",
                "machine_type": "c3d-highcpu-16",
                "spot": True,
                "boot_disk_type": "pd-balanced",
                "boot_disk_size_gb": 200,
                "max_retry_count": 5,
                "max_run_duration_hours": 48,
                "service_account": "worker@proj.iam.gserviceaccount.com",
                "allowed_zones": ["us-east1-b", "us-east1-c"],
                "vpc_network": "projects/proj/global/networks/my-vpc",
            },
        )
        job = worker._build_batch_job(config)

        # Container
        runnable = job.task_groups[0].task_spec.runnables[0]
        assert runnable.container.image_uri == "gcr.io/proj/img:v2"

        # Machine / Spot
        policy = job.allocation_policy.instances[0].policy
        assert policy.machine_type == "c3d-highcpu-16"
        assert policy.provisioning_model == batch_v1.AllocationPolicy.ProvisioningModel.SPOT
        assert policy.boot_disk.type_ == "pd-balanced"
        assert policy.boot_disk.size_gb == 200

        # Task spec
        task_spec = job.task_groups[0].task_spec
        assert task_spec.max_retry_count == 5
        assert task_spec.max_run_duration.total_seconds() == 48 * 3600

        # Service account
        assert job.allocation_policy.service_account.email == "worker@proj.iam.gserviceaccount.com"

        # Zones (with prefix normalization)
        assert job.allocation_policy.location.allowed_locations == [
            "zones/us-east1-b",
            "zones/us-east1-c",
        ]

        # VPC
        interfaces = job.allocation_policy.network.network_interfaces
        assert interfaces[0].network == "projects/proj/global/networks/my-vpc"

    def test_spot_string_coercion(self):
        """Prefect's template engine may resolve booleans as strings.
        Without this handling, spot silently becomes False."""
        worker = _make_worker()
        for truthy in ("True", "true", "1", "yes"):
            spec = _make_configuration().job_spec.copy()
            spec["spot"] = truthy
            config = _make_configuration(job_spec=spec)
            job = worker._build_batch_job(config)
            policy = job.allocation_policy.instances[0].policy
            assert (
                policy.provisioning_model
                == batch_v1.AllocationPolicy.ProvisioningModel.SPOT
            ), f"spot={truthy!r} was not coerced to SPOT"

    def test_env_vars_exclude_none(self):
        """None env values must be filtered — Cloud Batch rejects them,
        and missing Prefect env vars mean the flow run can't report back."""
        worker = _make_worker()
        config = _make_configuration(
            env={"PREFECT_API_URL": "https://api.prefect.cloud", "EMPTY": None}
        )
        job = worker._build_batch_job(config)
        env_vars = job.task_groups[0].task_spec.runnables[0].environment.variables
        assert "PREFECT_API_URL" in env_vars
        assert "EMPTY" not in env_vars

    def test_lifecycle_policies_target_infra_exit_codes(self):
        """If the preemption exit codes are dropped, Spot retry silently
        stops working and jobs fail on first preemption."""
        worker = _make_worker()
        job = worker._build_batch_job(_make_configuration())

        policies = job.task_groups[0].task_spec.lifecycle_policies
        assert len(policies) == 1
        assert policies[0].action == batch_v1.LifecyclePolicy.Action.RETRY_TASK
        assert list(policies[0].action_condition.exit_codes) == _INFRASTRUCTURE_EXIT_CODES


# ---------------------------------------------------------------------------
# Job ID Generation
# ---------------------------------------------------------------------------


class TestJobId:
    @pytest.mark.parametrize(
        "name",
        [
            "my-flow-run",
            "a" * 200,
            "My Flow/Run (v2.1)",
            "123-numeric-start",
            "",
        ],
        ids=["normal", "long", "special-chars", "numeric-prefix", "empty"],
    )
    def test_job_id_valid(self, name):
        """Job IDs must satisfy GCP's regex and length constraint for any input."""
        worker = _make_worker()
        job_id = worker._generate_job_id(_make_flow_run(name))
        assert len(job_id) <= _MAX_JOB_ID_LENGTH
        assert _JOB_ID_PATTERN.match(job_id), f"Invalid job ID: {job_id!r} (from name={name!r})"

    def test_unique(self):
        worker = _make_worker()
        flow_run = _make_flow_run("same-name")
        ids = {worker._generate_job_id(flow_run) for _ in range(100)}
        assert len(ids) == 100


# ---------------------------------------------------------------------------
# Label Sanitization
# ---------------------------------------------------------------------------


class TestLabels:
    def test_prefect_labels_sanitized(self):
        """Prefect labels use dots/slashes which GCP rejects.
        Long values must be truncated to 63 chars."""
        worker = _make_worker()
        config = _make_configuration()
        config.labels = {
            "prefect.io/flow-name": "publish-v6",
            "x" * 100: "y" * 100,
        }
        labels = worker._get_sanitized_labels(config)
        for key, val in labels.items():
            assert re.match(r"^[a-z0-9_-]+$", key), f"Bad label key: {key}"
            assert len(key) <= 63
            assert len(val) <= 63


# ---------------------------------------------------------------------------
# Async Lifecycle: run / watch / kill
# ---------------------------------------------------------------------------


class TestRunLifecycle:
    @pytest.mark.parametrize(
        "terminal_state, expected_code",
        [
            (batch_v1.JobStatus.State.SUCCEEDED, 0),
            (batch_v1.JobStatus.State.FAILED, 1),
        ],
        ids=["success", "failure"],
    )
    async def test_run_returns_correct_status_code(self, terminal_state, expected_code):
        worker = _make_worker()
        worker.get_flow_run_logger = MagicMock(return_value=MagicMock())

        created_job = _make_batch_job_status(batch_v1.JobStatus.State.QUEUED)
        final_job = _make_batch_job_status(terminal_state)

        mock_client = AsyncMock()
        mock_client.create_job.return_value = created_job
        mock_client.get_job.return_value = final_job
        mock_client.list_tasks.return_value = AsyncIterList([])

        with patch.object(worker, "_get_async_client", return_value=mock_client):
            result = await worker.run(_make_flow_run(), _make_configuration())

        assert isinstance(result, CloudBatchWorkerResult)
        assert result.status_code == expected_code

    async def test_reports_infrastructure_pid_for_cancellation(self):
        """task_status.started() must be called with the job name,
        otherwise `prefect flow-run cancel` silently does nothing."""
        worker = _make_worker()
        worker.get_flow_run_logger = MagicMock(return_value=MagicMock())

        created_job = _make_batch_job_status(batch_v1.JobStatus.State.QUEUED)
        final_job = _make_batch_job_status(batch_v1.JobStatus.State.SUCCEEDED)

        mock_client = AsyncMock()
        mock_client.create_job.return_value = created_job
        mock_client.get_job.return_value = final_job

        task_status = MagicMock()
        with patch.object(worker, "_get_async_client", return_value=mock_client):
            await worker.run(_make_flow_run(), _make_configuration(), task_status=task_status)

        task_status.started.assert_called_once_with(created_job.name)


class TestWatchJob:
    async def test_polls_until_terminal_state(self):
        worker = _make_worker()
        mock_client = AsyncMock()
        mock_client.get_job.side_effect = [
            _make_batch_job_status(batch_v1.JobStatus.State.RUNNING),
            _make_batch_job_status(batch_v1.JobStatus.State.RUNNING),
            _make_batch_job_status(batch_v1.JobStatus.State.SUCCEEDED),
        ]

        result = await worker._watch_job(
            "projects/p/locations/r/jobs/j",
            mock_client,
            _make_configuration(),
            MagicMock(),
        )

        assert result.status.state == batch_v1.JobStatus.State.SUCCEEDED
        assert mock_client.get_job.await_count == 3


class TestKillInfrastructure:
    async def test_deletes_job(self):
        worker = _make_worker()
        worker.logger = MagicMock()
        mock_client = AsyncMock()

        with patch.object(worker, "_get_async_client", return_value=mock_client):
            await worker.kill_infrastructure(
                "projects/p/locations/r/jobs/j", _make_configuration()
            )

        mock_client.delete_job.assert_awaited_once_with(
            name="projects/p/locations/r/jobs/j"
        )
