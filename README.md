# prefect-gcp-batch

A [Prefect](https://www.prefect.io/) worker that executes flow runs as
[Google Cloud Batch](https://cloud.google.com/batch) jobs.

Each flow run becomes a single-task Cloud Batch job running your Docker image on
a GCE VM. Any machine type, automatic Spot preemption retry, full Prefect
logging integration.

## Why Cloud Batch?

Prefect's existing GCP workers have limitations:

| Feature | Vertex AI | Cloud Run v2 | **Cloud Batch** |
|---------|-----------|-------------|-----------------|
| Machine types | n1 only | serverless | **any GCE type** |
| Spot/preemptible | limited | no | **yes, native** |
| Max vCPUs | varies | 8 | **unlimited** |
| Preemption retry | no | n/a | **automatic** |
| Service fee | Vertex markup | per-request | **none** |

## Installation

```bash
pip install prefect-gcp-batch
```

## Quick Start

### 1. Prerequisites

Enable the Cloud Batch API in your GCP project:

```bash
gcloud services enable batch.googleapis.com --project=my-project
```

Create a `GcpCredentials` block in the
[Prefect Cloud UI](https://app.prefect.cloud) (Blocks > + > GCP Credentials)
with a service account key. The service account needs these IAM roles
(see [Credentials](#credentials) for the `gcloud` commands):

- `roles/batch.jobsAdmin`
- `roles/batch.agentReporter`
- `roles/iam.serviceAccountUser`
- `roles/artifactregistry.reader`
- `roles/logging.logWriter`

### 2. Create and configure a work pool

```bash
prefect work-pool create 'my-batch-pool' --type gcp-batch
```

Then configure it in the Prefect Cloud UI (Work Pools > my-batch-pool > Edit):

- **credentials**: select your GcpCredentials block
- **region**: `us-east1`
- **image**: `us-central1-docker.pkg.dev/my-project/docker/my-image:latest`
- **machine_type**: `c3d-highcpu-16`
- **spot**: `true`

### 3. Start the worker and run flows

```bash
# Start the worker (runs locally, dispatches to Cloud Batch)
prefect worker start --pool 'my-batch-pool'

# Submit flow runs as usual
prefect deployment run 'my-flow/my-deployment'
```

## How It Works

```
  Prefect Cloud                 Worker (local or cloud)         Google Cloud
 ┌──────────────┐              ┌──────────────┐            ┌─────────────────────┐
 │              │   polls for  │              │  creates   │  Cloud Batch        │
 │  Work Pool   │◄───────────  │    Worker    │ ─────────► │                     │
 │              │   flow runs  │   polls &    │  Batch job │  ┌───────────────┐  │
 │              │              │   monitors   │            │  │  GCE VM       │  │
 │              │              │   job status │            │  │               │  │
 │  ┌────────┐  │              │              │            │  │  Docker:      │  │
 │  │Flow Run│  │  submit job  │              │            │  │   prefect     │  │
 │  │Flow Run│──┼─────────────►│              │            │  │   flow-run    │  │
 │  │Flow Run│  │              │              │            │  │   execute     │  │
 │  └────────┘  │              └──────────────┘            │  └───────────────┘  │
 │              │                     │                    │                     │
 └──────────────┘                     │ cancel             │  Handles:           │
                                      ▼                    │  · VM lifecycle     │
                                 delete job ──────────────►│  · Retry on fail or │
                                                           │    spot preemption  │
                                                           └─────────────────────┘
```

1. Worker runs locally (Mac, small VM, wherever) and polls Prefect Cloud
2. For each queued flow run, the worker creates a Cloud Batch job
3. The job runs your Docker image with `prefect flow-run execute`
4. Cloud Batch handles VM lifecycle, Spot preemption, and retries
5. Worker polls until the job completes and reports the result

## Configuration

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `region` | str | *required* | GCE region (e.g. `us-east1`) |
| `image` | str | *required* | Docker image URI |
| `credentials` | GcpCredentials | *required* | GCP credentials block (see [Credentials](#credentials)) |
| `machine_type` | str | `e2-standard-4` | GCE machine type |
| `spot` | bool | `false` | Use Spot VMs (~60-85% cheaper) |
| `boot_disk_type` | str | `pd-ssd` | Boot disk type |
| `boot_disk_size_gb` | int | `100` | Boot disk size in GB |
| `max_retry_count` | int | `3` | Max retries on infrastructure failure |
| `max_run_duration_hours` | int | `24` | Task timeout in hours |
| `service_account` | str | None | Service account email for the VM |
| `allowed_zones` | list | None | Zones for Spot availability |
| `vpc_network` | str | None | VPC network for the VM |
| `job_watch_poll_interval` | float | `30.0` | Seconds between status polls |

## Spot Preemption

Cloud Batch handles Spot preemption natively:

1. VM preempted -> task gets exit code 50001
2. Lifecycle policy retries the task automatically
3. Cloud Batch provisions a new Spot VM
4. Up to `max_retry_count` retries

The worker doesn't need to know about preemption -- Cloud Batch handles it
internally. For application-level resilience, implement checkpointing in your
flow (e.g. skip already-completed steps on retry).

## Cancellation

The worker supports `prefect flow-run cancel`. When a flow run is cancelled,
the worker deletes the Cloud Batch job, which terminates the VM.

## Credentials

The worker requires a [GcpCredentials](https://docs.prefect.io/integrations/prefect-gcp)
block, configured in the work pool. See [Quick Start](#quick-start) for setup.

The service account needs these IAM roles:

- `roles/batch.jobsAdmin` — create, monitor, and delete Cloud Batch jobs
- `roles/batch.agentReporter` — allows VMs to report status back to Cloud Batch
- `roles/iam.serviceAccountUser` — act as the VM's service account
- `roles/artifactregistry.reader` — pull Docker images from Artifact Registry
- `roles/logging.logWriter` — write container logs to Cloud Logging

Grant them with:

```bash
SA=my-service-account@my-project.iam.gserviceaccount.com

for role in batch.jobsAdmin batch.agentReporter iam.serviceAccountUser \
            artifactregistry.reader logging.logWriter; do
    gcloud projects add-iam-policy-binding my-project \
        --member="serviceAccount:$SA" --role="roles/$role"
done
```

## Development

```bash
# Install in development mode with test dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/
```

## License

Apache 2.0
