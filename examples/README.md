# Example: Mandelbrot Set on Cloud Batch

Render a 4K Mandelbrot fractal on a Google Cloud Batch VM.

This example demonstrates the full prefect-gcp-batch lifecycle:
a Prefect flow runs on a GCE VM, does real CPU work, and reports
results back to Prefect Cloud as an artifact with the rendered
fractal embedded inline.

## Prerequisites

- A GCP project with billing enabled
- [Artifact Registry](https://console.cloud.google.com/artifacts) Docker
  repository (or use an existing one)
- A [Prefect Cloud](https://app.prefect.cloud) account (free tier works)
- CLI tools: `gcloud`, `docker`, `prefect`
- `gcloud` authenticated: `gcloud auth login`

## Step 1: Enable the Cloud Batch API

Cloud Batch is not enabled by default:

```bash
gcloud services enable batch.googleapis.com --project=YOUR_PROJECT
```

## Step 2: Set up IAM roles

The service account used by Cloud Batch needs three roles. Without all
three, jobs will either fail to create, get stuck, or fail to report:

```bash
export SA=your-service-account@your-project.iam.gserviceaccount.com
export PROJECT=your-project

# All five roles are required — missing any one causes silent failures
for role in batch.jobsAdmin batch.agentReporter iam.serviceAccountUser \
            artifactregistry.reader logging.logWriter; do
    gcloud projects add-iam-policy-binding $PROJECT \
        --member="serviceAccount:$SA" --role="roles/$role"
done
```

## Step 3: Authenticate Docker with Artifact Registry

```bash
gcloud auth configure-docker us-central1-docker.pkg.dev
```

Replace `us-central1` with your Artifact Registry region.

## Step 4: Build and push the Docker image

Build from the **package root** (one directory up from `examples/`):

```bash
cd prefect-gcp-batch/

export GCP_PROJECT=your-project-id
export GCP_REGION=us-central1
export IMAGE=$GCP_REGION-docker.pkg.dev/$GCP_PROJECT/docker/mandelbrot-example:latest

# buildx with --platform is required on Apple Silicon to produce x86 images
docker buildx build --platform linux/amd64 --push -t $IMAGE -f examples/Dockerfile .
```

## Step 5: Create and configure the work pool

```bash
prefect work-pool create 'batch-example' -t gcp-batch
```

Configure it in the [Prefect Cloud UI](https://app.prefect.cloud)
(Work Pools > batch-example > Edit):

| Variable | Value |
|----------|-------|
| credentials | your `GcpCredentials` block (create one at Blocks > + > GCP Credentials) |
| region | `us-central1` (or your preferred region) |
| image | the `$IMAGE` URI you pushed above |
| machine_type | `e2-standard-4` |
| boot_disk_size_gb | `30` (minimum required by Cloud Batch's container OS) |

## Step 6: Create a deployment

```bash
prefect deploy -n render-mandelbrot/mandelbrot-example -p batch-example \
    --prefect-file examples/prefect.yaml
```

Or from Python:

```python
from examples.mandelbrot_flow import render_mandelbrot

render_mandelbrot.deploy(
    name="mandelbrot-example",
    work_pool_name="batch-example",
    image="us-central1-docker.pkg.dev/YOUR_PROJECT/docker/mandelbrot-example:latest",
)
```

## Step 7: Start the worker

The worker runs locally and dispatches flow runs to Cloud Batch.
It doesn't do the compute — it manages the infrastructure lifecycle.

```bash
prefect worker start -p 'batch-example'
```

Leave this running in a terminal.

## Step 8: Run it

In another terminal:

```bash
# Default: 4096x4096, 1000 iterations
prefect deployment run 'render-mandelbrot/mandelbrot-example'

# Or with custom parameters
prefect deployment run 'render-mandelbrot/mandelbrot-example' \
    --param width=4096 \
    --param height=4096 \
    --param max_iterations=2000 \
    --param zoom=50 \
    --param x_center=-0.7435669 \
    --param y_center=0.1314023
```

Watch the flow run in the Prefect Cloud UI. You'll see:

1. Cloud Batch job state transitions in the logs
   (QUEUED > SCHEDULED > RUNNING > SUCCEEDED)
2. Compute output ("Rendering 4096x4096 Mandelbrot...")
3. An **Artifacts** tab with the rendered fractal and stats table

## Trying different machine types

Edit the work pool variables in the Prefect Cloud UI:

| Scenario | machine_type | Approx. cost |
|----------|-------------|--------------|
| Cheap test | `e2-medium` | ~$0.003 |
| Default (4K) | `e2-standard-4` | ~$0.01 |
| 8K deep zoom | `c3d-highcpu-16` | ~$0.04 |
| Maximum detail | `c3d-highcpu-44` | ~$0.10 |

NumPy parallelizes across available cores, so bigger machines
directly translate to faster renders.

## Running locally

Test the flow without any cloud infrastructure:

```bash
pip install prefect numpy pillow
python examples/mandelbrot_flow.py
```

This renders a 1024x1024 image locally.
