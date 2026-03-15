"""
Render a Mandelbrot set on Google Cloud Batch.

A CPU-intensive Prefect flow that renders a Mandelbrot fractal as a PNG,
demonstrating prefect-gcp-batch with real compute work on a Spot VM.

Usage:
    # Deploy and run via Cloud Batch (see examples/README.md for setup)
    prefect deployment run 'render-mandelbrot/cloud-batch'

    # Or run locally for testing
    python examples/mandelbrot_flow.py
"""

import base64
import io
import time

import numpy as np
from PIL import Image
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact


@task
def compute_mandelbrot(
    width: int,
    height: int,
    x_min: float,
    x_max: float,
    y_min: float,
    y_max: float,
    max_iterations: int,
) -> np.ndarray:
    """Compute the Mandelbrot set escape-time for each pixel.

    Returns an array of iteration counts (0 = in the set).
    """
    x = np.linspace(x_min, x_max, width)
    y = np.linspace(y_min, y_max, height)
    c = x[np.newaxis, :] + 1j * y[:, np.newaxis]

    z = np.zeros_like(c)
    iterations = np.zeros(c.shape, dtype=int)
    mask = np.ones(c.shape, dtype=bool)

    for i in range(1, max_iterations + 1):
        z[mask] = z[mask] ** 2 + c[mask]
        escaped = mask & (np.abs(z) > 2.0)
        iterations[escaped] = i
        mask &= ~escaped

    return iterations


@task
def colorize(iterations: np.ndarray, max_iterations: int) -> Image.Image:
    """Map iteration counts to colors using a cyclic gradient.

    Uses a log-scaled cyclic colormap so that detail is visible at all
    zoom levels, not just near the set boundary.
    """
    in_set = iterations == 0

    # Log-scale the iteration counts for perceptually uniform color spread,
    # then cycle through the palette so adjacent escape times get distinct colors.
    with np.errstate(divide="ignore", invalid="ignore"):
        t = np.where(in_set, 0, np.log(iterations.astype(float)))
    # Cycle period controls how fast colors repeat — lower = more bands
    cycle = 3.0
    t = (t % cycle) / cycle

    # HSV-style palette: cycle through hue, keep high saturation and value
    # Phase-shifted cosines give a smooth, vivid gradient
    r = 0.5 + 0.5 * np.cos(2 * np.pi * (t + 0.0))
    g = 0.5 + 0.5 * np.cos(2 * np.pi * (t + 0.33))
    b = 0.5 + 0.5 * np.cos(2 * np.pi * (t + 0.67))

    # Pixels in the set are black
    r[in_set] = 0
    g[in_set] = 0
    b[in_set] = 0

    rgb = np.stack([r, g, b], axis=-1)
    return Image.fromarray((rgb * 255).astype(np.uint8))


@flow(log_prints=True)
def render_mandelbrot(
    width: int = 4096,
    height: int = 4096,
    max_iterations: int = 1000,
    x_center: float = -0.75,
    y_center: float = 0.0,
    zoom: float = 1.0,
) -> str:
    """Render a Mandelbrot set fractal as a PNG image.

    Args:
        width: Image width in pixels.
        height: Image height in pixels.
        max_iterations: Escape-time iteration limit. Higher = more detail.
        x_center: Center of the view on the real axis.
        y_center: Center of the view on the imaginary axis.
        zoom: Zoom level. 1.0 = full set, higher = deeper zoom.
    """
    # Compute the view bounds from center + zoom
    aspect = width / height
    view_height = 2.5 / zoom
    view_width = view_height * aspect
    x_min = x_center - view_width / 2
    x_max = x_center + view_width / 2
    y_min = y_center - view_height / 2
    y_max = y_center + view_height / 2

    print(
        f"Rendering {width}x{height} Mandelbrot "
        f"({max_iterations} iterations, zoom={zoom}x)"
    )
    print(f"View: x=[{x_min:.4f}, {x_max:.4f}], y=[{y_min:.4f}, {y_max:.4f}]")

    t0 = time.perf_counter()
    iterations = compute_mandelbrot(
        width, height, x_min, x_max, y_min, y_max, max_iterations
    )
    compute_time = time.perf_counter() - t0
    print(f"Computed in {compute_time:.1f}s")

    image = colorize(iterations, max_iterations)

    pixels_in_set = int(np.sum(iterations == 0))
    total_pixels = width * height

    # Create a thumbnail for the Prefect artifact (keeps artifact size reasonable
    # even for 4K+ renders) and embed it as a data URI — no GCS bucket needed.
    thumbnail_size = 512
    thumb = image.copy()
    thumb.thumbnail((thumbnail_size, thumbnail_size))
    buf = io.BytesIO()
    thumb.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode()
    data_uri = f"data:image/png;base64,{b64}"
    print(f"Thumbnail: {len(b64) / 1024:.0f} KB base64")

    create_markdown_artifact(
        key="mandelbrot-result",
        markdown=(
            f"## Mandelbrot Render\n\n"
            f"![mandelbrot]({data_uri})\n\n"
            f"| | |\n|---|---|\n"
            f"| **Resolution** | {width} x {height} |\n"
            f"| **Iterations** | {max_iterations} |\n"
            f"| **Zoom** | {zoom}x at ({x_center}, {y_center}) |\n"
            f"| **Compute time** | {compute_time:.1f}s |\n"
            f"| **Pixels in set** | {pixels_in_set:,} / {total_pixels:,} "
            f"({pixels_in_set / total_pixels * 100:.1f}%) |\n"
        ),
    )

    return f"mandelbrot_{width}x{height}_z{zoom}"


if __name__ == "__main__":
    # Quick local test at lower resolution
    render_mandelbrot(width=1024, height=1024, max_iterations=500)
