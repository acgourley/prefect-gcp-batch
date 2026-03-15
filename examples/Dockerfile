FROM python:3.12-slim

# Install the worker package and example dependencies
COPY . /app
WORKDIR /app
RUN pip install --no-cache-dir . numpy pillow

# Prefect needs this to execute flow runs remotely
CMD ["prefect", "flow-run", "execute"]
