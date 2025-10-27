# Dockerfile

# Use a lightweight and stable Python base
FROM python:3.10-slim

# Set default environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

# Set the working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire source code (including src/)
COPY . /app

# Startup command. Runs the MCP server
CMD ["python", "-m", "vsphere_mcp_server.server"]
