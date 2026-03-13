# Use an official Python slim image for a smaller footprint
FROM python:3.10-slim

# Install system dependencies (ffmpeg) and clean up apt cache to keep image small
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy requirements and install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Start the bot worker
CMD ["python", "bot.py"]
