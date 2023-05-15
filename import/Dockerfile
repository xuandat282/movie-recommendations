# syntax=docker/dockerfile:1.4
FROM --platform=$BUILDPLATFORM python:3.10-alpine AS builder

# Set the working directory to /app
WORKDIR /import

# Copy the current directory contents into the container at /app
ADD . /import

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Run import.py when the container launches
CMD ["python","import.py"]