# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Avoid buffering Python stdout/stderr
ENV PYTHONUNBUFFERED=1

# set working directory
WORKDIR /app

# Copy requirements and install them
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Make entrypoint executable
RUN chmod +x ./entrypoint.sh

# Expose Streamlit port
EXPOSE 8501

# Default entrypoint
ENTRYPOINT ["./entrypoint.sh"]
