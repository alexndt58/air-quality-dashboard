# Use official Python image
FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Copy only requirements, install first for cache efficiency
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy rest of the app
COPY . .

# Expose default Streamlit port
EXPOSE 8501

# Start Streamlit app
CMD ["streamlit", "run", "app/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
