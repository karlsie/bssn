# Use official Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements (we'll create one) first for caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app code
COPY . .

# Expose port 8000
EXPOSE 8000

# Command to run the FastAPI app with Uvicorn
CMD ["uvicorn", "dummy_api:app", "--host", "0.0.0.0", "--port", "8000"]
