# Use the official Python image
FROM python:3.11

# Set the working directory inside the container
WORKDIR /app

# Copy requirements file first (leveraging Docker caching)
COPY requirements.txt /app/

# Install dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Now copy the rest of the application files
COPY . /app

# Ensure correct permissions
RUN chmod -R 755 /app

# Expose FastAPI port
EXPOSE 8000


# Run the FastAPI application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
