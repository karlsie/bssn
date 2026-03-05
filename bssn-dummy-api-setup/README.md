# BSSN Dummy API Server

Mockup API server for testing Airflow DAGs without relying on external APIs. This server simulates the behavior of a real API, allowing you to test your Airflow workflows in a controlled environment.

# Setup Instructions

## Build and Run the API Server

1. Navigate to the `dummy-api-server` directory:

   ```bash
   cd dummy-api-server
   ```
2. build the docker image: 

    ```bash
    make build-test-api
    ```

3. Run the API server using Docker Compose in Airflow
