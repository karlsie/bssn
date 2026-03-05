# BSSN

Top‑level repository for BSSN data platform infrastructure. This
mono‑repo contains two primary components:

- [Airflow orchestration](/airflow/) – DAGs, utilities, and
  Docker configuration used to run ETL pipelines.
- [Dummy API server](/bssn-dummy-api-setup/) – standalone
  FastAPI mock service that provides fake endpoints for testing the
  Airflow workflows.

Each subdirectory has its own README with setup instructions and
usage details; click the links above to get started.
