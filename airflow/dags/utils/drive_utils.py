import requests
import pandas as pd
from datetime import datetime


def download_file_from_only_office(file, drive_url, conn_username, conn_password):
    url = f"{drive_url}/{file}"

    print(f"Downloading file from OnlyOffice: {url}")
    response = requests.get(url, auth=(conn_username, conn_password))

    response.raise_for_status()

    with open(f"/tmp/{file}", "wb") as f:
        f.write(response.content)

    print(f"File downloaded successfully: /tmp/{file}")


def read_file_from_only_office(downloaded_file_path):
    """Read CSV, XLSX, or XLS files and add created_at timestamp."""
    # Determine file type by extension
    if downloaded_file_path.endswith('.csv'):
        df = pd.read_csv(downloaded_file_path)
    elif downloaded_file_path.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(downloaded_file_path)
    else:
        raise ValueError(f"Unsupported file format: {downloaded_file_path}. Supported: .csv, .xlsx, .xls")
    
    # Add created_at timestamp
    df['created_at'] = datetime.now()
    
    print(f"File read successfully: {downloaded_file_path}")
    return df
