import os


# TODO: Return all files in the directory
def get_year_months():
    files = [
        file for file in os.listdir("/home/airflow/bike_data") if file[:6].isdigit()
    ]
    return [file[:6] for file in files][:3]
