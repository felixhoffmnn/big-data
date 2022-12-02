import os


# Function to get the year month list
def get_year_months():
    files = [file for file in os.listdir("/home/airflow/bike_data") if file[:6].isdigit()]

    # To reduce the number of files being processed, add a slice to the list
    return [file[:6] for file in files]
