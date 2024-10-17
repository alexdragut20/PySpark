"""
This script downloads the "Online Retail" dataset from the UCI Machine Learning Repository using Python,
as required in the task. The dataset is saved as an Excel file locally.

Approach:
- The dataset URL is specified and the file is downloaded using the requests library.
- The file is saved to a specified directory, creating the directory if it does not exist.

Assumptions:
- The dataset URL is valid and accessible.
- The file will be saved in the 'data/raw/' directory as an Excel file.
"""
import requests
import os

dataset_url = "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"
local_filename = "../../data/raw/online_retail.xlsx"

# Create the output directory if not found
os.makedirs(os.path.dirname(local_filename), exist_ok=True)

# Download the dataset using requests
response = requests.get(dataset_url)
with open(local_filename, 'wb') as file:
    file.write(response.content)

print(f"Dataset downloaded and saved as {local_filename}")
