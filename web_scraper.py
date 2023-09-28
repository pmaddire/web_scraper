import requests
from bs4 import BeautifulSoup
import csv
import sys
from datetime import date
import threading
import os
import glob
import pandas as pd

# Global variables for storing scraped data
# fin_urls = []
# fin_dates = []
# fin_titles = []

def scrap(url):
    try:
        reqs = requests.get(url)
        reqs.raise_for_status()  # Raise an exception for non-2xx HTTP responses
        soup = BeautifulSoup(reqs.text, 'html.parser')
        for title in soup.find_all('title'):
            return title.get_text()
    except requests.exceptions.RequestException as e:
        print(f"Error scraping {url}: {e}")
        return None

# Check if the correct number of command-line arguments is provided
if len(sys.argv) != 4:
    print("Usage: python script.py input_file.csv output_file.csv thread amount")
    sys.exit(1)

input_file = sys.argv[1]
output_folder = sys.argv[2]
threads = sys.argv[3]
threads = int(threads)
def thread_start(urls_sub, i, my_path):

    titles = []
    dates = []
    urls = []
    for url in urls_sub:
        title = scrap(url)
        if title is not None:
            urls.append(url)
            titles.append(title)
            day = date.today()
            dates.append(day)

    with open(os.path.join(my_path, f'{i}.csv'), 'w', newline='', encoding='utf-8') as csv_output:
        csv_writer = csv.writer(csv_output)
        csv_writer.writerow(["Title", "URL", "Date"])  # Write header
        for title, url, dateT in zip(titles, urls, dates):
            csv_writer.writerow([title, url, dateT])

urls = []


my_path = output_folder

try:
    os.makedirs(my_path, exist_ok=True)  # creating directory
except OSError as error:  # dodging error if directory already exists
    t = 1

# Open the input CSV file for reading
with open(input_file, 'r', encoding='utf-8') as csv_file:
    csv_reader = csv.reader(csv_file)

    # Loop through each row in the CSV file
    for row in csv_reader:
        if row:  # Check if the row is not empty
            url = "https://" + str(row[0])  # Use the URL directly from the CSV file
            urls.append(url)
            print(url)

# Calculate the number of URLs per thread
url_amount = len(urls)
urls_per_thread = url_amount // threads
urls_excess = url_amount % threads

# Create and start threads
thread_list = []

start_index = 0
for i in range(threads):
    end_index = start_index + urls_per_thread+1
    if urls_excess > 0:
        end_index += 1
        urls_excess -= 1

    urls_sub = urls[start_index:end_index]
    thread = threading.Thread(target=thread_start, args=(urls_sub, i, my_path))
    thread.start()
    thread_list.append(thread)

    start_index = end_index

# Wait for all threads to finish
for thread in thread_list:
    thread.join()

# Open the output CSV file for writing

print(f'Titles, URLs, and Dates scraped and saved to {output_folder}')
# List all CSV files in the directory
csv_files = glob.glob(os.path.join(my_path, "*.csv"))

# Create an empty DataFrame to store the merged data
merged_data = pd.DataFrame()

for csv_file in csv_files:
    df = pd.read_csv(csv_file)
    merged_data = pd.concat([merged_data, df], ignore_index=True)


# Remove duplicates from the merged data (optional)
merged_data = merged_data.drop_duplicates()

# Path to the merged CSV file
merged_csv_file = "merged_data.csv"

# Save the merged data to a single CSV file
merged_data.to_csv(os.path.join(my_path, merged_csv_file), index=False)

print(f'Merged data saved to {merged_csv_file}')
