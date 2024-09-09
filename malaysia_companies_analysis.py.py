import logging
import pandas as pd
from bs4 import BeautifulSoup
import requests
import sqlite3

# Function to log progress messages
def log_progress(message):
    logging.basicConfig(filename='code_log.txt', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(message)

# Function to extract data from the webpage
def extract(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'class': 'wikitable sortable'})  # Adjusted to match Wikipedia's table class
    rows = table.find_all('tr')

    data = []
    for row in rows:
        cols = row.find_all(['td', 'th'])  # Includes headers for column names
        cols = [ele.text.strip() for ele in cols]
        if cols:
            data.append([ele for ele in cols if ele])  # Remove empty values

    return data

# Function to transform the extracted data
def transform(data):
    # First row is assumed to be headers, modify if needed
    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)  # Skip the first row since it's headers
    
    # Print column names for debugging
    print("Column Names:", df.columns.tolist())

    # Example transformations: Removing unwanted characters and converting to appropriate types
    if 'Revenue' in df.columns:
        df['Revenue'] = df['Revenue'].str.replace('[\$,]', '', regex=True).astype(float)
    if 'Profit' in df.columns:
        df['Profit'] = df['Profit'].str.replace('[\$,]', '', regex=True).astype(float)

    # Add any other necessary transformations
    return df

# Function to save DataFrame to CSV
def load_to_csv(dataframe, filename):
    dataframe.to_csv(filename, index=False)
    log_progress(f'Data saved to {filename}')

# Function to save DataFrame to a database
def load_to_db(dataframe, db_name):
    conn = sqlite3.connect(db_name)
    dataframe.to_sql('financial_metrics', conn, if_exists='replace', index=False)
    conn.close()
    log_progress(f'Data loaded to {db_name} database')

# Function to run SQL queries and print results
def run_queries(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Get and print actual column names for debugging
    cursor.execute("PRAGMA table_info(financial_metrics)")
    columns_info = cursor.fetchall()
    column_names = [info[1] for info in columns_info]  # Extract column names from table info
    print("Actual Column Names in Database:", column_names)

    # Example SQL query: Modify the column name based on the actual data
    try:
        cursor.execute("SELECT * FROM financial_metrics WHERE `Revenue` > 10000")  # Modify if column names differ
        results = cursor.fetchall()
        for row in results:
            print(row)
    except sqlite3.OperationalError as e:
        print("SQL Error:", e)

    conn.close()

# Main execution
if __name__ == "__main__":
    # Task 1: Log function
    log_progress('Started web scraping process.')

    # Task 2: Extract data
    url = 'https://en.wikipedia.org/wiki/List_of_largest_companies_in_Malaysia'
    data = extract(url)
    print("Extracted Data:", data[:5])  # Displaying only first 5 rows for brevity

    # Task 3: Transform data
    transformed_df = transform(data)
    print("Transformed DataFrame:\n", transformed_df.head())  # Displaying first 5 rows

    # Task 4: Save to CSV
    csv_filename = 'financial_metrics_malaysia.csv'
    load_to_csv(transformed_df, csv_filename)

    # Display CSV contents for screenshot
    with open(csv_filename, 'r') as file:
        print("CSV Contents:\n", file.read())

    # Task 5: Save to Database
    db_name = 'financial_metrics_malaysia.db'
    load_to_db(transformed_df, db_name)

    # Task 6: SQL Query Outputs
    run_queries(db_name)

    # Task 7: Display log file contents
    with open('code_log.txt', 'r') as log_file:
        print("Log File Contents:\n", log_file.read())
