import csv
import psycopg2

# Connect to your postgres DB
conn = psycopg2.connect("dbname=projet_modelisation user=postgres password=motdepasse")

# Open a cursor to perform database operations
cur = conn.cursor()

# Open the CSV file
with open('mariages\mariages_L3_5k.csv', 'r') as f:
    reader = csv.reader(f)
    # next(reader)  # Skip the header row
    for row in reader:
        # Assuming your table has columns: col1, col2, col3
        # And you only want to insert into col1 and col3
        cur.execute(
            "INSERT INTO test VALUES (%s)",
            (row[0],)  # row[0] corresponds to col1 and row[2] corresponds to col3
        )

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()