from DataBUS import insert_entityrelationship_to_db
import csv
import json
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
data = json.loads(os.getenv('PGDB_TANK'))

conn = psycopg2.connect(**data, connect_timeout = 5)
cur = conn.cursor()

with open("data/references_entities.csv", "r", newline="") as f:
    reader = csv.reader(f)
    for row in reader:
        # row order = ['entity', 'entitystatusid', 'reference']
        insert_entityrelationship_to_db(cur, row[0], row[1], row[2])