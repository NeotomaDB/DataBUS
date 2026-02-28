import os
import json
import psycopg2
import datetime
import glob
from dotenv import load_dotenv
from tqdm import tqdm
import DataBUS.neotomaValidator as nv
import DataBUS.neotomaHelpers as nh
from DataBUS.neotomaHelpers.logging_dict import logging_response

"""Example script demonstrating the use of DataBUS functions.
This script serves as an example of how to use the DataBUS library to validate and upload data to a Neotoma database.
It includes steps for hashing files, checking validation logs, and validating various data types such as sites, geopolitical units, collection units, etc.
The script reads data from specified CSV files, validates the data against the Neotoma database, and logs the validation process.
It also includes error handling to ensure that any issues during validation are properly logged and that database transactions
are rolled back if necessary.

The script should be run twice, once with the --upload flag set to False to perform validation and generate logs, and a second time with the --upload flag set to True to upload the validated data to the database.

Run with uv
Example usage:
    uv run databus_example.py --data data/ --template template.yml --logs data/logs/ --upload False
    uv run databus_example.py --data data/ --template template.yml --logs data/logs/ --upload True
"""

args = nh.parse_arguments()
# Load environment variables from .env file.
# Look at .env_example for expected format.
# This should be renamed to .env and updated with the appropriate database connection
# information for your environment.
load_dotenv()
connection = json.loads(os.getenv('PGDB_TANK'))

# Load YAML template and CSV files
filenames = glob.glob(args['data'] + "*.csv")
yml_dict = nh.template_to_dict(temp_file=args['template'])

# Connect to the PostgreSQL database using psycopg2
conn = psycopg2.connect(**connection, connect_timeout=5)
cur = conn.cursor()

start_time = datetime.now()
print(f"Start uploading at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

for filename in tqdm(filenames, desc="Files", unit="file"):
    conn.rollback()
    logfile = []
    databus = dict()

    csv_file = nh.read_csv(filename)
    hashcheck = nh.hash_file(filename)
    filecheck = nh.check_file(filename, validation_files="data/")

    logfile = logfile + hashcheck['message'] + filecheck['message']
    logfile.append(f"\nNew Upload started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if hashcheck['pass'] is False and filecheck['pass'] is False:
        logfile.append("File must be properly validated before it can be uploaded.")
        hashcheck = False
    else:
        hashcheck = True

    try:
        step_bar = tqdm(total=16, desc=os.path.basename(filename), leave=False, unit="step")

        # Not all steps are required for every upload.
        # This is only an example of how to run DataBUS. 
        # Modify the steps as needed for your specific use case.
        logfile.append(f"=== Sites ===")
        result = nh.safe_step("sites", lambda: nv.valid_site(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file), logfile, conn)
        if result is not None:
            databus['sites'] = result
            logfile = logging_response(databus['sites'], logfile)
        step_bar.update(1)

        logfile.append(f"=== GPUs ===")
        result = nh.safe_step("gpus", lambda: nv.valid_geopolitical_units(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus), logfile, conn)
        if result is not None:
            databus['gpuid'] = result
            logfile = logging_response(databus['gpuid'], logfile)
        step_bar.update(1)

        logfile.append(f"=== CUs ===")
        result = nh.safe_step("collunits", lambda: nv.valid_collunit(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus), logfile, conn)
        if result is not None:
            databus['collunits'] = result
            logfile = logging_response(databus['collunits'], logfile)
        step_bar.update(1)

        logfile.append(f"=== Analysis Units ===")
        result = nh.safe_step("analysisunits", lambda: nv.valid_analysisunit(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus), logfile, conn)
        if result is not None:
            databus['analysisunits'] = result
            logfile = logging_response(databus['analysisunits'], logfile)
        step_bar.update(1)

        logfile.append(f"=== Datasets ===")
        result = nh.safe_step("datasets", lambda: nv.valid_dataset(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus), logfile, conn)
        if result is not None:
            databus['datasets'] = result
            logfile = logging_response(databus['datasets'], logfile)
        step_bar.update(1)

        logfile.append(f"=== Geochron Datasets ===")
        result = nh.safe_step("geodataset", lambda: nv.valid_geochron_dataset(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus), logfile, conn)
        if result is not None:
            databus['geodataset'] = result
            logfile = logging_response(databus['geodataset'], logfile)
        step_bar.update(1)

        logfile.append(f"=== Chronologies ===")
        result = nh.safe_step("chronologies", lambda: nv.valid_chronologies(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus), logfile, conn)
        if result is not None:
            databus['chronologies'] = result
            logfile = logging_response(databus['chronologies'], logfile)
        step_bar.update(1)

        logfile.append(f"=== Chron Controls ===")
        result = nh.safe_step("chron_controls", lambda: nv.valid_chroncontrols(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus), logfile, conn)
        if result is not None:
            databus['chron_controls'] = result
            logfile = logging_response(databus['chron_controls'], logfile)
        step_bar.update(1)

        logfile.append(f"=== Geochron ===")
        result = nh.safe_step("geochron", lambda: nv.valid_geochron(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus), logfile, conn)
        if result is not None:
            databus['geochron'] = result
            logfile = logging_response(databus['geochron'], logfile)
        step_bar.update(1)

        logfile.append(f"=== Geochron Control ===")
        result = nh.safe_step("geochroncontrol", lambda: nv.valid_geochroncontrol(
            cur=cur, databus=databus), logfile, conn)
        if result is not None:
            databus['geochroncontrol'] = result
        step_bar.update(1)

        logfile.append(f"=== Contacts ===")
        result = nh.safe_step("contacts", lambda: nv.valid_contact(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus), logfile, conn)
        if result is not None:
            databus['contacts'] = result
            logfile = logging_response(databus['contacts'], logfile)
        step_bar.update(1)

        logfile.append(f"=== Database ===")
        result = nh.safe_step("database", lambda: nv.valid_dataset_database(
            cur=cur, yml_dict=yml_dict, databus=databus), logfile, conn)
        if result is not None:
            databus['database'] = result
            logfile = logging_response(databus['database'], logfile)
        step_bar.update(1)

        logfile.append(f"=== Samples ===")
        result = nh.safe_step("samples", lambda: nv.valid_sample(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus), logfile, conn)
        if result is not None:
            databus['samples'] = result
            logfile = logging_response(databus['samples'], logfile)
        step_bar.update(1)

        logfile.append(f"=== Sample Ages ===")
        result = nh.safe_step("sample_age", lambda: nv.valid_sample_age(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus), logfile, conn)
        if result is not None:
            databus['sample_age'] = result
            logfile = logging_response(databus['sample_age'], logfile)
        step_bar.update(1)

        logfile.append(f"=== Data ===")
        result = nh.safe_step("data", lambda: nv.valid_data(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus), logfile, conn)
        if result is not None:
            databus['data'] = result
            logfile = logging_response(databus['data'], logfile)
        step_bar.update(1)

        logfile.append(f"=== Publications ===")
        result = nh.safe_step("publications", lambda: nv.valid_publication(
            cur=cur, yml_dict=yml_dict, csv_file=csv_file, databus=databus), logfile, conn)
        if result is not None:
            databus['publications'] = result
            logfile = logging_response(databus['publications'], logfile)
        step_bar.update(1)

        step_bar.close()

        # Inserting Finalize
        # databus['finalize'] = nv.insert_final(cur,
        #                                       databus = databus)
        all_true = all([databus[key].validAll for key in databus])
        all_true = all_true and hashcheck
        if args.upload:
            if all_true:
                conn.commit()
                logfile.append("Data has been successfully uploaded to the database.")
            else:
                conn.rollback()
                logfile.append("Data must be fully validated before it can be uploaded to the database.")
        else:
            if all_true:
                conn.rollback()
                logfile.append("Data has been fully validated and is ready for upload.")
            else:
                conn.rollback()
                logfile.append("Data has not passed validation. Please review the log messages for details.")
    except Exception as e:
        conn.rollback()
        logfile.append(f"An error occurred during validation: {str(e)}")
    with open(filename + '.valid.log', 'w', encoding="utf-8") as writer:
        for i in logfile:
            writer.write(i)
            writer.write('\n')
