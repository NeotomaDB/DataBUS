import csv
import logging


def write_csv(data, filename="output.csv"):
    """Write CSV file from a structured list of dictionaries.
    """
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        if not filename.endswith(".csv"):
            logging.warning(f"Filename '{filename}' does not end with '.csv'. Adding '.csv' extension.")
            filename += ".csv"
        fieldnames = ["taxoncode", "taxonname", "author", "valid", "highertaxonid",
                      "extinct", "taxagroupid", "publicationid", "validatorid", 
                      "validatedate", "notes"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
