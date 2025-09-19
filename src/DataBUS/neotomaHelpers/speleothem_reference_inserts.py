from DataBUS import insert_entityrelationship_to_db
import csv
def speleothem_reference_inserts(cur, conn, file_path="data/references_entities.csv"):
    with open(file_path, "r", newline="") as f:
        reader = csv.reader(f)
        next(reader)
        seen = set()
        for row in reader:
            row_tuple = tuple(row)
            if row_tuple in seen:
                continue
            seen.add(row_tuple)
            query = """SELECT *
                       FROM ndb.entityrelationship
                       WHERE entityid = %(entityid)s
                         AND entitystatusid = %(entitystatusid)s
                         AND referenceentityid = %(referenceentityid)s"""
            cur.execute(query, {'entityid': row[0],
                                'entitystatusid': row[1],
                                'referenceentityid': row[2]})
            result = cur.fetchone()
            if result:
                seen.add(row_tuple)
                continue
            else:
                try:
                    print(f"inserting row: {row}")
                    insert_entityrelationship_to_db(cur, row[0], row[1], row[2])
                    conn.rollback()
                except Exception as e:
                    print(f"Error inserting row {row}: {e}")
                    conn.rollback()