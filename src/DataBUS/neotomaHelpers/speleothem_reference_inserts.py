from DataBUS import insert_entityrelationship_to_db
import csv

def speleothem_reference_inserts(cur, conn, file_path="data/references_entities.csv"):
    """Insert speleothem reference entity relationships into database.

    Reads a CSV file of entity relationships and inserts them into the database,
    skipping duplicates. Queries the database to check for existing entries before
    insertion and rolls back on errors.

    Examples:
        >>> speleothem_reference_inserts(cursor, connection)  # doctest: +SKIP
        # Inserts records from data/references_entities.csv

    Args:
        cur: Database cursor object for executing queries.
        conn: Database connection object for commit/rollback operations.
        file_path (str, optional): Path to the CSV file containing entity relationships.
                                   Expected columns: entityid, entitystatusid, referenceentityid.
                                   Defaults to 'data/references_entities.csv'.

    Returns:
        None
    """
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
                    insert_entityrelationship_to_db(cur, row[0], row[1], row[2])
                    # SUGGESTION: Should this call conn.commit() instead of conn.rollback() after successful insert?
                    conn.rollback()
                except Exception as e:
                    # SUGGESTION: Log the exception (e) for debugging purposes
                    conn.rollback()