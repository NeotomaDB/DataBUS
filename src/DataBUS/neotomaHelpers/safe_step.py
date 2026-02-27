def safe_step(name, fn, logfile, conn):
    """Run a single validation step inside a savepoint.

    Returns the Response on success; on any exception it rolls back to the
    savepoint, appends an error message to logfile, and returns None so that
    downstream steps can be skipped gracefully rather than crashing.

    Args:
        name (str): Human-readable step name (for log messages).
        fn (callable): Zero-argument callable that executes the step and
            returns a Response.
        logfile (list[str]): Accumulated log lines.
        conn: psycopg2 connection (used for savepoint management).

    Returns:
        Response | None
    """
    sp = f"sp_{name.replace(' ', '_').lower()}"
    try:
        conn.execute(f"SAVEPOINT {sp}")
    except Exception:
        # If we can't set the savepoint the connection may already be aborted;
        # try a full rollback and re-open a savepoint.
        try:
            conn.rollback()
            conn.execute(f"SAVEPOINT {sp}")
        except Exception:
            logfile.append(f"✗ [{name}] Could not establish savepoint – skipping step.")
            return None
    try:
        result = fn()
        conn.execute(f"RELEASE SAVEPOINT {sp}")
        return result
    except Exception as e:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            conn.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception:
            conn.rollback()
        logfile.append(f"✗ [{name}] Unexpected error (rolled back): {e}")
        return None
