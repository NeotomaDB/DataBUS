from DataBUS.Response import Response


def safe_step(name, fn, logfile, conn):
    """Run a single validation step inside a savepoint.

    Returns the Response on success; on any exception it rolls back to the
    savepoint, appends an error message to logfile, and returns a failed
    Response (never None) so that downstream steps can inspect messages
    rather than receiving a bare None.

    Args:
        name (str): Human-readable step name (for log messages).
        fn (callable): Zero-argument callable that executes the step and
            returns a Response.
        logfile (list[str]): Accumulated log lines.
        conn: psycopg2 connection (used for savepoint management).

    Returns:
        Response: the fn() result on success, or a failed Response whose
            message contains the error detail on failure.
    """
    sp = f"sp_{name.replace(' ', '_').lower()}"
    sp_cur = conn.cursor()
    try:
        sp_cur.execute(f"SAVEPOINT {sp}")
    except Exception:
        # If we can't set the savepoint the connection may already be aborted;
        # try a full rollback and re-open a savepoint.
        try:
            conn.rollback()
            sp_cur = conn.cursor()
            sp_cur.execute(f"SAVEPOINT {sp}")
        except Exception:
            msg = f"✗ [{name}] Could not establish savepoint – skipping step."
            logfile.append(msg)
            response = Response()
            response.valid.append(False)
            response.message.append(msg)
            return response
    try:
        result = fn()
        sp_cur.execute(f"RELEASE SAVEPOINT {sp}")
        return result
    except Exception as e:
        try:
            sp_cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            sp_cur.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception:
            conn.rollback()
        msg = f"✗ [{name}] Unexpected error (rolled back): {e}"
        logfile.append(msg)
        # Import Response here to avoid any module state issues
        from DataBUS.Response import Response as ResponseClass
        response = ResponseClass()
        response.valid.append(False)
        response.message.append(msg)
        return response
