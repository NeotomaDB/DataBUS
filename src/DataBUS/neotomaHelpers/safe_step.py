import os
import traceback

import psycopg2
from psycopg2 import errors as pg_errors
from psycopg2 import extensions as pg_extensions

from DataBUS.Response import Response


def _where(e):
    """Return ' (raised at file:line in function)' for the frame that raised e."""
    frames = traceback.extract_tb(e.__traceback__)
    if not frames:
        return ""
    frame = frames[-1]
    return f" (raised at {os.path.basename(frame.filename)}:{frame.lineno} in {frame.name})"


def _describe_error(e):
    """Turn an exception into a log line that says what failed and where.

    Database errors are unpacked from the Postgres diagnostics so the message
    names the table, column or constraint involved instead of only the error
    text. Any other exception reports its type, message and source location.
    """
    if isinstance(e, pg_errors.InFailedSqlTransaction):
        return (
            "A database statement failed earlier in this step and its error was "
            "not reported, so Postgres refused the commands that followed it."
            f"{_where(e)}"
        )
    if isinstance(e, psycopg2.Error):
        diag = getattr(e, "diag", None)
        primary = getattr(diag, "message_primary", None) or str(e).strip()
        parts = [f"Database error ({type(e).__name__}): {primary}"]
        for label, attr in (
            ("Detail", "message_detail"),
            ("Hint", "message_hint"),
            ("Table", "table_name"),
            ("Column", "column_name"),
            ("Constraint", "constraint_name"),
            ("Context", "context"),
        ):
            value = getattr(diag, attr, None)
            if value:
                parts.append(f"{label}: {value}")
        return " | ".join(parts) + _where(e)
    return f"{type(e).__name__}: {e}{_where(e)}"


def _in_failed_transaction(conn):
    """True when Postgres has aborted the current transaction."""
    try:
        return conn.get_transaction_status() == pg_extensions.TRANSACTION_STATUS_INERROR
    except Exception:
        return False


def _undo_step(sp_cur, sp, conn):
    """Roll back to the step savepoint, or the whole transaction if that fails."""
    try:
        sp_cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
        sp_cur.execute(f"RELEASE SAVEPOINT {sp}")
    except Exception:
        conn.rollback()


def _failed_response(msg):
    response = Response()
    response.valid.append(False)
    response.message.append(msg)
    return response


def safe_step(name, fn, logfile, conn):
    """Run a single validation step inside a savepoint.

    Returns the Response on success. Two kinds of failure are handled, and in
    both the step's database work is rolled back to the savepoint so later
    steps can still run:

    * fn() raises: a failed Response is returned whose message describes the
      error (Postgres table/column/constraint for database errors, otherwise
      the exception type and where it was raised).
    * fn() returns, but a database error it caught itself has left the
      transaction aborted: the Response fn() built is kept, since it holds the
      real reason, and is marked invalid with a note that the step's inserts
      were rolled back.

    The error is carried in the returned Response (never None), so callers
    should log that Response rather than expect a separate logfile entry.

    Args:
        name (str): Human-readable step name (for log messages).
        fn (callable): Zero-argument callable that executes the step and
            returns a Response.
        logfile (list[str]): Accumulated log lines. Only written to when the
            savepoint cannot be established.
        conn: psycopg2 connection (used for savepoint management).

    Returns:
        Response: the fn() result, or a failed Response whose message contains
            the error detail when fn() raised.
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
            return _failed_response(msg)
    try:
        result = fn()
    except Exception as e:
        _undo_step(sp_cur, sp, conn)
        return _failed_response(f"✗ [{name}] Step stopped and was rolled back. {_describe_error(e)}")

    if _in_failed_transaction(conn):
        # fn() caught a database error itself. Releasing the savepoint would
        # fail here and hide the messages fn() collected, so undo the step and
        # hand those messages back.
        _undo_step(sp_cur, sp, conn)
        msg = (
            f"✗ [{name}] A database statement failed in this step (see the first ✗ "
            "above for the reason). Everything this step inserted was rolled back, "
            "so later steps that need its IDs will fail too."
        )
        if isinstance(result, Response):
            result.valid.append(False)
            result.message.append(msg)
            return result
        return _failed_response(msg)

    try:
        sp_cur.execute(f"RELEASE SAVEPOINT {sp}")
    except Exception as e:
        _undo_step(sp_cur, sp, conn)
        msg = f"✗ [{name}] Could not close the step savepoint. {_describe_error(e)}"
        if isinstance(result, Response):
            result.valid.append(False)
            result.message.append(msg)
            return result
        return _failed_response(msg)
    return result
