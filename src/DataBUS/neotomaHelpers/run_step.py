from .logging_dict import logging_response
from .safe_step import safe_step


def run_step(name, key, fn, logfile, databus, conn):
    """Run one validation step in its own savepoint and log the Response.

    `fn` must be a zero-argument callable. `safe_step` rolls back to the
    savepoint on error so a single failing step no longer aborts the whole
    transaction and poisons every step after it.

    Args:
        name (str): Title written to the log, e.g. "Sites".
        key (str): Key under which the Response is stored in ``databus``. Also names the savepoint.
        fn (callable): Zero-argument callable that executes the step and returns a Response.
        logfile (list[str]): Accumulated log lines; the title and the Response are appended.
        databus (dict): Prior validation results; the Response is stored in ``databus[key]``.
        conn: psycopg2 connection (used for savepoint management).

    Examples:
        >>> run_step("Sites", "sites", lambda: valid_site(cur, yml_dict, csv_file),
        ...          logfile, databus, conn)  # doctest: +SKIP
    """
    logfile.append(f"\n === {name} ===")
    result = safe_step(key, fn, logfile, conn)
    if result is not None:
        databus[key] = result
        logging_response(result, logfile)
