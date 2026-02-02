import hashlib
import os

def hash_file(filename, validation_files="data/validation_logs/"):
    """Calculate MD5 hash of a file and compare against validation logs.

    Computes the MD5 hash of a file and compares it with previously stored hashes
    in validation log files to determine if the file has been validated or modified.

    Examples:
        >>> hash_file('pollen_data.csv')  # doctest: +SKIP
        {'pass': True, 'hash': 'abc123def456...', 'message': ['abc123def456...', 'Hashes match, file hasn\'t changed.']}
        >>> hash_file('chronology_template.xlsx')  # doctest: +SKIP
        {'pass': False, 'hash': 'xyz789abc123...', 'message': ['xyz789abc123...', 'File has changed, validating chronology_template.xlsx.']}

    Args:
        filename (str): Path to the file to hash.
        validation_files (str, optional): Path to the validation logs directory.
                                          Defaults to 'data/validation_logs/'.

    Returns:
        dict: Dictionary with keys:
              'pass' (bool): True if file matches validation log hash.
              'hash' (str): MD5 hash of the file in hexadecimal.
              'message' (list): List of status messages about validation result.
    """
    response = {"pass": False, "hash": None, "message": []}
    modified_filename = os.path.basename(filename)
    logfile = f"{validation_files}{modified_filename}"+ ".valid.log"
    not_val_logfile = f"{validation_files}not_validated/{modified_filename}"+ ".valid.log"
    # SUGGESTION: Use a context manager (with statement) to ensure file handle is properly closed
    response["hash"] = hashlib.md5(open(filename, "rb").read()).hexdigest()
    response["message"].append(response["hash"])
    if os.path.exists(logfile):
        with open(logfile) as f:
            hashline = f.readline().strip("\n")
        if hashline == response["hash"]:
            response["pass"] = True
            response["message"].append("Hashes match, file hasn't changed.")
        else:
            response["message"].append(f"File has changed, validating {filename}.")
    elif os.path.exists(not_val_logfile):
        with open(not_val_logfile) as f:
            hashline = f.readline().strip("\n")
        if hashline == response["hash"]:
            response["pass"] = False
            response["message"].append("Hashes match, file hasn't been corrected.")
        else:
            response["message"].append(f"File has changed, validating {filename}.")
    else:
        response["message"].append(f"Validating {filename}.")
    return response
