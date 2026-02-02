import re
import os

def check_file(filename, strict = False, validation_files = "data/validation_logs/"):
    """Checks validation log file for errors from prior validation runs.

    Examines validation log files to determine if a CSV file has been successfully
    validated. Checks both validated and not_validated directories. Counts errors
    and warnings, removing log file if strict mode passes.

    Examples:
        >>> check_file("pollen_data.csv", strict=False)  # doctest: +SKIP
        {'pass': True, 'match': 0, 'message': ['No errors found in the last validation.']}
        >>> check_file("chronology.csv", strict=True)  # doctest: +SKIP
        {'pass': False, 'match': 2, 'message': ['Errors found in the prior validation.']}

    Args:
        filename (str): File path or relative path for a template CSV file.
        strict (bool): If True, also count "Valid: FALSE" lines as errors. Defaults to False.
        validation_files (str): Path to validation logs directory. Defaults to "data/validation_logs/".

    Returns:
        dict: Dictionary with 'pass' (bool), 'match' (int error count), and 'message' (list).
    """
    response = {"pass": False, "match": 0, "message": []}
    modified_filename = os.path.basename(filename)
    logfile = f"{validation_files}{modified_filename}"+ ".valid.log"
    not_val_logfile = f"{validation_files}not_validated/{modified_filename}"+ ".valid.log"
    if os.path.exists(logfile): 
        with open(logfile, "r", encoding="utf-8") as f:
            for line in f:
                error = re.match("✗", line)
                error2 = re.match("Valid: FALSE", line)
                if error:
                    response["match"] = response["match"] + 1
                if strict == True and error2:
                    response["match"] = response["match"] + 1
        if response["match"] == 0:
            response["pass"] = True
            response["message"].append("No errors found in the last validation.")
        else:
            response["message"].append("Errors found in the prior validation.")
    elif os.path.exists(not_val_logfile): 
        with open(not_val_logfile, "r", encoding="utf-8") as f:
            for line in f:
                error = re.match("✗", line)
                error3 = re.match(r"^\s*✗$", line)
                error2 = re.match("Valid: FALSE", line)
                if error or error3:
                    response["match"] = response["match"] + 1
                if strict == True and error2:
                    response["match"] = response["match"] + 1
        if response["match"] == 0:
            response["pass"] = True
            os.remove(not_val_logfile)
        else:
            response["message"].append("Errors found in the prior validation.")
    else:
        response["message"].append("No prior log file exists.")
        response["pass"] = True
    return response
