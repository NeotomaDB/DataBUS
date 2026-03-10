import argparse
import os


def parse_arguments():
    """Parse commandline arguments for the Neotoma data uploader.

    Parses command-line arguments for paths to data directory, template file,
    logs directory, and overwrite option. Validates that specified
    paths exist before returning.

    Examples:
        >>> parse_arguments()  # doctest: +SKIP
        {'data': 'data/', 'template': 'pollen_template.yml', 'logs': 'data/logs/', 'overwrite': False}
        >>> parse_arguments()  # doctest: +SKIP (with --data paleolake_core --template chronology.xlsx)
        {'data': 'paleolake_core/', 'template': 'chronology.xlsx', 'logs': 'data/logs/', 'overwrite': False}

    Args:
        None

    Returns:
        dict: Dictionary with keys 'data', 'template', 'logs', and 'overwrite'.
              'data': Path to the data directory (str)
              'template': Path to the YAML/XLSX template file (str)
              'logs': Path to validation logs folder (str)
              'overwrite': Boolean flag for overwriting option (bool)

    Raises:
        FileNotFoundError: If data directory or template file does not exist.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data",
        type=str,
        nargs="?",
        const="data/",
        default="data/",
        help="Path to the data directory",
    )
    parser.add_argument(
        "--template",
        type=str,
        nargs="?",
        const="template.yml",
        default="template.yml",
        help="YAML/XLSX Template file to map to Neotoma",
    )
    parser.add_argument(
        "--logs",
        type=str,
        nargs="?",
        const="data/logs/",
        default="data/logs/",
        help="Validation logs. In order to get logs, validation must be run with --upload False first.",
    )

    parser.add_argument(
        "--upload",
        type=bool,
        nargs="?",
        const=False,
        default=False,
        help="Set to True if data should be uploaded to the database after validation.",
    )

    args = parser.parse_args()

    if not os.path.isdir(args.data):
        raise FileNotFoundError(
            f"There is no directory named '{args.data}' within the current path."
        )

    if not os.path.isfile(args.template):
        raise FileNotFoundError(
            f"The file '{args.template}' could not be found within the current path."
        )

    return {"data": args.data, "template": args.template, "upload": args.upload, "logs": args.logs}
