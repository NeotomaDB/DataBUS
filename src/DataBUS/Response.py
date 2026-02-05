class Response:
    """Base response class for handling validation and messaging.

    This class provides a standard structure for returning validation results
    and messages from database operations. It includes attributes for tracking
    validity, messages, and associated database IDs.

    Examples:
        >>> response = Response(valid=[True], message=["Pollen dataset validated successfully"])
        >>> response.validAll
        True
        >>> response = Response(valid=[True, True, False], message=["Data validation error"])
        >>> len(response.valid)
        3

    Args:
        valid (list | None): List of validation boolean values.
        message (list | None): List of message strings.

    Attributes:
        valid (list): Validation status values.
        message (list): Message strings.
        validAll (bool | None): Overall validation status.
        datasetid (int | None): Associated dataset ID.
        datasetpi (list): List of dataset principal investigator IDs.
        processor (list): List of data processor IDs.
        collector (list): List of collector IDs.
        repoid (int | None): Repository ID.
        databaseid (int | None): Database ID.
        sampleid (list): List of sample IDs.
        data_id (dict): Mapping of data identifiers.
        uncertaintyinputs (list): Uncertainty input values.
        id (list): General ID list.
        name (dict): Name mapping dictionary.
        indices (list): List of indices.
    """

    def __init__(self):
        """
        Initialize a Response object.
        """
        self.valid = []
        self.message = []
        self.id = []
        self.data_id = {}
        self.name = {}
        self.elements = []
        self.indices = []
        self.counter = 0
        self.nearby = []
        self.matched = {}
    
    @property
    def validAll(self):
        """
        True if valid is a non-empty list of booleans and all are True.
        False otherwise.
        """
        if not self.valid:
            return False
        return all(self.valid)

    def __str__(self):
        """Return string representation of the Response object.

        Returns:
            str: Formatted string showing validity and messages.
        """
        new_msg = "\n".join(str(m) for m in self.message)
        if self.validAll == False:
            return f"Valid: {str(self.validAll).upper()} \n" f"Message: \n" f"{new_msg}"
        else:
            return f"Valid: {self.validAll} \n" f"Message: \n" f"{new_msg}"