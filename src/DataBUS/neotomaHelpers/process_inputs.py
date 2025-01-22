def process_inputs(inputs, response=None, name=None, values=None):
    """
    Processes and validates the inputs dictionary, updating it in place.

    Args:
        inputs (dict): Dictionary containing various input fields.
        response (dict): Dictionary to store validation messages and status.

    Returns:
        None
    """

    def clean_list(value):
        """
        Returns the first item of a list if the value is a list, otherwise returns the value.

        Args:
            value: A list or a single value.

        Returns:
            The first item if value is a list, otherwise the value itself.
        """
        if isinstance(value, list):
            return value[0] if value else None
        return value

    # Check sitename
    if name is not None:
        if isinstance(inputs.get(name), list):
            if len(set(inputs[name])) > 1:
                response["message"].append(f"✗ There should only be one {name}.")
                response["valid"].append(False)
            if name == "sitename":
                inputs[name] = inputs[name][0]
            elif name == "handle":
                if inputs[name] is not None and inputs[name] != ["NA"]:
                    response["given_handle"] = True
                    inputs[name] = str(inputs[name][0])
                    response["message"].append(f"Handle has been given: {inputs[name]}")
                else:
                    response["given_handle"] = False
                    response["message"].append("A new Handle will be generated")
                    inputs[name] = str(inputs["core"][0])[:10]

    # Check other values
    if values is not None:
        for i in values:
            inputs[i] = clean_list(inputs.get(i))

    # Check geographic coordinates
    if "geog" in inputs.keys():
        try:
            coords = inputs["geog"]
            assert len(coords) == 2, "Coordinates must have two values"
            lat, long = coords
            assert -90 <= lat <= 90, "Latitude must be between -90 and 90"
            assert -180 <= long <= 180, "Longitude must be between -180 and 180"
            inputs["ns"] = lat
            inputs["ew"] = long
        except (AssertionError, KeyError, TypeError):
            response["message"].append(
                "✗ Coordinates are improperly formatted. They must be in the form 'LAT, LONG' [-90 -> 90] and [-180 -> 180]."
            )
            response["valid"].append(False)
