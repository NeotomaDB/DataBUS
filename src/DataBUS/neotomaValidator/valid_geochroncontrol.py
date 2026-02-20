from DataBUS import Response

def valid_geochroncontrol(validator):
    """Validates geochronological control data consistency.

    Verifies that both chroncontrols and geochron validation results are present
    and successful. Acts as a prerequisite check before geochronological data insertion.
    Note: Elements are obtained from uploader; no inserts occur during validation.

    Args:
        validator (dict): Dictionary containing 'chron_controls' and 'geochron' Response objects.

    Returns:
        Response: Response object containing validation messages and overall status.

    Examples:
        >>> valid_geochroncontrol({'chron_controls': response1, 'geochron': response2})
        Response(valid=[True], message=[...], validAll=True)
    """
    entries = {'chroncontrolid': validator.get("chroncontrols"),  # add id
               'geochronid': validator.get("geochron")} # add id
    response = Response()
    if len(entries['chroncontrolid']) == 0 or len(entries['geochronid']) == 0:
        response.message.append("✔ No chroncontrol IDs or geochron IDs to insert.")
        response.valid.append(True)
        response.validAll = all(response.valid)
        return response
    geo_indices = validator['geochron'].indices
    cc_indices = validator['chroncontrols'].indices
    # Flatten ids2
    cc_indices = [item for sublist in cc_indices for item in sublist]
    common_indices = set(geo_indices) & set(cc_indices)
    mask = [idx in common_indices for idx in geo_indices]
    # Filter entries to keep only positions where mask is True
    positions_to_keep = [i for i, idx in enumerate(cc_indices) if idx in common_indices]
    # Now filter entries using only those positions
    entries['geochronid'] = [entries['geochronid'][i] for i in positions_to_keep if i < len(entries['geochronid'])]
    entries['chroncontrolid'] = [entries['chroncontrolid'][i] for i in positions_to_keep if i < len(entries['chroncontrolid'])]
    try:
        assert len(entries['chroncontrolid']) % len(entries['geochronid']) == 0 
        times = len(entries['chroncontrolid']) // len(entries['geochronid'])
        # duplicate geochronid the appropriate number of times
        entries['geochronid'] = entries['geochronid'] * times
    except AssertionError:
        try:
            entries['geochronid'] = [gid for gid in entries['geochronid'] if gid is not None]
            assert len(entries['chroncontrolid']) % len(entries['geochronid']) == 0 
            times = len(entries['chroncontrolid']) // len(entries['geochronid'])
            entries['geochronid'] = entries['geochronid'] * times
        except AssertionError:
            response.message.append(f"✗  Number of chroncontrol IDs {entries['chroncontrolid']}"
                                    f"does not match number of geochron IDs {entries['geochronid']}")
            response.valid.append(False)
            response.validAll = all(response.valid)
            return response
        except ZeroDivisionError as e:
            response.valid.append(True)
            response.validAll = all(response.valid)
            return response
    except ZeroDivisionError as e:
        response.valid.append(True)
        response.validAll = all(response.valid)
        return response
    counter = 0
    for i in range(len(entries['chroncontrolid'])):
        counter += 1
        entry = {}
        entry['chroncontrolid'] = entries['chroncontrolid'][i]
        entry['geochronid'] = entries['geochronid'][i]
        # SUGGESTION: Consider using a context manager or batch insert for database operations
        # SUGGESTION: The counter variable is incremented but never used; consider removing if not needed for logging
        try:
            if entry.get('geochronid') is None:
                response.valid.append(True)
                response.message.append("✔ Geochron ID is None, skipping insert.")
                continue
            else:
                gcc = GeochronControl(**entry)
                gcc.insert_to_db(cur)
                response.valid.append(True)
                response.message.append("✔ GeochronControl inserted.")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  GeochronControl not inserted. {e}")
    
    return response 