from DataBUS import Response, GeochronControl

def insert_geochroncontrols(cur, yml_dict, csv_file, uploader):
    """Both elements are obtained from uploader. 
    Cannot validate as there are no inserts during the validation process."""
    
    entries = {'chroncontrolid': uploader['chroncontrols'].id, 
               'geochronid': uploader['geochron'].id}
    response = Response()
    if len(entries['chroncontrolid']) == 0 or len(entries['geochronid']) == 0:
        response.message.append("✔ No chroncontrol IDs or geochron IDs to insert.")
        response.valid.append(True)
        response.validAll = all(response.valid)
        return response
    indices = uploader['geochron'].indices
    indices2 = uploader['chroncontrols'].indices
    # Flatten ids2
    indices2 = [item for sublist in indices2 for item in sublist]
    common_indices = set(indices) & set(indices2)
    mask = [idx in common_indices for idx in indices]
    # Filter entries to keep only positions where mask is True
    #entries['geochronid'] = [entries['geochronid'][i] for i, keep in enumerate(mask) if keep]
    positions_to_keep = [i for i, idx in enumerate(indices2) if idx in common_indices]
    # Now filter entries using only those positions
    entries['geochronid'] = [entries['geochronid'][i] for i in positions_to_keep if i < len(entries['geochronid'])]
    entries['chroncontrolid'] = [entries['chroncontrolid'][i] for i in positions_to_keep if i < len(entries['chroncontrolid'])]
    #entries['chroncontrolid'] = [entries['chroncontrolid'][i] for i, keep in enumerate(mask) if keep]
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
        try:
            if entry.get('geochronid') is None:
                response.valid.append(True)
                response.message.append("✔ Geochron ID is None, skipping insert.")
                continue
            else:
                gcc = GeochronControl(**entry)
                gcc.insert_to_db(cur)    
                response.valid. append(True)
                response.message.append("✔ GeochronControl inserted.")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  GeochronControl not inserted. {e}")
    response.message = list(set(response.message))
    response.validAll = all(response.valid)
    return response