from DataBUS import GeochronControl, Response


def valid_geochroncontrol(cur, databus):
    """Validates and inserts geochronological control linkage records.

    Links each geochron record (ndb.geochronology) to its corresponding
    chroncontrol record (ndb.chroncontrols) by inserting rows into
    ndb.geochroncontrols.  IDs for both are taken from databus:
      - databus['chron_controls'].id_list  → chroncontrol IDs
      - databus['geochron'].id_list        → geochron IDs

    If either list is empty or None the step is skipped gracefully.  When the
    two lists differ in length the function tries to broadcast the shorter list;
    if neither is a multiple of the other it pairs them up to the shorter length.

    Args:
        cur: Database cursor for executing SQL queries.
        databus (dict): Dictionary containing 'chron_controls' and 'geochron'
            Response objects (populated by prior validation steps).

    Returns:
        Response: Response object containing validation messages and overall status.

    Examples:
        >>> valid_geochroncontrol(cursor, databus)
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()

    chron_controls_resp = databus.get("chron_controls")
    geochron_resp = databus.get("geochron")

    if chron_controls_resp is None or geochron_resp is None:
        response.message.append(
            "✔ No chron_controls or geochron results available, skipping geochroncontrol linking."
        )
        response.valid.append(True)
        return response

    cc_ids = [i for i in chron_controls_resp.id_list if i is not None]
    geo_ids = [i for i in geochron_resp.id_list if i is not None]

    if not cc_ids or not geo_ids:
        response.message.append("✔ No chroncontrol IDs or geochron IDs to insert.")
        response.valid.append(True)
        return response

    # Align list lengths
    try:
        if len(cc_ids) % len(geo_ids) == 0:
            times = len(cc_ids) // len(geo_ids)
            geo_ids_paired = geo_ids * times
            cc_ids_paired = cc_ids
        elif len(geo_ids) % len(cc_ids) == 0:
            times = len(geo_ids) // len(cc_ids)
            cc_ids_paired = cc_ids * times
            geo_ids_paired = geo_ids
        else:
            min_len = min(len(cc_ids), len(geo_ids))
            cc_ids_paired = cc_ids[:min_len]
            geo_ids_paired = geo_ids[:min_len]
            response.message.append(
                f"?  Mismatch: {len(cc_ids)} chroncontrol IDs vs "
                f"{len(geo_ids)} geochron IDs. Linking first {min_len} pairs."
            )
    except ZeroDivisionError:
        response.valid.append(True)
        return response

    for cc_id, geo_id in zip(cc_ids_paired, geo_ids_paired, strict=False):
        try:
            if geo_id is None:
                response.valid.append(True)
                response.message.append("✔ Geochron ID is None, skipping insert.")
                continue
            gcc = GeochronControl(chroncontrolid=cc_id, geochronid=geo_id)
            gcc.insert_to_db(cur)
            response.valid.append(True)
            response.message.append(
                f"✔ GeochronControl inserted (chroncontrolid={cc_id}, geochronid={geo_id})."
            )
        except Exception as e:
            response.valid.append(False)
            response.message.append(
                f"✗  GeochronControl not inserted "
                f"(chroncontrolid={cc_id}, geochronid={geo_id}): {e}"
            )

    return response
