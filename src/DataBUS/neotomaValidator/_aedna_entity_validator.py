import DataBUS.neotomaHelpers as nh
from DataBUS import Response


def validate_aedna_entity(cur, yml_dict, cls, databus, links, present_fields, resolve=None):
    """Validate and insert one flat aeDNA entity (assay or library).

    Reads ``cls``'s ``value:`` constants from the template (via ``retrieve_dict``,
    as ``valid_dataset`` does). If none of *present_fields* is set, returns an
    empty-but-valid Response (nothing to do). Otherwise runs the optional
    *resolve* hook, fills the *links* foreign keys from ``databus``, builds
    ``cls(**inputs)``, inserts it, and stores the new primary key in
    ``response.id_int``.

    Args:
        cur (cursor): Database cursor.
        yml_dict (dict): Parsed template.
        cls (type): An :class:`AeDNAEntity` subclass (``AeDNAAssay``/``AeDNALibrary``).
        databus (dict | None): Prior validation results supplying parent ids.
        links (dict): ``{param: databus_key}`` — ``databus[key].id_int`` fills the FK.
        present_fields (list): Params whose presence means the entity was described.
        resolve (callable | None): Optional ``(cur, inputs, response)`` pre-insert hook.

    Returns:
        Response: Validation result with the inserted id in ``id_int``.
    """
    response = Response()
    label = cls.__name__
    inputs = {}
    for param in cls._PARAMS:
        matches = nh.retrieve_dict(yml_dict, f"{cls._TABLE}.{param}")
        inputs[param] = next((m.get("value") for m in matches if m.get("value") is not None), None)

    if not any(inputs.get(field) for field in present_fields):
        response.valid.append(True)
        response.message.append(f"? No {label} entries found in template.")
        return response

    if resolve is not None:
        resolve(cur, inputs, response)

    for param, key in links.items():
        try:
            inputs[param] = databus[key].id_int
        except Exception as e:
            inputs[param] = None
            response.valid.append(False)
            response.message.append(f"✗ {key} ID not available for {label}: {e}")

    try:
        entity = cls(**inputs)
        response.valid.append(True)
        response.message.append(f"✔ {label} can be created.")
        response.id_int = entity.insert_to_db(cur)
        response.message.append(f"✔ {label} inserted with ID {response.id_int}.")
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ {label} cannot be created: {e}")
    return response
