from DataBUS import Response
from DataBUS.Project import (
    Grant,
    Institution,
    Project,
    insert_contact_institution,
    insert_funding_institution,
    insert_grant_awardee,
    insert_project_dataset,
    insert_project_grant,
    insert_project_keyword,
    insert_project_participant,
)


def valid_project(cur, yml_dict, csv_file, databus=None):
    """Validates and inserts project-level data into Neotoma.

    Handles the full project hierarchy from the YAML template:
      - ndb.projects (project name, description, dates)
      - ndb.grants (grant name, number, dates)
      - ndb.institutions (RoR id, name, location)
      - Junction tables: projectdatasets, projectgrants, projectparticipants,
        projectkeywords, fundinginstitutions, contactinstitutions, grantawardees

    Runs AFTER valid_dataset and valid_contact so that datasetid and contactids
    are available in the databus dict.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file: CSV/XLSX data (unused, kept for signature uniformity).
        databus (dict | None): Prior validation results. Uses
            ``databus["datasets"].id_int`` and ``databus["contacts"].id_int``.

    Returns:
        Response: Response object with validation results. ``id_int`` holds
            the projectid on success.
    """
    response = Response()

    # ── 1. Extract project info from the template ─────────────────────────
    project_info = _extract_project_info(yml_dict)
    if project_info is None:
        response.message.append("? No project entry found in template.")
        response.valid.append(True)
        return response

    # ── 2. Insert or find the project ─────────────────────────────────────
    projectname = project_info.get("projectname")
    if not projectname:
        response.message.append("✗ Project name is required but not provided.")
        response.valid.append(False)
        return response

    # Check if project already exists
    cur.execute(
        "SELECT projectid FROM ndb.projects WHERE LOWER(projectname) = %(name)s;",
        {"name": projectname.lower().strip()},
    )
    existing = cur.fetchone()
    if existing:
        projectid = existing[0]
        response.message.append(f"✔ Project '{projectname}' already exists (ID: {projectid}).")
        response.valid.append(True)
    else:
        try:
            proj = Project(
                projectname=projectname,
                projectdescription=project_info.get("projectdescription"),
                projectstartdate=project_info.get("projectstartdate"),
                projectenddate=project_info.get("projectenddate"),
            )
            projectid = proj.insert_to_db(cur)
            response.message.append(f"✔ Project '{projectname}' inserted (ID: {projectid}).")
            response.valid.append(True)
        except Exception as e:
            response.message.append(f"✗ Cannot insert project: {e}")
            response.valid.append(False)
            return response

    response.id_int = projectid

    # ── 3. Link project to dataset ────────────────────────────────────────
    try:
        datasetid = databus["datasets"].id_int
        insert_project_dataset(cur, projectid, datasetid)
        response.message.append("✔ Project linked to dataset.")
        response.valid.append(True)
    except Exception as e:
        response.message.append(f"? Could not link project to dataset: {e}")

    # ── 4. Handle grant(s) ────────────────────────────────────────────────
    grant_info = _extract_grant_info(yml_dict)
    if grant_info:
        for gi in grant_info:
            grantid = _insert_or_find_grant(cur, gi, response)
            if grantid is not None:
                try:
                    insert_project_grant(cur, projectid, grantid)
                    response.message.append(
                        f"✔ Grant '{gi.get('grantname', '')}' linked to project."
                    )
                except Exception as e:
                    response.message.append(f"? Could not link grant to project: {e}")

                # Link funding institution to grant
                inst_info = gi.get("institution")
                if inst_info:
                    instid = _insert_or_find_institution(cur, inst_info, response)
                    if instid is not None:
                        try:
                            insert_funding_institution(cur, grantid, instid)
                        except Exception as e:
                            response.message.append(f"? Could not link institution to grant: {e}")

                # Link grant awardee (PI)
                awardee = gi.get("awardee")
                if awardee:
                    contactid = _resolve_contact(cur, awardee)
                    if contactid:
                        try:
                            insert_grant_awardee(cur, grantid, contactid)
                        except Exception as e:
                            response.message.append(f"? Could not link awardee: {e}")

    # ── 5. Handle project participants ────────────────────────────────────
    participants = _extract_participants(yml_dict)
    for name in participants:
        contactid = _resolve_contact(cur, name)
        if contactid:
            try:
                insert_project_participant(cur, projectid, contactid)
                response.valid.append(True)
            except Exception as e:
                response.message.append(f"? Could not add participant '{name}': {e}")
        else:
            response.message.append(f"? Participant '{name}' not found in contacts.")

    # ── 6. Handle project keywords ────────────────────────────────────────
    keywords = _extract_keywords(yml_dict)
    for kw in keywords:
        keywordid = _resolve_keyword(cur, kw)
        if keywordid:
            try:
                insert_project_keyword(cur, projectid, keywordid)
                response.valid.append(True)
            except Exception as e:
                response.message.append(f"? Could not add keyword '{kw}': {e}")
        else:
            response.message.append(f"? Keyword '{kw}' not found in ndb.keywords.")

    # ── 7. Handle contact-institution links ───────────────────────────────
    contact_institutions = _extract_contact_institutions(yml_dict)
    for ci in contact_institutions:
        contactid = _resolve_contact(cur, ci["contactname"])
        if contactid and ci.get("institutionid"):
            instid = _insert_or_find_institution(cur, ci, response)
            if instid:
                try:
                    insert_contact_institution(cur, contactid, instid)
                except Exception as e:
                    response.message.append(f"? Could not link contact to institution: {e}")

    if not response.valid:
        response.valid.append(True)
        response.message.append("? Project validation completed with no actionable items.")

    return response


# ── Template extraction helpers ───────────────────────────────────────────────


def _extract_project_info(yml_dict):
    """Extract project metadata from the template.

    Looks for entries with neotoma prefix ``ndb.projects.``.
    """
    info = {}
    for entry in yml_dict.get("metadata", []):
        neotoma = entry.get("neotoma", "")
        if neotoma.startswith("ndb.projects."):
            field = neotoma.split(".")[-1]
            info[field] = entry.get("value", entry.get("column"))
    return info if info else None


def _extract_grant_info(yml_dict):
    """Extract grant metadata from the template.

    Each grant entry group starts with ``ndb.grants.`` and may include nested
    institution and awardee info.
    """
    grants = []
    current = {}
    for entry in yml_dict.get("metadata", []):
        neotoma = entry.get("neotoma", "")
        if neotoma.startswith("ndb.grants."):
            field = neotoma.split(".")[-1]
            current[field] = entry.get("value", entry.get("column"))
            # Nested institution info
            if entry.get("institutionid"):
                current["institution"] = {
                    "institutionid": entry.get("institutionid"),
                    "institutionname": entry.get("institutionname"),
                    "institutionlocation": entry.get("institutionlocation"),
                }
            if entry.get("awardee"):
                current["awardee"] = entry["awardee"]
        elif current:
            grants.append(current)
            current = {}
    if current:
        grants.append(current)
    return grants


def _extract_participants(yml_dict):
    """Extract project participant names from the template."""
    for entry in yml_dict.get("metadata", []):
        if entry.get("neotoma") == "ndb.projectparticipants.contactname":
            val = entry.get("value", "")
            if isinstance(val, str):
                return [v.strip() for v in val.split("|") if v.strip()]
            if isinstance(val, list):
                return val
    return []


def _extract_keywords(yml_dict):
    """Extract project keywords from the template."""
    for entry in yml_dict.get("metadata", []):
        if entry.get("neotoma") == "ndb.projectkeywords.keyword":
            val = entry.get("value", "")
            if isinstance(val, str):
                return [v.strip() for v in val.split("|") if v.strip()]
            if isinstance(val, list):
                return val
    return []


def _extract_contact_institutions(yml_dict):
    """Extract contact-institution pairings from the template."""
    results = []
    for entry in yml_dict.get("metadata", []):
        if entry.get("neotoma") == "ndb.contactinstitutions.contactname":
            results.append(
                {
                    "contactname": entry.get("value"),
                    "institutionid": entry.get("institutionid"),
                    "institutionname": entry.get("institutionname"),
                    "institutionlocation": entry.get("institutionlocation"),
                }
            )
    return results


# ── DB lookup helpers ─────────────────────────────────────────────────────────


def _resolve_contact(cur, name):
    """Look up a contactid by name. Returns int or None."""
    if not name:
        return None
    cur.execute(
        "SELECT contactid FROM ndb.contacts WHERE LOWER(contactname) = %(name)s;",
        {"name": name.lower().strip()},
    )
    result = cur.fetchone()
    return result[0] if result else None


def _resolve_keyword(cur, keyword):
    """Look up a keywordid by keyword text. Returns int or None."""
    if not keyword:
        return None
    cur.execute(
        "SELECT keywordid FROM ndb.keywords WHERE LOWER(keyword) = %(kw)s;",
        {"kw": keyword.lower().strip()},
    )
    result = cur.fetchone()
    return result[0] if result else None


def _insert_or_find_grant(cur, grant_info, response):
    """Insert a grant or find an existing one by grant number."""
    grantnumber = grant_info.get("grantnumber")
    if grantnumber:
        cur.execute(
            "SELECT grantid FROM ndb.grants WHERE grantnumber = %(num)s;",
            {"num": grantnumber},
        )
        existing = cur.fetchone()
        if existing:
            response.message.append(f"✔ Grant '{grantnumber}' already exists (ID: {existing[0]}).")
            response.valid.append(True)
            return existing[0]
    try:
        g = Grant(
            grantname=grant_info.get("grantname"),
            grantnumber=grantnumber,
            dateawarded=grant_info.get("dateawarded"),
            dateended=grant_info.get("dateended"),
        )
        grantid = g.insert_to_db(cur)
        response.message.append(f"✔ Grant inserted (ID: {grantid}).")
        response.valid.append(True)
        return grantid
    except Exception as e:
        response.message.append(f"✗ Cannot insert grant: {e}")
        response.valid.append(False)
        return None


def _insert_or_find_institution(cur, inst_info, response):
    """Insert an institution or find existing by RoR ID."""
    instid = inst_info.get("institutionid")
    if not instid:
        return None
    try:
        inst = Institution(
            institutionid=instid,
            institutionname=inst_info.get("institutionname"),
            institutionlocation=inst_info.get("institutionlocation"),
        )
        return inst.insert_to_db(cur)
    except Exception as e:
        response.message.append(f"✗ Cannot insert institution: {e}")
        response.valid.append(False)
        return None
