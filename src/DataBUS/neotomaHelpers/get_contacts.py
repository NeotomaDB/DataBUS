import re

def match_abbreviation_to_full(abbreviated, full_name):
    # Split the abbreviated name into surname and initials
    parts = abbreviated.split(", ")
    surname = parts[0]
    initials = parts[1] if len(parts) > 1 else ""

    # Build a regex pattern for initials dynamically
    initial_parts = initials.split()
    regex_initials = r"\s*".join([f"{init[0]}(?:\\w*)?\.?,?" for init in initial_parts])
    # Full regex pattern
    regex_pattern = fr"^{surname},\s*{regex_initials}$"
    regex_pattern2 = fr"^(\w+),\s*(\w+)\s+(\w)\.?(\w+)?$"
    # Perform the regex match
    return (bool(re.match(regex_pattern, full_name, re.IGNORECASE)) or
             bool(re.match(regex_pattern2, full_name, re.IGNORECASE)))

def get_contacts(cur, contacts_list):
    baseid = 1
    contids = list()
    if contacts_list:
        for i in contacts_list:
            i = i.strip()
            familyname = i.split(",")[0].strip()
            firstname = i.split(",")[1].strip() if len(i.split(",")) > 1 else ""
            if '.' in firstname:
                get_contact = """SELECT contactid, familyname || ', ' || leadinginitials AS fullname
                                 FROM ndb.contacts
                                 WHERE LOWER(familyname) = %(familyname)s AND
                                       LOWER(leadinginitials) ILIKE %(leadinginitials)s;"""
                cur.execute(get_contact, {"familyname": familyname.lower(),
                                        "leadinginitials": str(firstname.lower()+'%')})
            else:
                get_contact = """SELECT contactid, familyname || ', ' || givennames AS fullname
                                 FROM ndb.contacts
                                 WHERE LOWER(familyname) = %(familyname)s AND
                                       LOWER(givennames) ILIKE %(givennames)s;"""
                cur.execute(get_contact, {"familyname": familyname.lower(),
                                          "givennames": str(firstname.lower()+'%')})
            data = cur.fetchone()
            if data:
                d_name = data[1].lower()
                d_id = data[0]
                result = d_name.startswith(i.lower().rstrip("."))
                result_2 = match_abbreviation_to_full(d_name, i)
                if (result or result_2) == True:
                    contids.append({"name": d_name, "id": d_id, "order": baseid})
                else:
                    contids.append({"name": i, "id": None, "order": baseid})
                baseid +=1
            else:
                contactname = i.lower().strip()
                get_contact = """SELECT contactid, familyname || ', ' || givennames AS fullname
                                 FROM ndb.contacts
                                 WHERE LOWER(contactname) = %(contactname)s;"""
                cur.execute(get_contact, {"contactname": contactname})
                data = cur.fetchone()
                if data:
                    d_name = data[1].lower()
                    d_id = data[0]
                    contids.append({"name": d_name, "id": d_id, "order": baseid})
                    baseid +=1
                else:
                    contactname = contactname.replace(". ", ".")
                    cur.execute(get_contact, {"familyname": contactname})
                    data = cur.fetchone()
                    if data:
                        d_name = data[1].lower()
                        d_id = data[0]
                        contids.append({"name": d_name, "id": d_id, "order": baseid})
                        baseid +=1
                    else:
                        contids.append({"name": i, "id": None, "order": None})
    return contids