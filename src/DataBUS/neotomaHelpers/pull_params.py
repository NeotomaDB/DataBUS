import datetime
import re
from .retrieve_dict import retrieve_dict
from .clean_column import clean_column
 
def pull_params(params, yml_dict, csv_template, table=None, values=False):
    """Pull and process parameters for database insert statements.

    Extracts parameters from YAML template and CSV data, performs type conversions
    (date, int, float, coordinates, string), handles special cases like notes and
    chronologies, and returns cleaned data ready for insertion.

    Args:
        params (list): List of strings for columns needed to generate insert statement.
        yml_dict (dict): Dictionary returned by YAML template containing 'metadata' key.
        csv_template (dict): CSV data as list of dictionaries with column data to upload.
        table (str or list, optional): Name of the table(s) parameters are drawn for.
                                      If list, returns results for each table.
        name (str, optional): Name field identifier. Defaults to None.
        values (bool, optional): Whether to treat columns as value columns. Defaults to False.

    Returns:
        dict or list: Cleaned and formatted parameters ready for database insertion.
                      If table is a list, returns list of dicts. If table is str, returns single dict.
                      Returns hierarchical dicts for special tables like chronologies/sampleages.
    """
    results = []
    if isinstance(table, str):
        add_unit_inputs = {}
        if re.match(".*\.$", table) == None:
            table = table + "."
        # SUGGESTION: Extract date/type parsing into separate helper function for reusability
        for param in params:
            if values == False:
                subfields = [entry for entry in yml_dict['metadata'] if entry.get('neotoma', '').startswith(f'{table}{param}.')]
            else:
                subfields = [entry for entry in yml_dict['metadata'] if entry.get('column', '') == f'{param}']
            if subfields:
                for entry in subfields:
                    param_name = entry['neotoma'].replace(f'{table}', "")
                    params.append(param_name)
                if param in params:
                    params.remove(param)
        for i in params:
            if values == False:
                valor = retrieve_dict(yml_dict, table + i)
            else:
                valor = subfields
            if len(valor) > 0: 
                for count, val in enumerate(valor):
                    chron_counter = 0
                    try:
                        clean_valor = clean_column(val.get("column"), 
                                               csv_template, 
                                               clean=not val.get("rowwise"))
                    except KeyError as e:
                        continue
                    if clean_valor:
                        match val.get("type"):
                            case "date":
                                if val.get("rowwise"):
                                    clean_valor = list(map(lambda x: datetime.datetime.strptime(x.replace("/", "-"), "%Y-%m-%d").date(),
                                                           clean_valor))
                                else:
                                    clean_valor = datetime.datetime.strptime(clean_valor.replace("/", "-"), "%Y-%m-%d").date()
                            case "int":
                                clean_valor = list(map(int, clean_valor)) if val.get("rowwise") else int(clean_valor)
                            case "float":
                                if val.get("rowwise"):
                                    clean_valor = [float(value) if value not in ["NA", ""] else None
                                                   for value in clean_valor]
                                else:
                                    clean_valor = float(clean_valor)
                            case "coordinates (lat,long)":
                                clean_valor = [float(num) for num in clean_valor[0].split(",")]
                            case "string" | "str":
                                clean_valor = list(map(str, clean_valor)) if val.get("rowwise") else str(clean_valor)
                                if isinstance(clean_valor, list):
                                    clean_valor = [None if isinstance(value, str) and value.strip() in ("", "NA") 
                                               else str(value) if not isinstance(value, list) else value 
                                               for value in clean_valor]
                                    clean_valor = None if all(item in [None, ''] for item in clean_valor) and clean_valor else clean_valor
                        if i == 'notes':
                            if'notes' in add_unit_inputs:
                                if add_unit_inputs['notes'] is None:
                                    add_unit_inputs['notes'] = []
                                elif isinstance(add_unit_inputs['notes'], str):
                                    add_unit_inputs['notes'] = [add_unit_inputs['notes']]
                                elif isinstance(add_unit_inputs['notes'], list):
                                    add_unit_inputs[i].append({f"{val.get('column')}": clean_valor})
                            else:
                                add_unit_inputs[i] = []
                                add_unit_inputs[i].append({f"{val.get('column')}": clean_valor})
                        elif any(k in table for k in ('chronologies', 'sampleages')):
                            key = ('chronologies' if 'chronologies' in table 
                                   else 'sampleages' if 'sampleages' in table else None)
                            if key not in add_unit_inputs:
                                add_unit_inputs[key] = {}
                            if clean_valor is None:
                                continue
                            elif ((isinstance(clean_valor, list) and not all(x is None for x in clean_valor))
                                   or (not isinstance(clean_valor, list) and clean_valor is not None)):
                                if 'chronologyname' in val:
                                    if not add_unit_inputs[key].get(val['chronologyname']):
                                        add_unit_inputs[key][val.get('chronologyname', f'Chron_{chron_counter}')] = {}
                                        chron_counter += 1
                                    add_unit_inputs[key][val['chronologyname']][i] = clean_valor
                                    if i == 'age' and key == 'chronologies':
                                        add_unit_inputs['chronologies'][val['chronologyname']]['isdefault'] = val.get('default', False)
                                else: 
                                    k = val['neotoma'].split('.')[-1]
                                    add_unit_inputs[k] = clean_valor
                        elif 'taxonname' in val:
                            if not all(x is None for x in clean_valor): 
                                add_unit_inputs[val['taxonname']] = {}
                                add_unit_inputs[val['taxonname']]['value'] = clean_valor
                                if 'unitcolumn' in val:
                                    add_unit_inputs[val['taxonname']]['unitcolumn'] = val['unitcolumn']
                                if 'uncertaintyunit' in val:
                                    add_unit_inputs[val['taxonname']][f"uncertaintyunit"] = val['uncertaintyunit']
                                if 'uncertaintybasis' in val:
                                    add_unit_inputs[val['taxonname']][f"uncertaintybasis"] = val['uncertaintybasis']
                        else:
                            add_unit_inputs[i] = clean_valor
                    else:
                        add_unit_inputs[i] = None
            else:
                add_unit_inputs[i] = None
        if 'notes' in add_unit_inputs.keys():
            add_unit_inputs['notes'] = str(add_unit_inputs['notes']).replace("[", "").replace("]", "").replace("{", "").replace("}", "").replace("'", "").replace("notes:", "").strip()
            return add_unit_inputs
        else:
            if any(k in add_unit_inputs.keys() for k in ('chronologies', 'sampleages')):
                key = ('chronologies' if 'chronologies' in add_unit_inputs.keys()
                        else 'sampleages' if 'sampleages' in  add_unit_inputs.keys() else None)
                if isinstance(add_unit_inputs[key].values(), list):
                    add_unit_inputs[key] = {name: chron
                                        for name, chron in add_unit_inputs[key].items()
                                        if not all(all(v is None 
                                                        for v in chron[k]) 
                                                        for k in ('ageyounger', 'ageolder', 
                                                                    'age', 'ageboundolder', 
                                                                    'ageboundyounger'))}
                else:
                    add_unit_inputs[key] = {name: chron
                                        for name, chron in add_unit_inputs[key].items()
                                        if not all(v is None 
                                                    for v in chron.values())}
            return add_unit_inputs
    elif isinstance(table, list):
        for item in table:
            results.append(pull_params(params, yml_dict, csv_template, item))
        return results