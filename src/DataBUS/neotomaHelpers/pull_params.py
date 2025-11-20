import datetime
import re
from .retrieve_dict import retrieve_dict
from .clean_column import clean_column
from .clean_notes import clean_notes
 
def pull_params(params, yml_dict, csv_template, table=None, name = None, values = False):
    """
    Pull parameters associated with an insert statement from the yml/csv tables.
    Args:
        params (_list_): A list of strings for the columns needed to generate the insert statement.
        yml_dict (_dict_): A `dict` returned by the YAML template.
        csv_template (_dict_): The csv file with the required data to be uploaded.
        table (_string_): The name of the table the parameters are being drawn for.
    Returns:
        _dict_: Cleaned and repeated valors for input into a ts.insert functions.
    """
    results = []
    if isinstance(table, str):
        add_unit_inputs = {}
        if re.match(".*\.$", table) == None:
            table = table + "."
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
                    except KeyError:
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
                                add_unit_inputs[i].append({f"{val.get('column')}": clean_valor})
                            else:
                                add_unit_inputs[i] = []
                                add_unit_inputs[i].append({f"{val.get('column')}": clean_valor})
                        elif any(k in table for k in ('chronologies', 'sampleages')):
                            key = ('chronologies' if 'chronologies' in table 
                                   else 'sampleages' if 'sampleages' in table else None)
                            if key not in add_unit_inputs:
                                add_unit_inputs[key] = {}
                            if not all(x is None for x in clean_valor): 
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
        if 'notes' in add_unit_inputs.keys():
            add_unit_inputs['notes']=clean_notes(add_unit_inputs['notes'], name)
            return add_unit_inputs
        else:
            if any(k in add_unit_inputs.keys() for k in ('chronologies', 'sampleages')):
                key = ('chronologies' if 'chronologies' in add_unit_inputs.keys()
                        else 'sampleages' if 'sampleages' in  add_unit_inputs.keys() else None)
                add_unit_inputs[key] = {name: chron
                                            for name, chron in add_unit_inputs[key].items()
                                            if not all(all(v is None 
                                                            for v in chron[key]) 
                                                            for key in ('age', 'ageyounger', 'ageolder', 
                                                                        'age', 'ageboundolder', 
                                                                        'ageboundyounger'))}
            return add_unit_inputs
    elif isinstance(table, list):
        for item in table:
            results.append(pull_params(params, yml_dict, csv_template, item))
        return results