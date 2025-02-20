import datetime
import re
from .retrieve_dict import retrieve_dict
from .clean_column import clean_column
from .clean_notes import clean_notes
 
def pull_params(params, yml_dict, csv_template, table=None, name = None):
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
            subfields = [entry for entry in yml_dict['metadata'] if entry.get('neotoma', '').startswith(f'{table}{param}.')]
            if subfields:
                for entry in subfields:
                    param_name = entry['neotoma'].replace(f'{table}', "")
                    params.append(param_name)
                params.remove(param)
        for i in params:
            valor = retrieve_dict(yml_dict, table + i)
            if len(valor) > 0:
                for count, val in enumerate(valor):
                    clean_valor = clean_column(val.get("column"), 
                                               csv_template, 
                                               clean=not val.get("rowwise"))
                    if clean_valor:
                        match val.get("type"):
                            case "date":
                                if val.get("rowwise"):
                                    clean_valor = list(map(lambda x: datetime.datetime.strptime(x.replace("/", "-"), "%Y-%m-%d").date(),
                                                           clean_valor))
                                else:
                                    clean_valor = datetime.datetime.strptime(clean_valor.replace("/", "-"), "%Y-%m-%d")
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
                            case "string":
                                clean_valor = list(map(str, clean_valor)) if val.get("rowwise") else str(clean_valor)
                                clean_valor = None if all(item == '' for item in clean_valor) and clean_valor else clean_valor
                        if i == 'notes':
                            if'notes' in add_unit_inputs:
                                add_unit_inputs[i].append({f"{val.get('column')}": clean_valor})
                            else:
                                add_unit_inputs[i] = []
                                add_unit_inputs[i].append({f"{val.get('column')}": clean_valor})
                        elif 'taxonname' in val:
                            add_unit_inputs[val['taxonname']] = clean_valor
                            if 'uncertaintybasis' in val:
                                add_unit_inputs[f"{val['taxonname']}_uncertaintybasis"] = val['uncertaintybasis']
                            if 'uncertaintyunit' in val:
                                add_unit_inputs[f"{val['taxonname']}_uncertaintyunit"] = val['uncertaintyunit']
                            if 'notes' in val:
                                add_unit_inputs[f"{val['taxonname']}_notes"] = val['notes']
                        else:
                            add_unit_inputs[i] = clean_valor
            else:
                add_unit_inputs[i] = None
        if 'notes' in add_unit_inputs.keys():
            add_unit_inputs['notes']=clean_notes(add_unit_inputs['notes'], name)
            return add_unit_inputs
        else:
            return add_unit_inputs

    elif isinstance(table, list):
        for item in table:
            results.append(pull_params(params, yml_dict, csv_template, item))
        return results