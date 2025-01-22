import DataBUS.neotomaHelpers as nh
from DataBUS import Response, DataUncertainty
import re
 
def insert_datauncertainty(cur, yml_dict, csv_file, uploader):
    """"""
    response = Response()

    params = ["value"]
    # Data count from uploader
    # Gets me: uncertaintyvalue , uncertaintyunit, uncertaintybasisid # add to get notes as well
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.data")
    inputs = [d for d in inputs if "uncertainty" in d]
    assert len(uploader["data"].uncertaintyinputs) == len(
        inputs
    ), "Taxa Uncertainties and extracted uncertainties do not match."
    taxacounter = 0
    
    for i, uncertainty in enumerate(inputs):
        assert len(uncertainty["uncertainty"]) == len(
            uploader["data"].uncertaintyinputs[i]["dataid"]
        ), ("Number of " "uncertainty values does not match number of data values")
        # SQL uncertainty basis ID

        uncertainty["uncertaintybasis"] = re.sub(r'(\d+)SD', r'\1 Standard Deviation', uncertainty["uncertaintybasis"])
        # Replace SE with "Standard Error"
        uncertainty["uncertaintybasis"] = re.sub(r'(\d+)SE', r'\1 Standard Error', uncertainty["uncertaintybasis"])

        if uncertainty["uncertaintybasis"]:
            basis_q = """
                    SELECT uncertaintybasisid from ndb.uncertaintybases
                    WHERE LOWER(uncertaintybasis) = %(uncertaintybasis)s
                    """
            cur.execute(basis_q, {"uncertaintybasis": uncertainty["uncertaintybasis"].lower()})
            uncertainty["uncertaintybasisid"] = cur.fetchone()
            if uncertainty["uncertaintybasisid"]:
                uncertainty["uncertaintybasisid"] = uncertainty["uncertaintybasisid"][0]
        else:
            uncertainty["uncertaintybasisid"] = None

        for j in range(len(uncertainty["uncertainty"])):
            taxacounter += 1
            # SQL uncertaintyunitid
            get_vunitsid = """SELECT variableunitsid FROM ndb.variableunits 
                                WHERE LOWER(variableunits) = %(units)s;"""
            cur.execute(get_vunitsid, {"units": uncertainty["unitcolumn"][j].lower()})
            vunitsid = cur.fetchone()  # This is to get varunitsid
            if vunitsid:
                vunitsid = vunitsid[0]

            try:
                du = DataUncertainty(
                    dataid=int(
                        uploader["data"].uncertaintyinputs[i]["dataid"][j]
                    ),  # retrieve correct ID for insert
                    uncertaintyvalue=uncertainty["uncertainty"][j],
                    uncertaintyunitid=int(vunitsid),  # False - need to get the ID first
                    uncertaintybasisid=uncertainty[
                        "uncertaintybasisid"
                    ],  # Need to get from leadmodels
                    notes=uncertainty["uncertaintybasis_notes"],
                )
                response.valid.append(True)
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗ Data Uncertainty cannot be created: {e}")
                du = DataUncertainty()

            finally:
                try:
                    du.insert_to_db(cur)
                    response.valid.append(True)
                    #response.message.append(f"✔ Added Data Uncertainty.")
                except Exception as e:
                    response.valid.append(False)
                    response.message.append(f"✗ Cannot add Data Uncertainty: {e}")
    response.validAll = all(response.valid)
    return response
