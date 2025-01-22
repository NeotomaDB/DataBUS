import DataBUS.neotomaHelpers as nh
from DataBUS import Repository, Response


def insert_dataset_repository(cur, yml_dict, csv_file, uploader):
    """ """
    response = Response()
    params = ["acronym", "repo", "recdatecreated", "recdatemodified", "notes"]
    inputs = nh.clean_inputs(
        nh.pull_params(params, yml_dict, csv_file, "ndb.repository")
    )

    if inputs["repo"] is not None:
        try:
            repo = Repository(
                datasetid=uploader["datasetid"].datasetid,
                repositoryid=inputs["repo"],
                notes=inputs["notes"],
            )
            response.valid.append(True)
        except Exception:
            repo = Repository(
                datasetid=uploader["datasetid"].datasetid, repositoryid=None, notes=None
            )
            response.message.append("✗ Repo cannot be created.")
            response.valid.append(False)
        finally:
            try:
                repo.insert_to_db(cur)
                response.message.append(
                    f"✔ Inserting new ({repo.datasetid}, {repo.repositoryid} "
                    f"repositoryid, datasetid) row."
                )
                response.valid.append(True)
            except Exception as e:
                print(e)
                response.message.append(f"✗ Repo cannot be inserted: {e}.")
                response.valid.append(False)
    else:
        response.message.append("✔ Repository information is not available.")
        response.valid.append(True)

    response.validAll = all(response.valid)
    return response
