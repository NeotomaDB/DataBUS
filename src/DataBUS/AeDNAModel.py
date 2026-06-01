from .neotomaHelpers.utils import validate_int_values

AEDNAMODEL_PARAMS = ["sequenceid", "taxonid", "model"]


class AeDNAModel:
    """An aeDNA model record in the Neotoma database.

    Links a DNA sequence to a taxonomic interpretation under a specific
    bioinformatics model (e.g., DADA2). Supports model supersession tracking
    via supersededbymodelid.

    Attributes:
        modelid (int | None): Database ID (assigned after insertion).
        sequenceid (int | None): Sequence record identifier.
        taxonid (int | None): Taxon identifier.
        model (str | None): Model name (e.g., "DADA2").
        supersededbymodelid (int | None): ID of the model that supersedes this one.
        publicationid (int | None): Associated publication identifier.
        notes (str | None): Additional notes.

    Examples:
        >>> m = AeDNAModel(sequenceid=1, taxonid=42, model="DADA2")
        >>> m.model
        'DADA2'
    """

    def __init__(
        self,
        sequenceid=None,
        taxonid=None,
        model=None,
        supersededbymodelid=None,
        publicationid=None,
        notes=None,
    ):
        self.modelid = None
        self.sequenceid = validate_int_values(sequenceid, "sequenceid")
        self.taxonid = validate_int_values(taxonid, "taxonid")
        if model is not None and not isinstance(model, str):
            raise TypeError("model must be a string or None.")
        self.model = model
        self.supersededbymodelid = validate_int_values(supersededbymodelid, "supersededbymodelid")
        self.publicationid = validate_int_values(publicationid, "publicationid")
        if notes is not None and not isinstance(notes, str):
            raise TypeError("notes must be a string or None.")
        self.notes = notes

    def insert_to_db(self, cur):
        """Insert the aeDNA model record into the Neotoma database.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.

        Returns:
            int: The modelid assigned by the database.
        """
        model_q = """
            INSERT INTO ndb.aednamodels
                (sequenceid, taxonid, model, supersededbymodelid, publicationid, notes)
            VALUES
                (%(sequenceid)s, %(taxonid)s, %(model)s,
                 %(supersededbymodelid)s, %(publicationid)s, %(notes)s)
            RETURNING modelid;
        """
        inputs = {
            "sequenceid": self.sequenceid,
            "taxonid": self.taxonid,
            "model": self.model,
            "supersededbymodelid": self.supersededbymodelid,
            "publicationid": self.publicationid,
            "notes": self.notes,
        }
        cur.execute(model_q, inputs)
        self.modelid = cur.fetchone()[0]
        return self.modelid

    def supersede_previous(self, cur, superseeds_list):
        """Mark older aeDNA model entries as superseded by this model.

        For each model name in superseeds_list, finds existing aednamodels rows
        that share this taxonid and that model name, then sets their
        supersededbymodelid to this model's modelid.

        Args:
            cur (psycopg2.cursor): Database cursor for executing queries.
            superseeds_list (list[str]): Model names that this model supersedes.

        Returns:
            int: Total number of rows updated.
        """
        if not superseeds_list or self.modelid is None:
            return 0
        update_q = """
            UPDATE ndb.aednamodels
            SET supersededbymodelid = %(new_modelid)s
            WHERE taxonid = %(taxonid)s
              AND LOWER(model) = LOWER(%(old_model)s)
              AND supersededbymodelid IS NULL
              AND modelid != %(new_modelid)s;
        """
        total_updated = 0
        for old_model in superseeds_list:
            cur.execute(
                update_q,
                {
                    "new_modelid": self.modelid,
                    "taxonid": self.taxonid,
                    "old_model": old_model,
                },
            )
            total_updated += cur.rowcount
        return total_updated

    def __str__(self):
        return (
            f"AeDNAModel(modelid={self.modelid}, sequenceid={self.sequenceid}, "
            f"taxonid={self.taxonid}, model={self.model})"
        )
