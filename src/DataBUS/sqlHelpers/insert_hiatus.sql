CREATE OR REPLACE FUNCTION insert_hiatus(_analysisunitstart integer,
                                         _analysisunitend integer,
                                         _notes text DEFAULT NULL)
RETURNS integer
LANGUAGE sql
AS $function$
    INSERT INTO ndb.hiatuses(analysisunitstart, analysisunitend, notes)
    VALUES (_analysisunitstart, _analysisunitend, _notes)
    RETURNING id;
$function$;