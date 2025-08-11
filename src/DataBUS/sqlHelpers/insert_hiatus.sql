CREATE OR REPLACE FUNCTION insert_hiatus(_analysisunitstart integer,
                                         _analysisunitend integer,
                                         _notes character varying DEFAULT NULL::character varying)
RETURNS integer
LANGUAGE sql
AS $function$
    INSERT INTO ndb.hiatuses(analysisunitstart, analysisunitend, notes)
    VALUES (_analysisunitstart, _analysisunitend, _notes)
    RETURNING hiatusid;
$function$;