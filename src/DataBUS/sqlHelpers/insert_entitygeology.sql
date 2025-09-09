CREATE OR REPLACE FUNCTION insert_entitygeology(_entityid numeric,
                                                _speleothemgeologyid numeric DEFAULT NULL,
                                                _notes text DEFAULT NULL)
RETURNS void
LANGUAGE sql
AS $function$ 
    INSERT INTO ndb.entitygeology(entityid, speleothemgeologyid, notes)
    VALUES (_entityid, _speleothemgeologyid, _notes)
$function$