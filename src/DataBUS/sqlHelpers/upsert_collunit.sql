CREATE OR REPLACE FUNCTION insert_speleothem_cu(_entityid numeric DEFAULT NULL,
                                                _collectionunitid integer DEFAULT NULL)
 RETURNS NULL
 LANGUAGE sql
AS $function$
    INSERT INTO ndb.speleothemcollectionunit(entityid, collectionunitid)
    VALUES (_entityid, _collectionunitid);
    RETURN;
$function$