CREATE OR REPLACE FUNCTION insert_speleothem_cu(_entityid numeric DEFAULT NULL,
                                                _collectionunitid integer DEFAULT NULL,
                                                _persistid text DEFAULT NULL)
RETURNS void
LANGUAGE sql
AS $function$
    INSERT INTO ndb.speleothemcollectionunits(entityid, collectionunitid, persistid)
    VALUES (_entityid, _collectionunitid, _persistid);
$function$