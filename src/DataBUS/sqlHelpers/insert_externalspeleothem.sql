CREATE OR REPLACE FUNCTION insert_externalspeleothem(_entityid integer,
                                                     _externalid text,
                                                     _extdatabaseid integer,
                                                     _externaldescription character varying DEFAULT NULL::character varying)
RETURNS void
LANGUAGE sql
AS $function$ 
    INSERT INTO ndb.externalspeleothemdata(entityid, externalid, extdatabaseid, externaldescription)
    VALUES (_entityid, _externalid, _extdatabaseid, _externaldescription);
$function$