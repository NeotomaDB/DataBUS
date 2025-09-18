CREATE OR REPLACE FUNCTION insert_externalspeleothem(_entityid numeric,
                                                     _externalid character varying,
                                                     _extdatabaseid numeric,
                                                     _externaldescription character varying DEFAULT NULL::character varying)
RETURNS void
LANGUAGE sql
AS $function$ 
    INSERT INTO ndb.externalspeleothemdata(entityid, externalid, extdatabaseid, externaldescription)
    VALUES (_entityid, _externalid, _extdatabaseid, _externaldescription);
$function$