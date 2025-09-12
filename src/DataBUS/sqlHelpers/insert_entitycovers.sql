CREATE OR REPLACE FUNCTION insert_entitycovers(_entityid integer,
                                               _entitycoverid integer,
                                               _entitycoverthickness numeric DEFAULT NULL,
                                               _entitycoverunits numeric DEFAULT NULL)
RETURNS void
LANGUAGE sql
AS $function$ 
    INSERT INTO ndb.entitycovers(entityid, entitycoverid, entitycoverthickness, entitycoverunits)
    VALUES (_entityid, _entitycoverid, _entitycoverthickness, _entitycoverunits)
$function$