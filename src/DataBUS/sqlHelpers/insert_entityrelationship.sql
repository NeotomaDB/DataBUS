CREATE OR REPLACE FUNCTION insert_entityrelationship(_entityid numeric,
                                                _entitystatusid numeric,
                                                _referenceentityid numeric)
RETURNS void
LANGUAGE sql
AS $function$ 
    INSERT INTO ndb.entityrelationship(entityid, entitystatusid, referenceentityid)
    VALUES (_entityid, _entitystatusid, _referenceentityid)
$function$