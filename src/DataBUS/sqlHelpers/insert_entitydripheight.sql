CREATE OR REPLACE FUNCTION insert_entitydripheight(_entityid integer,
                                                   _speleothemdriptypeid integer,
                                                   _entitydripheight numeric DEFAULT NULL,
                                                   _entitydripheightunit numeric DEFAULT NULL)
RETURNS void
LANGUAGE sql
AS $function$ 
    INSERT INTO ndb.entitydripheight(entityid, speleothemdriptypeid, entitydripheight, entitydripheightunit)
    VALUES (_entityid, _speleothemdriptypeid, _entitydripheight, _entitydripheightunit)
$function$