CREATE OR REPLACE FUNCTION insert_entitylandusecover(_entityid numeric,
                                                     _landusecovertypeid numeric,
                                                     _landusecoverpercent numeric DEFAULT NULL,
                                                     _landusecovernotes character varying DEFAULT NULL::character varying
                                                     )
RETURNS void
LANGUAGE sql
AS $function$ 
    INSERT INTO ndb.entitylandusecover(entityid, landusecovertypeid, landusecoverpercent, landusecovernotes)
    VALUES (_entityid, _landusecovertypeid, _landusecoverpercent, _landusecovernotes);
$function$