CREATE OR REPLACE FUNCTION insert_entityvegetationcover(_entityid numeric,
                                                        _vegetationcovertypeid numeric,
                                                        _vegetationcoverpercent numeric DEFAULT NULL,
                                                        _vegetationcovernotes character varying DEFAULT NULL::character varying)in
RETURNS void
LANGUAGE sql
AS $function$ 
    INSERT INTO ndb.entityvegetationcover(entityid, vegetationcovertypeid, vegetationcoverpercent, vegetationcovernotes)
    VALUES (_entityid, _vegetationcovertypeid, _vegetationcoverpercent, _vegetationcovernotes);
$function$