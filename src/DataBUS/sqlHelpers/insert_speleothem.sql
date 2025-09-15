CREATE OR REPLACE FUNCTION insert_speleothem(_siteid numeric,
                                             _entityid numeric,
                                             _entityname text DEFAULT NULL,
                                             _monitoring boolean DEFAULT NULL,
                                             _rockageid numeric DEFAULT NULL,
                                             _entrancedistance numeric DEFAULT NULL,
                                             _entrancedistanceunits integer DEFAULT NULL,
                                             _speleothemtypeid integer DEFAULT NULL)
RETURNS void
LANGUAGE sql
AS $function$ 
    INSERT INTO ndb.speleothems(siteid, entityid, entityname, monitoring,
                                rockageid, entrancedistance, entrancedistanceunits,
                                speleothemtypeid)
    VALUES (_siteid, _entityid, _entityname, _monitoring,
            _rockageid, _entrancedistance, _entrancedistanceunits,
            _speleothemtypeid);
$function$