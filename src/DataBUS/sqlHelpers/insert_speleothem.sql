CREATE OR REPLACE FUNCTION insert_speleothem(_siteid numeric,
                                             _entityname text DEFAULT NULL,
                                             _monitoring boolean DEFAULT NULL,
                                             _rockageid numeric DEFAULT NULL,
                                             _entrancedistance numeric DEFAULT NULL,
                                             _entrancedistanceunits integer DEFAULT NULL,
                                             _speleothemtypeid integer DEFAULT NULL)
RETURNS integer
LANGUAGE sql
AS $function$ 
    INSERT INTO ndb.speleothems(siteid, entityname, monitoring,
                                rockageid, entrancedistance, entrancedistanceunits,
                                speleothemtypeid)
    VALUES (_siteid, _entityname, _monitoring,
            _rockageid, _entrancedistance, _entrancedistanceunits,
            _speleothemtypeid)
  RETURNING entityid
  $function$