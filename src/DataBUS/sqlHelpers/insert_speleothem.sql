CREATE OR REPLACE FUNCTION insertspeleothem(_siteid numeric,
                                            _entityid numeric DEFAULT NULL,
                                            _entityname text DEFAULT NULL,
                                            _monitoring boolean DEFAULT NULL,
                                            _rockageid numeric DEFAULT NULL,
                                            _entrancedistance numeric DEFAULT NULL,
                                            _entrancedistanceunits text DEFAULT NULL,
                                            _speleothemtype integer DEFAULT NULL)
 RETURNS integer
 LANGUAGE sql
AS $function$
    INSERT INTO ndb.speleothems(siteid, entityid, entityname, monitoring,
                                rockageid, entrancedistance, entrancedistanceunits,
                                speleothemtype)
    VALUES (_siteid, _entityid, _entityname, _monitoring,
            _rockageid, _entrancedistance, _entrancedistanceunits,
            _speleothemtype);
    RETURN;
$function$