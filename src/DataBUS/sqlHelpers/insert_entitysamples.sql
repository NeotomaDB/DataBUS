CREATE OR REPLACE FUNCTION insert_entitysamples(
    _entityid INT,
    _organics BOOLEAN DEFAULT FALSE,
    _fluid_inclusions BOOLEAN DEFAULT FALSE,
    _mineralogy_petrology_fabric BOOLEAN DEFAULT FALSE,
    _clumped_isotopes BOOLEAN DEFAULT FALSE,
    _noble_gas_temperatures BOOLEAN DEFAULT FALSE,
    _C14 BOOLEAN DEFAULT FALSE,
    _ODL BOOLEAN DEFAULT FALSE
)
RETURNS void
LANGUAGE sql
AS $function$ 
    INSERT INTO ndb.entitysamples(entityid, organics, fluidinclusions, 
    mineralogypetrologyfabric, clumpedisotopes, noblegastemperatures, c14, odl)
    VALUES (_entityid, _organics, _fluid_inclusions, _mineralogy_petrology_fabric,
    _clumped_isotopes, _noble_gas_temperatures, _C14, _ODL);
$function$