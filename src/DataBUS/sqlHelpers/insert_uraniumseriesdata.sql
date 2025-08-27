CREATE OR REPLACE FUNCTION insert_uraniumseriesdata(_geochronid integer,
                                            _dataid integer)
RETURNS void
LANGUAGE sql
AS $function$
    INSERT INTO ndb.uraniumseriesdata(geochronid, dataid)
    VALUES (_geochronid, _dataid);
$function$;
