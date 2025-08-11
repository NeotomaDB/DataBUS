CREATE OR REPLACE FUNCTION insert_uthseries(_geochronid integer,
                                            _decayconstantid integer,
                                            _ratio230th232th numeric DEFAULT NULL,
                                            _ratiouncertainty230th232th numeric DEFAULT NULL,
                                            _activity230th238u numeric DEFAULT NULL,
                                            _activityuncertainty230th238u numeric DEFAULT NULL,
                                            _activity234u238u numeric DEFAULT NULL,
                                            _activityuncertainty234u238u numeric DEFAULT NULL,
                                            _iniratio230th232th numeric DEFAULT NULL,
                                            _iniratiouncertainty230th232th numeric DEFAULT NULL)
RETURNS void
LANGUAGE sql
AS $function$
    INSERT INTO ndb.uraniumseries(geochronid, decayconstantid,
                              ratio230th232th, ratiouncertainty230th232th,
                              activity230th238u, activityuncertainty230th238u,
                              activity234u238u, activityuncertainty234u238u,
                              iniratio230th232th, iniratiouncertainty230th232th)
    VALUES (_geochronid, _decayconstantid,
            _ratio230th232th, _ratiouncertainty230th232th,
            _activity230th238u, _activityuncertainty230th238u,
            _activity234u238u, _activityuncertainty234u238u,
            _iniratio230th232th, _iniratiouncertainty230th232th);
$function$;