CREATE OR REPLACE FUNCTION insert_hiatuschronology(_hiatusid integer,
                                               _chronologyid integer,
                                               _hiatuslength numeric,
                                               _hiatusuncertainty numeric)
RETURNS void
LANGUAGE sql
AS $function$
    INSERT INTO ndb.hiatuschronology(hiatusid, chronologyid,
                                       hiatuslength, hiatusuncertainty)
    VALUES (_hiatusid, _chronologyid, _hiatuslength, _hiatusuncertainty);
$function$; 