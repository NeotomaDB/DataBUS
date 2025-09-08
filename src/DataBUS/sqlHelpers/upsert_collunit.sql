CREATE OR REPLACE FUNCTION upsert_collunit(_collectionunitid integer, 
                                           _handle character varying, 
                                           _siteid integer, 
                                           _colltypeid integer DEFAULT NULL::integer, 
                                           _depenvtid integer DEFAULT NULL::integer, 
                                           _collunitname character varying DEFAULT NULL::character varying, 
                                           _colldate date DEFAULT NULL::date, 
                                           _colldevice character varying DEFAULT NULL::character varying, 
                                           _gpslatitude double precision DEFAULT NULL::double precision, 
                                           _gpslongitude double precision DEFAULT NULL::double precision, 
                                           _gpsaltitude double precision DEFAULT NULL::double precision, 
                                           _gpserror double precision DEFAULT NULL::double precision, 
                                           _waterdepth double precision DEFAULT NULL::double precision, 
                                           _substrateid integer DEFAULT NULL::integer, 
                                           _slopeaspect integer DEFAULT NULL::integer, 
                                           _slopeangle integer DEFAULT NULL::integer, 
                                           _location character varying DEFAULT NULL::character varying, 
                                           _notes character varying DEFAULT NULL::character varying)
RETURNS integer
LANGUAGE sql
AS $function$
    INSERT INTO ndb.collectionunits (collectionunitid, handle, siteid, colltypeid, depenvtid, collunitname, 
                                    colldate, colldevice, gpslatitude, gpslongitude, 
                                    gpsaltitude, gpserror, waterdepth, substrateid, 
                                    slopeaspect, slopeangle, location, notes)
    VALUES (_collectionunitid, _handle, _siteid, _colltypeid, _depenvtid, _collunitname, _colldate, _colldevice, 
            _gpslatitude, _gpslongitude, _gpsaltitude, _gpserror, _waterdepth, _substrateid, 
            _slopeaspect, _slopeangle, _location, _notes)
    ON CONFLICT (collectionunitid) DO UPDATE
    SET siteid = COALESCE(EXCLUDED.siteid, collectionunits.siteid), 
        colltypeid = COALESCE(EXCLUDED.colltypeid, collectionunits.colltypeid),
        depenvtid = COALESCE(EXCLUDED.depenvtid, collectionunits.depenvtid),
        collunitname = COALESCE(EXCLUDED.collunitname, collectionunits.collunitname),
        colldate = COALESCE(EXCLUDED.colldate, collectionunits.colldate),
        colldevice = COALESCE(EXCLUDED.colldevice, collectionunits.colldevice),
        gpslatitude = COALESCE(EXCLUDED.gpslatitude, collectionunits.gpslatitude),
        gpslongitude = COALESCE(EXCLUDED.gpslongitude, collectionunits.gpslongitude),
        gpsaltitude = COALESCE(EXCLUDED.gpsaltitude, collectionunits.gpsaltitude),
        gpserror = COALESCE(EXCLUDED.gpserror, collectionunits.gpserror),
        waterdepth = COALESCE(EXCLUDED.waterdepth, collectionunits.waterdepth),
        substrateid = COALESCE(EXCLUDED.substrateid, collectionunits.substrateid),
        slopeaspect = COALESCE(EXCLUDED.slopeaspect, collectionunits.slopeaspect),
        slopeangle = COALESCE(EXCLUDED.slopeangle, collectionunits.slopeangle),
        location = COALESCE(EXCLUDED.location, collectionunits.location),
        notes = COALESCE(EXCLUDED.notes, collectionunits.notes)

    RETURNING collectionunitid;
$function$