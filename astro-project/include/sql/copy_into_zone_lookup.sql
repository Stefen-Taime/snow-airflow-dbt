-- COPY INTO for Taxi Zone Lookup (CSV)
-- Ref: spec.md Section 3.4
COPY INTO RAW.TLC_REFERENCE.taxi_zone_lookup (LocationID, Borough, Zone, service_zone)
FROM @RAW.TLC_REFERENCE.tlc_ref_stage/
FILE_FORMAT = (FORMAT_NAME = 'RAW.TLC_REFERENCE.csv_format')
ON_ERROR = ABORT_STATEMENT;
