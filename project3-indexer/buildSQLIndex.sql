CREATE TABLE IF NOT EXISTS SpatialItems(ItemID varchar(80), LatLong POINT NOT NULL, SPATIAL INDEX(LatLong)) ENGINE=MyISAM;
INSERT INTO SpatialItems(ItemID, LatLong)
SELECT ItemID, POINT(Latitude, Longitude)
FROM Item
WHERE LENGTH(Latitude) > 0 AND LENGTH(Longitude) > 0;
