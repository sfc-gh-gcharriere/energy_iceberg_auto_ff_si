-- create database from listing, look for title SMART_METER_READINGS
show available listings;

-- get global name
-- GZSYZ9QXXRID
create database energy from listing GZSYZ9QXXRID;

-- access listing
select * from energy.prod.smart_meter_readings limit 100;