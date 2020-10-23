

CREATE DATABASE fvh_db_dev;
CREATE DATABASE fvh_db_test;
\c fvh_db_test
CREATE EXTENSION hstore;
\c fvh_db_dev
CREATE EXTENSION hstore;

CREATE TABLE asset_data_hstore (
   id serial primary key,
   asset_name VARCHAR (255),
   asset_data hstore
);

  INSERT INTO asset_data_hstore (asset_name, asset_data) VALUES (
 'TA120-T246174-N',
 'sensor    => "TA120-T246174",
  observation_type     => "OM_Measurement",
  observed_property  => "Sound Pressure Level",
UoH => "{“name”:”Sound Pressure Level”, “symbol”:”LAeq”, “definition”:”http://unitsofmeasure.org/ucum.html#para-46” }"  ');


  INSERT INTO asset_data_hstore (asset_name, asset_data) VALUES (
 'TA120-T246174-O',
 'sensor    => "TA120-T246174",
  observation_type     => "OM_TruthObservation",
  observed_property  => "Overload",
UoH => NULL  ');


  INSERT INTO asset_data_hstore (asset_name, asset_data) VALUES (
 'TA120-T246174-U',
 'sensor    => "TA120-T246174",
  observation_type     => "OM_TruthObservation",
  observed_property  => "Underrange",
UoH => NULL ');


  INSERT INTO asset_data_hstore (asset_name, asset_data) VALUES (
 'TA120-T246174-S',
 'sensor    => "TA120-T246174",
  observation_type     => "OM_Observation",
  observed_property  => "Sound Pressure Level",
UoH => "{“name”:”Sound Pressure Level 30 s”, “symbol”:”LAeq1s”, “definition”:”http://unitsofmeasure.org/ucum.html#para-46” }"  ');


CREATE TABLE observation
(
	id bigserial NOT NULL,
	phenomenontime_begin timestamp without time zone NULL,
	phenomenontime_end timestamp without time zone NULL,
	resulttime timestamp without time zone NULL,
	result text NULL,
	resultquality text NULL,
	validtime_begin timestamp without time zone NULL,
	validtime_end timestamp without time zone NULL,
	parameters JSON NULL,
	datastream_id bigint NULL,
	featureofintrest_link text NULL
)
;

INSERT INTO observation(resulttime, result) VALUES ('2010-2-1'::timestamp, '10.0');
INSERT INTO observation(resulttime, result) VALUES ('2011-2-1'::timestamp, '11.0');
INSERT INTO observation(resulttime, result) VALUES ('2012-2-1'::timestamp, '12.0');
INSERT INTO observation(resulttime, result) VALUES ('2013-2-1'::timestamp, '13.0');
INSERT INTO observation(resulttime, result) VALUES ('2014-2-1'::timestamp, '14.0');


