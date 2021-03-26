

CREATE DATABASE fvh_db_dev;
CREATE DATABASE fvh_db_test;

/* Drop Tables */

DROP TABLE IF EXISTS datastream CASCADE
;

DROP TABLE IF EXISTS featureofintrest CASCADE
;

DROP TABLE IF EXISTS historicallocation CASCADE
;

DROP TABLE IF EXISTS location CASCADE
;

DROP TABLE IF EXISTS observation CASCADE
;

DROP TABLE IF EXISTS observedproperty CASCADE
;

DROP TABLE IF EXISTS sensor CASCADE
;

DROP TABLE IF EXISTS thing CASCADE
;

DROP TABLE IF EXISTS thingstolocation CASCADE
;

/* Create Tables */

CREATE TABLE datastream
(
	id bigserial NOT NULL,
	name text NULL,
	description text NULL,
	observationtype text NULL,
	unitofmeasurement text NULL,
	observedarea geometry(polygon) NULL,
	phenomenontime_begin timestamp without time zone NULL,
	phenomenontime_end timestamp without time zone NULL,
	resulttime_begin timestamp without time zone NULL,
	resulttime_end timestamp without time zone NULL,
	sensor_link text NULL,
	thing_link text NULL,
	observedproperty_link text NULL,
    sensor_id bigint,
    thing_id bigint
)
;

CREATE TABLE featureofinterest (
   id bigserial primary key,
   name text NOT NULL,
   description text NULL,
   encodingtype text NOT NULL,
   feature text NOT NULL
);

CREATE TABLE historicallocation
(
	time timestamp without time zone NULL,
	location_link text NULL,
	thing_link text NULL
)
;

CREATE TABLE location
(
	link text NOT NULL
)
;

CREATE TABLE observation
(
	id bigserial NOT NULL,
	phenomenontime_begin timestamp without time zone NULL,
	phenomenontime_end timestamp without time zone NULL,
	resulttime timestamp without time zone NULL,
	result text NULL,
	resultquality JSON NULL,
	validtime_begin timestamp without time zone NULL,
	validtime_end timestamp without time zone NULL,
	parameters text NULL,
	datastream_id bigint NULL,
	featureofinterest_id text NULL
)
;

CREATE TABLE observedproperty
(
	link text NOT NULL
)
;

CREATE TABLE sensor
(
	link text NOT NULL,
    id bigserial NOT NULL
)
;

CREATE TABLE thing
(
	link text NOT NULL,
    id bigserial NOT NULL
)
;

CREATE TABLE thingstolocation
(
	thing_link text NOT NULL,
	location_link text NOT NULL
)
;

/* Create Primary Keys, Indexes, Uniques, Checks */

ALTER TABLE datastream ADD CONSTRAINT "PK_datastream"
	PRIMARY KEY (id)
;

ALTER TABLE observation ADD CONSTRAINT "PK_observation"
	PRIMARY KEY (id)
;

ALTER TABLE thing ADD CONSTRAINT "PK_thing"
	PRIMARY KEY (id)
;

ALTER TABLE sensor ADD CONSTRAINT "PK_sensor"
	PRIMARY KEY (id)
;

ALTER TABLE featureofinterest ADD CONSTRAINT "PK_featureofinterest"
	PRIMARY KEY (id)
;

/* Create Table Comments, Sequences for Autonumber Columns */

/* populate tables for testing */

INSERT INTO featureofinterest(name,description,encodingtype,feature) VALUES (
    'Korkeasaari Zoo',
    'Island Zoo in Helsinki',
    'application/vnd.geo+json',
    '{"type":"Feature","geometry":{"type":"Point","coordinates":[60.181069, 24.990982]}}'
);

INSERT INTO featureofinterest(name,description,encodingtype,feature) VALUES (
    'Viikki Environmental House',
    'high-performance office building in Helsinki',
    'application/vnd.geo+json',
    '{"type":"Feature","geometry":{"type":"Point","coordinates":[60.2250606,25.0169025]}}'
);

INSERT INTO thing(link) VALUES (
    "inverter"
);

INSERT INTO sensor(link) VALUES (
    "voltage"
);

INSERT INTO sensor(link) VALUES (
    "current"
);

INSERT INTO datastream(description, sensor_id, thing_id) VALUES (
    "voltage from inverter",
    1,
    1
);

INSERT INTO datastream(description, sensor_id, thing_id) VALUES (
    "current from inverter",
    2,
    1
);


INSERT INTO observation(resulttime, result, datastream_id, featureofinterest_id) VALUES (
    now(),
    "2",
    1,
    1
);

INSERT INTO observation(resulttime, result, datastream_id, featureofinterest_id) VALUES (
    now(),
    "3",
    1,
    1
);

INSERT INTO observation(resulttime, result, datastream_id, featureofinterest_id) VALUES (
    now(),
    "3",
    2,
    1
);

INSERT INTO observation(resulttime, result, datastream_id, featureofinterest_id) VALUES (
    now(),
    "4",
    2,
    1
);



