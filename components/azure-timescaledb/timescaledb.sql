CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE iotdata (
  ts          TIMESTAMPTZ       NOT NULL,
  dev         CHAR(7)           NOT NULL,
  dsn         INTEGER           NOT NULL,
  mod         CHAR(5)           NOT NULL,
  eid         UUID              NOT NULL,
  cnt         INTEGER           NOT NULL,
  vol         SMALLINT          NOT NULL,
  cur         REAL              NOT NULL,
  spe         SMALLINT          NOT NULL,
  mas         SMALLINT          NOT NULL,
  odo         INT               NOT NULL,
  soc         REAL              NOT NULL,
  map         REAL              NOT NULL,
  cap         SMALLINT          NOT NULL,
  lat         DOUBLE PRECISION  NOT NULL,
  lon         DOUBLE PRECISION  NOT NULL,
  acc         SMALLINT          NOT NULL,
  bra         SMALLINT          NOT NULL,
  miv         REAL              NOT NULL,
  mit         REAL              NOT NULL,
  mav         REAL              NOT NULL,
  mat         REAL              NOT NULL,
  sdf         BOOLEAN           NOT NULL,
  sig         SMALLINT          NOT NULL,
  gps         BOOLEAN           NOT NULL,
  sat         SMALLINT          NOT NULL,
  blf         BOOLEAN           NOT NULL,
  sta         CHAR(3)           NOT NULL,
  jou         TIMESTAMPTZ       NOT NULL,
  eat         TIMESTAMPTZ       NOT NULL,
  pat         TIMESTAMPTZ       NOT NULL
);

CREATE INDEX mod_index ON iotdata (ts, mod);
CREATE INDEX dev_index ON iotdata (ts, dev);
CREATE UNIQUE INDEX eid_index ON iotdata (ts, eid);

SELECT create_hypertable('iotdata', 'ts');