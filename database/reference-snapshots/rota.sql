--
-- PostgreSQL database dump
--

\restrict TLai1jdybqWBZ6XSCYedcfTQeOwfDgg0q3QPLYqFAjNDXUowCRhUSVn4Y38KjeK

-- Dumped from database version 17.6
-- Dumped by pg_dump version 17.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: timescaledb; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;


--
-- Name: EXTENSION timescaledb; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION timescaledb IS 'Enables scalable inserts and complex queries for time-series data (Community Edition)';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: set_proxy_health_transition_preserved(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.set_proxy_health_transition_preserved() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
			BEGIN
			  NEW.transition_preserved := NOT (
			    NEW.applied = true
			    AND NEW.previous_status IS DISTINCT FROM NEW.resulting_status
			  );
			  RETURN NEW;
			END;
			$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: _compressed_hypertable_2; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_2 (
);


--
-- Name: _compressed_hypertable_4; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_4 (
);


--
-- Name: logs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.logs (
    id bigint NOT NULL,
    "timestamp" timestamp without time zone DEFAULT now() NOT NULL,
    level character varying(20) NOT NULL,
    message text NOT NULL,
    details text,
    metadata jsonb
);


--
-- Name: _hyper_1_12_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._hyper_1_12_chunk (
    CONSTRAINT constraint_9 CHECK ((("timestamp" >= '2026-07-30 00:00:00'::timestamp without time zone) AND ("timestamp" < '2026-08-06 00:00:00'::timestamp without time zone)))
)
INHERITS (public.logs);


--
-- Name: _hyper_1_16_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._hyper_1_16_chunk (
    CONSTRAINT constraint_11 CHECK ((("timestamp" >= '2026-08-06 00:00:00'::timestamp without time zone) AND ("timestamp" < '2026-08-13 00:00:00'::timestamp without time zone)))
)
INHERITS (public.logs);


--
-- Name: _hyper_1_20_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._hyper_1_20_chunk (
    CONSTRAINT constraint_13 CHECK ((("timestamp" >= '2026-08-13 00:00:00'::timestamp without time zone) AND ("timestamp" < '2026-08-20 00:00:00'::timestamp without time zone)))
)
INHERITS (public.logs);


--
-- Name: _hyper_1_5_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._hyper_1_5_chunk (
    CONSTRAINT constraint_5 CHECK ((("timestamp" >= '2026-07-16 00:00:00'::timestamp without time zone) AND ("timestamp" < '2026-07-23 00:00:00'::timestamp without time zone)))
)
INHERITS (public.logs);


--
-- Name: _hyper_1_8_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._hyper_1_8_chunk (
    CONSTRAINT constraint_7 CHECK ((("timestamp" >= '2026-07-23 00:00:00'::timestamp without time zone) AND ("timestamp" < '2026-07-30 00:00:00'::timestamp without time zone)))
)
INHERITS (public.logs);


--
-- Name: proxy_requests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_requests (
    id bigint NOT NULL,
    "timestamp" timestamp without time zone DEFAULT now() NOT NULL,
    proxy_id integer,
    proxy_address character varying(255) NOT NULL,
    method character varying(10) NOT NULL,
    url text,
    status_code integer,
    response_time integer,
    success boolean NOT NULL,
    error text
);


--
-- Name: _hyper_3_13_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._hyper_3_13_chunk (
    CONSTRAINT constraint_10 CHECK ((("timestamp" >= '2026-07-30 00:00:00'::timestamp without time zone) AND ("timestamp" < '2026-08-06 00:00:00'::timestamp without time zone)))
)
INHERITS (public.proxy_requests);


--
-- Name: _hyper_3_17_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._hyper_3_17_chunk (
    CONSTRAINT constraint_12 CHECK ((("timestamp" >= '2026-08-06 00:00:00'::timestamp without time zone) AND ("timestamp" < '2026-08-13 00:00:00'::timestamp without time zone)))
)
INHERITS (public.proxy_requests);


--
-- Name: _hyper_3_21_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._hyper_3_21_chunk (
    CONSTRAINT constraint_14 CHECK ((("timestamp" >= '2026-08-13 00:00:00'::timestamp without time zone) AND ("timestamp" < '2026-08-20 00:00:00'::timestamp without time zone)))
)
INHERITS (public.proxy_requests);


--
-- Name: _hyper_3_2_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._hyper_3_2_chunk (
    CONSTRAINT constraint_2 CHECK ((("timestamp" >= '2026-07-02 00:00:00'::timestamp without time zone) AND ("timestamp" < '2026-07-09 00:00:00'::timestamp without time zone)))
)
INHERITS (public.proxy_requests);


--
-- Name: _hyper_3_4_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._hyper_3_4_chunk (
    CONSTRAINT constraint_4 CHECK ((("timestamp" >= '2026-07-09 00:00:00'::timestamp without time zone) AND ("timestamp" < '2026-07-16 00:00:00'::timestamp without time zone)))
)
INHERITS (public.proxy_requests);


--
-- Name: _hyper_3_6_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._hyper_3_6_chunk (
    CONSTRAINT constraint_6 CHECK ((("timestamp" >= '2026-07-16 00:00:00'::timestamp without time zone) AND ("timestamp" < '2026-07-23 00:00:00'::timestamp without time zone)))
)
INHERITS (public.proxy_requests);


--
-- Name: _hyper_3_9_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._hyper_3_9_chunk (
    CONSTRAINT constraint_8 CHECK ((("timestamp" >= '2026-07-23 00:00:00'::timestamp without time zone) AND ("timestamp" < '2026-07-30 00:00:00'::timestamp without time zone)))
)
INHERITS (public.proxy_requests);


--
-- Name: compress_hyper_2_15_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal.compress_hyper_2_15_chunk (
    _ts_meta_count integer,
    level character varying(20),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp without time zone,
    _ts_meta_max_1 timestamp without time zone,
    "timestamp" _timescaledb_internal.compressed_data,
    message _timescaledb_internal.compressed_data,
    details _timescaledb_internal.compressed_data,
    metadata _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_15_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_15_chunk ALTER COLUMN level SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_15_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_15_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_15_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_15_chunk ALTER COLUMN "timestamp" SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_15_chunk ALTER COLUMN message SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_15_chunk ALTER COLUMN message SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_15_chunk ALTER COLUMN details SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_15_chunk ALTER COLUMN details SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_15_chunk ALTER COLUMN metadata SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_15_chunk ALTER COLUMN metadata SET STORAGE EXTENDED;


--
-- Name: compress_hyper_2_19_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal.compress_hyper_2_19_chunk (
    _ts_meta_count integer,
    level character varying(20),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp without time zone,
    _ts_meta_max_1 timestamp without time zone,
    "timestamp" _timescaledb_internal.compressed_data,
    message _timescaledb_internal.compressed_data,
    details _timescaledb_internal.compressed_data,
    metadata _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_19_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_19_chunk ALTER COLUMN level SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_19_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_19_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_19_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_19_chunk ALTER COLUMN "timestamp" SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_19_chunk ALTER COLUMN message SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_19_chunk ALTER COLUMN message SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_19_chunk ALTER COLUMN details SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_19_chunk ALTER COLUMN details SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_19_chunk ALTER COLUMN metadata SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_19_chunk ALTER COLUMN metadata SET STORAGE EXTENDED;


--
-- Name: compress_hyper_2_23_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal.compress_hyper_2_23_chunk (
    _ts_meta_count integer,
    level character varying(20),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp without time zone,
    _ts_meta_max_1 timestamp without time zone,
    "timestamp" _timescaledb_internal.compressed_data,
    message _timescaledb_internal.compressed_data,
    details _timescaledb_internal.compressed_data,
    metadata _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_23_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_23_chunk ALTER COLUMN level SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_23_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_23_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_23_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_23_chunk ALTER COLUMN "timestamp" SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_23_chunk ALTER COLUMN message SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_23_chunk ALTER COLUMN message SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_23_chunk ALTER COLUMN details SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_23_chunk ALTER COLUMN details SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_23_chunk ALTER COLUMN metadata SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_2_23_chunk ALTER COLUMN metadata SET STORAGE EXTENDED;


--
-- Name: compress_hyper_4_10_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal.compress_hyper_4_10_chunk (
    _ts_meta_count integer,
    proxy_id integer,
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp without time zone,
    _ts_meta_max_1 timestamp without time zone,
    "timestamp" _timescaledb_internal.compressed_data,
    proxy_address _timescaledb_internal.compressed_data,
    method _timescaledb_internal.compressed_data,
    url _timescaledb_internal.compressed_data,
    status_code _timescaledb_internal.compressed_data,
    response_time _timescaledb_internal.compressed_data,
    _ts_meta_min_2 boolean,
    _ts_meta_max_2 boolean,
    success _timescaledb_internal.compressed_data,
    error _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN proxy_id SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN "timestamp" SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN proxy_address SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN proxy_address SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN method SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN method SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN url SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN url SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN status_code SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN response_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN success SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN error SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_10_chunk ALTER COLUMN error SET STORAGE EXTENDED;


--
-- Name: compress_hyper_4_14_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal.compress_hyper_4_14_chunk (
    _ts_meta_count integer,
    proxy_id integer,
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp without time zone,
    _ts_meta_max_1 timestamp without time zone,
    "timestamp" _timescaledb_internal.compressed_data,
    proxy_address _timescaledb_internal.compressed_data,
    method _timescaledb_internal.compressed_data,
    url _timescaledb_internal.compressed_data,
    status_code _timescaledb_internal.compressed_data,
    response_time _timescaledb_internal.compressed_data,
    _ts_meta_min_2 boolean,
    _ts_meta_max_2 boolean,
    success _timescaledb_internal.compressed_data,
    error _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN proxy_id SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN "timestamp" SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN proxy_address SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN proxy_address SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN method SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN method SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN url SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN url SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN status_code SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN response_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN success SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN error SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_14_chunk ALTER COLUMN error SET STORAGE EXTENDED;


--
-- Name: compress_hyper_4_18_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal.compress_hyper_4_18_chunk (
    _ts_meta_count integer,
    proxy_id integer,
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp without time zone,
    _ts_meta_max_1 timestamp without time zone,
    "timestamp" _timescaledb_internal.compressed_data,
    proxy_address _timescaledb_internal.compressed_data,
    method _timescaledb_internal.compressed_data,
    url _timescaledb_internal.compressed_data,
    status_code _timescaledb_internal.compressed_data,
    response_time _timescaledb_internal.compressed_data,
    _ts_meta_min_2 boolean,
    _ts_meta_max_2 boolean,
    success _timescaledb_internal.compressed_data,
    error _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN proxy_id SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN "timestamp" SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN proxy_address SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN proxy_address SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN method SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN method SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN url SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN url SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN status_code SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN response_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN success SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN error SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_18_chunk ALTER COLUMN error SET STORAGE EXTENDED;


--
-- Name: compress_hyper_4_22_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal.compress_hyper_4_22_chunk (
    _ts_meta_count integer,
    proxy_id integer,
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp without time zone,
    _ts_meta_max_1 timestamp without time zone,
    "timestamp" _timescaledb_internal.compressed_data,
    proxy_address _timescaledb_internal.compressed_data,
    method _timescaledb_internal.compressed_data,
    url _timescaledb_internal.compressed_data,
    status_code _timescaledb_internal.compressed_data,
    response_time _timescaledb_internal.compressed_data,
    _ts_meta_min_2 boolean,
    _ts_meta_max_2 boolean,
    success _timescaledb_internal.compressed_data,
    error _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN proxy_id SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN "timestamp" SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN proxy_address SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN proxy_address SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN method SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN method SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN url SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN url SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN status_code SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN response_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN success SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN error SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_4_22_chunk ALTER COLUMN error SET STORAGE EXTENDED;


--
-- Name: admin_credentials; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.admin_credentials (
    id integer NOT NULL,
    username character varying(255) NOT NULL,
    password_hash text NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: admin_credentials_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.admin_credentials_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: admin_credentials_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.admin_credentials_id_seq OWNED BY public.admin_credentials.id;


--
-- Name: bullmq_proxy_slots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bullmq_proxy_slots (
    slot_name text NOT NULL,
    role text NOT NULL,
    slot_no integer NOT NULL,
    pool_id integer NOT NULL,
    user_id integer NOT NULL,
    proxy_id integer,
    assigned_at timestamp with time zone,
    ready_after timestamp with time zone,
    worker_id text,
    lease_until timestamp with time zone,
    last_heartbeat_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: logs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.logs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: logs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.logs_id_seq OWNED BY public.logs.id;


--
-- Name: pool_alert_rules; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pool_alert_rules (
    id integer NOT NULL,
    pool_id integer NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    min_active_proxies integer DEFAULT 5 NOT NULL,
    webhook_url text NOT NULL,
    webhook_method character varying(10) DEFAULT 'POST'::character varying NOT NULL,
    last_fired_at timestamp without time zone,
    cooldown_minutes integer DEFAULT 30 NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: pool_alert_rules_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.pool_alert_rules_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pool_alert_rules_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.pool_alert_rules_id_seq OWNED BY public.pool_alert_rules.id;


--
-- Name: pool_geo_filters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pool_geo_filters (
    id integer NOT NULL,
    pool_id integer NOT NULL,
    country_code character varying(3),
    city_name character varying(100)
);


--
-- Name: pool_geo_filters_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.pool_geo_filters_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pool_geo_filters_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.pool_geo_filters_id_seq OWNED BY public.pool_geo_filters.id;


--
-- Name: pool_isp_filters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pool_isp_filters (
    id integer NOT NULL,
    pool_id integer NOT NULL,
    isp text NOT NULL
);


--
-- Name: pool_isp_filters_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.pool_isp_filters_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pool_isp_filters_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.pool_isp_filters_id_seq OWNED BY public.pool_isp_filters.id;


--
-- Name: pool_proxies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pool_proxies (
    pool_id integer NOT NULL,
    proxy_id integer NOT NULL,
    added_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: pool_tag_filters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pool_tag_filters (
    id integer NOT NULL,
    pool_id integer NOT NULL,
    tag text NOT NULL
);


--
-- Name: pool_tag_filters_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.pool_tag_filters_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pool_tag_filters_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.pool_tag_filters_id_seq OWNED BY public.pool_tag_filters.id;


--
-- Name: proxies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxies (
    id integer NOT NULL,
    address character varying(255) NOT NULL,
    protocol character varying(20) DEFAULT 'http'::character varying NOT NULL,
    username character varying(255),
    password text,
    status character varying(20) DEFAULT 'idle'::character varying NOT NULL,
    requests bigint DEFAULT 0 NOT NULL,
    successful_requests bigint DEFAULT 0 NOT NULL,
    failed_requests bigint DEFAULT 0 NOT NULL,
    avg_response_time integer DEFAULT 0,
    last_check timestamp without time zone,
    last_error text,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    country_code character varying(3),
    country_name character varying(100),
    region_name character varying(100),
    city_name character varying(100),
    latitude double precision,
    longitude double precision,
    isp character varying(255),
    geo_updated_at timestamp without time zone,
    tags text[] DEFAULT '{}'::text[] NOT NULL,
    source_id integer,
    last_seen_at timestamp without time zone,
    cooldown_until timestamp without time zone,
    last_youtube_status integer,
    last_youtube_error text,
    last_youtube_check timestamp without time zone,
    youtube_successful_requests bigint DEFAULT 0 NOT NULL,
    youtube_failed_requests bigint DEFAULT 0 NOT NULL,
    youtube_avg_response_time integer,
    youtube_avg_detail_time integer,
    youtube_failure_score double precision DEFAULT 0 NOT NULL,
    last_youtube_success timestamp without time zone,
    last_youtube_failure timestamp without time zone,
    last_rota_youtube_status integer,
    last_rota_youtube_error text,
    last_rota_youtube_check timestamp without time zone,
    failed_since timestamp with time zone,
    failure_episode_kind character varying(32),
    next_health_check_at timestamp with time zone DEFAULT now(),
    health_check_not_before timestamp with time zone,
    last_health_check_at timestamp with time zone,
    last_health_success_at timestamp with time zone,
    base_health_status character varying(16),
    youtube_health_status character varying(16),
    last_health_verdict jsonb,
    archived_at timestamp with time zone,
    archive_reason text,
    node_identity character varying(64) NOT NULL,
    country_verified_at timestamp with time zone,
    egress_identity_mode text DEFAULT 'unknown'::text NOT NULL,
    sticky_session_key_encrypted bytea,
    identity_valid_until timestamp with time zone,
    network_identity_key text NOT NULL,
    last_identity_verified_at timestamp with time zone,
    continuous_failed_since timestamp with time zone,
    revalidation_required boolean DEFAULT false NOT NULL,
    health_generation bigint DEFAULT 0 NOT NULL,
    CONSTRAINT proxies_failure_episode_kind_check CHECK (((failure_episode_kind IS NULL) OR ((failure_episode_kind)::text = ANY ((ARRAY['hard_unreachable'::character varying, 'soft_unreachable'::character varying, 'youtube_unusable'::character varying])::text[])))),
    CONSTRAINT proxies_status_check CHECK (((status)::text = ANY ((ARRAY['idle'::character varying, 'active'::character varying, 'failed'::character varying, 'archived'::character varying])::text[])))
);


--
-- Name: proxies_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.proxies_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: proxies_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.proxies_id_seq OWNED BY public.proxies.id;


--
-- Name: proxy_control_business_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_control_business_runs (
    workload_scope text NOT NULL,
    business_run_id text NOT NULL,
    next_attempt_number integer DEFAULT 1 NOT NULL,
    retry_policy_id text NOT NULL,
    retry_policy_version integer NOT NULL,
    max_route_switches_per_execution integer NOT NULL,
    max_network_attempts_per_business_run integer NOT NULL,
    budget_exhausted_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT proxy_control_business_runs_max_network_attempts_per_busi_check CHECK ((max_network_attempts_per_business_run > 0)),
    CONSTRAINT proxy_control_business_runs_max_route_switches_per_execut_check CHECK ((max_route_switches_per_execution >= 0)),
    CONSTRAINT proxy_control_business_runs_next_attempt_number_check CHECK ((next_attempt_number > 0)),
    CONSTRAINT proxy_control_business_runs_retry_policy_version_check CHECK ((retry_policy_version > 0))
);


--
-- Name: proxy_control_command_receipts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_control_command_receipts (
    workload_scope text NOT NULL,
    command_kind text NOT NULL,
    request_id text NOT NULL,
    request_hash text NOT NULL,
    resource_kind text NOT NULL,
    resource_id text NOT NULL,
    result_kind text NOT NULL,
    sanitized_result jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    retain_until timestamp with time zone NOT NULL,
    CONSTRAINT proxy_control_command_receipts_command_kind_check CHECK ((command_kind = ANY (ARRAY['claim'::text, 'renew'::text, 'begin'::text, 'observe'::text, 'complete'::text, 'release'::text])))
);


--
-- Name: proxy_control_incident_observations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_control_incident_observations (
    workload_scope text NOT NULL,
    incident_id text NOT NULL,
    observation_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: proxy_control_leases; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_control_leases (
    lease_id text NOT NULL,
    workload_scope text NOT NULL,
    slot_name text NOT NULL,
    role text NOT NULL,
    worker_id text NOT NULL,
    worker_instance_id text NOT NULL,
    identity_policy_id text NOT NULL,
    identity_policy_version integer NOT NULL,
    identity_policy_hash text NOT NULL,
    status text NOT NULL,
    claim_request_id text NOT NULL,
    claim_request_hash text NOT NULL,
    last_renew_sequence bigint DEFAULT 0 NOT NULL,
    lease_until timestamp with time zone NOT NULL,
    released_at timestamp with time zone,
    release_reason text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT proxy_control_leases_status_check CHECK ((status = ANY (ARRAY['active'::text, 'released'::text, 'expired'::text, 'fenced'::text])))
);


--
-- Name: proxy_control_observations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_control_observations (
    observation_id text NOT NULL,
    workload_scope text NOT NULL,
    request_hash text NOT NULL,
    task_id text NOT NULL,
    slot_name text NOT NULL,
    worker_id text NOT NULL,
    worker_instance_id text NOT NULL,
    lease_id text NOT NULL,
    route_generation bigint NOT NULL,
    business_run_id text NOT NULL,
    network_identity_key text NOT NULL,
    kind text NOT NULL,
    source text NOT NULL,
    http_status integer,
    occurred_at timestamp with time zone NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    action text DEFAULT 'none'::text NOT NULL,
    incident_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: proxy_control_reports; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_control_reports (
    id bigint NOT NULL,
    incident_id text,
    proxy_id integer,
    proxy_user text,
    outcome text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    result jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT proxy_control_reports_outcome_check CHECK ((outcome = ANY (ARRAY['success'::text, 'failure'::text])))
);


--
-- Name: proxy_control_reports_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.proxy_control_reports_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: proxy_control_reports_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.proxy_control_reports_id_seq OWNED BY public.proxy_control_reports.id;


--
-- Name: proxy_control_tasks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_control_tasks (
    task_id text NOT NULL,
    attempt_request_id text NOT NULL,
    request_hash text NOT NULL,
    workload_scope text NOT NULL,
    business_run_id text NOT NULL,
    job_execution_id text NOT NULL,
    attempt_number integer NOT NULL,
    slot_name text NOT NULL,
    worker_id text NOT NULL,
    worker_instance_id text DEFAULT 'legacy'::text NOT NULL,
    lease_id text NOT NULL,
    route_generation bigint NOT NULL,
    task_kind text NOT NULL,
    identity_policy_id text DEFAULT 'legacy'::text NOT NULL,
    identity_policy_version integer DEFAULT 1 NOT NULL,
    identity_policy_hash text DEFAULT 'legacy'::text NOT NULL,
    status text NOT NULL,
    outcome text,
    failed_stage text,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    completion_request_id text,
    completion_request_hash text,
    completion_result jsonb,
    CONSTRAINT proxy_control_tasks_attempt_number_check CHECK ((attempt_number > 0)),
    CONSTRAINT proxy_control_tasks_route_generation_check CHECK ((route_generation >= 0)),
    CONSTRAINT proxy_control_tasks_status_check CHECK ((status = ANY (ARRAY['active'::text, 'completed'::text, 'abandoned'::text])))
);


--
-- Name: proxy_health_checks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_health_checks (
    id bigint NOT NULL,
    proxy_id integer NOT NULL,
    started_at timestamp with time zone NOT NULL,
    checked_at timestamp with time zone NOT NULL,
    base_result jsonb NOT NULL,
    youtube_result jsonb NOT NULL,
    verdict character varying(32) NOT NULL,
    conclusive boolean NOT NULL,
    control_path_healthy boolean NOT NULL,
    previous_status character varying(20) NOT NULL,
    resulting_status character varying(20) NOT NULL,
    applied boolean DEFAULT true NOT NULL,
    error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    transition_preserved boolean DEFAULT true NOT NULL
);


--
-- Name: proxy_health_checks_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.proxy_health_checks_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: proxy_health_checks_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.proxy_health_checks_id_seq OWNED BY public.proxy_health_checks.id;


--
-- Name: proxy_identity_profile_epochs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_identity_profile_epochs (
    identity_policy_id text NOT NULL,
    network_identity_key text NOT NULL,
    profile_epoch bigint NOT NULL,
    status text NOT NULL,
    retired_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT proxy_identity_profile_epochs_profile_epoch_check CHECK ((profile_epoch >= 0)),
    CONSTRAINT proxy_identity_profile_epochs_status_check CHECK ((status = ANY (ARRAY['active'::text, 'retired'::text])))
);


--
-- Name: proxy_inventory_reconciliation_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_inventory_reconciliation_runs (
    id bigint NOT NULL,
    source_id integer NOT NULL,
    refresh_generation bigint NOT NULL,
    mode text NOT NULL,
    observed_count integer NOT NULL,
    newly_missing_count integer NOT NULL,
    eligible_count integer NOT NULL,
    retired_membership_count integer NOT NULL,
    archived_proxy_count integer NOT NULL,
    reactivated_proxy_count integer NOT NULL,
    completed_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT proxy_inventory_reconciliation_runs_mode_check CHECK ((mode = ANY (ARRAY['shadow'::text, 'enforce'::text])))
);


--
-- Name: proxy_inventory_reconciliation_runs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.proxy_inventory_reconciliation_runs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: proxy_inventory_reconciliation_runs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.proxy_inventory_reconciliation_runs_id_seq OWNED BY public.proxy_inventory_reconciliation_runs.id;


--
-- Name: proxy_lifecycle_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_lifecycle_events (
    id bigint NOT NULL,
    proxy_id integer NOT NULL,
    health_check_id bigint,
    occurred_at timestamp with time zone NOT NULL,
    event_kind text NOT NULL,
    previous_status text NOT NULL,
    resulting_status text NOT NULL,
    reason text,
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: proxy_lifecycle_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.proxy_lifecycle_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: proxy_lifecycle_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.proxy_lifecycle_events_id_seq OWNED BY public.proxy_lifecycle_events.id;


--
-- Name: proxy_running_slots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_running_slots (
    slot_name text NOT NULL,
    role text NOT NULL,
    slot_no integer NOT NULL,
    pool_id integer NOT NULL,
    user_id integer NOT NULL,
    proxy_id integer,
    assignment_version bigint DEFAULT 0 NOT NULL,
    credential_generation bigint DEFAULT 0 NOT NULL,
    assigned_at timestamp with time zone,
    ready_after timestamp with time zone,
    worker_id text,
    lease_id text,
    lease_until timestamp with time zone,
    last_heartbeat_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    worker_instance_id text,
    current_lease_id text,
    identity_policy_id text,
    identity_policy_version integer,
    identity_policy_hash text,
    required_egress_country character varying(2),
    network_identity_key text,
    profile_epoch bigint DEFAULT 0 NOT NULL,
    active_task_id text,
    active_task_started_at timestamp with time zone,
    pending_action text,
    pending_incident_id text,
    control_state text DEFAULT 'unleased'::text NOT NULL,
    rotation_deadline_at timestamp with time zone,
    CONSTRAINT proxy_running_slots_role_check CHECK ((role = ANY (ARRAY['discover'::text, 'channel'::text, 'query_quality'::text, 'detail'::text]))),
    CONSTRAINT proxy_running_slots_slot_no_check CHECK ((slot_no > 0))
);


--
-- Name: proxy_lifecycle_invariant_violations; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.proxy_lifecycle_invariant_violations AS
 SELECT proxies.id AS proxy_id,
    proxies.status,
    'scheduled_state_without_due'::text AS violation
   FROM public.proxies
  WHERE (((proxies.status)::text = ANY ((ARRAY['idle'::character varying, 'failed'::character varying])::text[])) AND (proxies.next_health_check_at IS NULL))
UNION ALL
 SELECT proxies.id AS proxy_id,
    proxies.status,
    'incomplete_failure_episode'::text AS violation
   FROM public.proxies
  WHERE (((proxies.status)::text = 'failed'::text) AND ((proxies.failed_since IS NULL) OR (proxies.continuous_failed_since IS NULL) OR (proxies.failure_episode_kind IS NULL)))
UNION ALL
 SELECT proxies.id AS proxy_id,
    proxies.status,
    'invalid_archive_projection'::text AS violation
   FROM public.proxies
  WHERE (((proxies.status)::text = 'archived'::text) AND ((proxies.archived_at IS NULL) OR (proxies.archive_reason IS NULL) OR (proxies.next_health_check_at IS NOT NULL)))
UNION ALL
 SELECT p.id AS proxy_id,
    p.status,
    'archived_proxy_bound_to_running_slot'::text AS violation
   FROM (public.proxies p
     JOIN public.proxy_running_slots s ON ((s.proxy_id = p.id)))
  WHERE ((p.status)::text = 'archived'::text)
UNION ALL
 SELECT checks.proxy_id,
    proxies.status,
    'transition_evidence_not_preserved'::text AS violation
   FROM (public.proxy_health_checks checks
     JOIN public.proxies ON ((proxies.id = checks.proxy_id)))
  WHERE ((checks.applied = true) AND ((checks.previous_status)::text IS DISTINCT FROM (checks.resulting_status)::text) AND (checks.transition_preserved = false));


--
-- Name: proxy_lifecycle_repair_actions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_lifecycle_repair_actions (
    run_id text NOT NULL,
    proxy_id integer NOT NULL,
    planned_action text NOT NULL,
    evidence_health_check_id bigint,
    before_state jsonb NOT NULL,
    after_state jsonb NOT NULL,
    applied_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: proxy_lifecycle_repair_candidates; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.proxy_lifecycle_repair_candidates AS
 SELECT p.id AS proxy_id,
    p.status,
    p.updated_at AS proxy_updated_at,
    p.failed_since,
    p.failure_episode_kind,
    health.id AS evidence_health_check_id,
    health.checked_at AS evidence_checked_at,
    health.resulting_status AS evidence_resulting_status,
    health.verdict AS evidence_verdict,
        CASE
            WHEN ((health.resulting_status)::text = 'archived'::text) THEN 'restore_archive_projection'::text
            WHEN ((health.resulting_status)::text = 'failed'::text) THEN 'schedule_failed_recovery'::text
            ELSE 'reset_pending_validation'::text
        END AS recommended_action
   FROM (public.proxies p
     LEFT JOIN LATERAL ( SELECT checks.id,
            checks.checked_at,
            checks.resulting_status,
            checks.verdict
           FROM public.proxy_health_checks checks
          WHERE ((checks.proxy_id = p.id) AND (checks.applied = true))
          ORDER BY checks.checked_at DESC, checks.id DESC
         LIMIT 1) health ON (true))
  WHERE (((p.status)::text = ANY ((ARRAY['idle'::character varying, 'failed'::character varying])::text[])) AND (p.next_health_check_at IS NULL));


--
-- Name: proxy_pools; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_pools (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    description text,
    country_code character varying(3),
    region_name character varying(100),
    city_name character varying(100),
    rotation_method character varying(30) DEFAULT 'roundrobin'::character varying NOT NULL,
    stick_count integer DEFAULT 10 NOT NULL,
    health_check_url text DEFAULT 'https://www.youtube.com/watch?v=_xXsXvsYAhA'::text NOT NULL,
    health_check_cron character varying(100) DEFAULT '*/30 * * * *'::character varying NOT NULL,
    health_check_enabled boolean DEFAULT true NOT NULL,
    auto_sync boolean DEFAULT true NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    sync_mode character varying(10) DEFAULT 'auto'::character varying NOT NULL
);


--
-- Name: proxy_pools_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.proxy_pools_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: proxy_pools_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.proxy_pools_id_seq OWNED BY public.proxy_pools.id;


--
-- Name: proxy_requests_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.proxy_requests_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: proxy_requests_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.proxy_requests_id_seq OWNED BY public.proxy_requests.id;


--
-- Name: proxy_source_memberships; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_source_memberships (
    source_id integer NOT NULL,
    proxy_id integer NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_generation bigint DEFAULT 0 NOT NULL,
    consecutive_absences integer DEFAULT 0 NOT NULL,
    missing_since timestamp with time zone,
    retired_at timestamp with time zone,
    retirement_reason text
);


--
-- Name: proxy_sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_sources (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    url text NOT NULL,
    protocol character varying(20) DEFAULT 'http'::character varying NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    interval_minutes integer DEFAULT 60 NOT NULL,
    last_fetched_at timestamp without time zone,
    last_count integer DEFAULT 0 NOT NULL,
    last_error text,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    last_total integer DEFAULT 0 NOT NULL,
    cleanup_enabled boolean DEFAULT false NOT NULL,
    cleanup_days integer DEFAULT 7 NOT NULL,
    default_tags text[] DEFAULT '{}'::text[] NOT NULL,
    last_supported integer DEFAULT 0 NOT NULL,
    last_skipped integer DEFAULT 0 NOT NULL,
    successful_refresh_generation bigint DEFAULT 0 NOT NULL,
    last_complete_refresh_at timestamp with time zone
);


--
-- Name: proxy_sources_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.proxy_sources_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: proxy_sources_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.proxy_sources_id_seq OWNED BY public.proxy_sources.id;


--
-- Name: proxy_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_users (
    id integer NOT NULL,
    username character varying(255) NOT NULL,
    password_hash text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    main_pool_id integer,
    fallback_pool_ids integer[] DEFAULT '{}'::integer[] NOT NULL,
    max_retries integer DEFAULT 5 NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    requests_per_minute integer DEFAULT 0 NOT NULL
);


--
-- Name: proxy_users_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.proxy_users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: proxy_users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.proxy_users_id_seq OWNED BY public.proxy_users.id;


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version integer NOT NULL,
    description text NOT NULL,
    applied_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.settings (
    key character varying(255) NOT NULL,
    value jsonb NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: _hyper_1_12_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_1_12_chunk ALTER COLUMN id SET DEFAULT nextval('public.logs_id_seq'::regclass);


--
-- Name: _hyper_1_12_chunk timestamp; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_1_12_chunk ALTER COLUMN "timestamp" SET DEFAULT now();


--
-- Name: _hyper_1_16_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_1_16_chunk ALTER COLUMN id SET DEFAULT nextval('public.logs_id_seq'::regclass);


--
-- Name: _hyper_1_16_chunk timestamp; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_1_16_chunk ALTER COLUMN "timestamp" SET DEFAULT now();


--
-- Name: _hyper_1_20_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_1_20_chunk ALTER COLUMN id SET DEFAULT nextval('public.logs_id_seq'::regclass);


--
-- Name: _hyper_1_20_chunk timestamp; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_1_20_chunk ALTER COLUMN "timestamp" SET DEFAULT now();


--
-- Name: _hyper_1_5_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_1_5_chunk ALTER COLUMN id SET DEFAULT nextval('public.logs_id_seq'::regclass);


--
-- Name: _hyper_1_5_chunk timestamp; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_1_5_chunk ALTER COLUMN "timestamp" SET DEFAULT now();


--
-- Name: _hyper_1_8_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_1_8_chunk ALTER COLUMN id SET DEFAULT nextval('public.logs_id_seq'::regclass);


--
-- Name: _hyper_1_8_chunk timestamp; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_1_8_chunk ALTER COLUMN "timestamp" SET DEFAULT now();


--
-- Name: _hyper_3_13_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_13_chunk ALTER COLUMN id SET DEFAULT nextval('public.proxy_requests_id_seq'::regclass);


--
-- Name: _hyper_3_13_chunk timestamp; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_13_chunk ALTER COLUMN "timestamp" SET DEFAULT now();


--
-- Name: _hyper_3_17_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_17_chunk ALTER COLUMN id SET DEFAULT nextval('public.proxy_requests_id_seq'::regclass);


--
-- Name: _hyper_3_17_chunk timestamp; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_17_chunk ALTER COLUMN "timestamp" SET DEFAULT now();


--
-- Name: _hyper_3_21_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_21_chunk ALTER COLUMN id SET DEFAULT nextval('public.proxy_requests_id_seq'::regclass);


--
-- Name: _hyper_3_21_chunk timestamp; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_21_chunk ALTER COLUMN "timestamp" SET DEFAULT now();


--
-- Name: _hyper_3_2_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_2_chunk ALTER COLUMN id SET DEFAULT nextval('public.proxy_requests_id_seq'::regclass);


--
-- Name: _hyper_3_2_chunk timestamp; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_2_chunk ALTER COLUMN "timestamp" SET DEFAULT now();


--
-- Name: _hyper_3_4_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_4_chunk ALTER COLUMN id SET DEFAULT nextval('public.proxy_requests_id_seq'::regclass);


--
-- Name: _hyper_3_4_chunk timestamp; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_4_chunk ALTER COLUMN "timestamp" SET DEFAULT now();


--
-- Name: _hyper_3_6_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_6_chunk ALTER COLUMN id SET DEFAULT nextval('public.proxy_requests_id_seq'::regclass);


--
-- Name: _hyper_3_6_chunk timestamp; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_6_chunk ALTER COLUMN "timestamp" SET DEFAULT now();


--
-- Name: _hyper_3_9_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_9_chunk ALTER COLUMN id SET DEFAULT nextval('public.proxy_requests_id_seq'::regclass);


--
-- Name: _hyper_3_9_chunk timestamp; Type: DEFAULT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_9_chunk ALTER COLUMN "timestamp" SET DEFAULT now();


--
-- Name: admin_credentials id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admin_credentials ALTER COLUMN id SET DEFAULT nextval('public.admin_credentials_id_seq'::regclass);


--
-- Name: logs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logs ALTER COLUMN id SET DEFAULT nextval('public.logs_id_seq'::regclass);


--
-- Name: pool_alert_rules id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_alert_rules ALTER COLUMN id SET DEFAULT nextval('public.pool_alert_rules_id_seq'::regclass);


--
-- Name: pool_geo_filters id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_geo_filters ALTER COLUMN id SET DEFAULT nextval('public.pool_geo_filters_id_seq'::regclass);


--
-- Name: pool_isp_filters id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_isp_filters ALTER COLUMN id SET DEFAULT nextval('public.pool_isp_filters_id_seq'::regclass);


--
-- Name: pool_tag_filters id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_tag_filters ALTER COLUMN id SET DEFAULT nextval('public.pool_tag_filters_id_seq'::regclass);


--
-- Name: proxies id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxies ALTER COLUMN id SET DEFAULT nextval('public.proxies_id_seq'::regclass);


--
-- Name: proxy_control_reports id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_reports ALTER COLUMN id SET DEFAULT nextval('public.proxy_control_reports_id_seq'::regclass);


--
-- Name: proxy_health_checks id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_health_checks ALTER COLUMN id SET DEFAULT nextval('public.proxy_health_checks_id_seq'::regclass);


--
-- Name: proxy_inventory_reconciliation_runs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_inventory_reconciliation_runs ALTER COLUMN id SET DEFAULT nextval('public.proxy_inventory_reconciliation_runs_id_seq'::regclass);


--
-- Name: proxy_lifecycle_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_lifecycle_events ALTER COLUMN id SET DEFAULT nextval('public.proxy_lifecycle_events_id_seq'::regclass);


--
-- Name: proxy_pools id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_pools ALTER COLUMN id SET DEFAULT nextval('public.proxy_pools_id_seq'::regclass);


--
-- Name: proxy_requests id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_requests ALTER COLUMN id SET DEFAULT nextval('public.proxy_requests_id_seq'::regclass);


--
-- Name: proxy_sources id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_sources ALTER COLUMN id SET DEFAULT nextval('public.proxy_sources_id_seq'::regclass);


--
-- Name: proxy_users id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_users ALTER COLUMN id SET DEFAULT nextval('public.proxy_users_id_seq'::regclass);


--
-- Name: admin_credentials admin_credentials_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admin_credentials
    ADD CONSTRAINT admin_credentials_pkey PRIMARY KEY (id);


--
-- Name: admin_credentials admin_credentials_username_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admin_credentials
    ADD CONSTRAINT admin_credentials_username_key UNIQUE (username);


--
-- Name: bullmq_proxy_slots bullmq_proxy_slots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bullmq_proxy_slots
    ADD CONSTRAINT bullmq_proxy_slots_pkey PRIMARY KEY (slot_name);


--
-- Name: bullmq_proxy_slots bullmq_proxy_slots_role_slot_no_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bullmq_proxy_slots
    ADD CONSTRAINT bullmq_proxy_slots_role_slot_no_key UNIQUE (role, slot_no);


--
-- Name: pool_alert_rules pool_alert_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_alert_rules
    ADD CONSTRAINT pool_alert_rules_pkey PRIMARY KEY (id);


--
-- Name: pool_geo_filters pool_geo_filters_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_geo_filters
    ADD CONSTRAINT pool_geo_filters_pkey PRIMARY KEY (id);


--
-- Name: pool_geo_filters pool_geo_filters_pool_id_country_code_city_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_geo_filters
    ADD CONSTRAINT pool_geo_filters_pool_id_country_code_city_name_key UNIQUE (pool_id, country_code, city_name);


--
-- Name: pool_isp_filters pool_isp_filters_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_isp_filters
    ADD CONSTRAINT pool_isp_filters_pkey PRIMARY KEY (id);


--
-- Name: pool_isp_filters pool_isp_filters_pool_id_isp_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_isp_filters
    ADD CONSTRAINT pool_isp_filters_pool_id_isp_key UNIQUE (pool_id, isp);


--
-- Name: pool_proxies pool_proxies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_proxies
    ADD CONSTRAINT pool_proxies_pkey PRIMARY KEY (pool_id, proxy_id);


--
-- Name: pool_tag_filters pool_tag_filters_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_tag_filters
    ADD CONSTRAINT pool_tag_filters_pkey PRIMARY KEY (id);


--
-- Name: pool_tag_filters pool_tag_filters_pool_id_tag_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_tag_filters
    ADD CONSTRAINT pool_tag_filters_pool_id_tag_key UNIQUE (pool_id, tag);


--
-- Name: proxies proxies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxies
    ADD CONSTRAINT proxies_pkey PRIMARY KEY (id);


--
-- Name: proxy_control_business_runs proxy_control_business_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_business_runs
    ADD CONSTRAINT proxy_control_business_runs_pkey PRIMARY KEY (workload_scope, business_run_id);


--
-- Name: proxy_control_command_receipts proxy_control_command_receipts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_command_receipts
    ADD CONSTRAINT proxy_control_command_receipts_pkey PRIMARY KEY (workload_scope, command_kind, request_id);


--
-- Name: proxy_control_incident_observations proxy_control_incident_observ_workload_scope_observation_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_incident_observations
    ADD CONSTRAINT proxy_control_incident_observ_workload_scope_observation_id_key UNIQUE (workload_scope, observation_id);


--
-- Name: proxy_control_incident_observations proxy_control_incident_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_incident_observations
    ADD CONSTRAINT proxy_control_incident_observations_pkey PRIMARY KEY (workload_scope, incident_id, observation_id);


--
-- Name: proxy_control_leases proxy_control_leases_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_leases
    ADD CONSTRAINT proxy_control_leases_pkey PRIMARY KEY (lease_id);


--
-- Name: proxy_control_leases proxy_control_leases_workload_scope_claim_request_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_leases
    ADD CONSTRAINT proxy_control_leases_workload_scope_claim_request_id_key UNIQUE (workload_scope, claim_request_id);


--
-- Name: proxy_control_observations proxy_control_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_observations
    ADD CONSTRAINT proxy_control_observations_pkey PRIMARY KEY (workload_scope, observation_id);


--
-- Name: proxy_control_observations proxy_control_observations_task_id_observation_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_observations
    ADD CONSTRAINT proxy_control_observations_task_id_observation_id_key UNIQUE (task_id, observation_id);


--
-- Name: proxy_control_reports proxy_control_reports_incident_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_reports
    ADD CONSTRAINT proxy_control_reports_incident_id_key UNIQUE (incident_id);


--
-- Name: proxy_control_reports proxy_control_reports_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_reports
    ADD CONSTRAINT proxy_control_reports_pkey PRIMARY KEY (id);


--
-- Name: proxy_control_tasks proxy_control_tasks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_tasks
    ADD CONSTRAINT proxy_control_tasks_pkey PRIMARY KEY (task_id);


--
-- Name: proxy_control_tasks proxy_control_tasks_workload_scope_attempt_request_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_tasks
    ADD CONSTRAINT proxy_control_tasks_workload_scope_attempt_request_id_key UNIQUE (workload_scope, attempt_request_id);


--
-- Name: proxy_control_tasks proxy_control_tasks_workload_scope_business_run_id_attempt__key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_tasks
    ADD CONSTRAINT proxy_control_tasks_workload_scope_business_run_id_attempt__key UNIQUE (workload_scope, business_run_id, attempt_number);


--
-- Name: proxy_control_tasks proxy_control_tasks_workload_scope_completion_request_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_tasks
    ADD CONSTRAINT proxy_control_tasks_workload_scope_completion_request_id_key UNIQUE (workload_scope, completion_request_id);


--
-- Name: proxy_health_checks proxy_health_checks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_health_checks
    ADD CONSTRAINT proxy_health_checks_pkey PRIMARY KEY (id);


--
-- Name: proxy_identity_profile_epochs proxy_identity_profile_epochs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_identity_profile_epochs
    ADD CONSTRAINT proxy_identity_profile_epochs_pkey PRIMARY KEY (identity_policy_id, network_identity_key);


--
-- Name: proxy_inventory_reconciliation_runs proxy_inventory_reconciliation_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_inventory_reconciliation_runs
    ADD CONSTRAINT proxy_inventory_reconciliation_runs_pkey PRIMARY KEY (id);


--
-- Name: proxy_inventory_reconciliation_runs proxy_inventory_reconciliation_source_id_refresh_generation_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_inventory_reconciliation_runs
    ADD CONSTRAINT proxy_inventory_reconciliation_source_id_refresh_generation_key UNIQUE (source_id, refresh_generation);


--
-- Name: proxy_lifecycle_events proxy_lifecycle_events_health_check_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_lifecycle_events
    ADD CONSTRAINT proxy_lifecycle_events_health_check_id_key UNIQUE (health_check_id);


--
-- Name: proxy_lifecycle_events proxy_lifecycle_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_lifecycle_events
    ADD CONSTRAINT proxy_lifecycle_events_pkey PRIMARY KEY (id);


--
-- Name: proxy_lifecycle_repair_actions proxy_lifecycle_repair_actions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_lifecycle_repair_actions
    ADD CONSTRAINT proxy_lifecycle_repair_actions_pkey PRIMARY KEY (run_id, proxy_id);


--
-- Name: proxy_pools proxy_pools_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_pools
    ADD CONSTRAINT proxy_pools_pkey PRIMARY KEY (id);


--
-- Name: proxy_running_slots proxy_running_slots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_running_slots
    ADD CONSTRAINT proxy_running_slots_pkey PRIMARY KEY (slot_name);


--
-- Name: proxy_running_slots proxy_running_slots_role_slot_no_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_running_slots
    ADD CONSTRAINT proxy_running_slots_role_slot_no_key UNIQUE (role, slot_no);


--
-- Name: proxy_source_memberships proxy_source_memberships_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_source_memberships
    ADD CONSTRAINT proxy_source_memberships_pkey PRIMARY KEY (source_id, proxy_id);


--
-- Name: proxy_sources proxy_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_sources
    ADD CONSTRAINT proxy_sources_pkey PRIMARY KEY (id);


--
-- Name: proxy_users proxy_users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_users
    ADD CONSTRAINT proxy_users_pkey PRIMARY KEY (id);


--
-- Name: proxy_users proxy_users_username_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_users
    ADD CONSTRAINT proxy_users_username_key UNIQUE (username);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: settings settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.settings
    ADD CONSTRAINT settings_pkey PRIMARY KEY (key);


--
-- Name: proxies unique_proxy_node_identity; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxies
    ADD CONSTRAINT unique_proxy_node_identity UNIQUE (node_identity);


--
-- Name: _hyper_1_12_chunk_idx_logs_level; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_12_chunk_idx_logs_level ON _timescaledb_internal._hyper_1_12_chunk USING btree (level, "timestamp" DESC);


--
-- Name: _hyper_1_12_chunk_idx_logs_metadata_source; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_12_chunk_idx_logs_metadata_source ON _timescaledb_internal._hyper_1_12_chunk USING btree (((metadata ->> 'source'::text)));


--
-- Name: _hyper_1_12_chunk_idx_logs_timestamp; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_12_chunk_idx_logs_timestamp ON _timescaledb_internal._hyper_1_12_chunk USING btree ("timestamp" DESC);


--
-- Name: _hyper_1_16_chunk_idx_logs_level; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_16_chunk_idx_logs_level ON _timescaledb_internal._hyper_1_16_chunk USING btree (level, "timestamp" DESC);


--
-- Name: _hyper_1_16_chunk_idx_logs_metadata_source; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_16_chunk_idx_logs_metadata_source ON _timescaledb_internal._hyper_1_16_chunk USING btree (((metadata ->> 'source'::text)));


--
-- Name: _hyper_1_16_chunk_idx_logs_timestamp; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_16_chunk_idx_logs_timestamp ON _timescaledb_internal._hyper_1_16_chunk USING btree ("timestamp" DESC);


--
-- Name: _hyper_1_20_chunk_idx_logs_level; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_20_chunk_idx_logs_level ON _timescaledb_internal._hyper_1_20_chunk USING btree (level, "timestamp" DESC);


--
-- Name: _hyper_1_20_chunk_idx_logs_metadata_source; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_20_chunk_idx_logs_metadata_source ON _timescaledb_internal._hyper_1_20_chunk USING btree (((metadata ->> 'source'::text)));


--
-- Name: _hyper_1_20_chunk_idx_logs_timestamp; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_20_chunk_idx_logs_timestamp ON _timescaledb_internal._hyper_1_20_chunk USING btree ("timestamp" DESC);


--
-- Name: _hyper_1_5_chunk_idx_logs_level; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_5_chunk_idx_logs_level ON _timescaledb_internal._hyper_1_5_chunk USING btree (level, "timestamp" DESC);


--
-- Name: _hyper_1_5_chunk_idx_logs_metadata_source; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_5_chunk_idx_logs_metadata_source ON _timescaledb_internal._hyper_1_5_chunk USING btree (((metadata ->> 'source'::text)));


--
-- Name: _hyper_1_5_chunk_logs_timestamp_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_5_chunk_logs_timestamp_idx ON _timescaledb_internal._hyper_1_5_chunk USING btree ("timestamp" DESC);


--
-- Name: _hyper_1_8_chunk_idx_logs_level; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_8_chunk_idx_logs_level ON _timescaledb_internal._hyper_1_8_chunk USING btree (level, "timestamp" DESC);


--
-- Name: _hyper_1_8_chunk_idx_logs_metadata_source; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_8_chunk_idx_logs_metadata_source ON _timescaledb_internal._hyper_1_8_chunk USING btree (((metadata ->> 'source'::text)));


--
-- Name: _hyper_1_8_chunk_idx_logs_timestamp; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_1_8_chunk_idx_logs_timestamp ON _timescaledb_internal._hyper_1_8_chunk USING btree ("timestamp" DESC);


--
-- Name: _hyper_3_13_chunk_idx_proxy_requests_proxy_id; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_13_chunk_idx_proxy_requests_proxy_id ON _timescaledb_internal._hyper_3_13_chunk USING btree (proxy_id, "timestamp" DESC);


--
-- Name: _hyper_3_13_chunk_idx_proxy_requests_success; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_13_chunk_idx_proxy_requests_success ON _timescaledb_internal._hyper_3_13_chunk USING btree (success, "timestamp" DESC);


--
-- Name: _hyper_3_13_chunk_idx_proxy_requests_timestamp; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_13_chunk_idx_proxy_requests_timestamp ON _timescaledb_internal._hyper_3_13_chunk USING btree ("timestamp" DESC);


--
-- Name: _hyper_3_17_chunk_idx_proxy_requests_proxy_id; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_17_chunk_idx_proxy_requests_proxy_id ON _timescaledb_internal._hyper_3_17_chunk USING btree (proxy_id, "timestamp" DESC);


--
-- Name: _hyper_3_17_chunk_idx_proxy_requests_success; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_17_chunk_idx_proxy_requests_success ON _timescaledb_internal._hyper_3_17_chunk USING btree (success, "timestamp" DESC);


--
-- Name: _hyper_3_17_chunk_idx_proxy_requests_timestamp; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_17_chunk_idx_proxy_requests_timestamp ON _timescaledb_internal._hyper_3_17_chunk USING btree ("timestamp" DESC);


--
-- Name: _hyper_3_21_chunk_idx_proxy_requests_proxy_id; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_21_chunk_idx_proxy_requests_proxy_id ON _timescaledb_internal._hyper_3_21_chunk USING btree (proxy_id, "timestamp" DESC);


--
-- Name: _hyper_3_21_chunk_idx_proxy_requests_success; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_21_chunk_idx_proxy_requests_success ON _timescaledb_internal._hyper_3_21_chunk USING btree (success, "timestamp" DESC);


--
-- Name: _hyper_3_21_chunk_idx_proxy_requests_timestamp; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_21_chunk_idx_proxy_requests_timestamp ON _timescaledb_internal._hyper_3_21_chunk USING btree ("timestamp" DESC);


--
-- Name: _hyper_3_2_chunk_idx_proxy_requests_proxy_id; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_2_chunk_idx_proxy_requests_proxy_id ON _timescaledb_internal._hyper_3_2_chunk USING btree (proxy_id, "timestamp" DESC);


--
-- Name: _hyper_3_2_chunk_idx_proxy_requests_success; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_2_chunk_idx_proxy_requests_success ON _timescaledb_internal._hyper_3_2_chunk USING btree (success, "timestamp" DESC);


--
-- Name: _hyper_3_2_chunk_proxy_requests_timestamp_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_2_chunk_proxy_requests_timestamp_idx ON _timescaledb_internal._hyper_3_2_chunk USING btree ("timestamp" DESC);


--
-- Name: _hyper_3_4_chunk_idx_proxy_requests_proxy_id; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_4_chunk_idx_proxy_requests_proxy_id ON _timescaledb_internal._hyper_3_4_chunk USING btree (proxy_id, "timestamp" DESC);


--
-- Name: _hyper_3_4_chunk_idx_proxy_requests_success; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_4_chunk_idx_proxy_requests_success ON _timescaledb_internal._hyper_3_4_chunk USING btree (success, "timestamp" DESC);


--
-- Name: _hyper_3_4_chunk_proxy_requests_timestamp_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_4_chunk_proxy_requests_timestamp_idx ON _timescaledb_internal._hyper_3_4_chunk USING btree ("timestamp" DESC);


--
-- Name: _hyper_3_6_chunk_idx_proxy_requests_proxy_id; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_6_chunk_idx_proxy_requests_proxy_id ON _timescaledb_internal._hyper_3_6_chunk USING btree (proxy_id, "timestamp" DESC);


--
-- Name: _hyper_3_6_chunk_idx_proxy_requests_success; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_6_chunk_idx_proxy_requests_success ON _timescaledb_internal._hyper_3_6_chunk USING btree (success, "timestamp" DESC);


--
-- Name: _hyper_3_6_chunk_proxy_requests_timestamp_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_6_chunk_proxy_requests_timestamp_idx ON _timescaledb_internal._hyper_3_6_chunk USING btree ("timestamp" DESC);


--
-- Name: _hyper_3_9_chunk_idx_proxy_requests_proxy_id; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_9_chunk_idx_proxy_requests_proxy_id ON _timescaledb_internal._hyper_3_9_chunk USING btree (proxy_id, "timestamp" DESC);


--
-- Name: _hyper_3_9_chunk_idx_proxy_requests_success; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_9_chunk_idx_proxy_requests_success ON _timescaledb_internal._hyper_3_9_chunk USING btree (success, "timestamp" DESC);


--
-- Name: _hyper_3_9_chunk_idx_proxy_requests_timestamp; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX _hyper_3_9_chunk_idx_proxy_requests_timestamp ON _timescaledb_internal._hyper_3_9_chunk USING btree ("timestamp" DESC);


--
-- Name: compress_hyper_2_15_chunk_level__ts_meta_min_1__ts_meta_max_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX compress_hyper_2_15_chunk_level__ts_meta_min_1__ts_meta_max_idx ON _timescaledb_internal.compress_hyper_2_15_chunk USING btree (level, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC);


--
-- Name: compress_hyper_2_19_chunk_level__ts_meta_min_1__ts_meta_max_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX compress_hyper_2_19_chunk_level__ts_meta_min_1__ts_meta_max_idx ON _timescaledb_internal.compress_hyper_2_19_chunk USING btree (level, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC);


--
-- Name: compress_hyper_2_23_chunk_level__ts_meta_min_1__ts_meta_max_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX compress_hyper_2_23_chunk_level__ts_meta_min_1__ts_meta_max_idx ON _timescaledb_internal.compress_hyper_2_23_chunk USING btree (level, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC);


--
-- Name: compress_hyper_4_10_chunk_proxy_id__ts_meta_min_1__ts_meta__idx; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX compress_hyper_4_10_chunk_proxy_id__ts_meta_min_1__ts_meta__idx ON _timescaledb_internal.compress_hyper_4_10_chunk USING btree (proxy_id, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_4_14_chunk_proxy_id__ts_meta_min_1__ts_meta__idx; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX compress_hyper_4_14_chunk_proxy_id__ts_meta_min_1__ts_meta__idx ON _timescaledb_internal.compress_hyper_4_14_chunk USING btree (proxy_id, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_4_18_chunk_proxy_id__ts_meta_min_1__ts_meta__idx; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX compress_hyper_4_18_chunk_proxy_id__ts_meta_min_1__ts_meta__idx ON _timescaledb_internal.compress_hyper_4_18_chunk USING btree (proxy_id, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_4_22_chunk_proxy_id__ts_meta_min_1__ts_meta__idx; Type: INDEX; Schema: _timescaledb_internal; Owner: -
--

CREATE INDEX compress_hyper_4_22_chunk_proxy_id__ts_meta_min_1__ts_meta__idx ON _timescaledb_internal.compress_hyper_4_22_chunk USING btree (proxy_id, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: idx_bullmq_proxy_slots_lease; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bullmq_proxy_slots_lease ON public.bullmq_proxy_slots USING btree (role, lease_until);


--
-- Name: idx_bullmq_proxy_slots_proxy; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_bullmq_proxy_slots_proxy ON public.bullmq_proxy_slots USING btree (proxy_id) WHERE (proxy_id IS NOT NULL);


--
-- Name: idx_logs_level; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_logs_level ON public.logs USING btree (level, "timestamp" DESC);


--
-- Name: idx_logs_metadata_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_logs_metadata_source ON public.logs USING btree (((metadata ->> 'source'::text)));


--
-- Name: idx_logs_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_logs_timestamp ON public.logs USING btree ("timestamp" DESC);


--
-- Name: idx_pool_alert_rules_pool_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pool_alert_rules_pool_id ON public.pool_alert_rules USING btree (pool_id);


--
-- Name: idx_pool_geo_filters_pool_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pool_geo_filters_pool_id ON public.pool_geo_filters USING btree (pool_id);


--
-- Name: idx_pool_isp_filters_pool_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pool_isp_filters_pool_id ON public.pool_isp_filters USING btree (pool_id);


--
-- Name: idx_pool_proxies_pool_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pool_proxies_pool_id ON public.pool_proxies USING btree (pool_id);


--
-- Name: idx_pool_proxies_proxy_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pool_proxies_proxy_id ON public.pool_proxies USING btree (proxy_id);


--
-- Name: idx_pool_tag_filters_pool_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pool_tag_filters_pool_id ON public.pool_tag_filters USING btree (pool_id);


--
-- Name: idx_proxies_address; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxies_address ON public.proxies USING btree (address);


--
-- Name: idx_proxies_control_eligibility; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxies_control_eligibility ON public.proxies USING btree (status, cooldown_until, id);


--
-- Name: idx_proxies_country_code; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxies_country_code ON public.proxies USING btree (country_code);


--
-- Name: idx_proxies_endpoint_protocol; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxies_endpoint_protocol ON public.proxies USING btree (address, protocol);


--
-- Name: idx_proxies_health_check_due; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxies_health_check_due ON public.proxies USING btree (next_health_check_at, id) WHERE ((next_health_check_at IS NOT NULL) AND (((status)::text = ANY ((ARRAY['idle'::character varying, 'failed'::character varying])::text[])) OR (((status)::text = 'active'::text) AND revalidation_required)));


--
-- Name: idx_proxies_network_identity_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_proxies_network_identity_key ON public.proxies USING btree (network_identity_key);


--
-- Name: idx_proxies_operational_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxies_operational_status ON public.proxies USING btree (status, id) WHERE ((status)::text <> 'archived'::text);


--
-- Name: idx_proxies_protocol; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxies_protocol ON public.proxies USING btree (protocol);


--
-- Name: idx_proxies_region_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxies_region_name ON public.proxies USING btree (region_name);


--
-- Name: idx_proxies_source_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxies_source_id ON public.proxies USING btree (source_id);


--
-- Name: idx_proxies_source_last_seen; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxies_source_last_seen ON public.proxies USING btree (source_id, last_seen_at) WHERE (source_id IS NOT NULL);


--
-- Name: idx_proxies_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxies_status ON public.proxies USING btree (status);


--
-- Name: idx_proxies_tags; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxies_tags ON public.proxies USING gin (tags);


--
-- Name: idx_proxies_youtube_cooldown; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxies_youtube_cooldown ON public.proxies USING btree (status, cooldown_until);


--
-- Name: idx_proxy_control_reports_proxy_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_control_reports_proxy_created ON public.proxy_control_reports USING btree (proxy_id, created_at DESC);


--
-- Name: idx_proxy_health_checks_proxy_checked; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_health_checks_proxy_checked ON public.proxy_health_checks USING btree (proxy_id, checked_at DESC);


--
-- Name: idx_proxy_health_checks_retention; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_health_checks_retention ON public.proxy_health_checks USING btree (checked_at, id) WHERE (transition_preserved = true);


--
-- Name: idx_proxy_health_checks_unpreserved_transitions; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_health_checks_unpreserved_transitions ON public.proxy_health_checks USING btree (id, proxy_id) WHERE ((applied = true) AND ((previous_status)::text IS DISTINCT FROM (resulting_status)::text) AND (transition_preserved = false));


--
-- Name: idx_proxy_lifecycle_events_proxy_occurred; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_lifecycle_events_proxy_occurred ON public.proxy_lifecycle_events USING btree (proxy_id, occurred_at DESC);


--
-- Name: idx_proxy_requests_proxy_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_requests_proxy_id ON public.proxy_requests USING btree (proxy_id, "timestamp" DESC);


--
-- Name: idx_proxy_requests_success; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_requests_success ON public.proxy_requests USING btree (success, "timestamp" DESC);


--
-- Name: idx_proxy_requests_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_requests_timestamp ON public.proxy_requests USING btree ("timestamp" DESC);


--
-- Name: idx_proxy_running_slots_lease; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_running_slots_lease ON public.proxy_running_slots USING btree (role, lease_until);


--
-- Name: idx_proxy_running_slots_proxy; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_proxy_running_slots_proxy ON public.proxy_running_slots USING btree (proxy_id) WHERE (proxy_id IS NOT NULL);


--
-- Name: idx_proxy_running_slots_role_slot; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_proxy_running_slots_role_slot ON public.proxy_running_slots USING btree (role, slot_no);


--
-- Name: idx_proxy_running_slots_worker; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_proxy_running_slots_worker ON public.proxy_running_slots USING btree (worker_id) WHERE (worker_id IS NOT NULL);


--
-- Name: idx_proxy_source_memberships_proxy; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_source_memberships_proxy ON public.proxy_source_memberships USING btree (proxy_id, source_id);


--
-- Name: idx_proxy_source_memberships_reconcile; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_source_memberships_reconcile ON public.proxy_source_memberships USING btree (source_id, retired_at, missing_since, consecutive_absences);


--
-- Name: idx_proxy_sources_enabled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_sources_enabled ON public.proxy_sources USING btree (enabled);


--
-- Name: idx_proxy_users_enabled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_users_enabled ON public.proxy_users USING btree (enabled);


--
-- Name: idx_proxy_users_username; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_proxy_users_username ON public.proxy_users USING btree (username);


--
-- Name: logs_timestamp_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX logs_timestamp_idx ON public.logs USING btree ("timestamp" DESC);


--
-- Name: proxy_control_leases_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX proxy_control_leases_expiry ON public.proxy_control_leases USING btree (status, lease_until);


--
-- Name: proxy_control_leases_one_active_slot; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX proxy_control_leases_one_active_slot ON public.proxy_control_leases USING btree (workload_scope, slot_name) WHERE (status = 'active'::text);


--
-- Name: proxy_control_leases_one_active_worker_instance; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX proxy_control_leases_one_active_worker_instance ON public.proxy_control_leases USING btree (workload_scope, worker_id, worker_instance_id) WHERE (status = 'active'::text);


--
-- Name: proxy_control_observations_task_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX proxy_control_observations_task_created ON public.proxy_control_observations USING btree (task_id, created_at);


--
-- Name: proxy_control_tasks_job_execution; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX proxy_control_tasks_job_execution ON public.proxy_control_tasks USING btree (workload_scope, job_execution_id, started_at);


--
-- Name: proxy_control_tasks_one_active_business_run; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX proxy_control_tasks_one_active_business_run ON public.proxy_control_tasks USING btree (workload_scope, business_run_id) WHERE (status = 'active'::text);


--
-- Name: proxy_control_tasks_one_active_slot; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX proxy_control_tasks_one_active_slot ON public.proxy_control_tasks USING btree (slot_name) WHERE (status = 'active'::text);


--
-- Name: proxy_requests_timestamp_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX proxy_requests_timestamp_idx ON public.proxy_requests USING btree ("timestamp" DESC);


--
-- Name: _compressed_hypertable_2 ts_insert_blocker; Type: TRIGGER; Schema: _timescaledb_internal; Owner: -
--

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON _timescaledb_internal._compressed_hypertable_2 FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.insert_blocker();


--
-- Name: _compressed_hypertable_4 ts_insert_blocker; Type: TRIGGER; Schema: _timescaledb_internal; Owner: -
--

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON _timescaledb_internal._compressed_hypertable_4 FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.insert_blocker();


--
-- Name: proxy_health_checks proxy_health_transition_preserved_on_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER proxy_health_transition_preserved_on_insert BEFORE INSERT ON public.proxy_health_checks FOR EACH ROW EXECUTE FUNCTION public.set_proxy_health_transition_preserved();


--
-- Name: logs ts_insert_blocker; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON public.logs FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.insert_blocker();


--
-- Name: proxy_requests ts_insert_blocker; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON public.proxy_requests FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.insert_blocker();


--
-- Name: _hyper_3_13_chunk 13_5_proxy_requests_proxy_id_fkey; Type: FK CONSTRAINT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_13_chunk
    ADD CONSTRAINT "13_5_proxy_requests_proxy_id_fkey" FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE CASCADE;


--
-- Name: _hyper_3_17_chunk 17_6_proxy_requests_proxy_id_fkey; Type: FK CONSTRAINT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_17_chunk
    ADD CONSTRAINT "17_6_proxy_requests_proxy_id_fkey" FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE CASCADE;


--
-- Name: _hyper_3_21_chunk 21_7_proxy_requests_proxy_id_fkey; Type: FK CONSTRAINT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_21_chunk
    ADD CONSTRAINT "21_7_proxy_requests_proxy_id_fkey" FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE CASCADE;


--
-- Name: _hyper_3_2_chunk 2_1_proxy_requests_proxy_id_fkey; Type: FK CONSTRAINT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_2_chunk
    ADD CONSTRAINT "2_1_proxy_requests_proxy_id_fkey" FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE CASCADE;


--
-- Name: _hyper_3_4_chunk 4_2_proxy_requests_proxy_id_fkey; Type: FK CONSTRAINT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_4_chunk
    ADD CONSTRAINT "4_2_proxy_requests_proxy_id_fkey" FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE CASCADE;


--
-- Name: _hyper_3_6_chunk 6_3_proxy_requests_proxy_id_fkey; Type: FK CONSTRAINT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_6_chunk
    ADD CONSTRAINT "6_3_proxy_requests_proxy_id_fkey" FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE CASCADE;


--
-- Name: _hyper_3_9_chunk 9_4_proxy_requests_proxy_id_fkey; Type: FK CONSTRAINT; Schema: _timescaledb_internal; Owner: -
--

ALTER TABLE ONLY _timescaledb_internal._hyper_3_9_chunk
    ADD CONSTRAINT "9_4_proxy_requests_proxy_id_fkey" FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE CASCADE;


--
-- Name: bullmq_proxy_slots bullmq_proxy_slots_pool_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bullmq_proxy_slots
    ADD CONSTRAINT bullmq_proxy_slots_pool_id_fkey FOREIGN KEY (pool_id) REFERENCES public.proxy_pools(id) ON DELETE CASCADE;


--
-- Name: bullmq_proxy_slots bullmq_proxy_slots_proxy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bullmq_proxy_slots
    ADD CONSTRAINT bullmq_proxy_slots_proxy_id_fkey FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE SET NULL;


--
-- Name: bullmq_proxy_slots bullmq_proxy_slots_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bullmq_proxy_slots
    ADD CONSTRAINT bullmq_proxy_slots_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.proxy_users(id) ON DELETE CASCADE;


--
-- Name: pool_alert_rules pool_alert_rules_pool_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_alert_rules
    ADD CONSTRAINT pool_alert_rules_pool_id_fkey FOREIGN KEY (pool_id) REFERENCES public.proxy_pools(id) ON DELETE CASCADE;


--
-- Name: pool_geo_filters pool_geo_filters_pool_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_geo_filters
    ADD CONSTRAINT pool_geo_filters_pool_id_fkey FOREIGN KEY (pool_id) REFERENCES public.proxy_pools(id) ON DELETE CASCADE;


--
-- Name: pool_isp_filters pool_isp_filters_pool_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_isp_filters
    ADD CONSTRAINT pool_isp_filters_pool_id_fkey FOREIGN KEY (pool_id) REFERENCES public.proxy_pools(id) ON DELETE CASCADE;


--
-- Name: pool_proxies pool_proxies_pool_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_proxies
    ADD CONSTRAINT pool_proxies_pool_id_fkey FOREIGN KEY (pool_id) REFERENCES public.proxy_pools(id) ON DELETE CASCADE;


--
-- Name: pool_proxies pool_proxies_proxy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_proxies
    ADD CONSTRAINT pool_proxies_proxy_id_fkey FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE CASCADE;


--
-- Name: pool_tag_filters pool_tag_filters_pool_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_tag_filters
    ADD CONSTRAINT pool_tag_filters_pool_id_fkey FOREIGN KEY (pool_id) REFERENCES public.proxy_pools(id) ON DELETE CASCADE;


--
-- Name: proxies proxies_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxies
    ADD CONSTRAINT proxies_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.proxy_sources(id) ON DELETE SET NULL;


--
-- Name: proxy_control_incident_observations proxy_control_incident_observ_workload_scope_observation_i_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_incident_observations
    ADD CONSTRAINT proxy_control_incident_observ_workload_scope_observation_i_fkey FOREIGN KEY (workload_scope, observation_id) REFERENCES public.proxy_control_observations(workload_scope, observation_id) ON DELETE CASCADE;


--
-- Name: proxy_control_leases proxy_control_leases_slot_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_leases
    ADD CONSTRAINT proxy_control_leases_slot_name_fkey FOREIGN KEY (slot_name) REFERENCES public.proxy_running_slots(slot_name);


--
-- Name: proxy_control_observations proxy_control_observations_slot_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_observations
    ADD CONSTRAINT proxy_control_observations_slot_name_fkey FOREIGN KEY (slot_name) REFERENCES public.proxy_running_slots(slot_name);


--
-- Name: proxy_control_observations proxy_control_observations_task_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_observations
    ADD CONSTRAINT proxy_control_observations_task_id_fkey FOREIGN KEY (task_id) REFERENCES public.proxy_control_tasks(task_id) ON DELETE CASCADE;


--
-- Name: proxy_control_reports proxy_control_reports_proxy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_reports
    ADD CONSTRAINT proxy_control_reports_proxy_id_fkey FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE SET NULL;


--
-- Name: proxy_control_tasks proxy_control_tasks_slot_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_control_tasks
    ADD CONSTRAINT proxy_control_tasks_slot_name_fkey FOREIGN KEY (slot_name) REFERENCES public.proxy_running_slots(slot_name);


--
-- Name: proxy_health_checks proxy_health_checks_proxy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_health_checks
    ADD CONSTRAINT proxy_health_checks_proxy_id_fkey FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE CASCADE;


--
-- Name: proxy_inventory_reconciliation_runs proxy_inventory_reconciliation_runs_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_inventory_reconciliation_runs
    ADD CONSTRAINT proxy_inventory_reconciliation_runs_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.proxy_sources(id) ON DELETE CASCADE;


--
-- Name: proxy_lifecycle_events proxy_lifecycle_events_health_check_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_lifecycle_events
    ADD CONSTRAINT proxy_lifecycle_events_health_check_id_fkey FOREIGN KEY (health_check_id) REFERENCES public.proxy_health_checks(id) ON DELETE SET NULL;


--
-- Name: proxy_lifecycle_events proxy_lifecycle_events_proxy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_lifecycle_events
    ADD CONSTRAINT proxy_lifecycle_events_proxy_id_fkey FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE CASCADE;


--
-- Name: proxy_lifecycle_repair_actions proxy_lifecycle_repair_actions_evidence_health_check_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_lifecycle_repair_actions
    ADD CONSTRAINT proxy_lifecycle_repair_actions_evidence_health_check_id_fkey FOREIGN KEY (evidence_health_check_id) REFERENCES public.proxy_health_checks(id) ON DELETE SET NULL;


--
-- Name: proxy_lifecycle_repair_actions proxy_lifecycle_repair_actions_proxy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_lifecycle_repair_actions
    ADD CONSTRAINT proxy_lifecycle_repair_actions_proxy_id_fkey FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE CASCADE;


--
-- Name: proxy_requests proxy_requests_proxy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_requests
    ADD CONSTRAINT proxy_requests_proxy_id_fkey FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE CASCADE;


--
-- Name: proxy_running_slots proxy_running_slots_pool_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_running_slots
    ADD CONSTRAINT proxy_running_slots_pool_id_fkey FOREIGN KEY (pool_id) REFERENCES public.proxy_pools(id) ON DELETE CASCADE;


--
-- Name: proxy_running_slots proxy_running_slots_proxy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_running_slots
    ADD CONSTRAINT proxy_running_slots_proxy_id_fkey FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE SET NULL;


--
-- Name: proxy_running_slots proxy_running_slots_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_running_slots
    ADD CONSTRAINT proxy_running_slots_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.proxy_users(id) ON DELETE CASCADE;


--
-- Name: proxy_source_memberships proxy_source_memberships_proxy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_source_memberships
    ADD CONSTRAINT proxy_source_memberships_proxy_id_fkey FOREIGN KEY (proxy_id) REFERENCES public.proxies(id) ON DELETE CASCADE;


--
-- Name: proxy_source_memberships proxy_source_memberships_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_source_memberships
    ADD CONSTRAINT proxy_source_memberships_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.proxy_sources(id) ON DELETE CASCADE;


--
-- Name: proxy_users proxy_users_main_pool_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_users
    ADD CONSTRAINT proxy_users_main_pool_id_fkey FOREIGN KEY (main_pool_id) REFERENCES public.proxy_pools(id) ON DELETE SET NULL;


--
-- PostgreSQL database dump complete
--

\unrestrict TLai1jdybqWBZ6XSCYedcfTQeOwfDgg0q3QPLYqFAjNDXUowCRhUSVn4Y38KjeK

