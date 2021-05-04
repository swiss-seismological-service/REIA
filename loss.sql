--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.24
-- Dumped by pg_dump version 9.5.5

-- Started on 2021-04-27 15:53:39

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 2243 (class 1262 OID 47300)
-- Name: lossd; Type: DATABASE; Schema: -; Owner: seiscomp3
--

CREATE DATABASE lossd WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8';


ALTER DATABASE lossd OWNER TO seiscomp3;

\connect lossd

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 1 (class 3079 OID 12395)
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- TOC entry 2246 (class 0 OID 0)
-- Dependencies: 1
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

--
-- TOC entry 181 (class 1259 OID 47301)
-- Name: object_seq; Type: SEQUENCE; Schema: public; Owner: seiscomp3
--

CREATE SEQUENCE object_seq
    START WITH 208302507
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE object_seq OWNER TO seiscomp3;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 187 (class 1259 OID 47404)
-- Name: loss_asset; Type: TABLE; Schema: public; Owner: seiscomp3
--

CREATE TABLE loss_asset (
    _oid bigint DEFAULT nextval('object_seq'::regclass) NOT NULL,
    _parent_oid bigint NOT NULL,
    m_publicid_resourceid character varying(255) DEFAULT ''::character varying NOT NULL,
    buildingcount integer DEFAULT 1 NOT NULL,
    m_contentvalue_value double precision NOT NULL,
    m_contentvalue_uncertainty double precision,
    m_contentvalue_loweruncertainty double precision,
    m_contentvalue_upperuncertainty double precision,
    m_contentvalue_confidencelevel double precision,
    m_contentvalue_pdf_variable_content bytea,
    m_contentvalue_pdf_probability_content bytea,
    m_contentvalue_pdf_used boolean DEFAULT false NOT NULL,
    m_structuralvalue_value double precision NOT NULL,
    m_structuralvalue_uncertainty double precision,
    m_structuralvalue_loweruncertainty double precision,
    m_structuralvalue_upperuncertainty double precision,
    m_structuralvalue_confidencelevel double precision,
    m_structuralvalue_pdf_variable_content bytea,
    m_structuralvalue_pdf_probability_content bytea,
    m_structuralvalue_pdf_used boolean DEFAULT false NOT NULL,
    m_occupancydaytime_value double precision NOT NULL,
    m_occupancydaytime_uncertainty double precision,
    m_occupancydaytime_loweruncertainty double precision,
    m_occupancydaytime_upperuncertainty double precision,
    m_occupancydaytime_confidencelevel double precision,
    m_occupancydaytime_pdf_variable_content bytea,
    m_occupancydaytime_pdf_probability_content bytea,
    m_occupancydaytime_pdf_used boolean DEFAULT false NOT NULL,
    m_taxonomy_concept character varying DEFAULT ''::character varying NOT NULL,
    m_taxonomy_classificationsource_resourceid character varying,
    m_taxonomy_conceptschema_resourceid character varying DEFAULT ''::character varying NOT NULL
);


ALTER TABLE loss_asset OWNER TO seiscomp3;

--
-- TOC entry 185 (class 1259 OID 47349)
-- Name: loss_assetcollection; Type: TABLE; Schema: public; Owner: seiscomp3
--

CREATE TABLE loss_assetcollection (
    _oid bigint DEFAULT nextval('object_seq'::regclass) NOT NULL,
    m_publicid_resourceid character varying(255) DEFAULT ''::character varying NOT NULL,
    m_creationinfo_agencyid character varying(64),
    m_creationinfo_agencyuri character varying(255),
    m_creationinfo_author character varying(128),
    m_creationinfo_authoruri character varying(255),
    m_creationinfo_creationtime timestamp without time zone,
    m_creationinfo_creationtime_ms integer,
    m_creationinfo_modificationtime timestamp without time zone,
    m_creationinfo_modificationtime_ms integer,
    m_creationinfo_version character varying(64),
    m_creationinfo_used boolean DEFAULT false NOT NULL
);


ALTER TABLE loss_assetcollection OWNER TO seiscomp3;

--
-- TOC entry 182 (class 1259 OID 47303)
-- Name: loss_calculation; Type: TABLE; Schema: public; Owner: seiscomp3
--

CREATE TABLE loss_calculation (
    _oid bigint DEFAULT nextval('object_seq'::regclass) NOT NULL,
    m_publicid_resourceid character varying(255) DEFAULT ''::character varying NOT NULL,
    m_shakemapid_resourceid character varying(255) DEFAULT ''::character varying NOT NULL,
    m_map_id bigint
);


ALTER TABLE loss_calculation OWNER TO seiscomp3;

--
-- TOC entry 183 (class 1259 OID 47323)
-- Name: loss_calculationparameters; Type: TABLE; Schema: public; Owner: seiscomp3
--

CREATE TABLE loss_calculationparameters (
    _oid bigint DEFAULT nextval('object_seq'::regclass) NOT NULL,
    _parent_oid bigint NOT NULL,
    m_lossunit character varying NOT NULL,
    m_maincalculationmode character varying NOT NULL,
    m_masterseed integer,
    m_maximumdistance double precision,
    m_numberofgroundmotionfields integer NOT NULL,
    m_preparationcalculationmode character varying NOT NULL,
    m_randomseed integer
);


ALTER TABLE loss_calculationparameters OWNER TO seiscomp3;

--
-- TOC entry 191 (class 1259 OID 47492)
-- Name: loss_intensityspecificvulnerability; Type: TABLE; Schema: public; Owner: seiscomp3
--

CREATE TABLE loss_intensityspecificvulnerability (
    _oid bigint DEFAULT nextval('object_seq'::regclass) NOT NULL,
    _parent_oid bigint NOT NULL,
    m_covariancelossratio double precision NOT NULL,
    m_intensitymeasurelevel double precision NOT NULL,
    m_meanlossratio double precision NOT NULL
);


ALTER TABLE loss_intensityspecificvulnerability OWNER TO seiscomp3;

--
-- TOC entry 188 (class 1259 OID 47443)
-- Name: loss_lossbyasset; Type: TABLE; Schema: public; Owner: seiscomp3
--

CREATE TABLE loss_lossbyasset (
    _oid bigint DEFAULT nextval('object_seq'::regclass) NOT NULL,
    _parent_oid bigint NOT NULL,
    m_assetid character varying(255) NOT NULL,
    m_meanloss_value double precision NOT NULL,
    m_meanloss_uncertainty double precision,
    m_meanloss_loweruncertainty double precision,
    m_meanloss_upperuncertainty double precision,
    m_meanloss_confidencelevel double precision,
    m_meanloss_pdf_variable_content bytea,
    m_meanloss_pdf_probability_content bytea,
    m_meanloss_pdf_used boolean DEFAULT false NOT NULL
);


ALTER TABLE loss_lossbyasset OWNER TO seiscomp3;

--
-- TOC entry 186 (class 1259 OID 47360)
-- Name: loss_lossbyrealization; Type: TABLE; Schema: public; Owner: seiscomp3
--

CREATE TABLE loss_lossbyrealization (
    _oid bigint DEFAULT nextval('object_seq'::regclass) NOT NULL,
    _parent_oid bigint NOT NULL,
    m_eventrealizationid integer,
    m_aggregationidentifier character varying(255),
    m_loss double precision
);


ALTER TABLE loss_lossbyrealization OWNER TO seiscomp3;

--
-- TOC entry 184 (class 1259 OID 47337)
-- Name: loss_site; Type: TABLE; Schema: public; Owner: seiscomp3
--

CREATE TABLE loss_site (
    _oid bigint DEFAULT nextval('object_seq'::regclass) NOT NULL,
    m_publicid_resourceid character varying(255) DEFAULT ''::character varying NOT NULL,
    m_canton character varying NOT NULL,
    m_latitude_value double precision NOT NULL,
    m_latitude_uncertainty double precision,
    m_latitude_loweruncertainty double precision,
    m_latitude_upperuncertainty double precision,
    m_latitude_confidencelevel double precision,
    m_latitude_pdf_variable_content bytea,
    m_latitude_pdf_probability_content bytea,
    m_latitude_pdf_used boolean DEFAULT false NOT NULL,
    m_longitude_value double precision NOT NULL,
    m_longitude_uncertainty double precision,
    m_longitude_loweruncertainty double precision,
    m_longitude_upperuncertainty double precision,
    m_longitude_confidencelevel double precision,
    m_longitude_pdf_variable_content bytea,
    m_longitude_pdf_probability_content bytea,
    m_longitude_pdf_used boolean DEFAULT false NOT NULL,
    m_municipalityid integer NOT NULL,
    m_postalcode integer NOT NULL
);


ALTER TABLE loss_site OWNER TO seiscomp3;

--
-- TOC entry 190 (class 1259 OID 47479)
-- Name: loss_vulnerabilityfunction; Type: TABLE; Schema: public; Owner: seiscomp3
--

CREATE TABLE loss_vulnerabilityfunction (
    _oid bigint DEFAULT nextval('object_seq'::regclass) NOT NULL,
    _parent_oid bigint NOT NULL,
    m_distribution character varying(32) DEFAULT ''::character varying NOT NULL,
    m_intensitymeasuretype character varying(32) DEFAULT ''::character varying NOT NULL,
    m_taxonomy_concept character varying DEFAULT ''::character varying NOT NULL,
    m_taxonomy_classificationsource_resourceid character varying,
    m_taxonomy_conceptschema_resourceid character varying DEFAULT ''::character varying NOT NULL
);


ALTER TABLE loss_vulnerabilityfunction OWNER TO seiscomp3;

--
-- TOC entry 189 (class 1259 OID 47466)
-- Name: loss_vulnerabilitymodel; Type: TABLE; Schema: public; Owner: seiscomp3
--

CREATE TABLE loss_vulnerabilitymodel (
    _oid bigint DEFAULT nextval('object_seq'::regclass) NOT NULL,
    m_publicid_resourceid character varying(255) DEFAULT ''::character varying NOT NULL,
    m_assetcategory character varying(255),
    m_description bytea,
    m_losscategory character varying(255) DEFAULT ''::character varying NOT NULL
);


ALTER TABLE loss_vulnerabilitymodel OWNER TO seiscomp3;

--
-- TOC entry 2105 (class 2606 OID 47419)
-- Name: asset_pkey; Type: CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_asset
    ADD CONSTRAINT asset_pkey UNIQUE (_oid);


--
-- TOC entry 2107 (class 2606 OID 47442)
-- Name: asset_publicid_unique; Type: CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_asset
    ADD CONSTRAINT asset_publicid_unique UNIQUE (m_publicid_resourceid);


--
-- TOC entry 2117 (class 2606 OID 47497)
-- Name: intensityspecificvulnerability_pkey; Type: CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_intensityspecificvulnerability
    ADD CONSTRAINT intensityspecificvulnerability_pkey UNIQUE (_oid);


--
-- TOC entry 2101 (class 2606 OID 47359)
-- Name: loss_assetcollection_pkey; Type: CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_assetcollection
    ADD CONSTRAINT loss_assetcollection_pkey UNIQUE (_oid);


--
-- TOC entry 2095 (class 2606 OID 47322)
-- Name: loss_calculation_pkey; Type: CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_calculation
    ADD CONSTRAINT loss_calculation_pkey UNIQUE (_oid);


--
-- TOC entry 2099 (class 2606 OID 47348)
-- Name: loss_site_pkey; Type: CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_site
    ADD CONSTRAINT loss_site_pkey UNIQUE (_oid);


--
-- TOC entry 2109 (class 2606 OID 47452)
-- Name: lossbyasset_pkey; Type: CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_lossbyasset
    ADD CONSTRAINT lossbyasset_pkey UNIQUE (_oid);


--
-- TOC entry 2103 (class 2606 OID 47365)
-- Name: lossbyrealization_pkey; Type: CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_lossbyrealization
    ADD CONSTRAINT lossbyrealization_pkey UNIQUE (_oid);


--
-- TOC entry 2097 (class 2606 OID 47331)
-- Name: losscalculationparameters_pkey; Type: CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_calculationparameters
    ADD CONSTRAINT losscalculationparameters_pkey UNIQUE (_oid);


--
-- TOC entry 2115 (class 2606 OID 47486)
-- Name: vulnerabilityfunction_pkey; Type: CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_vulnerabilityfunction
    ADD CONSTRAINT vulnerabilityfunction_pkey UNIQUE (_oid);


--
-- TOC entry 2111 (class 2606 OID 47476)
-- Name: vulnerabilitymodel_pkey; Type: CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_vulnerabilitymodel
    ADD CONSTRAINT vulnerabilitymodel_pkey UNIQUE (_oid);


--
-- TOC entry 2113 (class 2606 OID 47478)
-- Name: vulnerabilitymodel_publicid; Type: CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_vulnerabilitymodel
    ADD CONSTRAINT vulnerabilitymodel_publicid UNIQUE (m_publicid_resourceid);


--
-- TOC entry 2120 (class 2606 OID 47420)
-- Name: loss_asset__parent_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_asset
    ADD CONSTRAINT loss_asset__parent_oid_fkey FOREIGN KEY (_parent_oid) REFERENCES loss_calculation(_oid);


--
-- TOC entry 2118 (class 2606 OID 47332)
-- Name: loss_calculationparameters__parent_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_calculationparameters
    ADD CONSTRAINT loss_calculationparameters__parent_oid_fkey FOREIGN KEY (_parent_oid) REFERENCES loss_calculation(_oid);


--
-- TOC entry 2124 (class 2606 OID 47498)
-- Name: loss_intensityspecificvulnerability__parent_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_intensityspecificvulnerability
    ADD CONSTRAINT loss_intensityspecificvulnerability__parent_oid_fkey FOREIGN KEY (_parent_oid) REFERENCES loss_vulnerabilityfunction(_oid);


--
-- TOC entry 2121 (class 2606 OID 47453)
-- Name: loss_lossbyasset__parent_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_lossbyasset
    ADD CONSTRAINT loss_lossbyasset__parent_oid_fkey FOREIGN KEY (_parent_oid) REFERENCES loss_calculation(_oid);


--
-- TOC entry 2122 (class 2606 OID 47458)
-- Name: loss_lossbyasset_m_assetid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_lossbyasset
    ADD CONSTRAINT loss_lossbyasset_m_assetid_fkey FOREIGN KEY (m_assetid) REFERENCES loss_asset(m_publicid_resourceid);


--
-- TOC entry 2119 (class 2606 OID 47366)
-- Name: loss_lossbyrealization__parent_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_lossbyrealization
    ADD CONSTRAINT loss_lossbyrealization__parent_oid_fkey FOREIGN KEY (_parent_oid) REFERENCES loss_calculation(_oid);


--
-- TOC entry 2123 (class 2606 OID 47487)
-- Name: loss_vulnerabilityfunction__parent_oid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: seiscomp3
--

ALTER TABLE ONLY loss_vulnerabilityfunction
    ADD CONSTRAINT loss_vulnerabilityfunction__parent_oid_fkey FOREIGN KEY (_parent_oid) REFERENCES loss_vulnerabilitymodel(_oid);


--
-- TOC entry 2245 (class 0 OID 0)
-- Dependencies: 7
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


-- Completed on 2021-04-27 15:53:42

--
-- PostgreSQL database dump complete
--

