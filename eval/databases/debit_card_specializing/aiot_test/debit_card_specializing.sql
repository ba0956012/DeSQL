--
-- PostgreSQL database dump
--

\restrict CD3gbZ9cLhscOZdqDEMmcXxWJb1fwOURojdcqpqkMZEMoIlFZUVIsn8fEYJYzcm

-- Dumped from database version 15.15 (Homebrew)
-- Dumped by pg_dump version 18.0

-- Started on 2026-04-07 08:50:51 CST

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

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 214 (class 1259 OID 41015)
-- Name: customers; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.customers (
    customerid bigint,
    segment text,
    currency text
);


ALTER TABLE public.customers OWNER TO postgres;

--
-- TOC entry 215 (class 1259 OID 41020)
-- Name: gasstations; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.gasstations (
    gasstationid bigint,
    chainid bigint,
    country text,
    segment text
);


ALTER TABLE public.gasstations OWNER TO postgres;

--
-- TOC entry 216 (class 1259 OID 41025)
-- Name: products; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.products (
    productid bigint,
    description text
);


ALTER TABLE public.products OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 41030)
-- Name: transactions_1k; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.transactions_1k (
    transactionid bigint,
    date date,
    "time" text,
    customerid bigint,
    cardid bigint,
    gasstationid bigint,
    productid bigint,
    amount bigint,
    price double precision
);


ALTER TABLE public.transactions_1k OWNER TO postgres;

--
-- TOC entry 218 (class 1259 OID 41035)
-- Name: yearmonth; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.yearmonth (
    customerid bigint,
    date text,
    consumption double precision
);


ALTER TABLE public.yearmonth OWNER TO postgres;

-- Completed on 2026-04-07 08:50:51 CST

--
-- PostgreSQL database dump complete
--

\unrestrict CD3gbZ9cLhscOZdqDEMmcXxWJb1fwOURojdcqpqkMZEMoIlFZUVIsn8fEYJYzcm

