--
-- PostgreSQL database dump
--

\restrict Y3Q3pMBsHxvbSXwfdSvkSGfGQSKqGt1v4VqhaBDc87xmQvOuEwpfG1MrxdxJxef

-- Dumped from database version 15.15 (Homebrew)
-- Dumped by pg_dump version 18.0

-- Started on 2026-04-07 09:15:38 CST

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
-- TOC entry 214 (class 1259 OID 40975)
-- Name: account; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.account (
    account_id bigint,
    district_id bigint,
    frequency text,
    date date
);


ALTER TABLE public.account OWNER TO postgres;

--
-- TOC entry 215 (class 1259 OID 40980)
-- Name: card; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.card (
    card_id bigint,
    disp_id bigint,
    type text,
    issued date
);


ALTER TABLE public.card OWNER TO postgres;

--
-- TOC entry 216 (class 1259 OID 40985)
-- Name: client; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.client (
    client_id bigint,
    gender text,
    birth_date date,
    district_id bigint
);


ALTER TABLE public.client OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 40990)
-- Name: disp; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.disp (
    disp_id bigint,
    client_id bigint,
    account_id bigint,
    type text
);


ALTER TABLE public.disp OWNER TO postgres;

--
-- TOC entry 218 (class 1259 OID 40995)
-- Name: district; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.district (
    district_id bigint,
    a2 text,
    a3 text,
    a4 text,
    a5 text,
    a6 text,
    a7 text,
    a8 bigint,
    a9 bigint,
    a10 double precision,
    a11 bigint,
    a12 double precision,
    a13 double precision,
    a14 bigint,
    a15 bigint,
    a16 bigint
);


ALTER TABLE public.district OWNER TO postgres;

--
-- TOC entry 219 (class 1259 OID 41000)
-- Name: loan; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.loan (
    loan_id bigint,
    account_id bigint,
    date date,
    amount bigint,
    duration bigint,
    payments double precision,
    status text
);


ALTER TABLE public.loan OWNER TO postgres;

--
-- TOC entry 220 (class 1259 OID 41005)
-- Name: order; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public."order" (
    order_id bigint,
    account_id bigint,
    bank_to text,
    account_to bigint,
    amount double precision,
    k_symbol text
);


ALTER TABLE public."order" OWNER TO postgres;

--
-- TOC entry 221 (class 1259 OID 41010)
-- Name: trans; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.trans (
    trans_id bigint,
    account_id bigint,
    date date,
    type text,
    operation text,
    amount bigint,
    balance bigint,
    k_symbol text,
    bank text,
    account bigint
);


ALTER TABLE public.trans OWNER TO postgres;

-- Completed on 2026-04-07 09:15:39 CST

--
-- PostgreSQL database dump complete
--

\unrestrict Y3Q3pMBsHxvbSXwfdSvkSGfGQSKqGt1v4VqhaBDc87xmQvOuEwpfG1MrxdxJxef

