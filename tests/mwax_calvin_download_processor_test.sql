-- Create simple database objects for base mwax_db tests
--
-- NOTE test database must already exist!
--

--
-- calibration_request
--
DROP TABLE IF EXISTS public.calibration_request;

DROP SEQUENCE IF EXISTS public.calibration_request_id_seq;

CREATE SEQUENCE IF NOT EXISTS public.calibration_request_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 2147483647
    CACHE 1;

CREATE TABLE IF NOT EXISTS public.calibration_request
(
    id integer NOT NULL DEFAULT nextval('calibration_request_id_seq'::regclass),
    cal_id bigint NOT NULL,
    request_added_datetime timestamp with time zone NOT NULL DEFAULT now(),
    assigned_datetime timestamp with time zone,
    assigned_hostname text COLLATE pg_catalog."default",
    download_mwa_asvo_job_submitted_datetime timestamp with time zone,
    download_mwa_asvo_job_id bigint,
    download_started_datetime timestamp with time zone,
    download_completed_datetime timestamp with time zone,
    download_error_datetime timestamp with time zone,
    download_error_message text COLLATE pg_catalog."default",
    calibration_started_datetime timestamp with time zone,
    calibration_completed_datetime timestamp with time zone,
    calibration_error_datetime timestamp with time zone,
    calibration_error_message text COLLATE pg_catalog."default",
    calibration_fit_id bigint,
    CONSTRAINT calibration_request_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

-- Index: calibration_request_cal_id
DROP INDEX IF EXISTS public.calibration_request_cal_id;

CREATE INDEX IF NOT EXISTS calibration_request_cal_id
    ON public.calibration_request USING btree
    (cal_id ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: calibration_request_id
DROP INDEX IF EXISTS public.calibration_request_id;

CREATE UNIQUE INDEX IF NOT EXISTS calibration_request_id
    ON public.calibration_request USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;