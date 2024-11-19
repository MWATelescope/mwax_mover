-- Create simple database objects for base mwax_db_dbhandler tests
--
-- NOTE test database must already exist!
--

-- mwa_setting
-- Table: public.mwa_setting
DROP TABLE IF EXISTS public.mwa_setting CASCADE;

CREATE TABLE IF NOT EXISTS public.mwa_setting
(
    starttime bigint NOT NULL,
    stoptime bigint,
    obsname text COLLATE pg_catalog."default",
    creator text COLLATE pg_catalog."default" DEFAULT 'mwa'::text,
    modtime timestamp with time zone DEFAULT now(),
    mode text COLLATE pg_catalog."default" NOT NULL DEFAULT 'NO_CAPTURE'::text,
    unpowered_tile_name text COLLATE pg_catalog."default" DEFAULT 'default'::text,
    mode_params text COLLATE pg_catalog."default",
    dec_phase_center double precision,
    ra_phase_center double precision,
    projectid text COLLATE pg_catalog."default" NOT NULL DEFAULT 'C001'::text,
    dataquality integer DEFAULT 1,
    dataqualitycomment text COLLATE pg_catalog."default",
    int_time double precision DEFAULT 0.5,
    freq_res double precision DEFAULT 40,
    processed boolean,
    deleted boolean,
    groupid bigint,
    deleted_timestamp timestamp with time zone,
    delaymode_name text COLLATE pg_catalog."default",
    deripple boolean,
    CONSTRAINT mwa_setting_pkey PRIMARY KEY (starttime)
    /*,
    CONSTRAINT mwa_setting_projectid_fkey FOREIGN KEY (projectid)
        REFERENCES public.mwa_project (projectid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION*/
)
TABLESPACE pg_default;

-- Index: mwa_setting_dataquality_index
DROP INDEX IF EXISTS public.mwa_setting_dataquality_index;

CREATE INDEX IF NOT EXISTS mwa_setting_dataquality_index
    ON public.mwa_setting USING btree
    (dataquality ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: mwa_setting_deleted_timestamp
DROP INDEX IF EXISTS public.mwa_setting_deleted_timestamp;

CREATE INDEX IF NOT EXISTS mwa_setting_deleted_timestamp
    ON public.mwa_setting USING btree
    (deleted_timestamp ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: mwa_setting_mode_index
DROP INDEX IF EXISTS public.mwa_setting_mode_index;

CREATE INDEX IF NOT EXISTS mwa_setting_mode_index
    ON public.mwa_setting USING btree
    (mode COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: mwa_setting_modtime
DROP INDEX IF EXISTS public.mwa_setting_modtime;

CREATE INDEX IF NOT EXISTS mwa_setting_modtime
    ON public.mwa_setting USING btree
    (modtime ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: mwa_setting_projectid_index
DROP INDEX IF EXISTS public.mwa_setting_projectid_index;

CREATE INDEX IF NOT EXISTS mwa_setting_projectid_index
    ON public.mwa_setting USING btree
    (projectid COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: mwa_setting_stoptime_index
DROP INDEX IF EXISTS public.mwa_setting_stoptime_index;

CREATE INDEX IF NOT EXISTS mwa_setting_stoptime_index
    ON public.mwa_setting USING btree
    (stoptime ASC NULLS LAST)
    TABLESPACE pg_default;

-- Table: public.data_files
DROP TABLE IF EXISTS public.data_files CASCADE;

CREATE TABLE IF NOT EXISTS public.data_files
(
    observation_num bigint NOT NULL,
    filetype integer NOT NULL,
    size bigint,
    filename text COLLATE pg_catalog."default" NOT NULL,
    modtime timestamp with time zone DEFAULT now(),
    host text COLLATE pg_catalog."default",
    remote_archived boolean NOT NULL,
    deleted boolean NOT NULL,
    location integer,
    deleted_timestamp timestamp with time zone,
    checksum_type integer,
    checksum character varying(127) COLLATE pg_catalog."default",
    folder text COLLATE pg_catalog."default",
    bucket text COLLATE pg_catalog."default",
    trigger_id integer,
    CONSTRAINT data_files_pkey PRIMARY KEY (observation_num, filename),
    CONSTRAINT data_files_fk FOREIGN KEY (observation_num)
        REFERENCES public.mwa_setting (starttime) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
    /*,
    CONSTRAINT data_files_checksum_types_fk FOREIGN KEY (checksum_type)
        REFERENCES public.checksum_types (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,    
    CONSTRAINT data_files_location_fkey FOREIGN KEY (location)
        REFERENCES public.data_locations (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT data_files_type_fk FOREIGN KEY (filetype)
        REFERENCES public.data_file_types (type) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION*/
);

DROP INDEX IF EXISTS public.data_files_deleted_timestamp_idx;

-- Index: data_files_deleted_timestamp_idx
CREATE INDEX IF NOT EXISTS data_files_deleted_timestamp_idx
    ON public.data_files USING btree
    (deleted_timestamp ASC NULLS LAST)

    WHERE deleted_timestamp IS NOT NULL;

-- Index: data_files_filetype_idx
DROP INDEX IF EXISTS public.data_files_filetype_idx;
CREATE INDEX IF NOT EXISTS data_files_filetype_idx
    ON public.data_files USING btree
    (filetype ASC NULLS LAST)
;

-- Index: data_files_modtime_idx
DROP INDEX IF EXISTS public.data_files_modtime_idx;
CREATE INDEX IF NOT EXISTS data_files_modtime_idx
    ON public.data_files USING btree
    (modtime ASC NULLS LAST)
;

-- Index: data_files_trigger_id_idx

DROP INDEX IF EXISTS public.data_files_trigger_id_idx;
CREATE INDEX IF NOT EXISTS data_files_trigger_id_idx
    ON public.data_files USING btree
    (trigger_id ASC NULLS LAST)

    WHERE trigger_id IS NOT NULL;

--
-- test data
--
INSERT INTO mwa_setting
(starttime, stoptime, obsname, creator, modtime, mode, unpowered_tile_name, mode_params, dec_phase_center, ra_phase_center, projectid, dataquality, dataqualitycomment, int_time, freq_res, processed, deleted, groupid, deleted_timestamp, delaymode_name, deripple)
VALUES 
(1234567890, 1234567898, 'mwax_db_test1', 'gsleap', '2024-11-18', 'MWAX_CORRELATOR', 'default', NULL, -17.02, 333.6, 'C001', 1, NULL, 2, 40, NULL, NULL, 1234567890, NULL, 'FULLTRACK', false);

INSERT INTO data_files
(observation_num, filetype, size, filename, modtime, host, remote_archived, deleted, location, deleted_timestamp, checksum_type, checksum, folder, bucket, trigger_id)
VALUES
(1234567890, 18, 1024, '1234567890_202411191234_ch123_000.fits', '2024-11-18', 'test_host', false, false, 2, NULL, 1, '1a2b3c4d5e6f', NULL, NULL, NULL);