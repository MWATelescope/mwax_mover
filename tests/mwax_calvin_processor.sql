-- Table: public.calibration_fits
DROP TABLE IF EXISTS public.calibration_fits;

CREATE TABLE IF NOT EXISTS public.calibration_fits
(
    fitid bigint NOT NULL,
    obsid bigint NOT NULL,
    code_version text COLLATE pg_catalog."default",
    fit_time timestamp with time zone,
    creator text COLLATE pg_catalog."default",
    fit_niter integer,
    fit_limit real,
    filename text COLLATE pg_catalog."default",
    CONSTRAINT calibration_fits_pkey PRIMARY KEY (fitid)
)
TABLESPACE pg_default;

-- Index: calibration_fits_obsid_index
DROP INDEX IF EXISTS public.calibration_fits_obsid_index;

CREATE INDEX IF NOT EXISTS calibration_fits_obsid_index
    ON public.calibration_fits USING btree
    (obsid ASC NULLS LAST)
    TABLESPACE pg_default;

-- Table: public.calibration_solutions
DROP TABLE IF EXISTS public.calibration_solutions;

CREATE TABLE IF NOT EXISTS public.calibration_solutions
(
    fitid bigint NOT NULL,
    obsid bigint NOT NULL,
    tileid integer NOT NULL,
    x_delay_m real,
    y_delay_m real,
    x_intercept real,
    y_intercept real,
    x_gains real[],
    y_gains real[],
    x_gains_pol1 real[],
    y_gains_pol1 real[],
    x_gains_pol0 real[],
    y_gains_pol0 real[],
    x_phase_sigma_resid real,
    y_phase_sigma_resid real,
    x_phase_chi2dof real,
    y_phase_chi2dof real,
    x_phase_fit_quality real,
    y_phase_fit_quality real,
    x_gains_sigma_resid real[],
    y_gains_sigma_resid real[],
    x_gains_fit_quality real,
    y_gains_fit_quality real,
    CONSTRAINT calibration_solutions_pkey PRIMARY KEY (fitid, obsid, tileid)
)
TABLESPACE pg_default;


-- Index: calibration_solutions_obsid_index
DROP INDEX IF EXISTS public.calibration_solutions_obsid_index;

CREATE INDEX IF NOT EXISTS calibration_solutions_obsid_index
    ON public.calibration_solutions USING btree
    (obsid ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: calibration_solutions_tileid_index
DROP INDEX IF EXISTS public.calibration_solutions_tileid_index;

CREATE INDEX IF NOT EXISTS calibration_solutions_tileid_index
    ON public.calibration_solutions USING btree
    (tileid ASC NULLS LAST)
    TABLESPACE pg_default;