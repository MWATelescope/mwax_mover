-- Create simple database objects for base mwax_db tests
--
-- NOTE test database must already exist!
--

-- Test table
DROP TABLE IF EXISTS public.test_table;

CREATE TABLE IF NOT EXISTS public.test_table
(    
    id bigint NOT NULL,
    name varchar NULL,
    CONSTRAINT test_table_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

-- Test table: insert test data
INSERT INTO public.test_table VALUES (1, 'abc');
INSERT INTO public.test_table VALUES (2, 'def');
INSERT INTO public.test_table VALUES (3, 'ghi');