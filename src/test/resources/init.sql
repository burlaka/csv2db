CREATE TABLE table_name (
	col_uuid uuid,
	col_text varchar(18) NOT NULL,
	col_timestamp timestamp NOT NULL,
	col_timestamp_now timestamp NOT NULL,
	col_boolean bool NOT NULL,
	col_bigint BIGINT NOT NULL,
	col_float FLOAT NOT NULL,
	col_numeric NUMERIC NOT NULL,
	col_int INT NOT NULL
);

ALTER TABLE table_name ADD CONSTRAINT unique_text UNIQUE (col_text);

