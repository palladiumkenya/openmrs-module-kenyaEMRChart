DROP PROCEDURE IF EXISTS create_datatools_tables$$
CREATE PROCEDURE create_datatools_tables()
BEGIN
DECLARE script_id INT(11);

-- Log start time
INSERT INTO kenyaemr_etl.etl_script_status(script_name, start_time) VALUES('KenyaEMR_Data_Tool', NOW());
SET script_id = LAST_INSERT_ID();

UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= script_id;

END$$

