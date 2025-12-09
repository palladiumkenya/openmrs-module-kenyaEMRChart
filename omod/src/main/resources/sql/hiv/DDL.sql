DELIMITER $$

DROP PROCEDURE IF EXISTS create_etl_tables $$
CREATE PROCEDURE create_etl_tables()
BEGIN
    DECLARE etl_schema VARCHAR(200);
    DECLARE datatools_schema VARCHAR(200);
    DECLARE script_status_table VARCHAR(300);
    DECLARE script_id INT(11);
    DECLARE current_schema VARCHAR(200);
    DECLARE tenant_suffix VARCHAR(100);

    -- Get the current schema
    SET current_schema = DATABASE();
    SET tenant_suffix = SUBSTRING_INDEX(current_schema, 'openmrs_', -1);




    /* Build dynamic schema names */
    SET etl_schema        = CONCAT('kenyaemr_etl_', tenant_suffix);
    SET datatools_schema  = CONCAT('kenyaemr_datatools_', tenant_suffix);
    SET script_status_table = CONCAT(etl_schema, '.etl_script_status');

    /* --------------------------------------
       DROP & CREATE ETL / DATATOOLS SCHEMAS
       --------------------------------------*/

    /* Disable FK checks temporarily */
    SET FOREIGN_KEY_CHECKS = 0;

    /* Drop ETL schema */
    SET @drop_etl = CONCAT('DROP DATABASE IF EXISTS ', etl_schema, ';');
    PREPARE stmt FROM @drop_etl;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /* Drop DataTools schema */
    SET @drop_dt = CONCAT('DROP DATABASE IF EXISTS ', datatools_schema, ';');
    PREPARE stmt FROM @drop_dt;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /* Re-enable FK checks */
    SET FOREIGN_KEY_CHECKS = 1;

    /* Re-create ETL schema */
    SET @create_etl = CONCAT(
        'CREATE DATABASE ', etl_schema,
        ' DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;'
    );
    PREPARE stmt FROM @create_etl;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /* Re-create DataTools schema */
    SET @create_dt = CONCAT(
        'CREATE DATABASE ', datatools_schema,
        ' DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;'
    );
    PREPARE stmt FROM @create_dt;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /* --------------------------------------
       CREATE etl_script_status TABLE
       --------------------------------------*/

    SET @drop_status = CONCAT(
        'DROP TABLE IF EXISTS ', script_status_table, ';'
    );
    PREPARE stmt FROM @drop_status;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @create_status = CONCAT(
        'CREATE TABLE ', script_status_table, ' (',
        '  id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,',
        '  script_name VARCHAR(50) DEFAULT NULL,',
        '  start_time DATETIME DEFAULT NULL,',
        '  stop_time DATETIME DEFAULT NULL,',
        '  error VARCHAR(255) DEFAULT NULL',
        ');'
    );
    PREPARE stmt FROM @create_status;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /* --------------------------------------
       LOG START TIME
       --------------------------------------*/

    SET @log_start = CONCAT(
        'INSERT INTO ', script_status_table,
        ' (script_name, start_time) VALUES (''initial_creation_of_tables'', NOW());'
    );
    PREPARE stmt FROM @log_start;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET script_id = LAST_INSERT_ID();

-- create table etl_patient_demographics
SET @sql = CONCAT(
    'CREATE TABLE ', etl_schema, '.etl_patient_demographics (',
    'patient_id INT(11) NOT NULL PRIMARY KEY,',
    'uuid CHAR(38),',
    'given_name VARCHAR(255),',
    'middle_name VARCHAR(255),',
    'family_name VARCHAR(255),',
    'Gender VARCHAR(10),',
    'DOB DATE,',
    'national_id_no VARCHAR(50),',
    'huduma_no VARCHAR(50),',
    'passport_no VARCHAR(50),',
    'birth_certificate_no VARCHAR(50),',
    'unique_patient_no VARCHAR(50),',
    'alien_no VARCHAR(50),',
    'driving_license_no VARCHAR(50),',
    'national_unique_patient_identifier VARCHAR(50),',
    'hts_recency_id VARCHAR(50),',
    'nhif_number VARCHAR(50) DEFAULT NULL,',
    'patient_clinic_number VARCHAR(15) DEFAULT NULL,',
    'Tb_no VARCHAR(50),',
    'CPIMS_unique_identifier VARCHAR(50),',
    'openmrs_id VARCHAR(50),',
    'unique_prep_number VARCHAR(50),',
    'district_reg_no VARCHAR(50),',
    'hei_no VARCHAR(50),',
    'cwc_number VARCHAR(50),',
    'sha_number VARCHAR(100),',
    'shif_number VARCHAR(100),',
    'phone_number VARCHAR(50) DEFAULT NULL,',
    'birth_place VARCHAR(50) DEFAULT NULL,',
    'citizenship VARCHAR(50) DEFAULT NULL,',
    'email_address VARCHAR(100) DEFAULT NULL,',
    'occupation VARCHAR(100) DEFAULT NULL,',
    'next_of_kin VARCHAR(255) DEFAULT NULL,',
    'next_of_kin_phone VARCHAR(100) DEFAULT NULL,',
    'next_of_kin_relationship VARCHAR(100) DEFAULT NULL,',
    'marital_status VARCHAR(50) DEFAULT NULL,',
    'education_level VARCHAR(50) DEFAULT NULL,',
    'kdod_service_number VARCHAR(50) DEFAULT NULL,',
    'cadre VARCHAR(100) DEFAULT NULL,',
    'kdod_rank VARCHAR(100) DEFAULT NULL,',
    'unit VARCHAR(100) DEFAULT NULL,',
    'dead INT(11),',
    'death_date DATE DEFAULT NULL,',
    'voided INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'INDEX(patient_id),',
    'INDEX(Gender),',
    'INDEX(unique_patient_no),',
    'INDEX(DOB)',
    ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


END $$

DELIMITER ;