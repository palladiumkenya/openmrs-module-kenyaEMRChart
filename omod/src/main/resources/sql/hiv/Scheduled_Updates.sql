sql
DELIMITER $$

SET @OLD_SQL_MODE=@@SQL_MODE $$
SET SQL_MODE='' $$

DROP PROCEDURE IF EXISTS sp_set_tenant_session_vars $$
CREATE PROCEDURE sp_set_tenant_session_vars()
BEGIN
    DECLARE current_schema VARCHAR(200);
    DECLARE tenant_suffix VARCHAR(100);
    DECLARE etl_schema VARCHAR(200);
    DECLARE sql_stmt TEXT;

    SET current_schema = DATABASE();
    IF INSTR(current_schema, 'openmrs_') = 0 THEN
        SET tenant_suffix = '';
ELSE
        SET tenant_suffix = SUBSTRING_INDEX(current_schema, 'openmrs_', -1);
END IF;

    IF tenant_suffix <> '' AND tenant_suffix NOT REGEXP '^[A-Za-z0-9_]+$' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid tenant suffix in DATABASE()';
END IF;

    SET etl_schema = IF(tenant_suffix = '', 'kenyaemr_etl', CONCAT('kenyaemr_etl_', tenant_suffix));
    SET @etl_schema = etl_schema;
    SET @script_status_table = CONCAT(etl_schema, '.etl_script_status');
    SET @script_status_table_quoted = CONCAT('`', etl_schema, '`.`etl_script_status`');

    -- ensure tenant ETL database exists
    SET sql_stmt = CONCAT('CREATE DATABASE IF NOT EXISTS `', etl_schema, '` DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- ensure etl_script_status table exists
SET sql_stmt = CONCAT(
      'CREATE TABLE IF NOT EXISTS ', @script_status_table_quoted, ' (',
        'id INT NOT NULL AUTO_INCREMENT,',
        'script_name VARCHAR(200) NOT NULL,',
        'start_time DATETIME NOT NULL,',
        'stop_time DATETIME NULL,',
        'status VARCHAR(50) DEFAULT NULL,',
        'message TEXT,',
        'PRIMARY KEY (id),',
        'INDEX (script_name),',
        'INDEX (start_time)',
      ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
    );
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
END $$

DROP PROCEDURE IF EXISTS sp_update_etl_patient_demographics $$
CREATE PROCEDURE sp_update_etl_patient_demographics(IN last_update_time DATETIME)
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table_quoted VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET target_table_quoted = CONCAT('`', @etl_schema, '`.`etl_patient_demographics`');

    -- INSERT / UPSERT patient demographics (only rows changed since last_update_time)
    SET sql_stmt = CONCAT(
      'INSERT INTO ', target_table_quoted, ' (patient_id, uuid, given_name, middle_name, family_name, Gender, DOB, dead, date_created, date_last_modified, voided, death_date) ',
      'SELECT p.person_id, p.uuid, p.given_name, p.middle_name, p.family_name, p.gender, p.birthdate, p.dead, p.date_created, ',
      'IF((p.date_last_modified = ''0000-00-00 00:00:00'' OR p.date_last_modified = p.date_created), NULL, p.date_last_modified) AS date_last_modified, p.voided, p.death_date ',
      'FROM (',
        'SELECT p.person_id, p.uuid, pn.given_name, pn.middle_name, pn.family_name, p.gender, p.birthdate, p.dead, p.date_created, ',
        'GREATEST(IFNULL(p.date_changed, ''0000-00-00 00:00:00''), IFNULL(pn.date_changed, ''0000-00-00 00:00:00'')) AS date_last_modified, p.voided, p.death_date ',
        'FROM person p ',
        'LEFT JOIN patient pa ON pa.patient_id = p.person_id AND pa.voided = 0 ',
        'INNER JOIN person_name pn ON pn.person_id = p.person_id AND pn.voided = 0 ',
        'WHERE (pn.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
          ' OR pn.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
          ' OR pn.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
          ' OR p.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
          ' OR p.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
          ' OR p.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
        'GROUP BY p.person_id',
      ') p ',
      'ON DUPLICATE KEY UPDATE given_name = p.given_name, middle_name = p.middle_name, family_name = p.family_name, DOB = p.birthdate, dead = p.dead, voided = p.voided, death_date = p.death_date;'
    );
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- UPDATE attributes (phone, next_of_kin, birthplace, etc.)
SET sql_stmt = CONCAT(
      'UPDATE ', target_table_quoted, ' d ',
      'INNER JOIN (',
         'SELECT pa.person_id, ',
           'MAX(IF(pat.uuid = ''8d8718c2-c2cc-11de-8d13-0010c6dffd0f'', pa.value, NULL)) AS birthplace, ',
           'MAX(IF(pat.uuid = ''8d871afc-c2cc-11de-8d13-0010c6dffd0f'', pa.value, NULL)) AS citizenship, ',
           'MAX(IF(pat.uuid = ''8d871d18-c2cc-11de-8d13-0010c6dffd0f'', pa.value, NULL)) AS Mother_name, ',
           'MAX(IF(pat.uuid = ''b2c38640-2603-4629-aebd-3b54f33f1e3a'', pa.value, NULL)) AS phone_number, ',
           'MAX(IF(pat.uuid = ''342a1d39-c541-4b29-8818-930916f4c2dc'', pa.value, NULL)) AS next_of_kin_contact, ',
           'MAX(IF(pat.uuid = ''d0aa9fd1-2ac5-45d8-9c5e-4317c622c8f5'', pa.value, NULL)) AS next_of_kin_relationship, ',
           'MAX(IF(pat.uuid = ''7cf22bec-d90a-46ad-9f48-035952261294'', pa.value, NULL)) AS next_of_kin_address, ',
           'MAX(IF(pat.uuid = ''830bef6d-b01f-449d-9f8d-ac0fede8dbd3'', pa.value, NULL)) AS next_of_kin_name, ',
           'MAX(IF(pat.uuid = ''b8d0b331-1d2d-4a9a-b741-1816f498bdb6'', pa.value, NULL)) AS email_address, ',
           'MAX(IF(pat.uuid = ''848f5688-41c6-464c-b078-ea6524a3e971'', pa.value, NULL)) AS unit, ',
           'MAX(IF(pat.uuid = ''96a99acd-2f11-45bb-89f7-648dbcac5ddf'', pa.value, NULL)) AS cadre, ',
           'MAX(IF(pat.uuid = ''9f1f8254-20ea-4be4-a14d-19201fe217bf'', pa.value, NULL)) AS kdod_rank, ',
           'GREATEST(IFNULL(pa.date_changed, ''0000-00-00''), pa.date_created) AS latest_date ',
         'FROM person_attribute pa ',
         'INNER JOIN (SELECT pat.person_attribute_type_id, pat.name, pat.uuid FROM person_attribute_type pat WHERE pat.retired = 0) pat ',
           'ON pat.person_attribute_type_id = pa.person_attribute_type_id ',
         'WHERE (pa.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
           ' OR pa.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
           ' OR pa.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
         'AND pa.voided = 0 ',
         'AND pat.uuid IN (',
           '''8d8718c2-c2cc-11de-8d13-0010c6dffd0f'',',
           '''8d871afc-c2cc-11de-8d13-0010c6dffd0f'',',
           '''8d871d18-c2cc-11de-8d13-0010c6dffd0f'',',
           '''b2c38640-2603-4629-aebd-3b54f33f1e3a'',',
           '''342a1d39-c541-4b29-8818-930916f4c2dc'',',
           '''d0aa9fd1-2ac5-45d8-9c5e-4317c622c8f5'',',
           '''7cf22bec-d90a-46ad-9f48-035952261294'',',
           '''830bef6d-b01f-449d-9f8d-ac0fede8dbd3'',',
           '''b8d0b331-1d2d-4a9a-b741-1816f498bdb6'',',
           '''848f5688-41c6-464c-b078-ea6524a3e971'',',
           '''96a99acd-2f11-45bb-89f7-648dbcac5ddf'',',
           '''9f1f8254-20ea-4be4-a14d-19201fe217bf''',
         ') ',
         'GROUP BY pa.person_id',
      ') att ON att.person_id = d.patient_id ',
      'SET d.phone_number = att.phone_number, ',
          'd.next_of_kin = att.next_of_kin_name, ',
          'd.next_of_kin_relationship = att.next_of_kin_relationship, ',
          'd.next_of_kin_phone = att.next_of_kin_contact, ',
          'd.birth_place = att.birthplace, ',
          'd.citizenship = att.citizenship, ',
          'd.email_address = att.email_address, ',
          'd.unit = att.unit, ',
          'd.cadre = att.cadre, ',
          'd.kdod_rank = att.kdod_rank, ',
          'd.date_last_modified = IF(att.latest_date > IFNULL(d.date_last_modified, ''0000-00-00''), att.latest_date, d.date_last_modified);'
    );
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- UPDATE identifiers
SET sql_stmt = CONCAT(
      'UPDATE ', target_table_quoted, ' d ',
      'INNER JOIN (',
         'SELECT pi.patient_id, ',
           'COALESCE(MAX(IF(pit.uuid = ''05ee9cf4-7242-4a17-b4d4-00f707265c8a'', pi.identifier, NULL)), MAX(IF(pit.uuid = ''b51ffe55-3e76-44f8-89a2-14f5eaf11079'', pi.identifier, NULL))) AS upn, ',
           'MAX(IF(pit.uuid = ''d8ee3b8c-a8fc-4d6b-af6a-9423be5f8906'', pi.identifier, NULL)) AS district_reg_number, ',
           'MAX(IF(pit.uuid = ''c4e3caca-2dcc-4dc4-a8d9-513b6e63af91'', pi.identifier, NULL)) AS Tb_treatment_number, ',
           'MAX(IF(pit.uuid = ''b4d66522-11fc-45c7-83e3-39a1af21ae0d'', pi.identifier, NULL)) AS Patient_clinic_number, ',
           'MAX(IF(pit.uuid = ''49af6cdc-7968-4abb-bf46-de10d7f4859f'', pi.identifier, NULL)) AS National_id, ',
           'MAX(IF(pit.uuid = ''6428800b-5a8c-4f77-a285-8d5f6174e5fb'', pi.identifier, NULL)) AS Huduma_number, ',
           'MAX(IF(pit.uuid = ''be9beef6-aacc-4e1f-ac4e-5babeaa1e303'', pi.identifier, NULL)) AS Passport_number, ',
           'MAX(IF(pit.uuid = ''68449e5a-8829-44dd-bfef-c9c8cf2cb9b2'', pi.identifier, NULL)) AS Birth_cert_number, ',
           'MAX(IF(pit.uuid = ''0691f522-dd67-4eeb-92c8-af5083baf338'', pi.identifier, NULL)) AS Hei_id, ',
           'MAX(IF(pit.uuid = ''1dc8b419-35f2-4316-8d68-135f0689859b'', pi.identifier, NULL)) AS cwc_number, ',
           'MAX(IF(pit.uuid = ''f2b0c94f-7b2b-4ab0-aded-0d970f88c063'', pi.identifier, NULL)) AS kdod_service_number, ',
           'MAX(IF(pit.uuid = ''5065ae70-0b61-11ea-8d71-362b9e155667'', pi.identifier, NULL)) AS CPIMS_unique_identifier, ',
           'MAX(IF(pit.uuid = ''dfacd928-0370-4315-99d7-6ec1c9f7ae76'', pi.identifier, NULL)) AS openmrs_id, ',
           'MAX(IF(pit.uuid = ''ac64e5cb-e3e2-4efa-9060-0dd715a843a1'', pi.identifier, NULL)) AS unique_prep_number, ',
           'MAX(IF(pit.uuid = ''1c7d0e5b-2068-4816-a643-8de83ab65fbf'', pi.identifier, NULL)) AS alien_no, ',
           'MAX(IF(pit.uuid = ''ca125004-e8af-445d-9436-a43684150f8b'', pi.identifier, NULL)) AS driving_license_no, ',
           'MAX(IF(pit.uuid = ''f85081e2-b4be-4e48-b3a4-7994b69bb101'', pi.identifier, NULL)) AS national_unique_patient_identifier, ',
           'REPLACE(MAX(IF(pit.uuid = ''fd52829a-75d2-4732-8e43-4bff8e5b4f1a'', pi.identifier, NULL)), ''-'', '''') AS hts_recency_id, ',
           'MAX(IF(pit.uuid = ''09ebf4f9-b673-4d97-b39b-04f94088ba64'', pi.identifier, NULL)) AS nhif_number, ',
           'MAX(IF(pit.uuid = ''52c3c0c3-05b8-4b26-930e-2a6a54e14c90'', pi.identifier, NULL)) AS shif_number, ',
           'MAX(IF(pit.uuid = ''24aedd37-b5be-4e08-8311-3721b8d5100d'', pi.identifier, NULL)) AS sha_number, ',
           'GREATEST(IFNULL(MAX(pi.date_changed), ''0000-00-00''), MAX(pi.date_created)) AS latest_date ',
         'FROM patient_identifier pi ',
         'JOIN patient_identifier_type pit ON pi.identifier_type = pit.patient_identifier_type_id ',
         'WHERE (pi.voided = 0) AND (pi.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
           ' OR pi.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
           ' OR pi.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
         'GROUP BY pi.patient_id',
      ') pid ON pid.patient_id = d.patient_id ',
      'SET d.unique_patient_no = pid.upn, ',
          'd.national_id_no = pid.National_id, ',
          'd.huduma_no = pid.Huduma_number, ',
          'd.passport_no = pid.Passport_number, ',
          'd.birth_certificate_no = pid.Birth_cert_number, ',
          'd.patient_clinic_number = pid.Patient_clinic_number, ',
          'd.hei_no = pid.Hei_id, ',
          'd.cwc_number = pid.cwc_number, ',
          'd.Tb_no = pid.Tb_treatment_number, ',
          'd.district_reg_no = pid.district_reg_number, ',
          'd.kdod_service_number = pid.kdod_service_number, ',
          'd.CPIMS_unique_identifier = pid.CPIMS_unique_identifier, ',
          'd.openmrs_id = pid.openmrs_id, ',
          'd.unique_prep_number = pid.unique_prep_number, ',
          'd.alien_no = pid.alien_no, ',
          'd.driving_license_no = pid.driving_license_no, ',
          'd.national_unique_patient_identifier = pid.national_unique_patient_identifier, ',
          'd.hts_recency_id = pid.hts_recency_id, ',
          'd.nhif_number = pid.nhif_number, ',
          'd.shif_number = pid.shif_number, ',
          'd.sha_number = pid.sha_number, ',
          'd.date_last_modified = IF(pid.latest_date > IFNULL(d.date_last_modified, ''0000-00-00''), pid.latest_date, d.date_last_modified);'
    );
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- UPDATE obs-based statuses (marital, education, occupation)
SET sql_stmt = CONCAT(
      'UPDATE ', target_table_quoted, ' d ',
      'INNER JOIN (',
         'SELECT o.person_id AS patient_id, ',
           'MAX(IF(o.concept_id IN (1054), cn.name, NULL)) AS marital_status, ',
           'MAX(IF(o.concept_id IN (1712), cn.name, NULL)) AS education_level, ',
           'MAX(IF(o.concept_id IN (1542), cn.name, NULL)) AS occupation, ',
           'MAX(o.date_created) AS date_created ',
         'FROM obs o ',
         'JOIN concept_name cn ON cn.concept_id = o.value_coded AND cn.concept_name_type = ''FULLY_SPECIFIED'' AND cn.locale = ''en'' ',
         'WHERE o.concept_id IN (1054,1712,1542) AND o.voided = 0 ',
         'AND (o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
           ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
         'GROUP BY person_id',
      ') pstatus ON pstatus.patient_id = d.patient_id ',
      'SET d.marital_status = pstatus.marital_status, ',
          'd.education_level = pstatus.education_level, ',
          'd.occupation = pstatus.occupation, ',
          'd.date_last_modified = IF(pstatus.date_created > IFNULL(d.date_last_modified, ''0000-00-00''), pstatus.date_created, d.date_last_modified);'
    );
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END $$
SET sql_mode=@OLD_SQL_MODE $$

DROP PROCEDURE IF EXISTS sp_update_etl_hiv_enrollment $$
CREATE PROCEDURE sp_update_etl_hiv_enrollment(IN last_update_time DATETIME)
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table_quoted VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET target_table_quoted = CONCAT('`', @etl_schema, '`.`etl_hiv_enrollment`');

    SET sql_stmt = CONCAT(
      'INSERT INTO ', target_table_quoted, ' (',
      'patient_id, uuid, visit_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, patient_type, date_first_enrolled_in_care, entry_point, ',
      'transfer_in_date, facility_transferred_from, district_transferred_from, previous_regimen, date_started_art_at_transferring_facility, date_confirmed_hiv_positive, facility_confirmed_hiv_positive, ',
      'arv_status, ever_on_pmtct, ever_on_pep, ever_on_prep, ever_on_haart, cd4_test_result, cd4_test_date, viral_load_test_result, viral_load_test_date, who_stage, name_of_treatment_supporter, ',
      'relationship_of_treatment_supporter, treatment_supporter_telephone, treatment_supporter_address, in_school, orphan, date_of_discontinuation, discontinuation_reason, voided',
      ') ',
      'SELECT ',
      'e.patient_id, e.uuid, e.visit_id, e.encounter_datetime AS visit_date, e.location_id, e.encounter_id, e.creator, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id IN (164932), o.value_coded, IF(o.concept_id = 160563 AND o.value_coded = 1065, 160563, NULL))) AS patient_type, ',
      'MAX(IF(o.concept_id = 160555, o.value_datetime, NULL)) AS date_first_enrolled_in_care, ',
      'MAX(IF(o.concept_id = 160540, o.value_coded, NULL)) AS entry_point, ',
      'MAX(IF(o.concept_id = 160534, o.value_datetime, NULL)) AS transfer_in_date, ',
      'MAX(IF(o.concept_id = 160535, LEFT(TRIM(o.value_text),100), NULL)) AS facility_transferred_from, ',
      'MAX(IF(o.concept_id = 161551, LEFT(TRIM(o.value_text),100), NULL)) AS district_transferred_from, ',
      'MAX(IF(o.concept_id = 164855, o.value_coded, NULL)) AS previous_regimen, ',
      'MAX(IF(o.concept_id = 159599, o.value_datetime, NULL)) AS date_started_art_at_transferring_facility, ',
      'MAX(IF(o.concept_id = 160554, o.value_datetime, NULL)) AS date_confirmed_hiv_positive, ',
      'MAX(IF(o.concept_id = 160632, LEFT(TRIM(o.value_text),100), NULL)) AS facility_confirmed_hiv_positive, ',
      'MAX(IF(o.concept_id = 160533, o.value_coded, NULL)) AS arv_status, ',
      'MAX(IF(o.concept_id = 1148, o.value_coded, NULL)) AS ever_on_pmtct, ',
      'MAX(IF(o.concept_id = 1691, o.value_coded, NULL)) AS ever_on_pep, ',
      'MAX(IF(o.concept_id = 165269, o.value_coded, NULL)) AS ever_on_prep, ',
      'MAX(IF(o.concept_id = 1181, o.value_coded, NULL)) AS ever_on_haart, ',
      'MAX(IF(o.concept_id = 5497, o.value_numeric, NULL)) AS cd4_test_result, ',
      'MAX(IF(o.concept_id = 159376, o.value_datetime, NULL)) AS cd4_test_date, ',
      'MAX(IF(o.concept_id = 1305 AND o.value_coded = 1302, ''LDL'', IF(o.concept_id = 162086, o.value_text, NULL))) AS viral_load_test_result, ',
      'MAX(IF(o.concept_id = 163281, DATE(o.value_datetime), NULL)) AS viral_load_test_date, ',
      'MAX(IF(o.concept_id = 5356, o.value_coded, NULL)) AS who_stage, ',
      'MAX(IF(o.concept_id = 160638, LEFT(TRIM(o.value_text),100), NULL)) AS name_of_treatment_supporter, ',
      'MAX(IF(o.concept_id = 160640, o.value_coded, NULL)) AS relationship_of_treatment_supporter, ',
      'MAX(IF(o.concept_id = 160642, LEFT(TRIM(o.value_text),100), NULL)) AS treatment_supporter_telephone, ',
      'MAX(IF(o.concept_id = 160641, LEFT(TRIM(o.value_text),100), NULL)) AS treatment_supporter_address, ',
      'MAX(IF(o.concept_id = 5629, o.value_coded, NULL)) AS in_school, ',
      'MAX(IF(o.concept_id = 1174, o.value_coded, NULL)) AS orphan, ',
      'MAX(IF(o.concept_id = 164384, o.value_datetime, NULL)) AS date_of_discontinuation, ',
      'MAX(IF(o.concept_id = 161555, o.value_coded, NULL)) AS discontinuation_reason, ',
      'e.voided ',
      'FROM encounter e ',
      'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid = ''de78a6be-bfc5-4634-adc3-5f1a280455cc'') et ON et.encounter_type_id = e.encounter_type ',
      'JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (160555,160540,160534,160535,161551,159599,160554,160632,160533,160638,160640,160642,160641,164932,160563,5629,1174,1088,161555,164855,164384,1148,1691,165269,1181,5356,5497,159376,1305,162086,163281) ',
      'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
            ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
            ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
            ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
            ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
      'GROUP BY e.patient_id, e.encounter_id ',
      'ON DUPLICATE KEY UPDATE visit_date = VALUES(visit_date), encounter_provider = VALUES(encounter_provider), patient_type = VALUES(patient_type), date_first_enrolled_in_care = VALUES(date_first_enrolled_in_care), entry_point = VALUES(entry_point), transfer_in_date = VALUES(transfer_in_date), ',
      'facility_transferred_from = VALUES(facility_transferred_from), district_transferred_from = VALUES(district_transferred_from), previous_regimen = VALUES(previous_regimen), date_started_art_at_transferring_facility = VALUES(date_started_art_at_transferring_facility), ',
      'date_confirmed_hiv_positive = VALUES(date_confirmed_hiv_positive), facility_confirmed_hiv_positive = VALUES(facility_confirmed_hiv_positive), arv_status = VALUES(arv_status), ever_on_pmtct = VALUES(ever_on_pmtct), ever_on_pep = VALUES(ever_on_pep), ever_on_prep = VALUES(ever_on_prep), ever_on_haart = VALUES(ever_on_haart), ',
      'who_stage = VALUES(who_stage), name_of_treatment_supporter = VALUES(name_of_treatment_supporter), relationship_of_treatment_supporter = VALUES(relationship_of_treatment_supporter), treatment_supporter_telephone = VALUES(treatment_supporter_telephone), treatment_supporter_address = VALUES(treatment_supporter_address), ',
      'in_school = VALUES(in_school), orphan = VALUES(orphan), voided = VALUES(voided), date_of_discontinuation = VALUES(date_of_discontinuation), discontinuation_reason = VALUES(discontinuation_reason), cd4_test_result = VALUES(cd4_test_result), cd4_test_date = VALUES(cd4_test_date), viral_load_test_result = VALUES(viral_load_test_result), viral_load_test_date = VALUES(viral_load_test_date);'
    );
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
END $$

-- ------------- update etl_hiv_followup--------------------------------
-- sql
DROP PROCEDURE IF EXISTS sp_update_etl_hiv_followup $$
CREATE PROCEDURE sp_update_etl_hiv_followup(IN last_update_time DATETIME)
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table_quoted VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET target_table_quoted = CONCAT('`', @etl_schema, '`.`etl_patient_hiv_followup`');

SET sql_stmt = CONCAT(
'INSERT INTO ', target_table_quoted, ' (',
'uuid, patient_id, visit_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, ',
'visit_scheduled, person_present, weight, systolic_pressure, diastolic_pressure, height, temperature, pulse_rate, respiratory_rate, ',
'oxygen_saturation, muac, z_score_absolute, z_score, nutritional_status, population_type, key_population_type, who_stage, ',
'who_stage_associated_oi, presenting_complaints, clinical_notes, on_anti_tb_drugs, on_ipt, ever_on_ipt, cough, fever, ',
'weight_loss_poor_gain, night_sweats, tb_case_contact, lethargy, screened_for_tb, spatum_smear_ordered, chest_xray_ordered, genexpert_ordered, ',
'spatum_smear_result, chest_xray_result, genexpert_result, referral, clinical_tb_diagnosis, contact_invitation, evaluated_for_ipt, ',
'has_known_allergies, has_chronic_illnesses_cormobidities, has_adverse_drug_reaction, pregnancy_status, breastfeeding, wants_pregnancy, ',
'pregnancy_outcome, anc_number, expected_delivery_date, ever_had_menses, last_menstrual_period, menopausal, gravida, parity, ',
'full_term_pregnancies, abortion_miscarriages, family_planning_status, family_planning_method, reason_not_using_family_planning, ',
'tb_status, started_anti_TB, tb_rx_date, tb_treatment_no, general_examination, system_examination, skin_findings, eyes_findings, ',
'ent_findings, chest_findings, cvs_findings, abdomen_findings, cns_findings, genitourinary_findings, prophylaxis_given, ctx_adherence, ',
'ctx_dispensed, dapsone_adherence, dapsone_dispensed, inh_dispensed, arv_adherence, poor_arv_adherence_reason, poor_arv_adherence_reason_other, ',
'pwp_disclosure, pwp_pead_disclosure, pwp_partner_tested, condom_provided, substance_abuse_screening, screened_for_sti, cacx_screening, ',
'sti_partner_notification, experienced_gbv, depression_screening, at_risk_population, system_review_finding, next_appointment_date, refill_date, ',
'appointment_consent, next_appointment_reason, stability, differentiated_care_group, differentiated_care, established_differentiated_care, ',
'insurance_type, other_insurance_specify, insurance_status, voided',
') ',
'SELECT ',
'e.uuid, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id AS encounter_id, e.creator AS encounter_provider, ',
'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
'MAX(IF(o.concept_id = 1246, o.value_coded, NULL)) AS visit_scheduled, ',
'MAX(IF(o.concept_id = 161643, o.value_coded, NULL)) AS person_present, ',
'MAX(IF(o.concept_id = 5089, o.value_numeric, NULL)) AS weight, ',
'MAX(IF(o.concept_id = 5085, o.value_numeric, NULL)) AS systolic_pressure, ',
'MAX(IF(o.concept_id = 5086, o.value_numeric, NULL)) AS diastolic_pressure, ',
'MAX(IF(o.concept_id = 5090, o.value_numeric, NULL)) AS height, ',
'MAX(IF(o.concept_id = 5088, o.value_numeric, NULL)) AS temperature, ',
'MAX(IF(o.concept_id = 5087, o.value_numeric, NULL)) AS pulse_rate, ',
'MAX(IF(o.concept_id = 5242, o.value_numeric, NULL)) AS respiratory_rate, ',
'MAX(IF(o.concept_id = 5092, o.value_numeric, NULL)) AS oxygen_saturation, ',
'MAX(IF(o.concept_id = 1343, o.value_numeric, NULL)) AS muac, ',
'MAX(IF(o.concept_id = 162584, o.value_numeric, NULL)) AS z_score_absolute, ',
'MAX(IF(o.concept_id = 163515, o.value_coded, NULL)) AS z_score, ',
'MAX(IF(o.concept_id = 163300, o.value_coded, NULL)) AS nutritional_status, ',
'MAX(IF(o.concept_id = 164930, o.value_coded, NULL)) AS population_type, ',
'MAX(IF(o.concept_id = 160581, o.value_coded, NULL)) AS key_population_type, ',
'MAX(IF(o.concept_id = 5356, o.value_coded, NULL)) AS who_stage, ',
'CONCAT_WS('','', ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5006, ''Asymptomatic'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 130364, ''Persistent generalized lymphadenopathy)'' , '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 159214, ''Unexplained severe weight loss'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5330, ''Minor mucocutaneous manifestations'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 117543, ''Herpes zoster'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5012, ''Recurrent upper respiratory tract infections'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5018, ''Unexplained chronic diarrhoea'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5027, ''Unexplained persistent fever'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5337, ''Oral hairy leukoplakia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 42, ''Pulmonary tuberculosis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5333, ''Severe bacterial infections such as empyema or pyomyositis or meningitis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 133440, ''Acute necrotizing ulcerative stomatitis or gingivitis or periodontitis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 148849, ''Unexplained anaemia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 823, ''HIV wasting syndrome'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 137375, ''Pneumocystis jirovecipneumonia PCP'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 1215, ''Recurrent severe bacterial pneumonia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 1294, ''Cryptococcal meningitis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 990, ''Toxoplasmosis of the brain'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 143929, ''Chronic orolabial, genital or ano-rectal herpes simplex'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 110915, ''Kaposi sarcoma KS'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 160442, ''HIV encephalopathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5042, ''Extra pulmonary tuberculosis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 143110, ''Cryptosporidiosis with diarrhoea'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 136458, ''Isosporiasis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5033, ''Cryptococcosis extra pulmonary'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 160745, ''Disseminated non-tuberculous mycobacterial infection'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 154119, ''Cytomegalovirus CMV retinitis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5046, ''Progressive multifocal leucoencephalopathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 131357, ''Any disseminated mycosis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 146513, ''Candidiasis of the oesophagus or airways'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 160851, ''Non-typhoid salmonella NTS septicaemia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 155941, ''Lymphoma cerebral or B cell Non-Hodgkins Lymphoma'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 116023, ''Invasive cervical cancer'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 123084, ''Visceral leishmaniasis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 153701, ''Symptomatic HIV-associated nephropathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 825, ''Unexplained asymptomatic hepatosplenomegaly'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 1249, ''Papular pruritic eruptions'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 113116, ''Seborrheic dermatitis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 132387, ''Fungal nail infections'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 148762, ''Angular cheilitis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 159344, ''Linear gingival erythema'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 1212, ''Extensive HPV or molluscum infection'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 159912, ''Recurrent oral ulcerations'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 1210, ''Parotid enlargement'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 127784, ''Recurrent or chronic upper respiratory infection'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 134722, ''Unexplained moderate malnutrition'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 163282, ''Unexplained persistent fever'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5334, ''Oral candidiasis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 160515, ''Severe recurrent bacterial pneumonia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 135458, ''Lymphoid interstitial pneumonitis (LIP)'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 163712, ''HIV-related cardiomyopathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 162331, ''Unexplained severe wasting or severe malnutrition'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 130021, ''Pneumocystis pneumonia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 146518, ''Candida of trachea, bronchi or lungs'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 143744, ''Acquired recto-vesicular fistula'', '''')), '''') ',
') AS who_stage_associated_oi, ',
'MAX(IF(o.concept_id = 1154, o.value_coded, NULL)) AS presenting_complaints, ',
'NULL AS clinical_notes, ',
'MAX(IF(o.concept_id = 164948, o.value_coded, NULL)) AS on_anti_tb_drugs, ',
'MAX(IF(o.concept_id = 164949, o.value_coded, NULL)) AS on_ipt, ',
'MAX(IF(o.concept_id = 164950, o.value_coded, NULL)) AS ever_on_ipt, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded = 159799, o.value_coded, NULL)) AS cough, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded = 1494, o.value_coded, NULL)) AS fever, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded = 832, o.value_coded, NULL)) AS weight_loss_poor_gain, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded = 133027, o.value_coded, NULL)) AS night_sweats, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded = 124068, o.value_coded, NULL)) AS tb_case_contact, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded = 116334, o.value_coded, NULL)) AS lethargy, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded IN (159799,1494,832,133027,124068,116334,1066), ''Yes'', ''No'')) AS screened_for_tb, ',
'MAX(IF(o.concept_id = 1271 AND o.value_coded = 307, o.value_coded, NULL)) AS spatum_smear_ordered, ',
'MAX(IF(o.concept_id = 1271 AND o.value_coded = 12, o.value_coded, NULL)) AS chest_xray_ordered, ',
'MAX(IF(o.concept_id = 1271 AND o.value_coded = 162202, o.value_coded, NULL)) AS genexpert_ordered, ',
'MAX(IF(o.concept_id = 307, o.value_coded, NULL)) AS spatum_smear_result, ',
'MAX(IF(o.concept_id = 12, o.value_coded, NULL)) AS chest_xray_result, ',
'MAX(IF(o.concept_id = 162202, o.value_coded, NULL)) AS genexpert_result, ',
'MAX(IF(o.concept_id = 1272, o.value_coded, NULL)) AS referral, ',
'MAX(IF(o.concept_id = 163752, o.value_coded, NULL)) AS clinical_tb_diagnosis, ',
'MAX(IF(o.concept_id = 163414, o.value_coded, NULL)) AS contact_invitation, ',
'MAX(IF(o.concept_id = 162275, o.value_coded, NULL)) AS evaluated_for_ipt, ',
'MAX(IF(o.concept_id = 160557, o.value_coded, NULL)) AS has_known_allergies, ',
'MAX(IF(o.concept_id = 162747, o.value_coded, NULL)) AS has_chronic_illnesses_cormobidities, ',
'MAX(IF(o.concept_id = 121764, o.value_coded, NULL)) AS has_adverse_drug_reaction, ',
'MAX(IF(o.concept_id = 5272, o.value_coded, NULL)) AS pregnancy_status, ',
'MAX(IF(o.concept_id = 5632, o.value_coded, NULL)) AS breastfeeding, ',
'MAX(IF(o.concept_id = 164933, o.value_coded, NULL)) AS wants_pregnancy, ',
'MAX(IF(o.concept_id = 161033, o.value_coded, NULL)) AS pregnancy_outcome, ',
'MAX(IF(o.concept_id = 163530, o.value_text, NULL)) AS anc_number, ',
'MAX(IF(o.concept_id = 5596, DATE(o.value_datetime), NULL)) AS expected_delivery_date, ',
'MAX(IF(o.concept_id = 162877, o.value_coded, NULL)) AS ever_had_menses, ',
'MAX(IF(o.concept_id = 1427, DATE(o.value_datetime), NULL)) AS last_menstrual_period, ',
'MAX(IF(o.concept_id = 160596, o.value_coded, NULL)) AS menopausal, ',
'MAX(IF(o.concept_id = 5624, o.value_numeric, NULL)) AS gravida, ',
'MAX(IF(o.concept_id = 1053, o.value_numeric, NULL)) AS parity, ',
'MAX(IF(o.concept_id = 160080, o.value_numeric, NULL)) AS full_term_pregnancies, ',
'MAX(IF(o.concept_id = 1823, o.value_numeric, NULL)) AS abortion_miscarriages, ',
'MAX(IF(o.concept_id = 160653, o.value_coded, NULL)) AS family_planning_status, ',
'MAX(IF(o.concept_id = 374, o.value_coded, NULL)) AS family_planning_method, ',
'MAX(IF(o.concept_id = 160575, o.value_coded, NULL)) AS reason_not_using_family_planning, ',
'MAX(IF(o.concept_id = 1659, o.value_coded, NULL)) AS tb_status, ',
'MAX(IF(o.concept_id = 162309, o.value_coded, NULL)) AS started_anti_TB, ',
'MAX(IF(o.concept_id = 1113, o.value_datetime, NULL)) AS tb_rx_date, ',
'MAX(IF(o.concept_id = 161654, TRIM(o.value_text), NULL)) AS tb_treatment_no, ',
'CONCAT_WS('','', ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 1107, ''None'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 136443, ''Jaundice'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 460, ''Oedema'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 5334, ''Oral Thrush'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 5245, ''Pallor'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 140125, ''Finger Clubbing'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 126952, ''Lymph Node Axillary'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 143050, ''Cyanosis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 126939, ''Lymph Nodes Inguinal'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 823, ''Wasting'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 142630, ''Dehydration'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 116334, ''Lethargic'', '''')), '''') ',
') AS general_examination, ',
'MAX(IF(o.concept_id = 159615, o.value_coded, NULL)) AS system_examination, ',
'MAX(IF(o.concept_id = 1120, o.value_coded, NULL)) AS skin_findings, ',
'MAX(IF(o.concept_id = 163309, o.value_coded, NULL)) AS eyes_findings, ',
'MAX(IF(o.concept_id = 164936, o.value_coded, NULL)) AS ent_findings, ',
'MAX(IF(o.concept_id = 1123, o.value_coded, NULL)) AS chest_findings, ',
'MAX(IF(o.concept_id = 1124, o.value_coded, NULL)) AS cvs_findings, ',
'MAX(IF(o.concept_id = 1125, o.value_coded, NULL)) AS abdomen_findings, ',
'MAX(IF(o.concept_id = 164937, o.value_coded, NULL)) AS cns_findings, ',
'MAX(IF(o.concept_id = 1126, o.value_coded, NULL)) AS genitourinary_findings, ',
'MAX(IF(o.concept_id = 1109, o.value_coded, NULL)) AS prophylaxis_given, ',
'MAX(IF(o.concept_id = 161652, o.value_coded, NULL)) AS ctx_adherence, ',
'MAX(IF(o.concept_id = 162229 OR (o.concept_id = 1282 AND o.value_coded = 105281), o.value_coded, NULL)) AS ctx_dispensed, ',
'MAX(IF(o.concept_id = 164941, o.value_coded, NULL)) AS dapsone_adherence, ',
'MAX(IF(o.concept_id = 164940 OR (o.concept_id = 1282 AND o.value_coded = 74250), o.value_coded, NULL)) AS dapsone_dispensed, ',
'MAX(IF(o.concept_id = 162230, o.value_coded, NULL)) AS inh_dispensed, ',
'MAX(IF(o.concept_id = 1658, o.value_coded, NULL)) AS arv_adherence, ',
'MAX(IF(o.concept_id = 160582, o.value_coded, NULL)) AS poor_arv_adherence_reason, ',
'MAX(IF(o.concept_id = 160632, TRIM(o.value_text), NULL)) AS poor_arv_adherence_reason_other, ',
'MAX(IF(o.concept_id = 159423, o.value_coded, NULL)) AS pwp_disclosure, ',
'MAX(IF(o.concept_id = 5616, o.value_coded, NULL)) AS pwp_pead_disclosure, ',
'MAX(IF(o.concept_id = 161557, o.value_coded, NULL)) AS pwp_partner_tested, ',
'MAX(IF(o.concept_id = 159777, o.value_coded, NULL)) AS condom_provided, ',
'MAX(IF(o.concept_id = 112603, o.value_coded, NULL)) AS substance_abuse_screening, ',
'MAX(IF(o.concept_id = 161558, o.value_coded, NULL)) AS screened_for_sti, ',
'MAX(IF(o.concept_id = 164934, o.value_coded, NULL)) AS cacx_screening, ',
'MAX(IF(o.concept_id = 164935, o.value_coded, NULL)) AS sti_partner_notification, ',
'MAX(IF(o.concept_id = 167161, o.value_coded, NULL)) AS experienced_gbv, ',
'MAX(IF(o.concept_id = 165086, o.value_coded, NULL)) AS depression_screening, ',
'MAX(IF(o.concept_id = 160581, o.value_coded, NULL)) AS at_risk_population, ',
'MAX(IF(o.concept_id = 159615, o.value_coded, NULL)) AS system_review_finding, ',
'NULL AS next_appointment_date, NULL AS refill_date, ',
'MAX(IF(o.concept_id = 166607, o.value_coded, NULL)) AS appointment_consent, ',
'MAX(IF(o.concept_id = 160288, o.value_coded, NULL)) AS next_appointment_reason, ',
'MAX(IF(o.concept_id = 1855, o.value_coded, NULL)) AS stability, ',
'MAX(IF(o.concept_id = 164947, o.value_coded, NULL)) AS differentiated_care_group, ',
'MAX(IF(o.concept_id IN (164946,165287), o.value_coded, NULL)) AS differentiated_care, ',
'MAX(IF(o.concept_id = 164946 OR o.concept_id = 165287, o.value_coded, NULL)) AS established_differentiated_care, ',
'MAX(IF(o.concept_id = 159356, o.value_coded, NULL)) AS insurance_type, ',
'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS other_insurance_specify, ',
'MAX(IF(o.concept_id = 165911, o.value_coded, NULL)) AS insurance_status, ',
'e.voided AS voided ',
'FROM encounter e ',
'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''a0034eee-1940-4e35-847f-97537a35d05e'',''465a92f2-baf8-42e9-9612-53064be868e8'')) et ON et.encounter_type_id = e.encounter_type ',
'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
'AND o.concept_id IN (1282,1246,161643,5089,5085,5086,5090,5088,5087,5242,5092,1343,162584,163515,5356,167394,5272,5632,161033,163530,5596,1427,5624,1053,160653,374,160575,1659,161654,161652,162229,162230,1658,160582,160632,159423,5616,161557,159777,112603,161558,160581,5096,163300,164930,160581,1154,160430,164948,164949,164950,1271,307,12,162202,1272,163752,163414,162275,160557,162747,121764,164933,160080,1823,164940,164934,164935,159615,160288,1855,164947,162549,162877,160596,1109,162309,1113,1729,162737,159615,1120,163309,164936,1123,1124,1125,164937,1126,166607,159356,161011,165911,167161,165086,164946,165287) ',
'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
'GROUP BY e.patient_id, visit_date ',
'ON DUPLICATE KEY UPDATE ',
'visit_date=VALUES(visit_date),encounter_provider=VALUES(encounter_provider),visit_scheduled=VALUES(visit_scheduled), ',
'person_present=VALUES(person_present),weight=VALUES(weight),systolic_pressure=VALUES(systolic_pressure),diastolic_pressure=VALUES(diastolic_pressure),height=VALUES(height),temperature=VALUES(temperature),pulse_rate=VALUES(pulse_rate),respiratory_rate=VALUES(respiratory_rate), ',
'oxygen_saturation=VALUES(oxygen_saturation),muac=VALUES(muac),z_score_absolute=VALUES(z_score_absolute),z_score=VALUES(z_score),nutritional_status=VALUES(nutritional_status),population_type=VALUES(population_type),key_population_type=VALUES(key_population_type),who_stage=VALUES(who_stage),who_stage_associated_oi=VALUES(who_stage_associated_oi),presenting_complaints=VALUES(presenting_complaints), ',
'clinical_notes=VALUES(clinical_notes),on_anti_tb_drugs=VALUES(on_anti_tb_drugs),on_ipt=VALUES(on_ipt),ever_on_ipt=VALUES(ever_on_ipt),cough=VALUES(cough),fever=VALUES(fever),weight_loss_poor_gain=VALUES(weight_loss_poor_gain),night_sweats=VALUES(night_sweats),tb_case_contact=VALUES(tb_case_contact),lethargy=VALUES(lethargy),screened_for_tb=VALUES(screened_for_tb), ',
'spatum_smear_ordered=VALUES(spatum_smear_ordered),chest_xray_ordered=VALUES(chest_xray_ordered),genexpert_ordered=VALUES(genexpert_ordered),spatum_smear_result=VALUES(spatum_smear_result),chest_xray_result=VALUES(chest_xray_result),genexpert_result=VALUES(genexpert_result),referral=VALUES(referral),clinical_tb_diagnosis=VALUES(clinical_tb_diagnosis),contact_invitation=VALUES(contact_invitation),evaluated_for_ipt=VALUES(evaluated_for_ipt), ',
'has_known_allergies=VALUES(has_known_allergies),has_chronic_illnesses_cormobidities=VALUES(has_chronic_illnesses_cormobidities),has_adverse_drug_reaction=VALUES(has_adverse_drug_reaction),pregnancy_status=VALUES(pregnancy_status),breastfeeding=VALUES(breastfeeding),wants_pregnancy=VALUES(wants_pregnancy),pregnancy_outcome=VALUES(pregnancy_outcome),anc_number=VALUES(anc_number),expected_delivery_date=VALUES(expected_delivery_date), ',
'last_menstrual_period=VALUES(last_menstrual_period),gravida=VALUES(gravida),parity=VALUES(parity),full_term_pregnancies=VALUES(full_term_pregnancies),abortion_miscarriages=VALUES(abortion_miscarriages),family_planning_status=VALUES(family_planning_status),family_planning_method=VALUES(family_planning_method),reason_not_using_family_planning=VALUES(reason_not_using_family_planning),tb_status=VALUES(tb_status),started_anti_TB=VALUES(started_anti_TB),tb_rx_date=VALUES(tb_rx_date),tb_treatment_no=VALUES(tb_treatment_no), ',
'general_examination=VALUES(general_examination),system_examination=VALUES(system_examination),skin_findings=VALUES(skin_findings),eyes_findings=VALUES(eyes_findings),ent_findings=VALUES(ent_findings),chest_findings=VALUES(chest_findings),cvs_findings=VALUES(cvs_findings),abdomen_findings=VALUES(abdomen_findings),cns_findings=VALUES(cns_findings),genitourinary_findings=VALUES(genitourinary_findings), ',
'ctx_adherence=VALUES(ctx_adherence),ctx_dispensed=VALUES(ctx_dispensed),dapsone_adherence=VALUES(dapsone_adherence),dapsone_dispensed=VALUES(dapsone_dispensed),inh_dispensed=VALUES(inh_dispensed),arv_adherence=VALUES(arv_adherence),poor_arv_adherence_reason=VALUES(poor_arv_adherence_reason),poor_arv_adherence_reason_other=VALUES(poor_arv_adherence_reason_other),pwp_disclosure=VALUES(pwp_disclosure),pwp_pead_disclosure=VALUES(pwp_pead_disclosure), ',
'pwp_partner_tested=VALUES(pwp_partner_tested),condom_provided=VALUES(condom_provided),substance_abuse_screening=VALUES(substance_abuse_screening),screened_for_sti=VALUES(screened_for_sti),cacx_screening=VALUES(cacx_screening),sti_partner_notification=VALUES(sti_partner_notification),experienced_gbv=VALUES(experienced_gbv),depression_screening=VALUES(depression_screening),at_risk_population=VALUES(at_risk_population),system_review_finding=VALUES(system_review_finding),next_appointment_date=VALUES(next_appointment_date),refill_date=VALUES(refill_date),appointment_consent=VALUES(appointment_consent), ',
'next_appointment_reason=VALUES(next_appointment_reason),stability=VALUES(stability),differentiated_care_group=VALUES(differentiated_care_group),differentiated_care=VALUES(differentiated_care),established_differentiated_care=VALUES(established_differentiated_care),insurance_type=VALUES(insurance_type),other_insurance_specify=VALUES(other_insurance_specify),insurance_status=VALUES(insurance_status),voided=VALUES(voided);'
);

PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END $$

-- ------------------------------------- laboratory updates ---------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_laboratory_extract $$
CREATE PROCEDURE sp_update_etl_laboratory_extract(IN last_update_time DATETIME)
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table VARCHAR(300);

    CALL sp_set_tenant_session_vars();
    SET target_table = CONCAT('`', @etl_schema, '`.`etl_laboratory_extract`');

   SET @sql = CONCAT(
  'INSERT INTO ', target_table, ' (',
    'uuid, encounter_id, patient_id, location_id, visit_date, visit_id, order_id, lab_test, urgency, order_reason, ',
    'order_test_name, obs_id, result_test_name, result_name, set_member_conceptId, test_result, ',
    'date_test_requested, date_test_result_received, date_created, date_last_modified, created_by',
  ') ',
  'WITH FilteredOrders AS (',
    'SELECT patient_id, encounter_id, order_id, concept_id, date_activated, urgency, order_reason ',
    'FROM orders ',
    'WHERE order_type_id = 3 AND order_action IN (''NEW'',''REVISE'') AND voided = 0 ',
    'GROUP BY patient_id, encounter_id, order_id, concept_id, date_activated, urgency, order_reason',
  '), ',
  'LabOrderConcepts AS (',
    'SELECT cs.concept_set_id AS set_id, cs.concept_id AS member_concept_id, c.datatype_id AS member_datatype, c.class_id AS member_class, n.name ',
    'FROM concept_set cs ',
    'INNER JOIN concept c ON cs.concept_id = c.concept_id ',
    'INNER JOIN concept_name n ON c.concept_id = n.concept_id AND n.locale = ''en'' AND n.concept_name_type = ''FULLY_SPECIFIED'' ',
    'WHERE cs.concept_set = 1000628',
  '), ',
  'CodedLabOrderResults AS (',
    'SELECT o.obs_id AS obs_id, o.order_id, o.concept_id, o.obs_datetime, o.date_created, o.value_coded, n.name, n1.name AS test_name ',
    'FROM obs o ',
    'INNER JOIN concept c ON o.concept_id = c.concept_id ',
    'INNER JOIN concept_datatype cd ON c.datatype_id = cd.concept_datatype_id AND cd.name = ''Coded'' ',
    'LEFT JOIN concept_name n ON o.value_coded = n.concept_id AND n.locale = ''en'' AND n.concept_name_type = ''FULLY_SPECIFIED'' ',
    'LEFT JOIN concept_name n1 ON o.concept_id = n1.concept_id AND n1.locale = ''en'' AND n1.concept_name_type = ''FULLY_SPECIFIED'' ',
    'WHERE o.order_id IS NOT NULL',
  '), ',
  'NumericLabOrderResults AS (',
    'SELECT o.obs_id AS obs_id, o.order_id, o.concept_id, o.obs_datetime, o.date_created, o.value_numeric, n.name, n1.name AS test_name ',
    'FROM obs o ',
    'INNER JOIN concept c ON o.concept_id = c.concept_id ',
    'INNER JOIN concept_datatype cd ON c.datatype_id = cd.concept_datatype_id AND cd.name = ''Numeric'' ',
    'INNER JOIN concept_name n ON o.concept_id = n.concept_id AND n.locale = ''en'' AND n.concept_name_type = ''FULLY_SPECIFIED'' ',
    'LEFT JOIN concept_name n1 ON o.concept_id = n1.concept_id AND n1.locale = ''en'' AND n1.concept_name_type = ''FULLY_SPECIFIED'' ',
    'WHERE o.order_id IS NOT NULL',
  '), ',
  'TextLabOrderResults AS (',
    'SELECT o.obs_id AS obs_id, o.order_id, o.concept_id, o.obs_datetime, o.date_created, o.value_text, c.class_id, n.name, n1.name AS test_name ',
    'FROM obs o ',
    'INNER JOIN concept c ON o.concept_id = c.concept_id ',
    'INNER JOIN concept_datatype cd ON c.datatype_id = cd.concept_datatype_id AND cd.name = ''Text'' ',
    'INNER JOIN concept_name n ON o.concept_id = n.concept_id AND n.locale = ''en'' AND n.concept_name_type = ''FULLY_SPECIFIED'' ',
    'LEFT JOIN concept_name n1 ON o.concept_id = n1.concept_id AND n1.locale = ''en'' AND n1.concept_name_type = ''FULLY_SPECIFIED'' ',
    'WHERE o.order_id IS NOT NULL',
  ') ',
  'SELECT ',
    'UUID(), e.encounter_id, e.patient_id, e.location_id, COALESCE(o.date_activated, cr.obs_datetime, nr.obs_datetime, tr.obs_datetime) AS visit_date, ',
    'e.visit_id, o.order_id, o.concept_id AS lab_test, o.urgency, o.order_reason, lc.name AS order_test_name, ',
    'COALESCE(cr.obs_id, nr.obs_id, tr.obs_id) AS obs_id, ',
    'IF(cr.test_name IS NOT NULL, cr.test_name, IF(nr.test_name IS NOT NULL, nr.test_name, IF(tr.test_name IS NOT NULL, tr.test_name, ''''))) AS result_test_name, ',
    'COALESCE(cr.name, nr.value_numeric, tr.value_text) AS result_name, ',
    'IF(cr.concept_id IS NOT NULL, cr.concept_id, IF(nr.concept_id IS NOT NULL, nr.concept_id, IF(tr.concept_id IS NOT NULL, tr.concept_id, ''''))) AS set_member_conceptId, ',
    'COALESCE(cr.value_coded, nr.value_numeric, tr.value_text) AS test_result, ',
    'o.date_activated AS date_test_requested, e.encounter_datetime AS date_test_result_received, e.date_created, e.date_changed AS date_last_modified, e.creator ',
  'FROM encounter e ',
  'INNER JOIN FilteredOrders o ON o.encounter_id = e.encounter_id ',
  'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
  'LEFT JOIN LabOrderConcepts lc ON o.concept_id = lc.member_concept_id ',
  'LEFT JOIN CodedLabOrderResults cr ON o.order_id = cr.order_id ',
  'LEFT JOIN NumericLabOrderResults nr ON o.order_id = nr.order_id ',
  'LEFT JOIN TextLabOrderResults tr ON o.order_id = tr.order_id ',
  'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR cr.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR nr.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR tr.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ') ',
  'GROUP BY o.order_id ',
  'ON DUPLICATE KEY UPDATE visit_date = VALUES(visit_date), lab_test = VALUES(lab_test), set_member_conceptId = VALUES(set_member_conceptId), test_result = VALUES(test_result);'
);

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$

-- ------------- update etl_pharmacy_extract table--------------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_pharmacy_extract $$
CREATE PROCEDURE sp_update_etl_pharmacy_extract(IN last_update_time DATETIME)
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table VARCHAR(300);
    CALL sp_set_tenant_session_vars();
    SET target_table = CONCAT('`', @etl_schema, '`.`etl_pharmacy_extract`');
    SET @sql = CONCAT(
      'INSERT INTO ', target_table, ' (',
        'obs_group_id, patient_id, uuid, visit_date, visit_id, encounter_id, date_created, date_last_modified, encounter_name, location_id, ',
        'drug, drug_name, is_arv, is_ctx, is_dapsone, frequency, duration, duration_units, voided, date_voided, dispensing_provider',
      ') ',
      'SELECT ',
        'o.obs_group_id AS obs_group_id, ',
        'o.person_id AS patient_id, ',
        'MAX(IF(o.concept_id = 1282, o.uuid, NULL)) AS uuid, ',
        'DATE(o.obs_datetime) AS visit_date, ',
        'e.visit_id, ',
        'o.encounter_id, ',
        'e.date_created, ',
        'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
        'et.name AS encounter_name, ',
        'e.location_id, ',
        'MAX(IF(o.concept_id = 1282 AND o.value_coded IS NOT NULL, o.value_coded, NULL)) AS drug, ',
        'MAX(IF(o.concept_id = 1282, LEFT(cn.name,255), NULL)) AS drug_name, ',
        'MAX(IF(o.concept_id = 1282 AND cs.concept_set = 1085, 1, 0)) AS is_arv, ',
        'MAX(IF(o.concept_id = 1282 AND o.value_coded = 105281, 1, 0)) AS is_ctx, ',
        'MAX(IF(o.concept_id = 1282 AND o.value_coded = 74250, 1, 0)) AS is_dapsone, ',
        'MAX(IF(o.concept_id = 1443, o.value_numeric, NULL)) AS frequency, ',
        'MAX(IF(o.concept_id = 159368, IF(o.value_numeric > 10000, 10000, o.value_numeric), NULL)) AS duration, ',
        'MAX(IF(o.concept_id = 1732 AND o.value_coded = 1072, ''Days'', IF(o.concept_id = 1732 AND o.value_coded = 1073, ''Weeks'', IF(o.concept_id = 1732 AND o.value_coded = 1074, ''Months'', NULL)))) AS duration_units, ',
        'o.voided, o.date_voided, e.creator AS dispensing_provider ',
      'FROM obs o ',
        'INNER JOIN person p ON p.person_id = o.person_id AND p.voided = 0 ',
        'LEFT JOIN encounter e ON e.encounter_id = o.encounter_id ',
        'LEFT JOIN encounter_type et ON et.encounter_type_id = e.encounter_type ',
        'LEFT JOIN concept_name cn ON o.value_coded = cn.concept_id AND cn.locale = ''en'' AND cn.concept_name_type = ''FULLY_SPECIFIED'' ',
        'LEFT JOIN concept_set cs ON o.value_coded = cs.concept_id ',
      'WHERE o.voided = 0 AND o.concept_id IN (1282,1732,159368,1443,1444) ',
        'AND (e.voided = 0 OR e.voided IS NULL) ',
        'AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
          ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
          ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
          ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
          ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ') ',
      'GROUP BY o.obs_group_id, o.person_id, o.encounter_id ',
      'HAVING drug IS NOT NULL AND obs_group_id IS NOT NULL ',
      'ON DUPLICATE KEY UPDATE ',
        'visit_date = VALUES(visit_date), ',
        'encounter_name = VALUES(encounter_name), ',
        'is_arv = VALUES(is_arv), ',
        'is_ctx = VALUES(is_ctx), ',
        'is_dapsone = VALUES(is_dapsone), ',
        'frequency = VALUES(frequency), ',
        'duration = VALUES(duration), ',
        'duration_units = VALUES(duration_units), ',
        'voided = VALUES(voided), ',
        'date_voided = VALUES(date_voided);'
    );

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Compute duration_in_days in tenant table
    SET @sql = CONCAT(
      'UPDATE ', target_table, ' SET duration_in_days = ',
        'CASE ',
          'WHEN duration_units = ''Days'' THEN duration ',
          'WHEN duration_units = ''Weeks'' THEN duration * 7 ',
          'WHEN duration_units = ''Months'' THEN duration * 31 ',
          'ELSE NULL ',
        'END ',
      'WHERE duration IS NOT NULL AND duration_units IS NOT NULL;'
    );

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END $$

-- ------------ create table etl_patient_treatment_event----------------------------------
-- sql
DROP PROCEDURE IF EXISTS sp_update_etl_program_discontinuation $$
CREATE PROCEDURE sp_update_etl_program_discontinuation(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_patient_program_discontinuation`');

  SELECT "Processing program discontinuations", CONCAT("Time: ", NOW());

  SET sql_stmt = CONCAT(
    'INSERT INTO ', target_table, ' (',
      'patient_id, uuid, visit_id, visit_date, program_uuid, program_name, encounter_id, ',
      'discontinuation_reason, effective_discontinuation_date, trf_out_verified, trf_out_verification_date, ',
      'date_died, transfer_facility, transfer_date, death_reason, specific_death_cause, natural_causes, non_natural_cause, ',
      'date_created, date_last_modified',
    ') ',
    'SELECT ',
      'q.patient_id, q.uuid, q.visit_id, q.encounter_datetime, q.program_uuid, q.program_name, q.encounter_id, ',
      'q.reason_discontinued, q.effective_discontinuation_date, q.trf_out_verified, q.trf_out_verification_date, ',
      'q.date_died, COALESCE(l.`name`, q.to_facility_raw) AS transfer_facility, q.to_date, q.death_reason, q.specific_death_cause, ',
      'q.natural_causes, q.non_natural_cause, q.date_created, q.date_last_modified ',
    'FROM (',
      'SELECT ',
        'e.patient_id, e.uuid, e.visit_id, e.encounter_datetime, et.uuid AS program_uuid, ',
        '(CASE et.uuid ',
          'WHEN ''2bdada65-4c72-4a48-8730-859890e25cee'' THEN ''HIV'' ',
          'WHEN ''d3e3d723-7458-4b4e-8998-408e8a551a84'' THEN ''TB'' ',
          'WHEN ''01894f88-dc73-42d4-97a3-0929118403fb'' THEN ''MCH Child HEI'' ',
          'WHEN ''5feee3f1-aa16-4513-8bd0-5d9b27ef1208'' THEN ''MCH Child'' ',
          'WHEN ''7c426cfc-3b47-4481-b55f-89860c21c7de'' THEN ''MCH Mother'' ',
          'WHEN ''162382b8-0464-11ea-9a9f-362b9e155667'' THEN ''OTZ'' ',
          'WHEN ''5cf00d9e-09da-11ea-8d71-362b9e155667'' THEN ''OVC'' ',
          'WHEN ''d7142400-2495-11e9-ab14-d663bd873d93'' THEN ''KP'' ',
        'END) AS program_name, ',
        'e.encounter_id, ',
        'COALESCE(MAX(IF(o.concept_id = 161555, o.value_coded, NULL)), MAX(IF(o.concept_id = 159786, o.value_coded, NULL))) AS reason_discontinued, ',
        'COALESCE(MAX(IF(o.concept_id = 164384, o.value_datetime, NULL)), MAX(IF(o.concept_id = 159787, o.value_datetime, NULL))) AS effective_discontinuation_date, ',
        'MAX(IF(o.concept_id = 1285, o.value_coded, NULL)) AS trf_out_verified, ',
        'MAX(IF(o.concept_id = 164133, o.value_datetime, NULL)) AS trf_out_verification_date, ',
        'MAX(IF(o.concept_id = 1543, o.value_datetime, NULL)) AS date_died, ',
        'MAX(IF(o.concept_id = 159495, LEFT(TRIM(o.value_text),100), NULL)) AS to_facility_raw, ',
        'MAX(IF(o.concept_id = 160649, o.value_datetime, NULL)) AS to_date, ',
        'MAX(IF(o.concept_id = 1599, o.value_coded, NULL)) AS death_reason, ',
        'MAX(IF(o.concept_id = 1748, o.value_coded, NULL)) AS specific_death_cause, ',
        'MAX(IF(o.concept_id = 162580, LEFT(TRIM(o.value_text),200), NULL)) AS natural_causes, ',
        'MAX(IF(o.concept_id = 160218, LEFT(TRIM(o.value_text),200), NULL)) AS non_natural_cause, ',
        'e.date_created AS date_created, ',
        'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
      'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (161555,159786,159787,164384,1543,159495,160649,165380,1285,164133,1599,1748,162580,160218) ',
      'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (',
        '''2bdada65-4c72-4a48-8730-859890e25cee'',',
        '''d3e3d723-7458-4b4e-8998-408e8a551a84'',',
        '''5feee3f1-aa16-4513-8bd0-5d9b27ef1208'',',
        '''7c426cfc-3b47-4481-b55f-89860c21c7de'',',
        '''01894f88-dc73-42d4-97a3-0929118403fb'',',
        '''162382b8-0464-11ea-9a9f-362b9e155667'',',
        '''5cf00d9e-09da-11ea-8d71-362b9e155667'',',
        '''d7142400-2495-11e9-ab14-d663bd873d93''',
      ')) et ON et.encounter_type_id = e.encounter_type ',
      'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ') ',
      'GROUP BY e.encounter_id',
    ') q ',
    'LEFT JOIN location l ON l.uuid = q.to_facility_raw ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), ',
      'discontinuation_reason = VALUES(discontinuation_reason), ',
      'date_died = VALUES(date_died), ',
      'transfer_facility = VALUES(transfer_facility), ',
      'transfer_date = VALUES(transfer_date), ',
      'trf_out_verified = VALUES(trf_out_verified), ',
      'trf_out_verification_date = VALUES(trf_out_verification_date), ',
      'death_reason = VALUES(death_reason), ',
      'specific_death_cause = VALUES(specific_death_cause);'
  );

  PREPARE stmt FROM sql_stmt;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing discontinuation data", CONCAT("Time: ", NOW());
END $$

-- ------------- update etl_mch_enrollment-----------------------

DROP PROCEDURE IF EXISTS sp_update_etl_mch_enrollment $$
CREATE PROCEDURE sp_update_etl_mch_enrollment(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_mch_enrollment`');

  SELECT "Processing MCH Enrollments", CONCAT("Time: ", NOW());

  SET sql_stmt = CONCAT(
    'INSERT INTO ', target_table, ' (',
      'patient_id, uuid, visit_id, visit_date, location_id, encounter_id, service_type, anc_number, ',
      'first_anc_visit_date, gravida, parity, parity_abortion, age_at_menarche, lmp, lmp_estimated, edd_ultrasound, ',
      'blood_group, serology, tb_screening, bs_for_mps, hiv_status, hiv_test_date, partner_hiv_status, partner_hiv_test_date, ',
      'ti_date_started_art, ti_current_regimen, ti_care_facility, urine_microscopy, urinary_albumin, glucose_measurement, ',
      'urine_ph, urine_gravity, urine_nitrite_test, urine_leukocyte_esterace_test, urinary_ketone, urine_bile_salt_test, ',
      'urine_bile_pigment_test, urine_colour, urine_turbidity, urine_dipstick_for_blood, discontinuation_reason, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id=160478, o.value_coded, NULL)) AS service_type, ',
      'MAX(IF(o.concept_id=163530, o.value_text, NULL)) AS anc_number, ',
      'MAX(IF(o.concept_id=163547, o.value_datetime, NULL)) AS first_anc_visit_date, ',
      'MAX(IF(o.concept_id=5624, o.value_numeric, NULL)) AS gravida, ',
      'MAX(IF(o.concept_id=160080, o.value_numeric, NULL)) AS parity, ',
      'MAX(IF(o.concept_id=1823, o.value_numeric, NULL)) AS parity_abortion, ',
      'MAX(IF(o.concept_id=160598, o.value_numeric, NULL)) AS age_at_menarche, ',
      'MAX(IF(o.concept_id=1427, o.value_datetime, NULL)) AS lmp, ',
      'MAX(IF(o.concept_id=162095, o.value_datetime, NULL)) AS lmp_estimated, ',
      'MAX(IF(o.concept_id=5596, o.value_datetime, NULL)) AS edd_ultrasound, ',
      'MAX(IF(o.concept_id=300, o.value_coded, NULL)) AS blood_group, ',
      'MAX(IF(o.concept_id=299, o.value_coded, NULL)) AS serology, ',
      'MAX(IF(o.concept_id=160108, o.value_coded, NULL)) AS tb_screening, ',
      'MAX(IF(o.concept_id=32, o.value_coded, NULL)) AS bs_for_mps, ',
      'MAX(IF(o.concept_id=159427, o.value_coded, NULL)) AS hiv_status, ',
      'MAX(IF(o.concept_id=160554, o.value_datetime, NULL)) AS hiv_test_date, ',
      'MAX(IF(o.concept_id=1436, o.value_coded, NULL)) AS partner_hiv_status, ',
      'MAX(IF(o.concept_id=160082, o.value_datetime, NULL)) AS partner_hiv_test_date, ',
      'MAX(IF(o.concept_id=159599, o.value_datetime, NULL)) AS ti_date_started_art, ',
      'MAX(IF(o.concept_id=164855, o.value_coded, NULL)) AS ti_current_regimen, ',
      'MAX(IF(o.concept_id=162724, o.value_text, NULL)) AS ti_care_facility, ',
      'MAX(IF(o.concept_id=56, o.value_text, NULL)) AS urine_microscopy, ',
      'MAX(IF(o.concept_id=1875, o.value_coded, NULL)) AS urinary_albumin, ',
      'MAX(IF(o.concept_id=159734, o.value_coded, NULL)) AS glucose_measurement, ',
      'MAX(IF(o.concept_id=161438, o.value_numeric, NULL)) AS urine_ph, ',
      'MAX(IF(o.concept_id=161439, o.value_numeric, NULL)) AS urine_gravity, ',
      'MAX(IF(o.concept_id=161440, o.value_coded, NULL)) AS urine_nitrite_test, ',
      'MAX(IF(o.concept_id=161441, o.value_coded, NULL)) AS urine_leukocyte_esterace_test, ',
      'MAX(IF(o.concept_id=161442, o.value_coded, NULL)) AS urinary_ketone, ',
      'MAX(IF(o.concept_id=161444, o.value_coded, NULL)) AS urine_bile_salt_test, ',
      'MAX(IF(o.concept_id=161443, o.value_coded, NULL)) AS urine_bile_pigment_test, ',
      'MAX(IF(o.concept_id=162106, o.value_coded, NULL)) AS urine_colour, ',
      'MAX(IF(o.concept_id=162101, o.value_coded, NULL)) AS urine_turbidity, ',
      'MAX(IF(o.concept_id=162096, o.value_coded, NULL)) AS urine_dipstick_for_blood, ',
      'MAX(IF(o.concept_id=161555, o.value_coded, NULL)) AS discontinuation_reason, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (163530,163547,5624,160080,1823,160598,1427,162095,5596,300,299,160108,32,159427,160554,1436,160082,159599,164855,162724,56,1875,159734,161438,161439,161440,161441,161442,161444,161443,162106,162101,162096,161555,160478) ',
    'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''3ee036d8-7c13-4393-b5d6-036f2fe45126'')) et ON et.encounter_type_id = e.encounter_type ',
    'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date),service_type=VALUES(service_type),anc_number=VALUES(anc_number),first_anc_visit_date=VALUES(first_anc_visit_date),gravida=VALUES(gravida),parity=VALUES(parity),parity_abortion=VALUES(parity_abortion),age_at_menarche=VALUES(age_at_menarche),lmp=VALUES(lmp),lmp_estimated=VALUES(lmp_estimated),',
      'edd_ultrasound=VALUES(edd_ultrasound),blood_group=VALUES(blood_group),serology=VALUES(serology),tb_screening=VALUES(tb_screening),bs_for_mps=VALUES(bs_for_mps),hiv_status=VALUES(hiv_status),hiv_test_date=VALUES(hiv_test_date),partner_hiv_status=VALUES(partner_hiv_status),partner_hiv_test_date=VALUES(partner_hiv_test_date),',
      'ti_date_started_art=VALUES(ti_date_started_art),ti_current_regimen=VALUES(ti_current_regimen),ti_care_facility=VALUES(ti_care_facility),',
      'urine_microscopy=VALUES(urine_microscopy),urinary_albumin=VALUES(urinary_albumin),glucose_measurement=VALUES(glucose_measurement),urine_ph=VALUES(urine_ph),urine_gravity=VALUES(urine_gravity),urine_nitrite_test=VALUES(urine_nitrite_test),urine_leukocyte_esterace_test=VALUES(urine_leukocyte_esterace_test),urinary_ketone=VALUES(urinary_ketone),',
      'urine_bile_salt_test=VALUES(urine_bile_salt_test),urine_bile_pigment_test=VALUES(urine_bile_pigment_test),urine_colour=VALUES(urine_colour),urine_turbidity=VALUES(urine_turbidity),urine_dipstick_for_blood=VALUES(urine_dipstick_for_blood),discontinuation_reason=VALUES(discontinuation_reason);'
  );

  PREPARE stmt FROM sql_stmt;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing MCH Enrollments ", CONCAT("Time: ", NOW());
END $$

-- ------------- update etl_mch_antenatal_visit-------------------------

-- sql
DROP PROCEDURE IF EXISTS sp_update_etl_mch_antenatal_visit $$
CREATE PROCEDURE sp_update_etl_mch_antenatal_visit(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_mch_antenatal_visit`');

SET sql_stmt = CONCAT(
  'INSERT INTO ', target_table, ' (',
    'patient_id, uuid, visit_id, visit_date, location_id, encounter_id, provider, anc_visit_number, temperature, pulse_rate, systolic_bp, diastolic_bp, respiratory_rate, oxygen_saturation, weight,',
    'height, muac, hemoglobin, breast_exam_done, pallor, maturity, fundal_height, fetal_presentation, lie, fetal_heart_rate, fetal_movement, who_stage, cd4, vl_sample_taken, viral_load, ldl, arv_status,',
    'final_test_result, patient_given_result, partner_hiv_tested, partner_hiv_status, prophylaxis_given, haart_given, date_given_haart, baby_azt_dispensed, baby_nvp_dispensed, deworming_done_anc,',
    'IPT_dose_given_anc, TTT, IPT_malaria, iron_supplement, deworming, bed_nets, urine_microscopy, urinary_albumin, glucose_measurement, urine_ph, urine_gravity, urine_nitrite_test,',
    'urine_leukocyte_esterace_test, urinary_ketone, urine_bile_salt_test, urine_bile_pigment_test, urine_colour, urine_turbidity, urine_dipstick_for_blood, syphilis_test_status,',
    'syphilis_treated_status, bs_mps, diabetes_test, fgm_done, fgm_complications, fp_method_postpartum, anc_exercises, tb_screening, cacx_screening, cacx_screening_method, hepatitis_b_screening,',
    'hepatitis_b_treatment, has_other_illnes, counselled, counselled_on_birth_plans, counselled_on_danger_signs, counselled_on_family_planning, counselled_on_hiv, counselled_on_supplimental_feeding,',
    'counselled_on_breast_care, counselled_on_infant_feeding, counselled_on_treated_nets, intermittent_presumptive_treatment_given, intermittent_presumptive_treatment_dose, minimum_care_package,',
    'minimum_package_of_care_services, risk_reduction, partner_testing, sti_screening, condom_provision, prep_adherence, anc_visits_emphasis, pnc_fp_counseling, referral_vmmc, referral_dreams,',
    'referred_from, referred_to, clinical_notes, date_created, date_last_modified',
  ') ',
  'SELECT ',
    'e.patient_id, e.uuid, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.creator, ',
    'MAX(IF(o.concept_id=1425,o.value_numeric,NULL)) AS anc_visit_number, ',
    'MAX(IF(o.concept_id=5088,o.value_numeric,NULL)) AS temperature, ',
    'MAX(IF(o.concept_id=5087,o.value_numeric,NULL)) AS pulse_rate, ',
    'MAX(IF(o.concept_id=5085,o.value_numeric,NULL)) AS systolic_bp, ',
    'MAX(IF(o.concept_id=5086,o.value_numeric,NULL)) AS diastolic_bp, ',
    'MAX(IF(o.concept_id=5242,o.value_numeric,NULL)) AS respiratory_rate, ',
    'MAX(IF(o.concept_id=5092,o.value_numeric,NULL)) AS oxygen_saturation, ',
    'MAX(IF(o.concept_id=5089,o.value_numeric,NULL)) AS weight, ',
    'MAX(IF(o.concept_id=5090,o.value_numeric,NULL)) AS height, ',
    'MAX(IF(o.concept_id=1343,o.value_numeric,NULL)) AS muac, ',
    'MAX(IF(o.concept_id=21,o.value_numeric,NULL)) AS hemoglobin, ',
    'MAX(IF(o.concept_id=163590,o.value_coded,NULL)) AS breast_exam_done, ',
    'MAX(IF(o.concept_id=5245,o.value_coded,NULL)) AS pallor, ',
    'MAX(IF(o.concept_id=1438,o.value_numeric,NULL)) AS maturity, ',
    'MAX(IF(o.concept_id=1439,o.value_numeric,NULL)) AS fundal_height, ',
    'MAX(IF(o.concept_id=160090,o.value_coded,NULL)) AS fetal_presentation, ',
    'MAX(IF(o.concept_id=162089,o.value_coded,NULL)) AS lie, ',
    'MAX(IF(o.concept_id=1440,o.value_numeric,NULL)) AS fetal_heart_rate, ',
    'MAX(IF(o.concept_id=162107,o.value_coded,NULL)) AS fetal_movement, ',
    'MAX(IF(o.concept_id=5356,o.value_coded,NULL)) AS who_stage, ',
    'MAX(IF(o.concept_id=5497,o.value_numeric,NULL)) AS cd4, ',
    'MAX(IF(o.concept_id=1271,o.value_coded,NULL)) AS vl_sample_taken, ',
    'MAX(IF(o.concept_id=856,o.value_numeric,NULL)) AS viral_load, ',
    'MAX(IF(o.concept_id=1305,o.value_coded,NULL)) AS ldl, ',
    'MAX(IF(o.concept_id=1147,o.value_coded,NULL)) AS arv_status, ',
    'MAX(IF(o.concept_id=159427,(CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1138 THEN ''Inconclusive'' ELSE '''' END),NULL)) AS final_test_result, ',
    'MAX(IF(o.concept_id=164848,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS patient_given_result, ',
    'MAX(IF(o.concept_id=161557,o.value_coded,NULL)) AS partner_hiv_tested, ',
    'MAX(IF(o.concept_id=1436,o.value_coded,NULL)) AS partner_hiv_status, ',
    'MAX(IF(o.concept_id=1109,o.value_coded,NULL)) AS prophylaxis_given, ',
    'MAX(IF(o.concept_id=5576,o.value_coded,NULL)) AS haart_given, ',
    'MAX(IF(o.concept_id=163784,o.value_datetime,NULL)) AS date_given_haart, ',
    'MAX(IF(o.concept_id=1282 AND o.value_coded = 160123,o.value_coded,NULL)) AS baby_azt_dispensed, ',
    'MAX(IF(o.concept_id=1282 AND o.value_coded = 80586,o.value_coded,NULL)) AS baby_nvp_dispensed, ',
    'MAX(IF(o.concept_id=159922,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''N/A'' ELSE '''' END),NULL)) AS deworming_done_anc, ',
    'MAX(IF(o.concept_id=1418,o.value_numeric,NULL)) AS IPT_dose_given_anc, ',
    'MAX(IF(o.concept_id=984,(CASE o.value_coded WHEN 84879 THEN ''Yes'' ELSE '''' END),NULL)) AS TTT, ',
    'MAX(IF(o.concept_id=984,(CASE o.value_coded WHEN 159610 THEN ''Yes'' ELSE '''' END),NULL)) AS IPT_malaria, ',
    'MAX(IF(o.concept_id=984,(CASE o.value_coded WHEN 104677 THEN ''Yes'' ELSE '''' END),NULL)) AS iron_supplement, ',
    'MAX(IF(o.concept_id=984,(CASE o.value_coded WHEN 79413 THEN ''Yes'' ELSE '''' END),NULL)) AS deworming, ',
    'MAX(IF(o.concept_id=984,(CASE o.value_coded WHEN 160428 THEN ''Yes'' ELSE '''' END),NULL)) AS bed_nets, ',
    'MAX(IF(o.concept_id=56,o.value_text,NULL)) AS urine_microscopy, ',
    'MAX(IF(o.concept_id=1875,o.value_coded,NULL)) AS urinary_albumin, ',
    'MAX(IF(o.concept_id=159734,o.value_coded,NULL)) AS glucose_measurement, ',
    'MAX(IF(o.concept_id=161438,o.value_numeric,NULL)) AS urine_ph, ',
    'MAX(IF(o.concept_id=161439,o.value_numeric,NULL)) AS urine_gravity, ',
    'MAX(IF(o.concept_id=161440,o.value_coded,NULL)) AS urine_nitrite_test, ',
    'MAX(IF(o.concept_id=161441,o.value_coded,NULL)) AS urine_leukocyte_esterace_test, ',
    'MAX(IF(o.concept_id=161442,o.value_coded,NULL)) AS urinary_ketone, ',
    'MAX(IF(o.concept_id=161444,o.value_coded,NULL)) AS urine_bile_salt_test, ',
    'MAX(IF(o.concept_id=161443,o.value_coded,NULL)) AS urine_bile_pigment_test, ',
    'MAX(IF(o.concept_id=162106,o.value_coded,NULL)) AS urine_colour, ',
    'MAX(IF(o.concept_id=162101,o.value_coded,NULL)) AS urine_turbidity, ',
    'MAX(IF(o.concept_id=162096,o.value_coded,NULL)) AS urine_dipstick_for_blood, ',
    'MAX(IF(o.concept_id=299,o.value_coded,NULL)) AS syphilis_test_status, ',
    'MAX(IF(o.concept_id=159918,o.value_coded,NULL)) AS syphilis_treated_status, ',
    'MAX(IF(o.concept_id=32,o.value_coded,NULL)) AS bs_mps, ',
    'MAX(IF(o.concept_id=119481,o.value_coded,NULL)) AS diabetes_test, ',
    'MAX(IF(o.concept_id=165099,o.value_coded,NULL)) AS fgm_done, ',
    'MAX(IF(o.concept_id=120198,o.value_coded,NULL)) AS fgm_complications, ',
    'MAX(IF(o.concept_id=374,o.value_coded,NULL)) AS fp_method_postpartum, ',
    'MAX(IF(o.concept_id=161074,o.value_coded,NULL)) AS anc_exercises, ',
    'MAX(IF(o.concept_id=1659,o.value_coded,NULL)) AS tb_screening, ',
    'MAX(IF(o.concept_id=164934,o.value_coded,NULL)) AS cacx_screening, ',
    'MAX(IF(o.concept_id=163589,o.value_coded,NULL)) AS cacx_screening_method, ',
    'MAX(IF(o.concept_id=165040,o.value_coded,NULL)) AS hepatitis_b_screening, ',
    'MAX(IF(o.concept_id=166665,o.value_coded,NULL)) AS hepatitis_b_treatment, ',
    'MAX(IF(o.concept_id=162747,o.value_coded,NULL)) AS has_other_illnes, ',
    'MAX(IF(o.concept_id=1912,o.value_coded,NULL)) AS counselled, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=159758,o.value_coded,NULL)) AS counselled_on_birth_plans, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=159857,o.value_coded,NULL)) AS counselled_on_danger_signs, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=156277,o.value_coded,NULL)) AS counselled_on_family_planning, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=1914,o.value_coded,NULL)) AS counselled_on_hiv, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=159854,o.value_coded,NULL)) AS counselled_on_supplimental_feeding, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=159856,o.value_coded,NULL)) AS counselled_on_breast_care, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=161651,o.value_coded,NULL)) AS counselled_on_infant_feeding, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=1381,o.value_coded,NULL)) AS counselled_on_treated_nets, ',
    'MAX(IF(o.concept_id=1591,o.value_coded,NULL)) AS intermittent_presumptive_treatment_given, ',
    'MAX(IF(o.concept_id=1418,o.value_numeric,NULL)) AS intermittent_presumptive_treatment_dose, ',
    'MAX(IF(o.concept_id IN (165302,161595),o.value_coded,NULL)) AS minimum_care_package, ',
    'CONCAT_WS('','', ',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =165275,''Risk Reduction counselling'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =161557,''HIV Testing for the Partner'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =165190,''STI Screening and treatment'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =159777,''Condom Provision'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =165203,''PrEP with emphasis on adherence'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =165475,''Emphasize importance of follow up ANC Visits'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =1382,''Postnatal FP Counselling and support'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =162223,''Referrals for VMMC Services for partner'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =165368,''Referrals for OVC/DREAMS'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =166607,''Pre appointmnet SMS'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =166486,''Tartgeted home visits'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =1167,''Psychosocial and disclosure support'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =165002,''3-monthly Enhanced ART adherence assessments optimize TLD'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =163310,''Timely viral load monitoring, early ART switches'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =167410,''Complex case reviews in MDT/Consultation with clinical mentors'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =167079,''Enhanced longitudinal Mother-Infant Pair follow up'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =166563,''Early HEI case identification'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =160116,''Bi weekly random file audits to inform quality improvement'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =160031,''LTFU root cause audit and return to care plan default'', '''')),'''')',
    ') AS minimum_package_of_care_services, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=165275,o.value_coded,NULL)) AS risk_reduction, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=161557,o.value_coded,NULL)) AS partner_testing, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=165190,o.value_coded,NULL)) AS sti_screening, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=159777,o.value_coded,NULL)) AS condom_provision, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=165203,o.value_coded,NULL)) AS prep_adherence, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=165475,o.value_coded,NULL)) AS anc_visits_emphasis, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=1382,o.value_coded,NULL)) AS pnc_fp_counseling, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=162223,o.value_coded,NULL)) AS referral_vmmc, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=165368,o.value_coded,NULL)) AS referral_dreams, ',
    'MAX(IF(o.concept_id=160481,o.value_coded,NULL)) AS referred_from, ',
    'MAX(IF(o.concept_id=163145,o.value_coded,NULL)) AS referred_to, ',
    'MAX(IF(o.concept_id=159395,o.value_text,NULL)) AS clinical_notes, ',
    'e.date_created AS date_created, ',
    'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
  'FROM encounter e ',
  'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
  'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
    'AND o.concept_id IN (1282,159922,984,1418,1425,5088,5087,5085,5086,5242,5092,5089,5090,1343,21,163590,5245,1438,1439,160090,162089,1440,162107,5356,5497,856,1305,1147,159427,164848,161557,1436,1109,5576,128256,1875,159734,161438,161439,161440,161441,161442,161444,161443,162106,162101,162096,299,159918,32,119481,165099,120198,374,161074,1659,164934,163589,165040,166665,162747,1912,160481,163145,5096,159395,163784,1271,159853,165302,1592,1591,1418,1592,161595) ',
  'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''e8f98494-af35-4bb8-9fc7-c409c8fed843'',''d3ea25c7-a3e8-4f57-a6a9-e802c3565a30'')) f ON f.form_id = e.form_id ',
  'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
  ') ',
  'GROUP BY e.patient_id, visit_date ',
  'ON DUPLICATE KEY UPDATE ',
    'visit_date=VALUES(visit_date),provider=VALUES(provider),anc_visit_number=VALUES(anc_visit_number),temperature=VALUES(temperature),pulse_rate=VALUES(pulse_rate),systolic_bp=VALUES(systolic_bp),diastolic_bp=VALUES(diastolic_bp),respiratory_rate=VALUES(respiratory_rate),',
    'oxygen_saturation=VALUES(oxygen_saturation),weight=VALUES(weight),height=VALUES(height),muac=VALUES(muac),hemoglobin=VALUES(hemoglobin),breast_exam_done=VALUES(breast_exam_done),pallor=VALUES(pallor),maturity=VALUES(maturity),fundal_height=VALUES(fundal_height),fetal_presentation=VALUES(fetal_presentation),lie=VALUES(lie),',
    'fetal_heart_rate=VALUES(fetal_heart_rate),fetal_movement=VALUES(fetal_movement),who_stage=VALUES(who_stage),cd4=VALUES(cd4),vl_sample_taken=VALUES(vl_sample_taken),viral_load=VALUES(viral_load),ldl=VALUES(ldl),arv_status=VALUES(arv_status),final_test_result=VALUES(final_test_result),',
    'patient_given_result=VALUES(patient_given_result),partner_hiv_tested=VALUES(partner_hiv_tested),partner_hiv_status=VALUES(partner_hiv_status),prophylaxis_given=VALUES(prophylaxis_given),haart_given=VALUES(haart_given),date_given_haart=VALUES(date_given_haart),baby_azt_dispensed=VALUES(baby_azt_dispensed),baby_nvp_dispensed=VALUES(baby_nvp_dispensed),deworming_done_anc=VALUES(deworming_done_anc),',
    'TTT=VALUES(TTT),IPT_dose_given_anc=VALUES(IPT_dose_given_anc),IPT_malaria=VALUES(IPT_malaria),iron_supplement=VALUES(iron_supplement),deworming=VALUES(deworming),bed_nets=VALUES(bed_nets),urine_microscopy=VALUES(urine_microscopy),urinary_albumin=VALUES(urinary_albumin),glucose_measurement=VALUES(glucose_measurement),urine_ph=VALUES(urine_ph),urine_gravity=VALUES(urine_gravity),',
    'urine_nitrite_test=VALUES(urine_nitrite_test),urine_leukocyte_esterace_test=VALUES(urine_leukocyte_esterace_test),urinary_ketone=VALUES(urinary_ketone),urine_bile_salt_test=VALUES(urine_bile_salt_test),urine_bile_pigment_test=VALUES(urine_bile_pigment_test),urine_colour=VALUES(urine_colour),urine_turbidity=VALUES(urine_turbidity),urine_dipstick_for_blood=VALUES(urine_dipstick_for_blood),syphilis_test_status=VALUES(syphilis_test_status),syphilis_treated_status=VALUES(syphilis_treated_status),',
    'bs_mps=VALUES(bs_mps),diabetes_test=VALUES(diabetes_test),fgm_done=VALUES(fgm_done),fgm_complications=VALUES(fgm_complications),fp_method_postpartum=VALUES(fp_method_postpartum),anc_exercises=VALUES(anc_exercises),tb_screening=VALUES(tb_screening),cacx_screening=VALUES(cacx_screening),cacx_screening_method=VALUES(cacx_screening_method),hepatitis_b_screening=VALUES(hepatitis_b_screening),hepatitis_b_treatment=VALUES(hepatitis_b_treatment),',
    'has_other_illnes=VALUES(has_other_illnes),counselled=VALUES(counselled),counselled_on_birth_plans=VALUES(counselled_on_birth_plans),counselled_on_danger_signs=VALUES(counselled_on_danger_signs),counselled_on_family_planning=VALUES(counselled_on_family_planning),counselled_on_hiv=VALUES(counselled_on_hiv),counselled_on_supplimental_feeding=VALUES(counselled_on_supplimental_feeding),counselled_on_breast_care=VALUES(counselled_on_breast_care),counselled_on_infant_feeding=VALUES(counselled_on_infant_feeding),counselled_on_treated_nets=VALUES(counselled_on_treated_nets),referred_from=VALUES(referred_from),',
    'minimum_care_package=VALUES(minimum_care_package),risk_reduction=VALUES(risk_reduction),partner_testing=VALUES(partner_testing),sti_screening=VALUES(sti_screening),condom_provision=VALUES(condom_provision),prep_adherence=VALUES(prep_adherence),anc_visits_emphasis=VALUES(anc_visits_emphasis),pnc_fp_counseling=VALUES(pnc_fp_counseling),referral_vmmc=VALUES(referral_vmmc),referral_dreams=VALUES(referral_dreams),referred_to=VALUES(referred_to),next_appointment_date=VALUES(next_appointment_date),clinical_notes=VALUES(clinical_notes),intermittent_presumptive_treatment_given=VALUES(intermittent_presumptive_treatment_given),intermittent_presumptive_treatment_dose=VALUES(intermittent_presumptive_treatment_dose),minimum_package_of_care_services=VALUES(minimum_package_of_care_services);'
);

  PREPARE stmt FROM sql_stmt;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
END $$

-- ------------- update etl_mchs_delivery-------------------------

-- sql
DELIMITER $$

DROP PROCEDURE IF EXISTS sp_update_etl_mch_delivery $$
CREATE PROCEDURE sp_update_etl_mch_delivery(IN last_update_time DATETIME)
BEGIN
  CALL sp_set_tenant_session_vars();
  SET @target_table = CONCAT('`', @etl_schema, '`.`etl_mchs_delivery`');
  SET @sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id,uuid,provider,visit_id,visit_date,location_id,encounter_id,date_created,date_last_modified,number_of_anc_visits,',
      'vaginal_examination,uterotonic_given,chlohexidine_applied_on_code_stump,vitamin_K_given,kangaroo_mother_care_given,',
      'testing_done_in_the_maternity_hiv_status,infant_provided_with_arv_prophylaxis,mother_on_haart_during_anc,mother_started_haart_at_maternity,vdrl_rpr_results,',
      'date_of_last_menstrual_period,estimated_date_of_delivery,reason_for_referral,admission_number,duration_of_pregnancy,mode_of_delivery,',
      'date_of_delivery,blood_loss,condition_of_mother,delivery_outcome,apgar_score_1min,apgar_score_5min,apgar_score_10min,resuscitation_done,',
      'place_of_delivery,delivery_assistant,counseling_on_infant_feeding,counseling_on_exclusive_breastfeeding,counseling_on_infant_feeding_for_hiv_infected,',
      'mother_decision,placenta_complete,maternal_death_audited,cadre,delivery_complications,coded_delivery_complications,other_delivery_complications,',
      'duration_of_labor,baby_sex,baby_condition,teo_given,birth_weight,bf_within_one_hour,birth_with_deformity,type_of_birth_deformity,',
      'final_test_result,patient_given_result,partner_hiv_tested,partner_hiv_status,prophylaxis_given,baby_azt_dispensed,baby_nvp_dispensed,',
      'clinical_notes,stimulation_done,suction_done,oxygen_given,bag_mask_ventilation_provided,induction_done,artificial_rapture_done',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_datetime, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id=1590,o.value_numeric,NULL)) AS number_of_anc_visits, ',
      'MAX(IF(o.concept_id=160704,o.value_coded,NULL)) AS vaginal_examination, ',
      'MAX(IF(o.concept_id=1282 AND o.value_coded IN (81369,104590,5622,1107),o.value_coded,NULL)) AS uterotonic_given, ',
      'MAX(IF(o.concept_id=159369,o.value_coded,NULL)) AS chlohexidine_applied_on_code_stump, ',
      'MAX(IF(o.concept_id=984,o.value_coded,NULL)) AS vitamin_K_given, ',
      'MAX(IF(o.concept_id=161094,o.value_coded,NULL)) AS kangaroo_mother_care_given, ',
      'MAX(IF(o.concept_id=1396,o.value_coded,NULL)) AS testing_done_in_the_maternity_hiv_status, ',
      'MAX(IF(o.concept_id=161930,o.value_coded,NULL)) AS infant_provided_with_arv_prophylaxis, ',
      'MAX(IF(o.concept_id=163783,o.value_coded,NULL)) AS mother_on_haart_during_anc, ',
      'MAX(IF(o.concept_id=166665,o.value_coded,NULL)) AS mother_started_haart_at_maternity, ',
      'MAX(IF(o.concept_id=299,o.value_coded,NULL)) AS vdrl_rpr_results, ',
      'MAX(IF(o.concept_id=1427,o.value_datetime,NULL)) AS date_of_last_menstrual_period, ',
      'MAX(IF(o.concept_id=5596,o.value_datetime,NULL)) AS estimated_date_of_delivery, ',
      'MAX(IF(o.concept_id=164359,o.value_text,NULL)) AS reason_for_referral, ',
      'MAX(IF(o.concept_id=162054,o.value_text,NULL)) AS admission_number, ',
      'MAX(IF(o.concept_id=1789,o.value_numeric,NULL)) AS duration_of_pregnancy, ',
      'MAX(IF(o.concept_id=5630,o.value_coded,NULL)) AS mode_of_delivery, ',
      'MAX(IF(o.concept_id=5599,o.value_datetime,NULL)) AS date_of_delivery, ',
      'MAX(IF(o.concept_id=161928,o.value_numeric,NULL)) AS blood_loss, ',
      'MAX(IF(o.concept_id=1856,o.value_coded,NULL)) AS condition_of_mother, ',
      'MAX(IF(o.concept_id=159949,o.value_coded,NULL)) AS delivery_outcome, ',
      'MAX(IF(o.concept_id=159603,o.value_numeric,NULL)) AS apgar_score_1min, ',
      'MAX(IF(o.concept_id=159604,o.value_numeric,NULL)) AS apgar_score_5min, ',
      'MAX(IF(o.concept_id=159605,o.value_numeric,NULL)) AS apgar_score_10min, ',
      'MAX(IF(o.concept_id=162131,o.value_coded,NULL)) AS resuscitation_done, ',
      'MAX(IF(o.concept_id=1572,o.value_coded,NULL)) AS place_of_delivery, ',
      'MAX(IF(o.concept_id=1473,o.value_text,NULL)) AS delivery_assistant, ',
      'MAX(IF(o.concept_id=1379 AND o.value_coded=161651,o.value_coded,NULL)) AS counseling_on_infant_feeding, ',
      'MAX(IF(o.concept_id=1379 AND o.value_coded=161096,o.value_coded,NULL)) AS counseling_on_exclusive_breastfeeding, ',
      'MAX(IF(o.concept_id=162091,o.value_coded,NULL)) AS counseling_on_infant_feeding_for_hiv_infected, ',
      'MAX(IF(o.concept_id=1151,o.value_coded,NULL)) AS mother_decision, ',
      'MAX(IF(o.concept_id=163454,o.value_coded,NULL)) AS placenta_complete, ',
      'MAX(IF(o.concept_id=1602,o.value_coded,NULL)) AS maternal_death_audited, ',
      'MAX(IF(o.concept_id=1573,o.value_coded,NULL)) AS cadre, ',
      'MAX(IF(o.concept_id=120216,o.value_coded,NULL)) AS delivery_complications, ',
      'MAX(IF(o.concept_id=1576,o.value_coded,NULL)) AS coded_delivery_complications, ',
      'MAX(IF(o.concept_id=162093,o.value_text,NULL)) AS other_delivery_complications, ',
      'MAX(IF(o.concept_id=159616,o.value_numeric,NULL)) AS duration_of_labor, ',
      'MAX(IF(o.concept_id=1587,o.value_coded,NULL)) AS baby_sex, ',
      'MAX(IF(o.concept_id=159917,o.value_coded,NULL)) AS baby_condition, ',
      'MAX(IF(o.concept_id=1570,o.value_coded,NULL)) AS teo_given, ',
      'MAX(IF(o.concept_id=5916,o.value_numeric,NULL)) AS birth_weight, ',
      'MAX(IF(o.concept_id=161543,o.value_coded,NULL)) AS bf_within_one_hour, ',
      'MAX(IF(o.concept_id=164122,o.value_coded,NULL)) AS birth_with_deformity, ',
      'MAX(IF(o.concept_id=159521,o.value_coded,NULL)) AS type_of_birth_deformity, ',,
      'MAX(IF(o.concept_id=159427,(CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1138 THEN ''Inconclusive'' ELSE '''' END),NULL)) AS final_test_result, ',
      'MAX(IF(o.concept_id=164848,(CASE o.value_coded WHEN 1065 THEN \"Yes\" WHEN 1066 THEN \"No\" ELSE \"\" END),NULL)) AS patient_given_result, ',
      'MAX(IF(o.concept_id=161557,o.value_coded,NULL)) AS partner_hiv_tested, ',
      'MAX(IF(o.concept_id=1436,o.value_coded,NULL)) AS partner_hiv_status, ',
      'MAX(IF(o.concept_id=1109,o.value_coded,NULL)) AS prophylaxis_given, ',
      'MAX(IF(o.concept_id = 1282 AND o.value_coded = 160123,1,0)) AS baby_azt_dispensed, ',
      'MAX(IF(o.concept_id = 1282 AND o.value_coded = 80586,1,0)) AS baby_nvp_dispensed, ',
      'MAX(IF(o.concept_id=159395,o.value_text,NULL)) AS clinical_notes, ',
      'MAX(IF(o.concept_id=168751,o.value_coded,NULL)) AS stimulation_done, ',
      'MAX(IF(o.concept_id=1284,o.value_coded,NULL)) AS suction_done, ',
      'MAX(IF(o.concept_id=113316,o.value_coded,NULL)) AS oxygen_given, ',
      'MAX(IF(o.concept_id=165647,o.value_coded,NULL)) AS bag_mask_ventilation_provided, ',
      'MAX(IF(o.concept_id=113602,o.value_coded,NULL)) AS induction_done, ',
      'MAX(IF(o.concept_id=163445,o.value_coded,NULL)) AS artificial_rapture_done ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
        'AND o.concept_id IN (162054,1590,160704,1282,159369,984,161094,1396,161930,163783,166665,299,1427,5596,164359,1789,5630,5599,161928,1856,159603,159604,159605,162131,1572,1473,1379,1151,163454,1602,1573,162093,1576,120216,159616,1587,159917,1282,5916,161543,164122,159521,159427,164848,161557,1436,1109,5576,159595,163784,159395,168751,1284,113316,165647,113602,163445,159949,1570) ',
      'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''496c7cc3-0eea-4e84-a04c-2292949e2f7f'')) f ON f.form_id = e.form_id ',
    'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider),visit_id=VALUES(visit_id),visit_date=VALUES(visit_date),encounter_id=VALUES(encounter_id),number_of_anc_visits=VALUES(number_of_anc_visits),',
      'vaginal_examination=VALUES(vaginal_examination),uterotonic_given=VALUES(uterotonic_given),chlohexidine_applied_on_code_stump=VALUES(chlohexidine_applied_on_code_stump),vitamin_K_given=VALUES(vitamin_K_given),',
      'kangaroo_mother_care_given=VALUES(kangaroo_mother_care_given),testing_done_in_the_maternity_hiv_status=VALUES(testing_done_in_the_maternity_hiv_status),infant_provided_with_arv_prophylaxis=VALUES(infant_provided_with_arv_prophylaxis),',
      'mother_on_haart_during_anc=VALUES(mother_on_haart_during_anc),mother_started_haart_at_maternity=VALUES(mother_started_haart_at_maternity),vdrl_rpr_results=VALUES(vdrl_rpr_results),',
      'date_of_last_menstrual_period=VALUES(date_of_last_menstrual_period),estimated_date_of_delivery=VALUES(estimated_date_of_delivery),reason_for_referral=VALUES(reason_for_referral),',
      'date_created=VALUES(date_created),admission_number=VALUES(admission_number),duration_of_pregnancy=VALUES(duration_of_pregnancy),mode_of_delivery=VALUES(mode_of_delivery),date_of_delivery=VALUES(date_of_delivery),',
      'blood_loss=VALUES(blood_loss),condition_of_mother=VALUES(condition_of_mother),apgar_score_1min=VALUES(apgar_score_1min),apgar_score_5min=VALUES(apgar_score_5min),apgar_score_10min=VALUES(apgar_score_10min),',
      'resuscitation_done=VALUES(resuscitation_done),place_of_delivery=VALUES(place_of_delivery),delivery_assistant=VALUES(delivery_assistant),counseling_on_infant_feeding=VALUES(counseling_on_infant_feeding),',
      'counseling_on_exclusive_breastfeeding=VALUES(counseling_on_exclusive_breastfeeding),counseling_on_infant_feeding_for_hiv_infected=VALUES(counseling_on_infant_feeding_for_hiv_infected),mother_decision=VALUES(mother_decision),',
      'placenta_complete=VALUES(placenta_complete),maternal_death_audited=VALUES(maternal_death_audited),cadre=VALUES(cadre),delivery_complications=VALUES(delivery_complications),',
      'coded_delivery_complications=VALUES(coded_delivery_complications),other_delivery_complications=VALUES(other_delivery_complications),duration_of_labor=VALUES(duration_of_labor),baby_sex=VALUES(baby_sex),',
      'baby_condition=VALUES(baby_condition),teo_given=VALUES(teo_given),birth_weight=VALUES(birth_weight),bf_within_one_hour=VALUES(bf_within_one_hour),birth_with_deformity=VALUES(birth_with_deformity),',
      'type_of_birth_deformity=VALUES(type_of_birth_deformity),final_test_result=VALUES(final_test_result),patient_given_result=VALUES(patient_given_result),partner_hiv_tested=VALUES(partner_hiv_tested),',
      'partner_hiv_status=VALUES(partner_hiv_status),prophylaxis_given=VALUES(prophylaxis_given),baby_azt_dispensed=VALUES(baby_azt_dispensed),baby_nvp_dispensed=VALUES(baby_nvp_dispensed),',
      'clinical_notes=VALUES(clinical_notes),stimulation_done=VALUES(stimulation_done),suction_done=VALUES(suction_done),oxygen_given=VALUES(oxygen_given),',
      'bag_mask_ventilation_provided=VALUES(bag_mask_ventilation_provided),induction_done=VALUES(induction_done),artificial_rapture_done=VALUES(artificial_rapture_done);'
  );

  PREPARE stmt FROM @sql_stmt;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
END $$
DELIMITER ;

-- ------------- populate etl_mchs_discharge-------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_mch_discharge $$
CREATE PROCEDURE sp_update_etl_mch_discharge(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_mchs_discharge`');

  SELECT "Processing MCH Discharge ", CONCAT("Time: ", NOW());

  SET @sql = CONCAT(
    'INSERT INTO ', target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, date_created, date_last_modified, ',
      'counselled_on_feeding, baby_status, vitamin_A_dispensed, birth_notification_number, condition_of_mother, discharge_date, ',
      'referred_from, referred_to, clinical_notes',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_datetime, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 161651, o.value_coded, NULL)) AS counselled_on_feeding, ',
      'MAX(IF(o.concept_id = 159926, o.value_coded, NULL)) AS baby_status, ',
      'MAX(IF(o.concept_id = 161534, o.value_coded, NULL)) AS vitamin_A_dispensed, ',
      'MAX(IF(o.concept_id = 162051, o.value_text, NULL)) AS birth_notification_number, ',
      'MAX(IF(o.concept_id = 162093, o.value_text, NULL)) AS condition_of_mother, ',
      'MAX(IF(o.concept_id = 1641, o.value_datetime, NULL)) AS discharge_date, ',
      'MAX(IF(o.concept_id = 160481, o.value_coded, NULL)) AS referred_from, ',
      'MAX(IF(o.concept_id = 163145, o.value_coded, NULL)) AS referred_to, ',
      'MAX(IF(o.concept_id = 159395, o.value_text, NULL)) AS clinical_notes ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
        'AND o.concept_id IN (161651,159926,161534,162051,162093,1641,160481,163145,159395) ',
      'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''af273344-a5f9-11e8-98d0-529269fb1459'')) f ON f.form_id = e.form_id ',
    'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), ',
      'provider = VALUES(provider), ',
      'counselled_on_feeding = VALUES(counselled_on_feeding), ',
      'baby_status = VALUES(baby_status), ',
      'vitamin_A_dispensed = VALUES(vitamin_A_dispensed), ',
      'birth_notification_number = VALUES(birth_notification_number), ',
      'condition_of_mother = VALUES(condition_of_mother), ',
      'discharge_date = VALUES(discharge_date), ',
      'referred_from = VALUES(referred_from), ',
      'referred_to = VALUES(referred_to), ',
      'clinical_notes = VALUES(clinical_notes);'
  );

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing MCH Discharge visits", CONCAT("Time: ", NOW());
END $$

-- ------------- update etl_mch_postnatal_visit-------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_mch_postnatal_visit $$
CREATE PROCEDURE sp_update_etl_mch_postnatal_visit(IN last_update_time DATETIME)
BEGIN
  DECLARE target_table VARCHAR(300);
  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_mch_postnatal_visit`');

  SELECT "Processing MCH postnatal visits ", CONCAT("Time: ", NOW());

  SET @sql = CONCAT(
    'INSERT INTO ', target_table, ' (',
      'patient_id, uuid, visit_id, visit_date, location_id, encounter_id, provider, pnc_register_no, pnc_visit_no, delivery_date, ',
      'mode_of_delivery, place_of_delivery, visit_timing_mother, visit_timing_baby, delivery_outcome, temperature, pulse_rate, ',
      'systolic_bp, diastolic_bp, respiratory_rate, oxygen_saturation, weight, height, muac, hemoglobin, arv_status, general_condition, ',
      'breast, cs_scar, gravid_uterus, episiotomy, lochia, counselled_on_infant_feeding, pallor, pallor_severity, pph, mother_hiv_status, ',
      'condition_of_baby, baby_feeding_method, umblical_cord, baby_immunization_started, family_planning_counseling, other_maternal_complications, ',
      'uterus_examination, uterus_cervix_examination, vaginal_examination, parametrial_examination, external_genitalia_examination, ovarian_examination, ',
      'pelvic_lymph_node_exam, final_test_result, syphilis_results, patient_given_result, couple_counselled, partner_hiv_tested, partner_hiv_status, ',
      'pnc_hiv_test_timing_mother, mother_haart_given, prophylaxis_given, infant_prophylaxis_timing, baby_azt_dispensed, baby_nvp_dispensed, ',
      'pnc_exercises, maternal_condition, iron_supplementation, fistula_screening, cacx_screening, cacx_screening_method, family_planning_status, ',
      'family_planning_method, referred_from, referred_to, referral_reason, clinical_notes, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.visit_id, e.encounter_datetime, e.location_id, e.encounter_id, e.creator, ',
      'MAX(IF(o.concept_id=1646, o.value_text, NULL)) AS pnc_register_no, ',
      'MAX(IF(o.concept_id=159893, o.value_numeric, NULL)) AS pnc_visit_no, ',
      'MAX(IF(o.concept_id=5599, o.value_datetime, NULL)) AS delivery_date, ',
      'MAX(IF(o.concept_id=5630, o.value_coded, NULL)) AS mode_of_delivery, ',
      'MAX(IF(o.concept_id=1572, o.value_coded, NULL)) AS place_of_delivery, ',
      'MAX(IF(o.concept_id=1724, o.value_coded, NULL)) AS visit_timing_mother, ',
      'MAX(IF(o.concept_id=167017, o.value_coded, NULL)) AS visit_timing_baby, ',
      'MAX(IF(o.concept_id=159949, o.value_coded, NULL)) AS delivery_outcome, ',
      'MAX(IF(o.concept_id=5088, o.value_numeric, NULL)) AS temperature, ',
      'MAX(IF(o.concept_id=5087, o.value_numeric, NULL)) AS pulse_rate, ',
      'MAX(IF(o.concept_id=5085, o.value_numeric, NULL)) AS systolic_bp, ',
      'MAX(IF(o.concept_id=5086, o.value_numeric, NULL)) AS diastolic_bp, ',
      'MAX(IF(o.concept_id=5242, o.value_numeric, NULL)) AS respiratory_rate, ',
      'MAX(IF(o.concept_id=5092, o.value_numeric, NULL)) AS oxygen_saturation, ',
      'MAX(IF(o.concept_id=5089, o.value_numeric, NULL)) AS weight, ',
      'MAX(IF(o.concept_id=5090, o.value_numeric, NULL)) AS height, ',
      'MAX(IF(o.concept_id=1343, o.value_numeric, NULL)) AS muac, ',
      'MAX(IF(o.concept_id=21, o.value_numeric, NULL)) AS hemoglobin, ',
      'MAX(IF(o.concept_id=1147, o.value_coded, NULL)) AS arv_status, ',
      'MAX(IF(o.concept_id=1856, o.value_coded, NULL)) AS general_condition, ',
      'MAX(IF(o.concept_id=159780, o.value_coded, NULL)) AS breast, ',
      'MAX(IF(o.concept_id=162128, o.value_coded, NULL)) AS cs_scar, ',
      'MAX(IF(o.concept_id=162110, o.value_coded, NULL)) AS gravid_uterus, ',
      'MAX(IF(o.concept_id=159840, o.value_coded, NULL)) AS episiotomy, ',
      'MAX(IF(o.concept_id=159844, o.value_coded, NULL)) AS lochia, ',
      'MAX(IF(o.concept_id=161651, o.value_coded, NULL)) AS counselled_on_infant_feeding, ',
      'MAX(IF(o.concept_id=5245, o.value_coded, NULL)) AS pallor, ',
      'MAX(IF(o.concept_id=162642, o.value_coded, NULL)) AS pallor_severity, ',
      'MAX(IF(o.concept_id=230, o.value_coded, NULL)) AS pph, ',
      'MAX(IF(o.concept_id=1396, o.value_coded, NULL)) AS mother_hiv_status, ',
      'MAX(IF(o.concept_id=162134, o.value_coded, NULL)) AS condition_of_baby, ',
      'MAX(IF(o.concept_id=1151, o.value_coded, NULL)) AS baby_feeding_method, ',
      'MAX(IF(o.concept_id=162121, o.value_coded, NULL)) AS umblical_cord, ',
      'MAX(IF(o.concept_id=162127, o.value_coded, NULL)) AS baby_immunization_started, ',
      'MAX(IF(o.concept_id=1382, o.value_coded, NULL)) AS family_planning_counseling, ',
      'MAX(IF(o.concept_id=160632, o.value_text, NULL)) AS other_maternal_complications, ',
      'MAX(IF(o.concept_id=163742, o.value_coded, NULL)) AS uterus_examination, ',
      'MAX(IF(o.concept_id=160968, o.value_text, NULL)) AS uterus_cervix_examination, ',
      'MAX(IF(o.concept_id=160969, o.value_text, NULL)) AS vaginal_examination, ',
      'MAX(IF(o.concept_id=160970, o.value_text, NULL)) AS parametrial_examination, ',
      'MAX(IF(o.concept_id=160971, o.value_text, NULL)) AS external_genitalia_examination, ',
      'MAX(IF(o.concept_id=160975, o.value_text, NULL)) AS ovarian_examination, ',
      'MAX(IF(o.concept_id=160972, o.value_text, NULL)) AS pelvic_lymph_node_exam, ',
      'MAX(IF(o.concept_id=159427, (CASE o.value_coded WHEN 703 THEN \"Positive\" WHEN 664 THEN \"Negative\" WHEN 1138 THEN \"Inconclusive\" ELSE \"\" END), NULL)) AS final_test_result, ',
      'MAX(IF(o.concept_id=299, o.value_coded, NULL)) AS syphilis_results, ',
      'MAX(IF(o.concept_id=164848, (CASE o.value_coded WHEN 1065 THEN \"Yes\" WHEN 1066 THEN \"No\" ELSE \"\" END), NULL)) AS patient_given_result, ',
      'MAX(IF(o.concept_id=165070, o.value_coded, NULL)) AS couple_counselled, ',
      'MAX(IF(o.concept_id=161557, o.value_coded, NULL)) AS partner_hiv_tested, ',
      'MAX(IF(o.concept_id=1436, o.value_coded, NULL)) AS partner_hiv_status, ',
      'MAX(IF(o.concept_id=165218, o.value_coded, NULL)) AS pnc_hiv_test_timing_mother, ',
      'MAX(IF(o.concept_id=163783, o.value_coded, NULL)) AS mother_haart_given, ',
      'MAX(IF(o.concept_id=1109, o.value_coded, NULL)) AS prophylaxis_given, ',
      'MAX(IF(o.concept_id=166665, o.value_coded, NULL)) AS infant_prophylaxis_timing, ',
      'MAX(IF(o.concept_id=1282 AND o.value_coded = 160123, o.value_coded, NULL)) AS baby_azt_dispensed, ',
      'MAX(IF(o.concept_id=1282 AND o.value_coded = 80586, o.value_coded, NULL)) AS baby_nvp_dispensed, ',
      'MAX(IF(o.concept_id=161074, o.value_coded, NULL)) AS pnc_exercises, ',
      'MAX(IF(o.concept_id=160085, o.value_coded, NULL)) AS maternal_condition, ',
      'MAX(IF(o.concept_id=161004, o.value_coded, NULL)) AS iron_supplementation, ',
      'MAX(IF(o.concept_id=159921, o.value_coded, NULL)) AS fistula_screening, ',
      'MAX(IF(o.concept_id=164934, o.value_coded, NULL)) AS cacx_screening, ',
      'MAX(IF(o.concept_id=163589, o.value_coded, NULL)) AS cacx_screening_method, ',
      'MAX(IF(o.concept_id=160653, o.value_coded, NULL)) AS family_planning_status, ',
      'CONCAT_WS('','', ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =160570, \"Emergency contraceptive pills\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =780, \"Oral Contraceptives Pills\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =5279, \"Injectible\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =1359, \"Implant\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =5275, \"Intrauterine Device\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =136163, \"Lactational Amenorhea Method\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =5278, \"Diaphram/Cervical Cap\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =5277, \"Fertility Awareness\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =1472, \"Tubal Ligation\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =190, \"Condoms\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =1489, \"Vasectomy\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =162332, \"Undecided\", '''')), '''') ',
      ') AS family_planning_method, ',
      'MAX(IF(o.concept_id=160481, o.value_coded, NULL)) AS referred_from, ',
      'MAX(IF(o.concept_id=163145, o.value_coded, NULL)) AS referred_to, ',
      'MAX(IF(o.concept_id=164359, o.value_text, NULL)) AS referral_reason, ',
      'MAX(IF(o.concept_id=159395, o.value_text, NULL)) AS clinical_notes, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
        'AND o.concept_id IN (1646,159893,5599,5630,1572,5088,5087,5085,5086,5242,5092,5089,5090,1343,21,1147,1856,159780,162128,162110,159840,159844,5245,230,1396,162134,1151,162121,162127,1382,163742,160968,160969,160970,160971,160975,160972,159427,164848,161557,1436,1109,5576,159595,163784,1282,161074,160085,161004,159921,164934,163589,160653,374,160481,163145,159395,159949,5096,161651,165070,1724,167017,163783,162642,166665,165218,160632,299,159395) ',
      'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''72aa78e0-ee4b-47c3-9073-26f3b9ecc4a7'')) f ON f.form_id = e.form_id ',
    'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date), encounter_id=VALUES(encounter_id), provider=VALUES(provider), pnc_register_no=VALUES(pnc_register_no), ',
      'pnc_visit_no=VALUES(pnc_visit_no), delivery_date=VALUES(delivery_date), mode_of_delivery=VALUES(mode_of_delivery), place_of_delivery=VALUES(place_of_delivery), ',
      'visit_timing_mother=VALUES(visit_timing_mother), visit_timing_baby=VALUES(visit_timing_baby), delivery_outcome=VALUES(delivery_outcome), temperature=VALUES(temperature), ',
      'pulse_rate=VALUES(pulse_rate), systolic_bp=VALUES(systolic_bp), diastolic_bp=VALUES(diastolic_bp), respiratory_rate=VALUES(respiratory_rate), ',
      'oxygen_saturation=VALUES(oxygen_saturation), weight=VALUES(weight), height=VALUES(height), muac=VALUES(muac), hemoglobin=VALUES(hemoglobin), ',
      'arv_status=VALUES(arv_status), general_condition=VALUES(general_condition), breast=VALUES(breast), cs_scar=VALUES(cs_scar), gravid_uterus=VALUES(gravid_uterus), ',
      'episiotomy=VALUES(episiotomy), lochia=VALUES(lochia), pallor=VALUES(pallor), pph=VALUES(pph), mother_hiv_status=VALUES(mother_hiv_status), ',
      'condition_of_baby=VALUES(condition_of_baby), baby_feeding_method=VALUES(baby_feeding_method), umblical_cord=VALUES(umblical_cord), baby_immunization_started=VALUES(baby_immunization_started), ',
      'family_planning_counseling=VALUES(family_planning_counseling), uterus_examination=VALUES(uterus_examination), uterus_cervix_examination=VALUES(uterus_cervix_examination), ',
      'vaginal_examination=VALUES(vaginal_examination), parametrial_examination=VALUES(parametrial_examination), external_genitalia_examination=VALUES(external_genitalia_examination), ',
      'ovarian_examination=VALUES(ovarian_examination), pelvic_lymph_node_exam=VALUES(pelvic_lymph_node_exam), final_test_result=VALUES(final_test_result), ',
      'patient_given_result=VALUES(patient_given_result), couple_counselled=VALUES(couple_counselled), partner_hiv_tested=VALUES(partner_hiv_tested), ',
      'partner_hiv_status=VALUES(partner_hiv_status), mother_haart_given=VALUES(mother_haart_given), prophylaxis_given=VALUES(prophylaxis_given), ',
      'infant_prophylaxis_timing=VALUES(infant_prophylaxis_timing), baby_azt_dispensed=VALUES(baby_azt_dispensed), baby_nvp_dispensed=VALUES(baby_nvp_dispensed), ',
      'maternal_condition=VALUES(maternal_condition), iron_supplementation=VALUES(iron_supplementation), fistula_screening=VALUES(fistula_screening), ',
      'cacx_screening=VALUES(cacx_screening), cacx_screening_method=VALUES(cacx_screening_method), family_planning_status=VALUES(family_planning_status), ',
      'family_planning_method=VALUES(family_planning_method), referred_from=VALUES(referred_from), referred_to=VALUES(referred_to), referral_reason=VALUES(referral_reason), ',
      'clinical_notes=VALUES(clinical_notes), appointment_date=VALUES(appointment_date), counselled_on_infant_feeding=VALUES(counselled_on_infant_feeding), ',
      'pnc_hiv_test_timing_mother=VALUES(pnc_hiv_test_timing_mother), other_maternal_complications=VALUES(other_maternal_complications), syphilis_results=VALUES(syphilis_results);'
  );

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing MCH postnatal visits ", CONCAT("Time: ", NOW());
END $$

-- ------------- update etl_hei_enrollment-------------------------
DELIMITER $$

DROP PROCEDURE IF EXISTS sp_update_etl_hei_enrolment $$
CREATE PROCEDURE sp_update_etl_hei_enrolment(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_hei_enrollment`');
  SELECT "Processing HEI Enrollments", CONCAT("Time: ", NOW());

  SET @sql = CONCAT(
    'INSERT INTO ', target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, child_exposed, ',
      'spd_number, birth_weight, gestation_at_birth, birth_type, date_first_seen, birth_notification_number, ',
      'birth_certificate_number, need_for_special_care, reason_for_special_care, referral_source, transfer_in, ',
      'transfer_in_date, facility_transferred_from, district_transferred_from, date_first_enrolled_in_hei_care, ',
      'mother_breastfeeding, TB_contact_history_in_household, mother_alive, mother_on_pmtct_drugs, mother_on_drug, ',
      'mother_on_art_at_infant_enrollment, mother_drug_regimen, infant_prophylaxis, parent_ccc_number, mode_of_delivery, ',
      'place_of_delivery, birth_length, birth_order, health_facility_name, date_of_birth_notification, ',
      'date_of_birth_registration, birth_registration_place, permanent_registration_serial, mother_facility_registered, ',
      'exit_date, exit_reason, hiv_status_at_exit, encounter_type, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id=5303, o.value_coded, NULL)) AS child_exposed, ',
      'MAX(IF(o.concept_id=162054, o.value_text, NULL)) AS spd_number, ',
      'MAX(IF(o.concept_id=5916, o.value_numeric, NULL)) AS birth_weight, ',
      'MAX(IF(o.concept_id=1409, o.value_numeric, NULL)) AS gestation_at_birth, ',
      'MAX(IF(o.concept_id=159949, o.value_coded, NULL)) AS birth_type, ',
      'MAX(IF(o.concept_id=162140, o.value_datetime, NULL)) AS date_first_seen, ',
      'MAX(IF(o.concept_id=162051, o.value_text, NULL)) AS birth_notification_number, ',
      'MAX(IF(o.concept_id=162052, o.value_text, NULL)) AS birth_certificate_number, ',
      'MAX(IF(o.concept_id=161630, o.value_coded, NULL)) AS need_for_special_care, ',
      'MAX(IF(o.concept_id=161601, o.value_coded, NULL)) AS reason_for_special_care, ',
      'MAX(IF(o.concept_id=160540, o.value_coded, NULL)) AS referral_source, ',
      'MAX(IF(o.concept_id=160563, o.value_coded, NULL)) AS transfer_in, ',
      'MAX(IF(o.concept_id=160534, o.value_datetime, NULL)) AS transfer_in_date, ',
      'MAX(IF(o.concept_id=160535, o.value_text, NULL)) AS facility_transferred_from, ',
      'MAX(IF(o.concept_id=161551, o.value_text, NULL)) AS district_transferred_from, ',
      'MAX(IF(o.concept_id=160555, o.value_datetime, NULL)) AS date_first_enrolled_in_hei_care, ',
      'MAX(IF(o.concept_id=159941, o.value_coded, NULL)) AS mother_breastfeeding, ',
      'MAX(IF(o.concept_id=152460, o.value_coded, NULL)) AS TB_contact_history_in_household, ',
      'MAX(IF(o.concept_id=160429, o.value_coded, NULL)) AS mother_alive, ',
      'MAX(IF(o.concept_id=1148, o.value_coded, NULL)) AS mother_on_pmtct_drugs, ',
      'MAX(IF(o.concept_id=1086, o.value_coded, NULL)) AS mother_on_drug, ',
      'MAX(IF(o.concept_id=162055, o.value_coded, NULL)) AS mother_on_art_at_infant_enrollment, ',
      'MAX(IF(o.concept_id=1088, o.value_coded, NULL)) AS mother_drug_regimen, ',
      'MAX(IF(o.concept_id=1282, o.value_coded, NULL)) AS infant_prophylaxis, ',
      'MAX(IF(o.concept_id=162053, o.value_numeric, NULL)) AS parent_ccc_number, ',
      'MAX(IF(o.concept_id=5630, o.value_coded, NULL)) AS mode_of_delivery, ',
      'MAX(IF(o.concept_id=1572, o.value_coded, NULL)) AS place_of_delivery, ',
      'MAX(IF(o.concept_id=1503, o.value_numeric, NULL)) AS birth_length, ',
      'MAX(IF(o.concept_id=163460, o.value_numeric, NULL)) AS birth_order, ',
      'MAX(IF(o.concept_id=162724, o.value_text, NULL)) AS health_facility_name, ',
      'MAX(IF(o.concept_id=164130, o.value_datetime, NULL)) AS date_of_birth_notification, ',
      'MAX(IF(o.concept_id=164129, o.value_datetime, NULL)) AS date_of_birth_registration, ',
      'MAX(IF(o.concept_id=164140, o.value_text, NULL)) AS birth_registration_place, ',
      'MAX(IF(o.concept_id=1646, o.value_text, NULL)) AS permanent_registration_serial, ',
      'MAX(IF(o.concept_id=162724, o.value_text, NULL)) AS mother_facility_registered, ',
      'MAX(IF(o.concept_id=160753, o.value_datetime, NULL)) AS exit_date, ',
      'MAX(IF(o.concept_id=161555, o.value_coded, NULL)) AS exit_reason, ',
      'MAX(IF(o.concept_id=159427, (CASE o.value_coded WHEN 703 THEN \"Positive\" WHEN 664 THEN \"Negative\" WHEN 1138 THEN \"Inconclusive\" ELSE \"\" END), NULL)) AS hiv_status_at_exit, ',
      'CASE et.uuid WHEN ''01894f88-dc73-42d4-97a3-0929118403fb'' THEN ''MCHCS_HEI_COMPLETION'' WHEN ''415f5136-ca4a-49a8-8db3-f994187c3af6'' THEN ''MCHCS_HEI_ENROLLMENT'' END AS encounter_type, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
        'AND o.concept_id IN (5303,162054,5916,1409,162140,162051,162052,161630,161601,160540,160563,160534,160535,161551,160555,1282,159941,1282,152460,160429,1148,1086,162055,1088,1282,162053,5630,1572,161555,159427,1503,163460,162724,164130,164129,164140,1646,160753,161555,159427,159949) ',
      'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''415f5136-ca4a-49a8-8db3-f994187c3af6'',''01894f88-dc73-42d4-97a3-0929118403fb'')) et ON et.encounter_type_id = e.encounter_type ',
    'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, visit_date ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider), visit_id=VALUES(visit_id), visit_date=VALUES(visit_date), child_exposed=VALUES(child_exposed), ',
      'spd_number=VALUES(spd_number), birth_weight=VALUES(birth_weight), gestation_at_birth=VALUES(gestation_at_birth), date_first_seen=VALUES(date_first_seen), ',
      'birth_notification_number=VALUES(birth_notification_number), birth_certificate_number=VALUES(birth_certificate_number), need_for_special_care=VALUES(need_for_special_care), ',
      'reason_for_special_care=VALUES(reason_for_special_care), referral_source=VALUES(referral_source), transfer_in=VALUES(transfer_in), transfer_in_date=VALUES(transfer_in_date), ',
      'facility_transferred_from=VALUES(facility_transferred_from), district_transferred_from=VALUES(district_transferred_from), date_first_enrolled_in_hei_care=VALUES(date_first_enrolled_in_hei_care), ',
      'mother_breastfeeding=VALUES(mother_breastfeeding), TB_contact_history_in_household=VALUES(TB_contact_history_in_household), mother_alive=VALUES(mother_alive), ',
      'mother_on_pmtct_drugs=VALUES(mother_on_pmtct_drugs), mother_on_drug=VALUES(mother_on_drug), mother_on_art_at_infant_enrollment=VALUES(mother_on_art_at_infant_enrollment), ',
      'mother_drug_regimen=VALUES(mother_drug_regimen), infant_prophylaxis=VALUES(infant_prophylaxis), parent_ccc_number=VALUES(parent_ccc_number), ',
      'mode_of_delivery=VALUES(mode_of_delivery), place_of_delivery=VALUES(place_of_delivery), birth_length=VALUES(birth_length), birth_order=VALUES(birth_order), ',
      'health_facility_name=VALUES(health_facility_name), date_of_birth_notification=VALUES(date_of_birth_notification), date_of_birth_registration=VALUES(date_of_birth_registration), ',
      'birth_registration_place=VALUES(birth_registration_place), permanent_registration_serial=VALUES(permanent_registration_serial), mother_facility_registered=VALUES(mother_facility_registered), ',
      'exit_date=VALUES(exit_date), exit_reason=VALUES(exit_reason), hiv_status_at_exit=VALUES(hiv_status_at_exit), birth_type=VALUES(birth_type);'
  );

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  SELECT "Completed processing HEI Enrollments", CONCAT("Time: ", NOW());
END $$
DELIMITER ;

-- ------------- update etl_hei_follow_up_visit-------------------------

DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_hei_follow_up $$
CREATE PROCEDURE sp_populate_etl_hei_follow_up()
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_hei_follow_up_visit`');

  SELECT "Processing HEI Followup visits", CONCAT("Time: ", NOW());

  SET @sql = CONCAT(
    'INSERT INTO ', target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, weight, height, muac, primary_caregiver, ',
      'revisit_this_year, height_length, referred, referral_reason, danger_signs, infant_feeding, stunted, tb_assessment_outcome, ',
      'social_smile_milestone, head_control_milestone, response_to_sound_milestone, hand_extension_milestone, sitting_milestone, ',
      'walking_milestone, standing_milestone, talking_milestone, review_of_systems_developmental, weight_category, followup_type, ',
      'dna_pcr_sample_date, dna_pcr_contextual_status, dna_pcr_result, azt_given, nvp_given, ctx_given, multi_vitamin_given, ',
      'first_antibody_result, final_antibody_result, tetracycline_ointment_given, pupil_examination, sight_examination, squint, ',
      'deworming_drug, dosage, unit, vitaminA_given, disability, referred_from, referred_to, counselled_on, MNPS_Supplementation, ',
      'LLIN, comments, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id=5089,o.value_numeric,NULL)) AS weight, ',
      'MAX(IF(o.concept_id=5090,o.value_numeric,NULL)) AS height, ',
      'MAX(IF(o.concept_id=160908,o.value_coded,NULL)) AS muac, ',
      'MAX(IF(o.concept_id=160640,o.value_coded,NULL)) AS primary_caregiver, ',
      'MAX(IF(o.concept_id=164142,o.value_coded,NULL)) AS revisit_this_year, ',
      'MAX(IF(o.concept_id=164088,o.value_coded,NULL)) AS height_length, ',
      'MAX(IF(o.concept_id=1788,o.value_coded,NULL)) AS referred, ',
      'MAX(IF(o.concept_id=164359,o.value_text,NULL)) AS referral_reason, ',
      'MAX(IF(o.concept_id=159860,o.value_coded,NULL)) AS danger_signs, ',
      'MAX(IF(o.concept_id=1151,o.value_coded,NULL)) AS infant_feeding, ',
      'MAX(IF(o.concept_id=164088,o.value_coded,NULL)) AS stunted, ',
      'MAX(IF(o.concept_id=1659,o.value_coded,NULL)) AS tb_assessment_outcome, ',
      'MAX(IF(o.concept_id=162069 AND o.value_coded=162056,o.value_coded,NULL)) AS social_smile_milestone, ',
      'MAX(IF(o.concept_id=162069 AND o.value_coded=162057,o.value_coded,NULL)) AS head_control_milestone, ',
      'MAX(IF(o.concept_id=162069 AND o.value_coded=162058,o.value_coded,NULL)) AS response_to_sound_milestone, ',
      'MAX(IF(o.concept_id=162069 AND o.value_coded=162059,o.value_coded,NULL)) AS hand_extension_milestone, ',
      'MAX(IF(o.concept_id=162069 AND o.value_coded=162061,o.value_coded,NULL)) AS sitting_milestone, ',
      'MAX(IF(o.concept_id=162069 AND o.value_coded=162063,o.value_coded,NULL)) AS walking_milestone, ',
      'MAX(IF(o.concept_id=162069 AND o.value_coded=162062,o.value_coded,NULL)) AS standing_milestone, ',
      'MAX(IF(o.concept_id=162069 AND o.value_coded=162060,o.value_coded,NULL)) AS talking_milestone, ',
      'MAX(IF(o.concept_id=1189,o.value_coded,NULL)) AS review_of_systems_developmental, ',
      'MAX(IF(o.concept_id=1854,o.value_coded,NULL)) AS weight_category, ',
      'MAX(IF(o.concept_id=159402,o.value_coded,NULL)) AS followup_type, ',
      'MAX(IF(o.concept_id=159951,o.value_datetime,NULL)) AS dna_pcr_sample_date, ',
      'MAX(IF(o.concept_id=162084,o.value_coded,NULL)) AS dna_pcr_contextual_status, ',
      'MAX(IF(o.concept_id=1030,o.value_coded,NULL)) AS dna_pcr_result, ',
      'MAX(IF(o.concept_id=966 AND o.value_coded=86663,o.value_coded,NULL)) AS azt_given, ',
      'MAX(IF(o.concept_id=966 AND o.value_coded=80586,o.value_coded,NULL)) AS nvp_given, ',
      'MAX(IF(o.concept_id=1109,o.value_coded,NULL)) AS ctx_given, ',
      'MAX(IF(o.concept_id=1193,o.value_coded,NULL)) AS multi_vitamin_given, ',
      'MAX(IF(o.concept_id=1040,o.value_coded,NULL)) AS first_antibody_result, ',
      'MAX(IF(o.concept_id=1326,o.value_coded,NULL)) AS final_antibody_result, ',
      'MAX(IF(o.concept_id=162077,o.value_coded,NULL)) AS tetracycline_ointment_given, ',
      'MAX(IF(o.concept_id=162064,o.value_coded,NULL)) AS pupil_examination, ',
      'MAX(IF(o.concept_id=162067,o.value_coded,NULL)) AS sight_examination, ',
      'MAX(IF(o.concept_id=162066,o.value_coded,NULL)) AS squint, ',
      'MAX(IF(o.concept_id=1282,o.value_coded,NULL)) AS deworming_drug, ',
      'MAX(IF(o.concept_id=1443,o.value_numeric,NULL)) AS dosage, ',
      'MAX(IF(o.concept_id=1621,o.value_text,NULL)) AS unit, ',
      'MAX(IF(o.concept_id=161534,o.value_coded,NULL)) AS vitaminA_given, ',
      'MAX(IF(o.concept_id=162558,o.value_coded,NULL)) AS disability, ',
      'MAX(IF(o.concept_id=163145,o.value_coded,NULL)) AS referred_from, ',
      'MAX(IF(o.concept_id=160481,o.value_coded,NULL)) AS referred_to, ',
      'MAX(IF(o.concept_id=1379,o.value_coded,NULL)) AS counselled_on, ',
      'MAX(IF(o.concept_id=5484,o.value_coded,NULL)) AS MNPS_Supplementation, ',
      'MAX(IF(o.concept_id=159855,o.value_coded,NULL)) AS LLIN, ',
      'MAX(IF(o.concept_id=159395,o.value_text,NULL)) AS comments, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (5089,5090,160908,160640,164142,164088,1788,164359,159860,1151,1659,162069,1189,1854,159402,159951,162084,1030,966,1109,1193,1040,1326,162077,162064,162067,162066,1282,1443,1621,161534,162558,163145,160481,1379,5484,159855,159395) ',
    'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''bcc6da85-72f2-4291-b206-789b8186a021'',''c6d09e05-1f25-4164-8860-9f32c5a02df0'')) et ON et.encounter_type_id = e.encounter_type ',
    'WHERE e.voided = 0 ',
    'GROUP BY e.patient_id, DATE(e.encounter_datetime);'
  );

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing HEI Followup visits", CONCAT("Time: ", NOW());
END $$
DELIMITER ;


-- ------------- update etl_hei_immunization-------------------------

DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_hei_immunization $$
CREATE PROCEDURE sp_populate_etl_hei_immunization()
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_immunization`');

  SELECT "Processing hei_immunization data ", CONCAT("Time: ", NOW());

  SET @sql = CONCAT(
    'INSERT INTO ', target_table, ' (',
      'patient_id, visit_date, created_by, date_created, date_last_modified, encounter_id, ',
      'BCG, OPV_birth, OPV_1, OPV_2, OPV_3, IPV, DPT_Hep_B_Hib_1, DPT_Hep_B_Hib_2, DPT_Hep_B_Hib_3, ',
      'PCV_10_1, PCV_10_2, PCV_10_3, ROTA_1, ROTA_2, ROTA_3, Measles_rubella_1, Measles_rubella_2, ',
      'Yellow_fever, Measles_6_months, VitaminA_6_months, VitaminA_1_yr, VitaminA_1_and_half_yr, ',
      'VitaminA_2_yr, VitaminA_2_to_5_yr, HPV_1, HPV_2, HPV_3, Influenza, Sequence, fully_immunized',
    ') ',
    'SELECT ',
      'patient_id, visit_date, y.creator, y.date_created, y.date_last_modified, y.encounter_id, ',
      'MAX(IF(vaccine=''', 'BCG', ''', date_given, '''')) AS BCG, ',
      'MAX(IF(vaccine=''', 'OPV', ''' AND sequence=0, date_given, '''')) AS OPV_birth, ',
      'MAX(IF(vaccine=''', 'OPV', ''' AND sequence=1, date_given, '''')) AS OPV_1, ',
      'MAX(IF(vaccine=''', 'OPV', ''' AND sequence=2, date_given, '''')) AS OPV_2, ',
      'MAX(IF(vaccine=''', 'OPV', ''' AND sequence=3, date_given, '''')) AS OPV_3, ',
      'MAX(IF(vaccine=''', 'IPV', ''' , date_given, '''')) AS IPV, ',
      'MAX(IF(vaccine=''', 'DPT', ''' AND sequence=1, date_given, '''')) AS DPT_Hep_B_Hib_1, ',
      'MAX(IF(vaccine=''', 'DPT', ''' AND sequence=2, date_given, '''')) AS DPT_Hep_B_Hib_2, ',
      'MAX(IF(vaccine=''', 'DPT', ''' AND sequence=3, date_given, '''')) AS DPT_Hep_B_Hib_3, ',
      'MAX(IF(vaccine=''', 'PCV', ''' AND sequence=1, date_given, '''')) AS PCV_10_1, ',
      'MAX(IF(vaccine=''', 'PCV', ''' AND sequence=2, date_given, '''')) AS PCV_10_2, ',
      'MAX(IF(vaccine=''', 'PCV', ''' AND sequence=3, date_given, '''')) AS PCV_10_3, ',
      'MAX(IF(vaccine=''', 'ROTA', ''' AND sequence=1, date_given, '''')) AS ROTA_1, ',
      'MAX(IF(vaccine=''', 'ROTA', ''' AND sequence=2, date_given, '''')) AS ROTA_2, ',
      'MAX(IF(vaccine=''', 'ROTA', ''' AND sequence=3, date_given, '''')) AS ROTA_3, ',
      'MAX(IF(vaccine=''', 'measles_rubella', ''' AND sequence=1, date_given, '''')) AS Measles_rubella_1, ',
      'MAX(IF(vaccine=''', 'measles_rubella', ''' AND sequence=2, date_given, '''')) AS Measles_rubella_2, ',
      'MAX(IF(vaccine=''', 'yellow_fever', ''' , date_given, '''')) AS Yellow_fever, ',
      'MAX(IF(vaccine=''', 'measles', ''' , date_given, '''')) AS Measles_6_months, ',
      'MAX(IF(vaccine=''', 'Vitamin A', ''' AND sequence=1, date_given, '''')) AS VitaminA_6_months, ',
      'MAX(IF(vaccine=''', 'Vitamin A', ''' AND sequence=2, date_given, '''')) AS VitaminA_1_yr, ',
      'MAX(IF(vaccine=''', 'Vitamin A', ''' AND sequence=3, date_given, '''')) AS VitaminA_1_and_half_yr, ',
      'MAX(IF(vaccine=''', 'Vitamin A', ''' AND sequence=4, date_given, '''')) AS VitaminA_2_yr, ',
      'MAX(IF(vaccine=''', 'Vitamin A', ''' AND sequence=5, date_given, '''')) AS VitaminA_2_to_5_yr, ',
      'MAX(IF(vaccine=''', 'HPV', ''' AND sequence=1, date_given, '''')) AS HPV_1, ',
      'MAX(IF(vaccine=''', 'HPV', ''' AND sequence=2, date_given, '''')) AS HPV_2, ',
      'MAX(IF(vaccine=''', 'HPV', ''' AND sequence=3, date_given, '''')) AS HPV_3, ',
      'MAX(IF(vaccine=''', 'HEMOPHILUS INFLUENZA B', ''' , date_given, '''')) AS influenza, ',
      'y.sequence AS Sequence, y.fully_immunized AS fully_immunized ',
    'FROM (',
      ' (',
        '  SELECT ',
        '    person_id AS patient_id, ',
        '    DATE(encounter_datetime) AS visit_date, ',
        '    creator, ',
        '    DATE(date_created) AS date_created, ',
        '    date_last_modified, ',
        '    encounter_id, ',
        '    name AS encounter_type, ',
        '    MAX(IF(concept_id=1282, ''Vitamin A'', '''')) AS vaccine, ',
        '    MAX(IF(concept_id=1418, value_numeric, '''')) AS sequence, ',
        '    MAX(IF(concept_id=1282, DATE(obs_datetime), NULL)) AS date_given, ',
        '    MAX(IF(concept_id=164134, value_coded, '''')) AS fully_immunized, ',
        '    obs_group_id ',
        '  FROM (',
        '    SELECT o.person_id, e.encounter_datetime, e.creator, e.date_created, ',
        '           IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
        '           o.concept_id, o.value_coded, o.value_numeric, DATE(o.value_datetime) AS date_given, ',
        '           o.obs_group_id, o.encounter_id, et.uuid, et.name, o.obs_datetime ',
        '    FROM obs o ',
        '    INNER JOIN encounter e ON e.encounter_id = o.encounter_id ',
        '    INNER JOIN person p ON p.person_id = o.person_id AND p.voided = 0 ',
        '    INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''82169b8d-c945-4c41-be62-433dfd9d6c86'',''29c02aff-9a93-46c9-bf6f-48b552fcb1fa'')) et ',
        '      ON et.encounter_type_id = e.encounter_type ',
        '    WHERE concept_id IN (1282,1418,164134) AND o.voided = 0 ',
        '    GROUP BY obs_group_id, concept_id',
        '  ) t ',
        '  GROUP BY IFNULL(obs_group_id,1) ',
        '  HAVING (vaccine != '''' OR fully_immunized != '''') ',
      ' )',
      ' UNION ',
      ' (',
        '  SELECT ',
        '    person_id AS patient_id, ',
        '    DATE(encounter_datetime) AS visit_date, ',
        '    creator, ',
        '    DATE(date_created) AS date_created, ',
        '    date_last_modified, ',
        '    encounter_id, ',
        '    name AS encounter_type, ',
        '    MAX(IF(concept_id=984, CASE ',
        '         WHEN value_coded=886 THEN ''BCG'' ',
        '         WHEN value_coded=783 THEN ''OPV'' ',
        '         WHEN value_coded=1422 THEN ''IPV'' ',
        '         WHEN value_coded=781 THEN ''DPT'' ',
        '         WHEN value_coded=162342 THEN ''PCV'' ',
        '         WHEN value_coded=83531 THEN ''ROTA'' ',
        '         WHEN value_coded=162586 THEN ''measles_rubella'' ',
        '         WHEN value_coded=5864 THEN ''yellow_fever'' ',
        '         WHEN value_coded=36 THEN ''measles'' ',
        '         WHEN value_coded=84879 THEN ''TETANUS TOXOID'' ',
        '         WHEN value_coded=5261 THEN ''HEMOPHILUS INFLUENZA B'' ',
        '         WHEN value_coded=159708 THEN ''HPV'' ',
        '       END, '''')) AS vaccine, ',
        '    MAX(IF(concept_id=1418, value_numeric, '''')) AS sequence, ',
        '    MAX(IF(concept_id=1410, date_given, '''')) AS date_given, ',
        '    MAX(IF(concept_id=164134, value_coded, '''')) AS fully_immunized, ',
        '    obs_group_id ',
        '  FROM (',
        '    SELECT o.person_id, e.encounter_datetime, e.creator, e.date_created, ',
        '           IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
        '           o.concept_id, o.value_coded, o.value_numeric, DATE(o.value_datetime) AS date_given, ',
        '           o.obs_group_id, o.encounter_id, et.uuid, et.name ',
        '    FROM obs o ',
        '    INNER JOIN encounter e ON e.encounter_id = o.encounter_id ',
        '    INNER JOIN person p ON p.person_id = o.person_id AND p.voided = 0 ',
        '    INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''82169b8d-c945-4c41-be62-433dfd9d6c86'',''29c02aff-9a93-46c9-bf6f-48b552fcb1fa'')) et ',
        '      ON et.encounter_type_id = e.encounter_type ',
        '    WHERE concept_id IN (984,1418,1410,164134) AND o.voided = 0 ',
        '    GROUP BY obs_group_id, concept_id ',
        '  ) t ',
        '  GROUP BY IFNULL(obs_group_id,1) ',
        '  HAVING (vaccine != '''' OR fully_immunized != '''') ',
      ' )',
    ') y ',
    'LEFT JOIN obs o ON y.encounter_id = o.encounter_id AND o.voided = 0 ',
    'GROUP BY patient_id;'
  );

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  SELECT "Completed processing hei_immunization data ", CONCAT("Time: ", NOW());
END $$
DELIMITER ;

-- ------------- update etl_tb_enrollment-------------------------

-- sql
DROP PROCEDURE IF EXISTS sp_update_etl_tb_enrollment $$
CREATE PROCEDURE sp_update_etl_tb_enrollment(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_tb_enrollment`');

  SELECT "Processing TB Enrollments ", CONCAT("Time: ", NOW());

  SET @sql = CONCAT(
    'INSERT INTO ', target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, ',
      'date_treatment_started, district, referred_by, referral_date, date_transferred_in, ',
      'facility_transferred_from, district_transferred_from, date_first_enrolled_in_tb_care, ',
      'weight, height, treatment_supporter, relation_to_patient, treatment_supporter_address, ',
      'treatment_supporter_phone_contact, disease_classification, patient_classification, ',
      'pulmonary_smear_result, has_extra_pulmonary_pleurial_effusion, has_extra_pulmonary_milliary, ',
      'has_extra_pulmonary_lymph_node, has_extra_pulmonary_menengitis, has_extra_pulmonary_skeleton, ',
      'has_extra_pulmonary_abdominal, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id=1113, o.value_datetime, NULL)) AS date_treatment_started, ',
      'MAX(IF(o.concept_id=161564, LEFT(TRIM(o.value_text),100), NULL)) AS district, ',
      'MAX(IF(o.concept_id=160540, o.value_coded, NULL)) AS referred_by, ',
      'MAX(IF(o.concept_id=161561, o.value_datetime, NULL)) AS referral_date, ',
      'MAX(IF(o.concept_id=160534, o.value_datetime, NULL)) AS date_transferred_in, ',
      'MAX(IF(o.concept_id=160535, LEFT(TRIM(o.value_text),100), NULL)) AS facility_transferred_from, ',
      'MAX(IF(o.concept_id=161551, LEFT(TRIM(o.value_text),100), NULL)) AS district_transferred_from, ',
      'MAX(IF(o.concept_id=161552, o.value_datetime, NULL)) AS date_first_enrolled_in_tb_care, ',
      'MAX(IF(o.concept_id=5089, o.value_numeric, NULL)) AS weight, ',
      'MAX(IF(o.concept_id=5090, o.value_numeric, NULL)) AS height, ',
      'MAX(IF(o.concept_id=160638, LEFT(TRIM(o.value_text),100), NULL)) AS treatment_supporter, ',
      'MAX(IF(o.concept_id=160640, o.value_coded, NULL)) AS relation_to_patient, ',
      'MAX(IF(o.concept_id=160641, LEFT(TRIM(o.value_text),100), NULL)) AS treatment_supporter_address, ',
      'MAX(IF(o.concept_id=160642, LEFT(TRIM(o.value_text),100), NULL)) AS treatment_supporter_phone_contact, ',
      'MAX(IF(o.concept_id=160040, o.value_coded, NULL)) AS disease_classification, ',
      'MAX(IF(o.concept_id=159871, o.value_coded, NULL)) AS patient_classification, ',
      'MAX(IF(o.concept_id=159982, o.value_coded, NULL)) AS pulmonary_smear_result, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=130059, o.value_coded, NULL)) AS has_extra_pulmonary_pleurial_effusion, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=115753, o.value_coded, NULL)) AS has_extra_pulmonary_milliary, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=111953, o.value_coded, NULL)) AS has_extra_pulmonary_lymph_node, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=111967, o.value_coded, NULL)) AS has_extra_pulmonary_menengitis, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=112116, o.value_coded, NULL)) AS has_extra_pulmonary_skeleton, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=1350, o.value_coded, NULL)) AS has_extra_pulmonary_abdominal, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'LEFT JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
    'AND o.concept_id IN (160540,161561,160534,160535,161551,161552,5089,5090,160638,160640,160641,160642,160040,159871,159982,161356) ',
    'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''9d8498a4-372d-4dc4-a809-513a2434621e'')) et ',
    'ON et.encounter_type_id = e.encounter_type ',
    'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider),visit_id=VALUES(visit_id),visit_date=VALUES(visit_date),encounter_id=VALUES(encounter_id),',
      'date_treatment_started=VALUES(date_treatment_started),district=VALUES(district),referred_by=VALUES(referred_by),referral_date=VALUES(referral_date),',
      'date_transferred_in=VALUES(date_transferred_in),facility_transferred_from=VALUES(facility_transferred_from),',
      'district_transferred_from=VALUES(district_transferred_from),date_first_enrolled_in_tb_care=VALUES(date_first_enrolled_in_tb_care),',
      'weight=VALUES(weight),height=VALUES(height),treatment_supporter=VALUES(treatment_supporter),relation_to_patient=VALUES(relation_to_patient),',
      'treatment_supporter_address=VALUES(treatment_supporter_address),treatment_supporter_phone_contact=VALUES(treatment_supporter_phone_contact),',
      'disease_classification=VALUES(disease_classification),patient_classification=VALUES(patient_classification),',
      'pulmonary_smear_result=VALUES(pulmonary_smear_result),has_extra_pulmonary_pleurial_effusion=VALUES(has_extra_pulmonary_pleurial_effusion),',
      'has_extra_pulmonary_milliary=VALUES(has_extra_pulmonary_milliary),has_extra_pulmonary_lymph_node=VALUES(has_extra_pulmonary_lymph_node),',
      'has_extra_pulmonary_menengitis=VALUES(has_extra_pulmonary_menengitis),has_extra_pulmonary_skeleton=VALUES(has_extra_pulmonary_skeleton),',
      'has_extra_pulmonary_abdominal=VALUES(has_extra_pulmonary_abdominal);'
  );

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing TB Enrollments ", CONCAT("Time: ", NOW());
END $$

-- ------------- update etl_tb_follow_up_visit-------------------------

-- sql
DROP PROCEDURE IF EXISTS sp_update_etl_tb_follow_up_visit $$
CREATE PROCEDURE sp_update_etl_tb_follow_up_visit(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_tb_follow_up_visit`');

  SELECT "Processing TB Followup visits ", CONCAT("Time: ", NOW());

  SET @sql = CONCAT(
    'INSERT INTO ', target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, ',
      'spatum_test, spatum_result, result_serial_number, quantity, date_test_done, ',
      'bacterial_colonie_growth, number_of_colonies, resistant_s, resistant_r, resistant_inh, resistant_e, ',
      'sensitive_s, sensitive_r, sensitive_inh, sensitive_e, test_date, hiv_status, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id=159961,o.value_coded,NULL)) AS spatum_test, ',
      'MAX(IF(o.concept_id=307,o.value_coded,NULL)) AS spatum_result, ',
      'MAX(IF(o.concept_id=159968,o.value_numeric,NULL)) AS result_serial_number, ',
      'MAX(IF(o.concept_id=160023,o.value_numeric,NULL)) AS quantity, ',
      'MAX(IF(o.concept_id=159964,o.value_datetime,NULL)) AS date_test_done, ',
      'MAX(IF(o.concept_id=159982,o.value_coded,NULL)) AS bacterial_colonie_growth, ',
      'MAX(IF(o.concept_id=159952,o.value_numeric,NULL)) AS number_of_colonies, ',
      'MAX(IF(o.concept_id=159956 AND o.value_coded=84360,o.value_numeric,NULL)) AS resistant_s, ',
      'MAX(IF(o.concept_id=159956 AND o.value_coded=767,TRIM(o.value_text),NULL)) AS resistant_r, ',
      'MAX(IF(o.concept_id=159956 AND o.value_coded=78280,o.value_coded,NULL)) AS resistant_inh, ',
      'MAX(IF(o.concept_id=159956 AND o.value_coded=75948,TRIM(o.value_text),NULL)) AS resistant_e, ',
      'MAX(IF(o.concept_id=159958 AND o.value_coded=84360,TRIM(o.value_text),NULL)) AS sensitive_s, ',
      'MAX(IF(o.concept_id=159958 AND o.value_coded=767,o.value_coded,NULL)) AS sensitive_r, ',
      'MAX(IF(o.concept_id=159958 AND o.value_coded=78280,o.value_coded,NULL)) AS sensitive_inh, ',
      'MAX(IF(o.concept_id=159958 AND o.value_coded=75948,o.value_coded,NULL)) AS sensitive_e, ',
      'MAX(IF(o.concept_id=159964,o.value_datetime,NULL)) AS test_date, ',
      'MAX(IF(o.concept_id=1169,o.value_coded,NULL)) AS hiv_status, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
    'AND o.concept_id IN (159961,307,159968,160023,159964,159982,159952,159956,159958,1169,5096) ',
    'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''fbf0bfce-e9f4-45bb-935a-59195d8a0e35'')) et ON et.encounter_type_id = e.encounter_type ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider),visit_id=VALUES(visit_id),visit_date=VALUES(visit_date),encounter_id=VALUES(encounter_id),',
      'spatum_test=VALUES(spatum_test),spatum_result=VALUES(spatum_result),result_serial_number=VALUES(result_serial_number),quantity=VALUES(quantity),',
      'date_test_done=VALUES(date_test_done),bacterial_colonie_growth=VALUES(bacterial_colonie_growth),number_of_colonies=VALUES(number_of_colonies),',
      'resistant_s=VALUES(resistant_s),resistant_r=VALUES(resistant_r),resistant_inh=VALUES(resistant_inh),resistant_e=VALUES(resistant_e),',
      'sensitive_s=VALUES(sensitive_s),sensitive_r=VALUES(sensitive_r),sensitive_inh=VALUES(sensitive_inh),sensitive_e=VALUES(sensitive_e),',
      'test_date=VALUES(test_date),hiv_status=VALUES(hiv_status);'
  );

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing TB Followup visits ", CONCAT("Time: ", NOW());
END $$


-- ------------- update etl_tb_screening-------------------------

-- sql
DROP PROCEDURE IF EXISTS sp_update_etl_tb_screening $$
CREATE PROCEDURE sp_update_etl_tb_screening(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_tb_screening`');

  SELECT "Processing TB Screening data ", CONCAT("Time: ", NOW());

  SET sql_stmt = CONCAT(
    'INSERT INTO ', target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, encounter_id, location_id, ',
      'cough_for_2wks_or_more, confirmed_tb_contact, fever_for_2wks_or_more, noticeable_weight_loss, ',
      'night_sweat_for_2wks_or_more, lethargy, spatum_smear_ordered, chest_xray_ordered, genexpert_ordered, ',
      'spatum_smear_result, chest_xray_result, genexpert_result, referral, clinical_tb_diagnosis, resulting_tb_status, ',
      'contact_invitation, evaluated_for_ipt, started_anti_TB, tb_treatment_start_date, tb_prophylaxis, notes, ',
      'person_present, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.encounter_id, e.location_id, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded = 159799, o.value_coded, NULL)) AS cough_for_2wks_or_more, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded = 124068, o.value_coded, NULL)) AS confirmed_tb_contact, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded = 1494, o.value_coded, NULL)) AS fever_for_2wks_or_more, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded = 832, o.value_coded, NULL)) AS noticeable_weight_loss, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded = 133027, o.value_coded, NULL)) AS night_sweat_for_2wks_or_more, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded = 116334, o.value_coded, NULL)) AS lethargy, ',
      'MAX(IF(o.concept_id=1271 AND o.value_coded = 307, o.value_coded, NULL)) AS spatum_smear_ordered, ',
      'MAX(IF(o.concept_id=1271 AND o.value_coded = 12, o.value_coded, NULL)) AS chest_xray_ordered, ',
      'MAX(IF(o.concept_id=1271 AND o.value_coded = 162202, o.value_coded, NULL)) AS genexpert_ordered, ',
      'MAX(IF(o.concept_id=307, o.value_coded, NULL)) AS spatum_smear_result, ',
      'MAX(IF(o.concept_id=12, o.value_coded, NULL)) AS chest_xray_result, ',
      'MAX(IF(o.concept_id=162202, o.value_coded, NULL)) AS genexpert_result, ',
      'MAX(IF(o.concept_id=1272, o.value_coded, NULL)) AS referral, ',
      'MAX(IF(o.concept_id=163752, o.value_coded, NULL)) AS clinical_tb_diagnosis, ',
      'MAX(IF(o.concept_id=1659, o.value_coded, NULL)) AS resulting_tb_status, ',
      'MAX(IF(o.concept_id=163414, o.value_coded, NULL)) AS contact_invitation, ',
      'MAX(IF(o.concept_id=162275, o.value_coded, NULL)) AS evaluated_for_ipt, ',
      'MAX(IF(o.concept_id=162309, o.value_coded, NULL)) AS started_anti_TB, ',
      'MAX(IF(o.concept_id=1113, DATE(o.value_datetime), NULL)) AS tb_treatment_start_date, ',
      'MAX(IF(o.concept_id=1109, o.value_coded, NULL)) AS tb_prophylaxis, ',
      'MAX(IF(o.concept_id=160632, o.value_text, NULL)) AS notes, ',
      'MAX(IF(o.concept_id=161643, o.value_coded, NULL)) AS person_present, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (',
      '''22c68f86-bbf0-49ba-b2d1-23fa7ccf0259'', ',
      '''59ed8e62-7f1f-40ae-a2e3-eabe350277ce'', ',
      '''23b4ebbd-29ad-455e-be0e-04aa6bc30798'', ',
      '''72aa78e0-ee4b-47c3-9073-26f3b9ecc4a7''',
    ') ',
    'INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (1659,1113,160632,161643,1729,1271,307,12,162202,1272,163752,163414,162275,162309,1109) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.patient_id, visit_date ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider), visit_id=VALUES(visit_id), visit_date=VALUES(visit_date), encounter_id=VALUES(encounter_id), ',
      'cough_for_2wks_or_more=VALUES(cough_for_2wks_or_more), confirmed_tb_contact=VALUES(confirmed_tb_contact), ',
      'fever_for_2wks_or_more=VALUES(fever_for_2wks_or_more), noticeable_weight_loss=VALUES(noticeable_weight_loss), ',
      'night_sweat_for_2wks_or_more=VALUES(night_sweat_for_2wks_or_more), lethargy=VALUES(lethargy), ',
      'spatum_smear_ordered=VALUES(spatum_smear_ordered), chest_xray_ordered=VALUES(chest_xray_ordered), genexpert_ordered=VALUES(genexpert_ordered), ',
      'spatum_smear_result=VALUES(spatum_smear_result), chest_xray_result=VALUES(chest_xray_result), genexpert_result=VALUES(genexpert_result), ',
      'referral=VALUES(referral), clinical_tb_diagnosis=VALUES(clinical_tb_diagnosis), resulting_tb_status=VALUES(resulting_tb_status), ',
      'contact_invitation=VALUES(contact_invitation), evaluated_for_ipt=VALUES(evaluated_for_ipt), started_anti_TB=VALUES(started_anti_TB), ',
      'tb_treatment_start_date=VALUES(tb_treatment_start_date), tb_prophylaxis=VALUES(tb_prophylaxis), notes=VALUES(notes);'
  );

  PREPARE stmt FROM sql_stmt;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing TB Screening data ", CONCAT("Time: ", NOW());
END $$






















DROP PROCEDURE IF EXISTS sp_scheduled_updates $$
CREATE PROCEDURE sp_scheduled_updates()
BEGIN
    DECLARE update_script_id INT(11) DEFAULT NULL;
    DECLARE last_update_time DATETIME;
    DECLARE sql_stmt TEXT;

CALL sp_set_tenant_session_vars();

-- determine last successful update time from tenant script status
SET sql_stmt = CONCAT('SELECT MAX(start_time) INTO @last_update_time FROM ', @script_status_table, ' WHERE stop_time IS NOT NULL;');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET last_update_time = @last_update_time;

    -- record start
    SET sql_stmt = CONCAT('INSERT INTO ', @script_status_table, ' (script_name, start_time) VALUES (''scheduled_updates'', NOW());');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET update_script_id = LAST_INSERT_ID();

    -- run updates
CALL sp_update_etl_patient_demographics(last_update_time);
CALL sp_update_etl_hiv_enrollment(last_update_time);
CALL sp_update_etl_hiv_followup(last_update_time);
CALL sp_update_etl_program_discontinuation(last_update_time);
CALL sp_update_etl_mch_enrollment(last_update_time);
CALL sp_update_etl_mch_antenatal_visit(last_update_time);
CALL sp_update_etl_mch_postnatal_visit(last_update_time);
CALL sp_update_etl_generalized_anxiety_disorder(last_update_time);
CALL sp_update_etl_tb_enrollment(last_update_time);
CALL sp_update_etl_tb_follow_up_visit(last_update_time);
CALL sp_update_etl_tb_screening(last_update_time);
CALL sp_update_etl_hei_enrolment(last_update_time);
CALL sp_update_etl_hei_immunization(last_update_time);
CALL sp_update_etl_hei_follow_up(last_update_time);
CALL sp_update_etl_mch_discharge(last_update_time);
CALL sp_update_etl_mch_delivery(last_update_time);
CALL sp_update_drug_event(last_update_time);
CALL sp_update_etl_pharmacy_extract(last_update_time);
CALL sp_update_etl_laboratory_extract(last_update_time);
CALL sp_update_hts_test(last_update_time);
CALL sp_update_hts_linkage_and_referral(last_update_time);
CALL sp_update_hts_referral(last_update_time);
CALL sp_update_etl_ipt_initiation(last_update_time);
CALL sp_update_etl_ipt_outcome(last_update_time);
CALL sp_update_etl_ipt_follow_up(last_update_time);
CALL sp_update_etl_ccc_defaulter_tracing(last_update_time);
CALL sp_update_etl_ART_preparation(last_update_time);
CALL sp_update_etl_enhanced_adherence(last_update_time);
CALL sp_update_etl_patient_triage(last_update_time);
CALL sp_update_etl_prep_enrolment(last_update_time);
CALL sp_update_etl_prep_behaviour_risk_assessment(last_update_time);
CALL sp_update_etl_prep_monthly_refill(last_update_time);
CALL sp_update_etl_prep_followup(last_update_time);
CALL sp_update_etl_progress_note(last_update_time);
CALL sp_update_etl_prep_discontinuation(last_update_time);
CALL sp_update_etl_hts_linkage_tracing(last_update_time);
CALL sp_update_etl_patient_program(last_update_time);
CALL sp_update_etl_person_address(last_update_time);
CALL sp_update_etl_otz_enrollment(last_update_time);
CALL sp_update_etl_otz_activity(last_update_time);
CALL sp_update_etl_ovc_enrolment(last_update_time);
CALL sp_update_etl_cervical_cancer_screening(last_update_time);
CALL sp_update_etl_patient_contact(last_update_time);
CALL sp_update_etl_kp_contact(last_update_time);
CALL sp_update_etl_kp_client_enrollment(last_update_time);
CALL sp_update_etl_kp_clinical_visit(last_update_time);
CALL sp_update_etl_kp_sti_treatment(last_update_time);
CALL sp_update_etl_kp_peer_calendar(last_update_time);
CALL sp_update_etl_kp_peer_tracking(last_update_time);
CALL sp_update_etl_kp_treatment_verification(last_update_time);
-- CALL sp_update_etl_gender_based_violence(last_update_time);
CALL sp_update_etl_PrEP_verification(last_update_time);
CALL sp_update_etl_alcohol_drug_abuse_screening(last_update_time);
CALL sp_update_etl_gbv_screening(last_update_time);
CALL sp_update_etl_violence_reporting(last_update_time);
CALL sp_update_etl_link_facility_tracking(last_update_time);
CALL sp_update_etl_depression_screening(last_update_time);
CALL sp_update_etl_adverse_events(last_update_time);
CALL sp_update_etl_allergy_chronic_illness(last_update_time);
CALL sp_update_etl_ipt_screening(last_update_time);
CALL sp_update_etl_pre_hiv_enrollment_art(last_update_time);
CALL sp_update_etl_covid_19_assessment(last_update_time);
CALL sp_update_etl_vmmc_enrolment(last_update_time);
CALL sp_update_etl_vmmc_circumcision_procedure(last_update_time);
CALL sp_update_etl_vmmc_client_followup(last_update_time);
CALL sp_update_etl_vmmc_post_operation_assessment(last_update_time);
CALL sp_update_etl_hts_eligibility_screening(last_update_time);
CALL sp_update_etl_drug_order(last_update_time);
CALL sp_update_etl_preventive_services(last_update_time);
CALL sp_update_etl_overdose_reporting(last_update_time);
CALL sp_update_etl_art_fast_track(last_update_time);
CALL sp_update_etl_clinical_encounter(last_update_time);
CALL sp_update_etl_pep_management_survivor(last_update_time);
CALL sp_update_etl_sgbv_pep_followup(last_update_time);
CALL sp_update_etl_sgbv_post_rape_care(last_update_time);
CALL sp_update_etl_gbv_physical_emotional_abuse(last_update_time);
CALL sp_update_etl_family_planning(last_update_time);
CALL sp_update_etl_physiotherapy(last_update_time);
CALL sp_update_etl_psychiatry(last_update_time);
-- CALL sp_update_etl_special_clinics(last_update_time);
CALL sp_update_etl_kvp_clinical_enrollment(last_update_time);
CALL sp_update_etl_high_iit_intervention(last_update_time);
CALL sp_update_etl_home_visit_checklist(last_update_time);
CALL sp_update_etl_patient_appointments(last_update_time);
CALL sp_update_next_appointment_dates(last_update_time);
CALL sp_update_etl_adr_assessment_tool(last_update_time);
CALL sp_update_etl_ncd_enrollment(last_update_time);
CALL sp_update_etl_ncd_followup(last_update_time);
CALL sp_update_etl_inpatient_admission(last_update_time);
CALL sp_update_etl_inpatient_discharge(last_update_time);
CALL sp_update_dashboard_table();

-- finalize
SET sql_stmt = CONCAT('UPDATE ', @script_status_table, ' SET stop_time = NOW() WHERE id = ', update_script_id, ';');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT('DELETE FROM ', @script_status_table, ' WHERE script_name IN (''KenyaEMR_Data_Tool'', ''scheduled_updates'') AND start_time < DATE_SUB(NOW(), INTERVAL 12 HOUR);');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT update_script_id;
END $$

SET SQL_MODE=@OLD_SQL_MODE $$
DELIMITER ;
