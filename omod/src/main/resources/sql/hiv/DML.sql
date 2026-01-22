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
    SET @script_status_table = CONCAT(@etl_schema, '.etl_script_status');
    SET @script_status_table_quoted = CONCAT('`', @etl_schema, '`.`etl_script_status`');

    -- ensure tenant ETL database exists
    SET sql_stmt = CONCAT('CREATE DATABASE IF NOT EXISTS `', @etl_schema, '` DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- ensure etl_script_status table exists in the tenant ETL schema (quoted)
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

DROP PROCEDURE IF EXISTS sp_populate_etl_patient_demographics $$
CREATE PROCEDURE sp_populate_etl_patient_demographics()
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET target_table = CONCAT('`', @etl_schema, '`.`etl_patient_demographics`');

SELECT "Processing patient demographics data ", CONCAT("Time: ", NOW());

SET sql_stmt = CONCAT(
      'INSERT INTO ', target_table, ' (',
        'patient_id, uuid, given_name, middle_name, family_name, Gender, DOB, dead, date_created, date_last_modified, voided, death_date',
      ') ',
      'SELECT ',
        'p.person_id, p.uuid, p.given_name, p.middle_name, p.family_name, p.gender, p.birthdate, p.dead, p.date_created, ',
        'IF((p.date_last_modified = ''0000-00-00 00:00:00'' OR p.date_last_modified = p.date_created), NULL, p.date_last_modified) AS date_last_modified, p.voided, p.death_date ',
      'FROM (',
         'SELECT p.person_id, p.uuid, pn.given_name, pn.middle_name, pn.family_name, p.gender, p.birthdate, p.dead, p.date_created, ',
         'GREATEST(IFNULL(p.date_changed, ''0000-00-00 00:00:00''), IFNULL(pn.date_changed, ''0000-00-00 00:00:00'')) AS date_last_modified, p.voided, p.death_date ',
         'FROM person p ',
         'LEFT JOIN patient pa ON pa.patient_id = p.person_id ',
         'LEFT JOIN person_name pn ON pn.person_id = p.person_id AND pn.voided = 0 ',
         'WHERE p.voided = 0 ',
         'GROUP BY p.person_id',
      ') p ',
      'ON DUPLICATE KEY UPDATE given_name = p.given_name, middle_name = p.middle_name, family_name = p.family_name;'
    );
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT(
      'UPDATE ', target_table, ' d ',
      'LEFT JOIN (',
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
         'AND pat.uuid IN (',
           '''8d8718c2-c2cc-11de-8d13-0010c6dffd0f'', ''8d871afc-c2cc-11de-8d13-0010c6dffd0f'', ''8d871d18-c2cc-11de-8d13-0010c6dffd0f'', ',
           '''b2c38640-2603-4629-aebd-3b54f33f1e3a'', ''342a1d39-c541-4b29-8818-930916f4c2dc'', ''d0aa9fd1-2ac5-45d8-9c5e-4317c622c8f5'', ',
           '''7cf22bec-d90a-46ad-9f48-035952261294'', ''830bef6d-b01f-449d-9f8d-ac0fede8dbd3'', ''b8d0b331-1d2d-4a9a-b741-1816f498bdb6'', ',
           '''848f5688-41c6-464c-b078-ea6524a3e971'', ''96a99acd-2f11-45bb-89f7-648dbcac5ddf'', ''9f1f8254-20ea-4be4-a14d-19201fe217bf''',
         ') ',
         'WHERE pa.voided = 0 ',
         'GROUP BY pa.person_id',
      ') att ON att.person_id = d.patient_id ',
      'SET ',
        'd.phone_number = att.phone_number, ',
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

SET sql_stmt = CONCAT(
      'UPDATE ', target_table, ' d ',
      'JOIN (',
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
         'WHERE pi.voided = 0 ',
         'GROUP BY pi.patient_id',
      ') pid ON pid.patient_id = d.patient_id ',
      'SET ',
        'd.unique_patient_no = pid.upn, ',
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

SET sql_stmt = CONCAT(
      'UPDATE ', target_table, ' d ',
      'JOIN (',
         'SELECT o.person_id AS patient_id, ',
           'MAX(IF(o.concept_id IN (1054), cn.name, NULL)) AS marital_status, ',
           'MAX(IF(o.concept_id IN (1712), cn.name, NULL)) AS education_level, ',
           'MAX(IF(o.concept_id IN (1542), cn.name, NULL)) AS occupation, ',
           'MAX(o.date_created) AS date_created ',
         'FROM obs o ',
         'JOIN concept_name cn ON cn.concept_id = o.value_coded AND cn.concept_name_type = ''FULLY_SPECIFIED'' AND cn.locale = ''en'' ',
         'WHERE o.concept_id IN (1054,1712,1542) AND o.voided = 0 ',
         'GROUP BY person_id',
      ') pstatus ON pstatus.patient_id = d.patient_id ',
      'SET ',
        'd.marital_status = pstatus.marital_status, ',
        'd.education_level = pstatus.education_level, ',
        'd.occupation = pstatus.occupation, ',
        'd.date_last_modified = IF(pstatus.date_created > IFNULL(d.date_last_modified, ''0000-00-00''), pstatus.date_created, d.date_last_modified);'
    );
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END $$

DROP PROCEDURE IF EXISTS sp_populate_etl_hiv_enrollment $$
CREATE PROCEDURE sp_populate_etl_hiv_enrollment()
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET target_table = CONCAT('`', @etl_schema, '`.`etl_hiv_enrollment`');

SELECT "Processing HIV Enrollment data ", CONCAT("Time: ", NOW());

SET sql_stmt = CONCAT(
      'INSERT INTO ', target_table, ' (',
      'patient_id, uuid, visit_id, visit_date, location_id, encounter_id, encounter_provider, ',
      'date_created, date_last_modified, patient_type, date_first_enrolled_in_care, entry_point, ',
      'transfer_in_date, facility_transferred_from, district_transferred_from, previous_regimen, ',
      'date_started_art_at_transferring_facility, date_confirmed_hiv_positive, facility_confirmed_hiv_positive, ',
      'arv_status, ever_on_pmtct, ever_on_pep, ever_on_prep, ever_on_haart, cd4_test_result, ',
      'cd4_test_date, viral_load_test_result, viral_load_test_date, who_stage, name_of_treatment_supporter, ',
      'relationship_of_treatment_supporter, treatment_supporter_telephone, treatment_supporter_address, ',
      'in_school, orphan, date_of_discontinuation, discontinuation_reason, voided',
      ') ',
      'SELECT ',
      'e.patient_id, e.uuid, e.visit_id, e.encounter_datetime AS visit_date, e.location_id, e.encounter_id, e.creator, ',
      'e.date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id IN (164932), o.value_coded, IF(o.concept_id=160563 AND o.value_coded=1065, 160563, NULL))) AS patient_type, ',
      'MAX(IF(o.concept_id=160555, o.value_datetime, NULL)) AS date_first_enrolled_in_care, ',
      'MAX(IF(o.concept_id=160540, o.value_coded, NULL)) AS entry_point, ',
      'MAX(IF(o.concept_id=160534, o.value_datetime, NULL)) AS transfer_in_date, ',
      'MAX(IF(o.concept_id=160535, LEFT(TRIM(o.value_text),100), NULL)) AS facility_transferred_from, ',
      'MAX(IF(o.concept_id=161551, LEFT(TRIM(o.value_text),100), NULL)) AS district_transferred_from, ',
      'MAX(IF(o.concept_id=164855, o.value_coded, NULL)) AS previous_regimen, ',
      'MAX(IF(o.concept_id=159599, o.value_datetime, NULL)) AS date_started_art_at_transferring_facility, ',
      'MAX(IF(o.concept_id=160554, o.value_datetime, NULL)) AS date_confirmed_hiv_positive, ',
      'MAX(IF(o.concept_id=160632, LEFT(TRIM(o.value_text),100), NULL)) AS facility_confirmed_hiv_positive, ',
      'MAX(IF(o.concept_id=160533, o.value_coded, NULL)) AS arv_status, ',
      'MAX(IF(o.concept_id=1148, o.value_coded, NULL)) AS ever_on_pmtct, ',
      'MAX(IF(o.concept_id=1691, o.value_coded, NULL)) AS ever_on_pep, ',
      'MAX(IF(o.concept_id=165269, o.value_coded, NULL)) AS ever_on_prep, ',
      'MAX(IF(o.concept_id=1181, o.value_coded, NULL)) AS ever_on_haart, ',
      'MAX(IF(o.concept_id=5497, o.value_numeric, NULL)) AS cd4_test_result, ',
      'MAX(IF(o.concept_id=159376, o.value_datetime, NULL)) AS cd4_test_date, ',
      'MAX(IF(o.concept_id=1305 AND o.value_coded=1302, ''LDL'', IF(o.concept_id=162086, o.value_text, NULL))) AS viral_load_test_result, ',
      'MAX(IF(o.concept_id=163281, o.value_datetime, NULL)) AS viral_load_test_date, ',
      'MAX(IF(o.concept_id=5356, o.value_coded, NULL)) AS who_stage, ',
      'MAX(IF(o.concept_id=160638, LEFT(TRIM(o.value_text),100), NULL)) AS name_of_treatment_supporter, ',
      'MAX(IF(o.concept_id=160640, o.value_coded, NULL)) AS relationship_of_treatment_supporter, ',
      'MAX(IF(o.concept_id=160642, LEFT(TRIM(o.value_text),100), NULL)) AS treatment_supporter_telephone, ',
      'MAX(IF(o.concept_id=160641, LEFT(TRIM(o.value_text),100), NULL)) AS treatment_supporter_address, ',
      'MAX(IF(o.concept_id=5629, o.value_coded, NULL)) AS in_school, ',
      'MAX(IF(o.concept_id=1174, o.value_coded, NULL)) AS orphan, ',
      'MAX(IF(o.concept_id=164384, o.value_datetime, NULL)) AS date_of_discontinuation, ',
      'MAX(IF(o.concept_id=161555, o.value_coded, NULL)) AS discontinuation_reason, ',
      'e.voided ',
      'FROM encounter e ',
      'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid = ''de78a6be-bfc5-4634-adc3-5f1a280455cc'') et ON et.encounter_type_id = e.encounter_type ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (160555,160540,160534,160535,161551,159599,160554,160632,160533,160638,160640,160642,160641,164932,160563,5629,1174,1088,161555,164855,164384,1148,1691,165269,1181,5356,5497,159376,1305,162086,163281) ',
      'WHERE e.voided = 0 ',
      'GROUP BY e.patient_id, e.encounter_id;'
    );
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT "Completed processing HIV Enrollment data ", CONCAT("Time: ", NOW());
END $$


DROP PROCEDURE IF EXISTS sp_populate_etl_hiv_followup $$
CREATE PROCEDURE sp_populate_etl_hiv_followup()
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET target_table = CONCAT('`', @etl_schema, '`.`etl_patient_hiv_followup`');

SELECT "Processing HIV Followup data ", CONCAT("Time: ", NOW());

SET @sql = CONCAT('INSERT INTO ', target_table, ' (',
      'uuid, encounter_id, patient_id, location_id, visit_date, visit_id, encounter_provider, date_created, date_last_modified, ',
      'visit_scheduled, person_present, weight, systolic_pressure, diastolic_pressure, height, temperature, pulse_rate, ',
      'respiratory_rate, oxygen_saturation, muac, z_score_absolute, z_score, nutritional_status, population_type, key_population_type, ',
      'who_stage, who_stage_associated_oi, presenting_complaints, clinical_notes, on_anti_tb_drugs, on_ipt, ever_on_ipt, cough, fever, ',
      'weight_loss_poor_gain, night_sweats, tb_case_contact, lethargy, screened_for_tb, spatum_smear_ordered, chest_xray_ordered, ',
      'genexpert_ordered, spatum_smear_result, chest_xray_result, genexpert_result, referral, clinical_tb_diagnosis, contact_invitation, ',
      'evaluated_for_ipt, has_known_allergies, has_chronic_illnesses_cormobidities, has_adverse_drug_reaction, substitution_first_line_regimen_date, ',
      'substitution_first_line_regimen_reason, substitution_second_line_regimen_date, substitution_second_line_regimen_reason, ',
      'second_line_regimen_change_date, second_line_regimen_change_reason, pregnancy_status, breastfeeding, wants_pregnancy, pregnancy_outcome, ',
      'anc_number, expected_delivery_date, ever_had_menses, last_menstrual_period, menopausal, gravida, parity, full_term_pregnancies, ',
      'abortion_miscarriages, family_planning_status, family_planning_method, reason_not_using_family_planning, tb_status, started_anti_TB, ',
      'tb_rx_date, tb_treatment_no, general_examination, system_examination, skin_findings, eyes_findings, ent_findings, chest_findings, ',
      'cvs_findings, abdomen_findings, cns_findings, genitourinary_findings, prophylaxis_given, ctx_adherence, ctx_dispensed, dapsone_adherence, ',
      'dapsone_dispensed, inh_dispensed, arv_adherence, poor_arv_adherence_reason, poor_arv_adherence_reason_other, pwp_disclosure, ',
      'pwp_pead_disclosure, pwp_partner_tested, condom_provided, substance_abuse_screening, screened_for_sti, cacx_screening, sti_partner_notification, ',
      'experienced_gbv, depression_screening, at_risk_population, system_review_finding, next_appointment_date, refill_date, appointment_consent, ',
      'next_appointment_reason, stability, differentiated_care_group, differentiated_care, established_differentiated_care, insurance_type, ',
      'other_insurance_specify, insurance_status, voided',
    ') ');
SET @sql = CONCAT(@sql,
'SELECT ',
'e.uuid, ',
'e.encounter_id, ',
'e.patient_id, ',
'e.location_id, ',
'date(e.encounter_datetime) as visit_date, ',
'e.visit_id, ',
'e.creator, ',
'e.date_created, ',
'if(max(o.date_created) > min(e.date_created), max(o.date_created), NULL) as date_last_modified, ',
'max(if(o.concept_id=1246,o.value_coded,NULL)) as visit_scheduled, ',
'max(if(o.concept_id=161643,o.value_coded,NULL)) as person_present, ',
'max(if(o.concept_id=5089,o.value_numeric,NULL)) as weight, ',
'max(if(o.concept_id=5085,o.value_numeric,NULL)) as systolic_pressure, ',
'max(if(o.concept_id=5086,o.value_numeric,NULL)) as diastolic_pressure, ',
'max(if(o.concept_id=5090,o.value_numeric,NULL)) as height, ',
'max(if(o.concept_id=5088,o.value_numeric,NULL)) as temperature, ',
'max(if(o.concept_id=5087,o.value_numeric,NULL)) as pulse_rate, ',
'max(if(o.concept_id=5242,o.value_numeric,NULL)) as respiratory_rate, ',
'max(if(o.concept_id=5092,o.value_numeric,NULL)) as oxygen_saturation, ',
'max(if(o.concept_id=1343,o.value_numeric,NULL)) as muac, ',
'max(if(o.concept_id=162584,o.value_numeric,NULL)) as z_score_absolute, ',
'max(if(o.concept_id=163515,o.value_coded,NULL)) as z_score, ',
'max(if(o.concept_id=163515,o.value_coded,NULL)) as nutritional_status, ',
'max(if(o.concept_id=164930,o.value_coded,NULL)) as population_type, ',
'max(if(o.concept_id=160581,o.value_coded,NULL)) as key_population_type, ',
'max(if(o.concept_id=5356,o.value_coded,NULL)) as who_stage, ',
'CONCAT_WS('','', ',
'  MAX(IF(o.concept_id=167394 AND o.value_coded=5006, ''Asymptomatic'', NULL)), ',
'  MAX(IF(o.concept_id=167394 AND o.value_coded=130364, ''PGL'', NULL)), ',
'  MAX(IF(o.concept_id=167394 AND o.value_coded=143744, ''Acquired recto-vesicular fistula'', NULL)) ',
') AS who_stage_associated_oi, ',
'max(if(o.concept_id=1154,o.value_coded,NULL)) as presenting_complaints, ',
'NULL as clinical_notes, ',
'max(if(o.concept_id=164948,o.value_coded,NULL)) as on_anti_tb_drugs, ',
'max(if(o.concept_id=164949,o.value_coded,NULL)) as on_ipt, ',
'max(if(o.concept_id=164950,o.value_coded,NULL)) as ever_on_ipt, ',
'max(if(o.concept_id=1729 and o.value_coded=159799,o.value_coded,NULL)) as cough, ',
'max(if(o.concept_id=1729 and o.value_coded=1494,o.value_coded,NULL)) as fever, ',
'max(if(o.concept_id=1729 and o.value_coded=832,o.value_coded,NULL)) as weight_loss_poor_gain, ',
'max(if(o.concept_id=1729 and o.value_coded=133027,o.value_coded,NULL)) as night_sweats, ',
'max(if(o.concept_id=1729 and o.value_coded=124068,o.value_coded,NULL)) as tb_case_contact, ',
'max(if(o.concept_id=1729 and o.value_coded=116334,o.value_coded,NULL)) as lethargy, ',
'max(if(o.concept_id=1729 and o.value_coded IN (159799,1494,832,133027,124068,116334,1066), ''Yes'', ''No'')) as screened_for_tb, ',
'max(if(o.concept_id=1271 and o.value_coded=307,o.value_coded,NULL)) as spatum_smear_ordered, ',
'max(if(o.concept_id=1271 and o.value_coded=12,o.value_coded,NULL)) as chest_xray_ordered, ',
'max(if(o.concept_id=1271 and o.value_coded=162202,o.value_coded,NULL)) as genexpert_ordered, ',
'max(if(o.concept_id=307,o.value_coded,NULL)) as spatum_smear_result, ',
'max(if(o.concept_id=12,o.value_coded,NULL)) as chest_xray_result, ',
'max(if(o.concept_id=162202,o.value_coded,NULL)) as genexpert_result, ',
'max(if(o.concept_id=1272,o.value_coded,NULL)) as referral, ',
'max(if(o.concept_id=163752,o.value_coded,NULL)) as clinical_tb_diagnosis, ',
'max(if(o.concept_id=163414,o.value_coded,NULL)) as contact_invitation, ',
'max(if(o.concept_id=162275,o.value_coded,NULL)) as evaluated_for_ipt, ',
'max(if(o.concept_id=160557,o.value_coded,NULL)) as has_known_allergies, ',
'max(if(o.concept_id=162747,o.value_coded,NULL)) as has_chronic_illnesses_cormobidities, ',
'max(if(o.concept_id=121764,o.value_coded,NULL)) as has_adverse_drug_reaction, ',
'NULL, NULL, NULL, NULL, NULL, NULL, ',
'max(if(o.concept_id=5272,o.value_coded,NULL)) as pregnancy_status, ',
'max(if(o.concept_id=5632,o.value_coded,NULL)) as breastfeeding, ',
'max(if(o.concept_id=164933,o.value_coded,NULL)) as wants_pregnancy, ',
'max(if(o.concept_id=161033,o.value_coded,NULL)) as pregnancy_outcome, ',
'max(if(o.concept_id=163530,o.value_text,NULL)) as anc_number, ',
'max(if(o.concept_id=5596,date(o.value_datetime),NULL)) as expected_delivery_date, ',
'max(if(o.concept_id=162877,o.value_coded,NULL)) as ever_had_menses, ',
'max(if(o.concept_id=1427,date(o.value_datetime),NULL)) as last_menstrual_period, ',
'max(if(o.concept_id=160596,o.value_coded,NULL)) as menopausal, ',
'max(if(o.concept_id=5624,o.value_numeric,NULL)) as gravida, ',
'max(if(o.concept_id=1053,o.value_numeric,NULL)) as parity, ',
'max(if(o.concept_id=160080,o.value_numeric,NULL)) as full_term_pregnancies, ',
'max(if(o.concept_id=1823,o.value_numeric,NULL)) as abortion_miscarriages, ',
'max(if(o.concept_id=160653,o.value_coded,NULL)) as family_planning_status, ',
'max(if(o.concept_id=374,o.value_coded,NULL)) as family_planning_method, ',
'max(if(o.concept_id=160575,o.value_coded,NULL)) as reason_not_using_family_planning, ',
'max(if(o.concept_id=1659,o.value_coded,NULL)) as tb_status, ',
'max(if(o.concept_id=162309,o.value_coded,NULL)) as started_anti_TB, ',
'max(if(o.concept_id=1113,o.value_datetime,NULL)) as tb_rx_date, ',
'max(if(o.concept_id=161654,trim(o.value_text),NULL)) as tb_treatment_no, ',
'CONCAT_WS('','', ',
'  MAX(IF(o.concept_id=162737 AND o.value_coded=1107, ''None'', NULL)), ',
'  MAX(IF(o.concept_id=162737 AND o.value_coded=136443, ''Jaundice'', NULL)), ',
'  MAX(IF(o.concept_id=162737 AND o.value_coded=460, ''Oedema'', NULL)), ',
'  MAX(IF(o.concept_id=162737 AND o.value_coded=5334, ''Oral Thrush'', NULL)), ',
'  MAX(IF(o.concept_id=162737 AND o.value_coded=5245, ''Pallor'', NULL)), ',
'  MAX(IF(o.concept_id=162737 AND o.value_coded=140125, ''Finger Clubbing'', NULL)), ',
'  MAX(IF(o.concept_id=162737 AND o.value_coded=126952, ''Lymph Node Axillary'', NULL)), ',
'  MAX(IF(o.concept_id=162737 AND o.value_coded=143050, ''Cyanosis'', NULL)), ',
'  MAX(IF(o.concept_id=162737 AND o.value_coded=126939, ''Lymph Nodes Inguinal'', NULL)), ',
'  MAX(IF(o.concept_id=162737 AND o.value_coded=823, ''Wasting'', NULL)), ',
'  MAX(IF(o.concept_id=162737 AND o.value_coded=142630, ''Dehydration'', NULL)), ',
'  MAX(IF(o.concept_id=162737 AND o.value_coded=116334, ''Lethargic'', NULL)) ',
') AS general_examination, ',
'max(if(o.concept_id=159615,o.value_coded,NULL)) as system_examination, ',
'max(if(o.concept_id=1120,o.value_coded,NULL)) as skin_findings, ',
'max(if(o.concept_id=163309,o.value_coded,NULL)) as eyes_findings, ',
'max(if(o.concept_id=164936,o.value_coded,NULL)) as ent_findings, ',
'max(if(o.concept_id=1123,o.value_coded,NULL)) as chest_findings, ',
'max(if(o.concept_id=1124,o.value_coded,NULL)) as cvs_findings, ',
'max(if(o.concept_id=1125,o.value_coded,NULL)) as abdomen_findings, ',
'max(if(o.concept_id=164937,o.value_coded,NULL)) as cns_findings, ',
'max(if(o.concept_id=1126,o.value_coded,NULL)) as genitourinary_findings, ',
'max(if(o.concept_id=1109,o.value_coded,NULL)) as prophylaxis_given, ',
'max(if(o.concept_id=161652,o.value_coded,NULL)) as ctx_adherence, ',
'max(if(o.concept_id=162229 OR (o.concept_id=1282 AND o.value_coded=105281),o.value_coded,NULL)) as ctx_dispensed, ',
'max(if(o.concept_id=164941,o.value_coded,NULL)) as dapsone_adherence, ',
'max(if(o.concept_id=164940 OR (o.concept_id=1282 AND o.value_coded=74250),o.value_coded,NULL)) as dapsone_dispensed, ',
'max(if(o.concept_id=162230,o.value_coded,NULL)) as inh_dispensed, ',
'max(if(o.concept_id=1658,o.value_coded,NULL)) as arv_adherence, ',
'max(if(o.concept_id=160582,o.value_coded,NULL)) as poor_arv_adherence_reason, ',
'NULL as poor_arv_adherence_reason_other, ',
'max(if(o.concept_id=159423,o.value_coded,NULL)) as pwp_disclosure, ',
'max(if(o.concept_id=5616,o.value_coded,NULL)) as pwp_pead_disclosure, ',
'max(if(o.concept_id=161557,o.value_coded,NULL)) as pwp_partner_tested, ',
'max(if(o.concept_id=159777,o.value_coded,NULL)) as condom_provided, ',
'max(if(o.concept_id=112603,o.value_coded,NULL)) as substance_abuse_screening, ',
'max(if(o.concept_id=161558,o.value_coded,NULL)) as screened_for_sti, ',
'max(if(o.concept_id=164934,o.value_coded,NULL)) as cacx_screening, ',
'max(if(o.concept_id=164935,o.value_coded,NULL)) as sti_partner_notification, ',
'max(if(o.concept_id=167161,o.value_coded,NULL)) as experienced_gbv, ',
'max(if(o.concept_id=165086,o.value_coded,NULL)) as depression_screening, ',
'max(if(o.concept_id=160581,o.value_coded,NULL)) as at_risk_population, ',
'max(if(o.concept_id=159615,o.value_coded,NULL)) as system_review_finding, ',
'max(if(o.concept_id=5096, date(o.value_datetime), NULL)) as next_appointment_date, ',
'NULL as refill_date, ',
'max(if(o.concept_id=166607,o.value_coded,NULL)) as appointment_consent, ',
'max(if(o.concept_id=160288,o.value_coded,NULL)) as next_appointment_reason, ',
'max(if(o.concept_id=1855,o.value_coded,NULL)) as stability, ',
'max(if(o.concept_id=164947,o.value_coded,NULL)) as differentiated_care_group, ',
'max(if(o.concept_id IN (164946,165287),o.value_coded,NULL)) as differentiated_care, ',
'max(if(o.concept_id=164946 OR o.concept_id=165287,o.value_coded,NULL)) as established_differentiated_care, ',
'max(if(o.concept_id=159356,o.value_coded,NULL)) as insurance_type, ',
'max(if(o.concept_id=161011,o.value_text,NULL)) as other_insurance_specify, ',
'max(if(o.concept_id=165911,o.value_coded,NULL)) as insurance_status, ',
'e.voided ',
'from encounter e ',
'inner join person p on p.person_id=e.patient_id and p.voided=0 ',
'inner join form f on f.form_id = e.form_id and f.uuid in (''22c68f86-bbf0-49ba-b2d1-23fa7ccf0259'',''23b4ebbd-29ad-455e-be0e-04aa6bc30798'',''465a92f2-baf8-42e9-9612-53064be868e8'') ',
'left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0 ',
'where e.voided=0 ',
'group by e.encounter_id;'
);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SELECT "Completed processing HIV Followup data ", CONCAT("Time: ", NOW());
END $$


-- ------------- populate etl_laboratory_extract  uuid:  --------------------------------


-- sql
DROP PROCEDURE IF EXISTS sp_populate_etl_laboratory_extract $$
CREATE PROCEDURE sp_populate_etl_laboratory_extract()
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table VARCHAR(300);

    CALL sp_set_tenant_session_vars();
    SET target_table = CONCAT('`', @etl_schema, '`.`etl_laboratory_extract`');

    SELECT "Processing Laboratory data ", CONCAT("Time: ", NOW());

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
        'GROUP BY patient_id, encounter_id, concept_id',
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
        'SELECT o.obs_id AS obs_id, o.order_id, o.concept_id, o.value_numeric, n.name, n1.name AS test_name ',
        'FROM obs o ',
        'INNER JOIN concept c ON o.concept_id = c.concept_id ',
        'INNER JOIN concept_datatype cd ON c.datatype_id = cd.concept_datatype_id AND cd.name = ''Numeric'' ',
        'INNER JOIN concept_name n ON o.concept_id = n.concept_id AND n.locale = ''en'' AND n.concept_name_type = ''FULLY_SPECIFIED'' ',
        'LEFT JOIN concept_name n1 ON o.concept_id = n1.concept_id AND n1.locale = ''en'' AND n1.concept_name_type = ''FULLY_SPECIFIED'' ',
        'WHERE o.order_id IS NOT NULL',
      '), ',
      'TextLabOrderResults AS (',
        'SELECT o.obs_id AS obs_id, o.order_id, o.concept_id, o.value_text, c.class_id, n.name, n1.name AS test_name ',
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
      'WHERE e.voided = 0;'
    );

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SELECT "Completed processing Laboratory data ", CONCAT("Time: ", NOW());
END $$



-- ------------- populate etl_pharmacy_extract table--------------------------------

DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_pharmacy_extract $$
CREATE PROCEDURE sp_populate_etl_pharmacy_extract()
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table VARCHAR(300);

    CALL sp_set_tenant_session_vars();
    SET target_table = CONCAT('`', @etl_schema, '`.`etl_pharmacy_extract`');

    SELECT "Processing Pharmacy data ", CONCAT("Time: ", NOW());

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
        'MAX(IF(o.concept_id = 1444, o.value_numeric, NULL)) AS frequency, ',
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
      'GROUP BY o.obs_group_id, o.person_id, o.encounter_id ',
      'HAVING drug IS NOT NULL AND obs_group_id IS NOT NULL;'
    );

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- compute duration_in_days in tenant table
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

    SELECT "Completed processing Pharmacy data ", CONCAT("Time: ", NOW());
END $$
DELIMITER ;

-- ------------ create table etl_patient_treatment_event----------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_program_discontinuation $$
CREATE PROCEDURE sp_populate_etl_program_discontinuation()
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_patient_program_discontinuation`');

  SELECT "Processing Program HIV, TB, MCH,TPT,OTZ,OVC ... discontinuations", CONCAT("Time: ", NOW());

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
      'q.date_died, COALESCE(l.`name`, q.to_facility_raw) AS to_facility_name, q.to_date, q.death_reason, q.specific_death_cause, ',
      'q.natural_causes, q.non_natural_cause, q.date_created, q.date_last_modified ',
    'FROM (',
      'SELECT ',
        'e.patient_id, e.uuid, e.visit_id, e.encounter_datetime, ',
        'et.uuid AS program_uuid, ',
        'CASE et.uuid ',
          'WHEN ''2bdada65-4c72-4a48-8730-859890e25cee'' THEN ''HIV'' ',
          'WHEN ''d3e3d723-7458-4b4e-8998-408e8a551a84'' THEN ''TB'' ',
          'WHEN ''01894f88-dc73-42d4-97a3-0929118403fb'' THEN ''MCH Child HEI'' ',
          'WHEN ''5feee3f1-aa16-4513-8bd0-5d9b27ef1208'' THEN ''MCH Child'' ',
          'WHEN ''7c426cfc-3b47-4481-b55f-89860c21c7de'' THEN ''MCH Mother'' ',
          'WHEN ''bb77c683-2144-48a5-a011-66d904d776c9'' THEN ''TPT'' ',
          'WHEN ''162382b8-0464-11ea-9a9f-362b9e155667'' THEN ''OTZ'' ',
          'WHEN ''5cf00d9e-09da-11ea-8d71-362b9e155667'' THEN ''OVC'' ',
          'WHEN ''d7142400-2495-11e9-ab14-d663bd873d93'' THEN ''KP'' ',
          'WHEN ''4f02dfed-a2ec-40c2-b546-85dab5831871'' THEN ''VMMC'' ',
          'WHEN ''c4994dd7-f2b6-4c28-bdc7-8b1d9d2a6a97'' THEN ''NCD'' ',
        'END AS program_name, ',
        'e.encounter_id, ',
        'COALESCE(MAX(IF(o.concept_id=161555, o.value_coded, NULL)), MAX(IF(o.concept_id=159786, o.value_coded, NULL))) AS reason_discontinued, ',
        'COALESCE(MAX(IF(o.concept_id=164384, o.value_datetime, NULL)), MAX(IF(o.concept_id=159787, o.value_datetime, NULL))) AS effective_discontinuation_date, ',
        'MAX(IF(o.concept_id=1285, o.value_coded, NULL)) AS trf_out_verified, ',
        'MAX(IF(o.concept_id=164133, o.value_datetime, NULL)) AS trf_out_verification_date, ',
        'MAX(IF(o.concept_id=1543, o.value_datetime, NULL)) AS date_died, ',
        'MAX(IF(o.concept_id=159495, LEFT(TRIM(o.value_text),100), NULL)) AS to_facility_raw, ',
        'MAX(IF(o.concept_id=160649, o.value_datetime, NULL)) AS to_date, ',
        'MAX(IF(o.concept_id=1599, o.value_coded, NULL)) AS death_reason, ',
        'MAX(IF(o.concept_id=1748, o.value_coded, NULL)) AS specific_death_cause, ',
        'MAX(IF(o.concept_id=162580, LEFT(TRIM(o.value_text),200), NULL)) AS natural_causes, ',
        'MAX(IF(o.concept_id=160218, LEFT(TRIM(o.value_text),200), NULL)) AS non_natural_cause, ',
        'e.date_created AS date_created, ',
        'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
      'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (161555,159786,159787,164384,1543,159495,160649,1285,164133,1599,1748,162580,160218) ',
      'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (',
        '''2bdada65-4c72-4a48-8730-859890e25cee'', ''d3e3d723-7458-4b4e-8998-408e8a551a84'', ''5feee3f1-aa16-4513-8bd0-5d9b27ef1208'', ',
        '''7c426cfc-3b47-4481-b55f-89860c21c7de'', ''01894f88-dc73-42d4-97a3-0929118403fb'', ''bb77c683-2144-48a5-a011-66d904d776c9'', ',
        '''162382b8-0464-11ea-9a9f-362b9e155667'', ''5cf00d9e-09da-11ea-8d71-362b9e155667'', ''d7142400-2495-11e9-ab14-d663bd873d93'', ',
        '''4f02dfed-a2ec-40c2-b546-85dab5831871'', ''c4994dd7-f2b6-4c28-bdc7-8b1d9d2a6a97'')) et ON et.encounter_type_id = e.encounter_type ',
      'GROUP BY e.encounter_id',
    ') q ',
    'LEFT JOIN location l ON l.uuid = q.to_facility_raw;'
  );

  PREPARE stmt FROM sql_stmt;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing discontinuation data ", CONCAT("Time: ", NOW());
END $$

-- ------------- populate etl_mch_enrollment-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_mch_enrollment $$
CREATE PROCEDURE sp_populate_etl_mch_enrollment()
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_mch_enrollment`');

  SELECT "Processing MCH Enrollments ", CONCAT("Time: ", NOW());

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
      'e.patient_id, e.uuid, e.visit_id, e.encounter_datetime, e.location_id, e.encounter_id, ',
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
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (163530,163547,5624,160080,1823,160598,1427,162095,5596,300,299,160108,32,159427,160554,1436,160082,159599,164855,162724,56,1875,159734,161438,161439,161440,161441,161442,161444,161443,162106,162101,162096,161555,160478) ',
    'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''3ee036d8-7c13-4393-b5d6-036f2fe45126'')) et ON et.encounter_type_id = e.encounter_type ',
    'WHERE e.voided = 0 ',
    'GROUP BY e.encounter_id;'
  );

  PREPARE stmt FROM sql_stmt;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing MCH Enrollments ", CONCAT("Time: ", NOW());
END $$

- ------------- populate etl_mch_antenatal_visit-------------------------

-- sql
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_mch_antenatal_visit $$
CREATE PROCEDURE sp_populate_etl_mch_antenatal_visit()
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_mch_antenatal_visit`');
  SELECT "Processing MCH antenatal visits ", CONCAT("Time: ", NOW());
SET sql_stmt = CONCAT(
'INSERT INTO ', target_table, ' (',
' patient_id,
  uuid,
  visit_id,
  visit_date,
  location_id,
  encounter_id,
  provider,
  anc_visit_number,
  temperature,
  pulse_rate,
  systolic_bp,
  diastolic_bp,
  respiratory_rate,
  oxygen_saturation,
  weight,
  height,
  muac,
  hemoglobin,
  breast_exam_done,
  pallor,
  maturity,
  fundal_height,
  fetal_presentation,
  lie,
  fetal_heart_rate,
  fetal_movement,
  who_stage,
  cd4,
  vl_sample_taken,
  viral_load,
  ldl,
  arv_status,
  test_1_kit_name,
  test_1_kit_lot_no,
  test_1_kit_expiry,
  test_1_result,
  test_2_kit_name,
  test_2_kit_lot_no,
  test_2_kit_expiry,
  test_2_result,
  test_3_kit_name,
  test_3_kit_lot_no,
  test_3_kit_expiry,
  test_3_result,
  final_test_result,
  patient_given_result,
  partner_hiv_tested,
  partner_hiv_status,
  prophylaxis_given,
        haart_given,
  date_given_haart,
  baby_azt_dispensed,
  baby_nvp_dispensed,
        deworming_done_anc,
        IPT_dose_given_anc,
  TTT,
  IPT_malaria,
  iron_supplement,
  deworming,
  bed_nets,
  urine_microscopy,
  urinary_albumin,
  glucose_measurement,
  urine_ph,
  urine_gravity,
  urine_nitrite_test,
  urine_leukocyte_esterace_test,
  urinary_ketone,
  urine_bile_salt_test,
  urine_bile_pigment_test,
  urine_colour,
  urine_turbidity,
  urine_dipstick_for_blood,
  syphilis_test_status,
  syphilis_treated_status,
  bs_mps,
        diabetes_test,
        fgm_done,
        fgm_complications,
        fp_method_postpartum,
  anc_exercises,
  tb_screening,
  cacx_screening,
  cacx_screening_method,
        hepatitis_b_screening,
        hepatitis_b_treatment,
  has_other_illnes,
  counselled,
  counselled_on_birth_plans,
      counselled_on_danger_signs,
      counselled_on_family_planning,
      counselled_on_hiv,
      counselled_on_supplimental_feeding,
      counselled_on_breast_care,
      counselled_on_infant_feeding,
      counselled_on_treated_nets,
      intermittent_presumptive_treatment_given,
      intermittent_presumptive_treatment_dose,
      minimum_care_package,
      minimum_package_of_care_services,
      risk_reduction,
      partner_testing,
      sti_screening,
      condom_provision,
      prep_adherence,
      anc_visits_emphasis,
      pnc_fp_counseling,
      referral_vmmc,
      referral_dreams,
  referred_from,
  referred_to,
  clinical_notes,
  date_created,
  date_last_modified'
,') ',
'SELECT ',
  '	e.patient_id,
      e.uuid,
      e.visit_id,
      date(e.encounter_datetime) as visit_date,
      e.location_id,
      e.encounter_id,
      e.creator,
      max(if(o.concept_id=1425,o.value_numeric,null)) as anc_visit_number,
      max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
      max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
      max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_bp,
      max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_bp,
      max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
      max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
      max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
      max(if(o.concept_id=5090,o.value_numeric,null)) as height,
      max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
      max(if(o.concept_id=21,o.value_numeric,null)) as hemoglobin,
      max(if(o.concept_id=163590,o.value_coded,null)) as breast_exam_done,
      max(if(o.concept_id=5245,o.value_coded,null)) as pallor,
      max(if(o.concept_id=1438,o.value_numeric,null)) as maturity,
      max(if(o.concept_id=1439,o.value_numeric,null)) as fundal_height,
      max(if(o.concept_id=160090,o.value_coded,null)) as fetal_presentation,
      max(if(o.concept_id=162089,o.value_coded,null)) as lie,
      max(if(o.concept_id=1440,o.value_numeric,null)) as fetal_heart_rate,
      max(if(o.concept_id=162107,o.value_coded,null)) as fetal_movement,
      max(if(o.concept_id=5356,o.value_coded,null)) as who_stage,
      max(if(o.concept_id=5497,o.value_numeric,null)) as cd4,
      max(if(o.concept_id=1271,o.value_coded,null)) as vl_sample_taken,
      max(if(o.concept_id=856,o.value_numeric,null)) as viral_load,
      max(if(o.concept_id=1305,o.value_coded,null)) as ldl,
      max(if(o.concept_id=1147,o.value_coded,null)) as arv_status,
      max(if(t.test_1_result is not null, t.kit_name, null)) as test_1_kit_name,
      max(if(t.test_1_result is not null, t.lot_no, null)) as test_1_kit_lot_no,
      max(if(t.test_1_result is not null, t.expiry_date, null)) as test_1_kit_expiry,
      max(if(t.test_1_result is not null, t.test_1_result, null)) as test_1_result,
      max(if(t.test_2_result is not null, t.kit_name, null)) as test_2_kit_name,
      max(if(t.test_2_result is not null, t.lot_no, null)) as test_2_kit_lot_no,
      max(if(t.test_2_result is not null, t.expiry_date, null)) as test_2_kit_expiry,
      max(if(t.test_2_result is not null, t.test_2_result, null)) as test_2_result,
      max(if(t.test_3_result is not null, t.kit_name, null)) as test_3_kit_name,
      max(if(t.test_3_result is not null, t.lot_no, null)) as test_3_kit_lot_no,
      max(if(t.test_3_result is not null, t.expiry_date, null)) as test_3_kit_expiry,
      max(if(t.test_3_result is not null, t.test_3_result, null)) as test_3_result,
      max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" else "" end),null)) as final_test_result,
      max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_given_result,
      max(if(o.concept_id=161557,o.value_coded,null)) as partner_hiv_tested,
      max(if(o.concept_id=1436,o.value_coded,null)) as partner_hiv_status,
      max(if(o.concept_id=1109,o.value_coded,null)) as prophylaxis_given,
      max(if(o.concept_id=5576,o.value_coded,null)) as haart_given,
      max(if(o.concept_id=163784,o.value_datetime,null)) as date_given_haart,
      max(if(o.concept_id=1282 and o.value_coded = 160123,o.value_coded,null)) as baby_azt_dispensed,
      max(if(o.concept_id=1282 and o.value_coded = 80586,o.value_coded,null)) as baby_nvp_dispensed,
            max(if(o.concept_id=159922,(case o.value_coded when 1065 then "Yes" when 1066 then "No" when 1175 then "N/A" else "" end),null)) as deworming_done_anc,
            max(if(concept_id=1418, value_numeric, null)) as IPT_dose_given_anc,
      max(if(o.concept_id=984,(case o.value_coded when 84879 then "Yes" else "" end),null)) as TTT,
      max(if(o.concept_id=984,(case o.value_coded when 159610 then "Yes" else "" end),null)) as IPT_malaria,
      max(if(o.concept_id=159853 and o.value_coded = 159854, "Yes",null)) as iron_supplement,
      max(if(o.concept_id=984,(case o.value_coded when 79413 then "Yes"  else "" end),null)) as deworming,
      max(if(o.concept_id=159853 and o.value_coded = 1381, "Yes",null)) as bed_nets,
      max(if(o.concept_id=56,o.value_text,null)) as urine_microscopy,
      max(if(o.concept_id=1875,o.value_coded,null)) as urinary_albumin,
      max(if(o.concept_id=159734,o.value_coded,null)) as glucose_measurement,
      max(if(o.concept_id=161438,o.value_numeric,null)) as urine_ph,
      max(if(o.concept_id=161439,o.value_numeric,null)) as urine_gravity,
      max(if(o.concept_id=161440,o.value_coded,null)) as urine_nitrite_test,
      max(if(o.concept_id=161441,o.value_coded,null)) as urine_leukocyte_esterace_test,
      max(if(o.concept_id=161442,o.value_coded,null)) as urinary_ketone,
      max(if(o.concept_id=161444,o.value_coded,null)) as urine_bile_salt_test,
      max(if(o.concept_id=161443,o.value_coded,null)) as urine_bile_pigment_test,
      max(if(o.concept_id=162106,o.value_coded,null)) as urine_colour,
      max(if(o.concept_id=162101,o.value_coded,null)) as urine_turbidity,
      max(if(o.concept_id=162096,o.value_coded,null)) as urine_dipstick_for_blood,
      max(if(o.concept_id=299,o.value_coded,null)) as syphilis_test_status,
      max(if(o.concept_id=159918,o.value_coded,null)) as syphilis_treated_status,
      max(if(o.concept_id=32,o.value_coded,null)) as bs_mps,
      max(if(o.concept_id=119481,o.value_coded,null)) as diabetes_test,
    max(if(o.concept_id=165099,o.value_coded,null)) as fgm_done,
    max(if(o.concept_id=120198,o.value_coded,null)) as fgm_complications,
    max(if(o.concept_id=374,o.value_coded,null)) as fp_method_postpartum,
      max(if(o.concept_id=161074,o.value_coded,null)) as anc_exercises,
      max(if(o.concept_id=1659,o.value_coded,null)) as tb_screening,
      max(if(o.concept_id=164934,o.value_coded,null)) as cacx_screening,
      max(if(o.concept_id=163589,o.value_coded,null)) as cacx_screening_method,
      max(if(o.concept_id=165040,o.value_coded,null)) as hepatitis_b_screening,
    max(if(o.concept_id=166665,o.value_coded,null)) as hepatitis_b_treatment,
      max(if(o.concept_id=162747,o.value_coded,null)) as has_other_illnes,
      max(if(o.concept_id=1912,o.value_coded,null)) as counselled,
      max(if(o.concept_id=159853 and o.value_coded=159758,o.value_coded,null)) counselled_on_birth_plans,
    max(if(o.concept_id=159853 and o.value_coded=159857,o.value_coded,null)) counselled_on_danger_signs,
    max(if(o.concept_id=159853 and o.value_coded=156277,o.value_coded,null)) counselled_on_family_planning,
    max(if(o.concept_id=159853 and o.value_coded=1914,o.value_coded,null)) counselled_on_hiv,
    max(if(o.concept_id=159853 and o.value_coded=159854,o.value_coded,null)) counselled_on_supplimental_feeding,
    max(if(o.concept_id=159853 and o.value_coded=159856,o.value_coded,null)) counselled_on_breast_care,
    max(if(o.concept_id=159853 and o.value_coded=161651,o.value_coded,null)) counselled_on_infant_feeding,
    max(if(o.concept_id=159853 and o.value_coded=1381,o.value_coded,null)) counselled_on_treated_nets,
    max(if(o.concept_id=1591,o.value_coded,null)) as intermittent_presumptive_treatment_given,
    max(if(o.concept_id=1418,o.value_numeric,null)) as intermittent_presumptive_treatment_dose,
    max(if(o.concept_id in (165302,161595),o.value_coded,null)) as minimum_care_package,
            concat_ws('','',nullif(max(if(o.concept_id=1592 and o.value_coded =165275,"Risk Reduction counselling",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =161557,"HIV Testing for the Partner",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =165190,"STI Screening and treatment",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =159777,"Condom Provision",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =165203,"PrEP with emphasis on adherence",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =165475,"Emphasize importance of follow up ANC Visits",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =1382,"Postnatal FP Counselling and support",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =162223,"Referrals for VMMC Services for partner",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =165368,"Referrals for OVC/DREAMS",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =166607,"Pre appointmnet SMS",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =166486,"Tartgeted home visits",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =1167,"Psychosocial and disclosure support",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =165002,"3-monthly Enhanced ART adherence assessments optimize TLD",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =163310,"Timely viral load monitoring, early ART switches",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =167410,"Complex case reviews in MDT/Consultation with clinical mentors",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =167079,"Enhanced longitudinal Mother-Infant Pair follow up",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =166563,"Early HEI case identification",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =160116,"Bi weekly random file audits to inform quality improvement",'''')),''''),
                      nullif(max(if(o.concept_id=1592 and o.value_coded =160031,"LTFU root cause audit and return to care plan default",'''')),'''')
                ) as minimum_package_of_care_services,
    max(if(o.concept_id=1592 and o.value_coded=165275,o.value_coded,null)) risk_reduction,
    max(if(o.concept_id=1592 and o.value_coded=161557,o.value_coded,null)) partner_testing,
    max(if(o.concept_id=1592 and o.value_coded=165190,o.value_coded,null)) sti_screening,
    max(if(o.concept_id=1592 and o.value_coded=159777,o.value_coded,null)) condom_provision,
    max(if(o.concept_id=1592 and o.value_coded=165203,o.value_coded,null)) prep_adherence,
    max(if(o.concept_id=1592 and o.value_coded=165475,o.value_coded,null)) anc_visits_emphasis,
    max(if(o.concept_id=1592 and o.value_coded=1382,o.value_coded,null)) pnc_fp_counseling,
    max(if(o.concept_id=1592 and o.value_coded=162223,o.value_coded,null)) referral_vmmc,
    max(if(o.concept_id=1592 and o.value_coded=165368,o.value_coded,null)) referral_dreams,
      max(if(o.concept_id=160481,o.value_coded,null)) as referred_from,
      max(if(o.concept_id=163145,o.value_coded,null)) as referred_to,
      max(if(o.concept_id=159395,o.value_text,null)) as clinical_notes,
      e.date_created as date_created,
    if(max(o.date_created) > min(e.date_created),max(o.date_created),NULL) as date_last_modified
  from encounter e',
  '				inner join person p on p.person_id=e.patient_id and p.voided=0
      inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
                                              and o.concept_id in(1282,159922,984,1418,1425,5088,5087,5085,5086,5242,5092,5089,5090,1343,21,163590,5245,1438,1439,160090,162089,1440,162107,5356,5497,856,1305,1147,159427,164848,161557,1436,1109,5576,128256,1875,159734,161438,161439,161440,161441,161442,161444,161443,162106,162101,162096,299,159918,32,119481,165099,120198,374,161074,1659,164934,163589,165040,166665,162747,1912,160481,163145,5096,159395,163784,1271,159853,165302,1592,1591,1418,1592,161595,299)
      inner join
      (
          select form_id, uuid,name from form where
              uuid in(''e8f98494-af35-4bb8-9fc7-c409c8fed843'',''d3ea25c7-a3e8-4f57-a6a9-e802c3565a30'')
      ) f on f.form_id=e.form_id
      left join (
                               select
                                   o.person_id,
                                   o.encounter_id,
                                   o.obs_group_id,
                                   max(if(o.concept_id=1040, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 163611 then "Invalid"  else "" end),null)) as test_1_result ,
                                   max(if(o.concept_id=1326, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_2_result ,
                                   max(if(o.concept_id=1000630, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_3_result ,
                                   max(if(o.concept_id=164962, (case o.value_coded when 164960 then "Determine" when 164961 then "First Response" when 165351 then "Dual Kit" when 169126 then "One step" when 169127 then "Trinscreen" else "" end),null)) as kit_name ,
                                   max(if(o.concept_id=164964,trim(o.value_text),null)) as lot_no,
                                   max(if(o.concept_id=162502,date(o.value_datetime),null)) as expiry_date
                               from obs o
                                   inner join encounter e on e.encounter_id = o.encounter_id
                                         inner join person p on p.person_id = o.person_id and p.voided=0
                                   inner join form f on f.form_id=e.form_id and f.uuid in (''e8f98494-af35-4bb8-9fc7-c409c8fed843'')
                               where o.concept_id in (1040, 1326,1000630, 164962, 164964, 162502) and o.voided=0
                               group by e.encounter_id, o.obs_group_id
                           ) t on e.encounter_id = t.encounter_id
where e.voided=0
  group by e.patient_id,visit_date;'
);

  PREPARE stmt FROM sql_stmt;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing MCH antenatal visits ", CONCAT("Time: ", NOW());
END $$
DELIMITER ;

-- ------------- populate etl_mchs_delivery-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_mch_delivery $$
CREATE PROCEDURE sp_populate_etl_mch_delivery()
BEGIN
  CALL sp_set_tenant_session_vars();
  SELECT "Processing MCH Delivery visits", CONCAT("Time: ", NOW());
  SET @target_table = CONCAT('`', @etl_schema, '`.`etl_mchs_delivery`');
  SET @sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, date_created, date_last_modified, ',
      'number_of_anc_visits, vaginal_examination, uterotonic_given, chlohexidine_applied_on_code_stump, vitamin_K_given, ',
      'kangaroo_mother_care_given, testing_done_in_the_maternity_hiv_status, infant_provided_with_arv_prophylaxis, ',
      'mother_on_haart_during_anc, mother_started_haart_at_maternity, vdrl_rpr_results, date_of_last_menstrual_period, ',
      'estimated_date_of_delivery, reason_for_referral, admission_number, duration_of_pregnancy, mode_of_delivery, ',
      'date_of_delivery, blood_loss, condition_of_mother, delivery_outcome, apgar_score_1min, apgar_score_5min, ',
      'apgar_score_10min, resuscitation_done, place_of_delivery, delivery_assistant, counseling_on_infant_feeding, ',
      'counseling_on_exclusive_breastfeeding, counseling_on_infant_feeding_for_hiv_infected, mother_decision, ',
      'placenta_complete, maternal_death_audited, cadre, delivery_complications, coded_delivery_complications, ',
      'other_delivery_complications, duration_of_labor, baby_sex, baby_condition, teo_given, birth_weight, bf_within_one_hour, ',
      'birth_with_deformity, test_1_kit_name, test_1_kit_lot_no, test_1_kit_expiry, test_1_result, test_2_kit_name, ',
      'test_2_kit_lot_no, test_2_kit_expiry, test_2_result, test_3_kit_name, test_3_kit_lot_no, test_3_kit_expiry, ',
      'test_3_result, final_test_result, patient_given_result, partner_hiv_tested, partner_hiv_status, prophylaxis_given, ',
      'baby_azt_dispensed, baby_nvp_dispensed, clinical_notes, stimulation_done, suction_done, oxygen_given, ',
      'bag_mask_ventilation_provided, induction_done, artificial_rapture_done',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id=1590,o.value_numeric,NULL)) AS number_of_anc_visits, ',
      'MAX(IF(o.concept_id=160704,o.value_coded,NULL)) AS vaginal_examination, ',
      'MAX(IF(o.concept_id=1282 AND o.value_coded IN (81369,104590,1107),o.value_coded,NULL)) AS uterotonic_given, ',
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
      'MAX(IF(t.test_1_result IS NOT NULL, t.kit_name, NULL)) AS test_1_kit_name, ',
      'MAX(IF(t.test_1_result IS NOT NULL, t.lot_no, NULL)) AS test_1_kit_lot_no, ',
      'MAX(IF(t.test_1_result IS NOT NULL, t.expiry_date, NULL)) AS test_1_kit_expiry, ',
      'MAX(IF(t.test_1_result IS NOT NULL, t.test_1_result, NULL)) AS test_1_result, ',
      'MAX(IF(t.test_2_result IS NOT NULL, t.kit_name, NULL)) AS test_2_kit_name, ',
      'MAX(IF(t.test_2_result IS NOT NULL, t.lot_no, NULL)) AS test_2_kit_lot_no, ',
      'MAX(IF(t.test_2_result IS NOT NULL, t.expiry_date, NULL)) AS test_2_kit_expiry, ',
      'MAX(IF(t.test_2_result IS NOT NULL, t.test_2_result, NULL)) AS test_2_result, ',
      'MAX(IF(t.test_3_result IS NOT NULL, t.kit_name, NULL)) AS test_3_kit_name, ',
      'MAX(IF(t.test_3_result IS NOT NULL, t.lot_no, NULL)) AS test_3_kit_lot_no, ',
      'MAX(IF(t.test_3_result IS NOT NULL, t.expiry_date, NULL)) AS test_3_kit_expiry, ',
      'MAX(IF(t.test_3_result IS NOT NULL, t.test_3_result, NULL)) AS test_3_result, ',
      'MAX(IF(o.concept_id=159427,(CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1138 THEN ''Inconclusive'' ELSE '''' END),NULL)) AS final_test_result, ',
      'MAX(IF(o.concept_id=164848,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS patient_given_result, ',
      'MAX(IF(o.concept_id=161557,o.value_coded,NULL)) AS partner_hiv_tested, ',
      'MAX(IF(o.concept_id=1436,o.value_coded,NULL)) AS partner_hiv_status, ',
      'MAX(IF(o.concept_id=1109,o.value_coded,NULL)) AS prophylaxis_given, ',
      'MAX(IF(o.concept_id=1282 AND o.value_coded = 160123,1,0)) AS baby_azt_dispensed, ',
      'MAX(IF(o.concept_id=1282 AND o.value_coded = 80586,1,0)) AS baby_nvp_dispensed, ',
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
      'AND o.concept_id IN (162054,1590,160704,1282,159369,984,161094,1396,161930,163783,166665,299,1427,5596,164359,1789,5630,5599,161928,1856,162093,159603,159604,159605,162131,1572,1473,1379,1151,163454,1602,1573,162093,1576,120216,159616,1587,159917,1282,5916,161543,164122,159427,164848,161557,1436,1109,5576,159595,163784,159395,168751,1284,113316,165647,113602,163445,159949,1570) ',
    'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''496c7cc3-0eea-4e84-a04c-2292949e2f7f'')) f ON f.form_id = e.form_id ',
    'LEFT JOIN (',
      'SELECT o.person_id, o.encounter_id, o.obs_group_id, ',
        'MAX(IF(o.concept_id=1040, (CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 163611 THEN ''Invalid'' ELSE '''' END),NULL)) AS test_1_result, ',
        'MAX(IF(o.concept_id=1326, (CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1175 THEN ''N/A'' ELSE '''' END),NULL)) AS test_2_result, ',
        'MAX(IF(o.concept_id=1000630, (CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1175 THEN ''N/A'' ELSE '''' END),NULL)) AS test_3_result, ',
        'MAX(IF(o.concept_id=164962, (CASE o.value_coded WHEN 164960 THEN ''Determine'' WHEN 164961 THEN ''First Response'' WHEN 165351 THEN ''Dual Kit'' WHEN 169126 THEN ''One step'' WHEN 169127 THEN ''Trinscreen'' ELSE '''' END),NULL)) AS kit_name, ',
        'MAX(IF(o.concept_id=164964,TRIM(o.value_text),NULL)) AS lot_no, ',
        'MAX(IF(o.concept_id=162502,DATE(o.value_datetime),NULL)) AS expiry_date ',
      'FROM obs o ',
      'INNER JOIN encounter e ON e.encounter_id = o.encounter_id ',
      'INNER JOIN person p ON p.person_id = o.person_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''496c7cc3-0eea-4e84-a04c-2292949e2f7f'') ',
      'WHERE o.concept_id IN (1040, 1326, 1000630, 164962, 164964, 162502) AND o.voided = 0 ',
      'GROUP BY e.encounter_id, o.obs_group_id ',
    ') t ON e.encounter_id = t.encounter_id ',
    'WHERE e.voided = 0 ',
    'GROUP BY e.encounter_id;'
  );

  PREPARE stmt FROM @sql_stmt;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  SELECT "Completed processing MCH Delivery visits", CONCAT("Time: ", NOW());
END $$
DELIMITER ;

-- ------------- populate etl_mchs_discharge-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_mch_discharge $$
CREATE PROCEDURE sp_populate_etl_mch_discharge()
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_mchs_discharge`');

  SELECT "Processing MCH Discharge ", CONCAT("Time: ", NOW());

  SET sql_stmt = CONCAT(
    'INSERT INTO ', target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, date_created, date_last_modified, ',
      'counselled_on_feeding, baby_status, vitamin_A_dispensed, birth_notification_number, condition_of_mother, ',
      'discharge_date, referred_from, referred_to, clinical_notes',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_datetime, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id=161651, o.value_coded, NULL)) AS counselled_on_feeding, ',
      'MAX(IF(o.concept_id=159926, o.value_coded, NULL)) AS baby_status, ',
      'MAX(IF(o.concept_id=161534, o.value_coded, NULL)) AS vitamin_A_dispensed, ',
      'MAX(IF(o.concept_id=162051, o.value_text, NULL)) AS birth_notification_number, ',
      'MAX(IF(o.concept_id=162093, o.value_text, NULL)) AS condition_of_mother, ',
      'MAX(IF(o.concept_id=1641, o.value_datetime, NULL)) AS discharge_date, ',
      'MAX(IF(o.concept_id=160481, o.value_coded, NULL)) AS referred_from, ',
      'MAX(IF(o.concept_id=163145, o.value_coded, NULL)) AS referred_to, ',
      'MAX(IF(o.concept_id=159395, o.value_text, NULL)) AS clinical_notes ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (161651,159926,161534,162051,162093,1641,160481,163145,159395) ',
    'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''af273344-a5f9-11e8-98d0-529269fb1459'')) f ON f.form_id = e.form_id ',
    'WHERE e.voided = 0 ',
    'GROUP BY e.encounter_id;'
  );

  PREPARE stmt FROM sql_stmt;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing MCH Discharge visits", CONCAT("Time: ", NOW());
END $$
DELIMITER ;

-- ------------- populate etl_mch_postnatal_visit-------------------------
-- sql
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_mch_postnatal_visit $$
CREATE PROCEDURE sp_populate_etl_mch_postnatal_visit()
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_mch_postnatal_visit`');

  SELECT "Processing MCH postnatal visits ", CONCAT("Time: ", NOW());

  SET @sql = CONCAT(
    'INSERT INTO ', target_table, ' (',
      'patient_id, uuid, visit_id, visit_date, location_id, encounter_id, provider, pnc_register_no, pnc_visit_no, ',
      'delivery_date, mode_of_delivery, place_of_delivery, visit_timing_mother, visit_timing_baby, delivery_outcome, ',
      'temperature, pulse_rate, systolic_bp, diastolic_bp, respiratory_rate, oxygen_saturation, weight, height, muac, hemoglobin, ',
      'arv_status, general_condition, breast, cs_scar, gravid_uterus, episiotomy, lochia, counselled_on_infant_feeding, pallor, pallor_severity, ',
      'pph, mother_hiv_status, condition_of_baby, baby_feeding_method, umblical_cord, baby_immunization_started, family_planning_counseling, ',
      'other_maternal_complications, uterus_examination, uterus_cervix_examination, vaginal_examination, parametrial_examination, ',
      'external_genitalia_examination, ovarian_examination, pelvic_lymph_node_exam, test_1_kit_name, test_1_kit_lot_no, test_1_kit_expiry, ',
      'test_1_result, test_2_kit_name, test_2_kit_lot_no, test_2_kit_expiry, test_2_result, test_3_kit_name, test_3_kit_lot_no, test_3_kit_expiry, ',
      'test_3_result, final_test_result, syphilis_results, patient_given_result, couple_counselled, partner_hiv_tested, partner_hiv_status, ',
      'pnc_hiv_test_timing_mother, mother_haart_given, prophylaxis_given, infant_prophylaxis_timing, baby_azt_dispensed, baby_nvp_dispensed, ',
      'pnc_exercises, maternal_condition, iron_supplementation, fistula_screening, cacx_screening, cacx_screening_method, family_planning_status, ',
      'family_planning_method, referred_from, referred_to, referral_reason, clinical_notes, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.visit_id, e.encounter_datetime, e.location_id, e.encounter_id, e.creator, ',
      'MAX(IF(o.concept_id=1646,o.value_text,NULL)) AS pnc_register_no, ',
      'MAX(IF(o.concept_id=159893,o.value_numeric,NULL)) AS pnc_visit_no, ',
      'MAX(IF(o.concept_id=5599,o.value_datetime,NULL)) AS delivery_date, ',
      'MAX(IF(o.concept_id=5630,o.value_coded,NULL)) AS mode_of_delivery, ',
      'MAX(IF(o.concept_id=1572,o.value_coded,NULL)) AS place_of_delivery, ',
      'MAX(IF(o.concept_id=1724,o.value_coded,NULL)) AS visit_timing_mother, ',
      'MAX(IF(o.concept_id=167017,o.value_coded,NULL)) AS visit_timing_baby, ',
      'MAX(IF(o.concept_id=159949,o.value_coded,NULL)) AS delivery_outcome, ',
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
      'MAX(IF(o.concept_id=1147,o.value_coded,NULL)) AS arv_status, ',
      'MAX(IF(o.concept_id=1856,o.value_coded,NULL)) AS general_condition, ',
      'MAX(IF(o.concept_id=159780,o.value_coded,NULL)) AS breast, ',
      'MAX(IF(o.concept_id=162128,o.value_coded,NULL)) AS cs_scar, ',
      'MAX(IF(o.concept_id=162110,o.value_coded,NULL)) AS gravid_uterus, ',
      'MAX(IF(o.concept_id=159840,o.value_coded,NULL)) AS episiotomy, ',
      'MAX(IF(o.concept_id=159844,o.value_coded,NULL)) AS lochia, ',
      'MAX(IF(o.concept_id=161651,o.value_coded,NULL)) AS counselled_on_infant_feeding, ',
      'MAX(IF(o.concept_id=5245,o.value_coded,NULL)) AS pallor, ',
      'MAX(IF(o.concept_id=162642,o.value_coded,NULL)) AS pallor_severity, ',
      'MAX(IF(o.concept_id=230,o.value_coded,NULL)) AS pph, ',
      'MAX(IF(o.concept_id=1396,o.value_coded,NULL)) AS mother_hiv_status, ',
      'MAX(IF(o.concept_id=162134,o.value_coded,NULL)) AS condition_of_baby, ',
      'MAX(IF(o.concept_id=1151,o.value_coded,NULL)) AS baby_feeding_method, ',
      'MAX(IF(o.concept_id=162121,o.value_coded,NULL)) AS umblical_cord, ',
      'MAX(IF(o.concept_id=162127,o.value_coded,NULL)) AS baby_immunization_started, ',
      'MAX(IF(o.concept_id=1382,o.value_coded,NULL)) AS family_planning_counseling, ',
      'MAX(IF(o.concept_id=160632,o.value_text,NULL)) AS other_maternal_complications, ',
      'MAX(IF(o.concept_id=163742,o.value_coded,NULL)) AS uterus_examination, ',
      'MAX(IF(o.concept_id=160968,o.value_text,NULL)) AS uterus_cervix_examination, ',
      'MAX(IF(o.concept_id=160969,o.value_text,NULL)) AS vaginal_examination, ',
      'MAX(IF(o.concept_id=160970,o.value_text,NULL)) AS parametrial_examination, ',
      'MAX(IF(o.concept_id=160971,o.value_text,NULL)) AS external_genitalia_examination, ',
      'MAX(IF(o.concept_id=160975,o.value_text,NULL)) AS ovarian_examination, ',
      'MAX(IF(o.concept_id=160972,o.value_text,NULL)) AS pelvic_lymph_node_exam, ',
      'MAX(IF(t.test_1_result IS NOT NULL, t.kit_name, NULL)) AS test_1_kit_name, ',
      'MAX(IF(t.test_1_result IS NOT NULL, t.lot_no, NULL)) AS test_1_kit_lot_no, ',
      'MAX(IF(t.test_1_result IS NOT NULL, t.expiry_date, NULL)) AS test_1_kit_expiry, ',
      'MAX(IF(t.test_1_result IS NOT NULL, t.test_1_result, NULL)) AS test_1_result, ',
      'MAX(IF(t.test_2_result IS NOT NULL, t.kit_name, NULL)) AS test_2_kit_name, ',
      'MAX(IF(t.test_2_result IS NOT NULL, t.lot_no, NULL)) AS test_2_kit_lot_no, ',
      'MAX(IF(t.test_2_result IS NOT NULL, t.expiry_date, NULL)) AS test_2_kit_expiry, ',
      'MAX(IF(t.test_2_result IS NOT NULL, t.test_2_result, NULL)) AS test_2_result, ',
      'MAX(IF(t.test_3_result IS NOT NULL, t.kit_name, NULL)) AS test_3_kit_name, ',
      'MAX(IF(t.test_3_result IS NOT NULL, t.lot_no, NULL)) AS test_3_kit_lot_no, ',
      'MAX(IF(t.test_3_result IS NOT NULL, t.expiry_date, NULL)) AS test_3_kit_expiry, ',
      'MAX(IF(t.test_3_result IS NOT NULL, t.test_3_result, NULL)) AS test_3_result, ',
      'MAX(IF(o.concept_id=159427,(CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1138 THEN ''Inconclusive'' ELSE '''' END),NULL)) AS final_test_result, ',
      'MAX(IF(o.concept_id=299,o.value_coded,NULL)) AS syphilis_results, ',
      'MAX(IF(o.concept_id=164848,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS patient_given_result, ',
      'MAX(IF(o.concept_id=165070,o.value_coded,NULL)) AS couple_counselled, ',
      'MAX(IF(o.concept_id=161557,o.value_coded,NULL)) AS partner_hiv_tested, ',
      'MAX(IF(o.concept_id=1436,o.value_coded,NULL)) AS partner_hiv_status, ',
      'MAX(IF(o.concept_id=165218,o.value_coded,NULL)) AS pnc_hiv_test_timing_mother, ',
      'MAX(IF(o.concept_id=163783,o.value_coded,NULL)) AS mother_haart_given, ',
      'MAX(IF(o.concept_id=1109,o.value_coded,NULL)) AS prophylaxis_given, ',
      'MAX(IF(o.concept_id=166665,o.value_coded,NULL)) AS infant_prophylaxis_timing, ',
      'MAX(IF(o.concept_id=1282 AND o.value_coded = 160123,o.value_coded,NULL)) AS baby_azt_dispensed, ',
      'MAX(IF(o.concept_id=1282 AND o.value_coded = 80586,o.value_coded,NULL)) AS baby_nvp_dispensed, ',
      'MAX(IF(o.concept_id=161074,o.value_coded,NULL)) AS pnc_exercises, ',
      'MAX(IF(o.concept_id=160085,o.value_coded,NULL)) AS maternal_condition, ',
      'MAX(IF(o.concept_id=161004,o.value_coded,NULL)) AS iron_supplementation, ',
      'MAX(IF(o.concept_id=159921,o.value_coded,NULL)) AS fistula_screening, ',
      'MAX(IF(o.concept_id=164934,o.value_coded,NULL)) AS cacx_screening, ',
      'MAX(IF(o.concept_id=163589,o.value_coded,NULL)) AS cacx_screening_method, ',
      'MAX(IF(o.concept_id=160653,o.value_coded,NULL)) AS family_planning_status, ',
      'CONCAT_WS('','', ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =160570,''Emergency contraceptive pills'','''')),''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =780,''Oral Contraceptives Pills'','''')),''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =5279,''Injectible'','''')),''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =1359,''Implant'','''')),''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =5275,''Intrauterine Device'','''')),''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =136163,''Lactational Amenorhea Method'','''')),''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =5278,''Diaphram/Cervical Cap'','''')),''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =5277,''Fertility Awareness'','''')),''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =1472,''Tubal Ligation'','''')),''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =190,''Condoms'','''')),''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =1489,''Vasectomy'','''')),''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =162332,''Undecided'','''')),'')) AS family_planning_method, ',
      'MAX(IF(o.concept_id=160481,o.value_coded,NULL)) AS referred_from, ',
      'MAX(IF(o.concept_id=163145,o.value_coded,NULL)) AS referred_to, ',
      'MAX(IF(o.concept_id=164359,o.value_text,NULL)) AS referral_reason, ',
      'MAX(IF(o.concept_id=159395,o.value_text,NULL)) AS clinical_notes, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created),MAX(o.date_created),NULL) AS date_last_modified ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (1646,159893,5599,5630,1572,5088,5087,5085,5086,5242,5092,5089,5090,1343,21,1147,1856,159780,162128,162110,159840,159844,5245,230,1396,162134,1151,162121,162127,1382,163742,160968,160969,160970,160971,160975,160972,159427,164848,161557,1436,1109,5576,159595,163784,1282,161074,160085,161004,159921,164934,163589,160653,374,160481,163145,159395,159949,5096,161651,165070,1724,167017,163783,162642,166665,165218,160632,299,164359) ',
    'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''72aa78e0-ee4b-47c3-9073-26f3b9ecc4a7'')) f ON f.form_id = e.form_id ',
    'LEFT JOIN (',
      'SELECT o.person_id, o.encounter_id, o.obs_group_id, ',
      'MAX(IF(o.concept_id=1040, (CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 163611 THEN ''Invalid'' ELSE '''' END), NULL)) AS test_1_result, ',
      'MAX(IF(o.concept_id=1326, (CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1175 THEN ''N/A'' ELSE '''' END), NULL)) AS test_2_result, ',
      'MAX(IF(o.concept_id=1000630, (CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1175 THEN ''N/A'' ELSE '''' END), NULL)) AS test_3_result, ',
      'MAX(IF(o.concept_id=164962, (CASE o.value_coded WHEN 164960 THEN ''Determine'' WHEN 164961 THEN ''First Response'' WHEN 165351 THEN ''Dual Kit'' WHEN 169126 THEN ''One step'' WHEN 169127 THEN ''Trinscreen'' ELSE '''' END), NULL)) AS kit_name, ',
      'MAX(IF(o.concept_id=164964, TRIM(o.value_text), NULL)) AS lot_no, ',
      'MAX(IF(o.concept_id=162502, DATE(o.value_datetime), NULL)) AS expiry_date ',
      'FROM obs o ',
      'INNER JOIN encounter e ON e.encounter_id = o.encounter_id ',
      'INNER JOIN person p ON p.person_id = o.person_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''72aa78e0-ee4b-47c3-9073-26f3b9ecc4a7'') ',
      'WHERE o.concept_id IN (1040,1326,1000630,164962,164964,162502) AND o.voided = 0 ',
      'GROUP BY e.encounter_id, o.obs_group_id',
    ') t ON e.encounter_id = t.encounter_id ',
    'WHERE e.voided = 0 ',
    'GROUP BY e.encounter_id;'
  );

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing MCH postnatal visits ", CONCAT("Time: ", NOW());
END $$
DELIMITER ;

-- ------------- populate etl_hei_enrollment-------------------------

DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_hei_enrolment $$
CREATE PROCEDURE sp_populate_etl_hei_enrolment()
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_hei_enrollment`');

  SELECT "Processing HEI Enrollments", CONCAT("Time: ", NOW());

  SET @sql = CONCAT(
    'INSERT INTO ', target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, child_exposed, spd_number, birth_weight, ',
      'gestation_at_birth, birth_type, date_first_seen, birth_notification_number, birth_certificate_number, need_for_special_care, ',
      'reason_for_special_care, referral_source, transfer_in, transfer_in_date, facility_transferred_from, district_transferred_from, ',
      'date_first_enrolled_in_hei_care, mother_breastfeeding, TB_contact_history_in_household, mother_alive, mother_on_pmtct_drugs, ',
      'mother_on_drug, mother_on_art_at_infant_enrollment, mother_drug_regimen, infant_prophylaxis, parent_ccc_number, mode_of_delivery, ',
      'place_of_delivery, birth_length, birth_order, health_facility_name, date_of_birth_notification, date_of_birth_registration, ',
      'birth_registration_place, permanent_registration_serial, mother_facility_registered, exit_date, exit_reason, hiv_status_at_exit, ',
      'encounter_type, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id=5303,o.value_coded,NULL)) AS child_exposed, ',
      'MAX(IF(o.concept_id=162054,o.value_text,NULL)) AS spd_number, ',
      'MAX(IF(o.concept_id=5916,o.value_numeric,NULL)) AS birth_weight, ',
      'MAX(IF(o.concept_id=1409,o.value_numeric,NULL)) AS gestation_at_birth, ',
      'MAX(IF(o.concept_id=159949,o.value_coded,NULL)) AS birth_type, ',
      'MAX(IF(o.concept_id=162140,o.value_datetime,NULL)) AS date_first_seen, ',
      'MAX(IF(o.concept_id=162051,o.value_text,NULL)) AS birth_notification_number, ',
      'MAX(IF(o.concept_id=162052,o.value_text,NULL)) AS birth_certificate_number, ',
      'MAX(IF(o.concept_id=161630,o.value_coded,NULL)) AS need_for_special_care, ',
      'MAX(IF(o.concept_id=161601,o.value_coded,NULL)) AS reason_for_special_care, ',
      'MAX(IF(o.concept_id=160540,o.value_coded,NULL)) AS referral_source, ',
      'MAX(IF(o.concept_id=160563,o.value_coded,NULL)) AS transfer_in, ',
      'MAX(IF(o.concept_id=160534,o.value_datetime,NULL)) AS transfer_in_date, ',
      'MAX(IF(o.concept_id=160535,o.value_text,NULL)) AS facility_transferred_from, ',
      'MAX(IF(o.concept_id=161551,o.value_text,NULL)) AS district_transferred_from, ',
      'MAX(IF(o.concept_id=160555,o.value_datetime,NULL)) AS date_first_enrolled_in_hei_care, ',
      'MAX(IF(o.concept_id=159941,o.value_coded,NULL)) AS mother_breastfeeding, ',
      'MAX(IF(o.concept_id=152460,o.value_coded,NULL)) AS TB_contact_history_in_household, ',
      'MAX(IF(o.concept_id=160429,o.value_coded,NULL)) AS mother_alive, ',
      'MAX(IF(o.concept_id=1148,o.value_coded,NULL)) AS mother_on_pmtct_drugs, ',
      'MAX(IF(o.concept_id=1086,o.value_coded,NULL)) AS mother_on_drug, ',
      'MAX(IF(o.concept_id=162055,o.value_coded,NULL)) AS mother_on_art_at_infant_enrollment, ',
      'MAX(IF(o.concept_id=1088,o.value_coded,NULL)) AS mother_drug_regimen, ',
      'MAX(IF(o.concept_id=1282,o.value_coded,NULL)) AS infant_prophylaxis, ',
      'MAX(IF(o.concept_id=162053,o.value_numeric,NULL)) AS parent_ccc_number, ',
      'MAX(IF(o.concept_id=5630,o.value_coded,NULL)) AS mode_of_delivery, ',
      'MAX(IF(o.concept_id=1572,o.value_coded,NULL)) AS place_of_delivery, ',
      'MAX(IF(o.concept_id=1503,o.value_numeric,NULL)) AS birth_length, ',
      'MAX(IF(o.concept_id=163460,o.value_numeric,NULL)) AS birth_order, ',
      'MAX(IF(o.concept_id=162724,o.value_text,NULL)) AS health_facility_name, ',
      'MAX(IF(o.concept_id=164130,o.value_datetime,NULL)) AS date_of_birth_notification, ',
      'MAX(IF(o.concept_id=164129,o.value_datetime,NULL)) AS date_of_birth_registration, ',
      'MAX(IF(o.concept_id=164140,o.value_text,NULL)) AS birth_registration_place, ',
      'MAX(IF(o.concept_id=1646,o.value_text,NULL)) AS permanent_registration_serial, ',
      'MAX(IF(o.concept_id=162724,o.value_text,NULL)) AS mother_facility_registered, ',
      'MAX(IF(o.concept_id=160753,o.value_datetime,NULL)) AS exit_date, ',
      'MAX(IF(o.concept_id=161555,o.value_coded,NULL)) AS exit_reason, ',
      'MAX(IF(o.concept_id=159427,(CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1138 THEN ''Inconclusive'' ELSE '''' END),NULL)) AS hiv_status_at_exit, ',
      'CASE et.uuid WHEN ''01894f88-dc73-42d4-97a3-0929118403fb'' THEN ''MCHCS_HEI_COMPLETION'' WHEN ''415f5136-ca4a-49a8-8db3-f994187c3af6'' THEN ''MCHCS_HEI_ENROLLMENT'' ELSE NULL END AS encounter_type, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (5303,162054,5916,1409,162140,162051,162052,161630,161601,160540,160563,160534,160535,161551,160555,1282,159941,152460,160429,1148,1086,162055,1088,162053,5630,1572,161555,159427,1503,163460,162724,164130,164129,164140,1646,160753,159949) ',
    'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''415f5136-ca4a-49a8-8db3-f994187c3af6'',''01894f88-dc73-42d4-97a3-0929118403fb'')) et ON et.encounter_type_id = e.encounter_type ',
    'WHERE e.voided = 0 ',
    'GROUP BY e.patient_id, visit_date;'
  );

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing HEI Enrollments", CONCAT("Time: ", NOW());
END $$
DELIMITER ;

-- ------------- populate etl_hei_follow_up_visit-------------------------

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

-- ------------- populate etl_immunization   --------------------------------

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


-- ------------ create table etl_tb_enrollment-----------------------

DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_tb_enrollment $$
CREATE PROCEDURE sp_populate_etl_tb_enrollment()
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
      'treatment_supporter_phone_contact, disease_classification, patient_classification, pulmonary_smear_result, ',
      'has_extra_pulmonary_pleurial_effusion, has_extra_pulmonary_milliary, has_extra_pulmonary_lymph_node, ',
      'has_extra_pulmonary_menengitis, has_extra_pulmonary_skeleton, has_extra_pulmonary_abdominal, ',
      'date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id=1113,o.value_datetime,NULL)) AS date_treatment_started, ',
      'MAX(IF(o.concept_id=161564,TRIM(o.value_text),NULL)) AS district, ',
      'MAX(IF(o.concept_id=160540,o.value_coded,NULL)) AS referred_by, ',
      'MAX(IF(o.concept_id=161561,o.value_datetime,NULL)) AS referral_date, ',
      'MAX(IF(o.concept_id=160534,o.value_datetime,NULL)) AS date_transferred_in, ',
      'MAX(IF(o.concept_id=160535,LEFT(TRIM(o.value_text),100),NULL)) AS facility_transferred_from, ',
      'MAX(IF(o.concept_id=161551,LEFT(TRIM(o.value_text),100),NULL)) AS district_transferred_from, ',
      'MAX(IF(o.concept_id=161552,o.value_datetime,NULL)) AS date_first_enrolled_in_tb_care, ',
      'MAX(IF(o.concept_id=5089,o.value_numeric,NULL)) AS weight, ',
      'MAX(IF(o.concept_id=5090,o.value_numeric,NULL)) AS height, ',
      'MAX(IF(o.concept_id=160638,LEFT(TRIM(o.value_text),100),NULL)) AS treatment_supporter, ',
      'MAX(IF(o.concept_id=160640,o.value_coded,NULL)) AS relation_to_patient, ',
      'MAX(IF(o.concept_id=160641,LEFT(TRIM(o.value_text),100),NULL)) AS treatment_supporter_address, ',
      'MAX(IF(o.concept_id=160642,LEFT(TRIM(o.value_text),100),NULL)) AS treatment_supporter_phone_contact, ',
      'MAX(IF(o.concept_id=160040,o.value_coded,NULL)) AS disease_classification, ',
      'MAX(IF(o.concept_id=159871,o.value_coded,NULL)) AS patient_classification, ',
      'MAX(IF(o.concept_id=159982,o.value_coded,NULL)) AS pulmonary_smear_result, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=130059,o.value_coded,NULL)) AS has_extra_pulmonary_pleurial_effusion, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=115753,o.value_coded,NULL)) AS has_extra_pulmonary_milliary, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=111953,o.value_coded,NULL)) AS has_extra_pulmonary_lymph_node, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=111967,o.value_coded,NULL)) AS has_extra_pulmonary_menengitis, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=112116,o.value_coded,NULL)) AS has_extra_pulmonary_skeleton, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=1350,o.value_coded,NULL)) AS has_extra_pulmonary_abdominal, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (160540,161561,160534,160535,161551,161552,5089,5090,160638,160640,160641,160642,160040,159871,159982,161356) ',
    'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''9d8498a4-372d-4dc4-a809-513a2434621e'')) et ON et.encounter_type_id = e.encounter_type ',
    'WHERE e.voided = 0 ',
    'GROUP BY e.encounter_id;'
  );

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing TB Enrollments ", CONCAT("Time: ", NOW());
END $$
DELIMITER ;


-- ------------- populate etl_tb_follow_up_visit-------------------------

-- sql
DROP PROCEDURE IF EXISTS sp_populate_etl_tb_follow_up_visit $$
CREATE PROCEDURE sp_populate_etl_tb_follow_up_visit()
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  CALL sp_set_tenant_session_vars();
  SET target_table = CONCAT('`', @etl_schema, '`.`etl_tb_follow_up_visit`');

  SELECT "Processing TB Followup visits ", CONCAT("Time: ", NOW());

  SET sql_stmt = CONCAT(
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
      'MAX(IF(o.concept_id=159956 AND o.value_coded=84360,o.value_coded,NULL)) AS resistant_s, ',
      'MAX(IF(o.concept_id=159956 AND o.value_coded=767,o.value_coded,NULL)) AS resistant_r, ',
      'MAX(IF(o.concept_id=159956 AND o.value_coded=78280,o.value_coded,NULL)) AS resistant_inh, ',
      'MAX(IF(o.concept_id=159956 AND o.value_coded=75948,o.value_coded,NULL)) AS resistant_e, ',
      'MAX(IF(o.concept_id=159958 AND o.value_coded=84360,o.value_coded,NULL)) AS sensitive_s, ',
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
    'WHERE e.voided = 0 ',
    'GROUP BY e.encounter_id;'
  );

  PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

  SELECT "Completed processing TB Followup visits ", CONCAT("Time: ", NOW());
END $$


- ------------- populate etl_tb_screening-------------------------

-- sql
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_tb_screening $$
CREATE PROCEDURE sp_populate_etl_tb_screening()
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
      'contact_invitation, evaluated_for_ipt, started_anti_TB, tb_treatment_start_date, tb_prophylaxis, notes, person_present, ',
      'date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, date(e.encounter_datetime) AS visit_date, e.encounter_id, e.location_id, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded=159799,o.value_coded,NULL)) AS cough_for_2wks_or_more, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded IN (124068,1066),o.value_coded,NULL)) AS confirmed_tb_contact, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded=1494,o.value_coded,NULL)) AS fever_for_2wks_or_more, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded=832,o.value_coded,NULL)) AS noticeable_weight_loss, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded=133027,o.value_coded,NULL)) AS night_sweat_for_2wks_or_more, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded=116334,o.value_coded,NULL)) AS lethargy, ',
      'MAX(IF(o.concept_id=1271 AND o.value_coded=307,o.value_coded,NULL)) AS spatum_smear_ordered, ',
      'MAX(IF(o.concept_id=1271 AND o.value_coded=12,o.value_coded,NULL)) AS chest_xray_ordered, ',
      'MAX(IF(o.concept_id=1271 AND o.value_coded=162202,o.value_coded,NULL)) AS genexpert_ordered, ',
      'MAX(IF(o.concept_id=307,o.value_coded,NULL)) AS spatum_smear_result, ',
      'MAX(IF(o.concept_id=12,o.value_coded,NULL)) AS chest_xray_result, ',
      'MAX(IF(o.concept_id=162202,o.value_coded,NULL)) AS genexpert_result, ',
      'MAX(IF(o.concept_id=1272,o.value_coded,NULL)) AS referral, ',
      'MAX(IF(o.concept_id=163752,o.value_coded,NULL)) AS clinical_tb_diagnosis, ',
      'MAX(IF(o.concept_id=1659,o.value_coded,NULL)) AS resulting_tb_status, ',
      'MAX(IF(o.concept_id=163414,o.value_coded,NULL)) AS contact_invitation, ',
      'MAX(IF(o.concept_id=162275,o.value_coded,NULL)) AS evaluated_for_ipt, ',
      'MAX(IF(o.concept_id=162309,o.value_coded,NULL)) AS started_anti_TB, ',
      'MAX(IF(o.concept_id=1113, DATE(o.value_datetime), NULL)) AS tb_treatment_start_date, ',
      'MAX(IF(o.concept_id=1109,o.value_coded,NULL)) AS tb_prophylaxis, ',
      'NULL AS notes, ',
      'MAX(IF(o.concept_id=161643,o.value_coded,NULL)) AS person_present, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (',
      '''22c68f86-bbf0-49ba-b2d1-23fa7ccf0259'', ',
      '''59ed8e62-7f1f-40ae-a2e3-eabe350277ce'', ',
      '''23b4ebbd-29ad-455e-be0e-04aa6bc30798'', ',
      '''72aa78e0-ee4b-47c3-9073-26f3b9ecc4a7''',
    ') ',
    'INNER JOIN obs o ON o.encounter_id = e.encounter_id ',
      'AND o.concept_id IN (1659,1113,160632,161643,1729,1271,307,12,162202,1272,163752,163414,162275,162309,1109) ',
      'AND o.voided = 0 ',
    'WHERE e.voided = 0 ',
    'GROUP BY e.patient_id, visit_date;'
  );

  PREPARE stmt FROM sql_stmt;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  SELECT "Completed processing TB Screening data ", CONCAT("Time: ", NOW());
END $$
DELIMITER ;













































DROP PROCEDURE IF EXISTS sp_first_time_setup $$
CREATE PROCEDURE sp_first_time_setup()
BEGIN
    DECLARE populate_script_id INT DEFAULT NULL;
    DECLARE sql_stmt TEXT;

CALL sp_set_tenant_session_vars();

SELECT "Beginning first time setup", CONCAT("Time: ", NOW());
SET sql_stmt = CONCAT('INSERT INTO ', @script_status_table_quoted, ' (script_name, start_time) VALUES (''initial_population_of_tables'', NOW())');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET populate_script_id = LAST_INSERT_ID();

CALL sp_populate_etl_patient_demographics();
CALL sp_populate_etl_hiv_enrollment();
CALL sp_populate_etl_hiv_followup();
CALL sp_populate_etl_laboratory_extract();
CALL sp_populate_etl_pharmacy_extract();
CALL sp_populate_etl_program_discontinuation();
CALL sp_populate_etl_mch_enrollment();
CALL sp_populate_etl_mch_antenatal_visit();
CALL sp_populate_etl_mch_postnatal_visit();
CALL sp_populate_etl_tb_enrollment();
CALL sp_populate_etl_tb_follow_up_visit();
CALL sp_populate_etl_tb_screening();
CALL sp_populate_etl_hei_enrolment();
CALL sp_populate_etl_hei_immunization();
CALL sp_populate_etl_hei_follow_up();
CALL sp_populate_etl_mch_delivery();
CALL sp_populate_etl_patient_appointment();
CALL sp_populate_etl_mch_discharge();
CALL sp_drug_event();
CALL sp_populate_hts_test();
CALL sp_populate_etl_generalized_anxiety_disorder();
CALL sp_populate_hts_linkage_and_referral();
CALL sp_populate_hts_referral();
CALL sp_populate_etl_ccc_defaulter_tracing();
CALL sp_populate_etl_ART_preparation();
CALL sp_populate_etl_enhanced_adherence();
CALL sp_populate_etl_patient_triage();
CALL sp_populate_etl_ipt_initiation();
CALL sp_populate_etl_ipt_follow_up();
CALL sp_populate_etl_ipt_outcome();
CALL sp_populate_etl_prep_enrolment();
CALL sp_populate_etl_prep_followup();
CALL sp_populate_etl_prep_behaviour_risk_assessment();
CALL sp_populate_etl_prep_monthly_refill();
CALL sp_populate_etl_progress_note();
CALL sp_populate_etl_prep_discontinuation();
CALL sp_populate_etl_hts_linkage_tracing();
CALL sp_populate_etl_patient_program();
CALL sp_create_default_facility_table();
CALL sp_populate_etl_person_address();
CALL sp_populate_etl_otz_enrollment();
CALL sp_populate_etl_otz_activity();
CALL sp_populate_etl_ovc_enrolment();
CALL sp_populate_etl_cervical_cancer_screening();
CALL sp_populate_etl_patient_contact();
CALL sp_populate_etl_client_trace();
CALL sp_populate_etl_kp_contact();
CALL sp_populate_etl_kp_client_enrollment();
CALL sp_populate_etl_kp_clinical_visit();
CALL sp_populate_etl_kp_sti_treatment();
CALL sp_populate_etl_kp_peer_calendar();
CALL sp_populate_etl_kp_peer_tracking();
CALL sp_populate_etl_kp_treatment_verification();
-- CALL sp_populate_etl_gender_based_violence();
CALL sp_populate_etl_PrEP_verification();
CALL sp_populate_etl_alcohol_drug_abuse_screening();
CALL sp_populate_etl_gbv_screening();
CALL sp_populate_etl_gbv_screening_action();
CALL sp_populate_etl_violence_reporting();
CALL sp_populate_etl_link_facility_tracking();
CALL sp_populate_etl_depression_screening();
CALL sp_populate_etl_adverse_events();
CALL sp_populate_etl_allergy_chronic_illness();
CALL sp_populate_etl_ipt_screening();
CALL sp_populate_etl_pre_hiv_enrollment_art();
CALL sp_populate_etl_covid_19_assessment();
CALL sp_populate_etl_vmmc_enrolment();
CALL sp_populate_etl_vmmc_circumcision_procedure();
CALL sp_populate_etl_vmmc_client_followup();
CALL sp_populate_etl_vmmc_medical_history();
CALL sp_populate_etl_vmmc_post_operation_assessment();
CALL sp_populate_etl_hts_eligibility_screening();
CALL sp_populate_etl_drug_order();
CALL sp_populate_etl_preventive_services();
CALL sp_populate_etl_overdose_reporting();
CALL sp_populate_etl_art_fast_track();
CALL sp_populate_etl_clinical_encounter();
CALL sp_populate_etl_pep_management_survivor();
CALL sp_populate_etl_sgbv_pep_followup();
CALL sp_populate_etl_sgbv_post_rape_care();
CALL sp_populate_etl_gbv_physical_emotional_abuse();
CALL sp_populate_etl_family_planning();
CALL sp_populate_etl_physiotherapy();
CALL sp_populate_etl_psychiatry();
CALL sp_populate_etl_kvp_clinical_enrollment();
CALL sp_populate_etl_high_iit_intervention();
CALL sp_populate_etl_home_visit_checklist();
CALL sp_populate_etl_special_clinics();
CALL sp_populate_etl_adr_assessment_tool();
CALL sp_populate_etl_ncd_enrollment();
CALL sp_populate_etl_inpatient_admission();
CALL sp_populate_etl_inpatient_discharge();
CALL sp_update_next_appointment_date();
CALL sp_update_dashboard_table();
SET sql_stmt = CONCAT('UPDATE ', @script_status_table_quoted, ' SET stop_time = NOW() WHERE id = ', populate_script_id);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT "Completed first time setup", CONCAT("Time: ", NOW());
END $$

SET SQL_MODE=@OLD_SQL_MODE $$
DELIMITER ;
