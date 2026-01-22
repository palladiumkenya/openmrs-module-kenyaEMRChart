-- sql
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

    SET current_schema = DATABASE();
    SET tenant_suffix = SUBSTRING_INDEX(current_schema, 'openmrs_', -1);
    SET etl_schema        = CONCAT('kenyaemr_etl_', tenant_suffix);
    SET datatools_schema  = CONCAT('kenyaemr_datatools_', tenant_suffix);
    SET script_status_table = CONCAT(etl_schema, '.etl_script_status');

    SET FOREIGN_KEY_CHECKS = 0;
    SET @drop_etl_db = CONCAT('DROP DATABASE IF EXISTS `', etl_schema, '`;');
PREPARE stmt FROM @drop_etl_db; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @drop_dt_db = CONCAT('DROP DATABASE IF EXISTS `', datatools_schema, '`;');
PREPARE stmt FROM @drop_dt_db; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET FOREIGN_KEY_CHECKS = 1;

    SET @create_etl = CONCAT(
        'CREATE DATABASE `', etl_schema, '`',
        ' DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;'
    );
PREPARE stmt FROM @create_etl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @create_dt = CONCAT(
        'CREATE DATABASE `', datatools_schema, '`',
        ' DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;'
    );
PREPARE stmt FROM @create_dt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

/* --------------------------------------
   CREATE etl_script_status TABLE
   --------------------------------------*/
SET @drop_status = CONCAT('DROP TABLE IF EXISTS `', script_status_table, '`;');
PREPARE stmt FROM @drop_status; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @create_status = CONCAT(
        'CREATE TABLE `', script_status_table, '` (',
        '  id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,',
        '  script_name VARCHAR(50) DEFAULT NULL,',
        '  start_time DATETIME DEFAULT NULL,',
        '  stop_time DATETIME DEFAULT NULL,',
        '  error VARCHAR(255) DEFAULT NULL',
        ');'
    );
PREPARE stmt FROM @create_status; EXECUTE stmt; DEALLOCATE PREPARE stmt;

/* --------------------------------------
   LOG START TIME
   --------------------------------------*/
SET @log_start = CONCAT(
        'INSERT INTO `', script_status_table, '`',
        ' (script_name, start_time) VALUES (''initial_creation_of_tables'', NOW());'
    );
PREPARE stmt FROM @log_start; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET script_id = LAST_INSERT_ID();

    /* --------------------------------------
       DROP ETL TABLES IF THEY EXIST (tenant-aware)
       --------------------------------------*/
    SET @drop_etl_hiv = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_hiv_enrollment`;');
PREPARE stmt FROM @drop_etl_hiv; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @drop_etl_pd = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_patient_demographics`;');
PREPARE stmt FROM @drop_etl_pd; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- create table etl_patient_demographics
SET @sql = CONCAT(
        'CREATE TABLE `', etl_schema, '`.etl_patient_demographics (',
        'patient_id INT(11) NOT NULL PRIMARY KEY,',
        'uuid CHAR(36),',
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
        'cause_of_death INT(11),',
        'death_date DATE DEFAULT NULL,',
        'voided INT(11),',
        'date_created DATETIME NOT NULL,',
        'date_last_modified DATETIME,',
        'INDEX(Gender),',
        'INDEX(unique_patient_no),',
        'INDEX(DOB)',
        ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
    );

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT "Successfully created etl_patient_demographics table";

-- create table etl_hiv_enrollment
SET @sql = CONCAT(
      'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_hiv_enrollment` (',
      ' uuid CHAR(38),',
      ' patient_id INT(11) NOT NULL,',
      ' visit_id INT(11) DEFAULT NULL,',
      ' visit_date DATE,',
      ' location_id INT(11) DEFAULT NULL,',
      ' encounter_id INT(11) NOT NULL PRIMARY KEY,',
      ' encounter_provider INT(11),',
      ' patient_type INT(11),',
      ' date_first_enrolled_in_care DATE,',
      ' entry_point INT(11),',
      ' transfer_in_date DATE,',
      ' facility_transferred_from VARCHAR(255),',
      ' district_transferred_from VARCHAR(255),',
      ' date_started_art_at_transferring_facility DATE,',
      ' date_confirmed_hiv_positive DATE,',
      ' facility_confirmed_hiv_positive VARCHAR(255),',
      ' previous_regimen VARCHAR(255),',
      ' arv_status INT(11),',
      ' ever_on_pmtct INT(11),',
      ' ever_on_pep INT(11),',
      ' ever_on_prep INT(11),',
      ' ever_on_haart INT(11),',
      ' cd4_test_result INT(11),',
      ' cd4_test_date DATE,',
      ' viral_load_test_result INT(11),',
      ' viral_load_test_date DATE,',
      ' who_stage INT(11),',
      ' name_of_treatment_supporter VARCHAR(255),',
      ' relationship_of_treatment_supporter INT(11),',
      ' treatment_supporter_telephone VARCHAR(100),',
      ' treatment_supporter_address VARCHAR(100),',
      ' in_school INT(11) DEFAULT NULL,',
      ' orphan INT(11) DEFAULT NULL,',
      ' date_of_discontinuation DATETIME,',
      ' discontinuation_reason INT(11),',
      ' date_created DATETIME NOT NULL,',
      ' date_last_modified DATETIME,',
      ' voided INT(11),',
      ' CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
      ' CONSTRAINT unique_uuid UNIQUE (uuid),',
      ' INDEX (patient_id),',
      ' INDEX (visit_id),',
      ' INDEX (visit_date),',
      ' INDEX (date_started_art_at_transferring_facility),',
      ' INDEX (arv_status),',
      ' INDEX (date_confirmed_hiv_positive),',
      ' INDEX (entry_point),',
      ' INDEX (transfer_in_date),',
      ' INDEX (date_first_enrolled_in_care),',
      ' INDEX (entry_point, transfer_in_date, visit_date, patient_id)',
      ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
    );
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT "Successfully created etl_hiv_enrollment table";

/* --------------------------------------
   DROP & CREATE etl_patient_hiv_followup (tenant-aware)
   --------------------------------------*/
SET @drop_etl_followup = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_patient_hiv_followup`;');
PREPARE stmt FROM @drop_etl_followup; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_patient_hiv_followup` (',
  'uuid CHAR(38),',
  'encounter_id INT(11) NOT NULL PRIMARY KEY,',
  'patient_id INT(11) NOT NULL ,',
  'location_id INT(11) DEFAULT NULL,',
  'visit_date DATE,',
  'visit_id INT(11),',
  'encounter_provider INT(11),',
  'date_created DATETIME NOT NULL,',
  'date_last_modified DATETIME,',
  'visit_scheduled INT(11),',
  'person_present INT(11),',
  'weight DOUBLE,',
  'systolic_pressure DOUBLE,',
  'diastolic_pressure DOUBLE,',
  'height DOUBLE,',
  'temperature DOUBLE,',
  'pulse_rate DOUBLE,',
  'respiratory_rate DOUBLE,',
  'oxygen_saturation DOUBLE,',
  'muac DOUBLE,',
  'z_score_absolute DOUBLE DEFAULT NULL,',
  'z_score INT(11),',
  'nutritional_status INT(11) DEFAULT NULL,',
  'population_type INT(11) DEFAULT NULL,',
  'key_population_type INT(11) DEFAULT NULL,',
  'who_stage INT(11),',
  'who_stage_associated_oi VARCHAR(1000),',
  'presenting_complaints INT(11) DEFAULT NULL,',
  'clinical_notes VARCHAR(600) DEFAULT NULL,',
  'on_anti_tb_drugs INT(11) DEFAULT NULL,',
  'on_ipt INT(11) DEFAULT NULL,',
  'ever_on_ipt INT(11) DEFAULT NULL,',
  'cough INT(11) DEFAULT -1,',
  'fever INT(11) DEFAULT -1,',
  'weight_loss_poor_gain INT(11) DEFAULT -1,',
  'night_sweats INT(11) DEFAULT -1,',
  'tb_case_contact INT(11) DEFAULT -1,',
  'lethargy INT(11) DEFAULT -1,',
  'screened_for_tb VARCHAR(50),',
  'spatum_smear_ordered INT(11) DEFAULT NULL,',
  'chest_xray_ordered INT(11) DEFAULT NULL,',
  'genexpert_ordered INT(11) DEFAULT NULL,',
  'spatum_smear_result INT(11) DEFAULT NULL,',
  'chest_xray_result INT(11) DEFAULT NULL,',
  'genexpert_result INT(11) DEFAULT NULL,',
  'referral INT(11) DEFAULT NULL,',
  'clinical_tb_diagnosis INT(11) DEFAULT NULL,',
  'contact_invitation INT(11) DEFAULT NULL,',
  'evaluated_for_ipt INT(11) DEFAULT NULL,',
  'has_known_allergies INT(11) DEFAULT NULL,',
  'has_chronic_illnesses_cormobidities INT(11) DEFAULT NULL,',
  'has_adverse_drug_reaction INT(11) DEFAULT NULL,',
  'substitution_first_line_regimen_date DATE ,',
  'substitution_first_line_regimen_reason INT(11),',
  'substitution_second_line_regimen_date DATE,',
  'substitution_second_line_regimen_reason INT(11),',
  'second_line_regimen_change_date DATE,',
  'second_line_regimen_change_reason INT(11),',
  'pregnancy_status INT(11),',
  'breastfeeding INT(11),',
  'wants_pregnancy INT(11) DEFAULT NULL,',
  'pregnancy_outcome INT(11),',
  'anc_number VARCHAR(50),',
  'expected_delivery_date DATE,',
  'ever_had_menses INT(11),',
  'last_menstrual_period DATE,',
  'menopausal INT(11),',
  'gravida INT(11),',
  'parity INT(11),',
  'full_term_pregnancies INT(11),',
  'abortion_miscarriages INT(11),',
  'family_planning_status INT(11),',
  'family_planning_method INT(11),',
  'reason_not_using_family_planning INT(11),',
  'tb_status INT(11),',
  'started_anti_TB INT(11),',
  'tb_rx_date DATE,',
  'tb_treatment_no VARCHAR(50),',
  'general_examination VARCHAR(255),',
  'system_examination INT(11),',
  'skin_findings INT(11),',
  'eyes_findings INT(11),',
  'ent_findings INT(11),',
  'chest_findings INT(11),',
  'cvs_findings INT(11),',
  'abdomen_findings INT(11),',
  'cns_findings INT(11),',
  'genitourinary_findings INT(11),',
  'prophylaxis_given VARCHAR(50),',
  'ctx_adherence INT(11),',
  'ctx_dispensed INT(11),',
  'dapsone_adherence INT(11),',
  'dapsone_dispensed INT(11),',
  'inh_dispensed INT(11),',
  'arv_adherence INT(11),',
  'poor_arv_adherence_reason INT(11),',
  'poor_arv_adherence_reason_other VARCHAR(200),',
  'pwp_disclosure INT(11),',
  'pwp_pead_disclosure INT(11),',
  'pwp_partner_tested INT(11),',
  'condom_provided INT(11),',
  'substance_abuse_screening INT(11),',
  'screened_for_sti INT(11),',
  'cacx_screening INT(11),',
  'sti_partner_notification INT(11),',
  'experienced_gbv INT(11),',
  'depression_screening INT(11),',
  'at_risk_population INT(11),',
  'system_review_finding INT(11),',
  'next_appointment_date DATE,',
  'refill_date DATE,',
  'appointment_consent INT(11),',
  'next_appointment_reason INT(11),',
  'stability INT(11),',
  'differentiated_care_group INT(11),',
  'differentiated_care INT(11),',
  'established_differentiated_care INT(11),',
  'insurance_type INT(11),',
  'other_insurance_specify VARCHAR(200),',
  'insurance_status INT(11),',
  'voided INT(11),',
  'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
  'CONSTRAINT unique_uuid UNIQUE(uuid),',
  'INDEX(visit_date),',
  'INDEX(encounter_id),',
  'INDEX(patient_id),',
  'INDEX(patient_id, visit_date),',
  'INDEX(who_stage),',
  'INDEX(pregnancy_status),',
  'INDEX(breastfeeding),',
  'INDEX(pregnancy_outcome),',
  'INDEX(family_planning_status),',
  'INDEX(family_planning_method),',
  'INDEX(tb_status),',
  'INDEX(condom_provided),',
  'INDEX(ctx_dispensed),',
  'INDEX(inh_dispensed),',
  'INDEX(at_risk_population),',
  'INDEX(population_type),',
  'INDEX(key_population_type),',
  'INDEX(on_anti_tb_drugs),',
  'INDEX(on_ipt),',
  'INDEX(ever_on_ipt),',
  'INDEX(differentiated_care),',
  'INDEX(visit_date, patient_id),',
  'INDEX(visit_date, condom_provided),',
  'INDEX(visit_date, family_planning_method),',
  'INDEX(nutritional_status),',
  'INDEX(next_appointment_date),',
  'INDEX(appointment_consent),',
  'INDEX(visit_date, next_appointment_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Successfully created etl_patient_hiv_followup table';


-- ------- create table etl_laboratory_extract-----------------------------------------

-- sql
SET @drop_etl_laboratory = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_laboratory_extract`;');
PREPARE stmt FROM @drop_etl_laboratory; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_laboratory_extract` (',
  'uuid CHAR(38) PRIMARY KEY,',
  'encounter_id INT(11),',
  'patient_id INT(11) NOT NULL,',
  'location_id INT(11) DEFAULT NULL,',
  'visit_date DATE,',
  'visit_id INT(11),',
  'order_id VARCHAR(200),',
  'lab_test VARCHAR(180),',
  'urgency VARCHAR(50),',
  'order_reason VARCHAR(180),',
  'order_test_name VARCHAR(180),',
  'obs_id INT,',
  'result_test_name VARCHAR(180),',
  'result_name VARCHAR(400),',
  'set_member_conceptId VARCHAR(100),',
  'test_result VARCHAR(180),',
  'date_test_requested DATE DEFAULT NULL,',
  'date_test_result_received DATE,',
  'test_requested_by INT(11),',
  'date_created DATETIME NOT NULL,',
  'date_last_modified DATETIME,',
  'created_by INT(11),',
  'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
  'CONSTRAINT unique_uuid UNIQUE (uuid),',
  'INDEX(visit_date),',
  'INDEX(encounter_id),',
  'INDEX(patient_id),',
  'INDEX(lab_test),',
  'INDEX(test_result),',
  'INDEX(set_member_conceptId)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Successfully created etl_laboratory_extract table';

-- ------------ create table etl_pharmacy_extract-----------------------

SET @drop_etl_pharmacy = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_pharmacy_extract`;');
PREPARE stmt FROM @drop_etl_pharmacy; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_pharmacy_extract` (',
  'obs_group_id INT(11) PRIMARY KEY,',
  'uuid CHAR(38),',
  'patient_id INT(11) NOT NULL,',
  'location_id INT(11) DEFAULT NULL,',
  'visit_date DATE,',
  'visit_id INT(11),',
  'encounter_id INT(11),',
  'encounter_name VARCHAR(100),',
  'drug INT(11),',
  'is_arv INT(11),',
  'is_ctx INT(11),',
  'is_dapsone INT(11),',
  'drug_name VARCHAR(255),',
  'dose INT(11),',
  'unit INT(11),',
  'frequency INT(11),',
  'duration INT(11),',
  'duration_units VARCHAR(20),',
  'duration_in_days INT(11),',
  'prescription_provider VARCHAR(50),',
  'dispensing_provider VARCHAR(50),',
  'regimen MEDIUMTEXT,',
  'adverse_effects VARCHAR(100),',
  'date_of_refill DATE,',
  'date_created DATETIME NOT NULL,',
  'date_last_modified DATETIME,',
  'voided INT(11),',
  'date_voided DATE,',
  'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
  'CONSTRAINT unique_uuid UNIQUE (uuid),',
  'INDEX(visit_date),',
  'INDEX(encounter_id),',
  'INDEX(patient_id),',
  'INDEX(drug),',
  'INDEX(is_arv)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_pharmacy_extract table';

-- ------------ create table etl_patient_treatment_discontinuation-----------------------

SET @drop_etl_ppd = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_patient_program_discontinuation`;');
PREPARE stmt FROM @drop_etl_ppd; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_patient_program_discontinuation` (',
  ' uuid CHAR(38),',
  ' patient_id INT(11) NOT NULL,',
  ' visit_id INT(11),',
  ' visit_date DATETIME,',
  ' location_id INT(11) DEFAULT NULL,',
  ' program_uuid CHAR(38),',
  ' program_name VARCHAR(50),',
  ' encounter_id INT(11) NOT NULL PRIMARY KEY,',
  ' discontinuation_reason INT(11),',
  ' effective_discontinuation_date DATE,',
  ' trf_out_verified INT(11),',
  ' trf_out_verification_date DATE,',
  ' date_died DATE,',
  ' transfer_facility VARCHAR(100),',
  ' transfer_date DATE,',
  ' death_reason INT(11),',
  ' specific_death_cause INT(11),',
  ' natural_causes VARCHAR(200) DEFAULT NULL,',
  ' non_natural_cause VARCHAR(200) DEFAULT NULL,',
  ' date_created DATETIME NOT NULL,',
  ' date_last_modified DATETIME,',
  ' CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
  ' CONSTRAINT unique_uuid UNIQUE (uuid),',
  ' INDEX(visit_date),',
  ' INDEX(visit_date, program_name, patient_id),',
  ' INDEX(visit_date, patient_id),',
  ' INDEX(encounter_id),',
  ' INDEX(patient_id),',
  ' INDEX(discontinuation_reason),',
  ' INDEX(date_died),',
  ' INDEX(transfer_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Successfully created etl_patient_program_discontinuation table';


-- ------------ create table etl_mch_enrollment-----------------------

SET @drop_etl_mch = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_mch_enrollment`;');
PREPARE stmt FROM @drop_etl_mch; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_mch_enrollment` (',
    'uuid CHAR(38),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'service_type INT(11),',
    'anc_number VARCHAR(50),',
    'first_anc_visit_date DATE,',
    'gravida INT(11),',
    'parity INT(11),',
    'parity_abortion INT(11),',
    'age_at_menarche INT(11),',
    'lmp DATE,',
    'lmp_estimated INT(11),',
    'edd_ultrasound DATE,',
    'blood_group INT(11),',
    'serology INT(11),',
    'tb_screening INT(11),',
    'bs_for_mps INT(11),',
    'hiv_status INT(11),',
    'hiv_test_date DATE,',
    'partner_hiv_status INT(11),',
    'partner_hiv_test_date DATE,',
    'ti_date_started_art DATE,',
    'ti_current_regimen INT(11),',
    'ti_care_facility VARCHAR(100),',
    'urine_microscopy VARCHAR(100),',
    'urinary_albumin INT(11),',
    'glucose_measurement INT(11),',
    'urine_ph INT(11),',
    'urine_gravity INT(11),',
    'urine_nitrite_test INT(11),',
    'urine_leukocyte_esterace_test INT(11),',
    'urinary_ketone INT(11),',
    'urine_bile_salt_test INT(11),',
    'urine_bile_pigment_test INT(11),',
    'urine_colour INT(11),',
    'urine_turbidity INT(11),',
    'urine_dipstick_for_blood INT(11),',
    'date_of_discontinuation DATETIME,',
    'discontinuation_reason INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(tb_screening),',
    'INDEX(hiv_status),',
    'INDEX(hiv_test_date),',
    'INDEX(partner_hiv_status)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Successfully created etl_mch_enrollment table';


-- ------------ create table etl_mch_antenatal_visit-----------------------

SET @drop_etl_mch_antenatal = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_mch_antenatal_visit`;');
PREPARE stmt FROM @drop_etl_mch_antenatal; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_mch_antenatal_visit` (',
    'uuid CHAR(38),',
    'patient_id INT(11) NOT NULL ,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'provider INT(11),',
    'anc_visit_number INT(11),',
    'anc_number VARCHAR(50),',
    'parity INT(11),',
    'gravidae INT(11),',
    'lmp_date DATE,',
    'expected_delivery_date DATE,',
    'gestation_in_weeks INT(11),',
    'temperature DOUBLE,',
    'pulse_rate DOUBLE,',
    'systolic_bp DOUBLE,',
    'diastolic_bp DOUBLE,',
    'respiratory_rate DOUBLE,',
    'oxygen_saturation INT(11),',
    'weight DOUBLE,',
    'height DOUBLE,',
    'muac DOUBLE,',
    'hemoglobin DOUBLE,',
    'blood_sugar_test INT(11),',
    'blood_glucose INT(11),',
    'breast_exam_done INT(11),',
    'pallor INT(11),',
    'maturity INT(11),',
    'fundal_height DOUBLE,',
    'fetal_presentation INT(11),',
    'lie INT(11),',
    'fetal_heart_rate INT(11),',
    'fetal_movement INT(11),',
    'who_stage INT(11),',
    'cd4 INT(11),',
    'vl_sample_taken INT(11),',
    'viral_load INT(11),',
    'ldl INT(11),',
    'arv_status INT(11),',
    'hiv_test_during_visit INT(11),',
    'test_1_kit_name VARCHAR(50),',
    'test_1_kit_lot_no VARCHAR(50) DEFAULT NULL,',
    'test_1_kit_expiry DATE DEFAULT NULL,',
    'test_1_result VARCHAR(50) DEFAULT NULL,',
    'test_2_kit_name VARCHAR(50),',
    'test_2_kit_lot_no VARCHAR(50) DEFAULT NULL,',
    'test_2_kit_expiry DATE DEFAULT NULL,',
    'test_2_result VARCHAR(50) DEFAULT NULL,',
    'test_3_kit_name VARCHAR(50),',
    'test_3_kit_lot_no VARCHAR(50) DEFAULT NULL,',
    'test_3_kit_expiry DATE DEFAULT NULL,',
    'test_3_result VARCHAR(50) DEFAULT NULL,',
    'final_test_result VARCHAR(50) DEFAULT NULL,',
    'patient_given_result VARCHAR(50) DEFAULT NULL,',
    'partner_hiv_tested INT(11),',
    'partner_hiv_status INT(11),',
    'prophylaxis_given INT(11),',
    'started_haart_at_anc INT(11),',
    'haart_given INT(11),',
    'date_given_haart DATE,',
    'baby_azt_dispensed INT(11),',
    'baby_nvp_dispensed INT(11),',
    'deworming_done_anc VARCHAR(100),',
    'IPT_dose_given_anc INT(11),',
    'TTT VARCHAR(50) DEFAULT NULL,',
    'IPT_malaria VARCHAR(50) DEFAULT NULL,',
    'iron_supplement VARCHAR(50) DEFAULT NULL,',
    'deworming VARCHAR(50) DEFAULT NULL,',
    'bed_nets VARCHAR(50) DEFAULT NULL,',
    'urine_microscopy VARCHAR(100),',
    'urinary_albumin INT(11),',
    'glucose_measurement INT(11),',
    'urine_ph INT(11),',
    'urine_gravity INT(11),',
    'urine_nitrite_test INT(11),',
    'urine_leukocyte_esterace_test INT(11),',
    'urinary_ketone INT(11),',
    'urine_bile_salt_test INT(11),',
    'urine_bile_pigment_test INT(11),',
    'urine_colour INT(11),',
    'urine_turbidity INT(11),',
    'urine_dipstick_for_blood INT(11),',
    'syphilis_test_status INT(11),',
    'syphilis_treated_status INT(11),',
    'bs_mps INT(11),',
    'diabetes_test INT(11),',
    'intermittent_presumptive_treatment_given INT(11),',
    'intermittent_presumptive_treatment_dose INT(11),',
    'minimum_package_of_care_given INT(11),',
    'minimum_package_of_care_services VARCHAR(1000),',
    'fgm_done INT(11),',
    'fgm_complications VARCHAR(255),',
    'fp_method_postpartum INT(11),',
    'anc_exercises INT(11),',
    'tb_screening INT(11),',
    'cacx_screening INT(11),',
    'cacx_screening_method INT(11),',
    'hepatitis_b_screening INT(11),',
    'hepatitis_b_treatment INT(11),',
    'has_other_illnes INT(11),',
    'counselled INT(11),',
    'counselled_on_birth_plans INT(11),',
    'counselled_on_danger_signs INT(11),',
    'counselled_on_family_planning INT(11),',
    'counselled_on_hiv INT(11),',
    'counselled_on_supplimental_feeding INT(11),',
    'counselled_on_breast_care INT(11),',
    'counselled_on_infant_feeding INT(11),',
    'counselled_on_treated_nets INT(11),',
    'minimum_care_package INT(11),',
    'risk_reduction INT(11),',
    'partner_testing INT(11),',
    'sti_screening INT(11),',
    'condom_provision INT(11),',
    'prep_adherence INT(11),',
    'anc_visits_emphasis INT(11),',
    'pnc_fp_counseling INT(11),',
    'referral_vmmc INT(11),',
    'referral_dreams INT(11),',
    'referred_from INT(11),',
    'referred_to INT(11),',
    'next_appointment_date DATE,',
    'referral_reason VARCHAR(255),',
    'clinical_notes VARCHAR(255) DEFAULT NULL,',
    'form VARCHAR(50),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE(uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(who_stage),',
    'INDEX(anc_visit_number),',
    'INDEX(final_test_result),',
    'INDEX(tb_screening),',
    'INDEX(syphilis_test_status),',
    'INDEX(cacx_screening),',
    'INDEX(next_appointment_date),',
    'INDEX(arv_status)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_mch_antenatal_visit table';


-- ------------ create table etl_mchs_delivery-----------------------

-- sql
SET @drop_etl_mchs_delivery = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_mchs_delivery`;');
PREPARE stmt FROM @drop_etl_mchs_delivery; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_mchs_delivery` (',
    'uuid CHAR(38),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'number_of_anc_visits INT(11),',
    'vaginal_examination INT(11),',
    'uterotonic_given INT(11),',
    'chlohexidine_applied_on_code_stump INT(11),',
    'vitamin_K_given INT(11),',
    'kangaroo_mother_care_given INT(11),',
    'testing_done_in_the_maternity_hiv_status INT(11),',
    'infant_provided_with_arv_prophylaxis INT(11),',
    'mother_on_haart_during_anc INT(11),',
    'mother_started_haart_at_maternity INT(11),',
    'vdrl_rpr_results INT(11),',
    'date_of_last_menstrual_period DATE,',
    'estimated_date_of_delivery DATE,',
    'reason_for_referral VARCHAR(100),',
    'admission_number VARCHAR(50),',
    'duration_of_pregnancy DOUBLE,',
    'mode_of_delivery INT(11),',
    'date_of_delivery DATETIME,',
    'blood_loss DOUBLE,',
    'condition_of_mother INT(11),',
    'delivery_outcome VARCHAR(255),',
    'apgar_score_1min DOUBLE,',
    'apgar_score_5min DOUBLE,',
    'apgar_score_10min DOUBLE,',
    'resuscitation_done INT(11),',
    'place_of_delivery INT(11),',
    'delivery_assistant VARCHAR(100),',
    'counseling_on_infant_feeding INT(11),',
    'counseling_on_exclusive_breastfeeding INT(11),',
    'counseling_on_infant_feeding_for_hiv_infected INT(11),',
    'mother_decision INT(11),',
    'placenta_complete INT(11),',
    'maternal_death_audited INT(11),',
    'cadre INT(11),',
    'delivery_complications INT(11),',
    'coded_delivery_complications INT(11),',
    'other_delivery_complications VARCHAR(100),',
    'duration_of_labor INT(11),',
    'baby_sex INT(11),',
    'baby_condition INT(11),',
    'teo_given INT(11),',
    'birth_weight INT(11),',
    'bf_within_one_hour INT(11),',
    'birth_with_deformity INT(11),',
    'type_of_birth_deformity INT(11),',
    'test_1_kit_name VARCHAR(50),',
    'test_1_kit_lot_no VARCHAR(50) DEFAULT NULL,',
    'test_1_kit_expiry DATE DEFAULT NULL,',
    'test_1_result VARCHAR(50) DEFAULT NULL,',
    'test_2_kit_name VARCHAR(50),',
    'test_2_kit_lot_no VARCHAR(50) DEFAULT NULL,',
    'test_2_kit_expiry DATE DEFAULT NULL,',
    'test_2_result VARCHAR(50) DEFAULT NULL,',
    'test_3_kit_name VARCHAR(50),',
    'test_3_kit_lot_no VARCHAR(50) DEFAULT NULL,',
    'test_3_kit_expiry DATE DEFAULT NULL,',
    'test_3_result VARCHAR(50) DEFAULT NULL,',
    'final_test_result VARCHAR(50) DEFAULT NULL,',
    'patient_given_result VARCHAR(50) DEFAULT NULL,',
    'partner_hiv_tested INT(11),',
    'partner_hiv_status INT(11),',
    'prophylaxis_given INT(11),',
    'baby_azt_dispensed INT(11),',
    'baby_nvp_dispensed INT(11),',
    'clinical_notes VARCHAR(200) DEFAULT NULL,',
    'stimulation_done INT(11),',
    'suction_done INT(11),',
    'oxygen_given INT(11),',
    'bag_mask_ventilation_provided INT(11),',
    'induction_done INT(11),',
    'artificial_rapture_done INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(final_test_result),',
    'INDEX(baby_sex),',
    'INDEX(partner_hiv_tested),',
    'INDEX(partner_hiv_status)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT "Successfully created etl_mchs_delivery table";

-- ------------ create table etl_mchs_discharge-----------------------

SET @drop_etl_mchs_discharge = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_mchs_discharge`;');
PREPARE stmt FROM @drop_etl_mchs_discharge; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_mchs_discharge` (',
    'uuid CHAR(38),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'counselled_on_feeding INT(11),',
    'baby_status INT(11),',
    'vitamin_A_dispensed INT(11),',
    'birth_notification_number VARCHAR(100),',
    'condition_of_mother VARCHAR(100),',
    'discharge_date DATE,',
    'referred_from INT(11),',
    'referred_to INT(11),',
    'clinical_notes VARCHAR(200) DEFAULT NULL,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(baby_status),',
    'INDEX(discharge_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT "Successfully created etl_mchs_discharge table";

-- ------------ create table etl_mch_postnatal_visit-----------------------
-- sql
SET @drop_etl_mch_postnatal = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_mch_postnatal_visit`;');
PREPARE stmt FROM @drop_etl_mch_postnatal; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_mch_postnatal_visit` (',
    'uuid CHAR(38),',
    'patient_id INT(11) NOT NULL ,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'provider INT(11),',
    'pnc_register_no VARCHAR(50),',
    'pnc_visit_no INT(11),',
    'delivery_date DATE,',
    'mode_of_delivery INT(11),',
    'place_of_delivery INT(11),',
    'visit_timing_mother INT(11),',
    'visit_timing_baby INT(11),',
    'delivery_outcome INT(11),',
    'temperature DOUBLE,',
    'pulse_rate DOUBLE,',
    'systolic_bp DOUBLE,',
    'diastolic_bp DOUBLE,',
    'respiratory_rate DOUBLE,',
    'oxygen_saturation INT(11),',
    'weight DOUBLE,',
    'height DOUBLE,',
    'muac DOUBLE,',
    'hemoglobin DOUBLE,',
    'arv_status INT(11),',
    'general_condition INT(11),',
    'breast INT(11),',
    'cs_scar INT(11),',
    'gravid_uterus INT(11),',
    'episiotomy INT(11),',
    'lochia INT(11),',
    'counselled_on_infant_feeding INT(11),',
    'pallor INT(11),',
    'pallor_severity INT(11),',
    'pph INT(11),',
    'mother_hiv_status INT(11),',
    'condition_of_baby INT(11),',
    'baby_feeding_method INT(11),',
    'umblical_cord INT(11),',
    'baby_immunization_started INT(11),',
    'family_planning_counseling INT(11),',
    'other_maternal_complications VARCHAR(255),',
    'uterus_examination INT(11),',
    'uterus_cervix_examination VARCHAR(100),',
    'vaginal_examination VARCHAR(100),',
    'parametrial_examination VARCHAR(100),',
    'external_genitalia_examination VARCHAR(100),',
    'ovarian_examination VARCHAR(100),',
    'pelvic_lymph_node_exam VARCHAR(100),',
    'hiv_test_type VARCHAR(50),',
    'hiv_test_timing VARCHAR(50),',
    'test_1_kit_name VARCHAR(50),',
    'test_1_kit_lot_no VARCHAR(50) DEFAULT NULL,',
    'test_1_kit_expiry DATE DEFAULT NULL,',
    'test_1_result VARCHAR(50) DEFAULT NULL,',
    'test_2_kit_name VARCHAR(50),',
    'test_2_kit_lot_no VARCHAR(50) DEFAULT NULL,',
    'test_2_kit_expiry DATE DEFAULT NULL,',
    'test_2_result VARCHAR(50) DEFAULT NULL,',
    'test_3_kit_name VARCHAR(50),',
    'test_3_kit_lot_no VARCHAR(50) DEFAULT NULL,',
    'test_3_kit_expiry DATE DEFAULT NULL,',
    'test_3_result VARCHAR(50) DEFAULT NULL,',
    'final_test_result VARCHAR(50) DEFAULT NULL,',
    'syphilis_results INT(11) DEFAULT NULL,',
    'patient_given_result VARCHAR(50) DEFAULT NULL,',
    'couple_counselled INT(11),',
    'partner_hiv_tested INT(11),',
    'partner_hiv_status INT(11),',
    'pnc_hiv_test_timing_mother INT(11),',
    'mother_haart_given INT(11),',
    'prophylaxis_given INT(11),',
    'infant_prophylaxis_timing INT(11),',
    'baby_azt_dispensed INT(11),',
    'baby_nvp_dispensed INT(11),',
    'pnc_exercises INT(11),',
    'maternal_condition INT(11),',
    'iron_supplementation INT(11),',
    'fistula_screening INT(11),',
    'cacx_screening INT(11),',
    'cacx_screening_method INT(11),',
    'family_planning_status INT(11),',
    'family_planning_method VARCHAR(1000),',
    'referred_from INT(11),',
    'referred_to INT(11),',
    'referral_reason VARCHAR(255) DEFAULT NULL,',
    'clinical_notes VARCHAR(200) DEFAULT NULL,',
    'appointment_date DATE DEFAULT NULL,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(arv_status),',
    'INDEX(mother_hiv_status)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT "Successfully created etl_mch_postnatal_visit table";

-- ------------ create table etl_hei_enrollment-----------------------
SET @drop_etl_hei = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_hei_enrollment`;');
PREPARE stmt FROM @drop_etl_hei; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_hei_enrollment` (',
    'serial_no INT(11) NOT NULL AUTO_INCREMENT,',
    'uuid CHAR(38),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'provider INT(11),',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'child_exposed INT(11),',
    'hei_id_number VARCHAR(50),',
    'spd_number VARCHAR(50),',
    'birth_weight DOUBLE,',
    'gestation_at_birth DOUBLE,',
    'birth_type VARCHAR(50),',
    'date_first_seen DATE,',
    'birth_notification_number VARCHAR(50),',
    'birth_certificate_number VARCHAR(50),',
    'need_for_special_care INT(11),',
    'reason_for_special_care INT(11),',
    'referral_source INT(11),',
    'transfer_in INT(11),',
    'transfer_in_date DATE,',
    'facility_transferred_from VARCHAR(50),',
    'district_transferred_from VARCHAR(50),',
    'date_first_enrolled_in_hei_care DATE,',
    'arv_prophylaxis INT(11),',
    'mother_breastfeeding INT(11),',
    'mother_on_NVP_during_breastfeeding INT(11),',
    'TB_contact_history_in_household INT(11),',
    'infant_mother_link INT(11),',
    'mother_alive INT(11),',
    'mother_on_pmtct_drugs INT(11),',
    'mother_on_drug INT(11),',
    'mother_on_art_at_infant_enrollment INT(11),',
    'mother_drug_regimen INT(11),',
    'infant_prophylaxis INT(11),',
    'parent_ccc_number VARCHAR(50),',
    'mode_of_delivery INT(11),',
    'place_of_delivery INT(11),',
    'birth_length INT(11),',
    'birth_order INT(11),',
    'health_facility_name VARCHAR(50),',
    'date_of_birth_notification DATE,',
    'date_of_birth_registration DATE,',
    'birth_registration_place VARCHAR(50),',
    'permanent_registration_serial VARCHAR(50),',
    'mother_facility_registered VARCHAR(50),',
    'exit_date DATE,',
    'exit_reason INT(11),',
    'hiv_status_at_exit VARCHAR(50),',
    'encounter_type VARCHAR(250),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'CONSTRAINT unique_serial_no UNIQUE (serial_no),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(transfer_in),',
    'INDEX(child_exposed),',
    'INDEX(need_for_special_care),',
    'INDEX(reason_for_special_care),',
    'INDEX(serial_no)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_hei_enrollment table';


-- ------------ create table etl_hei_follow_up_visit-----------------------
SET @drop_etl_hei_followup = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_hei_follow_up_visit`;');
PREPARE stmt FROM @drop_etl_hei_followup; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_hei_follow_up_visit` (',
    'uuid CHAR(38),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL ,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'weight DOUBLE,',
    'height DOUBLE,',
    'muac INT(11),',
    'primary_caregiver INT(11),',
    'revisit_this_year INT(11),',
    'height_length INT(11),',
    'referred INT(11),',
    'referral_reason VARCHAR(255),',
    'danger_signs INT(11),',
    'infant_feeding INT(11),',
    'stunted INT(11),',
    'tb_assessment_outcome INT(11),',
    'social_smile_milestone INT(11),',
    'head_control_milestone INT(11),',
    'response_to_sound_milestone INT(11),',
    'hand_extension_milestone INT(11),',
    'sitting_milestone INT(11),',
    'walking_milestone INT(11),',
    'standing_milestone INT(11),',
    'talking_milestone INT(11),',
    'review_of_systems_developmental INT(11),',
    'weight_category INT(11),',
    'followup_type INT(11),',
    'dna_pcr_sample_date DATE,',
    'dna_pcr_contextual_status INT(11),',
    'dna_pcr_result INT(11),',
    'dna_pcr_dbs_sample_code VARCHAR(100),',
    'dna_pcr_results_date DATE,',
    'azt_given INT(11),',
    'nvp_given INT(11),',
    'ctx_given INT(11),',
    'multi_vitamin_given INT(11),',
    'first_antibody_sample_date DATE,',
    'first_antibody_result INT(11),',
    'first_antibody_dbs_sample_code VARCHAR(100),',
    'first_antibody_result_date DATE,',
    'final_antibody_sample_date DATE,',
    'final_antibody_result INT(11),',
    'final_antibody_dbs_sample_code VARCHAR(100),',
    'final_antibody_result_date DATE,',
    'tetracycline_ointment_given INT(11),',
    'pupil_examination INT(11),',
    'sight_examination INT(11),',
    'squint INT(11),',
    'deworming_drug INT(11),',
    'dosage INT(11),',
    'unit VARCHAR(100),',
    'vitaminA_given INT(11),',
    'disability INT(11),',
    'next_appointment_date DATE,',
    'comments VARCHAR(100),',
    'referred_from INT(11),',
    'referred_to INT(11),',
    'counselled_on INT(11),',
    'MNPS_Supplementation INT(11),',
    'LLIN INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(infant_feeding)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_hei_follow_up_visit table';


-- ------- create table etl_hei_immunization table-----------------------------------------
SET @drop_etl_immunization = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_immunization`;');
PREPARE stmt FROM @drop_etl_immunization; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_immunization` (',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'patient_id INT(11) NOT NULL,',
    'visit_date DATE,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'created_by INT(11),',
    'BCG VARCHAR(50),',
    'OPV_birth VARCHAR(50),',
    'OPV_1 VARCHAR(50),',
    'OPV_2 VARCHAR(50),',
    'OPV_3 VARCHAR(50),',
    'IPV VARCHAR(50),',
    'DPT_Hep_B_Hib_1 VARCHAR(50),',
    'DPT_Hep_B_Hib_2 VARCHAR(50),',
    'DPT_Hep_B_Hib_3 VARCHAR(50),',
    'PCV_10_1 VARCHAR(50),',
    'PCV_10_2 VARCHAR(50),',
    'PCV_10_3 VARCHAR(50),',
    'ROTA_1 VARCHAR(50),',
    'ROTA_2 VARCHAR(50),',
    'ROTA_3 VARCHAR(50),',
    'Measles_rubella_1 VARCHAR(50),',
    'Measles_rubella_2 VARCHAR(50),',
    'Yellow_fever VARCHAR(50),',
    'Measles_6_months VARCHAR(50),',
    'VitaminA_6_months VARCHAR(50),',
    'VitaminA_1_yr VARCHAR(50),',
    'VitaminA_1_and_half_yr VARCHAR(50),',
    'VitaminA_2_yr VARCHAR(50),',
    'VitaminA_2_to_5_yr VARCHAR(50),',
    'HPV_1 VARCHAR(50),',
    'HPV_2 VARCHAR(50),',
    'HPV_3 VARCHAR(50),',
    'influenza VARCHAR(50),',
    'sequence VARCHAR(50),',
    'fully_immunized INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'INDEX(visit_date),',
    'INDEX(encounter_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Successfully created etl_immunization table';

-- ------------ create table etl_tb_enrollment-----------------------
-- sql
SET @drop_etl_tb = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_tb_enrollment`;');
PREPARE stmt FROM @drop_etl_tb; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_tb_enrollment` (',
    'uuid CHAR(38),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'provider INT(11),',
    'date_treatment_started DATE,',
    'district VARCHAR(50),',
    'district_registration_number VARCHAR(20),',
    'referred_by INT(11),',
    'referral_date DATE,',
    'date_transferred_in DATE,',
    'facility_transferred_from VARCHAR(100),',
    'district_transferred_from VARCHAR(100),',
    'date_first_enrolled_in_tb_care DATE,',
    'weight DOUBLE,',
    'height DOUBLE,',
    'treatment_supporter VARCHAR(100),',
    'relation_to_patient INT(11),',
    'treatment_supporter_address VARCHAR(100),',
    'treatment_supporter_phone_contact VARCHAR(100),',
    'disease_classification INT(11),',
    'patient_classification INT(11),',
    'pulmonary_smear_result INT(11),',
    'has_extra_pulmonary_pleurial_effusion INT(11),',
    'has_extra_pulmonary_milliary INT(11),',
    'has_extra_pulmonary_lymph_node INT(11),',
    'has_extra_pulmonary_menengitis INT(11),',
    'has_extra_pulmonary_skeleton INT(11),',
    'has_extra_pulmonary_abdominal INT(11),',
    'has_extra_pulmonary_other VARCHAR(100),',
    'treatment_outcome INT(11),',
    'treatment_outcome_date DATE,',
    'date_of_discontinuation DATETIME,',
    'discontinuation_reason INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX (visit_date),',
    'INDEX (encounter_id),',
    'INDEX (patient_id),',
    'INDEX (disease_classification),',
    'INDEX (patient_classification),',
    'INDEX (pulmonary_smear_result),',
    'INDEX (date_first_enrolled_in_tb_care)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_tb_enrollment table';

-- ------------ create table etl_tb_follow_up_visit-----------------------
-- sql
SET @drop_etl_tb_followup = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_tb_follow_up_visit`;');
PREPARE stmt FROM @drop_etl_tb_followup; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_tb_follow_up_visit` (',
    'uuid CHAR(38),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'spatum_test INT(11),',
    'spatum_result INT(11),',
    'result_serial_number VARCHAR(20),',
    'quantity DOUBLE,',
    'date_test_done DATE,',
    'bacterial_colonie_growth INT(11),',
    'number_of_colonies DOUBLE,',
    'resistant_s INT(11),',
    'resistant_r INT(11),',
    'resistant_inh INT(11),',
    'resistant_e INT(11),',
    'sensitive_s INT(11),',
    'sensitive_r INT(11),',
    'sensitive_inh INT(11),',
    'sensitive_e INT(11),',
    'test_date DATE,',
    'hiv_status INT(11),',
    'next_appointment_date DATE,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(hiv_status)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_tb_follow_up_visit table';

-- ------------ create table etl_tb_screening-----------------------
-- sql
SET @drop_etl_tb_screening = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_tb_screening`;');
PREPARE stmt FROM @drop_etl_tb_screening; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_tb_screening` (',
    'uuid CHAR(38),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'cough_for_2wks_or_more INT(11),',
    'confirmed_tb_contact INT(11),',
    'fever_for_2wks_or_more INT(11),',
    'noticeable_weight_loss INT(11),',
    'night_sweat_for_2wks_or_more INT(11),',
    'lethargy INT(11),',
    'spatum_smear_ordered INT(11) DEFAULT NULL,',
    'chest_xray_ordered INT(11) DEFAULT NULL,',
    'genexpert_ordered INT(11) DEFAULT NULL,',
    'spatum_smear_result INT(11) DEFAULT NULL,',
    'chest_xray_result INT(11) DEFAULT NULL,',
    'genexpert_result INT(11) DEFAULT NULL,',
    'referral INT(11) DEFAULT NULL,',
    'clinical_tb_diagnosis INT(11) DEFAULT NULL,',
    'resulting_tb_status INT(11),',
    'contact_invitation INT(11) DEFAULT NULL,',
    'evaluated_for_ipt INT(11) DEFAULT NULL,',
    'started_anti_TB INT(11),',
    'tb_treatment_start_date DATE DEFAULT NULL,',
    'tb_prophylaxis VARCHAR(50),',
    'notes VARCHAR(100),',
    'person_present INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(cough_for_2wks_or_more),',
    'INDEX(confirmed_tb_contact),',
    'INDEX(noticeable_weight_loss),',
    'INDEX(night_sweat_for_2wks_or_more),',
    'INDEX(resulting_tb_status)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_tb_screening table';






























UPDATE `', etl_schema, '`.etl_script_status SET stop_time=NOW() where id= script_id;

END $$
DELIMITER ;
