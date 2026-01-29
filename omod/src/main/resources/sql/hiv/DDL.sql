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
    IF current_schema IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No database selected. Use "USE openmrs_..."';
END IF;

    SET tenant_suffix = SUBSTRING_INDEX(current_schema, 'openmrs_', -1);
    SET etl_schema        = CONCAT('`kenyaemr_etl_', tenant_suffix, '`');
    SET datatools_schema  = CONCAT('`kenyaemr_datatools_', tenant_suffix, '`');
    SET script_status_table = CONCAT(etl_schema, '.`etl_script_status`');
    SET FOREIGN_KEY_CHECKS = 0;
    SET @sql = CONCAT('DROP DATABASE IF EXISTS ', etl_schema);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT('DROP DATABASE IF EXISTS ', datatools_schema);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET FOREIGN_KEY_CHECKS = 1;
    SET @sql = CONCAT('CREATE DATABASE ', etl_schema, ' DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT('CREATE DATABASE ', datatools_schema, ' DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT('DROP TABLE IF EXISTS ', script_status_table);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
        'CREATE TABLE ', script_status_table, ' (',
        '  id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,',
        '  script_name VARCHAR(50) DEFAULT NULL,',
        '  start_time DATETIME DEFAULT NULL,',
        '  stop_time DATETIME DEFAULT NULL,',
        '  error VARCHAR(255) DEFAULT NULL',
        ') ENGINE=InnoDB;'
    );
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT('INSERT INTO ', script_status_table, ' (script_name, start_time) VALUES (''initial_creation_of_tables'', NOW())');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET script_id = LAST_INSERT_ID();

    SET @sql = CONCAT(
        'CREATE TABLE ', etl_schema, '.`etl_patient_demographics` (',
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
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;


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

-- ------------ create table etl_patients_booked_today-----------------------
-- sql
SET @drop_etl_patients_booked_today = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_patients_booked_today`;');
PREPARE stmt FROM @drop_etl_patients_booked_today; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_patients_booked_today` (',
    'id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,',
    'patient_id INT(11) NOT NULL,',
    'last_visit_date DATE,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'INDEX(patient_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Successfully created etl_patients_booked_today table';

-- ------------ create table etl_missed_appointments-----------------------

SET @drop_etl_missed = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_missed_appointments`;');
PREPARE stmt FROM @drop_etl_missed; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_missed_appointments` (',
    'id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,',
    'patient_id INT(11) NOT NULL,',
    'last_tca_date DATE,',
    'last_visit_date DATE,',
    'last_encounter_type VARCHAR(100),',
    'days_since_last_visit INT(11),',
    'date_table_created DATE,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'INDEX(patient_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_missed_appointments table';


-- --------------------------- CREATE drug_event table ---------------------

SET @drop_etl_drug_event = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_drug_event`;');
PREPARE stmt FROM @drop_etl_drug_event; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_drug_event` (',
    'uuid CHAR(38),',
    'patient_id INT(11) NOT NULL,',
    'date_started DATE,',
    'visit_date DATE,',
    'provider INT(11),',
    'encounter_id INT(11) NOT NULL,',
    'program VARCHAR(50),',
    'regimen MEDIUMTEXT,',
    'regimen_name VARCHAR(100),',
    'regimen_line VARCHAR(50),',
    'discontinued INT(11),',
    'regimen_discontinued VARCHAR(255),',
    'regimen_stopped INT(11),',
    'date_discontinued DATE,',
    'reason_discontinued INT(11),',
    'reason_discontinued_other VARCHAR(100),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(patient_id),',
    'INDEX(date_started),',
    'INDEX(date_discontinued),',
    'INDEX(patient_id, date_started)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_drug_event table';

-- -------------------------- CREATE hts_test table ---------------------------------

SET @drop_etl_hts = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_hts_test`;');
PREPARE stmt FROM @drop_etl_hts; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_hts_test` (',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'encounter_uuid CHAR(38) NOT NULL,',
    'encounter_location INT(11) NOT NULL,',
    'creator INT(11) NOT NULL,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'visit_date DATE,',
    'test_type INT(11) DEFAULT NULL,',
    'population_type VARCHAR(50),',
    'key_population_type VARCHAR(50),',
    'priority_population_type VARCHAR(50),',
    'ever_tested_for_hiv VARCHAR(10),',
    'months_since_last_test INT(11),',
    'patient_disabled VARCHAR(50),',
    'disability_type VARCHAR(255),',
    'patient_consented VARCHAR(50) DEFAULT NULL,',
    'client_tested_as VARCHAR(50),',
    'setting VARCHAR(50),',
    'approach VARCHAR(50),',
    'test_strategy VARCHAR(50),',
    'hts_entry_point VARCHAR(50),',
    'hts_risk_category VARCHAR(50),',
    'hts_risk_score DOUBLE,',
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
    'syphillis_test_result VARCHAR(50) DEFAULT NULL,',
    'patient_given_result VARCHAR(50) DEFAULT NULL,',
    'couple_discordant VARCHAR(100) DEFAULT NULL,',
    'referred INT(10) DEFAULT NULL,',
    'referral_for VARCHAR(100) DEFAULT NULL,',
    'referral_facility VARCHAR(200) DEFAULT NULL,',
    'other_referral_facility VARCHAR(200) DEFAULT NULL,',
    'neg_referral_for VARCHAR(500) DEFAULT NULL,',
    'neg_referral_specify VARCHAR(500) DEFAULT NULL,',
    'tb_screening VARCHAR(20) DEFAULT NULL,',
    'patient_had_hiv_self_test VARCHAR(50) DEFAULT NULL,',
    'remarks VARCHAR(255) DEFAULT NULL,',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'INDEX(patient_id),',
    'INDEX(visit_id),',
    'INDEX(tb_screening),',
    'INDEX(visit_date),',
    'INDEX(population_type),',
    'INDEX(hts_risk_category),',
    'INDEX(hts_risk_score),',
    'INDEX(test_type),',
    'INDEX(final_test_result),',
    'INDEX(couple_discordant),',
    'INDEX(test_1_kit_name),',
    'INDEX(test_2_kit_name),',
    'INDEX(test_3_kit_name)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_hts_test table';

-- ------------- CREATE HTS LINKAGE AND REFERRALS ------------------------

-- sql
SET @drop_etl_hts_referral = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_hts_referral_and_linkage`;');
PREPARE stmt FROM @drop_etl_hts_referral; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_hts_referral_and_linkage` (',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'encounter_uuid CHAR(38) NOT NULL,',
    'encounter_location INT(11) NOT NULL,',
    'creator INT(11) NOT NULL,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'visit_date DATE,',
    'tracing_type VARCHAR(50),',
    'tracing_status VARCHAR(100),',
    'ccc_number VARCHAR(100),',
    'referral_facility VARCHAR(200) DEFAULT NULL,',
    'facility_linked_to VARCHAR(200) DEFAULT NULL,',
    'enrollment_date DATE,',
    'art_start_date DATE,',
    'provider_handed_to VARCHAR(100),',
    'cadre VARCHAR(100),',
    'remarks VARCHAR(255),',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'INDEX(patient_id),',
    'INDEX(visit_id),',
    'INDEX(visit_date),',
    'INDEX(tracing_type),',
    'INDEX(tracing_status)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_hts_referral_and_linkage table';


-- -------------- create referral form ----------------------------

SET @drop_etl_hts_referral = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_hts_referral`;');
PREPARE stmt FROM @drop_etl_hts_referral; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_hts_referral` (',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'encounter_uuid CHAR(38) NOT NULL,',
    'encounter_location INT(11) NOT NULL,',
    'creator INT(11) NOT NULL,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'visit_date DATE,',
    'facility_referred_to VARCHAR(200) DEFAULT NULL,',
    'date_to_enrol DATE DEFAULT NULL,',
    'remarks VARCHAR(255) DEFAULT NULL,',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'INDEX(patient_id),',
    'INDEX(visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_hts_referral table';


-- ------------ create table etl_ipt_screening-----------------------

SET @drop_etl_ipt_screening = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_ipt_screening`;');
PREPARE stmt FROM @drop_etl_ipt_screening; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_ipt_screening` (',
    'uuid CHAR(38),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11),',
    'obs_id INT(11) NOT NULL PRIMARY KEY,',
    'cough INT(11) DEFAULT NULL,',
    'fever INT(11) DEFAULT NULL,',
    'weight_loss_poor_gain INT(11) DEFAULT NULL,',
    'night_sweats INT(11) DEFAULT NULL,',
    'contact_with_tb_case INT(11) DEFAULT NULL,',
    'lethargy INT(11) DEFAULT NULL,',
    'yellow_urine INT(11),',
    'numbness_bs_hands_feet INT(11),',
    'eyes_yellowness INT(11),',
    'upper_rightQ_abdomen_tenderness INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'INDEX(visit_date),',
    'INDEX(patient_id),',
    'INDEX(encounter_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_ipt_screening table';


-- ------------ create table etl_ipt_follow_up -----------------------

SET @drop_etl_ipt_followup = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_ipt_follow_up`;');
PREPARE stmt FROM @drop_etl_ipt_followup; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_ipt_follow_up` (',
    'uuid CHAR(38),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'provider INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'ipt_due_date DATE DEFAULT NULL,',
    'date_collected_ipt DATE DEFAULT NULL,',
    'weight DOUBLE,',
    'hepatotoxity VARCHAR(100) DEFAULT NULL,',
    'peripheral_neuropathy VARCHAR(100) DEFAULT NULL,',
    'rash VARCHAR(100),',
    'has_other_symptoms VARCHAR(50),',
    'adherence VARCHAR(100),',
    'action_taken VARCHAR(100),',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(hepatotoxity),',
    'INDEX(peripheral_neuropathy),',
    'INDEX(rash),',
    'INDEX(adherence)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_ipt_follow_up table';



SET @drop_etl_ccc_defaulter_tracing = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_ccc_defaulter_tracing`;');
PREPARE stmt FROM @drop_etl_ccc_defaulter_tracing; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_ccc_defaulter_tracing` (',
    'uuid CHAR(38),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'tracing_type INT(11),',
    'missed_appointment_date DATE,',
    'reason_for_missed_appointment INT(11),',
    'non_coded_missed_appointment_reason VARCHAR(100),',
    'tracing_outcome INT(11),',
    'reason_not_contacted INT(11),',
    'attempt_number INT(11),',
    'is_final_trace INT(11),',
    'true_status INT(11),',
    'cause_of_death INT(11),',
    'comments VARCHAR(100),',
    'booking_date DATE,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(missed_appointment_date),',
    'INDEX(true_status),',
    'INDEX(cause_of_death),',
    'INDEX(tracing_type)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_ccc_defaulter_tracing table';

-- ------------ create table etl_ART_preparation-----------------------

SET @drop_etl_art_prep = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_ART_preparation`;');
PREPARE stmt FROM @drop_etl_art_prep; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_ART_preparation` (',
    'uuid CHAR(38),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'provider INT(11),',
    'understands_hiv_art_benefits VARCHAR(10),',
    'screened_negative_substance_abuse VARCHAR(10),',
    'screened_negative_psychiatric_illness VARCHAR(10),',
    'HIV_status_disclosure VARCHAR(10),',
    'trained_drug_admin VARCHAR(10),',
    'informed_drug_side_effects VARCHAR(10),',
    'caregiver_committed VARCHAR(10),',
    'adherance_barriers_identified VARCHAR(10),',
    'caregiver_location_contacts_known VARCHAR(10),',
    'ready_to_start_art VARCHAR(10),',
    'identified_drug_time VARCHAR(10),',
    'treatment_supporter_engaged VARCHAR(10),',
    'support_grp_meeting_awareness VARCHAR(10),',
    'enrolled_in_reminder_system VARCHAR(10),',
    'other_support_systems VARCHAR(10),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(ready_to_start_art)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_ART_preparation table';

-- ------------ create table etl_enhanced_adherence-----------------------

SET @drop_etl_enhanced_adherence = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_enhanced_adherence`;');
PREPARE stmt FROM @drop_etl_enhanced_adherence; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_enhanced_adherence` (',
    'uuid CHAR(38),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'provider INT(11),',
    'session_number INT(11),',
    'first_session_date DATE,',
    'pill_count INT(11),',
    'MMAS4_1_forgets_to_take_meds VARCHAR(255),',
    'MMAS4_2_careless_taking_meds VARCHAR(255),',
    'MMAS4_3_stops_on_reactive_meds VARCHAR(255),',
    'MMAS4_4_stops_meds_on_feeling_good VARCHAR(255),',
    'MMSA8_1_took_meds_yesterday VARCHAR(255),',
    'MMSA8_2_stops_meds_on_controlled_symptoms VARCHAR(255),',
    'MMSA8_3_struggles_to_comply_tx_plan VARCHAR(255),',
    'MMSA8_4_struggles_remembering_taking_meds VARCHAR(255),',
    'arv_adherence VARCHAR(50),',
    'has_vl_results VARCHAR(10),',
    'vl_results_suppressed VARCHAR(10),',
    'vl_results_feeling VARCHAR(255),',
    'cause_of_high_vl VARCHAR(255),',
    'way_forward VARCHAR(255),',
    'patient_hiv_knowledge VARCHAR(255),',
    'patient_drugs_uptake VARCHAR(255),',
    'patient_drugs_reminder_tools VARCHAR(255),',
    'patient_drugs_uptake_during_travels VARCHAR(255),',
    'patient_drugs_side_effects_response VARCHAR(255),',
    'patient_drugs_uptake_most_difficult_times VARCHAR(255),',
    'patient_drugs_daily_uptake_feeling VARCHAR(255),',
    'patient_ambitions VARCHAR(255),',
    'patient_has_people_to_talk VARCHAR(10),',
    'patient_enlisting_social_support VARCHAR(255),',
    'patient_income_sources VARCHAR(255),',
    'patient_challenges_reaching_clinic VARCHAR(10),',
    'patient_worried_of_accidental_disclosure VARCHAR(10),',
    'patient_treated_differently VARCHAR(10),',
    'stigma_hinders_adherence VARCHAR(10),',
    'patient_tried_faith_healing VARCHAR(10),',
    'patient_adherence_improved VARCHAR(10),',
    'patient_doses_missed VARCHAR(10),',
    'review_and_barriers_to_adherence VARCHAR(255),',
    'other_referrals VARCHAR(10),',
    'appointments_honoured VARCHAR(10),',
    'referral_experience VARCHAR(255),',
    'home_visit_benefit VARCHAR(10),',
    'adherence_plan VARCHAR(255),',
    'next_appointment_date DATE,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT "Successfully created etl_enhanced_adherence table";


-- ------------ create table etl_patient_triage-----------------------

SET @drop_etl_triage = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_patient_triage`;');
PREPARE stmt FROM @drop_etl_triage; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_patient_triage` (',
    'uuid CHAR(38),',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'patient_id INT(11) NOT NULL,',
    'location_id INT(11) DEFAULT NULL,',
    'visit_date DATE,',
    'visit_id INT(11),',
    'encounter_provider INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'visit_reason VARCHAR(255),',
    'complaint_today VARCHAR(10),',
    'complaint_duration DOUBLE,',
    'weight DOUBLE,',
    'height DOUBLE,',
    'systolic_pressure DOUBLE,',
    'diastolic_pressure DOUBLE,',
    'temperature DOUBLE,',
    'temperature_collection_mode INT(11),',
    'pulse_rate DOUBLE,',
    'respiratory_rate DOUBLE,',
    'oxygen_saturation DOUBLE,',
    'oxygen_saturation_collection_mode INT(11),',
    'muac DOUBLE,',
    'z_score_absolute DOUBLE DEFAULT NULL,',
    'z_score INT(11),',
    'nutritional_status INT(11) DEFAULT NULL,',
    'nutritional_intervention INT(11) DEFAULT NULL,',
    'last_menstrual_period DATE,',
    'hpv_vaccinated INT(11),',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE(uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_patient_triage table';

-- ------------ create table etl_generalized_anxiety_disorder-----------------------

SET @drop_etl_gad = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_generalized_anxiety_disorder`;');
PREPARE stmt FROM @drop_etl_gad; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_generalized_anxiety_disorder` (',
    'uuid CHAR(38),',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'patient_id INT(11) NOT NULL,',
    'location_id INT(11) DEFAULT NULL,',
    'visit_date DATE,',
    'visit_id INT(11),',
    'encounter_provider INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'feeling_nervous_anxious INT(11),',
    'control_worrying INT(11),',
    'worrying_much INT(11),',
    'trouble_relaxing INT(11),',
    'being_restless INT(11),',
    'feeling_bad INT(11),',
    'feeling_afraid INT(11),',
    'assessment_outcome INT(11),',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_generalized_anxiety_disorder table';

-- ------------ create table etl_prep_behaviour_risk_assessment-----------------------
-- sql
SET @drop_etl_prep = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_prep_behaviour_risk_assessment`;');
PREPARE stmt FROM @drop_etl_prep; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_prep_behaviour_risk_assessment` (',
    'uuid CHAR(38),',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'sexual_partner_hiv_status VARCHAR(255),',
    'sexual_partner_on_art VARCHAR(10),',
    'risk VARCHAR(255),',
    'high_risk_partner VARCHAR(50),',
    'sex_with_multiple_partners VARCHAR(10),',
    'ipv_gbv VARCHAR(10),',
    'transactional_sex VARCHAR(10),',
    'recent_sti_infected VARCHAR(10),',
    'recurrent_pep_use VARCHAR(10),',
    'recurrent_sex_under_influence VARCHAR(10),',
    'inconsistent_no_condom_use VARCHAR(10),',
    'sharing_drug_needles VARCHAR(255),',
    'other_reasons VARCHAR(10),',
    'other_reason_specify VARCHAR(255),',
    'assessment_outcome VARCHAR(255),',
    'risk_education_offered VARCHAR(10),',
    'risk_reduction VARCHAR(10),',
    'willing_to_take_prep VARCHAR(10),',
    'reason_not_willing VARCHAR(255),',
    'risk_edu_offered VARCHAR(10),',
    'risk_education VARCHAR(255),',
    'referral_for_prevention_services VARCHAR(500),',
    'referral_facility VARCHAR(255),',
    'time_partner_hiv_positive_known VARCHAR(255),',
    'partner_enrolled_ccc VARCHAR(255),',
    'partner_ccc_number VARCHAR(255),',
    'partner_art_start_date DATE,',
    'serodiscordant_confirmation_date DATE,',
    'HIV_serodiscordant_duration_months INT(11),',
    'recent_unprotected_sex_with_positive_partner VARCHAR(10),',
    'children_with_hiv_positive_partner VARCHAR(255),',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(patient_id),',
    'INDEX(visit_date),',
    'INDEX(encounter_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Successfully created etl_prep_behaviour_risk_assessment table';


-- ------------ create table etl_prep_monthly_refill-----------------------

SET @drop_etl_prep_monthly_refill = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_prep_monthly_refill`;');
PREPARE stmt FROM @drop_etl_prep_monthly_refill; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_prep_monthly_refill` (',
    'uuid CHAR(38),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'assessed_for_behavior_risk VARCHAR(255),',
    'risk_for_hiv_positive_partner VARCHAR(255),',
    'client_assessment VARCHAR(255),',
    'adherence_assessment VARCHAR(255),',
    'poor_adherence_reasons VARCHAR(255),',
    'other_poor_adherence_reasons VARCHAR(255),',
    'adherence_counselling_done VARCHAR(10),',
    'prep_status VARCHAR(255),',
    'switching_option VARCHAR(255),',
    'switching_date DATE,',
    'prep_type VARCHAR(10),',
    'prescribed_prep_today VARCHAR(10),',
    'prescribed_regimen VARCHAR(10),',
    'prescribed_regimen_months VARCHAR(10),',
    'number_of_condoms_issued INT(11),',
    'prep_discontinue_reasons VARCHAR(255),',
    'prep_discontinue_other_reasons VARCHAR(255),',
    'appointment_given VARCHAR(10),',
    'next_appointment DATE,',
    'remarks VARCHAR(255),',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(next_appointment)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_prep_monthly_refill table';


-- ------------ create table etl_prep_discontinuation-----------------------
-- sql
SET @drop_etl_prep_disc = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_prep_discontinuation`;');
PREPARE stmt FROM @drop_etl_prep_disc; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_prep_discontinuation` (',
    'uuid CHAR(38),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'discontinue_reason VARCHAR(255),',
    'care_end_date DATE,',
    'last_prep_dose_date DATE,',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(discontinue_reason),',
    'INDEX(care_end_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_prep_discontinuation table';

-- ------------ create table etl_prep_enrollment-----------------------

SET @drop_etl_prep_enroll = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_prep_enrollment`;');
PREPARE stmt FROM @drop_etl_prep_enroll; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_prep_enrollment` (',
    'uuid CHAR(38),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'patient_type VARCHAR(255),',
    'population_type VARCHAR(255),',
    'kp_type VARCHAR(255),',
    'transfer_in_entry_point VARCHAR(255),',
    'referred_from VARCHAR(255),',
    'transit_from VARCHAR(255),',
    'transfer_in_date DATE,',
    'transfer_from VARCHAR(255),',
    'initial_enrolment_date DATE,',
    'date_started_prep_trf_facility DATE,',
    'previously_on_prep VARCHAR(10),',
    'prep_type VARCHAR(10),',
    'regimen VARCHAR(255),',
    'prep_last_date DATE,',
    'in_school VARCHAR(10),',
    'buddy_name VARCHAR(255),',
    'buddy_alias VARCHAR(255),',
    'buddy_relationship VARCHAR(255),',
    'buddy_phone VARCHAR(255),',
    'buddy_alt_phone VARCHAR(255),',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_prep_enrollment table';

-- ------------ create table etl_prep_followup-----------------------
-- sql
SET @drop_etl_prep_followup = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_prep_followup`;');
PREPARE stmt FROM @drop_etl_prep_followup; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_prep_followup` (',
    'uuid CHAR(38),',
    'form VARCHAR(50),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'sti_screened VARCHAR(10),',
    'genital_ulcer_disease VARCHAR(255),',
    'vaginal_discharge VARCHAR(255),',
    'cervical_discharge VARCHAR(255),',
    'pid VARCHAR(255),',
    'urethral_discharge VARCHAR(255),',
    'anal_discharge VARCHAR(255),',
    'other_sti_symptoms VARCHAR(255),',
    'sti_treated VARCHAR(10),',
    'vmmc_screened VARCHAR(10),',
    'vmmc_status VARCHAR(255),',
    'vmmc_referred VARCHAR(255),',
    'lmp DATE,',
    'menopausal_status VARCHAR(10),',
    'pregnant VARCHAR(10),',
    'edd DATE,',
    'planned_pregnancy VARCHAR(10),',
    'wanted_pregnancy VARCHAR(10),',
    'breastfeeding VARCHAR(10),',
    'fp_status VARCHAR(255),',
    'fp_method VARCHAR(500),',
    'ended_pregnancy VARCHAR(255),',
    'pregnancy_outcome VARCHAR(10),',
    'outcome_date DATE,',
    'defects VARCHAR(10),',
    'has_chronic_illness VARCHAR(10),',
    'adverse_reactions VARCHAR(255),',
    'known_allergies VARCHAR(10),',
    'hepatitisB_vaccinated VARCHAR(10),',
    'hepatitisB_treated VARCHAR(10),',
    'hepatitisC_vaccinated VARCHAR(10),',
    'hepatitisC_treated VARCHAR(10),',
    'hiv_signs VARCHAR(10),',
    'adherence_counselled VARCHAR(10),',
    'adherence_outcome VARCHAR(50),',
    'poor_adherence_reasons VARCHAR(255),',
    'other_poor_adherence_reasons VARCHAR(255),',
    'prep_contraindications VARCHAR(255),',
    'treatment_plan VARCHAR(255),',
    'reason_for_starting_prep INT(11),',
    'switching_option VARCHAR(255),',
    'switching_date DATE,',
    'prep_type VARCHAR(10),',
    'prescribed_PrEP VARCHAR(10),',
    'regimen_prescribed VARCHAR(255),',
    'months_prescribed_regimen INT(11),',
    'condoms_issued VARCHAR(10),',
    'number_of_condoms VARCHAR(10),',
    'appointment_given VARCHAR(10),',
    'appointment_date DATE,',
    'reason_no_appointment VARCHAR(255),',
    'clinical_notes VARCHAR(255),',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(form)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_prep_followup table';

-- ------------ create table etl_progress_note-----------------------

-- sql
SET @drop_etl_progress_note = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_progress_note`;');
PREPARE stmt FROM @drop_etl_progress_note; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_progress_note` (',
    'uuid CHAR(38),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'notes VARCHAR(255),',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_progress_note table';

-- ------------ create table etl_ipt_initiation -----------------------
-- sql
SET @drop_etl_ipt_initiation = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_ipt_initiation`;');
PREPARE stmt FROM @drop_etl_ipt_initiation; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_ipt_initiation` (',
    'uuid CHAR(38),',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'patient_id INT(11) NOT NULL,',
    'location_id INT(11) DEFAULT NULL,',
    'visit_date DATE,',
    'encounter_provider INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'ipt_indication INT(11),',
    'sub_county_reg_number VARCHAR(255),',
    'sub_county_reg_date DATE,',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_ipt_initiation table';

-- --------------------- creating ipt outcome table -------------------------------
-- sql
SET @drop_etl_ipt_outcome = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_ipt_outcome`;');
PREPARE stmt FROM @drop_etl_ipt_outcome; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_ipt_outcome` (',
    'uuid CHAR(38),',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'patient_id INT(11) NOT NULL,',
    'location_id INT(11) DEFAULT NULL,',
    'visit_date DATE,',
    'encounter_provider INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'outcome INT(11),',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(outcome),',
    'INDEX(patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_ipt_outcome table';

-- --------------------- creating hts tracing table -------------------------------
-- sql
SET @drop_etl_hts_linkage_tracing = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_hts_linkage_tracing`;');
PREPARE stmt FROM @drop_etl_hts_linkage_tracing; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_hts_linkage_tracing` (',
    'uuid CHAR(38),',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'patient_id INT(11) NOT NULL,',
    'location_id INT(11) DEFAULT NULL,',
    'visit_date DATE,',
    'encounter_provider INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'tracing_type INT(11),',
    'tracing_outcome INT(11),',
    'reason_not_contacted INT(11),',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(tracing_type),',
    'INDEX(tracing_outcome),',
    'INDEX(reason_not_contacted),',
    'INDEX(patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Successfully created etl_hts_linkage_tracing table';


-- ------------------------ create patient program table ---------------------


SET @drop_etl_patient_program = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_patient_program`;');
PREPARE stmt FROM @drop_etl_patient_program; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_patient_program` (',
    'uuid CHAR(38) NOT NULL PRIMARY KEY,',
    'patient_id INT(11) NOT NULL,',
    'location_id INT(11) DEFAULT NULL,',
    'program VARCHAR(100) NOT NULL,',
    'date_enrolled DATE NOT NULL,',
    'date_completed DATE DEFAULT NULL,',
    'outcome INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'voided INT(11),',
    'CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(date_enrolled),',
    'INDEX(date_completed),',
    'INDEX(patient_id),',
    'INDEX(outcome)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_patient_program table';

-- ------------------------ create person address table ---------------------

SET @drop_etl_person_address = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_person_address`;');
PREPARE stmt FROM @drop_etl_person_address; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_person_address` (',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `patient_id` INT(11) NOT NULL,',
  '  `county` VARCHAR(100) DEFAULT NULL,',
  '  `sub_county` VARCHAR(100) DEFAULT NULL,',
  '  `location` VARCHAR(100) DEFAULT NULL,',
  '  `ward` VARCHAR(100) DEFAULT NULL,',
  '  `sub_location` VARCHAR(100) DEFAULT NULL,',
  '  `village` VARCHAR(100) DEFAULT NULL,',
  '  `postal_address` VARCHAR(100) DEFAULT NULL,',
  '  `land_mark` VARCHAR(100) DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`uuid`),',
  '  CONSTRAINT `fk_person_address_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_person_address table';

-- --------------------- creating OTZ activity table -------------------------------

SET @drop_etl_otz_activity = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_otz_activity`;');
PREPARE stmt FROM @drop_etl_otz_activity; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_otz_activity` (',
  ' uuid CHAR(38),',
  ' encounter_id INT(11) NOT NULL PRIMARY KEY,',
  ' patient_id INT(11) NOT NULL,',
  ' location_id INT(11) DEFAULT NULL,',
  ' visit_id INT(11),',
  ' visit_date DATE,',
  ' encounter_provider INT(11),',
  ' date_created DATETIME NOT NULL,',
  ' date_last_modified DATETIME,',
  ' orientation VARCHAR(11) DEFAULT NULL,',
  ' leadership VARCHAR(11) DEFAULT NULL,',
  ' participation VARCHAR(11) DEFAULT NULL,',
  ' treatment_literacy VARCHAR(11) DEFAULT NULL,',
  ' transition_to_adult_care VARCHAR(11) DEFAULT NULL,',
  ' making_decision_future VARCHAR(11) DEFAULT NULL,',
  ' srh VARCHAR(11) DEFAULT NULL,',
  ' beyond_third_ninety VARCHAR(11) DEFAULT NULL,',
  ' attended_support_group VARCHAR(11) DEFAULT NULL,',
  ' remarks VARCHAR(255) DEFAULT NULL,',
  ' voided INT(11) DEFAULT 0,',
  ' CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id) ON DELETE RESTRICT ON UPDATE CASCADE,',
  ' CONSTRAINT unique_uuid UNIQUE (uuid),',
  ' INDEX (visit_date),',
  ' INDEX (encounter_id),',
  ' INDEX (patient_id),',
  ' INDEX (patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_otz_activity table';


-- --------------------- creating OTZ enrollment table -------------------------------
-- sql
SET @drop_etl_otz_enrollment = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_otz_enrollment`;');
PREPARE stmt FROM @drop_etl_otz_enrollment; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_otz_enrollment` (',
  ' uuid CHAR(38),',
  ' encounter_id INT(11) NOT NULL PRIMARY KEY,',
  ' patient_id INT(11) NOT NULL,',
  ' location_id INT(11) DEFAULT NULL,',
  ' visit_date DATE,',
  ' encounter_provider INT(11),',
  ' date_created DATETIME NOT NULL,',
  ' date_last_modified DATETIME,',
  ' orientation VARCHAR(11) DEFAULT NULL,',
  ' leadership VARCHAR(11) DEFAULT NULL,',
  ' participation VARCHAR(11) DEFAULT NULL,',
  ' treatment_literacy VARCHAR(11) DEFAULT NULL,',
  ' transition_to_adult_care VARCHAR(11) DEFAULT NULL,',
  ' making_decision_future VARCHAR(11) DEFAULT NULL,',
  ' srh VARCHAR(11) DEFAULT NULL,',
  ' beyond_third_ninety VARCHAR(11) DEFAULT NULL,',
  ' transfer_in VARCHAR(11) DEFAULT NULL,',
  ' voided INT(11) DEFAULT 0,',
  ' CONSTRAINT FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id) ON DELETE RESTRICT ON UPDATE CASCADE,',
  ' CONSTRAINT unique_uuid UNIQUE (uuid),',
  ' INDEX (visit_date),',
  ' INDEX (encounter_id),',
  ' INDEX (patient_id),',
  ' INDEX (patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_otz_enrollment table';

-- --------------------- creating OVC enrollment table -------------------------------

SET @drop_etl_ovc = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_ovc_enrolment`;');
PREPARE stmt FROM @drop_etl_ovc; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_ovc_enrolment` (',
  ' uuid CHAR(38),',
  ' encounter_id INT(11) NOT NULL PRIMARY KEY,',
  ' patient_id INT(11) NOT NULL,',
  ' location_id INT(11) DEFAULT NULL,',
  ' visit_id INT(11),',
  ' visit_date DATE,',
  ' encounter_provider INT(11),',
  ' date_created DATETIME NOT NULL,',
  ' date_last_modified DATETIME,',
  ' caregiver_enrolled_here VARCHAR(11) DEFAULT NULL,',
  ' caregiver_name VARCHAR(255) DEFAULT NULL,',
  ' caregiver_gender VARCHAR(255) DEFAULT NULL,',
  ' relationship_to_client VARCHAR(255) DEFAULT NULL,',
  ' caregiver_phone_number VARCHAR(255) DEFAULT NULL,',
  ' client_enrolled_cpims VARCHAR(11) DEFAULT NULL,',
  ' partner_offering_ovc VARCHAR(255) DEFAULT NULL,',
  ' ovc_comprehensive_program VARCHAR(255) DEFAULT NULL,',
  ' dreams_program VARCHAR(255) DEFAULT NULL,',
  ' ovc_preventive_program VARCHAR(255) DEFAULT NULL,',
  ' voided INT(11) DEFAULT 0,',
  ' CONSTRAINT `fk_ovc_patient` FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id) ON DELETE RESTRICT ON UPDATE CASCADE,',
  ' CONSTRAINT `unique_uuid` UNIQUE (uuid),',
  ' INDEX (visit_date),',
  ' INDEX (encounter_id),',
  ' INDEX (patient_id),',
  ' INDEX (patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_ovc_enrolment table';


-- --------------------- creating Cervical cancer screening table -------------------------------
-- sql
SET @drop_etl_cervical = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_cervical_cancer_screening`;');
PREPARE stmt FROM @drop_etl_cervical; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_cervical_cancer_screening` (',
  ' uuid CHAR(38),',
  ' encounter_id INT(11) NOT NULL PRIMARY KEY,',
  ' encounter_provider INT(11),',
  ' patient_id INT(11) NOT NULL,',
  ' visit_id INT(11) DEFAULT NULL,',
  ' visit_date DATE,',
  ' location_id INT(11) DEFAULT NULL,',
  ' date_created DATETIME NOT NULL,',
  ' date_last_modified DATETIME,',
  ' visit_type VARCHAR(255) DEFAULT NULL,',
  ' screening_type VARCHAR(255) DEFAULT NULL,',
  ' post_treatment_complication_cause VARCHAR(255) DEFAULT NULL,',
  ' post_treatment_complication_other VARCHAR(255) DEFAULT NULL,',
  ' cervical_cancer VARCHAR(255) DEFAULT NULL,',
  ' colposcopy_screening_method VARCHAR(255) DEFAULT NULL,',
  ' hpv_screening_method VARCHAR(255) DEFAULT NULL,',
  ' pap_smear_screening_method VARCHAR(255) DEFAULT NULL,',
  ' via_vili_screening_method VARCHAR(255) DEFAULT NULL,',
  ' colposcopy_screening_result VARCHAR(255) DEFAULT NULL,',
  ' hpv_screening_result VARCHAR(255) DEFAULT NULL,',
  ' pap_smear_screening_result VARCHAR(255) DEFAULT NULL,',
  ' via_vili_screening_result VARCHAR(255) DEFAULT NULL,',
  ' colposcopy_treatment_method VARCHAR(255) DEFAULT NULL,',
  ' hpv_treatment_method VARCHAR(255) DEFAULT NULL,',
  ' pap_smear_treatment_method VARCHAR(255) DEFAULT NULL,',
  ' via_vili_treatment_method VARCHAR(255) DEFAULT NULL,',
  ' colorectal_cancer VARCHAR(255) DEFAULT NULL,',
  ' fecal_occult_screening_method VARCHAR(255) DEFAULT NULL,',
  ' colonoscopy_method VARCHAR(255) DEFAULT NULL,',
  ' fecal_occult_screening_results VARCHAR(255) DEFAULT NULL,',
  ' colonoscopy_method_results VARCHAR(255) DEFAULT NULL,',
  ' fecal_occult_screening_treatment VARCHAR(255) DEFAULT NULL,',
  ' colonoscopy_method_treatment VARCHAR(255) DEFAULT NULL,',
  ' retinoblastoma_cancer VARCHAR(255) DEFAULT NULL,',
  ' retinoblastoma_eua_screening_method VARCHAR(255) DEFAULT NULL,',
  ' retinoblastoma_gene_method VARCHAR(255) DEFAULT NULL,',
  ' retinoblastoma_eua_screening_results VARCHAR(255) DEFAULT NULL,',
  ' retinoblastoma_gene_method_results VARCHAR(255) DEFAULT NULL,',
  ' retinoblastoma_eua_treatment VARCHAR(255) DEFAULT NULL,',
  ' retinoblastoma_gene_treatment VARCHAR(255) DEFAULT NULL,',
  ' oral_cancer VARCHAR(255) DEFAULT NULL,',
  ' oral_cancer_visual_exam_method VARCHAR(255) DEFAULT NULL,',
  ' oral_cancer_cytology_method VARCHAR(255) DEFAULT NULL,',
  ' oral_cancer_imaging_method VARCHAR(255) DEFAULT NULL,',
  ' oral_cancer_biopsy_method VARCHAR(255) DEFAULT NULL,',
  ' oral_cancer_visual_exam_results VARCHAR(255) DEFAULT NULL,',
  ' oral_cancer_cytology_results VARCHAR(255) DEFAULT NULL,',
  ' oral_cancer_imaging_results VARCHAR(255) DEFAULT NULL,',
  ' oral_cancer_biopsy_results VARCHAR(255) DEFAULT NULL,',
  ' oral_cancer_visual_exam_treatment VARCHAR(255) DEFAULT NULL,',
  ' oral_cancer_cytology_treatment VARCHAR(255) DEFAULT NULL,',
  ' oral_cancer_imaging_treatment VARCHAR(255) DEFAULT NULL,',
  ' oral_cancer_biopsy_treatment VARCHAR(255) DEFAULT NULL,',
  ' prostate_cancer VARCHAR(255) DEFAULT NULL,',
  ' digital_rectal_prostate_examination VARCHAR(255) DEFAULT NULL,',
  ' digital_rectal_prostate_results VARCHAR(255) DEFAULT NULL,',
  ' digital_rectal_prostate_treatment VARCHAR(255) DEFAULT NULL,',
  ' prostatic_specific_antigen_test VARCHAR(255) DEFAULT NULL,',
  ' prostatic_specific_antigen_results VARCHAR(255) DEFAULT NULL,',
  ' prostatic_specific_antigen_treatment VARCHAR(255) DEFAULT NULL,',
  ' breast_cancer VARCHAR(50) DEFAULT NULL,',
  ' clinical_breast_examination_screening_method VARCHAR(255) DEFAULT NULL,',
  ' ultrasound_screening_method VARCHAR(255) DEFAULT NULL,',
  ' mammography_smear_screening_method VARCHAR(255) DEFAULT NULL,',
  ' clinical_breast_examination_screening_result VARCHAR(255) DEFAULT NULL,',
  ' ultrasound_screening_result VARCHAR(255) DEFAULT NULL,',
  ' mammography_screening_result VARCHAR(255) DEFAULT NULL,',
  ' clinical_breast_examination_treatment_method VARCHAR(255) DEFAULT NULL,',
  ' ultrasound_treatment_method VARCHAR(255) DEFAULT NULL,',
  ' breast_tissue_diagnosis VARCHAR(255) DEFAULT NULL,',
  ' breast_tissue_diagnosis_date DATE,',
  ' reason_tissue_diagnosis_not_done VARCHAR(255) DEFAULT NULL,',
  ' mammography_treatment_method VARCHAR(255) DEFAULT NULL,',
  ' referred_out VARCHAR(100) DEFAULT NULL,',
  ' referral_facility VARCHAR(100) DEFAULT NULL,',
  ' referral_reason VARCHAR(255) DEFAULT NULL,',
  ' followup_date DATETIME,',
  ' hiv_status VARCHAR(100) DEFAULT NULL,',
  ' smoke_cigarattes VARCHAR(255) DEFAULT NULL,',
  ' other_forms_tobacco VARCHAR(255) DEFAULT NULL,',
  ' take_alcohol VARCHAR(255) DEFAULT NULL,',
  ' previous_treatment VARCHAR(255) DEFAULT NULL,',
  ' previous_treatment_specify VARCHAR(255) DEFAULT NULL,',
  ' signs_symptoms VARCHAR(500) DEFAULT NULL,',
  ' signs_symptoms_specify VARCHAR(500) DEFAULT NULL,',
  ' family_history VARCHAR(100) DEFAULT NULL,',
  ' number_of_years_smoked VARCHAR(100) DEFAULT NULL,',
  ' number_of_cigarette_per_day VARCHAR(100) DEFAULT NULL,',
  ' clinical_notes VARCHAR(500) DEFAULT NULL,',
  ' voided INT(11) DEFAULT 0,',
  ' CONSTRAINT `fk_cervical_patient` FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (patient_id) ON DELETE RESTRICT ON UPDATE CASCADE,',
  ' CONSTRAINT `unique_uuid` UNIQUE (uuid),',
  ' INDEX (visit_date),',
  ' INDEX (encounter_id),',
  ' INDEX (patient_id),',
  ' INDEX (patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_cervical_cancer_screening table';

-- --------------------- creating patient contact  table -------------------------------

SET @drop_etl_contact = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_patient_contact`;');
PREPARE stmt FROM @drop_etl_contact; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_patient_contact` (',
  '  `encounter_id` INT(11) NOT NULL PRIMARY KEY,',
  '  `patient_id` INT(11) NOT NULL,',
  '  `patient_related_to` INT(11) DEFAULT NULL,',
  '  `relationship_type` INT(11) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `start_date` DATE DEFAULT NULL,',
  '  `end_date` DATE DEFAULT NULL,',
  '  `physical_address` VARCHAR(255) DEFAULT NULL,',
  '  `baseline_hiv_status` VARCHAR(255) DEFAULT NULL,',
  '  `reported_test_date` DATETIME DEFAULT NULL,',
  '  `living_with_patient` VARCHAR(100) DEFAULT NULL,',
  '  `pns_approach` VARCHAR(100) DEFAULT NULL,',
  '  `appointment_date` DATETIME DEFAULT NULL,',
  '  `ipv_outcome` VARCHAR(255) DEFAULT NULL,',
  '  `contact_listing_decline_reason` VARCHAR(255) DEFAULT NULL,',
  '  `consented_contact_listing` VARCHAR(100) DEFAULT NULL,',
  '  `encounter_provider` INT(11) DEFAULT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  `uuid` CHAR(38) DEFAULT NULL,',
  '  CONSTRAINT `fk_patient_contact_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `fk_patient_contact_related` FOREIGN KEY (`patient_related_to`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid` UNIQUE (`uuid`),',
  '  INDEX (`date_created`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`patient_related_to`),',
  '  INDEX (`patient_id`, `date_created`),',
  '  INDEX (`location_id`),',
  '  INDEX (`appointment_date`),',
  '  INDEX (`reported_test_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_patient_contact table';

-- --------------------- creating client trace  table -------------------------------
-- sql
SET @drop_etl_client_trace = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_client_trace`;');
PREPARE stmt FROM @drop_etl_client_trace; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_client_trace` (',
  '  `id` INT(11) NOT NULL AUTO_INCREMENT,',
  '  `uuid` CHAR(38) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `encounter_date` DATETIME DEFAULT NULL,',
  '  `client_id` INT(11) DEFAULT NULL,',
  '  `contact_type` VARCHAR(255) DEFAULT NULL,',
  '  `status` VARCHAR(255) DEFAULT NULL,',
  '  `unique_patient_no` VARCHAR(255) DEFAULT NULL,',
  '  `facility_linked_to` VARCHAR(255) DEFAULT NULL,',
  '  `health_worker_handed_to` VARCHAR(255) DEFAULT NULL,',
  '  `remarks` VARCHAR(255) DEFAULT NULL,',
  '  `appointment_date` DATETIME DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`id`),',
  '  CONSTRAINT `fk_client_trace_patient` FOREIGN KEY (`client_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid` UNIQUE (`uuid`),',
  '  INDEX (`date_created`),',
  '  INDEX (`client_id`),',
  '  INDEX (`id`, `date_created`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_client_trace table';

-- --------------------- creating Viral Load table -------------------------------

SET @drop_etl_viral_load = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_viral_load`;');
PREPARE stmt FROM @drop_etl_viral_load; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_viral_load` (',
  '  `uuid` CHAR(38),',
  '  `encounter_id` INT(11) NOT NULL PRIMARY KEY,',
  '  `patient_id` INT(11) NOT NULL,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `order_date` DATE,',
  '  `date_of_result` DATE,',
  '  `order_reason` VARCHAR(255) DEFAULT NULL,',
  '  `previous_vl_result` VARCHAR(50) DEFAULT NULL,',
  '  `current_vl_result` VARCHAR(50) DEFAULT NULL,',
  '  `previous_vl_date` DATE,',
  '  `previous_vl_reason` VARCHAR(255) DEFAULT NULL,',
  '  `vl_months_since_hiv_enrollment` INT(11) DEFAULT NULL,',
  '  `vl_months_since_otz_enrollment` INT(11) DEFAULT NULL,',
  '  `eligibility` VARCHAR(50) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `voided` INT(11),',
  '  CONSTRAINT `fk_etl_viral_load_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`patient_id`, `visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_viral_load table';


-- create table etl_contact

SET @drop_etl_contact = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_contact`;');
PREPARE stmt FROM @drop_etl_contact; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_contact` (',
  '  `uuid` CHAR(38),',
  '  `unique_identifier` VARCHAR(50),',
  '  `client_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL PRIMARY KEY,',
  '  `encounter_provider` INT(11),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `patient_type` VARCHAR(50),',
  '  `transfer_in_date` DATE,',
  '  `date_first_enrolled_in_kp` DATE,',
  '  `facility_transferred_from` VARCHAR(255),',
  '  `key_population_type` VARCHAR(255),',
  '  `priority_population_type` VARCHAR(255),',
  '  `implementation_county` VARCHAR(200),',
  '  `implementation_subcounty` VARCHAR(200),',
  '  `implementation_ward` VARCHAR(200),',
  '  `contacted_by_peducator` VARCHAR(10),',
  '  `program_name` VARCHAR(255),',
  '  `frequent_hotspot_name` VARCHAR(255),',
  '  `frequent_hotspot_type` VARCHAR(255),',
  '  `year_started_sex_work` VARCHAR(10),',
  '  `year_started_sex_with_men` VARCHAR(10),',
  '  `year_started_drugs` VARCHAR(10),',
  '  `avg_weekly_sex_acts` INT(11),',
  '  `avg_weekly_anal_sex_acts` INT(11),',
  '  `avg_daily_drug_injections` INT(11),',
  '  `contact_person_name` VARCHAR(255),',
  '  `contact_person_alias` VARCHAR(255),',
  '  `contact_person_phone` VARCHAR(255),',
  '  `voided` INT(11) DEFAULT 0,',
  '  CONSTRAINT `fk_etl_contact_patient` FOREIGN KEY (`client_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid` UNIQUE (`uuid`),',
  '  INDEX (`client_id`),',
  '  INDEX (`unique_identifier`),',
  '  INDEX (`key_population_type`),',
  '  INDEX (`priority_population_type`),',
  '  INDEX (`patient_type`),',
  '  INDEX (`transfer_in_date`),',
  '  INDEX (`date_first_enrolled_in_kp`),',
  '  INDEX (`implementation_subcounty`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_contact table';

-- --------- Create table kp_client enrollment

SET @drop_etl_contact = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_contact`;');
PREPARE stmt FROM @drop_etl_contact; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_contact` (',
  '  `uuid` CHAR(38),',
  '  `unique_identifier` VARCHAR(50),',
  '  `client_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL PRIMARY KEY,',
  '  `encounter_provider` INT(11),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `patient_type` VARCHAR(50),',
  '  `transfer_in_date` DATE,',
  '  `date_first_enrolled_in_kp` DATE,',
  '  `facility_transferred_from` VARCHAR(255),',
  '  `key_population_type` VARCHAR(255),',
  '  `priority_population_type` VARCHAR(255),',
  '  `implementation_county` VARCHAR(200),',
  '  `implementation_subcounty` VARCHAR(200),',
  '  `implementation_ward` VARCHAR(200),',
  '  `contacted_by_peducator` VARCHAR(10),',
  '  `program_name` VARCHAR(255),',
  '  `frequent_hotspot_name` VARCHAR(255),',
  '  `frequent_hotspot_type` VARCHAR(255),',
  '  `year_started_sex_work` VARCHAR(10),',
  '  `year_started_sex_with_men` VARCHAR(10),',
  '  `year_started_drugs` VARCHAR(10),',
  '  `avg_weekly_sex_acts` INT(11),',
  '  `avg_weekly_anal_sex_acts` INT(11),',
  '  `avg_daily_drug_injections` INT(11),',
  '  `contact_person_name` VARCHAR(255),',
  '  `contact_person_alias` VARCHAR(255),',
  '  `contact_person_phone` VARCHAR(255),',
  '  `voided` INT(11),',
  '  CONSTRAINT FOREIGN KEY (`client_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics`(`patient_id`),',
  '  CONSTRAINT `unique_uuid` UNIQUE (`uuid`),',
  '  INDEX(`client_id`),',
  '  INDEX(`unique_identifier`),',
  '  INDEX(`key_population_type`),',
  '  INDEX(`priority_population_type`),',
  '  INDEX(`patient_type`),',
  '  INDEX(`transfer_in_date`),',
  '  INDEX(`date_first_enrolled_in_kp`),',
  '  INDEX(`implementation_subcounty`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_contact table';


-- create table etl_kp_clinical_visit

SET @drop_etl_clinical = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_clinical_visit`;');
PREPARE stmt FROM @drop_etl_clinical; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_clinical_visit` (',
  '  `uuid` CHAR(38),',
  '  `client_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `encounter_provider` INT(11),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `implementing_partner` VARCHAR(255),',
  '  `type_of_visit` VARCHAR(255),',
  '  `visit_reason` VARCHAR(255),',
  '  `service_delivery_model` VARCHAR(255),',
  '  `sti_screened` VARCHAR(10),',
  '  `sti_results` VARCHAR(255),',
  '  `sti_treated` VARCHAR(10),',
  '  `sti_referred` VARCHAR(10),',
  '  `sti_referred_text` VARCHAR(255),',
  '  `tb_screened` VARCHAR(10),',
  '  `tb_results` VARCHAR(255),',
  '  `tb_treated` VARCHAR(10),',
  '  `tb_referred` VARCHAR(10),',
  '  `tb_referred_text` VARCHAR(255),',
  '  `hepatitisB_screened` VARCHAR(10),',
  '  `hepatitisB_results` VARCHAR(255),',
  '  `hepatitisB_confirmatory_results` VARCHAR(50),',
  '  `hepatitisB_vaccinated` VARCHAR(50),',
  '  `hepatitisB_treated` VARCHAR(10),',
  '  `hepatitisB_referred` VARCHAR(10),',
  '  `hepatitisB_text` VARCHAR(255),',
  '  `hepatitisC_screened` VARCHAR(10),',
  '  `hepatitisC_results` VARCHAR(255),',
  '  `hepatitisC_confirmatory_results` VARCHAR(255),',
  '  `hepatitisC_treated` VARCHAR(10),',
  '  `hepatitisC_referred` VARCHAR(10),',
  '  `hepatitisC_text` VARCHAR(255),',
  '  `overdose_screened` VARCHAR(10),',
  '  `overdose_results` VARCHAR(255),',
  '  `overdose_treated` VARCHAR(10),',
  '  `received_naloxone` VARCHAR(10),',
  '  `overdose_referred` VARCHAR(10),',
  '  `overdose_text` VARCHAR(255),',
  '  `abscess_screened` VARCHAR(10),',
  '  `abscess_results` VARCHAR(255),',
  '  `abscess_treated` VARCHAR(10),',
  '  `abscess_referred` VARCHAR(10),',
  '  `abscess_text` VARCHAR(255),',
  '  `alcohol_screened` VARCHAR(10),',
  '  `alcohol_results` VARCHAR(255),',
  '  `alcohol_treated` VARCHAR(10),',
  '  `alcohol_referred` VARCHAR(10),',
  '  `alcohol_text` VARCHAR(255),',
  '  `cerv_cancer_screened` VARCHAR(10),',
  '  `cerv_cancer_results` VARCHAR(255),',
  '  `cerv_cancer_treated` VARCHAR(10),',
  '  `cerv_cancer_referred` VARCHAR(10),',
  '  `cerv_cancer_text` VARCHAR(255),',
  '  `anal_cancer_screened` VARCHAR(10),',
  '  `anal_cancer_results` VARCHAR(255),',
  '  `prep_screened` VARCHAR(10),',
  '  `prep_results` VARCHAR(255),',
  '  `prep_treated` VARCHAR(10),',
  '  `prep_referred` VARCHAR(10),',
  '  `prep_text` VARCHAR(255),',
  '  `violence_screened` VARCHAR(10),',
  '  `violence_results` VARCHAR(255),',
  '  `violence_treated` VARCHAR(10),',
  '  `violence_referred` VARCHAR(10),',
  '  `violence_text` VARCHAR(255),',
  '  `risk_red_counselling_screened` VARCHAR(10),',
  '  `risk_red_counselling_eligibility` VARCHAR(255),',
  '  `risk_red_counselling_support` VARCHAR(10),',
  '  `risk_red_counselling_ebi_provided` VARCHAR(10),',
  '  `risk_red_counselling_text` VARCHAR(255),',
  '  `fp_screened` VARCHAR(10),',
  '  `fp_eligibility` VARCHAR(255),',
  '  `fp_treated` VARCHAR(10),',
  '  `fp_referred` VARCHAR(10),',
  '  `fp_text` VARCHAR(255),',
  '  `mental_health_screened` VARCHAR(10),',
  '  `mental_health_results` VARCHAR(255),',
  '  `mental_health_support` VARCHAR(100),',
  '  `mental_health_referred` VARCHAR(10),',
  '  `mental_health_text` VARCHAR(255),',
  '  `mat_screened` VARCHAR(10),',
  '  `mat_results` VARCHAR(255),',
  '  `mat_treated` VARCHAR(100),',
  '  `mat_referred` VARCHAR(10),',
  '  `mat_text` VARCHAR(255),',
  '  `hiv_self_rep_status` VARCHAR(50),',
  '  `last_hiv_test_setting` VARCHAR(100),',
  '  `counselled_for_hiv` VARCHAR(10),',
  '  `hiv_tested` VARCHAR(10),',
  '  `test_frequency` VARCHAR(100),',
  '  `received_results` VARCHAR(10),',
  '  `test_results` VARCHAR(100),',
  '  `linked_to_art` VARCHAR(10),',
  '  `facility_linked_to` VARCHAR(10),',
  '  `self_test_education` VARCHAR(10),',
  '  `self_test_kits_given` VARCHAR(100),',
  '  `self_use_kits` VARCHAR(10),',
  '  `distribution_kits` VARCHAR(10),',
  '  `self_tested` VARCHAR(10),',
  '  `hiv_test_date` DATE,',
  '  `self_test_frequency` VARCHAR(100),',
  '  `self_test_results` VARCHAR(100),',
  '  `test_confirmatory_results` VARCHAR(100),',
  '  `confirmatory_facility` VARCHAR(100),',
  '  `offsite_confirmatory_facility` VARCHAR(100),',
  '  `self_test_linked_art` VARCHAR(10),',
  '  `self_test_link_facility` VARCHAR(255),',
  '  `hiv_care_facility` VARCHAR(255),',
  '  `other_hiv_care_facility` VARCHAR(255),',
  '  `initiated_art_this_month` VARCHAR(10),',
  '  `started_on_art` VARCHAR(10),',
  '  `date_started_art` DATE,',
  '  `active_art` VARCHAR(10),',
  '  `primary_care_facility_name` VARCHAR(250),',
  '  `ccc_number` VARCHAR(50),',
  '  `eligible_vl` VARCHAR(50),',
  '  `vl_test_done` VARCHAR(100),',
  '  `vl_results` VARCHAR(100),',
  '  `vl_results_date` DATE,',
  '  `received_vl_results` VARCHAR(100),',
  '  `condom_use_education` VARCHAR(10),',
  '  `post_abortal_care` VARCHAR(10),',
  '  `referral` VARCHAR(10),',
  '  `linked_to_psychosocial` VARCHAR(10),',
  '  `male_condoms_no` VARCHAR(10),',
  '  `female_condoms_no` VARCHAR(10),',
  '  `lubes_no` VARCHAR(10),',
  '  `syringes_needles_no` VARCHAR(10),',
  '  `pep_eligible` VARCHAR(10),',
  '  `pep_status` VARCHAR(10),',
  '  `exposure_type` VARCHAR(100),',
  '  `other_exposure_type` VARCHAR(100),',
  '  `initiated_pep_within_72hrs` VARCHAR(10),',
  '  `clinical_notes` VARCHAR(255),',
  '  `appointment_date` DATE,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_clinical_patient` FOREIGN KEY (`client_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_clinical_visit` UNIQUE (`uuid`),',
  '  INDEX `idx_client_id` (`client_id`),',
  '  INDEX `idx_client_visit_date` (`client_id`, `visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_clinical_visit table';

-- ------------ create table etl_kp_peer_calendar-----------------------

SET @drop_etl_peer_calendar = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_peer_calendar`;');
PREPARE stmt FROM @drop_etl_peer_calendar; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_peer_calendar` (',
    'uuid CHAR(38),',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'client_id INT(11) NOT NULL,',
    'location_id INT(11) DEFAULT NULL,',
    'visit_date DATE,',
    'visit_id INT(11),',
    'encounter_provider INT(11),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'hotspot_name VARCHAR(255),',
    'typology VARCHAR(255),',
    'other_hotspots VARCHAR(255),',
    'weekly_sex_acts INT(10),',
    'monthly_condoms_required INT(10),',
    'weekly_anal_sex_acts INT(10),',
    'monthly_lubes_required INT(10),',
    'daily_injections INT(10),',
    'monthly_syringes_required INT(10),',
    'years_in_sexwork_drugs INT(10),',
    'experienced_violence VARCHAR(10),',
    'service_provided_within_last_month VARCHAR(255),',
    'monthly_n_and_s_distributed INT(10),',
    'monthly_male_condoms_distributed INT(10),',
    'monthly_lubes_distributed INT(10),',
    'monthly_female_condoms_distributed INT(10),',
    'monthly_self_test_kits_distributed INT(10),',
    'received_clinical_service VARCHAR(10),',
    'violence_reported VARCHAR(10),',
    'referred VARCHAR(10),',
    'health_edu VARCHAR(10),',
    'remarks VARCHAR(255),',
    'voided INT(11) DEFAULT 0,',
    'CONSTRAINT FOREIGN KEY (client_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
    'CONSTRAINT unique_uuid UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(client_id, visit_date),',
    'INDEX(location_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_peer_calendar table';


-- ------------ create table etl_kp_sti_treatment-----------------------
-- sql
SET @drop_etl_sti = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_sti_treatment`;');
PREPARE stmt FROM @drop_etl_sti; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_sti_treatment` (',
  '  `uuid` CHAR(38),',
  '  `encounter_id` INT(11) NOT NULL PRIMARY KEY,',
  '  `client_id` INT(11) NOT NULL,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `visit_id` INT(11),',
  '  `encounter_provider` INT(11),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `visit_reason` VARCHAR(255),',
  '  `syndrome` VARCHAR(10),',
  '  `other_syndrome` VARCHAR(255),',
  '  `drug_prescription` VARCHAR(10),',
  '  `other_drug_prescription` VARCHAR(255),',
  '  `genital_exam_done` VARCHAR(10),',
  '  `lab_referral` VARCHAR(10),',
  '  `lab_form_number` VARCHAR(100),',
  '  `referred_to_facility` VARCHAR(10),',
  '  `facility_name` VARCHAR(255),',
  '  `partner_referral_done` VARCHAR(10),',
  '  `given_lubes` VARCHAR(10),',
  '  `no_of_lubes` INT(10),',
  '  `given_condoms` VARCHAR(10),',
  '  `no_of_condoms` INT(10),',
  '  `provider_comments` VARCHAR(255),',
  '  `provider_name` VARCHAR(255),',
  '  `appointment_date` DATE,',
  '  `voided` INT(11) DEFAULT 0,',
  '  CONSTRAINT `fk_etl_sti_patient` FOREIGN KEY (`client_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_sti` UNIQUE (`uuid`),',
  '  INDEX(`visit_date`),',
  '  INDEX(`client_id`),',
  '  INDEX(`visit_reason`),',
  '  INDEX(`given_lubes`),',
  '  INDEX(`given_condoms`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_sti_treatment table';


-- sql
SET @drop_etl_peer_tracking = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_peer_tracking`;');
PREPARE stmt FROM @drop_etl_peer_tracking; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_peer_tracking` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `client_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11),',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL PRIMARY KEY,',
  '  `tracing_attempted` VARCHAR(10),',
  '  `tracing_not_attempted_reason` VARCHAR(100),',
  '  `attempt_number` VARCHAR(11),',
  '  `tracing_date` DATE,',
  '  `tracing_type` VARCHAR(100),',
  '  `tracing_outcome` VARCHAR(100),',
  '  `is_final_trace` VARCHAR(10),',
  '  `tracing_outcome_status` VARCHAR(100),',
  '  `voluntary_exit_comment` VARCHAR(255),',
  '  `status_in_program` VARCHAR(100),',
  '  `source_of_information` VARCHAR(100),',
  '  `other_informant` VARCHAR(100),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `voided` INT(11) DEFAULT 0,',
  '  CONSTRAINT `fk_etl_peer_tracking_patient` FOREIGN KEY (`client_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid` UNIQUE (`uuid`),',
  '  INDEX(`visit_date`),',
  '  INDEX(`encounter_id`),',
  '  INDEX(`client_id`),',
  '  INDEX(`status_in_program`),',
  '  INDEX(`tracing_type`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_peer_tracking table';

-- sql
SET @drop_etl_treatment_verification = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_treatment_verification`;');
PREPARE stmt FROM @drop_etl_treatment_verification; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_treatment_verification` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `client_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11),',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `date_diagnosed_with_hiv` DATE,',
  '  `art_health_facility` VARCHAR(100) DEFAULT NULL,',
  '  `ccc_number` VARCHAR(100) DEFAULT NULL,',
  '  `is_pepfar_site` VARCHAR(11) DEFAULT NULL,',
  '  `date_initiated_art` DATE,',
  '  `current_regimen` VARCHAR(100) DEFAULT NULL,',
  '  `information_source` VARCHAR(100) DEFAULT NULL,',
  '  `cd4_test_date` DATE,',
  '  `cd4` VARCHAR(100) DEFAULT NULL,',
  '  `vl_test_date` DATE,',
  '  `viral_load` VARCHAR(100) DEFAULT NULL,',
  '  `disclosed_status` VARCHAR(11) DEFAULT NULL,',
  '  `person_disclosed_to` VARCHAR(100) DEFAULT NULL,',
  '  `other_person_disclosed_to` VARCHAR(100) DEFAULT NULL,',
  '  `IPT_start_date` DATE,',
  '  `IPT_completion_date` DATE,',
  '  `on_diff_care` VARCHAR(11) DEFAULT NULL,',
  '  `in_support_group` VARCHAR(11) DEFAULT NULL,',
  '  `support_group_name` VARCHAR(100) DEFAULT NULL,',
  '  `opportunistic_infection` VARCHAR(100) DEFAULT NULL,',
  '  `oi_diagnosis_date` DATE,',
  '  `oi_treatment_start_date` DATE,',
  '  `oi_treatment_end_date` DATE,',
  '  `comment` VARCHAR(100) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_treatment_verification_patient` FOREIGN KEY (`client_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`client_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_treatment_verification table';

-- sql
SET @drop_etl_prep_verification = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_PrEP_verification`;');
PREPARE stmt FROM @drop_etl_prep_verification; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_PrEP_verification` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `client_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11),',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `date_enrolled` DATE,',
  '  `health_facility_accessing_PrEP` VARCHAR(100) DEFAULT NULL,',
  '  `is_pepfar_site` VARCHAR(11) DEFAULT NULL,',
  '  `date_initiated_PrEP` DATE,',
  '  `PrEP_regimen` VARCHAR(100) DEFAULT NULL,',
  '  `information_source` VARCHAR(100) DEFAULT NULL,',
  '  `PrEP_status` VARCHAR(100) DEFAULT NULL,',
  '  `verification_date` DATE,',
  '  `discontinuation_reason` VARCHAR(100) DEFAULT NULL,',
  '  `other_discontinuation_reason` VARCHAR(100) DEFAULT NULL,',
  '  `appointment_date` DATE,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_prep_verification_patient` FOREIGN KEY (`client_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_prep_verification` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`client_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_PrEP_verification table';


-- ------------ create table etl_alcohol_drug_abuse_screening-----------------------

-- sql
SET @drop_etl_alcohol = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_alcohol_drug_abuse_screening`;');
PREPARE stmt FROM @drop_etl_alcohol; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_alcohol_drug_abuse_screening` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11),',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `alcohol_drinking_frequency` VARCHAR(50) DEFAULT NULL,',
  '  `smoking_frequency` VARCHAR(50) DEFAULT NULL,',
  '  `drugs_use_frequency` VARCHAR(50) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_alcohol_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_alcohol_drug_abuse` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`patient_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_alcohol_drug_abuse_screening table';

-- ------------ create table etl_gbv_screening-----------------------
-- sql
SET @drop_etl_gbv = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_gbv_screening`;');
PREPARE stmt FROM @drop_etl_gbv; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_gbv_screening` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11),',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `ipv` VARCHAR(50) DEFAULT NULL,',
  '  `physical_ipv` VARCHAR(50) DEFAULT NULL,',
  '  `emotional_ipv` VARCHAR(50) DEFAULT NULL,',
  '  `sexual_ipv` VARCHAR(50) DEFAULT NULL,',
  '  `ipv_relationship` VARCHAR(50) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_gbv_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_gbv` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`patient_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_gbv_screening table';


-- ------------ create table etl_gbv_screening_action-----------------------

SET @drop_etl_gbv_action = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_gbv_screening_action`;');
PREPARE stmt FROM @drop_etl_gbv_action; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_gbv_screening_action` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `patient_id` INT(11) NOT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11),',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `obs_id` INT(11) NOT NULL PRIMARY KEY,',
  '  `help_provider` VARCHAR(100) DEFAULT NULL,',
  '  `action_taken` VARCHAR(100) DEFAULT NULL,',
  '  `action_date` DATE DEFAULT NULL,',
  '  `reason_for_not_reporting` VARCHAR(100) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  CONSTRAINT `fk_etl_gbv_action_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_gbv_action` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`obs_id`),',
  '  INDEX (`patient_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_gbv_screening_action table';


-- create table etl_violence_reporting
SET @drop_etl_violence_reporting = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_violence_reporting`;');
PREPARE stmt FROM @drop_etl_violence_reporting; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_violence_reporting` (',
    'uuid CHAR(38),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL PRIMARY KEY,',
    'place_of_incident VARCHAR(100),',
    'date_of_incident DATE,',
    'time_of_incident INT(11),',
    'abuse_against INT(11),',
    'form_of_incident VARCHAR(500),',
    'perpetrator VARCHAR(500),',
    'date_of_crisis_response DATE,',
    'support_service VARCHAR(100),',
    'hiv_testing_duration INT(11),',
    'hiv_testing_provided_within_5_days INT(11),',
    'duration_on_emergency_contraception INT(11),',
    'emergency_contraception_provided_within_5_days INT(11),',
    'psychosocial_trauma_counselling_duration VARCHAR(50),',
    'psychosocial_trauma_counselling_provided_within_5_days INT(11),',
    'pep_provided_duration VARCHAR(50),',
    'pep_provided_within_5_days INT(11),',
    'sti_screening_and_treatment_duration VARCHAR(50),',
    'sti_screening_and_treatment_provided_within_5_days INT(11),',
    'legal_support_duration VARCHAR(50),',
    'legal_support_provided_within_5_days INT(11),',
    'medical_examination_duration VARCHAR(50),',
    'medical_examination_provided_within_5_days INT(11),',
    'prc_form_file_duration VARCHAR(50),',
    'prc_form_file_provided_within_5_days INT(11),',
    'other_services_provided VARCHAR(100),',
    'medical_services_and_care_duration VARCHAR(50),',
    'medical_services_and_care_provided_within_5_days INT(11),',
    'duration_of_non_sexual_legal_support VARCHAR(50),',
    'duration_of_non_sexual_legal_support_within_5_days INT(11),',
    'current_location_of_person INT(11),',
    'follow_up_plan VARCHAR(100),',
    'resolution_date DATE,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'voided INT(11) DEFAULT 0,',
    'CONSTRAINT `fk_violence_patient` FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
    'CONSTRAINT `unique_uuid` UNIQUE (uuid),',
    'INDEX (visit_date),',
    'INDEX (encounter_id),',
    'INDEX (patient_id),',
    'INDEX (patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_violence_reporting table';


-- --- ----------Create table etl_link_facility_tracking-----------------------

-- sql
SET @drop_etl_link = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_link_facility_tracking`;');
PREPARE stmt FROM @drop_etl_link; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_link_facility_tracking` (',
    'uuid CHAR(38),',
    'provider INT(11),',
    'patient_id INT(11) NOT NULL,',
    'visit_id INT(11),',
    'visit_date DATE,',
    'location_id INT(11) DEFAULT NULL,',
    'encounter_id INT(11) NOT NULL,',
    'county VARCHAR(100),',
    'sub_county VARCHAR(100),',
    'ward VARCHAR(100),',
    'facility_name VARCHAR(100),',
    'ccc_number VARCHAR(100),',
    'date_diagnosed DATE,',
    'date_initiated_art DATE,',
    'original_regimen VARCHAR(255),',
    'current_regimen VARCHAR(255),',
    'date_switched DATE,',
    'reason_for_switch VARCHAR(500),',
    'date_of_last_visit DATE,',
    'date_viral_load_sample_collected DATE,',
    'date_viral_load_results_received DATE,',
    'viral_load_results VARCHAR(100),',
    'viral_load_results_copies INT(11),',
    'date_of_next_visit DATE,',
    'enrolled_in_pssg VARCHAR(100),',
    'attended_pssg VARCHAR(100),',
    'on_pmtct VARCHAR(100),',
    'date_of_delivery DATE,',
    'tb_screening VARCHAR(100),',
    'sti_treatment VARCHAR(100),',
    'trauma_counselling VARCHAR(100),',
    'cervical_cancer_screening VARCHAR(100),',
    'family_planning VARCHAR(100),',
    'currently_on_tb_treatment VARCHAR(100),',
    'date_initiated_tb_treatment DATE,',
    'tpt_status VARCHAR(100),',
    'date_initiated_tpt DATE,',
    'data_collected_through VARCHAR(100),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'voided INT(11) DEFAULT 0,',
    'PRIMARY KEY (encounter_id),',
    'CONSTRAINT `fk_link_facility_patient` FOREIGN KEY (patient_id) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
    'CONSTRAINT `unique_uuid_link_facility` UNIQUE (uuid),',
    'INDEX (visit_date),',
    'INDEX (patient_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_link_facility_tracking`') AS message;

-- ------------ create table etl_depression_screening-----------------------
-- sql
SET @drop_etl_depression = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_depression_screening`;');
PREPARE stmt FROM @drop_etl_depression; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_depression_screening` (',
    '`uuid` CHAR(38),',
    '`provider` INT(11),',
    '`patient_id` INT(11) NOT NULL,',
    '`visit_id` INT(11),',
    '`visit_date` DATE,',
    '`location_id` INT(11) DEFAULT NULL,',
    '`encounter_id` INT(11) NOT NULL,',
    '`little_interest` INT(11),',
    '`feeling_down` INT(11),',
    '`trouble_sleeping` INT(11),',
    '`feeling_tired` INT(11),',
    '`poor_appetite` INT(11),',
    '`feeling_bad` INT(11),',
    '`trouble_concentrating` INT(11),',
    '`moving_or_speaking_slowly` INT(11),',
    '`self_hurtful_thoughts` INT(11),',
    '`phq_9_rating` VARCHAR(255),',
    '`pfa_offered` INT(11),',
    '`client_referred` INT(11),',
    '`facility_referred` INT(11),',
    '`facility_name` VARCHAR(255),',
    '`services_referred_for` VARCHAR(255),',
    '`date_created` DATETIME NOT NULL,',
    '`date_last_modified` DATETIME DEFAULT NULL,',
    '`voided` INT(11) DEFAULT 0,',
    'PRIMARY KEY (`encounter_id`),',
    'CONSTRAINT `fk_depression_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
    'CONSTRAINT `unique_uuid_depression` UNIQUE (`uuid`),',
    'INDEX (`visit_date`),',
    'INDEX (`patient_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_depression_screening`') AS message;


-- ------------ create table etl_adverse_events-----------------------
-- sql
-- Use @etl_schema if running manually, or ensure etl_schema is declared locally
SET @drop_etl_adverse = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_adverse_events`;');
PREPARE stmt FROM @drop_etl_adverse; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_adverse_events` (',
  '  `uuid` CHAR(38),',
  '  `form` VARCHAR(50),',
  '  `provider` INT,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT,',
  '  `obs_id` INT NOT NULL,',
  '  `cause` INT,',
  '  `adverse_event` INT,',
  '  `severity` INT,',
  '  `start_date` DATE,',
  '  `action_taken` INT,',
  '  `voided` INT DEFAULT 0,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  PRIMARY KEY (`obs_id`),',
  '  CONSTRAINT `fk_etl_adverse_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_adverse` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`form`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- --------------------------------------
-- CREATE TABLE: etl_pre_hiv_enrollment_art
-- Tenant-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_pre_hiv_art = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_pre_hiv_enrollment_art`;');
PREPARE stmt FROM @drop_etl_pre_hiv_art; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_pre_hiv_enrollment_art` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11),',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `obs_id` INT(11) NOT NULL,',
  '  `PMTCT` INT(11),',
  '  `PMTCT_regimen` INT(11),',
  '  `PEP` INT(11),',
  '  `PEP_regimen` INT(11),',
  '  `PrEP` INT(11),',
  '  `PrEP_regimen` INT(11),',
  '  `HAART` INT(11),',
  '  `HAART_regimen` INT(11),',
  '  `voided` INT(11) DEFAULT 0,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  PRIMARY KEY (`obs_id`),',
  '  CONSTRAINT `fk_etl_pre_hiv_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`obs_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_pre_hiv_enrollment_art`') AS message;

-- --------------------------------------
-- TABLE: etl_covid19_assessment
-- Purpose: tenant-aware creation of COVID-19 assessment ETL table
-- Tenant-aware: uses `etl_schema`
-- --------------------------------------
SET @drop_etl_covid19_assessment = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_covid19_assessment`;');
PREPARE stmt FROM @drop_etl_covid19_assessment; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_covid19_assessment` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11),',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `obs_id` INT(11) NOT NULL,',
  '  `ever_vaccinated` VARCHAR(10),',
  '  `first_vaccine_type` VARCHAR(50),',
  '  `second_vaccine_type` VARCHAR(50),',
  '  `first_dose` VARCHAR(10),',
  '  `second_dose` VARCHAR(10),',
  '  `first_dose_date` DATE,',
  '  `second_dose_date` DATE,',
  '  `first_vaccination_verified` VARCHAR(10),',
  '  `second_vaccination_verified` VARCHAR(10),',
  '  `final_vaccination_status` VARCHAR(20),',
  '  `ever_received_booster` VARCHAR(10),',
  '  `booster_vaccine_taken` VARCHAR(50),',
  '  `date_taken_booster_vaccine` DATE,',
  '  `booster_sequence` VARCHAR(20),',
  '  `booster_dose_verified` VARCHAR(10),',
  '  `ever_tested_covid_19_positive` VARCHAR(10),',
  '  `symptomatic` VARCHAR(10),',
  '  `date_tested_positive` DATE,',
  '  `hospital_admission` VARCHAR(10),',
  '  `admission_unit` VARCHAR(50),',
  '  `on_ventillator` VARCHAR(10),',
  '  `on_oxygen_supplement` VARCHAR(10),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`obs_id`),',
  '  CONSTRAINT `fk_etl_covid19_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_covid19_assessment` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_covid19_assessment`') AS message;



-- sql
-- --------------------------------------
-- CREATE TABLE: etl_vmmc_enrolment
-- Purpose: tenant-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_vmmc = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_vmmc_enrolment`;');
PREPARE stmt FROM @drop_etl_vmmc; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_vmmc_enrolment` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11) DEFAULT NULL,',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `referee` INT(11) DEFAULT NULL,',
  '  `other_referee` VARCHAR(100) DEFAULT NULL,',
  '  `source_of_vmmc_info` INT(11) DEFAULT NULL,',
  '  `other_source_of_vmmc_info` VARCHAR(100) DEFAULT NULL,',
  '  `county_of_origin` VARCHAR(100) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_vmmc_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_vmmc_enrolment` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`source_of_vmmc_info`),',
  '  INDEX (`county_of_origin`),',
  '  INDEX (`patient_id`, `visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_vmmc_enrolment`') AS message;


-- sql
-- --------------------------------------
-- TABLE: etl_vmmc_circumcision_procedure
-- Purpose: tenant-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_vmmc_circ = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_vmmc_circumcision_procedure`;');
PREPARE stmt FROM @drop_etl_vmmc_circ; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_vmmc_circumcision_procedure` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11) DEFAULT NULL,',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `circumcision_method` INT(11) DEFAULT NULL,',
  '  `surgical_circumcision_method` INT(11) DEFAULT NULL,',
  '  `reason_circumcision_ineligible` VARCHAR(100) DEFAULT NULL,',
  '  `circumcision_device` INT(11) DEFAULT NULL,',
  '  `specific_other_device` VARCHAR(100) DEFAULT NULL,',
  '  `device_size` VARCHAR(100) DEFAULT NULL,',
  '  `lot_number` VARCHAR(100) DEFAULT NULL,',
  '  `anaesthesia_type` INT(11) DEFAULT NULL,',
  '  `anaesthesia_used` INT(11) DEFAULT NULL,',
  '  `anaesthesia_concentration` VARCHAR(100) DEFAULT NULL,',
  '  `anaesthesia_volume` INT(11) DEFAULT NULL,',
  '  `time_of_first_placement_cut` DATETIME DEFAULT NULL,',
  '  `time_of_last_device_closure` DATETIME DEFAULT NULL,',
  '  `has_adverse_event` INT(11) DEFAULT NULL,',
  '  `adverse_event` VARCHAR(255) DEFAULT NULL,',
  '  `severity` VARCHAR(100) DEFAULT NULL,',
  '  `adverse_event_management` VARCHAR(255) DEFAULT NULL,',
  '  `clinician_name` VARCHAR(100) DEFAULT NULL,',
  '  `clinician_cadre` INT(11) DEFAULT NULL,',
  '  `assist_clinician_name` VARCHAR(100) DEFAULT NULL,',
  '  `assist_clinician_cadre` INT(11) DEFAULT NULL,',
  '  `theatre_number` VARCHAR(100) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_vmmc_circumcision_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_vmmc_circumcision_procedure` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`circumcision_method`),',
  '  INDEX (`has_adverse_event`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_vmmc_circumcision_procedure`') AS message;


-- sql
-- --------------------------------------
-- TABLE: etl_vmmc_medical_history
-- Purpose: tenant aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_vmmc_medical_history = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_vmmc_medical_history`;');
PREPARE stmt FROM @drop_etl_vmmc_medical_history; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_vmmc_medical_history` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `assent_given` INT(11) DEFAULT NULL,',
  '  `consent_given` INT(11) DEFAULT NULL,',
  '  `hiv_status` INT(11) DEFAULT NULL,',
  '  `hiv_unknown_reason` VARCHAR(255) DEFAULT NULL,',
  '  `hiv_test_date` DATE DEFAULT NULL,',
  '  `art_start_date` DATE DEFAULT NULL,',
  '  `current_regimen` VARCHAR(100) DEFAULT NULL,',
  '  `ccc_number` VARCHAR(100) DEFAULT NULL,',
  '  `next_appointment_date` DATE DEFAULT NULL,',
  '  `hiv_care_facility` INT(11) DEFAULT NULL,',
  '  `hiv_care_facility_name` VARCHAR(100) DEFAULT NULL,',
  '  `vl` VARCHAR(50) DEFAULT NULL,',
  '  `cd4_count` VARCHAR(50) DEFAULT NULL,',
  '  `bleeding_disorder` VARCHAR(255) DEFAULT NULL,',
  '  `diabetes` VARCHAR(255) DEFAULT NULL,',
  '  `client_presenting_complaints` VARCHAR(255) DEFAULT NULL,',
  '  `other_complaints` VARCHAR(255) DEFAULT NULL,',
  '  `ongoing_treatment` VARCHAR(255) DEFAULT NULL,',
  '  `other_ongoing_treatment` VARCHAR(255) DEFAULT NULL,',
  '  `hb_level` INT(11) DEFAULT NULL,',
  '  `sugar_level` INT(11) DEFAULT NULL,',
  '  `has_known_allergies` INT(11) DEFAULT NULL,',
  '  `ever_had_surgical_operation` INT(11) DEFAULT NULL,',
  '  `specific_surgical_operation` VARCHAR(255) DEFAULT NULL,',
  '  `proven_tetanus_booster` INT(11) DEFAULT NULL,',
  '  `ever_received_tetanus_booster` INT(11) DEFAULT NULL,',
  '  `date_received_tetanus_booster` DATE DEFAULT NULL,',
  '  `blood_pressure` VARCHAR(50) DEFAULT NULL,',
  '  `pulse_rate` INT(11) DEFAULT NULL,',
  '  `temperature` VARCHAR(50) DEFAULT NULL,',
  '  `in_good_health` INT(11) DEFAULT NULL,',
  '  `counselled` INT(11) DEFAULT NULL,',
  '  `reason_ineligible` VARCHAR(100) DEFAULT NULL,',
  '  `circumcision_method_chosen` VARCHAR(100) DEFAULT NULL,',
  '  `conventional_method_chosen` INT(11) DEFAULT NULL,',
  '  `device_name` INT(11) DEFAULT NULL,',
  '  `device_size` INT(11) DEFAULT NULL,',
  '  `other_conventional_method_device_chosen` VARCHAR(100) DEFAULT NULL,',
  '  `services_referral` VARCHAR(100) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_vmmc_medical_history_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_vmmc_medical_history` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`consent_given`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_vmmc_medical_history`') AS message;


-- sql
-- --------------------------------------
-- TABLE: etl_vmmc_client_followup
-- Purpose: tenant\-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_vmmc_client_followup = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_vmmc_client_followup`;');
PREPARE stmt FROM @drop_etl_vmmc_client_followup; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_vmmc_client_followup` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `visit_type` INT(11) DEFAULT NULL,',
  '  `days_since_circumcision` VARCHAR(50) DEFAULT NULL,',
  '  `has_adverse_event` INT(11) DEFAULT NULL,',
  '  `adverse_event` VARCHAR(255) DEFAULT NULL,',
  '  `severity` VARCHAR(100) DEFAULT NULL,',
  '  `adverse_event_management` VARCHAR(255) DEFAULT NULL,',
  '  `medications_given` VARCHAR(255) DEFAULT NULL,',
  '  `other_medications_given` VARCHAR(255) DEFAULT NULL,',
  '  `clinician_name` VARCHAR(255) DEFAULT NULL,',
  '  `clinician_cadre` INT(11) DEFAULT NULL,',
  '  `clinician_notes` VARCHAR(255) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_vmmc_client_followup_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_vmmc_client_followup` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`visit_type`),',
  '  INDEX (`has_adverse_event`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_vmmc_client_followup`') AS message;



-- sql
-- --------------------------------------
-- TABLE: etl_vmmc_post_operation_assessment
-- Purpose: tenant-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_vmmc_postop = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_vmmc_post_operation_assessment`;');
PREPARE stmt FROM @drop_etl_vmmc_postop; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_vmmc_post_operation_assessment` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `blood_pressure` VARCHAR(100) DEFAULT NULL,',
  '  `pulse_rate` INT(11) DEFAULT NULL,',
  '  `temperature` INT(11) DEFAULT NULL,',
  '  `penis_elevated` INT(11) DEFAULT NULL,',
  '  `given_post_procedure_instruction` INT(11) DEFAULT NULL,',
  '  `post_procedure_instructions` VARCHAR(250) DEFAULT NULL,',
  '  `given_post_operation_medication` INT(11) DEFAULT NULL,',
  '  `medication_given` VARCHAR(250) DEFAULT NULL,',
  '  `other_medication_given` VARCHAR(250) DEFAULT NULL,',
  '  `removal_date` DATETIME DEFAULT NULL,',
  '  `next_appointment_date` DATETIME DEFAULT NULL,',
  '  `discharged_by` VARCHAR(250) DEFAULT NULL,',
  '  `cadre` INT(11) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_vmmc_post_operation_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_vmmc_post_operation_assessment` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_vmmc_post_operation_assessment`') AS message;

-- --------------------------------------
-- TABLE: etl_hts_eligibility_screening
-- Purpose: tenant-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_hts_eligibility = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_hts_eligibility_screening`;');
PREPARE stmt FROM @drop_etl_hts_eligibility; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_hts_eligibility_screening` (',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT(11) NOT NULL,',
  '  `provider` INT(11) NOT NULL,',
  '  `visit_date` DATE,',
  '  `population_type` VARCHAR(100),',
  '  `key_population_type` VARCHAR(100),',
  '  `priority_population_type` VARCHAR(100),',
  '  `patient_disabled` VARCHAR(50),',
  '  `disability_type` VARCHAR(255),',
  '  `recommended_test` VARCHAR(50),',
  '  `department` INT(11),',
  '  `patient_type` INT(11),',
  '  `is_health_worker` INT(11),',
  '  `relationship_with_contact` VARCHAR(100),',
  '  `mother_hiv_status` INT(11),',
  '  `tested_hiv_before` INT(11),',
  '  `who_performed_test` INT(11),',
  '  `test_results` INT(11),',
  '  `date_tested` DATE,',
  '  `started_on_art` INT(11),',
  '  `upn_number` VARCHAR(80),',
  '  `child_defiled` INT(11),',
  '  `ever_had_sex` INT(11),',
  '  `sexually_active` VARCHAR(100),',
  '  `new_partner` VARCHAR(100),',
  '  `partner_hiv_status` VARCHAR(100),',
  '  `couple_discordant` VARCHAR(100),',
  '  `multiple_partners` VARCHAR(100),',
  '  `number_partners` VARCHAR(100),',
  '  `alcohol_sex` VARCHAR(100),',
  '  `test_strategy` VARCHAR(50),',
  '  `hts_entry_point` VARCHAR(50),',
  '  `hts_risk_category` VARCHAR(50),',
  '  `hts_risk_score` DOUBLE,',
  '  `money_sex` VARCHAR(100),',
  '  `condom_burst` VARCHAR(100),',
  '  `unknown_status_partner` VARCHAR(100),',
  '  `known_status_partner` VARCHAR(100),',
  '  `experienced_gbv` VARCHAR(100),',
  '  `type_of_gbv` VARCHAR(100),',
  '  `service_received` VARCHAR(100),',
  '  `currently_on_prep` VARCHAR(100),',
  '  `recently_on_pep` VARCHAR(100),',
  '  `recently_had_sti` VARCHAR(100),',
  '  `tb_screened` VARCHAR(100),',
  '  `cough` INT(11),',
  '  `fever` INT(11),',
  '  `weight_loss` INT(11),',
  '  `night_sweats` INT(11),',
  '  `contact_with_tb_case` INT(11),',
  '  `lethargy` INT(11),',
  '  `tb_status` INT(11),',
  '  `shared_needle` VARCHAR(100),',
  '  `needle_stick_injuries` INT(11),',
  '  `traditional_procedures` INT(11),',
  '  `child_reasons_for_ineligibility` VARCHAR(100),',
  '  `pregnant` VARCHAR(100),',
  '  `breastfeeding_mother` VARCHAR(100),',
  '  `eligible_for_test` INT(11),',
  '  `referred_for_testing` INT(11),',
  '  `reason_to_test` VARCHAR(100),',
  '  `reason_not_to_test` VARCHAR(100),',
  '  `reasons_for_ineligibility` VARCHAR(100),',
  '  `specific_reason_for_ineligibility` VARCHAR(255),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_hts_eligibility_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_hts_eligibility_screening` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`department`),',
  '  INDEX (`population_type`),',
  '  INDEX (`eligible_for_test`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_hts_eligibility_screening`') AS message;

-- sql
-- --------------------------------------
-- TABLE: etl_patient_appointment
-- Purpose: tenant-aware creation using \`etl_schema\`
-- --------------------------------------
SET @drop_etl_patient_appointment = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_patient_appointment`;');
PREPARE stmt FROM @drop_etl_patient_appointment; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_patient_appointment` (',
  '  `patient_appointment_id` INT NOT NULL,',
  '  `provider_id` INT DEFAULT NULL,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_date` DATE NOT NULL,',
  '  `start_date_time` DATETIME DEFAULT NULL,',
  '  `end_date_time` DATETIME DEFAULT NULL,',
  '  `appointment_service_id` INT DEFAULT NULL,',
  '  `appointment_service_type_id` INT DEFAULT NULL,',
  '  `status` VARCHAR(45) NOT NULL,',
  '  `location_id` INT DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  PRIMARY KEY (`patient_appointment_id`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`location_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`appointment_service_id`),',
  '  CONSTRAINT `fk_etl_patient_appointment_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_patient_appointment`') AS message;


-- sql
-- --------------------------------------
-- TABLE: etl_drug_order
-- Purpose: tenant-aware creation using `etl_schema`
-- Tenant-aware: uses `etl_schema` variable and dynamic CREATE via CONCAT + PREPARE
-- --------------------------------------
SET @drop_etl_drug_order = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_drug_order`;');
PREPARE stmt FROM @drop_etl_drug_order; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_drug_order` (',
  '  `uuid` CHAR(38),',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `order_group_id` INT(11),',
  '  `patient_id` INT(11) NOT NULL,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `visit_id` INT(11),',
  '  `provider` INT(11),',
  '  `order_id` INT(11),',
  '  `urgency` VARCHAR(50),',
  '  `drug_id` INT(11),',
  '  `drug_concept_id` VARCHAR(50),',
  '  `drug_short_name` VARCHAR(50),',
  '  `drug_name` VARCHAR(255),',
  '  `frequency` VARCHAR(100),',
  '  `enc_name` VARCHAR(100),',
  '  `dose` VARCHAR(50),',
  '  `dose_units` VARCHAR(100),',
  '  `quantity` VARCHAR(50),',
  '  `quantity_units` VARCHAR(100),',
  '  `dosing_instructions` VARCHAR(100),',
  '  `duration` INT(11),',
  '  `duration_units` VARCHAR(10),',
  '  `instructions` VARCHAR(255),',
  '  `route` VARCHAR(255),',
  '  `voided` INT(11) DEFAULT 0,',
  '  `date_voided` DATE,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_drug_order_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_drug_order` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`patient_id`, `visit_date`),',
  '  INDEX (`order_id`),',
  '  INDEX (`drug_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_drug_order`') AS message;

-- sql
-- --------------------------------------
-- TABLE: etl_preventive_services
-- Purpose: tenant-aware creation using `etl_schema`
-- Tenant-aware: uses `etl_schema` variable and dynamic CREATE via CONCAT + PREPARE
-- --------------------------------------
SET @drop_etl_preventive_services = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_preventive_services`;');
PREPARE stmt FROM @drop_etl_preventive_services; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_preventive_services` (',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_date` DATE,',
  '  `provider` INT(11),',
  '  `location_id` INT(11),',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `obs_group_id` INT(11) NOT NULL,',
  '  `malaria_prophylaxis_1` DATE,',
  '  `malaria_prophylaxis_2` DATE,',
  '  `malaria_prophylaxis_3` DATE,',
  '  `tetanus_taxoid_1` DATE,',
  '  `tetanus_taxoid_2` DATE,',
  '  `tetanus_taxoid_3` DATE,',
  '  `tetanus_taxoid_4` DATE,',
  '  `tetanus_taxoid_5` DATE,',
  '  `folate_iron_1` DATE,',
  '  `folate_iron_2` DATE,',
  '  `folate_iron_3` DATE,',
  '  `folate_iron_4` DATE,',
  '  `folate_1` DATE,',
  '  `folate_2` DATE,',
  '  `folate_3` DATE,',
  '  `folate_4` DATE,',
  '  `iron_1` DATE,',
  '  `iron_2` DATE,',
  '  `iron_3` DATE,',
  '  `iron_4` DATE,',
  '  `mebendazole` DATE,',
  '  `albendazole` DATE,',
  '  `long_lasting_insecticidal_net` DATE DEFAULT NULL,',
  '  `calcium` DATE,',
  '  `comment` VARCHAR(250) DEFAULT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `date_created` DATETIME NOT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`patient_id`, `encounter_id`, `obs_group_id`),',
  '  CONSTRAINT `fk_etl_preventive_services_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_preventive_services`') AS message;


-- sql
-- --------------------------------------
-- TABLE: etl_overdose_reporting
-- Purpose: tenant-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_overdose_reporting = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_overdose_reporting`;');
PREPARE stmt FROM @drop_etl_overdose_reporting; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_overdose_reporting` (',
  '  `client_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT(11) NOT NULL,',
  '  `provider` INT(11) NOT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `overdose_location` VARCHAR(100) DEFAULT NULL,',
  '  `overdose_date` DATE DEFAULT NULL,',
  '  `incident_type` INT(11) DEFAULT NULL,',
  '  `incident_site_name` VARCHAR(255) DEFAULT NULL,',
  '  `incident_site_type` INT(11) DEFAULT NULL,',
  '  `naloxone_provided` INT(11) DEFAULT NULL,',
  '  `risk_factors` INT(11) DEFAULT NULL,',
  '  `other_risk_factors` VARCHAR(255) DEFAULT NULL,',
  '  `drug` INT(11) DEFAULT NULL,',
  '  `other_drug` VARCHAR(255) DEFAULT NULL,',
  '  `outcome` INT(11) DEFAULT NULL,',
  '  `remarks` VARCHAR(255) DEFAULT NULL,',
  '  `reported_by` VARCHAR(255) DEFAULT NULL,',
  '  `date_reported` DATE DEFAULT NULL,',
  '  `witness` VARCHAR(255) DEFAULT NULL,',
  '  `date_witnessed` DATE DEFAULT NULL,',
  '  `encounter` VARCHAR(255) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_overdose_reporting_patient` FOREIGN KEY (`client_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_overdose_reporting` UNIQUE (`uuid`),',
  '  INDEX (`client_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`naloxone_provided`),',
  '  INDEX (`outcome`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_overdose_reporting`') AS message;

-- --------------------------------------
-- TABLE: etl_art_fast_track
-- Purpose: tenant-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_art_fast_track = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_art_fast_track`;');
PREPARE stmt FROM @drop_etl_art_fast_track; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_art_fast_track` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11) DEFAULT NULL,',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `art_refill_model` INT(11) DEFAULT NULL,',
  '  `ctx_dispensed` INT(11) DEFAULT NULL,',
  '  `dapsone_dispensed` INT(11) DEFAULT NULL,',
  '  `oral_contraceptives_dispensed` INT(11) DEFAULT NULL,',
  '  `condoms_distributed` INT(11) DEFAULT NULL,',
  '  `missed_arv_doses_since_last_visit` INT(11) DEFAULT NULL,',
  '  `doses_missed` INT(11) DEFAULT NULL,',
  '  `fatigue` INT(11) DEFAULT NULL,',
  '  `cough` INT(11) DEFAULT NULL,',
  '  `fever` INT(11) DEFAULT NULL,',
  '  `rash` INT(11) DEFAULT NULL,',
  '  `nausea_vomiting` INT(11) DEFAULT NULL,',
  '  `genital_sore_discharge` INT(11) DEFAULT NULL,',
  '  `diarrhea` INT(11) DEFAULT NULL,',
  '  `other_symptoms` INT(11) DEFAULT NULL,',
  '  `other_specific_symptoms` INT(11) DEFAULT NULL,',
  '  `pregnant` INT(11) DEFAULT NULL,',
  '  `family_planning_status` INT(11) DEFAULT NULL,',
  '  `family_planning_method` VARCHAR(250) DEFAULT NULL,',
  '  `reason_not_on_family_planning` VARCHAR(250) DEFAULT NULL,',
  '  `referred_to_clinic` INT(11) DEFAULT NULL,',
  '  `return_visit_date` DATE DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_art_fast_track_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_art_fast_track` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_art_fast_track`') AS message;


-- sql
SET @drop_etl_clinical_encounter = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_clinical_encounter`;');
PREPARE stmt FROM @drop_etl_clinical_encounter; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_clinical_encounter` (',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT(11) NOT NULL,',
  '  `provider` INT(11) NOT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `visit_type` VARCHAR(100) DEFAULT NULL,',
  '  `referred_from` INT(11) DEFAULT NULL,',
  '  `therapy_ordered` VARCHAR(100) DEFAULT NULL,',
  '  `other_therapy_ordered` VARCHAR(100) DEFAULT NULL,',
  '  `counselling_ordered` VARCHAR(100) DEFAULT NULL,',
  '  `other_counselling_ordered` VARCHAR(100) DEFAULT NULL,',
  '  `procedures_prescribed` INT(11) DEFAULT NULL,',
  '  `procedures_ordered` VARCHAR(100) DEFAULT NULL,',
  '  `patient_outcome` INT(11) DEFAULT NULL,',
  '  `diagnosis_category` VARCHAR(100) DEFAULT NULL,',
  '  `general_examination` VARCHAR(255) DEFAULT NULL,',
  '  `admission_needed` INT(11) DEFAULT NULL,',
  '  `date_of_patient_admission` DATE DEFAULT NULL,',
  '  `admission_reason` VARCHAR(100) DEFAULT NULL,',
  '  `admission_type` VARCHAR(100) DEFAULT NULL,',
  '  `priority_of_admission` VARCHAR(100) DEFAULT NULL,',
  '  `admission_ward` VARCHAR(100) DEFAULT NULL,',
  '  `hospital_stay` VARCHAR(100) DEFAULT NULL,',
  '  `referral_needed` VARCHAR(100) DEFAULT NULL,',
  '  `referral_ordered` VARCHAR(100) DEFAULT NULL,',
  '  `referral_to` VARCHAR(100) DEFAULT NULL,',
  '  `other_facility` VARCHAR(100) DEFAULT NULL,',
  '  `this_facility` VARCHAR(100) DEFAULT NULL,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_clinical_encounter_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_clinical_encounter` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_clinical_encounter`') AS message;


-- --------------------------------------
-- TABLE: etl_pep_management_survivor
-- Purpose: tenant-aware creation using `etl_schema`
-- Source: dynamic CREATE via CONCAT + PREPARE
-- --------------------------------------
SET @drop_etl_pep = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_pep_management_survivor`;');
PREPARE stmt FROM @drop_etl_pep; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_pep_management_survivor` (',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT(11) NOT NULL,',
  '  `provider` INT(11) NOT NULL,',
  '  `visit_date` DATE,',
  '  `prc_number` VARCHAR(100),',
  '  `incident_reporting_date` DATE,',
  '  `type_of_violence` INT(11),',
  '  `disabled` INT(11),',
  '  `other_type_of_violence` VARCHAR(255),',
  '  `type_of_assault` VARCHAR(255),',
  '  `other_type_of_assault` VARCHAR(255),',
  '  `incident_date` DATE,',
  '  `perpetrator_identity` VARCHAR(100),',
  '  `survivor_relation_to_perpetrator` INT(11),',
  '  `perpetrator_compulsory_HIV_test_done` INT(11),',
  '  `perpetrator_compulsory_HIV_test_result` INT(11),',
  '  `perpetrator_file_number` VARCHAR(100),',
  '  `survivor_state` VARCHAR(255),',
  '  `clothing_state` VARCHAR(255),',
  '  `other_injuries` VARCHAR(255),',
  '  `genitalia_examination` VARCHAR(255),',
  '  `high_vaginal_or_anal_swab` VARCHAR(255),',
  '  `rpr_vdrl` INT(11),',
  '  `survivor_hiv_test_result` INT(11),',
  '  `given_pep` INT(11),',
  '  `referred_to_psc` INT(11),',
  '  `pdt` INT(11),',
  '  `emergency_contraception_issued` INT(11),',
  '  `reason_emergency_contraception_not_issued` INT(11),',
  '  `sti_prophylaxis_and_treatment` INT(11),',
  '  `reason_sti_prophylaxis_not_issued` VARCHAR(255),',
  '  `pep_regimen_issued` INT(11),',
  '  `reason_pep_regimen_not_issued` VARCHAR(255),',
  '  `starter_pack_given` INT(11),',
  '  `date_given_pep` DATE,',
  '  `HBsAG_result` INT(11),',
  '  `LFTs_ALT` VARCHAR(100),',
  '  `RFTs_creatinine` VARCHAR(100),',
  '  `other_tests` VARCHAR(255),',
  '  `next_appointment_date` DATE,',
  '  `voided` INT(11) DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_pep_survivor_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_pep_survivor` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`type_of_violence`),',
  '  INDEX (`incident_reporting_date`),',
  '  INDEX (`type_of_assault`),',
  '  INDEX (`incident_date`),',
  '  INDEX (`survivor_relation_to_perpetrator`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_pep_management_survivor`') AS message;


-- sql
-- --------------------------------------
-- TABLE: etl_sgbv_pep_followup
-- Purpose: tenant-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_sgbv = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_sgbv_pep_followup`;');
PREPARE stmt FROM @drop_etl_sgbv; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_sgbv_pep_followup` (',
  '`patient_id` INT(11) NOT NULL,',
  '`visit_id` INT(11) DEFAULT NULL,',
  '`encounter_id` INT(11) NOT NULL,',
  '`uuid` CHAR(38) NOT NULL,',
  '`location_id` INT(11) NOT NULL,',
  '`provider` INT(11) NOT NULL,',
  '`visit_date` DATE,',
  '`visit_number` INT(11),',
  '`pep_completed` INT(11),',
  '`reason_pep_not_completed` VARCHAR(255),',
  '`hiv_test_done` INT(11),',
  '`hiv_test_result` INT(11),',
  '`pdt_test_done` INT(11),',
  '`pdt_test_result` INT(11),',
  '`HBsAG_test_done` INT(11),',
  '`HBsAG_test_result` INT(11),',
  '`lfts_alt` VARCHAR(50),',
  '`rfts_creatinine` VARCHAR(50),',
  '`three_month_post_exposure_HIV_serology_result` INT(11),',
  '`patient_assessment` VARCHAR(255),',
  '`next_appointment_date` DATE,',
  '`voided` INT(11) DEFAULT 0,',
  'PRIMARY KEY (`encounter_id`),',
  'CONSTRAINT `fk_sgbv_pep_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  'CONSTRAINT `unique_uuid_sgbv_pep` UNIQUE (`uuid`),',
  'INDEX (`patient_id`),',
  'INDEX (`visit_id`),',
  'INDEX (`visit_date`),',
  'INDEX (`visit_number`),',
  'INDEX (`pep_completed`),',
  'INDEX (`three_month_post_exposure_HIV_serology_result`),',
  'INDEX (`hiv_test_result`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_sgbv_pep_followup`') AS message;


-- sql
-- --------------------------------------
-- TABLE: etl_sgbv_post_rape_care
-- Purpose: tenant-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_sgbv_post_rape = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_sgbv_post_rape_care`;');
PREPARE stmt FROM @drop_etl_sgbv_post_rape; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_sgbv_post_rape_care` (',
  '`patient_id` INT(11) NOT NULL,',
  '`visit_id` INT(11) DEFAULT NULL,',
  '`encounter_id` INT(11) NOT NULL,',
  '`uuid` CHAR(38) NOT NULL,',
  '`location_id` INT(11) NOT NULL,',
  '`provider` INT(11) NOT NULL,',
  '`visit_date` DATE,',
  '`examination_date` DATE,',
  '`incident_date` DATE,',
  '`number_of_perpetrators` VARCHAR(10),',
  '`is_perpetrator_known` INT(11),',
  '`survivor_relation_to_perpetrator` VARCHAR(100),',
  '`county` VARCHAR(100),',
  '`sub_county` VARCHAR(100),',
  '`landmark` VARCHAR(100),',
  '`observation_on_chief_complaint` VARCHAR(255),',
  '`chief_complaint_report` VARCHAR(255),',
  '`circumstances_around_incident` VARCHAR(255),',
  '`type_of_sexual_violence` INT(11),',
  '`other_type_of_sexual_violence` VARCHAR(255),',
  '`use_of_condoms` INT(11),',
  '`prior_attendance_to_health_facility` INT(11),',
  '`attended_health_facility_name` VARCHAR(100),',
  '`date_attended_health_facility` DATE,',
  '`treated_at_facility` INT(11),',
  '`given_referral_notes` INT(11),',
  '`incident_reported_to_police` INT(11),',
  '`police_station_name` VARCHAR(100),',
  '`police_report_date` DATE,',
  '`medical_or_surgical_history` VARCHAR(255),',
  '`additional_info_from_survivor` VARCHAR(255),',
  '`physical_examination` VARCHAR(255),',
  '`parity_term` INT(11),',
  '`parity_abortion` INT(11),',
  '`on_contraception` INT(11),',
  '`known_pregnancy` INT(11),',
  '`date_of_last_consensual_sex` DATE,',
  '`systolic` INT(11),',
  '`diastolic` INT(11),',
  '`demeanor` INT(11),',
  '`changed_clothes` INT(11),',
  '`state_of_clothes` VARCHAR(100),',
  '`means_clothes_transported` INT(11),',
  '`details_about_clothes_transport` VARCHAR(255),',
  '`clothes_handed_to_police` INT(11),',
  '`survivor_went_to_toilet` INT(11),',
  '`survivor_bathed` INT(11),',
  '`bath_details` VARCHAR(255),',
  '`survivor_left_marks_on_perpetrator` INT(11),',
  '`details_of_marks_on_perpetrator` INT(11),',
  '`physical_injuries` VARCHAR(255),',
  '`details_outer_genitalia` VARCHAR(255),',
  '`details_vagina` VARCHAR(255),',
  '`details_hymen` VARCHAR(255),',
  '`details_anus` VARCHAR(255),',
  '`significant_orifice` VARCHAR(255),',
  '`pep_first_dose` INT(11),',
  '`ecp_given` INT(11),',
  '`stitching_done` INT(11),',
  '`stitching_notes` VARCHAR(255),',
  '`treated_for_sti` INT(11),',
  '`sti_treatment_remarks` VARCHAR(255),',
  '`other_medications` VARCHAR(255),',
  '`referred_to` VARCHAR(255),',
  '`web_prep_microscopy` INT(11),',
  '`samples_packed` VARCHAR(255),',
  '`examining_officer` VARCHAR(100),',
  '`voided` INT(11) DEFAULT 0,',
  'PRIMARY KEY (`encounter_id`),',
  'CONSTRAINT `fk_sgbv_post_rape_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  'CONSTRAINT `unique_uuid_sgbv_post_rape` UNIQUE (`uuid`),',
  'INDEX (`patient_id`),',
  'INDEX (`visit_id`),',
  'INDEX (`visit_date`),',
  'INDEX (`incident_date`),',
  'INDEX (`pep_first_dose`),',
  'INDEX (`ecp_given`),',
  'INDEX (`survivor_relation_to_perpetrator`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_sgbv_post_rape_care`') AS message;


-- MULTITENANT: create etl_gbv_physical_emotional_abuse using `etl_schema`
SET @drop_etl_gbv = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_gbv_physical_emotional_abuse`;');
PREPARE stmt FROM @drop_etl_gbv; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_gbv_physical_emotional_abuse` (',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT(11) NOT NULL,',
  '  `provider` INT(11) NOT NULL,',
  '  `visit_date` DATE,',
  '  `gbv_number` VARCHAR(100),',
  '  `referred_from` INT(11),',
  '  `entry_point` INT(11),',
  '  `other_referral_source` VARCHAR(100),',
  '  `type_of_violence` INT(11),',
  '  `date_of_incident` DATE,',
  '  `trauma_counselling` INT(11),',
  '  `trauma_counselling_comments` VARCHAR(255),',
  '  `referred_to` VARCHAR(255),',
  '  `other_referral` VARCHAR(255),',
  '  `next_appointment_date` DATE,',
  '  `voided` INT(11) DEFAULT 0,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_gbv_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_gbv_physical_emotional_abuse` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`entry_point`),',
  '  INDEX (`referred_from`),',
  '  INDEX (`date_of_incident`),',
  '  INDEX (`type_of_violence`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_gbv_physical_emotional_abuse`') AS message;

-- sql
-- --------------------------------------
-- TABLE: etl_family_planning
-- Purpose: tenant-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_family_planning = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_family_planning`;');
PREPARE stmt FROM @drop_etl_family_planning; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_family_planning` (',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT(11) NOT NULL,',
  '  `provider` INT(11) NOT NULL,',
  '  `visit_date` DATE,',
  '  `first_user_of_contraceptive` INT(11),',
  '  `counselled_on_fp` INT(11),',
  '  `contraceptive_dispensed` INT(11),',
  '  `type_of_visit_for_method` INT(11),',
  '  `type_of_service` INT(11),',
  '  `quantity_dispensed` VARCHAR(10),',
  '  `reasons_for_larc_removal` INT(11),',
  '  `other_reasons_for_larc_removal` VARCHAR(255),',
  '  `counselled_on_natural_fp` INT(11),',
  '  `circle_beads_given` INT(11),',
  '  `receiving_postpartum_fp` INT(11),',
  '  `experienced_intimate_partner_violence` INT(11),',
  '  `referred_for_fp` INT(11),',
  '  `referred_to` INT(11),',
  '  `referred_from` INT(11),',
  '  `reasons_for_referral` VARCHAR(255),',
  '  `voided` INT(11) DEFAULT 0,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_family_planning_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_family_planning` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`type_of_visit_for_method`),',
  '  INDEX (`referred_from`),',
  '  INDEX (`contraceptive_dispensed`),',
  '  INDEX (`receiving_postpartum_fp`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_family_planning`') AS message;


-- sql
-- --------------------------------------
-- TABLE: etl_physiotherapy
-- Purpose: tenant-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_physiotherapy = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_physiotherapy`;');
PREPARE stmt FROM @drop_etl_physiotherapy; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_physiotherapy` (',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT(11) NOT NULL,',
  '  `provider` INT(11) NOT NULL,',
  '  `visit_date` DATE,',
  '  `visit_type` INT(11),',
  '  `referred_from` INT(11),',
  '  `referred_from_department` INT(11),',
  '  `referred_from_department_other` VARCHAR(100),',
  '  `number_of_sessions` INT(11),',
  '  `referral_reason` VARCHAR(255),',
  '  `disorder_category` INT(11),',
  '  `other_disorder_category` VARCHAR(255),',
  '  `clinical_notes` VARCHAR(255),',
  '  `pin_scale` INT(11),',
  '  `affected_region` INT(11),',
  '  `range_of_motion` INT(11),',
  '  `strength_test` INT(11),',
  '  `functional_assessment` INT(11),',
  '  `assessment_finding` VARCHAR(255),',
  '  `goals` VARCHAR(255),',
  '  `planned_interventions` INT(11),',
  '  `other_interventions` VARCHAR(255),',
  '  `sessions_per_week` VARCHAR(255),',
  '  `patient_outcome` INT(11),',
  '  `referred_for` VARCHAR(255),',
  '  `referred_to` INT(11),',
  '  `transfer_to_facility` VARCHAR(255),',
  '  `services_referred_for` VARCHAR(255),',
  '  `date_of_admission` DATE,',
  '  `reason_for_admission` VARCHAR(255),',
  '  `type_of_admission` INT(11),',
  '  `priority_of_admission` INT(11),',
  '  `admission_ward` INT(11),',
  '  `duration_of_hospital_stay` INT(11),',
  '  `voided` INT(11) DEFAULT 0,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_physiotherapy_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_physiotherapy` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_physiotherapy`') AS message;

-- sql
-- --------------------------------------
-- TABLE: etl_psychiatry
-- Purpose: tenant-aware creation using `etl_schema`
-- Tenant-aware: uses `etl_schema` variable and dynamic CREATE via CONCAT + PREPARE
-- --------------------------------------
SET @drop_etl_psychiatry = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_psychiatry`;');
PREPARE stmt FROM @drop_etl_psychiatry; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_psychiatry` (',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT(11) NOT NULL,',
  '  `provider` INT(11) NOT NULL,',
  '  `visit_date` DATE,',
  '  `visit_type` INT(11),',
  '  `referred_from` INT(11),',
  '  `referred_from_department` INT(11),',
  '  `presenting_allegations` INT(11),',
  '  `other_allegations` VARCHAR(255),',
  '  `contact_with_TB_case` INT(11),',
  '  `history_of_present_illness` VARCHAR(255),',
  '  `surgical_history` INT(11),',
  '  `type_of_surgery` VARCHAR(255),',
  '  `surgery_date` DATE,',
  '  `on_medication` INT(11),',
  '  `childhood_mistreatment` INT(11),',
  '  `persistent_cruelty_meanness` INT(11),',
  '  `physically_abused` INT(11),',
  '  `sexually_abused` INT(11),',
  '  `patient_occupation_history` VARCHAR(255),',
  '  `reproductive_history` VARCHAR(255),',
  '  `lmp_date` INT(11),',
  '  `general_examination_findings` VARCHAR(255),',
  '  `mental_status` INT(11),',
  '  `attitude_and_behaviour` INT(11),',
  '  `speech` INT(11),',
  '  `mood` INT(11),',
  '  `illusions` INT(11),',
  '  `attention_concentration` INT(11),',
  '  `memory_recall` INT(11),',
  '  `judgement` INT(11),',
  '  `insight` INT(11),',
  '  `affect` VARCHAR(255),',
  '  `thought_process` VARCHAR(255),',
  '  `thought_content` VARCHAR(255),',
  '  `hallucinations` VARCHAR(255),',
  '  `orientation_status` VARCHAR(255),',
  '  `management_plan` VARCHAR(255),',
  '  `counselling_prescribed` VARCHAR(255),',
  '  `patient_outcome` INT(11),',
  '  `referred_to` INT(11),',
  '  `facility_transferred_to` VARCHAR(255),',
  '  `date_of_admission` DATE,',
  '  `reason_for_admission` VARCHAR(255),',
  '  `type_of_admission` INT(11),',
  '  `priority_of_admission` INT(11),',
  '  `admission_ward` INT(11),',
  '  `duration_of_hospital_stay` INT(11),',
  '  `voided` INT(11),',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_psychiatry_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_psychiatry` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_psychiatry`') AS message;


-- --------------------------------------
-- TABLE: etl_kvp_clinical_enrollment
-- Purpose: tenant-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_kvp = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_kvp_clinical_enrollment`;');
PREPARE stmt FROM @drop_etl_kvp; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_kvp_clinical_enrollment` (',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT(11) NOT NULL,',
  '  `provider` INT(11) NOT NULL,',
  '  `visit_date` DATE,',
  '  `contacted_by_pe_for_health_services` INT(11),',
  '  `has_regular_non_paying_sexual_partner` INT(11),',
  '  `number_of_sexual_partners` INT(11),',
  '  `year_started_fsw` INT(11),',
  '  `year_started_msm` INT(11),',
  '  `year_started_using_drugs` INT(11),',
  '  `trucker_duration_on_transit` INT(11),',
  '  `duration_working_as_trucker` INT(11),',
  '  `duration_working_as_fisherfolk` INT(11),',
  '  `year_tested_discordant_couple` INT(11),',
  '  `ever_experienced_violence` INT(11),',
  '  `type_of_violence_experienced` INT(11),',
  '  `ever_tested_for_hiv` INT(11),',
  '  `latest_hiv_test_method` INT(11),',
  '  `latest_hiv_test_results` INT(11),',
  '  `willing_to_test_for_hiv` INT(11),',
  '  `reason_not_willing_to_test_for_hiv` VARCHAR(255),',
  '  `receiving_hiv_care` INT(11),',
  '  `hiv_care_facility` INT(11),',
  '  `other_hiv_care_facility` INT(11),',
  '  `ccc_number` VARCHAR(50),',
  '  `consent_followup` VARCHAR(50),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `voided` INT(11),',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_kvp_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_kvp` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_kvp_clinical_enrollment`') AS message;


-- sql
-- --------------------------------------
-- TABLE: etl_special_clinics
-- Purpose: tenant-aware creation using `etl_schema`
-- --------------------------------------
SET @drop_etl_special = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_special_clinics`;');
PREPARE stmt FROM @drop_etl_special; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_special_clinics` (',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT(11) NOT NULL,',
  '  `provider` INT(11) NOT NULL,',
  '  `visit_date` DATE,',
  '  `visit_type` INT(11),',
  '  `pregnantOrLactating` INT(11),',
  '  `referred_from` INT(11),',
  '  `eye_assessed` INT(11),',
  '  `acuity_finding` INT(11),',
  '  `referred_to` INT(11),',
  '  `ot_intervention` VARCHAR(255),',
  '  `assistive_technology` VARCHAR(255),',
  '  `enrolled_in_school` INT(11),',
  '  `patient_with_disability` INT(11),',
  '  `patient_has_edema` INT(11),',
  '  `nutritional_status` INT(11),',
  '  `patient_pregnant` INT(11),',
  '  `sero_status` INT(11),',
  '  `nutritional_intervention` INT(11),',
  '  `postnatal` INT(11),',
  '  `patient_on_arv` INT(11),',
  '  `anaemia_level` INT(11),',
  '  `metabolic_disorders` VARCHAR(255),',
  '  `critical_nutrition_practices` VARCHAR(255),',
  '  `maternal_nutrition` INT(11),',
  '  `therapeutic_food` VARCHAR(255),',
  '  `supplemental_food` VARCHAR(255),',
  '  `micronutrients` VARCHAR(255),',
  '  `referral_status` INT(11),',
  '  `criteria_for_admission` INT(11),',
  '  `type_of_admission` INT(11),',
  '  `cadre` INT(11),',
  '  `neuron_developmental_findings` VARCHAR(255),',
  '  `neurodiversity_conditions` INT(11),',
  '  `learning_findings` VARCHAR(255),',
  '  `screening_site` INT(11),',
  '  `communication_mode` INT(11),',
  '  `neonatal_risk_factor` INT(11),',
  '  `presence_of_comobidities` VARCHAR(255),',
  '  `first_screening_date` DATE,',
  '  `first_screening_outcome` INT(11),',
  '  `second_screening_outcome` INT(11),',
  '  `symptoms_for_otc` VARCHAR(255),',
  '  `nutritional_details` INT(11),',
  '  `first_0_6_months` INT(11),',
  '  `second_6_12_months` INT(11),',
  '  `disability_classification` VARCHAR(255),',
  '  `treatment_intervention` VARCHAR(255),',
  '  `area_of_service` INT(11),',
  '  `diagnosis_category` VARCHAR(100),',
  '  `next_appointment_date` DATE,',
  '  `orthopaedic_patient_no` INT(11),',
  '  `patient_outcome` INT(11),',
  '  `special_clinic` VARCHAR(255),',
  '  `special_clinic_form_uuid` CHAR(38),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_special_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`),',
  '  CONSTRAINT `unique_uuid_special` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_type`),',
  '  INDEX (`visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_special_clinics`') AS message;



-- sql
-- File: `src/main/resources/sql/hiv/DDL.sql`
-- --------------------------------------
-- TABLE: etl_high_iit_intervention
-- Purpose: tenant-aware creation using `etl_schema`
-- Tenant-aware: dynamic DROP/CREATE via CONCAT + PREPARE
-- --------------------------------------
SET @drop_etl_high_iit_intervention = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_high_iit_intervention`;');
PREPARE stmt FROM @drop_etl_high_iit_intervention; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_high_iit_intervention` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11),',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `interventions_offered` VARCHAR(500),',
  '  `appointment_mgt_interventions` VARCHAR(500),',
  '  `reminder_methods` VARCHAR(255),',
  '  `enrolled_in_ushauri` INT(11),',
  '  `appointment_mngt_intervention_date` DATE,',
  '  `date_assigned_case_manager` DATE,',
  '  `eacs_recommended` INT(11),',
  '  `enrolled_in_psychosocial_support_group` INT(11),',
  '  `robust_literacy_interventions_date` DATE,',
  '  `expanding_differentiated_service_delivery_interventions` INT(11),',
  '  `enrolled_in_nishauri` INT(11),',
  '  `expanded_differentiated_service_delivery_interventions_date` DATE,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_high_iit_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`),',
  '  CONSTRAINT `unique_uuid_high_iit` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_high_iit_intervention`') AS message;


-- --------------------------------------
-- File: `src/main/resources/sql/hiv/DDL.sql`
-- TABLE: etl_home_visit_checklist
-- Purpose: tenant aware creation using `etl_schema`
-- Tenant-aware: dynamic DROP/CREATE via CONCAT + PREPARE
-- --------------------------------------
SET @drop_etl_home_visit_checklist = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_home_visit_checklist`;');
PREPARE stmt FROM @drop_etl_home_visit_checklist; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_home_visit_checklist` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11),',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `independence_in_daily_activities` VARCHAR(255),',
  '  `other_independence_activities` VARCHAR(255),',
  '  `meeting_basic_needs` VARCHAR(255),',
  '  `other_basic_needs` VARCHAR(255),',
  '  `disclosure_to_sexual_partner` INT(11),',
  '  `disclosure_to_household_members` INT(11),',
  '  `disclosure_to` VARCHAR(255),',
  '  `mode_of_storing_arv_drugs` VARCHAR(255),',
  '  `arv_drugs_taking_regime` VARCHAR(255),',
  '  `receives_household_social_support` INT(11),',
  '  `household_social_support_given` VARCHAR(255),',
  '  `receives_community_social_support` INT(11),',
  '  `community_social_support_given` VARCHAR(255),',
  '  `linked_to_non_clinical_services` VARCHAR(255),',
  '  `linked_to_other_services` VARCHAR(255),',
  '  `has_mental_health_issues` INT(11),',
  '  `suffering_stressful_situation` INT(11),',
  '  `uses_drugs_alcohol` INT(11),',
  '  `has_side_medications_effects` INT(11),',
  '  `medication_side_effects` VARCHAR(255),',
  '  `assessment_notes` VARCHAR(255),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_home_visit_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`),',
  '  CONSTRAINT `unique_uuid_home_visit` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_home_visit_checklist`') AS message;


-- sql
-- File: `src/main/resources/sql/hiv/DDL.sql`
-- --------------------------------------
-- TABLE: etl_ncd_enrollment
-- Purpose: tenant-aware creation using `etl_schema`
-- Tenant-aware: dynamic DROP/CREATE via CONCAT + PREPARE
-- --------------------------------------
SET @drop_etl_ncd = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_ncd_enrollment`;');
PREPARE stmt FROM @drop_etl_ncd; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_ncd_enrollment` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11),',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `visit_type` VARCHAR(255) DEFAULT NULL,',
  '  `referred_from` INT(11),',
  '  `referred_from_department` INT(11),',
  '  `referred_from_department_other` VARCHAR(100),',
  '  `patient_complaint` INT(11),',
  '  `specific_complaint` VARCHAR(255),',
  '  `disease_type` INT(11),',
  '  `diabetes_condition` INT(11),',
  '  `diabetes_type` INT(11),',
  '  `diabetes_diagnosis_date` DATE,',
  '  `hypertension_condition` INT(11),',
  '  `hypertension_stage` VARCHAR(100),',
  '  `hypertension_type` INT(11),',
  '  `comorbid_condition` INT(11),',
  '  `diagnosis_date` DATE,',
  '  `hiv_status` INT(11),',
  '  `hiv_positive_on_art` INT(11),',
  '  `tb_screening` INT(11),',
  '  `smoke_check` INT(11),',
  '  `date_stopped_smoke` DATE,',
  '  `drink_alcohol` INT(11),',
  '  `date_stopped_alcohol` DATE,',
  '  `cessation_counseling` INT(11),',
  '  `physical_activity` INT(11),',
  '  `diet_routine` INT(11),',
  '  `existing_complications` VARCHAR(500),',
  '  `other_existing_complications` VARCHAR(500),',
  '  `new_complications` VARCHAR(500),',
  '  `other_new_complications` VARCHAR(500),',
  '  `examination_findings` VARCHAR(500),',
  '  `cardiovascular` INT(11),',
  '  `respiratory` INT(11),',
  '  `abdominal_pelvic` INT(11),',
  '  `neurological` INT(11),',
  '  `oral_exam` INT(11),',
  '  `foot_risk` INT(11),',
  '  `foot_low_risk` VARCHAR(500),',
  '  `foot_high_risk` VARCHAR(500),',
  '  `diabetic_foot` INT(11),',
  '  `describe_diabetic_foot_type` VARCHAR(255),',
  '  `treatment_given` VARCHAR(255),',
  '  `other_treatment_given` VARCHAR(255),',
  '  `lifestyle_advice` VARCHAR(255),',
  '  `nutrition_assessment` VARCHAR(255),',
  '  `footcare_outcome` INT(11),',
  '  `referred_to` VARCHAR(255),',
  '  `reasons_for_referral` VARCHAR(255),',
  '  `clinical_notes` VARCHAR(255),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `voided` INT(11),',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_ncd_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`),',
  '  CONSTRAINT `unique_uuid_ncd` UNIQUE (`uuid`),',
  '  INDEX(`visit_date`),',
  '  INDEX(`encounter_id`),',
  '  INDEX(`patient_id`),',
  '  INDEX(`disease_type`),',
  '  INDEX(`diabetes_type`),',
  '  INDEX(`hypertension_type`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_ncd_enrollment`') AS message;



-- sql
-- TABLE: etl_adr_assessment_tool
-- Purpose: tenant-aware creation using `etl_schema`
-- Tenant-aware: dynamic DROP/CREATE via CONCAT + PREPARE
SET @drop_etl_adr = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_adr_assessment_tool`;');
PREPARE stmt FROM @drop_etl_adr; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_adr_assessment_tool` (',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT(11) NOT NULL,',
  '  `provider` INT(11) NOT NULL,',
  '  `visit_date` DATE,',
  '  `weight_taken` INT(11),',
  '  `weight_not_taken_specify` VARCHAR(255),',
  '  `taking_arvs_everyday` INT(11),',
  '  `not_taking_arvs_everyday` VARCHAR(255),',
  '  `correct_dosage_per_weight` INT(11),',
  '  `dosage_not_correct_specify` VARCHAR(255),',
  '  `arv_dosage_frequency` INT(11),',
  '  `other_medication_dosage_frequency` INT(11),',
  '  `arv_medication_time` INT(11),',
  '  `arv_timing_working` INT(11),',
  '  `arv_timing_not_working_specify` VARCHAR(255),',
  '  `other_medication_time` INT(11),',
  '  `other_medication_timing_working` INT(11),',
  '  `other_medication_time_not_working_specify` VARCHAR(255),',
  '  `arv_frequency_difficult_to_follow` INT(11),',
  '  `difficult_arv_to_follow_specify` VARCHAR(255),',
  '  `difficulty_with_arv_tablets_or_liquids` INT(11),',
  '  `difficulty_with_arv_tablets_or_liquids_specify` VARCHAR(255),',
  '  `othe_drugs_frequency_difficult_to_follow` INT(11),',
  '  `difficult_other_drugs_to_follow_specify` VARCHAR(255),',
  '  `difficulty_other_drugs_tablets_or_liquids` INT(11),',
  '  `difficulty_other_drugs_tablets_or_liquids_specify` VARCHAR(255),',
  '  `arv_difficulty_due_to_taste_or_size` INT(11),',
  '  `arv_difficulty_due_to_taste_or_size_specify` VARCHAR(255),',
  '  `arv_symptoms_on_intake` VARCHAR(500),',
  '  `laboratory_abnormalities` INT(11),',
  '  `laboratory_abnormalities_specify` INT(11),',
  '  `summary_findings` VARCHAR(500),',
  '  `severity_of_reaction` INT(11),',
  '  `reaction_seriousness` INT(11),',
  '  `reason_for_seriousness` INT(11),',
  '  `action_taken_on_reaction` INT(11),',
  '  `reaction_resolved_on_dose_change` INT(11),',
  '  `reaction_reappeared_after_drug_introduced` INT(11),',
  '  `laboratory_investigations_done` VARCHAR(255),',
  '  `outcome` INT(11),',
  '  `reported_adr_to_pharmacy_board` INT(11),',
  '  `name_of_adr` VARCHAR(255),',
  '  `adr_report_number` VARCHAR(50),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_adr_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics` (`patient_id`),',
  '  CONSTRAINT `unique_uuid_adr` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_adr_assessment_tool`') AS message;


-- sql
-- --------------------------------------
-- File: `src/main/resources/sql/hiv/DDL.sql`
-- TABLE: etl_ncd_followup
-- Purpose: tenant-aware creation using `etl_schema`
-- Tenant-aware: dynamic DROP/CREATE via CONCAT + PREPARE
-- --------------------------------------
SET @drop_etl_ncd_followup = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_ncd_followup`;');
PREPARE stmt FROM @drop_etl_ncd_followup; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_ncd_followup` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT(11),',
  '  `patient_id` INT(11) NOT NULL,',
  '  `visit_id` INT(11),',
  '  `visit_date` DATE,',
  '  `location_id` INT(11) DEFAULT NULL,',
  '  `encounter_id` INT(11) NOT NULL,',
  '  `visit_type` VARCHAR(255) DEFAULT NULL,',
  '  `tobacco_use` INT(11),',
  '  `drink_alcohol` INT(11),',
  '  `physical_activity` INT(11),',
  '  `healthy_diet` INT(11),',
  '  `patient_complaint` INT(11),',
  '  `specific_complaint` VARCHAR(500),',
  '  `other_specific_complaint` VARCHAR(500),',
  '  `examination_findings` VARCHAR(500),',
  '  `cardiovascular` INT(11),',
  '  `respiratory` INT(11),',
  '  `abdominal_pelvic` INT(11),',
  '  `neurological` INT(11),',
  '  `oral_exam` INT(11),',
  '  `foot_exam` VARCHAR(255),',
  '  `diabetic_foot` INT(11),',
  '  `foot_risk_assessment` VARCHAR(100),',
  '  `diabetic_foot_risk` INT(11),',
  '  `adhering_medication` INT(11),',
  '  `referred_to` VARCHAR(255),',
  '  `reasons_for_referral` VARCHAR(255),',
  '  `clinical_notes` VARCHAR(255),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `voided` INT(11),',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_ncd_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics`(`patient_id`),',
  '  CONSTRAINT `unique_uuid_ncd` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_ncd_followup`') AS message;


-- sql
-- --------------------------------------
-- TABLE: etl_inpatient_admission
-- Purpose: tenant-aware creation using `etl_schema`
-- Tenant-aware: dynamic DROP/CREATE via CONCAT + PREPARE
-- --------------------------------------
SET @drop_etl_inpatient_admission = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_inpatient_admission`;');
PREPARE stmt FROM @drop_etl_inpatient_admission; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_inpatient_admission` (',
  '`patient_id` INT(11) NOT NULL,',
  '`visit_id` INT(11) DEFAULT NULL,',
  '`encounter_id` INT(11) NOT NULL,',
  '`uuid` CHAR(38) NOT NULL,',
  '`location_id` INT(11) NOT NULL,',
  '`provider` INT(11) NOT NULL,',
  '`visit_date` DATE,',
  '`admission_date` DATE,',
  '`payment_mode` INT(11),',
  '`admission_location_id` INT(11),',
  '`admission_location_name` VARCHAR(255),',
  '`date_created` DATETIME NOT NULL,',
  '`date_last_modified` DATETIME,',
  '`voided` INT(11),',
  'PRIMARY KEY (`encounter_id`),',
  'CONSTRAINT `fk_inpatient_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics`(`patient_id`),',
  'CONSTRAINT `unique_uuid_inpatient` UNIQUE (`uuid`),',
  'INDEX (`patient_id`),',
  'INDEX (`visit_id`),',
  'INDEX (`visit_date`),',
  'INDEX (`admission_date`),',
  'INDEX (`payment_mode`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_inpatient_admission`') AS message;

-- sql
-- --------------------------------------
-- TABLE: etl_inpatient_discharge
-- Purpose: tenant-aware creation using `etl_schema`
-- Tenant-aware: dynamic DROP/CREATE via CONCAT + PREPARE
-- --------------------------------------
SET @drop_etl_inpatient_discharge = CONCAT('DROP TABLE IF EXISTS `', etl_schema, '`.`etl_inpatient_discharge`;');
PREPARE stmt FROM @drop_etl_inpatient_discharge; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS `', etl_schema, '`.`etl_inpatient_discharge` (',
  '`patient_id` INT(11) NOT NULL,',
  '`visit_id` INT(11) DEFAULT NULL,',
  '`encounter_id` INT(11) NOT NULL,',
  '`uuid` CHAR(38) NOT NULL,',
  '`location_id` INT(11) NOT NULL,',
  '`provider` INT(11) NOT NULL,',
  '`visit_date` DATE,',
  '`discharge_instructions` VARCHAR(255),',
  '`discharge_status` INT(11),',
  '`follow_up_date` DATE,',
  '`followup_specialist` INT(11),',
  '`date_created` DATETIME NOT NULL,',
  '`date_last_modified` DATETIME,',
  '`voided` INT(11),',
  'PRIMARY KEY (`encounter_id`),',
  'CONSTRAINT `fk_inpatient_discharge_patient` FOREIGN KEY (`patient_id`) REFERENCES `', etl_schema, '`.`etl_patient_demographics`(`patient_id`),',
  'CONSTRAINT `unique_uuid_inpatient_discharge` UNIQUE (`uuid`),',
  'INDEX (`patient_id`),',
  'INDEX (`visit_id`),',
  'INDEX (`visit_date`),',
  'INDEX (`discharge_status`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_inpatient_discharge`') AS message;

END $$

DELIMITER ;