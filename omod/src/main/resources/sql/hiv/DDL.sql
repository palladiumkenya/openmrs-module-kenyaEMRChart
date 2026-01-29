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


-- ---------------------------------------------------------
-- TABLE: etl_hiv_enrollment
-- ---------------------------------------------------------
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_hiv_enrollment`');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
      'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_hiv_enrollment` (',
      ' uuid CHAR(38),',
      ' patient_id INT NOT NULL,',
      ' visit_id INT DEFAULT NULL,',
      ' visit_date DATE,',
      ' location_id INT DEFAULT NULL,',
      ' encounter_id INT NOT NULL PRIMARY KEY,',
      ' encounter_provider INT,',
      ' patient_type INT,',
      ' date_first_enrolled_in_care DATE,',
      ' entry_point INT,',
      ' transfer_in_date DATE,',
      ' facility_transferred_from VARCHAR(255),',
      ' district_transferred_from VARCHAR(255),',
      ' date_started_art_at_transferring_facility DATE,',
      ' date_confirmed_hiv_positive DATE,',
      ' facility_confirmed_hiv_positive VARCHAR(255),',
      ' previous_regimen VARCHAR(255),',
      ' arv_status INT,',
      ' ever_on_pmtct INT,',
      ' ever_on_pep INT,',
      ' ever_on_prep INT,',
      ' ever_on_haart INT,',
      ' cd4_test_result INT,',
      ' cd4_test_date DATE,',
      ' viral_load_test_result INT,',
      ' viral_load_test_date DATE,',
      ' who_stage INT,',
      ' name_of_treatment_supporter VARCHAR(255),',
      ' relationship_of_treatment_supporter INT,',
      ' treatment_supporter_telephone VARCHAR(100),',
      ' treatment_supporter_address VARCHAR(100),',
      ' in_school INT DEFAULT NULL,',
      ' orphan INT DEFAULT NULL,',
      ' date_of_discontinuation DATETIME,',
      ' discontinuation_reason INT,',
      ' date_created DATETIME NOT NULL,',
      ' date_last_modified DATETIME,',
      ' voided INT,',
      ' CONSTRAINT FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
      ' CONSTRAINT unique_uuid UNIQUE (uuid),',
      ' INDEX (patient_id),',
      ' INDEX (visit_date),',
      ' INDEX (entry_point, transfer_in_date, visit_date, patient_id)',
      ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
    );
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT "Successfully created etl_hiv_enrollment table";


SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_patient_hiv_followup`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_patient_hiv_followup` (',
  'uuid CHAR(38),',
  'encounter_id INT NOT NULL PRIMARY KEY,',
  'patient_id INT NOT NULL ,',
  'location_id INT DEFAULT NULL,',
  'visit_date DATE,',
  'visit_id INT,',
  'encounter_provider INT,',
  'date_created DATETIME NOT NULL,',
  'date_last_modified DATETIME,',
  'visit_scheduled INT,',
  'person_present INT,',
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
  'z_score INT,',
  'nutritional_status INT DEFAULT NULL,',
  'population_type INT DEFAULT NULL,',
  'key_population_type INT DEFAULT NULL,',
  'who_stage INT,',
  'who_stage_associated_oi VARCHAR(1000),',
  'presenting_complaints INT DEFAULT NULL,',
  'clinical_notes VARCHAR(600) DEFAULT NULL,',
  'on_anti_tb_drugs INT DEFAULT NULL,',
  'on_ipt INT DEFAULT NULL,',
  'ever_on_ipt INT DEFAULT NULL,',
  'cough INT DEFAULT -1,',
  'fever INT DEFAULT -1,',
  'weight_loss_poor_gain INT DEFAULT -1,',
  'night_sweats INT DEFAULT -1,',
  'tb_case_contact INT DEFAULT -1,',
  'lethargy INT DEFAULT -1,',
  'screened_for_tb VARCHAR(50),',
  'spatum_smear_ordered INT DEFAULT NULL,',
  'chest_xray_ordered INT DEFAULT NULL,',
  'genexpert_ordered INT DEFAULT NULL,',
  'spatum_smear_result INT DEFAULT NULL,',
  'chest_xray_result INT DEFAULT NULL,',
  'genexpert_result INT DEFAULT NULL,',
  'referral INT DEFAULT NULL,',
  'clinical_tb_diagnosis INT DEFAULT NULL,',
  'contact_invitation INT DEFAULT NULL,',
  'evaluated_for_ipt INT DEFAULT NULL,',
  'has_known_allergies INT DEFAULT NULL,',
  'has_chronic_illnesses_cormobidities INT DEFAULT NULL,',
  'has_adverse_drug_reaction INT DEFAULT NULL,',
  'substitution_first_line_regimen_date DATE ,',
  'substitution_first_line_regimen_reason INT,',
  'substitution_second_line_regimen_date DATE,',
  'substitution_second_line_regimen_reason INT,',
  'second_line_regimen_change_date DATE,',
  'second_line_regimen_change_reason INT,',
  'pregnancy_status INT,',
  'breastfeeding INT,',
  'wants_pregnancy INT DEFAULT NULL,',
  'pregnancy_outcome INT,',
  'anc_number VARCHAR(50),',
  'expected_delivery_date DATE,',
  'ever_had_menses INT,',
  'last_menstrual_period DATE,',
  'menopausal INT,',
  'gravida INT,',
  'parity INT,',
  'full_term_pregnancies INT,',
  'abortion_miscarriages INT,',
  'family_planning_status INT,',
  'family_planning_method INT,',
  'reason_not_using_family_planning INT,',
  'tb_status INT,',
  'started_anti_TB INT,',
  'tb_rx_date DATE,',
  'tb_treatment_no VARCHAR(50),',
  'general_examination VARCHAR(255),',
  'system_examination INT,',
  'skin_findings INT,',
  'eyes_findings INT,',
  'ent_findings INT,',
  'chest_findings INT,',
  'cvs_findings INT,',
  'abdomen_findings INT,',
  'cns_findings INT,',
  'genitourinary_findings INT,',
  'prophylaxis_given VARCHAR(50),',
  'ctx_adherence INT,',
  'ctx_dispensed INT,',
  'dapsone_adherence INT,',
  'dapsone_dispensed INT,',
  'inh_dispensed INT,',
  'arv_adherence INT,',
  'poor_arv_adherence_reason INT,',
  'poor_arv_adherence_reason_other VARCHAR(200),',
  'pwp_disclosure INT,',
  'pwp_pead_disclosure INT,',
  'pwp_partner_tested INT,',
  'condom_provided INT,',
  'substance_abuse_screening INT,',
  'screened_for_sti INT,',
  'cacx_screening INT,',
  'sti_partner_notification INT,',
  'experienced_gbv INT,',
  'depression_screening INT,',
  'at_risk_population INT,',
  'system_review_finding INT,',
  'next_appointment_date DATE,',
  'refill_date DATE,',
  'appointment_consent INT,',
  'next_appointment_reason INT,',
  'stability INT,',
  'differentiated_care_group INT,',
  'differentiated_care INT,',
  'established_differentiated_care INT,',
  'insurance_type INT,',
  'other_insurance_specify VARCHAR(200),',
  'insurance_status INT,',
  'voided INT,',
  'CONSTRAINT `fk_hiv_followup_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
  'CONSTRAINT unique_uuid_followup UNIQUE(uuid),',
  'INDEX(visit_date),',
  'INDEX(patient_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_patient_hiv_followup table';


-- ------- create table etl_laboratory_extract-----------------------------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_laboratory_extract`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_laboratory_extract` (',
  'uuid CHAR(38) PRIMARY KEY,',
  'encounter_id INT,',
  'patient_id INT NOT NULL,',
  'location_id INT DEFAULT NULL,',
  'visit_date DATE,',
  'visit_id INT,',
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
  'test_requested_by INT,',
  'date_created DATETIME NOT NULL,',
  'date_last_modified DATETIME,',
  'created_by INT,',
  'CONSTRAINT `fk_laboratory_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
  'CONSTRAINT `unique_uuid_lab` UNIQUE (uuid),',
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
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_pharmacy_extract`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_pharmacy_extract` (',
  'obs_group_id INT PRIMARY KEY,',
  'uuid CHAR(38),',
  'patient_id INT NOT NULL,',
  'location_id INT DEFAULT NULL,',
  'visit_date DATE,',
  'visit_id INT,',
  'encounter_id INT,',
  'encounter_name VARCHAR(100),',
  'drug INT,',
  'is_arv INT,',
  'is_ctx INT,',
  'is_dapsone INT,',
  'drug_name VARCHAR(255),',
  'dose INT,',
  'unit INT,',
  'frequency INT,',
  'duration INT,',
  'duration_units VARCHAR(20),',
  'duration_in_days INT,',
  'prescription_provider VARCHAR(50),',
  'dispensing_provider VARCHAR(50),',
  'regimen MEDIUMTEXT,',
  'adverse_effects VARCHAR(100),',
  'date_of_refill DATE,',
  'date_created DATETIME NOT NULL,',
  'date_last_modified DATETIME,',
  'voided INT,',
  'date_voided DATE,',
  -- Removed the backticks surrounding etl_schema
  'CONSTRAINT `fk_pharmacy_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
  -- Made constraint name unique to this table
  'CONSTRAINT `unique_uuid_pharmacy` UNIQUE (uuid),',
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

-- 1. DROP statement using consistent @sql variable
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_patient_program_discontinuation`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_patient_program_discontinuation` (',
  ' uuid CHAR(38),',
  ' patient_id INT NOT NULL,',
  ' visit_id INT,',
  ' visit_date DATETIME,',
  ' location_id INT DEFAULT NULL,',
  ' program_uuid CHAR(38),',
  ' program_name VARCHAR(50),',
  ' encounter_id INT NOT NULL PRIMARY KEY,',
  ' discontinuation_reason INT,',
  ' effective_discontinuation_date DATE,',
  ' trf_out_verified INT,',
  ' trf_out_verification_date DATE,',
  ' date_died DATE,',
  ' transfer_facility VARCHAR(100),',
  ' transfer_date DATE,',
  ' death_reason INT,',
  ' specific_death_cause INT,',
  ' natural_causes VARCHAR(200) DEFAULT NULL,',
  ' non_natural_cause VARCHAR(200) DEFAULT NULL,',
  ' date_created DATETIME NOT NULL,',
  ' date_last_modified DATETIME,',
  ' CONSTRAINT `fk_ppd_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
  ' CONSTRAINT `unique_uuid_ppd` UNIQUE (uuid),',
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
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_mch_enrollment`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_mch_enrollment` (',
    'uuid CHAR(38),',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'service_type INT,',
    'anc_number VARCHAR(50),',
    'first_anc_visit_date DATE,',
    'gravida INT,',
    'parity INT,',
    'parity_abortion INT,',
    'age_at_menarche INT,',
    'lmp DATE,',
    'lmp_estimated INT,',
    'edd_ultrasound DATE,',
    'blood_group INT,',
    'serology INT,',
    'tb_screening INT,',
    'bs_for_mps INT,',
    'hiv_status INT,',
    'hiv_test_date DATE,',
    'partner_hiv_status INT,',
    'partner_hiv_test_date DATE,',
    'ti_date_started_art DATE,',
    'ti_current_regimen INT,',
    'ti_care_facility VARCHAR(100),',
    'urine_microscopy VARCHAR(100),',
    'urinary_albumin INT,',
    'glucose_measurement INT,',
    'urine_ph INT,',
    'urine_gravity INT,',
    'urine_nitrite_test INT,',
    'urine_leukocyte_esterace_test INT,',
    'urinary_ketone INT,',
    'urine_bile_salt_test INT,',
    'urine_bile_pigment_test INT,',
    'urine_colour INT,',
    'urine_turbidity INT,',
    'urine_dipstick_for_blood INT,',
    'date_of_discontinuation DATETIME,',
    'discontinuation_reason INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT `fk_mch_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_mch` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(tb_screening),',
    'INDEX(hiv_status),',
    'INDEX(hiv_test_date),',
    'INDEX(partner_hiv_status)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Successfully created etl_mch_enrollment table';


-- ------------ create table etl_mch_antenatal_visit-----------------------
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_mch_antenatal_visit`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_mch_antenatal_visit` (',
    'uuid CHAR(38),',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'provider INT,',
    'anc_visit_number INT,',
    'anc_number VARCHAR(50),',
    'parity INT,',
    'gravidae INT,',
    'lmp_date DATE,',
    'expected_delivery_date DATE,',
    'gestation_in_weeks INT,',
    'temperature DOUBLE,',
    'pulse_rate DOUBLE,',
    'systolic_bp DOUBLE,',
    'diastolic_bp DOUBLE,',
    'respiratory_rate DOUBLE,',
    'oxygen_saturation INT,',
    'weight DOUBLE,',
    'height DOUBLE,',
    'muac DOUBLE,',
    'hemoglobin DOUBLE,',
    'blood_sugar_test INT,',
    'blood_glucose INT,',
    'breast_exam_done INT,',
    'pallor INT,',
    'maturity INT,',
    'fundal_height DOUBLE,',
    'fetal_presentation INT,',
    'lie INT,',
    'fetal_heart_rate INT,',
    'fetal_movement INT,',
    'who_stage INT,',
    'cd4 INT,',
    'vl_sample_taken INT,',
    'viral_load INT,',
    'ldl INT,',
    'arv_status INT,',
    'hiv_test_during_visit INT,',
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
    'partner_hiv_tested INT,',
    'partner_hiv_status INT,',
    'prophylaxis_given INT,',
    'started_haart_at_anc INT,',
    'haart_given INT,',
    'date_given_haart DATE,',
    'baby_azt_dispensed INT,',
    'baby_nvp_dispensed INT,',
    'deworming_done_anc VARCHAR(100),',
    'IPT_dose_given_anc INT,',
    'TTT VARCHAR(50) DEFAULT NULL,',
    'IPT_malaria VARCHAR(50) DEFAULT NULL,',
    'iron_supplement VARCHAR(50) DEFAULT NULL,',
    'deworming VARCHAR(50) DEFAULT NULL,',
    'bed_nets VARCHAR(50) DEFAULT NULL,',
    'urine_microscopy VARCHAR(100),',
    'urinary_albumin INT,',
    'glucose_measurement INT,',
    'urine_ph INT,',
    'urine_gravity INT,',
    'urine_nitrite_test INT,',
    'urine_leukocyte_esterace_test INT,',
    'urinary_ketone INT,',
    'urine_bile_salt_test INT,',
    'urine_bile_pigment_test INT,',
    'urine_colour INT,',
    'urine_turbidity INT,',
    'urine_dipstick_for_blood INT,',
    'syphilis_test_status INT,',
    'syphilis_treated_status INT,',
    'bs_mps INT,',
    'diabetes_test INT,',
    'intermittent_presumptive_treatment_given INT,',
    'intermittent_presumptive_treatment_dose INT,',
    'minimum_package_of_care_given INT,',
    'minimum_package_of_care_services VARCHAR(1000),',
    'fgm_done INT,',
    'fgm_complications VARCHAR(255),',
    'fp_method_postpartum INT,',
    'anc_exercises INT,',
    'tb_screening INT,',
    'cacx_screening INT,',
    'cacx_screening_method INT,',
    'hepatitis_b_screening INT,',
    'hepatitis_b_treatment INT,',
    'has_other_illnes INT,',
    'counselled INT,',
    'counselled_on_birth_plans INT,',
    'counselled_on_danger_signs INT,',
    'counselled_on_family_planning INT,',
    'counselled_on_hiv INT,',
    'counselled_on_supplimental_feeding INT,',
    'counselled_on_breast_care INT,',
    'counselled_on_infant_feeding INT,',
    'counselled_on_treated_nets INT,',
    'minimum_care_package INT,',
    'risk_reduction INT,',
    'partner_testing INT,',
    'sti_screening INT,',
    'condom_provision INT,',
    'prep_adherence INT,',
    'anc_visits_emphasis INT,',
    'pnc_fp_counselling INT,',
    'referral_vmmc INT,',
    'referral_dreams INT,',
    'referred_from INT,',
    'referred_to INT,',
    'next_appointment_date DATE,',
    'referral_reason VARCHAR(255),',
    'clinical_notes VARCHAR(255) DEFAULT NULL,',
    'form VARCHAR(50),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT `fk_mch_antenatal_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_mch_antenatal` UNIQUE (uuid),',
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
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_mchs_delivery`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_mchs_delivery` (',
    'uuid CHAR(38),',
    'provider INT,',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'number_of_anc_visits INT,',
    'vaginal_examination INT,',
    'uterotonic_given INT,',
    'chlohexidine_applied_on_code_stump INT,',
    'vitamin_K_given INT,',
    'kangaroo_mother_care_given INT,',
    'testing_done_in_the_maternity_hiv_status INT,',
    'infant_provided_with_arv_prophylaxis INT,',
    'mother_on_haart_during_anc INT,',
    'mother_started_haart_at_maternity INT,',
    'vdrl_rpr_results INT,',
    'date_of_last_menstrual_period DATE,',
    'estimated_date_of_delivery DATE,',
    'reason_for_referral VARCHAR(100),',
    'admission_number VARCHAR(50),',
    'duration_of_pregnancy DOUBLE,',
    'mode_of_delivery INT,',
    'date_of_delivery DATETIME,',
    'blood_loss DOUBLE,',
    'condition_of_mother INT,',
    'delivery_outcome VARCHAR(255),',
    'apgar_score_1min DOUBLE,',
    'apgar_score_5min DOUBLE,',
    'apgar_score_10min DOUBLE,',
    'resuscitation_done INT,',
    'place_of_delivery INT,',
    'delivery_assistant VARCHAR(100),',
    'counseling_on_infant_feeding INT,',
    'counseling_on_exclusive_breastfeeding INT,',
    'counseling_on_infant_feeding_for_hiv_infected INT,',
    'mother_decision INT,',
    'placenta_complete INT,',
    'maternal_death_audited INT,',
    'cadre INT,',
    'delivery_complications INT,',
    'coded_delivery_complications INT,',
    'other_delivery_complications VARCHAR(100),',
    'duration_of_labor INT,',
    'baby_sex INT,',
    'baby_condition INT,',
    'teo_given INT,',
    'birth_weight INT,',
    'bf_within_one_hour INT,',
    'birth_with_deformity INT,',
    'type_of_birth_deformity INT,',
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
    'partner_hiv_tested INT,',
    'partner_hiv_status INT,',
    'prophylaxis_given INT,',
    'baby_azt_dispensed INT,',
    'baby_nvp_dispensed INT,',
    'clinical_notes VARCHAR(200) DEFAULT NULL,',
    'stimulation_done INT,',
    'suction_done INT,',
    'oxygen_given INT,',
    'bag_mask_ventilation_provided INT,',
    'induction_done INT,',
    'artificial_rapture_done INT,',
    'CONSTRAINT `fk_mchs_delivery_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_mchs_delivery` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(final_test_result),',
    'INDEX(baby_sex)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT "Successfully created etl_mchs_delivery table";

-- ------------ create table etl_mchs_discharge-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_mchs_discharge`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_mchs_discharge` (',
    'uuid CHAR(38),',
    'provider INT,',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'counselled_on_feeding INT,',
    'baby_status INT,',
    'vitamin_A_dispensed INT,',
    'birth_notification_number VARCHAR(100),',
    'condition_of_mother VARCHAR(100),',
    'discharge_date DATE,',
    'referred_from INT,',
    'referred_to INT,',
    'clinical_notes VARCHAR(200) DEFAULT NULL,',
    'CONSTRAINT `fk_mchs_discharge_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_mchs_discharge` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(baby_status),',
    'INDEX(discharge_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT "Successfully created etl_mchs_discharge table";

-- ------------ create table etl_mch_postnatal_visit-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_mch_postnatal_visit`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_mch_postnatal_visit` (',
    'uuid CHAR(38),',
    'patient_id INT NOT NULL ,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'provider INT,',
    'pnc_register_no VARCHAR(50),',
    'pnc_visit_no INT,',
    'delivery_date DATE,',
    'mode_of_delivery INT,',
    'place_of_delivery INT,',
    'visit_timing_mother INT,',
    'visit_timing_baby INT,',
    'delivery_outcome INT,',
    'temperature DOUBLE,',
    'pulse_rate DOUBLE,',
    'systolic_bp DOUBLE,',
    'diastolic_bp DOUBLE,',
    'respiratory_rate DOUBLE,',
    'oxygen_saturation INT,',
    'weight DOUBLE,',
    'height DOUBLE,',
    'muac DOUBLE,',
    'hemoglobin DOUBLE,',
    'arv_status INT,',
    'general_condition INT,',
    'breast INT,',
    'cs_scar INT,',
    'gravid_uterus INT,',
    'episiotomy INT,',
    'lochia INT,',
    'counselled_on_infant_feeding INT,',
    'pallor INT,',
    'pallor_severity INT,',
    'pph INT,',
    'mother_hiv_status INT,',
    'condition_of_baby INT,',
    'baby_feeding_method INT,',
    'umblical_cord INT,',
    'baby_immunization_started INT,',
    'family_planning_counseling INT,',
    'other_maternal_complications VARCHAR(255),',
    'uterus_examination INT,',
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
    'syphilis_results INT DEFAULT NULL,',
    'patient_given_result VARCHAR(50) DEFAULT NULL,',
    'couple_counselled INT,',
    'partner_hiv_tested INT,',
    'partner_hiv_status INT,',
    'pnc_hiv_test_timing_mother INT,',
    'mother_haart_given INT,',
    'prophylaxis_given INT,',
    'infant_prophylaxis_timing INT,',
    'baby_azt_dispensed INT,',
    'baby_nvp_dispensed INT,',
    'pnc_exercises INT,',
    'maternal_condition INT,',
    'iron_supplementation INT,',
    'fistula_screening INT,',
    'cacx_screening INT,',
    'cacx_screening_method INT,',
    'family_planning_status INT,',
    'family_planning_method VARCHAR(1000),',
    'referred_from INT,',
    'referred_to INT,',
    'referral_reason VARCHAR(255) DEFAULT NULL,',
    'clinical_notes VARCHAR(200) DEFAULT NULL,',
    'appointment_date DATE DEFAULT NULL,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT `fk_mch_postnatal_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_mch_postnatal` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT "Successfully created etl_mch_postnatal_visit table";

-- ------------ create table etl_hei_enrollment-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_hei_enrollment`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_hei_enrollment` (',
    'serial_no INT NOT NULL AUTO_INCREMENT,',
    'uuid CHAR(38),',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'provider INT,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'child_exposed INT,',
    'hei_id_number VARCHAR(50),',
    'spd_number VARCHAR(50),',
    'birth_weight DOUBLE,',
    'gestation_at_birth DOUBLE,',
    'birth_type VARCHAR(50),',
    'date_first_seen DATE,',
    'birth_notification_number VARCHAR(50),',
    'birth_certificate_number VARCHAR(50),',
    'need_for_special_care INT,',
    'reason_for_special_care INT,',
    'referral_source INT,',
    'transfer_in INT,',
    'transfer_in_date DATE,',
    'facility_transferred_from VARCHAR(50),',
    'district_transferred_from VARCHAR(50),',
    'date_first_enrolled_in_hei_care DATE,',
    'arv_prophylaxis INT,',
    'mother_breastfeeding INT,',
    'mother_on_NVP_during_breastfeeding INT,',
    'TB_contact_history_in_household INT,',
    'infant_mother_link INT,',
    'mother_alive INT,',
    'mother_on_pmtct_drugs INT,',
    'mother_on_drug INT,',
    'mother_on_art_at_infant_enrollment INT,',
    'mother_drug_regimen INT,',
    'infant_prophylaxis INT,',
    'parent_ccc_number VARCHAR(50),',
    'mode_of_delivery INT,',
    'place_of_delivery INT,',
    'birth_length INT,',
    'birth_order INT,',
    'health_facility_name VARCHAR(50),',
    'date_of_birth_notification DATE,',
    'date_of_birth_registration DATE,',
    'birth_registration_place VARCHAR(50),',
    'permanent_registration_serial VARCHAR(50),',
    'mother_facility_registered VARCHAR(50),',
    'exit_date DATE,',
    'exit_reason INT,',
    'hiv_status_at_exit VARCHAR(50),',
    'encounter_type VARCHAR(250),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT `fk_hei_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_hei` UNIQUE (uuid),',
    'KEY `serial_no_key` (serial_no),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(transfer_in),',
    'INDEX(child_exposed)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_hei_enrollment table';


-- ------------ create table etl_hei_follow_up_visit-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_hei_follow_up_visit`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_hei_follow_up_visit` (',
    'uuid CHAR(38),',
    'provider INT,',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'weight DOUBLE,',
    'height DOUBLE,',
    'muac INT,',
    'primary_caregiver INT,',
    'revisit_this_year INT,',
    'height_length INT,',
    'referred INT,',
    'referral_reason VARCHAR(255),',
    'danger_signs INT,',
    'infant_feeding INT,',
    'stunted INT,',
    'tb_assessment_outcome INT,',
    'social_smile_milestone INT,',
    'head_control_milestone INT,',
    'response_to_sound_milestone INT,',
    'hand_extension_milestone INT,',
    'sitting_milestone INT,',
    'walking_milestone INT,',
    'standing_milestone INT,',
    'talking_milestone INT,',
    'review_of_systems_developmental INT,',
    'weight_category INT,',
    'followup_type INT,',
    'dna_pcr_sample_date DATE,',
    'dna_pcr_contextual_status INT,',
    'dna_pcr_result INT,',
    'dna_pcr_dbs_sample_code VARCHAR(100),',
    'dna_pcr_results_date DATE,',
    'azt_given INT,',
    'nvp_given INT,',
    'ctx_given INT,',
    'multi_vitamin_given INT,',
    'first_antibody_sample_date DATE,',
    'first_antibody_result INT,',
    'first_antibody_dbs_sample_code VARCHAR(100),',
    'first_antibody_result_date DATE,',
    'final_antibody_sample_date DATE,',
    'final_antibody_result INT,',
    'final_antibody_dbs_sample_code VARCHAR(100),',
    'final_antibody_result_date DATE,',
    'tetracycline_ointment_given INT,',
    'pupil_examination INT,',
    'sight_examination INT,',
    'squint INT,',
    'deworming_drug INT,',
    'dosage INT,',
    'unit VARCHAR(100),',
    'vitaminA_given INT,',
    'disability INT,',
    'next_appointment_date DATE,',
    'comments VARCHAR(100),',
    'referred_from INT,',
    'referred_to INT,',
    'counselled_on INT,',
    'MNPS_Supplementation INT,',
    'LLIN INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT `fk_hei_followup_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_hei_followup` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(infant_feeding)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_hei_follow_up_visit table';


-- ------- create table etl_hei_immunization table-----------------------------------------
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_immunization`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_immunization` (',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'patient_id INT NOT NULL,',
    'visit_date DATE,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'created_by INT,',
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
    'fully_immunized INT,',
    'CONSTRAINT `fk_immunization_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'INDEX(visit_date),',
    'INDEX(patient_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Successfully created etl_immunization table';

-- ------------ create table etl_tb_enrollment-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_tb_enrollment`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_tb_enrollment` (',
    'uuid CHAR(38),',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'provider INT,',
    'date_treatment_started DATE,',
    'district VARCHAR(50),',
    'district_registration_number VARCHAR(20),',
    'referred_by INT,',
    'referral_date DATE,',
    'date_transferred_in DATE,',
    'facility_transferred_from VARCHAR(100),',
    'district_transferred_from VARCHAR(100),',
    'date_first_enrolled_in_tb_care DATE,',
    'weight DOUBLE,',
    'height DOUBLE,',
    'treatment_supporter VARCHAR(100),',
    'relation_to_patient INT,',
    'treatment_supporter_address VARCHAR(100),',
    'treatment_supporter_phone_contact VARCHAR(100),',
    'disease_classification INT,',
    'patient_classification INT,',
    'pulmonary_smear_result INT,',
    'has_extra_pulmonary_pleurial_effusion INT,',
    'has_extra_pulmonary_milliary INT,',
    'has_extra_pulmonary_lymph_node INT,',
    'has_extra_pulmonary_menengitis INT,',
    'has_extra_pulmonary_skeleton INT,',
    'has_extra_pulmonary_abdominal INT,',
    'has_extra_pulmonary_other VARCHAR(100),',
    'treatment_outcome INT,',
    'treatment_outcome_date DATE,',
    'date_of_discontinuation DATETIME,',
    'discontinuation_reason INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT `fk_tb_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_tb_enrollment` UNIQUE (uuid),',
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
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_tb_follow_up_visit`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_tb_follow_up_visit` (',
    'uuid CHAR(38),',
    'provider INT,',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'spatum_test INT,',
    'spatum_result INT,',
    'result_serial_number VARCHAR(20),',
    'quantity DOUBLE,',
    'date_test_done DATE,',
    'bacterial_colonie_growth INT,',
    'number_of_colonies DOUBLE,',
    'resistant_s INT,',
    'resistant_r INT,',
    'resistant_inh INT,',
    'resistant_e INT,',
    'sensitive_s INT,',
    'sensitive_r INT,',
    'sensitive_inh INT,',
    'sensitive_e INT,',
    'test_date DATE,',
    'hiv_status INT,',
    'next_appointment_date DATE,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT `fk_tb_followup_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_tb_followup` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(hiv_status)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_tb_follow_up_visit table';

-- ------------ create table etl_tb_screening-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_tb_screening`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_tb_screening` (',
    'uuid CHAR(38),',
    'provider INT,',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'cough_for_2wks_or_more INT,',
    'confirmed_tb_contact INT,',
    'fever_for_2wks_or_more INT,',
    'noticeable_weight_loss INT,',
    'night_sweat_for_2wks_or_more INT,',
    'lethargy INT,',
    'spatum_smear_ordered INT DEFAULT NULL,',
    'chest_xray_ordered INT DEFAULT NULL,',
    'genexpert_ordered INT DEFAULT NULL,',
    'spatum_smear_result INT DEFAULT NULL,',
    'chest_xray_result INT DEFAULT NULL,',
    'genexpert_result INT DEFAULT NULL,',
    'referral INT DEFAULT NULL,',
    'clinical_tb_diagnosis INT DEFAULT NULL,',
    'resulting_tb_status INT,',
    'contact_invitation INT DEFAULT NULL,',
    'evaluated_for_ipt INT DEFAULT NULL,',
    'started_anti_TB INT,',
    'tb_treatment_start_date DATE DEFAULT NULL,',
    'tb_prophylaxis VARCHAR(50),',
    'notes VARCHAR(100),',
    'person_present INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT `fk_tb_screening_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_tb_screening` UNIQUE (uuid),',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_patients_booked_today`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_patients_booked_today` (',
    'id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,',
    'patient_id INT NOT NULL,',
    'last_visit_date DATE,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT `fk_patients_booked_today_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'INDEX(patient_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_patients_booked_today table';

-- ------------ create table etl_missed_appointments-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_missed_appointments`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_missed_appointments` (',
    'id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,',
    'patient_id INT NOT NULL,',
    'last_tca_date DATE,',
    'last_visit_date DATE,',
    'last_encounter_type VARCHAR(100),',
    'days_since_last_visit INT,',
    'date_table_created DATE,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT `fk_missed_appointments_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'INDEX(patient_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_missed_appointments table';


-- --------------------------- CREATE drug_event table ---------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_drug_event`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_drug_event` (',
    'uuid CHAR(38),',
    'patient_id INT NOT NULL,',
    'date_started DATE,',
    'visit_date DATE,',
    'provider INT,',
    'encounter_id INT NOT NULL,',
    'program VARCHAR(50),',
    'regimen MEDIUMTEXT,',
    'regimen_name VARCHAR(100),',
    'regimen_line VARCHAR(50),',
    'discontinued INT,',
    'regimen_discontinued VARCHAR(255),',
    'regimen_stopped INT,',
    'date_discontinued DATE,',
    'reason_discontinued INT,',
    'reason_discontinued_other VARCHAR(100),',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'voided INT,',
    'CONSTRAINT `fk_drug_event_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_drug_event` UNIQUE (uuid),',
    'INDEX(patient_id),',
    'INDEX(date_started),',
    'INDEX(date_discontinued),',
    'INDEX(patient_id, date_started)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_drug_event table';

-- -------------------------- CREATE hts_test table ---------------------------------
-- 1. Use @sql for the DROP to maintain session consistency
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_hts_test`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_hts_test` (',
    'patient_id INT NOT NULL,',
    'visit_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'encounter_uuid CHAR(38) NOT NULL,',
    'encounter_location INT NOT NULL,',
    'creator INT NOT NULL,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'visit_date DATE,',
    'test_type INT DEFAULT NULL,',
    'population_type VARCHAR(50),',
    'key_population_type VARCHAR(50),',
    'priority_population_type VARCHAR(50),',
    'ever_tested_for_hiv VARCHAR(10),',
    'months_since_last_test INT,',
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
    'referred INT DEFAULT NULL,',
    'referral_for VARCHAR(100) DEFAULT NULL,',
    'referral_facility VARCHAR(200) DEFAULT NULL,',
    'other_referral_facility VARCHAR(200) DEFAULT NULL,',
    'neg_referral_for VARCHAR(500) DEFAULT NULL,',
    'neg_referral_specify VARCHAR(500) DEFAULT NULL,',
    'tb_screening VARCHAR(20) DEFAULT NULL,',
    'patient_had_hiv_self_test VARCHAR(50) DEFAULT NULL,',
    'remarks VARCHAR(255) DEFAULT NULL,',
    'voided INT,',
    'CONSTRAINT `fk_hts_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
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
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_hts_referral_and_linkage`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_hts_referral_and_linkage` (',
    'patient_id INT NOT NULL,',
    'visit_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'encounter_uuid CHAR(38) NOT NULL,',
    'encounter_location INT NOT NULL,',
    'creator INT NOT NULL,',
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
    'voided INT,',
    'CONSTRAINT `fk_hts_referral_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_hts_referral`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_hts_referral` (',
    'patient_id INT NOT NULL,',
    'visit_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'encounter_uuid CHAR(38) NOT NULL,',
    'encounter_location INT NOT NULL,',
    'creator INT NOT NULL,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'visit_date DATE,',
    'facility_referred_to VARCHAR(200) DEFAULT NULL,',
    'date_to_enrol DATE DEFAULT NULL,',
    'remarks VARCHAR(255) DEFAULT NULL,',
    'voided INT,',
    'CONSTRAINT `fk_hts_referral_demographics` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'INDEX(patient_id),',
    'INDEX(visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_hts_referral table';


-- ------------ create table etl_ipt_screening-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_ipt_screening`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_ipt_screening` (',
    'uuid CHAR(38),',
    'provider INT,',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT,',
    'obs_id INT NOT NULL PRIMARY KEY,',
    'cough INT DEFAULT NULL,',
    'fever INT DEFAULT NULL,',
    'weight_loss_poor_gain INT DEFAULT NULL,',
    'night_sweats INT DEFAULT NULL,',
    'contact_with_tb_case INT DEFAULT NULL,',
    'lethargy INT DEFAULT NULL,',
    'yellow_urine INT,',
    'numbness_bs_hands_feet INT,',
    'eyes_yellowness INT,',
    'upper_rightQ_abdomen_tenderness INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'voided INT,',
    'CONSTRAINT `fk_ipt_screening_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'INDEX(visit_date),',
    'INDEX(patient_id),',
    'INDEX(encounter_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_ipt_screening table';


-- ------------ create table etl_ipt_follow_up -----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_ipt_follow_up`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_ipt_follow_up` (',
    'uuid CHAR(38),',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'provider INT,',
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
    'voided INT,',
    'CONSTRAINT `fk_ipt_followup_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_ipt_followup` UNIQUE (uuid),',
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



SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_ccc_defaulter_tracing`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_ccc_defaulter_tracing` (',
    'uuid CHAR(38),',
    'provider INT,',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'tracing_type INT,',
    'missed_appointment_date DATE,',
    'reason_for_missed_appointment INT,',
    'non_coded_missed_appointment_reason VARCHAR(100),',
    'tracing_outcome INT,',
    'reason_not_contacted INT,',
    'attempt_number INT,',
    'is_final_trace INT,',
    'true_status INT,',
    'cause_of_death INT,',
    'comments VARCHAR(100),',
    'booking_date DATE,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'CONSTRAINT `fk_ccc_defaulter_tracing_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_ccc_defaulter` UNIQUE (uuid),',
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
-- 1. Standardized DROP using @sql
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_ART_preparation`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 2. CREATE statement with named constraints and fixed backticks
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_ART_preparation` (',
    'uuid CHAR(38),',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'provider INT,',
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
    'CONSTRAINT `fk_art_preparation_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_art_prep` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(ready_to_start_art)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_ART_preparation table';

-- ------------ create table etl_enhanced_adherence-----------------------
-- 1. Standardized DROP using @sql
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_enhanced_adherence`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 2. CREATE statement with named constraints and fixed backticks
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_enhanced_adherence` (',
    'uuid CHAR(38),',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'provider INT,',
    'session_number INT,',
    'first_session_date DATE,',
    'pill_count INT,',
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
    'CONSTRAINT `fk_enhanced_adherence_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_enhanced_adherence` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT "Successfully created etl_enhanced_adherence table";


-- ------------ create table etl_patient_triage-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_patient_triage`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_patient_triage` (',
    'uuid CHAR(38),',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'patient_id INT NOT NULL,',
    'location_id INT DEFAULT NULL,',
    'visit_date DATE,',
    'visit_id INT,',
    'encounter_provider INT,',
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
    'temperature_collection_mode INT,',
    'pulse_rate DOUBLE,',
    'respiratory_rate DOUBLE,',
    'oxygen_saturation DOUBLE,',
    'oxygen_saturation_collection_mode INT,',
    'muac DOUBLE,',
    'z_score_absolute DOUBLE DEFAULT NULL,',
    'z_score INT,',
    'nutritional_status INT DEFAULT NULL,',
    'nutritional_intervention INT DEFAULT NULL,',
    'last_menstrual_period DATE,',
    'hpv_vaccinated INT,',
    'voided INT,',
    'CONSTRAINT `fk_patient_triage_demographics` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_patient_triage` UNIQUE(uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_patient_triage table';

-- ------------ create table etl_generalized_anxiety_disorder-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_generalized_anxiety_disorder`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_generalized_anxiety_disorder` (',
    'uuid CHAR(38),',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'patient_id INT NOT NULL,',
    'location_id INT DEFAULT NULL,',
    'visit_date DATE,',
    'visit_id INT,',
    'encounter_provider INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'feeling_nervous_anxious INT,',
    'control_worrying INT,',
    'worrying_much INT,',
    'trouble_relaxing INT,',
    'being_restless INT,',
    'feeling_bad INT,',
    'feeling_afraid INT,',
    'assessment_outcome INT,',
    'voided INT,',
    'CONSTRAINT `fk_gad_patient_demographics` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_gad` UNIQUE (uuid),',
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
-- 1. Standardized DROP using @sql
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_prep_behaviour_risk_assessment`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 2. CREATE statement with named constraints and fixed backticks
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_prep_behaviour_risk_assessment` (',
    'uuid CHAR(38),',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'provider INT,',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
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
    'HIV_serodiscordant_duration_months INT,',
    'recent_unprotected_sex_with_positive_partner VARCHAR(10),',
    'children_with_hiv_positive_partner VARCHAR(255),',
    'voided INT,',
    'CONSTRAINT `fk_prep_risk_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_prep_risk` UNIQUE (uuid),',
    'INDEX(patient_id),',
    'INDEX(visit_date),',
    'INDEX(encounter_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Successfully created etl_prep_behaviour_risk_assessment table';


-- ------------ create table etl_prep_monthly_refill-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_prep_monthly_refill`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_prep_monthly_refill` (',
    'uuid CHAR(38),',
    'provider INT,',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
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
    'number_of_condoms_issued INT,',
    'prep_discontinue_reasons VARCHAR(255),',
    'prep_discontinue_other_reasons VARCHAR(255),',
    'appointment_given VARCHAR(10),',
    'next_appointment DATE,',
    'remarks VARCHAR(255),',
    'voided INT,',
    'CONSTRAINT `fk_prep_refill_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_prep_refill` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(next_appointment)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_prep_monthly_refill table';


-- ------------ create table etl_prep_discontinuation-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_prep_discontinuation`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_prep_discontinuation` (',
    'uuid CHAR(38),',
    'provider INT,',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'discontinue_reason VARCHAR(255),',
    'care_end_date DATE,',
    'last_prep_dose_date DATE,',
    'voided INT,',
    'CONSTRAINT `fk_prep_discontinuation_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_prep_discontinuation` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(discontinue_reason),',
    'INDEX(care_end_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_prep_discontinuation table';

-- ------------ create table etl_prep_enrollment-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_prep_enrollment`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_prep_enrollment` (',
    'uuid CHAR(38),',
    'provider INT,',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
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
    'voided INT,',
    'CONSTRAINT `fk_prep_enrollment_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_prep_enrollment` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_prep_enrollment table';

-- ------------ create table etl_prep_followup-----------------------
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_prep_followup`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_prep_followup` (',
    'uuid CHAR(38),',
    'form VARCHAR(50),',
    'provider INT,',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
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
    'reason_for_starting_prep INT,',
    'switching_option VARCHAR(255),',
    'switching_date DATE,',
    'prep_type VARCHAR(10),',
    'prescribed_PrEP VARCHAR(10),',
    'regimen_prescribed VARCHAR(255),',
    'months_prescribed_regimen INT,',
    'condoms_issued VARCHAR(10),',
    'number_of_condoms VARCHAR(10),',
    'appointment_given VARCHAR(10),',
    'appointment_date DATE,',
    'reason_no_appointment VARCHAR(255),',
    'clinical_notes VARCHAR(255),',
    'voided INT,',
    'CONSTRAINT `fk_prep_followup_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_prep_followup` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(form)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_prep_followup table';

-- ------------ create table etl_progress_note-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_progress_note`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_progress_note` (',
    'uuid CHAR(38),',
    'provider INT,',
    'patient_id INT NOT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'location_id INT DEFAULT NULL,',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'notes VARCHAR(255),',
    'voided INT,',
    'CONSTRAINT `fk_progress_note_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_progress_note` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_progress_note table';

-- ------------ create table etl_ipt_initiation -----------------------
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_ipt_initiation`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_ipt_initiation` (',
    'uuid CHAR(38),',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'patient_id INT NOT NULL,',
    'location_id INT DEFAULT NULL,',
    'visit_date DATE,',
    'encounter_provider INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'ipt_indication INT,',
    'sub_county_reg_number VARCHAR(255),',
    'sub_county_reg_date DATE,',
    'voided INT,',
    'CONSTRAINT `fk_ipt_initiation_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_ipt_initiation` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(encounter_id),',
    'INDEX(patient_id),',
    'INDEX(patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_ipt_initiation table';

-- --------------------- creating ipt outcome table -------------------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_ipt_outcome`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_ipt_outcome` (',
    'uuid CHAR(38),',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'patient_id INT NOT NULL,',
    'location_id INT DEFAULT NULL,',
    'visit_date DATE,',
    'encounter_provider INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'outcome INT,',
    'voided INT,',
    'CONSTRAINT `fk_ipt_outcome_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_ipt_outcome` UNIQUE (uuid),',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_hts_linkage_tracing`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_hts_linkage_tracing` (',
    'uuid CHAR(38),',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'patient_id INT NOT NULL,',
    'location_id INT DEFAULT NULL,',
    'visit_date DATE,',
    'encounter_provider INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'tracing_type INT,',
    'tracing_outcome INT,',
    'reason_not_contacted INT,',
    'voided INT,',
    'CONSTRAINT `fk_hts_linkage_tracing_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'CONSTRAINT `unique_uuid_hts_linkage_tracing` UNIQUE (uuid),',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_patient_program`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_patient_program` (',
    'uuid CHAR(38) NOT NULL PRIMARY KEY,',
    'patient_id INT NOT NULL,',
    'location_id INT DEFAULT NULL,',
    'program VARCHAR(100) NOT NULL,',
    'date_enrolled DATE NOT NULL,',
    'date_completed DATE DEFAULT NULL,',
    'outcome INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'voided INT,',
    'CONSTRAINT `fk_patient_program_demographics` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id),',
    'INDEX(date_enrolled),',
    'INDEX(date_completed),',
    'INDEX(patient_id),',
    'INDEX(outcome)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_patient_program table';

-- ------------------------ create person address table ---------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_person_address`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_person_address` (',
    'uuid CHAR(38) NOT NULL,',
    'patient_id INT NOT NULL,',
    'county VARCHAR(100) DEFAULT NULL,',
    'sub_county VARCHAR(100) DEFAULT NULL,',
    'location VARCHAR(100) DEFAULT NULL,',
    'ward VARCHAR(100) DEFAULT NULL,',
    'sub_location VARCHAR(100) DEFAULT NULL,',
    'village VARCHAR(100) DEFAULT NULL,',
    'postal_address VARCHAR(100) DEFAULT NULL,',
    'land_mark VARCHAR(100) DEFAULT NULL,',
    'voided INT DEFAULT 0,',
    'PRIMARY KEY (uuid),',
    'CONSTRAINT `fk_person_address_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id) ON DELETE RESTRICT ON UPDATE CASCADE,',
    'CONSTRAINT `unique_uuid_person_address` UNIQUE (uuid),',
    'INDEX (patient_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_person_address table';

-- --------------------- creating OTZ activity table -------------------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_otz_activity`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_otz_activity` (',
    'uuid CHAR(38),',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'patient_id INT NOT NULL,',
    'location_id INT DEFAULT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'encounter_provider INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'orientation VARCHAR(11) DEFAULT NULL,',
    'leadership VARCHAR(11) DEFAULT NULL,',
    'participation VARCHAR(11) DEFAULT NULL,',
    'treatment_literacy VARCHAR(11) DEFAULT NULL,',
    'transition_to_adult_care VARCHAR(11) DEFAULT NULL,',
    'making_decision_future VARCHAR(11) DEFAULT NULL,',
    'srh VARCHAR(11) DEFAULT NULL,',
    'beyond_third_ninety VARCHAR(11) DEFAULT NULL,',
    'attended_support_group VARCHAR(11) DEFAULT NULL,',
    'remarks VARCHAR(255) DEFAULT NULL,',
    'voided INT DEFAULT 0,',
    'CONSTRAINT `fk_otz_activity_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id) ON DELETE RESTRICT ON UPDATE CASCADE,',
    'CONSTRAINT `unique_uuid_otz_activity` UNIQUE (uuid),',
    'INDEX (visit_date),',
    'INDEX (encounter_id),',
    'INDEX (patient_id),',
    'INDEX (patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_otz_activity table';


-- --------------------- creating OTZ enrollment table -------------------------------
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_otz_enrollment`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_otz_enrollment` (',
    'uuid CHAR(38),',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'patient_id INT NOT NULL,',
    'location_id INT DEFAULT NULL,',
    'visit_date DATE,',
    'encounter_provider INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'orientation VARCHAR(11) DEFAULT NULL,',
    'leadership VARCHAR(11) DEFAULT NULL,',
    'participation VARCHAR(11) DEFAULT NULL,',
    'treatment_literacy VARCHAR(11) DEFAULT NULL,',
    'transition_to_adult_care VARCHAR(11) DEFAULT NULL,',
    'making_decision_future VARCHAR(11) DEFAULT NULL,',
    'srh VARCHAR(11) DEFAULT NULL,',
    'beyond_third_ninety VARCHAR(11) DEFAULT NULL,',
    'transfer_in VARCHAR(11) DEFAULT NULL,',
    'voided INT DEFAULT 0,',
    'CONSTRAINT `fk_otz_enrollment_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id) ON DELETE RESTRICT ON UPDATE CASCADE,',
    'CONSTRAINT `unique_uuid_otz_enrollment` UNIQUE (uuid),',
    'INDEX (visit_date),',
    'INDEX (encounter_id),',
    'INDEX (patient_id),',
    'INDEX (patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Successfully created etl_otz_enrollment table';

-- --------------------- creating OVC enrollment table -------------------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_ovc_enrolment`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_ovc_enrolment` (',
    'uuid CHAR(38),',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'patient_id INT NOT NULL,',
    'location_id INT DEFAULT NULL,',
    'visit_id INT,',
    'visit_date DATE,',
    'encounter_provider INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'caregiver_enrolled_here VARCHAR(11) DEFAULT NULL,',
    'caregiver_name VARCHAR(255) DEFAULT NULL,',
    'caregiver_gender VARCHAR(255) DEFAULT NULL,',
    'relationship_to_client VARCHAR(255) DEFAULT NULL,',
    'caregiver_phone_number VARCHAR(255) DEFAULT NULL,',
    'client_enrolled_cpims VARCHAR(11) DEFAULT NULL,',
    'partner_offering_ovc VARCHAR(255) DEFAULT NULL,',
    'ovc_comprehensive_program VARCHAR(255) DEFAULT NULL,',
    'dreams_program VARCHAR(255) DEFAULT NULL,',
    'ovc_preventive_program VARCHAR(255) DEFAULT NULL,',
    'voided INT DEFAULT 0,',
    'CONSTRAINT `fk_ovc_enrolment_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id) ON DELETE RESTRICT ON UPDATE CASCADE,',
    'CONSTRAINT `unique_uuid_ovc_enrolment` UNIQUE (uuid),',
    'INDEX (visit_date),',
    'INDEX (encounter_id),',
    'INDEX (patient_id),',
    'INDEX (patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_ovc_enrolment table';


-- --------------------- creating Cervical cancer screening table -------------------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_cervical_cancer_screening`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_cervical_cancer_screening` (',
  ' uuid CHAR(38),',
  ' encounter_id INT NOT NULL PRIMARY KEY,',
  ' encounter_provider INT,',
  ' patient_id INT NOT NULL,',
  ' visit_id INT DEFAULT NULL,',
  ' visit_date DATE,',
  ' location_id INT DEFAULT NULL,',
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
  ' voided INT DEFAULT 0,',
  ' CONSTRAINT `fk_cancer_screening_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id) ON DELETE RESTRICT ON UPDATE CASCADE,',
  ' CONSTRAINT `unique_uuid_cancer_screening` UNIQUE (uuid),',
  ' INDEX (visit_date),',
  ' INDEX (encounter_id),',
  ' INDEX (patient_id),',
  ' INDEX (patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_cervical_cancer_screening table';

-- --------------------- creating patient contact  table -------------------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_patient_contact`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_patient_contact` (',
  '  `encounter_id` INT NOT NULL PRIMARY KEY,',
  '  `patient_id` INT NOT NULL,',
  '  `patient_related_to` INT DEFAULT NULL,',
  '  `relationship_type` INT DEFAULT NULL,',
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
  '  `encounter_provider` INT DEFAULT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `location_id` INT DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  `uuid` CHAR(38) DEFAULT NULL,',
  '  CONSTRAINT `fk_patient_contact_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `fk_patient_contact_related` FOREIGN KEY (`patient_related_to`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_patient_contact` UNIQUE (`uuid`),',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_client_trace`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_client_trace` (',
  '  `id` INT NOT NULL AUTO_INCREMENT,',
  '  `uuid` CHAR(38) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `encounter_date` DATETIME DEFAULT NULL,',
  '  `client_id` INT DEFAULT NULL,',
  '  `contact_type` VARCHAR(255) DEFAULT NULL,',
  '  `status` VARCHAR(255) DEFAULT NULL,',
  '  `unique_patient_no` VARCHAR(255) DEFAULT NULL,',
  '  `facility_linked_to` VARCHAR(255) DEFAULT NULL,',
  '  `health_worker_handed_to` VARCHAR(255) DEFAULT NULL,',
  '  `remarks` VARCHAR(255) DEFAULT NULL,',
  '  `appointment_date` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`id`),',
  '  CONSTRAINT `fk_client_trace_patient` FOREIGN KEY (`client_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_client_trace` UNIQUE (`uuid`),',
  '  INDEX (`date_created`),',
  '  INDEX (`client_id`),',
  '  INDEX (`id`, `date_created`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_client_trace table';

-- --------------------- creating Viral Load table -------------------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_viral_load`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_viral_load` (',
  '  `uuid` CHAR(38),',
  '  `encounter_id` INT NOT NULL PRIMARY KEY,',
  '  `patient_id` INT NOT NULL,',
  '  `location_id` INT DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `order_date` DATE,',
  '  `date_of_result` DATE,',
  '  `order_reason` VARCHAR(255) DEFAULT NULL,',
  '  `previous_vl_result` VARCHAR(50) DEFAULT NULL,',
  '  `current_vl_result` VARCHAR(50) DEFAULT NULL,',
  '  `previous_vl_date` DATE,',
  '  `previous_vl_reason` VARCHAR(255) DEFAULT NULL,',
  '  `vl_months_since_hiv_enrollment` INT DEFAULT NULL,',
  '  `vl_months_since_otz_enrollment` INT DEFAULT NULL,',
  '  `eligibility` VARCHAR(50) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `voided` INT,',
  '  CONSTRAINT `fk_etl_viral_load_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_viral_load` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`patient_id`, `visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_viral_load table';


-- create table etl_contact

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_contact`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_contact` (',
  '  `uuid` CHAR(38),',
  '  `unique_identifier` VARCHAR(50),',
  '  `client_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL PRIMARY KEY,',
  '  `encounter_provider` INT,',
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
  '  `avg_weekly_sex_acts` INT,',
  '  `avg_weekly_anal_sex_acts` INT,',
  '  `avg_daily_drug_injections` INT,',
  '  `contact_person_name` VARCHAR(255),',
  '  `contact_person_alias` VARCHAR(255),',
  '  `contact_person_phone` VARCHAR(255),',
  '  `voided` INT DEFAULT 0,',
  '  CONSTRAINT `fk_etl_contact_patient` FOREIGN KEY (`client_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_kp_contact` UNIQUE (`uuid`),',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_contact`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_contact` (',
  '  `uuid` CHAR(38),',
  '  `unique_identifier` VARCHAR(50),',
  '  `client_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL PRIMARY KEY,',
  '  `encounter_provider` INT,',
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
  '  `avg_weekly_sex_acts` INT,',
  '  `avg_weekly_anal_sex_acts` INT,',
  '  `avg_daily_drug_injections` INT,',
  '  `contact_person_name` VARCHAR(255),',
  '  `contact_person_alias` VARCHAR(255),',
  '  `contact_person_phone` VARCHAR(255),',
  '  `voided` INT,',
  '  CONSTRAINT `fk_etl_contact_patient` FOREIGN KEY (`client_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics`(`patient_id`),',
  '  CONSTRAINT `unique_uuid_kp_contact` UNIQUE (`uuid`),',
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


-- create table etl_kp_clinical_visit

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_clinical_visit`;');
PREPARE stmt FROM @drop_etl_clinical; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_clinical_visit` (',
  '  `uuid` CHAR(38),',
  '  `client_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `encounter_provider` INT,',
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
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_clinical_patient` FOREIGN KEY (`client_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_clinical_visit` UNIQUE (`uuid`),',
  '  INDEX `idx_client_id` (`client_id`),',
  '  INDEX `idx_client_visit_date` (`client_id`, `visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_clinical_visit table';

-- ------------ create table etl_kp_peer_calendar-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_peer_calendar`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_peer_calendar` (',
    'uuid CHAR(38),',
    'encounter_id INT NOT NULL PRIMARY KEY,',
    'client_id INT NOT NULL,',
    'location_id INT DEFAULT NULL,',
    'visit_date DATE,',
    'visit_id INT,',
    'encounter_provider INT,',
    'date_created DATETIME NOT NULL,',
    'date_last_modified DATETIME,',
    'hotspot_name VARCHAR(255),',
    'typology VARCHAR(255),',
    'other_hotspots VARCHAR(255),',
    'weekly_sex_acts INT,',
    'monthly_condoms_required INT,',
    'weekly_anal_sex_acts INT,',
    'monthly_lubes_required INT,',
    'daily_injections INT,',
    'monthly_syringes_required INT,',
    'years_in_sexwork_drugs INT,',
    'experienced_violence VARCHAR(10),',
    'service_provided_within_last_month VARCHAR(255),',
    'monthly_n_and_s_distributed INT,',
    'monthly_male_condoms_distributed INT,',
    'monthly_lubes_distributed INT,',
    'monthly_female_condoms_distributed INT,',
    'monthly_self_test_kits_distributed INT,',
    'received_clinical_service VARCHAR(10),',
    'violence_reported VARCHAR(10),',
    'referred VARCHAR(10),',
    'health_edu VARCHAR(10),',
    'remarks VARCHAR(255),',
    'voided INT DEFAULT 0,',
    'CONSTRAINT `fk_peer_calendar_client` FOREIGN KEY (client_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (patient_id) ON DELETE RESTRICT ON UPDATE CASCADE,',
    'CONSTRAINT `unique_uuid_peer_calendar` UNIQUE (uuid),',
    'INDEX(visit_date),',
    'INDEX(client_id, visit_date),',
    'INDEX(location_id)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_peer_calendar table';


-- ------------ create table etl_kp_sti_treatment-----------------------
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_sti_treatment`;');
PREPARE stmt FROM @drop_etl_sti; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_sti_treatment` (',
  '  `uuid` CHAR(38),',
  '  `encounter_id` INT NOT NULL PRIMARY KEY,',
  '  `client_id` INT NOT NULL,',
  '  `location_id` INT DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `visit_id` INT,',
  '  `encounter_provider` INT,',
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
  '  `no_of_lubes` INT,',
  '  `given_condoms` VARCHAR(10),',
  '  `no_of_condoms` INT,',
  '  `provider_comments` VARCHAR(255),',
  '  `provider_name` VARCHAR(255),',
  '  `appointment_date` DATE,',
  '  `voided` INT DEFAULT 0,',
  '  CONSTRAINT `fk_etl_sti_patient` FOREIGN KEY (`client_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
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
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_peer_tracking`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_peer_tracking` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT,',
  '  `client_id` INT NOT NULL,',
  '  `visit_id` INT,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL PRIMARY KEY,',
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
  '  `voided` INT DEFAULT 0,',
  '  CONSTRAINT `fk_etl_peer_tracking_patient` FOREIGN KEY (`client_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_peer_tracking` UNIQUE (`uuid`),',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_treatment_verification`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_treatment_verification` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT,',
  '  `client_id` INT NOT NULL,',
  '  `visit_id` INT,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
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
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_treatment_verification_patient` FOREIGN KEY (`client_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_treatment_verification` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`client_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_treatment_verification table';

-- sql
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_PrEP_verification`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_PrEP_verification` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT,',
  '  `client_id` INT NOT NULL,',
  '  `visit_id` INT,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
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
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_prep_verification_patient` FOREIGN KEY (`client_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
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
-- 1. Standardized DROP
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_alcohol_drug_abuse_screening`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 2. CREATE statement with fixed variable concatenation and named constraints
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_alcohol_drug_abuse_screening` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `alcohol_drinking_frequency` VARCHAR(50) DEFAULT NULL,',
  '  `smoking_frequency` VARCHAR(50) DEFAULT NULL,',
  '  `drugs_use_frequency` VARCHAR(50) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_alcohol_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_alcohol_drug_abuse` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`patient_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_alcohol_drug_abuse_screening table';

-- ------------ create table etl_gbv_screening-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_gbv_screening`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_gbv_screening` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `ipv` VARCHAR(50) DEFAULT NULL,',
  '  `physical_ipv` VARCHAR(50) DEFAULT NULL,',
  '  `emotional_ipv` VARCHAR(50) DEFAULT NULL,',
  '  `sexual_ipv` VARCHAR(50) DEFAULT NULL,',
  '  `ipv_relationship` VARCHAR(50) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_gbv_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_gbv` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`patient_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_gbv_screening table';


-- ------------ create table etl_gbv_screening_action-----------------------

-- 1. Standardized DROP
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_gbv_screening_action`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 2. CREATE statement with fixed variable concatenation and named constraints
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_gbv_screening_action` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT,',
  '  `patient_id` INT NOT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `visit_id` INT,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `obs_id` INT NOT NULL PRIMARY KEY,',
  '  `help_provider` VARCHAR(100) DEFAULT NULL,',
  '  `action_taken` VARCHAR(100) DEFAULT NULL,',
  '  `action_date` DATE DEFAULT NULL,',
  '  `reason_for_not_reporting` VARCHAR(100) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  CONSTRAINT `fk_etl_gbv_action_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_gbv_action` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`obs_id`),',
  '  INDEX (`patient_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_gbv_screening_action table';


-- create table etl_violence_reporting

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_violence_reporting`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_violence_reporting` (',
    '  `uuid` CHAR(38),',
    '  `provider` INT,',
    '  `patient_id` INT NOT NULL,',
    '  `visit_id` INT,',
    '  `visit_date` DATE,',
    '  `location_id` INT DEFAULT NULL,',
    '  `encounter_id` INT NOT NULL PRIMARY KEY,',
    '  `place_of_incident` VARCHAR(100),',
    '  `date_of_incident` DATE,',
    '  `time_of_incident` INT,',
    '  `abuse_against` INT,',
    '  `form_of_incident` VARCHAR(500),',
    '  `perpetrator` VARCHAR(500),',
    '  `date_of_crisis_response` DATE,',
    '  `support_service` VARCHAR(100),',
    '  `hiv_testing_duration` INT,',
    '  `hiv_testing_provided_within_5_days` INT,',
    '  `duration_on_emergency_contraception` INT,',
    '  `emergency_contraception_provided_within_5_days` INT,',
    '  `psychosocial_trauma_counselling_duration` VARCHAR(50),',
    '  `psychosocial_trauma_counselling_provided_within_5_days` INT,',
    '  `pep_provided_duration` VARCHAR(50),',
    '  `pep_provided_within_5_days` INT,',
    '  `sti_screening_and_treatment_duration` VARCHAR(50),',
    '  `sti_screening_and_treatment_provided_within_5_days` INT,',
    '  `legal_support_duration` VARCHAR(50),',
    '  `legal_support_provided_within_5_days` INT,',
    '  `medical_examination_duration` VARCHAR(50),',
    '  `medical_examination_provided_within_5_days` INT,',
    '  `prc_form_file_duration` VARCHAR(50),',
    '  `prc_form_file_provided_within_5_days` INT,',
    '  `other_services_provided` VARCHAR(100),',
    '  `medical_services_and_care_duration` VARCHAR(50),',
    '  `medical_services_and_care_provided_within_5_days` INT,',
    '  `duration_of_non_sexual_legal_support` VARCHAR(50),',
    '  `duration_of_non_sexual_legal_support_within_5_days` INT,',
    '  `current_location_of_person` INT,',
    '  `follow_up_plan` VARCHAR(100),',
    '  `resolution_date` DATE,',
    '  `date_created` DATETIME NOT NULL,',
    '  `date_last_modified` DATETIME,',
    '  `voided` INT DEFAULT 0,',
    '  CONSTRAINT `fk_violence_patient` FOREIGN KEY (patient_id) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
    '  CONSTRAINT `unique_uuid_violence_reporting` UNIQUE (uuid),',
    '  INDEX (visit_date),',
    '  INDEX (encounter_id),',
    '  INDEX (patient_id),',
    '  INDEX (patient_id, visit_date)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Successfully created etl_violence_reporting table';


-- --- ----------Create table etl_link_facility_tracking-----------------------


SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_link_facility_tracking`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_link_facility_tracking` (',
    '  `uuid` CHAR(38),',
    '  `provider` INT,',
    '  `patient_id` INT NOT NULL,',
    '  `visit_id` INT,',
    '  `visit_date` DATE,',
    '  `location_id` INT DEFAULT NULL,',
    '  `encounter_id` INT NOT NULL,',
    '  `county` VARCHAR(100),',
    '  Sub_county` VARCHAR(100),',
    '  `ward` VARCHAR(100),',
    '  `facility_name` VARCHAR(100),',
    '  `ccc_number` VARCHAR(100),',
    '  `date_diagnosed` DATE,',
    '  `date_initiated_art` DATE,',
    '  `original_regimen` VARCHAR(255),',
    '  `current_regimen` VARCHAR(255),',
    '  `date_switched` DATE,',
    '  `reason_for_switch` VARCHAR(500),',
    '  `date_of_last_visit` DATE,',
    '  `date_viral_load_sample_collected` DATE,',
    '  `date_viral_load_results_received` DATE,',
    '  `viral_load_results` VARCHAR(100),',
    '  `viral_load_results_copies` INT,',
    '  `date_of_next_visit` DATE,',
    '  `enrolled_in_pssg` VARCHAR(100),',
    '  `attended_pssg` VARCHAR(100),',
    '  `on_pmtct` VARCHAR(100),',
    '  `date_of_delivery` DATE,',
    '  `tb_screening` VARCHAR(100),',
    '  `sti_treatment` VARCHAR(100),',
    '  `trauma_counselling` VARCHAR(100),',
    '  `cervical_cancer_screening` VARCHAR(100),',
    '  `family_planning` VARCHAR(100),',
    '  `currently_on_tb_treatment` VARCHAR(100),',
    '  `date_initiated_tb_treatment` DATE,',
    '  `tpt_status` VARCHAR(100),',
    '  `date_initiated_tpt` DATE,',
    '  `data_collected_through` VARCHAR(100),',
    '  `date_created` DATETIME NOT NULL,',
    '  `date_last_modified` DATETIME,',
    '  `voided` INT DEFAULT 0,',
    '  PRIMARY KEY (`encounter_id`),',
    '  CONSTRAINT `fk_link_facility_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
    '  CONSTRAINT `unique_uuid_link_facility` UNIQUE (`uuid`),',
    '  INDEX (`visit_date`),',
    '  INDEX (`patient_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_link_facility_tracking`') AS message;

-- ------------ create table etl_depression_screening-----------------------

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_depression_screening`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_depression_screening` (',
    '  `uuid` CHAR(38),',
    '  `provider` INT,',
    '  `patient_id` INT NOT NULL,',
    '  `visit_id` INT,',
    '  `visit_date` DATE,',
    '  `location_id` INT DEFAULT NULL,',
    '  `encounter_id` INT NOT NULL,',
    '  `little_interest` INT,',
    '  `feeling_down` INT,',
    '  `trouble_sleeping` INT,',
    '  `feeling_tired` INT,',
    '  `poor_appetite` INT,',
    '  `feeling_bad` INT,',
    '  `trouble_concentrating` INT,',
    '  `moving_or_speaking_slowly` INT,',
    '  `self_hurtful_thoughts` INT,',
    '  `phq_9_rating` VARCHAR(255),',
    '  `pfa_offered` INT,',
    '  `client_referred` INT,',
    '  `facility_referred` INT,',
    '  `facility_name` VARCHAR(255),',
    '  `services_referred_for` VARCHAR(255),',
    '  `date_created` DATETIME NOT NULL,',
    '  `date_last_modified` DATETIME DEFAULT NULL,',
    '  `voided` INT DEFAULT 0,',
    '  PRIMARY KEY (`encounter_id`),',
    '  CONSTRAINT `fk_depression_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
    '  CONSTRAINT `unique_uuid_depression` UNIQUE (`uuid`),',
    '  INDEX (`visit_date`),',
    '  INDEX (`patient_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_depression_screening`') AS message;


-- ------------ create table etl_adverse_events-----------------------
-- sql
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_adverse_events`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

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


SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_pre_hiv_enrollment_art`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_pre_hiv_enrollment_art` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `obs_id` INT NOT NULL,',
  '  `PMTCT` INT,',
  '  `PMTCT_regimen` INT,',
  '  `PEP` INT,',
  '  `PEP_regimen` INT,',
  '  `PrEP` INT,',
  '  `PrEP_regimen` INT,',
  '  `HAART` INT,',
  '  `HAART_regimen` INT,',
  '  `voided` INT DEFAULT 0,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  PRIMARY KEY (`obs_id`),',
  '  CONSTRAINT `fk_etl_pre_hiv_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_pre_hiv_enrollment_art`') AS message;

-- --------------------------------------
-- TABLE: etl_covid19_assessment

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_covid19_assessment`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_covid19_assessment` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `obs_id` INT NOT NULL,',
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
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`obs_id`),',
  '  CONSTRAINT `fk_etl_covid19_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_vmmc_enrolment`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_vmmc_enrolment` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT DEFAULT NULL,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `referee` INT DEFAULT NULL,',
  '  `other_referee` VARCHAR(100) DEFAULT NULL,',
  '  `source_of_vmmc_info` INT DEFAULT NULL,',
  '  `other_source_of_vmmc_info` VARCHAR(100) DEFAULT NULL,',
  '  `county_of_origin` VARCHAR(100) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_vmmc_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
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




-- TABLE: etl_vmmc_circumcision_procedure

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_vmmc_circumcision_procedure`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_vmmc_circumcision_procedure` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT DEFAULT NULL,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `circumcision_method` INT DEFAULT NULL,',
  '  `surgical_circumcision_method` INT DEFAULT NULL,',
  '  `reason_circumcision_ineligible` VARCHAR(100) DEFAULT NULL,',
  '  `circumcision_device` INT DEFAULT NULL,',
  '  `specific_other_device` VARCHAR(100) DEFAULT NULL,',
  '  `device_size` VARCHAR(100) DEFAULT NULL,',
  '  `lot_number` VARCHAR(100) DEFAULT NULL,',
  '  `anaesthesia_type` INT DEFAULT NULL,',
  '  `anaesthesia_used` INT DEFAULT NULL,',
  '  `anaesthesia_concentration` VARCHAR(100) DEFAULT NULL,',
  '  `anaesthesia_volume` INT DEFAULT NULL,',
  '  `time_of_first_placement_cut` DATETIME DEFAULT NULL,',
  '  `time_of_last_device_closure` DATETIME DEFAULT NULL,',
  '  `has_adverse_event` INT DEFAULT NULL,',
  '  `adverse_event` VARCHAR(255) DEFAULT NULL,',
  '  `severity` VARCHAR(100) DEFAULT NULL,',
  '  `adverse_event_management` VARCHAR(255) DEFAULT NULL,',
  '  `clinician_name` VARCHAR(100) DEFAULT NULL,',
  '  `clinician_cadre` INT DEFAULT NULL,',
  '  `assist_clinician_name` VARCHAR(100) DEFAULT NULL,',
  '  `assist_clinician_cadre` INT DEFAULT NULL,',
  '  `theatre_number` VARCHAR(100) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_vmmc_circumcision_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
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



-- --------------------------------------
-- TABLE: etl_vmmc_medical_history

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_vmmc_medical_history`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_vmmc_medical_history` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `assent_given` INT DEFAULT NULL,',
  '  `consent_given` INT DEFAULT NULL,',
  '  `hiv_status` INT DEFAULT NULL,',
  '  `hiv_unknown_reason` VARCHAR(255) DEFAULT NULL,',
  '  `hiv_test_date` DATE DEFAULT NULL,',
  '  `art_start_date` DATE DEFAULT NULL,',
  '  `current_regimen` VARCHAR(100) DEFAULT NULL,',
  '  `ccc_number` VARCHAR(100) DEFAULT NULL,',
  '  `next_appointment_date` DATE DEFAULT NULL,',
  '  `hiv_care_facility` INT DEFAULT NULL,',
  '  `hiv_care_facility_name` VARCHAR(100) DEFAULT NULL,',
  '  `vl` VARCHAR(50) DEFAULT NULL,',
  '  `cd4_count` VARCHAR(50) DEFAULT NULL,',
  '  `bleeding_disorder` VARCHAR(255) DEFAULT NULL,',
  '  `diabetes` VARCHAR(255) DEFAULT NULL,',
  '  `client_presenting_complaints` VARCHAR(255) DEFAULT NULL,',
  '  `other_complaints` VARCHAR(255) DEFAULT NULL,',
  '  `ongoing_treatment` VARCHAR(255) DEFAULT NULL,',
  '  `other_ongoing_treatment` VARCHAR(255) DEFAULT NULL,',
  '  `hb_level` INT DEFAULT NULL,',
  '  `sugar_level` INT DEFAULT NULL,',
  '  `has_known_allergies` INT DEFAULT NULL,',
  '  `ever_had_surgical_operation` INT DEFAULT NULL,',
  '  `specific_surgical_operation` VARCHAR(255) DEFAULT NULL,',
  '  `proven_tetanus_booster` INT DEFAULT NULL,',
  '  `ever_received_tetanus_booster` INT DEFAULT NULL,',
  '  `date_received_tetanus_booster` DATE DEFAULT NULL,',
  '  `blood_pressure` VARCHAR(50) DEFAULT NULL,',
  '  `pulse_rate` INT DEFAULT NULL,',
  '  `temperature` VARCHAR(50) DEFAULT NULL,',
  '  `in_good_health` INT DEFAULT NULL,',
  '  `counselled` INT DEFAULT NULL,',
  '  `reason_ineligible` VARCHAR(100) DEFAULT NULL,',
  '  `circumcision_method_chosen` VARCHAR(100) DEFAULT NULL,',
  '  `conventional_method_chosen` INT DEFAULT NULL,',
  '  `device_name` INT DEFAULT NULL,',
  '  `device_size` INT DEFAULT NULL,',
  '  `other_conventional_method_device_chosen` VARCHAR(100) DEFAULT NULL,',
  '  `services_referral` VARCHAR(100) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_vmmc_medical_history_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
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

SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_vmmc_client_followup`') AS message;

-- TABLE: etl_vmmc_post_operation_assessment

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_vmmc_client_followup`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_vmmc_client_followup` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `visit_type` INT DEFAULT NULL,',
  '  `days_since_circumcision` VARCHAR(50) DEFAULT NULL,',
  '  `has_adverse_event` INT DEFAULT NULL,',
  '  `adverse_event` VARCHAR(255) DEFAULT NULL,',
  '  `severity` VARCHAR(100) DEFAULT NULL,',
  '  `adverse_event_management` VARCHAR(255) DEFAULT NULL,',
  '  `medications_given` VARCHAR(255) DEFAULT NULL,',
  '  `other_medications_given` VARCHAR(255) DEFAULT NULL,',
  '  `clinician_name` VARCHAR(255) DEFAULT NULL,',
  '  `clinician_cadre` INT DEFAULT NULL,',
  '  `clinician_notes` VARCHAR(255) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_vmmc_client_followup_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_vmmc_client_followup` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`),',
  '  INDEX (`visit_type`),',
  '  INDEX (`has_adverse_event`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;




SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_vmmc_post_operation_assessment`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_vmmc_post_operation_assessment` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `blood_pressure` VARCHAR(100) DEFAULT NULL,',
  '  `pulse_rate` INT DEFAULT NULL,',
  '  `temperature` INT DEFAULT NULL,',
  '  `penis_elevated` INT DEFAULT NULL,',
  '  `given_post_procedure_instruction` INT DEFAULT NULL,',
  '  `post_procedure_instructions` VARCHAR(250) DEFAULT NULL,',
  '  `given_post_operation_medication` INT DEFAULT NULL,',
  '  `medication_given` VARCHAR(250) DEFAULT NULL,',
  '  `other_medication_given` VARCHAR(250) DEFAULT NULL,',
  '  `removal_date` DATETIME DEFAULT NULL,',
  '  `next_appointment_date` DATETIME DEFAULT NULL,',
  '  `discharged_by` VARCHAR(250) DEFAULT NULL,',
  '  `cadre` INT DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_vmmc_post_operation_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_vmmc_post_op_assessment` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_vmmc_post_operation_assessment`') AS message;

-- --------------------------------------
-- TABLE: etl_hts_eligibility_screening

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_hts_eligibility_screening`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_hts_eligibility_screening` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `population_type` VARCHAR(100),',
  '  `key_population_type` VARCHAR(100),',
  '  `priority_population_type` VARCHAR(100),',
  '  `patient_disabled` VARCHAR(50),',
  '  `disability_type` VARCHAR(255),',
  '  `recommended_test` VARCHAR(50),',
  '  `department` INT,',
  '  `patient_type` INT,',
  '  `is_health_worker` INT,',
  '  `relationship_with_contact` VARCHAR(100),',
  '  `mother_hiv_status` INT,',
  '  `tested_hiv_before` INT,',
  '  `who_performed_test` INT,',
  '  `test_results` INT,',
  '  `date_tested` DATE,',
  '  `started_on_art` INT,',
  '  `upn_number` VARCHAR(80),',
  '  `child_defiled` INT,',
  '  `ever_had_sex` INT,',
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
  '  `cough` INT,',
  '  `fever` INT,',
  '  `weight_loss` INT,',
  '  `night_sweats` INT,',
  '  `contact_with_tb_case` INT,',
  '  `lethargy` INT,',
  '  `tb_status` INT,',
  '  `shared_needle` VARCHAR(100),',
  '  `needle_stick_injuries` INT,',
  '  `traditional_procedures` INT,',
  '  `child_reasons_for_ineligibility` VARCHAR(100),',
  '  `pregnant` VARCHAR(100),',
  '  `breastfeeding_mother` VARCHAR(100),',
  '  `eligible_for_test` INT,',
  '  `referred_for_testing` INT,',
  '  `reason_to_test` VARCHAR(100),',
  '  `reason_not_to_test` VARCHAR(100),',
  '  `reasons_for_ineligibility` VARCHAR(100),',
  '  `specific_reason_for_ineligibility` VARCHAR(255),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_hts_eligibility_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
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


-- TABLE: etl_patient_appointment


SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_patient_appointment`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_patient_appointment` (',
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
  '  INDEX (`status`),',
  '  INDEX (`appointment_service_id`),',
  '  CONSTRAINT `fk_etl_patient_appointment_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_patient_appointment`') AS message;



-- TABLE: etl_drug_order

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_drug_order`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_drug_order` (',
  '  `uuid` CHAR(38),',
  '  `encounter_id` INT NOT NULL,',
  '  `order_group_id` INT,',
  '  `patient_id` INT NOT NULL,',
  '  `location_id` INT DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `visit_id` INT,',
  '  `provider` INT,',
  '  `order_id` INT,',
  '  `urgency` VARCHAR(50),',
  '  `drug_id` INT,',
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
  '  `duration` INT,',
  '  `duration_units` VARCHAR(10),',
  '  `instructions` VARCHAR(255),',
  '  `route` VARCHAR(255),',
  '  `voided` INT DEFAULT 0,',
  '  `date_voided` DATE,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_drug_order_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_drug_order` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_preventive_services`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_preventive_services` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `provider` INT,',
  '  `location_id` INT,',
  '  `encounter_id` INT NOT NULL,',
  '  `obs_group_id` INT NOT NULL,',
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
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`patient_id`, `encounter_id`, `obs_group_id`),',
  '  CONSTRAINT `fk_etl_preventive_services_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_preventive_services`') AS message;

-- TABLE: etl_overdose_reporting

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_overdose_reporting`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_overdose_reporting` (',
  '  `client_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `overdose_location` VARCHAR(100) DEFAULT NULL,',
  '  `overdose_date` DATE DEFAULT NULL,',
  '  `incident_type` INT DEFAULT NULL,',
  '  `incident_site_name` VARCHAR(255) DEFAULT NULL,',
  '  `incident_site_type` INT DEFAULT NULL,',
  '  `naloxone_provided` INT DEFAULT NULL,',
  '  `risk_factors` INT DEFAULT NULL,',
  '  `other_risk_factors` VARCHAR(255) DEFAULT NULL,',
  '  `drug` INT DEFAULT NULL,',
  '  `other_drug` VARCHAR(255) DEFAULT NULL,',
  '  `outcome` INT DEFAULT NULL,',
  '  `remarks` VARCHAR(255) DEFAULT NULL,',
  '  `reported_by` VARCHAR(255) DEFAULT NULL,',
  '  `date_reported` DATE DEFAULT NULL,',
  '  `witness` VARCHAR(255) DEFAULT NULL,',
  '  `date_witnessed` DATE DEFAULT NULL,',
  '  `encounter` VARCHAR(255) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_overdose_reporting_patient` FOREIGN KEY (`client_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_art_fast_track`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_art_fast_track` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT DEFAULT NULL,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `art_refill_model` INT DEFAULT NULL,',
  '  `ctx_dispensed` INT DEFAULT NULL,',
  '  `dapsone_dispensed` INT DEFAULT NULL,',
  '  `oral_contraceptives_dispensed` INT DEFAULT NULL,',
  '  `condoms_distributed` INT DEFAULT NULL,',
  '  `missed_arv_doses_since_last_visit` INT DEFAULT NULL,',
  '  `doses_missed` INT DEFAULT NULL,',
  '  `fatigue` INT DEFAULT NULL,',
  '  `cough` INT DEFAULT NULL,',
  '  `fever` INT DEFAULT NULL,',
  '  `rash` INT DEFAULT NULL,',
  '  `nausea_vomiting` INT DEFAULT NULL,',
  '  `genital_sore_discharge` INT DEFAULT NULL,',
  '  `diarrhea` INT DEFAULT NULL,',
  '  `other_symptoms` INT DEFAULT NULL,',
  '  `other_specific_symptoms` INT DEFAULT NULL,',
  '  `pregnant` INT DEFAULT NULL,',
  '  `family_planning_status` INT DEFAULT NULL,',
  '  `family_planning_method` VARCHAR(250) DEFAULT NULL,',
  '  `reason_not_on_family_planning` VARCHAR(250) DEFAULT NULL,',
  '  `referred_to_clinic` INT DEFAULT NULL,',
  '  `return_visit_date` DATE DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_art_fast_track_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_art_fast_track` UNIQUE (`uuid`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`encounter_id`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_art_fast_track`') AS message;



SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_clinical_encounter`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_clinical_encounter` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `visit_type` VARCHAR(100) DEFAULT NULL,',
  '  `referred_from` INT DEFAULT NULL,',
  '  `therapy_ordered` VARCHAR(100) DEFAULT NULL,',
  '  `other_therapy_ordered` VARCHAR(100) DEFAULT NULL,',
  '  `counselling_ordered` VARCHAR(100) DEFAULT NULL,',
  '  `other_counselling_ordered` VARCHAR(100) DEFAULT NULL,',
  '  `procedures_prescribed` INT DEFAULT NULL,',
  '  `procedures_ordered` VARCHAR(100) DEFAULT NULL,',
  '  `patient_outcome` INT DEFAULT NULL,',
  '  `diagnosis_category` VARCHAR(100) DEFAULT NULL,',
  '  `general_examination` VARCHAR(255) DEFAULT NULL,',
  '  `admission_needed` INT DEFAULT NULL,',
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
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_etl_clinical_encounter_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_clinical_encounter` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- --------------------------------------
-- TABLE: etl_pep_management_survivor

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_pep_management_survivor`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_pep_management_survivor` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `prc_number` VARCHAR(100),',
  '  `incident_reporting_date` DATE,',
  '  `type_of_violence` INT,',
  '  `disabled` INT,',
  '  `other_type_of_violence` VARCHAR(255),',
  '  `type_of_assault` VARCHAR(255),',
  '  `other_type_of_assault` VARCHAR(255),',
  '  `incident_date` DATE,',
  '  `perpetrator_identity` VARCHAR(100),',
  '  `survivor_relation_to_perpetrator` INT,',
  '  `perpetrator_compulsory_HIV_test_done` INT,',
  '  `perpetrator_compulsory_HIV_test_result` INT,',
  '  `perpetrator_file_number` VARCHAR(100),',
  '  `survivor_state` VARCHAR(255),',
  '  `clothing_state` VARCHAR(255),',
  '  `other_injuries` VARCHAR(255),',
  '  `genitalia_examination` VARCHAR(255),',
  '  `high_vaginal_or_anal_swab` VARCHAR(255),',
  '  `rpr_vdrl` INT,',
  '  `survivor_hiv_test_result` INT,',
  '  `given_pep` INT,',
  '  `referred_to_psc` INT,',
  '  `pdt` INT,',
  '  `emergency_contraception_issued` INT,',
  '  `reason_emergency_contraception_not_issued` INT,',
  '  `sti_prophylaxis_and_treatment` INT,',
  '  `reason_sti_prophylaxis_not_issued` VARCHAR(255),',
  '  `pep_regimen_issued` INT,',
  '  `reason_pep_regimen_not_issued` VARCHAR(255),',
  '  `starter_pack_given` INT,',
  '  `date_given_pep` DATE,',
  '  `HBsAG_result` INT,',
  '  `LFTs_ALT` VARCHAR(100),',
  '  `RFTs_creatinine` VARCHAR(100),',
  '  `other_tests` VARCHAR(255),',
  '  `next_appointment_date` DATE,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_pep_survivor_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_sgbv_pep_followup`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_sgbv_pep_followup` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `visit_number` INT,',
  '  `pep_completed` INT,',
  '  `reason_pep_not_completed` VARCHAR(255),',
  '  `hiv_test_done` INT,',
  '  `hiv_test_result` INT,',
  '  `pdt_test_done` INT,',
  '  `pdt_test_result` INT,',
  '  `HBsAG_test_done` INT,',
  '  `HBsAG_test_result` INT,',
  '  `lfts_alt` VARCHAR(50),',
  '  `rfts_creatinine` VARCHAR(50),',
  '  `three_month_post_exposure_HIV_serology_result` INT,',
  '  `patient_assessment` VARCHAR(255),',
  '  `next_appointment_date` DATE,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_sgbv_pep_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_sgbv_pep_followup` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`visit_number`),',
  '  INDEX (`pep_completed`),',
  '  INDEX (`three_month_post_exposure_HIV_serology_result`),',
  '  INDEX (`hiv_test_result`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_sgbv_pep_followup`') AS message;


-- sql
-- --------------------------------------
-- TABLE: etl_sgbv_post_rape_care

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_sgbv_post_rape_care`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_sgbv_post_rape_care` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `examination_date` DATE,',
  '  `incident_date` DATE,',
  '  `number_of_perpetrators` VARCHAR(10),',
  '  `is_perpetrator_known` INT,',
  '  `survivor_relation_to_perpetrator` VARCHAR(100),',
  '  `county` VARCHAR(100),',
  '  `sub_county` VARCHAR(100),',
  '  `landmark` VARCHAR(100),',
  '  `observation_on_chief_complaint` VARCHAR(255),',
  '  `chief_complaint_report` VARCHAR(255),',
  '  `circumstances_around_incident` VARCHAR(255),',
  '  `type_of_sexual_violence` INT,',
  '  `other_type_of_sexual_violence` VARCHAR(255),',
  '  `use_of_condoms` INT,',
  '  `prior_attendance_to_health_facility` INT,',
  '  `attended_health_facility_name` VARCHAR(100),',
  '  `date_attended_health_facility` DATE,',
  '  `treated_at_facility` INT,',
  '  `given_referral_notes` INT,',
  '  `incident_reported_to_police` INT,',
  '  `police_station_name` VARCHAR(100),',
  '  `police_report_date` DATE,',
  '  `medical_or_surgical_history` VARCHAR(255),',
  '  `additional_info_from_survivor` VARCHAR(255),',
  '  `physical_examination` VARCHAR(255),',
  '  `parity_term` INT,',
  '  `parity_abortion` INT,',
  '  `on_contraception` INT,',
  '  `known_pregnancy` INT,',
  '  `date_of_last_consensual_sex` DATE,',
  '  `systolic` INT,',
  '  `diastolic` INT,',
  '  `demeanor` INT,',
  '  `changed_clothes` INT,',
  '  `state_of_clothes` VARCHAR(100),',
  '  `means_clothes_transported` INT,',
  '  `details_about_clothes_transport` VARCHAR(255),',
  '  `clothes_handed_to_police` INT,',
  '  `survivor_went_to_toilet` INT,',
  '  `survivor_bathed` INT,',
  '  `bath_details` VARCHAR(255),',
  '  `survivor_left_marks_on_perpetrator` INT,',
  '  `details_of_marks_on_perpetrator` INT,',
  '  `physical_injuries` VARCHAR(255),',
  '  `details_outer_genitalia` VARCHAR(255),',
  '  `details_vagina` VARCHAR(255),',
  '  `details_hymen` VARCHAR(255),',
  '  `details_anus` VARCHAR(255),',
  '  `significant_orifice` VARCHAR(255),',
  '  `pep_first_dose` INT,',
  '  `ecp_given` INT,',
  '  `stitching_done` INT,',
  '  `stitching_notes` VARCHAR(255),',
  '  `treated_for_sti` INT,',
  '  `sti_treatment_remarks` VARCHAR(255),',
  '  `other_medications` VARCHAR(255),',
  '  `referred_to` VARCHAR(255),',
  '  `web_prep_microscopy` INT,',
  '  `samples_packed` VARCHAR(255),',
  '  `examining_officer` VARCHAR(100),',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_sgbv_post_rape_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_sgbv_post_rape` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`incident_date`),',
  '  INDEX (`pep_first_dose`),',
  '  INDEX (`ecp_given`),',
  '  INDEX (`survivor_relation_to_perpetrator`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_sgbv_post_rape_care`') AS message;



SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_gbv_physical_emotional_abuse`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_gbv_physical_emotional_abuse` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `gbv_number` VARCHAR(100),',
  '  `referred_from` INT,',
  '  `entry_point` INT,',
  '  `other_referral_source` VARCHAR(100),',
  '  `type_of_violence` INT,',
  '  `date_of_incident` DATE,',
  '  `trauma_counselling` INT,',
  '  `trauma_counselling_comments` VARCHAR(255),',
  '  `referred_to` VARCHAR(255),',
  '  `other_referral` VARCHAR(255),',
  '  `next_appointment_date` DATE,',
  '  `voided` INT DEFAULT 0,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_gbv_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_gbv_phys_emot_abuse` UNIQUE (`uuid`),',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_family_planning`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_family_planning` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `first_user_of_contraceptive` INT,',
  '  `counselled_on_fp` INT,',
  '  `contraceptive_dispensed` INT,',
  '  `type_of_visit_for_method` INT,',
  '  `type_of_service` INT,',
  '  `quantity_dispensed` VARCHAR(10),',
  '  `reasons_for_larc_removal` INT,',
  '  `other_reasons_for_larc_removal` VARCHAR(255),',
  '  `counselled_on_natural_fp` INT,',
  '  `circle_beads_given` INT,',
  '  `receiving_postpartum_fp` INT,',
  '  `experienced_intimate_partner_violence` INT,',
  '  `referred_for_fp` INT,',
  '  `referred_to` INT,',
  '  `referred_from` INT,',
  '  `reasons_for_referral` VARCHAR(255),',
  '  `voided` INT DEFAULT 0,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_family_planning_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_physiotherapy`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_physiotherapy` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `visit_type` INT,',
  '  `referred_from` INT,',
  '  `referred_from_department` INT,',
  '  `referred_from_department_other` VARCHAR(100),',
  '  `number_of_sessions` INT,',
  '  `referral_reason` VARCHAR(255),',
  '  `disorder_category` INT,',
  '  `other_disorder_category` VARCHAR(255),',
  '  `clinical_notes` VARCHAR(255),',
  '  `pin_scale` INT,',
  '  `affected_region` INT,',
  '  `range_of_motion` INT,',
  '  `strength_test` INT,',
  '  `functional_assessment` INT,',
  '  `assessment_finding` VARCHAR(255),',
  '  `goals` VARCHAR(255),',
  '  `planned_interventions` INT,',
  '  `other_interventions` VARCHAR(255),',
  '  `sessions_per_week` VARCHAR(255),',
  '  `patient_outcome` INT,',
  '  `referred_for` VARCHAR(255),',
  '  `referred_to` INT,',
  '  `transfer_to_facility` VARCHAR(255),',
  '  `services_referred_for` VARCHAR(255),',
  '  `date_of_admission` DATE,',
  '  `reason_for_admission` VARCHAR(255),',
  '  `type_of_admission` INT,',
  '  `priority_of_admission` INT,',
  '  `admission_ward` INT,',
  '  `duration_of_hospital_stay` INT,',
  '  `voided` INT DEFAULT 0,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_physiotherapy_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
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

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_psychiatry`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_psychiatry` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `visit_type` INT,',
  '  `referred_from` INT,',
  '  `referred_from_department` INT,',
  '  `presenting_allegations` INT,',
  '  `other_allegations` VARCHAR(255),',
  '  `contact_with_TB_case` INT,',
  '  `history_of_present_illness` VARCHAR(255),',
  '  `surgical_history` INT,',
  '  `type_of_surgery` VARCHAR(255),',
  '  `surgery_date` DATE,',
  '  `on_medication` INT,',
  '  `childhood_mistreatment` INT,',
  '  `persistent_cruelty_meanness` INT,',
  '  `physically_abused` INT,',
  '  `sexually_abused` INT,',
  '  `patient_occupation_history` VARCHAR(255),',
  '  `reproductive_history` VARCHAR(255),',
  '  `lmp_date` INT,',
  '  `general_examination_findings` VARCHAR(255),',
  '  `mental_status` INT,',
  '  `attitude_and_behaviour` INT,',
  '  `speech` INT,',
  '  `mood` INT,',
  '  `illusions` INT,',
  '  `attention_concentration` INT,',
  '  `memory_recall` INT,',
  '  `judgement` INT,',
  '  `insight` INT,',
  '  `affect` VARCHAR(255),',
  '  `thought_process` VARCHAR(255),',
  '  `thought_content` VARCHAR(255),',
  '  `hallucinations` VARCHAR(255),',
  '  `orientation_status` VARCHAR(255),',
  '  `management_plan` VARCHAR(255),',
  '  `counselling_prescribed` VARCHAR(255),',
  '  `patient_outcome` INT,',
  '  `referred_to` INT,',
  '  `facility_transferred_to` VARCHAR(255),',
  '  `date_of_admission` DATE,',
  '  `reason_for_admission` VARCHAR(255),',
  '  `type_of_admission` INT,',
  '  `priority_of_admission` INT,',
  '  `admission_ward` INT,',
  '  `duration_of_hospital_stay` INT,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_psychiatry_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_psychiatry` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`mental_status`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_psychiatry`') AS message;


-- --------------------------------------
-- TABLE: etl_kvp_clinical_enrollment

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_kvp_clinical_enrollment`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_kvp_clinical_enrollment` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `contacted_by_pe_for_health_services` INT,',
  '  `has_regular_non_paying_sexual_partner` INT,',
  '  `number_of_sexual_partners` INT,',
  '  `year_started_fsw` INT,',
  '  `year_started_msm` INT,',
  '  `year_started_using_drugs` INT,',
  '  `trucker_duration_on_transit` INT,',
  '  `duration_working_as_trucker` INT,',
  '  `duration_working_as_fisherfolk` INT,',
  '  `year_tested_discordant_couple` INT,',
  '  `ever_experienced_violence` INT,',
  '  `type_of_violence_experienced` INT,',
  '  `ever_tested_for_hiv` INT,',
  '  `latest_hiv_test_method` INT,',
  '  `latest_hiv_test_results` INT,',
  '  `willing_to_test_for_hiv` INT,',
  '  `reason_not_willing_to_test_for_hiv` VARCHAR(255),',
  '  `receiving_hiv_care` INT,',
  '  `hiv_care_facility` INT,',
  '  `other_hiv_care_facility` INT,',
  '  `ccc_number` VARCHAR(50),',
  '  `consent_followup` VARCHAR(50),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_kvp_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_kvp_enrollment` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`latest_hiv_test_results`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_kvp_clinical_enrollment`') AS message;


-- sql
-- --------------------------------------
-- TABLE: etl_special_clinics

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_special_clinics`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_special_clinics` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `visit_type` INT,',
  '  `pregnantOrLactating` INT,',
  '  `referred_from` INT,',
  '  `eye_assessed` INT,',
  '  `acuity_finding` INT,',
  '  `referred_to` INT,',
  '  `ot_intervention` VARCHAR(255),',
  '  `assistive_technology` VARCHAR(255),',
  '  `enrolled_in_school` INT,',
  '  `patient_with_disability` INT,',
  '  `patient_has_edema` INT,',
  '  `nutritional_status` INT,',
  '  `patient_pregnant` INT,',
  '  `sero_status` INT,',
  '  `nutritional_intervention` INT,',
  '  `postnatal` INT,',
  '  `patient_on_arv` INT,',
  '  `anaemia_level` INT,',
  '  `metabolic_disorders` VARCHAR(255),',
  '  `critical_nutrition_practices` VARCHAR(255),',
  '  `maternal_nutrition` INT,',
  '  `therapeutic_food` VARCHAR(255),',
  '  `supplemental_food` VARCHAR(255),',
  '  `micronutrients` VARCHAR(255),',
  '  `referral_status` INT,',
  '  `criteria_for_admission` INT,',
  '  `type_of_admission` INT,',
  '  `cadre` INT,',
  '  `neuron_developmental_findings` VARCHAR(255),',
  '  `neurodiversity_conditions` INT,',
  '  `learning_findings` VARCHAR(255),',
  '  `screening_site` INT,',
  '  `communication_mode` INT,',
  '  `neonatal_risk_factor` INT,',
  '  `presence_of_comobidities` VARCHAR(255),',
  '  `first_screening_date` DATE,',
  '  `first_screening_outcome` INT,',
  '  `second_screening_outcome` INT,',
  '  `symptoms_for_otc` VARCHAR(255),',
  '  `nutritional_details` INT,',
  '  `first_0_6_months` INT,',
  '  `second_6_12_months` INT,',
  '  `disability_classification` VARCHAR(255),',
  '  `treatment_intervention` VARCHAR(255),',
  '  `area_of_service` INT,',
  '  `diagnosis_category` VARCHAR(100),',
  '  `next_appointment_date` DATE,',
  '  `orthopaedic_patient_no` INT,',
  '  `patient_outcome` INT,',
  '  `special_clinic` VARCHAR(255),',
  '  `special_clinic_form_uuid` CHAR(38),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_special_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_special_clinics` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_type`),',
  '  INDEX (`visit_date`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_special_clinics`') AS message;




-- --------------------------------------
-- TABLE: etl_high_iit_intervention

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_high_iit_intervention`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_high_iit_intervention` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT DEFAULT NULL,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `interventions_offered` VARCHAR(500) DEFAULT NULL,',
  '  `appointment_mgt_interventions` VARCHAR(500) DEFAULT NULL,',
  '  `reminder_methods` VARCHAR(255) DEFAULT NULL,',
  '  `enrolled_in_ushauri` INT DEFAULT NULL,',
  '  `appointment_mngt_intervention_date` DATE DEFAULT NULL,',
  '  `date_assigned_case_manager` DATE DEFAULT NULL,',
  '  `eacs_recommended` INT DEFAULT NULL,',
  '  `enrolled_in_psychosocial_support_group` INT DEFAULT NULL,',
  '  `robust_literacy_interventions_date` DATE DEFAULT NULL,',
  '  `expanding_differentiated_service_delivery_interventions` INT DEFAULT NULL,',
  '  `enrolled_in_nishauri` INT DEFAULT NULL,',
  '  `expanded_differentiated_service_delivery_interventions_date` DATE DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_high_iit_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_high_iit_intervention` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`enrolled_in_ushauri`),',
  '  INDEX (`enrolled_in_nishauri`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_high_iit_intervention`') AS message;



-- TABLE: etl_home_visit_checklist

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_home_visit_checklist`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_home_visit_checklist` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT DEFAULT NULL,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `visit_date` DATE DEFAULT NULL,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `independence_in_daily_activities` VARCHAR(255) DEFAULT NULL,',
  '  `other_independence_activities` VARCHAR(255) DEFAULT NULL,',
  '  `meeting_basic_needs` VARCHAR(255) DEFAULT NULL,',
  '  `other_basic_needs` VARCHAR(255) DEFAULT NULL,',
  '  `disclosure_to_sexual_partner` INT DEFAULT NULL,',
  '  `disclosure_to_household_members` INT DEFAULT NULL,',
  '  `disclosure_to` VARCHAR(255) DEFAULT NULL,',
  '  `mode_of_storing_arv_drugs` VARCHAR(255) DEFAULT NULL,',
  '  `arv_drugs_taking_regime` VARCHAR(255) DEFAULT NULL,',
  '  `receives_household_social_support` INT DEFAULT NULL,',
  '  `household_social_support_given` VARCHAR(255) DEFAULT NULL,',
  '  `receives_community_social_support` INT DEFAULT NULL,',
  '  `community_social_support_given` VARCHAR(255) DEFAULT NULL,',
  '  `linked_to_non_clinical_services` VARCHAR(255) DEFAULT NULL,',
  '  `linked_to_other_services` VARCHAR(255) DEFAULT NULL,',
  '  `has_mental_health_issues` INT DEFAULT NULL,',
  '  `suffering_stressful_situation` INT DEFAULT NULL,',
  '  `uses_drugs_alcohol` INT DEFAULT NULL,',
  '  `has_side_medications_effects` INT DEFAULT NULL,',
  '  `medication_side_effects` VARCHAR(255) DEFAULT NULL,',
  '  `assessment_notes` VARCHAR(255) DEFAULT NULL,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_home_visit_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_home_visit_checklist` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`has_mental_health_issues`),',
  '  INDEX (`disclosure_to_sexual_partner`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_home_visit_checklist`') AS message;


-- TABLE: etl_ncd_enrollment
-- 1. Standardized DROP
SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_ncd_enrollment`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 2. CREATE statement with optimized indexing and foreign keys
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_ncd_enrollment` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT DEFAULT NULL,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `visit_type` VARCHAR(255) DEFAULT NULL,',
  '  `referred_from` INT DEFAULT NULL,',
  '  `referred_from_department` INT DEFAULT NULL,',
  '  `referred_from_department_other` VARCHAR(100),',
  '  `patient_complaint` INT DEFAULT NULL,',
  '  `specific_complaint` VARCHAR(255),',
  '  `disease_type` INT DEFAULT NULL,',
  '  `diabetes_condition` INT DEFAULT NULL,',
  '  `diabetes_type` INT DEFAULT NULL,',
  '  `diabetes_diagnosis_date` DATE,',
  '  `hypertension_condition` INT DEFAULT NULL,',
  '  `hypertension_stage` VARCHAR(100),',
  '  `hypertension_type` INT DEFAULT NULL,',
  '  `comorbid_condition` INT DEFAULT NULL,',
  '  `diagnosis_date` DATE,',
  '  `hiv_status` INT DEFAULT NULL,',
  '  `hiv_positive_on_art` INT DEFAULT NULL,',
  '  `tb_screening` INT DEFAULT NULL,',
  '  `smoke_check` INT DEFAULT NULL,',
  '  `date_stopped_smoke` DATE,',
  '  `drink_alcohol` INT DEFAULT NULL,',
  '  `date_stopped_alcohol` DATE,',
  '  `cessation_counseling` INT DEFAULT NULL,',
  '  `physical_activity` INT DEFAULT NULL,',
  '  `diet_routine` INT DEFAULT NULL,',
  '  `existing_complications` VARCHAR(500),',
  '  `other_existing_complications` VARCHAR(500),',
  '  `new_complications` VARCHAR(500),',
  '  `other_new_complications` VARCHAR(500),',
  '  `examination_findings` VARCHAR(500),',
  '  `cardiovascular` INT DEFAULT NULL,',
  '  `respiratory` INT DEFAULT NULL,',
  '  `abdominal_pelvic` INT DEFAULT NULL,',
  '  `neurological` INT DEFAULT NULL,',
  '  `oral_exam` INT DEFAULT NULL,',
  '  `foot_risk` INT DEFAULT NULL,',
  '  `foot_low_risk` VARCHAR(500),',
  '  `foot_high_risk` VARCHAR(500),',
  '  `diabetic_foot` INT DEFAULT NULL,',
  '  `describe_diabetic_foot_type` VARCHAR(255),',
  '  `treatment_given` VARCHAR(255),',
  '  `other_treatment_given` VARCHAR(255),',
  '  `lifestyle_advice` VARCHAR(255),',
  '  `nutrition_assessment` VARCHAR(255),',
  '  `footcare_outcome` INT DEFAULT NULL,',
  '  `referred_to` VARCHAR(255),',
  '  `reasons_for_referral` VARCHAR(255),',
  '  `clinical_notes` VARCHAR(255),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_ncd_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_ncd_enrollment` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`disease_type`),',
  '  INDEX (`diabetes_type`),',
  '  INDEX (`hypertension_type`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_ncd_enrollment`') AS message;




-- TABLE: etl_adr_assessment_tool

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_adr_assessment_tool`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_adr_assessment_tool` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `weight_taken` INT,',
  '  `weight_not_taken_specify` VARCHAR(255),',
  '  `taking_arvs_everyday` INT,',
  '  `not_taking_arvs_everyday` VARCHAR(255),',
  '  `correct_dosage_per_weight` INT,',
  '  `dosage_not_correct_specify` VARCHAR(255),',
  '  `arv_dosage_frequency` INT,',
  '  `other_medication_dosage_frequency` INT,',
  '  `arv_medication_time` INT,',
  '  `arv_timing_working` INT,',
  '  `arv_timing_not_working_specify` VARCHAR(255),',
  '  `other_medication_time` INT,',
  '  `other_medication_timing_working` INT,',
  '  `other_medication_time_not_working_specify` VARCHAR(255),',
  '  `arv_frequency_difficult_to_follow` INT,',
  '  `difficult_arv_to_follow_specify` VARCHAR(255),',
  '  `difficulty_with_arv_tablets_or_liquids` INT,',
  '  `difficulty_with_arv_tablets_or_liquids_specify` VARCHAR(255),',
  '  `othe_drugs_frequency_difficult_to_follow` INT,',
  '  `difficult_other_drugs_to_follow_specify` VARCHAR(255),',
  '  `difficulty_other_drugs_tablets_or_liquids` INT,',
  '  `difficulty_other_drugs_tablets_or_liquids_specify` VARCHAR(255),',
  '  `arv_difficulty_due_to_taste_or_size` INT,',
  '  `arv_difficulty_due_to_taste_or_size_specify` VARCHAR(255),',
  '  `arv_symptoms_on_intake` VARCHAR(500),',
  '  `laboratory_abnormalities` INT,',
  '  `laboratory_abnormalities_specify` INT,',
  '  `summary_findings` VARCHAR(500),',
  '  `severity_of_reaction` INT,',
  '  `reaction_seriousness` INT,',
  '  `reason_for_seriousness` INT,',
  '  `action_taken_on_reaction` INT,',
  '  `reaction_resolved_on_dose_change` INT,',
  '  `reaction_reappeared_after_drug_introduced` INT,',
  '  `laboratory_investigations_done` VARCHAR(255),',
  '  `outcome` INT,',
  '  `reported_adr_to_pharmacy_board` INT,',
  '  `name_of_adr` VARCHAR(255),',
  '  `adr_report_number` VARCHAR(50),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_adr_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_adr_assessment` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`severity_of_reaction`),',
  '  INDEX (`reported_adr_to_pharmacy_board`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_adr_assessment_tool`') AS message;



-- TABLE: etl_ncd_followup

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_ncd_followup`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;


SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_ncd_followup` (',
  '  `uuid` CHAR(38),',
  '  `provider` INT DEFAULT NULL,',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `visit_date` DATE,',
  '  `location_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `visit_type` VARCHAR(255) DEFAULT NULL,',
  '  `tobacco_use` INT DEFAULT NULL,',
  '  `drink_alcohol` INT DEFAULT NULL,',
  '  `physical_activity` INT DEFAULT NULL,',
  '  `healthy_diet` INT DEFAULT NULL,',
  '  `patient_complaint` INT DEFAULT NULL,',
  '  `specific_complaint` VARCHAR(500),',
  '  `other_specific_complaint` VARCHAR(500),',
  '  `examination_findings` VARCHAR(500),',
  '  `cardiovascular` INT DEFAULT NULL,',
  '  `respiratory` INT DEFAULT NULL,',
  '  `abdominal_pelvic` INT DEFAULT NULL,',
  '  `neurological` INT DEFAULT NULL,',
  '  `oral_exam` INT DEFAULT NULL,',
  '  `foot_exam` VARCHAR(255),',
  '  `diabetic_foot` INT DEFAULT NULL,',
  '  `foot_risk_assessment` VARCHAR(100),',
  '  `diabetic_foot_risk` INT DEFAULT NULL,',
  '  `adhering_medication` INT DEFAULT NULL,',
  '  `referred_to` VARCHAR(255),',
  '  `reasons_for_referral` VARCHAR(255),',
  '  `clinical_notes` VARCHAR(255),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_ncd_followup_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_ncd_followup` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`adhering_medication`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_ncd_followup`') AS message;


-- sql
-- --------------------------------------
-- TABLE: etl_inpatient_admission

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_inpatient_admission`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;


SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_inpatient_admission` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `admission_date` DATE,',
  '  `payment_mode` INT,',
  '  `admission_location_id` INT,',
  '  `admission_location_name` VARCHAR(255),',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_inpatient_admission_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_inpatient_admission` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`admission_date`),',
  '  INDEX (`payment_mode`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_inpatient_admission`') AS message;


-- TABLE: etl_inpatient_discharge

SET @sql = CONCAT('DROP TABLE IF EXISTS ', etl_schema, '.`etl_inpatient_discharge`;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = CONCAT(
  'CREATE TABLE IF NOT EXISTS ', etl_schema, '.`etl_inpatient_discharge` (',
  '  `patient_id` INT NOT NULL,',
  '  `visit_id` INT DEFAULT NULL,',
  '  `encounter_id` INT NOT NULL,',
  '  `uuid` CHAR(38) NOT NULL,',
  '  `location_id` INT NOT NULL,',
  '  `provider` INT NOT NULL,',
  '  `visit_date` DATE,',
  '  `discharge_instructions` VARCHAR(255),',
  '  `discharge_status` INT,',
  '  `follow_up_date` DATE,',
  '  `followup_specialist` INT,',
  '  `date_created` DATETIME NOT NULL,',
  '  `date_last_modified` DATETIME DEFAULT NULL,',
  '  `voided` INT DEFAULT 0,',
  '  PRIMARY KEY (`encounter_id`),',
  '  CONSTRAINT `fk_inpatient_discharge_patient` FOREIGN KEY (`patient_id`) REFERENCES ', etl_schema, '.`etl_patient_demographics` (`patient_id`) ON DELETE RESTRICT ON UPDATE CASCADE,',
  '  CONSTRAINT `unique_uuid_etl_inpatient_discharge` UNIQUE (`uuid`),',
  '  INDEX (`patient_id`),',
  '  INDEX (`visit_id`),',
  '  INDEX (`visit_date`),',
  '  INDEX (`discharge_status`)',
  ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'
);

PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created `', etl_schema, '`.`etl_inpatient_discharge`') AS message;

END $$

DELIMITER ;