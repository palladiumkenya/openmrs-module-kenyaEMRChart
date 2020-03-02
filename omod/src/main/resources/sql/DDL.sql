DROP PROCEDURE IF EXISTS create_etl_tables$$
CREATE PROCEDURE create_etl_tables()
BEGIN
DECLARE script_id INT(11);

-- create/recreate database kenyaemr_etl
drop database if exists kenyaemr_etl;
create database kenyaemr_etl;

drop database if exists kenyaemr_datatools;
create database kenyaemr_datatools;

DROP TABLE IF EXISTS kenyaemr_etl.etl_script_status;
CREATE TABLE kenyaemr_etl.etl_script_status(
  id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
  script_name VARCHAR(50) DEFAULT null,
  start_time DATETIME DEFAULT NULL,
  stop_time DATETIME DEFAULT NULL,
  error VARCHAR(255) DEFAULT NULL
);

-- Log start time
INSERT INTO kenyaemr_etl.etl_script_status(script_name, start_time) VALUES('initial_creation_of_tables', NOW());
SET script_id = LAST_INSERT_ID();

DROP TABLE if exists kenyaemr_etl.etl_hiv_enrollment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_patient_hiv_followup;
DROP TABLE IF EXISTS kenyaemr_etl.etl_laboratory_extract;
DROP TABLE IF EXISTS kenyaemr_etl.etl_pharmacy_extract;
DROP TABLE IF EXISTS kenyaemr_etl.etl_patient_treatment_event;
DROP TABLE IF EXISTS kenyaemr_etl.etl_patient_program_discontinuation;
DROP TABLE IF EXISTS kenyaemr_etl.etl_mch_enrollment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_mch_antenatal_visit;
DROP TABLE IF EXISTS kenyaemr_etl.etl_mch_postnatal_visit;
DROP TABLE IF EXISTS kenyaemr_etl.etl_tb_enrollment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_tb_follow_up_visit;
DROP TABLE IF EXISTS kenyaemr_etl.etl_tb_screening;
DROP TABLE IF EXISTS kenyaemr_etl.etl_hei_enrollment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_hei_follow_up_visit;
DROP TABLE IF EXISTS kenyaemr_etl.etl_mchs_delivery;
DROP TABLE IF EXISTS kenyaemr_etl.etl_mchs_discharge;
DROP TABLE IF EXISTS kenyaemr_etl.etl_hei_immunization;
DROP TABLE IF EXISTS kenyaemr_etl.etl_patients_booked_today;
DROP TABLE IF EXISTS kenyaemr_etl.etl_missed_appointments;
DROP TABLE if exists kenyaemr_etl.etl_patient_demographics;
DROP TABLE IF EXISTS kenyaemr_etl.etl_drug_event;
DROP TABLE IF EXISTS kenyaemr_etl.etl_hts_test;
DROP TABLE IF EXISTS kenyaemr_etl.etl_hts_referral_and_linkage;
DROP TABLE IF EXISTS kenyaemr_etl.tmp_regimen_events_ordered;
DROP TABLE IF EXISTS kenyaemr_etl.etl_ccc_defaulter_tracing;
DROP TABLE IF EXISTS kenyaemr_etl.etl_ART_preparation;
DROP TABLE IF EXISTS kenyaemr_etl.etl_enhanced_adherence;
DROP TABLE IF EXISTS kenyaemr_etl.etl_patient_triage;
DROP TABLE IF EXISTS kenyaemr_etl.etl_hts_linkage_tracing;
DROP TABLE IF EXISTS kenyaemr_etl.etl_ipt_initiation;
DROP TABLE IF EXISTS kenyaemr_etl.etl_ipt_follow_up;
DROP TABLE IF EXISTS kenyaemr_etl.etl_ipt_outcome;
DROP TABLE IF EXISTS kenyaemr_etl.etl_patient_program;
DROP TABLE IF EXISTS kenyaemr_etl.etl_default_facility_info;
DROP TABLE IF EXISTS kenyaemr_etl.etl_hts_referral;

DROP TABLE IF EXISTS kenyaemr_etl.etl_prep_behaviour_risk_assessment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_prep_monthly_refill;
DROP TABLE IF EXISTS kenyaemr_etl.etl_prep_discontinuation;
DROP TABLE IF EXISTS kenyaemr_etl.etl_prep_enrollment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_prep_followup;
DROP TABLE IF EXISTS kenyaemr_etl.etl_progress_note;
DROP TABLE IF EXISTS kenyaemr_etl.etl_ovc_enrolment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_cervical_cancer_screening;

DROP TABLE IF EXISTS kenyaemr_etl.etl_client_registration;
DROP TABLE IF EXISTS kenyaemr_etl.etl_contact;
DROP TABLE IF EXISTS kenyaemr_etl.etl_client_enrollment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_clinical_visit;
DROP TABLE IF EXISTS kenyaemr_etl.etl_peer_calendar;
DROP TABLE IF EXISTS kenyaemr_etl.etl_sti_treatment;

-- create table etl_patient_demographics
create table kenyaemr_etl.etl_patient_demographics (
patient_id INT(11) not null primary key,
given_name VARCHAR(255),
middle_name VARCHAR(255),
family_name VARCHAR(255),
Gender VARCHAR(10),
DOB DATE,
national_id_no VARCHAR(50),
unique_patient_no VARCHAR(50),
patient_clinic_number VARCHAR(15) DEFAULT NULL,
Tb_no VARCHAR(50),
district_reg_no VARCHAR(50),
hei_no VARCHAR(50),
phone_number VARCHAR(50) DEFAULT NULL,
birth_place VARCHAR(50) DEFAULT NULL,
citizenship VARCHAR(50) DEFAULT NULL,
email_address VARCHAR(100) DEFAULT NULL,
next_of_kin VARCHAR(255) DEFAULT NULL,
next_of_kin_phone VARCHAR(100) DEFAULT NULL,
next_of_kin_relationship VARCHAR(100) DEFAULT NULL,
marital_status VARCHAR(50) DEFAULT NULL,
education_level VARCHAR(50) DEFAULT NULL,
dead INT(11),
death_date DATE DEFAULT NULL,
voided INT(11),
index(patient_id),
index(Gender),
index(unique_patient_no),
index(DOB)

);

SELECT "Successfully created etl_patient_demographics table";
-- create table etl_hiv_enrollment


create table kenyaemr_etl.etl_hiv_enrollment(
uuid char(38) ,
patient_id INT(11) NOT NULL,
visit_id INT(11) DEFAULT NULL,
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
encounter_provider INT(11),
patient_type INT(11),
date_first_enrolled_in_care DATE,
entry_point INT(11),
transfer_in_date DATE,
facility_transferred_from VARCHAR(255),
district_transferred_from VARCHAR(255),
date_started_art_at_transferring_facility DATE,
date_confirmed_hiv_positive DATE,
facility_confirmed_hiv_positive VARCHAR(255),
arv_status INT(11),
name_of_treatment_supporter VARCHAR(255),
relationship_of_treatment_supporter INT(11),
treatment_supporter_telephone VARCHAR(100),
treatment_supporter_address VARCHAR(100),
date_of_discontinuation DATETIME,
discontinuation_reason INT(11),
date_created DATE,
voided INT(11),
constraint foreign key(patient_id) references kenyaemr_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
index(patient_id),
index(visit_id),
index(visit_date),
index(date_started_art_at_transferring_facility),
index(arv_status),
index(date_confirmed_hiv_positive),
index(entry_point),
index(transfer_in_date),
index(date_first_enrolled_in_care),
index(entry_point, transfer_in_date, visit_date, patient_id)

);

SELECT "Successfully created etl_hiv_enrollment table";
-- create table etl_hiv_followup

CREATE TABLE kenyaemr_etl.etl_patient_hiv_followup (
uuid CHAR(38),
encounter_id INT(11) NOT NULL PRIMARY KEY,
patient_id INT(11) NOT NULL ,
location_id INT(11) DEFAULT NULL,
visit_date DATE,
visit_id INT(11),
encounter_provider INT(11),
date_created DATE,
visit_scheduled INT(11),
person_present INT(11),
weight DOUBLE,
systolic_pressure DOUBLE,
diastolic_pressure DOUBLE,
height DOUBLE,
temperature DOUBLE,
pulse_rate DOUBLE,
respiratory_rate DOUBLE,
oxygen_saturation DOUBLE,
muac DOUBLE,
nutritional_status INT(11) DEFAULT NULL,
population_type INT(11) DEFAULT NULL,
key_population_type INT(11) DEFAULT NULL,
who_stage INT(11),
presenting_complaints INT(11) DEFAULT NULL,
clinical_notes VARCHAR(600) DEFAULT NULL,
on_anti_tb_drugs INT(11) DEFAULT NULL,
on_ipt INT(11) DEFAULT NULL,
ever_on_ipt INT(11) DEFAULT NULL,
spatum_smear_ordered INT(11) DEFAULT NULL,
chest_xray_ordered INT(11) DEFAULT NULL,
genexpert_ordered INT(11) DEFAULT NULL,
spatum_smear_result INT(11) DEFAULT NULL,
chest_xray_result INT(11) DEFAULT NULL,
genexpert_result INT(11) DEFAULT NULL,
referral INT(11) DEFAULT NULL,
clinical_tb_diagnosis INT(11) DEFAULT NULL,
contact_invitation INT(11) DEFAULT NULL,
evaluated_for_ipt INT(11) DEFAULT NULL,
has_known_allergies INT(11) DEFAULT NULL,
has_chronic_illnesses_cormobidities INT(11) DEFAULT NULL,
has_adverse_drug_reaction INT(11) DEFAULT NULL,
substitution_first_line_regimen_date DATE ,
substitution_first_line_regimen_reason INT(11),
substitution_second_line_regimen_date DATE,
substitution_second_line_regimen_reason INT(11),
second_line_regimen_change_date DATE,
second_line_regimen_change_reason INT(11),
pregnancy_status INT(11),
wants_pregnancy INT(11) DEFAULT NULL,
pregnancy_outcome INT(11),
anc_number VARCHAR(50),
expected_delivery_date DATE,
last_menstrual_period DATE,
gravida INT(11),
parity INT(11),
full_term_pregnancies INT(11),
abortion_miscarriages INT(11),
family_planning_status INT(11),
family_planning_method INT(11),
reason_not_using_family_planning INT(11),
tb_status INT(11),
tb_treatment_no VARCHAR(50),
ctx_adherence INT(11),
ctx_dispensed INT(11),
dapsone_adherence INT(11),
dapsone_dispensed INT(11),
inh_dispensed INT(11),
arv_adherence INT(11),
poor_arv_adherence_reason INT(11),
poor_arv_adherence_reason_other VARCHAR(200),
pwp_disclosure INT(11),
pwp_partner_tested INT(11),
condom_provided INT(11),
screened_for_sti INT(11),
cacx_screening INT(11), 
sti_partner_notification INT(11),
at_risk_population INT(11),
system_review_finding INT(11),
next_appointment_date DATE,
next_appointment_reason INT(11),
stability INT(11),
differentiated_care INT(11),
voided INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(patient_id, visit_date),
INDEX(who_stage),
INDEX(pregnancy_status),
INDEX(pregnancy_outcome),
INDEX(family_planning_status),
INDEX(family_planning_method),
INDEX(tb_status),
INDEX(condom_provided),
INDEX(ctx_dispensed),
INDEX(inh_dispensed),
INDEX(at_risk_population),
INDEX(population_type),
INDEX(key_population_type),
INDEX(on_anti_tb_drugs),
INDEX(on_ipt),
INDEX(ever_on_ipt),
INDEX(differentiated_care),
INDEX(visit_date, patient_id),
INDEX(visit_date, condom_provided),
INDEX(visit_date, family_planning_method)

);

SELECT "Successfully created etl_patient_hiv_followup table";

-- ------- create table etl_laboratory_extract-----------------------------------------
  SELECT "Creating etl_laboratory_extract table";
CREATE TABLE kenyaemr_etl.etl_laboratory_extract (
uuid char(38) PRIMARY KEY,
encounter_id INT(11),
patient_id INT(11) NOT NULL ,
location_id INT(11) DEFAULT NULL,
visit_date DATE,
visit_id INT(11),
order_id VARCHAR(200),
lab_test VARCHAR(180),
urgency VARCHAR(50),
test_result VARCHAR(180),
date_test_requested DATE DEFAULT null,
date_test_result_received DATE,
test_requested_by INT(11),
date_created DATE,
created_by INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(lab_test),
INDEX(test_result)

);
SELECT "Successfully created etl_laboratory_extract table";

-- ------------ create table etl_pharmacy_extract-----------------------


CREATE TABLE kenyaemr_etl.etl_pharmacy_extract(
obs_group_id INT(11) PRIMARY KEY,
uuid char(38),
patient_id INT(11) NOT NULL ,
location_id INT(11) DEFAULT NULL,
visit_date DATE,
visit_id INT(11),
encounter_id INT(11),
encounter_name VARCHAR(100),
drug INT(11),
is_arv INT(11),
is_ctx INT(11),
is_dapsone INT(11),
drug_name VARCHAR(255),
dose INT(11),
unit INT(11),
frequency INT(11),
duration INT(11),
duration_units VARCHAR(20) ,
duration_in_days INT(11),
prescription_provider VARCHAR(50),
dispensing_provider VARCHAR(50),
regimen MEDIUMTEXT,
adverse_effects VARCHAR(100),
date_of_refill DATE,
date_created DATE,
voided INT(11),
date_voided DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(drug),
INDEX(is_arv)

);
SELECT "Successfully created etl_pharmacy_extract table";
-- ------------ create table etl_patient_treatment_discontinuation-----------------------

CREATE TABLE kenyaemr_etl.etl_patient_program_discontinuation(
uuid char(38),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATETIME,
location_id INT(11) DEFAULT NULL,
program_uuid CHAR(38) ,
program_name VARCHAR(50),
encounter_id INT(11) NOT NULL PRIMARY KEY,
discontinuation_reason INT(11),
date_died DATE,
transfer_facility VARCHAR(100),
transfer_date DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(visit_date, program_name, patient_id),
INDEX(visit_date, patient_id),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(discontinuation_reason),
INDEX(date_died),
INDEX(transfer_date)
);
SELECT "Successfully created etl_patient_program_discontinuation table";

-- ------------ create table etl_mch_enrollment-----------------------
  CREATE TABLE kenyaemr_etl.etl_mch_enrollment (
    uuid char(38),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    anc_number VARCHAR(50),
    first_anc_visit_date DATE,
    gravida INT(11),
    parity INT(11),
    parity_abortion INT(11),
    age_at_menarche INT(11),
    lmp DATE,
    lmp_estimated INT(11),
    edd_ultrasound DATE,
    blood_group INT(11),
    serology INT(11),
    tb_screening INT(11),
    bs_for_mps INT(11),
    hiv_status INT(11),
    hiv_test_date DATE,
    partner_hiv_status INT(11),
    partner_hiv_test_date DATE,
    urine_microscopy VARCHAR(100),
    urinary_albumin INT(11),
    glucose_measurement INT(11),
    urine_ph INT(11),
    urine_gravity INT(11),
    urine_nitrite_test INT(11),
    urine_leukocyte_esterace_test INT(11),
    urinary_ketone INT(11),
    urine_bile_salt_test INT(11),
    urine_bile_pigment_test INT(11),
    urine_colour INT(11),
    urine_turbidity INT(11),
    urine_dipstick_for_blood INT(11),
    date_of_discontinuation DATETIME,
    discontinuation_reason INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(tb_screening),
    INDEX(hiv_status),
    INDEX(hiv_test_date),
    INDEX(partner_hiv_status)
  );
  SELECT "Successfully created etl_mch_enrollment table";

  -- ------------ create table etl_mch_antenatal_visit-----------------------

  CREATE TABLE kenyaemr_etl.etl_mch_antenatal_visit (
    uuid char(38),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    provider INT(11),
    anc_visit_number INT(11),
    temperature DOUBLE,
    pulse_rate DOUBLE,
    systolic_bp DOUBLE,
    diastolic_bp DOUBLE,
    respiratory_rate DOUBLE,
    oxygen_saturation INT(11),
    weight DOUBLE,
    height DOUBLE,
    muac DOUBLE,
    hemoglobin DOUBLE,
    breast_exam_done INT(11),
    pallor INT(11),
    maturity INT(11),
    fundal_height DOUBLE,
    fetal_presentation INT(11),
    lie INT(11),
    fetal_heart_rate INT(11),
    fetal_movement INT(11),
    who_stage INT(11),
    cd4 INT(11),
    viral_load INT(11),
    ldl INT(11),
    arv_status INT(11),
    test_1_kit_name VARCHAR(50),
    test_1_kit_lot_no VARCHAR(50) DEFAULT NULL,
    test_1_kit_expiry DATE DEFAULT NULL,
    test_1_result VARCHAR(50) DEFAULT NULL,
    test_2_kit_name VARCHAR(50),
    test_2_kit_lot_no VARCHAR(50) DEFAULT NULL,
    test_2_kit_expiry DATE DEFAULT NULL,
    test_2_result VARCHAR(50) DEFAULT NULL,
    final_test_result VARCHAR(50) DEFAULT NULL,
    patient_given_result VARCHAR(50) DEFAULT NULL,
    partner_hiv_tested INT(11),
    partner_hiv_status INT(11),
    prophylaxis_given INT(11),
    baby_azt_dispensed INT(11),
    baby_nvp_dispensed INT(11),
    TTT VARCHAR(50) DEFAULT NULL,
    IPT_malaria VARCHAR(50) DEFAULT NULL,
    iron_supplement VARCHAR(50) DEFAULT NULL,
    deworming VARCHAR(50) DEFAULT NULL,
    bed_nets VARCHAR(50) DEFAULT NULL,
    urine_microscopy VARCHAR(100),
    urinary_albumin INT(11),
    glucose_measurement INT(11),
    urine_ph INT(11),
    urine_gravity INT(11),
    urine_nitrite_test INT(11),
    urine_leukocyte_esterace_test INT(11),
    urinary_ketone INT(11),
    urine_bile_salt_test INT(11),
    urine_bile_pigment_test INT(11),
    urine_colour INT(11),
    urine_turbidity INT(11),
    urine_dipstick_for_blood INT(11),
    syphilis_test_status INT(11),
    syphilis_treated_status INT(11),
    bs_mps INT(11),
    anc_exercises INT(11),
    tb_screening INT(11),
    cacx_screening INT(11),
    cacx_screening_method INT(11),
    has_other_illnes INT(11),
    counselled INT(11),
    referred_from INT(11),
    referred_to INT(11),
    next_appointment_date DATE,
    clinical_notes VARCHAR(200) DEFAULT NULL,
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(who_stage),
    INDEX(anc_visit_number),
    INDEX(final_test_result),
    INDEX(tb_screening),
    INDEX(syphilis_test_status),
    INDEX(cacx_screening),
    INDEX(next_appointment_date),
    INDEX(arv_status)
  );
  SELECT "Successfully created etl_mch_antenatal_visit table";

  -- ------------ create table etl_mchs_delivery-----------------------

  CREATE TABLE kenyaemr_etl.etl_mchs_delivery (
    uuid char(38),
    provider INT(11),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    date_created DATE,
    admission_number VARCHAR(50),
    duration_of_pregnancy DOUBLE,
    mode_of_delivery INT(11),
    date_of_delivery DATETIME,
    blood_loss INT(11),
    condition_of_mother INT(11),
    apgar_score_1min  DOUBLE,
    apgar_score_5min  DOUBLE,
    apgar_score_10min DOUBLE,
    resuscitation_done INT(11),
    place_of_delivery INT(11),
    delivery_assistant VARCHAR(100),
    counseling_on_infant_feeding  INT(11),
    counseling_on_exclusive_breastfeeding INT(11),
    counseling_on_infant_feeding_for_hiv_infected INT(11),
    mother_decision INT(11),
    placenta_complete INT(11),
    maternal_death_audited INT(11),
    cadre INT(11),
    delivery_complications INT(11),
    coded_delivery_complications INT(11),
    other_delivery_complications VARCHAR(100),
    duration_of_labor INT(11),
    baby_sex INT(11),
    baby_condition INT(11),
    teo_given INT(11),
    birth_weight INT(11),
    bf_within_one_hour INT(11),
    birth_with_deformity INT(11),
    test_1_kit_name VARCHAR(50),
    test_1_kit_lot_no VARCHAR(50) DEFAULT NULL,
    test_1_kit_expiry DATE DEFAULT NULL,
    test_1_result VARCHAR(50) DEFAULT NULL,
    test_2_kit_name VARCHAR(50),
    test_2_kit_lot_no VARCHAR(50) DEFAULT NULL,
    test_2_kit_expiry DATE DEFAULT NULL,
    test_2_result VARCHAR(50) DEFAULT NULL,
    final_test_result VARCHAR(50) DEFAULT NULL,
    patient_given_result VARCHAR(50) DEFAULT NULL,
    partner_hiv_tested INT(11),
    partner_hiv_status INT(11),
    prophylaxis_given INT(11),
    baby_azt_dispensed INT(11),
    baby_nvp_dispensed INT(11),
    clinical_notes VARCHAR(200) DEFAULT NULL,

    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(final_test_result),
    INDEX(baby_sex),
    INDEX( partner_hiv_tested),
    INDEX( partner_hiv_status)

  );
  SELECT "Successfully created etl_mchs_delivery table";

  -- ------------ create table etl_mchs_discharge-----------------------

  CREATE TABLE kenyaemr_etl.etl_mchs_discharge (
    uuid char(38),
    provider INT(11),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    date_created DATE,
    counselled_on_feeding INT(11),
    baby_status INT(11),
    vitamin_A_dispensed INT(11),
    birth_notification_number INT(50),
    condition_of_mother VARCHAR(100),
    discharge_date DATE,
    referred_from INT(11),
    referred_to INT(11),
    clinical_notes VARCHAR(200) DEFAULT NULL,
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(baby_status),
    INDEX(discharge_date)
  );
  SELECT "Successfully created etl_mchs_discharge table";

  -- ------------ create table etl_mch_postnatal_visit-----------------------

  CREATE TABLE kenyaemr_etl.etl_mch_postnatal_visit (
    uuid char(38),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    provider INT(11),
    pnc_register_no VARCHAR(50),
    pnc_visit_no INT(11),
    delivery_date DATE,
    mode_of_delivery INT(11),
    place_of_delivery INT(11),
    temperature DOUBLE,
    pulse_rate DOUBLE,
    systolic_bp DOUBLE,
    diastolic_bp DOUBLE,
    respiratory_rate DOUBLE,
    oxygen_saturation INT(11),
    weight DOUBLE,
    height DOUBLE,
    muac DOUBLE,
    hemoglobin DOUBLE,
    arv_status INT(11),
    general_condition INT(11),
    breast INT(11),
    cs_scar INT(11),
    gravid_uterus INT(11),
    episiotomy INT(11),
    lochia INT(11),
    pallor INT(11),
    pph INT(11),
    mother_hiv_status INT(11),
    condition_of_baby INT(11),
    baby_feeding_method INT(11),
    umblical_cord INT(11),
    baby_immunization_started INT(11),
    family_planning_counseling INT(11),
    uterus_examination VARCHAR(100),
    uterus_cervix_examination VARCHAR(100),
    vaginal_examination VARCHAR(100),
    parametrial_examination VARCHAR(100),
    external_genitalia_examination VARCHAR(100),
    ovarian_examination VARCHAR(100),
    pelvic_lymph_node_exam VARCHAR(100),
    test_1_kit_name VARCHAR(50),
    test_1_kit_lot_no VARCHAR(50) DEFAULT NULL,
    test_1_kit_expiry DATE DEFAULT NULL,
    test_1_result VARCHAR(50) DEFAULT NULL,
    test_2_kit_name VARCHAR(50),
    test_2_kit_lot_no VARCHAR(50) DEFAULT NULL,
    test_2_kit_expiry DATE DEFAULT NULL,
    test_2_result VARCHAR(50) DEFAULT NULL,
    final_test_result VARCHAR(50) DEFAULT NULL,
    patient_given_result VARCHAR(50) DEFAULT NULL,
    partner_hiv_tested INT(11),
    partner_hiv_status INT(11),
    prophylaxis_given INT(11),
    baby_azt_dispensed INT(11),
    baby_nvp_dispensed INT(11),
    pnc_exercises INT(11),
    maternal_condition INT(11),
    iron_supplementation INT(11),
    fistula_screening INT(11),
    cacx_screening INT(11),
    cacx_screening_method INT(11),
    family_planning_status INT(11),
    family_planning_method INT(11),
    referred_from INT(11),
    referred_to INT(11),
    clinical_notes VARCHAR(200) DEFAULT NULL,
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(arv_status),
    INDEX(mother_hiv_status),
    INDEX(arv_status)
  );

  SELECT "Successfully created etl_mch_postnatal_visit table";
  -- ------------ create table etl_hei_enrollment-----------------------

  CREATE TABLE kenyaemr_etl.etl_hei_enrollment (
    serial_no INT(11)NOT NULL AUTO_INCREMENT,
    uuid char(38),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    provider INT(11),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    child_exposed INT(11),
    hei_id_number VARCHAR(50),
    spd_number VARCHAR(50),
    birth_weight DOUBLE,
    gestation_at_birth DOUBLE,
    date_first_seen DATE,
    birth_notification_number VARCHAR(50),
    birth_certificate_number VARCHAR(50),
    need_for_special_care INT(11),
    reason_for_special_care INT(11),
    referral_source INT(11),
    transfer_in INT(11),
    transfer_in_date DATE,
    facility_transferred_from VARCHAR(50),
    district_transferred_from VARCHAR(50),
    date_first_enrolled_in_hei_care DATE,
    arv_prophylaxis INT(11),
    mother_breastfeeding INT(11),
    mother_on_NVP_during_breastfeeding INT(11),
    TB_contact_history_in_household INT(11),
    infant_mother_link INT(11),
    mother_alive INT(11),
    mother_on_pmtct_drugs INT(11),
    mother_on_drug INT(11),
    mother_on_art_at_infant_enrollment INT(11),
    mother_drug_regimen INT(11),
    infant_prophylaxis INT(11),
    parent_ccc_number VARCHAR(50),
    mode_of_delivery INT(11),
    place_of_delivery INT(11),
    birth_length INT(11),
    birth_order INT(11),
    health_facility_name VARCHAR(50),
    date_of_birth_notification DATE,
    date_of_birth_registration DATE,
    birth_registration_place VARCHAR(50),
    permanent_registration_serial VARCHAR(50),
    mother_facility_registered VARCHAR(50),
    exit_date DATE,
    exit_reason INT(11),
    hiv_status_at_exit VARCHAR(50),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(transfer_in),
    INDEX(child_exposed),
    INDEX(need_for_special_care),
    INDEX(reason_for_special_care),
    INDEX(referral_source),
    INDEX(transfer_in),
    INDEX(serial_no)
  );
  SELECT "Successfully created etl_hei_enrollment table";

  -- ------------ create table etl_hei_follow_up_visit-----------------------

  CREATE TABLE kenyaemr_etl.etl_hei_follow_up_visit (
    uuid char(38),
    provider INT(11),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    weight DOUBLE,
    height DOUBLE,
    primary_caregiver INT(11),
    infant_feeding INT(11),
    tb_assessment_outcome INT(11),
    social_smile_milestone INT(11),
    head_control_milestone INT(11),
    response_to_sound_milestone INT(11),
    hand_extension_milestone INT(11),
    sitting_milestone INT(11),
    walking_milestone INT(11),
    standing_milestone INT(11),
    talking_milestone INT(11),
    review_of_systems_developmental INT(11),
    dna_pcr_sample_date DATE,
    dna_pcr_contextual_status INT(11),
    dna_pcr_result INT(11),
    dna_pcr_dbs_sample_code VARCHAR(100),
    dna_pcr_results_date DATE,
    azt_given INT(11),
    nvp_given INT(11),
    ctx_given INT(11),
    first_antibody_sample_date DATE,
    first_antibody_result INT(11),
    first_antibody_dbs_sample_code VARCHAR(100),
    first_antibody_result_date DATE,
    final_antibody_sample_date DATE,
    final_antibody_result INT(11),
    final_antibody_dbs_sample_code VARCHAR(100),
    final_antibody_result_date DATE,
    tetracycline_ointment_given INT(11),
    pupil_examination INT(11),
    sight_examination INT(11),
    squint INT(11),
    deworming_drug INT(11),
    dosage INT(11),
    unit VARCHAR(100),
    next_appointment_date DATE,
    comments VARCHAR(100),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(infant_feeding)
  );
  SELECT "Successfully created etl_hei_follow_up_visit table";

  -- ------- create table etl_hei_immunization table-----------------------------------------
  SELECT "Creating etl_hei_immunization table";
  CREATE TABLE kenyaemr_etl.etl_hei_immunization (
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    patient_id INT(11) NOT NULL ,
    visit_date DATE,
    date_created DATE,
    created_by INT(11),
    BCG VARCHAR(50),
    OPV_birth VARCHAR(50),
    OPV_1 VARCHAR(50),
    OPV_2 VARCHAR(50),
    OPV_3 VARCHAR(50),
    IPV VARCHAR(50),
    DPT_Hep_B_Hib_1 VARCHAR(50),
    DPT_Hep_B_Hib_2 VARCHAR(50),
    DPT_Hep_B_Hib_3 VARCHAR(50),
    PCV_10_1 VARCHAR(50),
    PCV_10_2 VARCHAR(50),
    PCV_10_3 VARCHAR(50),
    ROTA_1 VARCHAR(50),
    ROTA_2 VARCHAR(50),
    Measles_rubella_1 VARCHAR(50),
    Measles_rubella_2 VARCHAR(50),
    Yellow_fever VARCHAR(50),
    Measles_6_months VARCHAR(50),
    VitaminA_6_months VARCHAR(50),
    VitaminA_1_yr VARCHAR(50),
    VitaminA_1_and_half_yr VARCHAR(50),
    VitaminA_2_yr VARCHAR(50),
    VitaminA_2_to_5_yr VARCHAR(50),
    fully_immunized DATE,
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    INDEX(visit_date),
    INDEX(encounter_id)


  );
  SELECT "Successfully created etl_hei_immunization table";

-- ------------ create table etl_tb_enrollment-----------------------

CREATE TABLE kenyaemr_etl.etl_tb_enrollment (
uuid char(38),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
provider INT(11),
date_treatment_started DATE,
district VARCHAR(50),
district_registration_number VARCHAR(20),
referred_by INT(11),
referral_date DATE,
date_transferred_in DATE,
facility_transferred_from VARCHAR(100),
district_transferred_from VARCHAR(100),
date_first_enrolled_in_tb_care DATE,
weight DOUBLE,
height DOUBLE,
treatment_supporter VARCHAR(100),
relation_to_patient INT(11),
treatment_supporter_address VARCHAR(100),
treatment_supporter_phone_contact VARCHAR(100),
disease_classification INT(11),
patient_classification INT(11),
pulmonary_smear_result INT(11),
has_extra_pulmonary_pleurial_effusion INT(11),
has_extra_pulmonary_milliary INT(11),
has_extra_pulmonary_lymph_node INT(11),
has_extra_pulmonary_menengitis INT(11),
has_extra_pulmonary_skeleton INT(11),
has_extra_pulmonary_abdominal INT(11),
has_extra_pulmonary_other VARCHAR(100),
treatment_outcome INT(11),
treatment_outcome_date DATE,
date_of_discontinuation DATETIME,
discontinuation_reason INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(disease_classification),
INDEX(patient_classification),
INDEX(pulmonary_smear_result),
INDEX(date_first_enrolled_in_tb_care)
);

-- ------------ create table etl_tb_follow_up_visit-----------------------

CREATE TABLE kenyaemr_etl.etl_tb_follow_up_visit (
uuid char(38),
provider INT(11),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
spatum_test INT(11),
spatum_result INT(11),
result_serial_number VARCHAR(20),
quantity DOUBLE ,
date_test_done DATE,
bacterial_colonie_growth INT(11),
number_of_colonies DOUBLE,
resistant_s INT(11),
resistant_r INT(11),
resistant_inh INT(11),
resistant_e INT(11),
sensitive_s INT(11),
sensitive_r INT(11),
sensitive_inh INT(11),
sensitive_e INT(11),
test_date DATE,
hiv_status INT(11),
next_appointment_date DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(hiv_status)
);

-- ------------ create table etl_tb_screening-----------------------

CREATE TABLE kenyaemr_etl.etl_tb_screening (
uuid char(38),
provider INT(11),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
cough_for_2wks_or_more INT(11),
confirmed_tb_contact INT(11),
fever_for_2wks_or_more INT(11),
noticeable_weight_loss INT(11),
night_sweat_for_2wks_or_more INT(11),
resulting_tb_status INT(11),
tb_treatment_start_date DATE DEFAULT NULL,
notes VARCHAR(100),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(cough_for_2wks_or_more),
INDEX(confirmed_tb_contact),
INDEX(noticeable_weight_loss),
INDEX(night_sweat_for_2wks_or_more),
INDEX(resulting_tb_status)
);

-- ------------ create table etl_patients_booked_today-----------------------

CREATE TABLE kenyaemr_etl.etl_patients_booked_today(
id INT(11) NOT NULL PRIMARY KEY AUTO_INCREMENT,
patient_id INT(11) NOT NULL ,
last_visit_date DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
INDEX(patient_id)
);

-- ------------ create table etl_missed_appointments-----------------------

CREATE TABLE kenyaemr_etl.etl_missed_appointments(
id INT(11) NOT NULL PRIMARY KEY,
patient_id INT(11) NOT NULL ,
last_tca_date DATE,
last_visit_date DATE,
last_encounter_type VARCHAR(100),
days_since_last_visit INT(11),
date_table_created DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
INDEX(patient_id)
);

-- --------------------------- CREATE drug_event table ---------------------

  CREATE TABLE kenyaemr_etl.etl_drug_event(
    uuid CHAR(38) PRIMARY KEY,
    patient_id INT(11) NOT NULL,
    date_started DATE,
    visit_date DATE,
    provider INT(11),
    encounter_id INT(11) NOT NULL,
    program VARCHAR(50),
    regimen MEDIUMTEXT,
    regimen_name VARCHAR(100),
    regimen_line VARCHAR(50),
    discontinued INT(11),
    regimen_discontinued VARCHAR(255),
    date_discontinued DATE,
    reason_discontinued INT(11),
    reason_discontinued_other VARCHAR(100),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    INDEX(patient_id),
    INDEX(date_started),
    INDEX(date_discontinued),
    INDEX(patient_id, date_started)
  );

-- -------------------------- CREATE hts_test table ---------------------------------

create table kenyaemr_etl.etl_hts_test (
patient_id INT(11) not null,
visit_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL primary key,
encounter_uuid CHAR(38) NOT NULL,
encounter_location INT(11) NOT NULL,
creator INT(11) NOT NULL,
date_created DATE NOT NULL,
visit_date DATE,
test_type INT(11) DEFAULT NULL,
population_type VARCHAR(50),
key_population_type VARCHAR(50),
ever_tested_for_hiv VARCHAR(10),
months_since_last_test INT(11),
patient_disabled VARCHAR(50),
disability_type VARCHAR(50),
patient_consented VARCHAR(50) DEFAULT NULL,
client_tested_as VARCHAR(50),
test_strategy VARCHAR(50),
hts_entry_point VARCHAR(50),
test_1_kit_name VARCHAR(50),
test_1_kit_lot_no VARCHAR(50) DEFAULT NULL,
test_1_kit_expiry DATE DEFAULT NULL,
test_1_result VARCHAR(50) DEFAULT NULL,
test_2_kit_name VARCHAR(50),
test_2_kit_lot_no VARCHAR(50) DEFAULT NULL,
test_2_kit_expiry DATE DEFAULT NULL,
test_2_result VARCHAR(50) DEFAULT NULL,
final_test_result VARCHAR(50) DEFAULT NULL,
patient_given_result VARCHAR(50) DEFAULT NULL,
couple_discordant VARCHAR(100) DEFAULT NULL,
tb_screening VARCHAR(20) DEFAULT NULL,
patient_had_hiv_self_test VARCHAR(50) DEFAULT NULL,
remarks VARCHAR(255) DEFAULT NULL,
voided INT(11),
index(patient_id),
index(visit_id),
index(tb_screening),
index(visit_date),
index(population_type),
index(test_type),
index(final_test_result),
index(couple_discordant),
index(test_1_kit_name),
index(test_2_kit_name)
);

-- ------------- CREATE HTS LINKAGE AND REFERRALS ------------------------

CREATE TABLE kenyaemr_etl.etl_hts_referral_and_linkage (
patient_id INT(11) not null,
visit_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL primary key,
encounter_uuid CHAR(38) NOT NULL,
encounter_location INT(11) NOT NULL,
creator INT(11) NOT NULL,
date_created DATE NOT NULL,
visit_date DATE,
tracing_type VARCHAR(50),
tracing_status VARCHAR(100),
ccc_number VARCHAR(100),
facility_linked_to VARCHAR(100),
enrollment_date DATE,
art_start_date DATE,
provider_handed_to VARCHAR(100),
voided INT(11),
index(patient_id),
index(visit_date),
index(tracing_type),
index(tracing_status)
);


-- -------------- create referral form ----------------------------

CREATE TABLE kenyaemr_etl.etl_hts_referral (
patient_id INT(11) not null,
visit_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL primary key,
encounter_uuid CHAR(38) NOT NULL,
encounter_location INT(11) NOT NULL,
creator INT(11) NOT NULL,
date_created DATE NOT NULL,
visit_date DATE,
facility_referred_to VARCHAR(50),
date_to_enrol DATE DEFAULT NULL,
remarks VARCHAR(100),
voided INT(11),
index(patient_id),
index(visit_date)
);


-- ------------ create table etl_ipt_screening-----------------------

CREATE TABLE kenyaemr_etl.etl_ipt_screening (
uuid char(38),
provider INT(11),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
yellow_urine INT(11),
numbness INT(11),
yellow_eyes INT(11),
abdominal_tenderness INT(11),
ipt_started INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(patient_id),
INDEX(visit_date, ipt_started, patient_id),
INDEX(ipt_started, visit_date),
INDEX(encounter_id),
INDEX(ipt_started)
);

-- ------------ create table etl_ipt_follow_up -----------------------
CREATE TABLE kenyaemr_etl.etl_ipt_follow_up (
uuid char(38),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
provider INT(11),
date_created DATE NOT NULL,
ipt_due_date DATE DEFAULT NULL,
date_collected_ipt DATE DEFAULT NULL,
weight DOUBLE,
hepatotoxity VARCHAR(100) DEFAULT NULL,
peripheral_neuropathy VARCHAR(100) DEFAULT NULL ,
rash VARCHAR(100),
adherence VARCHAR(100),
action_taken VARCHAR(100),
voided INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(hepatotoxity),
INDEX(peripheral_neuropathy),
INDEX(rash),
INDEX(adherence)
);

CREATE TABLE kenyaemr_etl.etl_ccc_defaulter_tracing (
uuid char(38),
provider INT(11),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
tracing_type INT(11),
tracing_outcome INT(11),
attempt_number INT(11),
is_final_trace INT(11) ,
true_status INT(11),
cause_of_death INT(11),
comments VARCHAR(100),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(true_status),
INDEX(cause_of_death),
INDEX(tracing_type)
);

-- ------------ create table etl_ART_preparation-----------------------
CREATE TABLE kenyaemr_etl.etl_ART_preparation (
  uuid char(38),
  patient_id INT(11) NOT NULL ,
  visit_id INT(11),
  visit_date DATE,
  location_id INT(11) DEFAULT NULL,
  encounter_id INT(11) NOT NULL PRIMARY KEY,
  provider INT(11),
  understands_hiv_art_benefits varchar(10),
  screened_negative_substance_abuse varchar(10),
  screened_negative_psychiatric_illness varchar(10),
  HIV_status_disclosure varchar(10),
  trained_drug_admin varchar(10),
  informed_drug_side_effects varchar(10),
  caregiver_committed varchar(10),
  adherance_barriers_identified varchar(10),
  caregiver_location_contacts_known varchar(10),
  ready_to_start_art varchar(10),
  identified_drug_time varchar(10),
  treatment_supporter_engaged varchar(10),
  support_grp_meeting_awareness varchar(10),
  enrolled_in_reminder_system varchar(10),
  other_support_systems varchar(10),
  CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
  CONSTRAINT unique_uuid UNIQUE(uuid),
  INDEX(visit_date),
  INDEX(encounter_id),
  INDEX(ready_to_start_art)
);
SELECT "Successfully created etl_ART_preparation table";

  -- ------------ create table etl_enhanced_adherence-----------------------
  CREATE TABLE kenyaemr_etl.etl_enhanced_adherence (
    uuid char(38),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    provider INT(11),
    session_number INT(11),
    first_session_date DATE,
    pill_count INT(11),
    arv_adherence varchar(50),
    has_vl_results varchar(10),
    vl_results_suppressed varchar(10),
    vl_results_feeling varchar(255),
    cause_of_high_vl varchar(255),
    way_forward varchar(255),
    patient_hiv_knowledge varchar(255),
    patient_drugs_uptake varchar(255),
    patient_drugs_reminder_tools varchar(255),
    patient_drugs_uptake_during_travels varchar(255),
    patient_drugs_side_effects_response varchar(255),
    patient_drugs_uptake_most_difficult_times varchar(255),
    patient_drugs_daily_uptake_feeling varchar(255),
    patient_ambitions varchar(255),
    patient_has_people_to_talk varchar(10),
    patient_enlisting_social_support varchar(255),
    patient_income_sources varchar(255),
    patient_challenges_reaching_clinic varchar(10),
    patient_worried_of_accidental_disclosure varchar(10),
    patient_treated_differently varchar(10),
    stigma_hinders_adherence varchar(10),
    patient_tried_faith_healing varchar(10),
    patient_adherence_improved varchar(10),
    patient_doses_missed varchar(10),
    review_and_barriers_to_adherence varchar(255),
    other_referrals varchar(10),
    appointments_honoured varchar(10),
    referral_experience varchar(255),
    home_visit_benefit varchar(10),
    adherence_plan varchar(255),
    next_appointment_date DATE,
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id)
    );
  SELECT "Successfully created etl_enhanced_adherence table";

  -- ------------ create table etl_patient_triage-----------------------
  CREATE TABLE kenyaemr_etl.etl_patient_triage (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    patient_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    visit_reason VARCHAR(255),
    weight DOUBLE,
    height DOUBLE,
    systolic_pressure DOUBLE,
    diastolic_pressure DOUBLE,
    temperature DOUBLE,
    pulse_rate DOUBLE,
    respiratory_rate DOUBLE,
    oxygen_saturation DOUBLE,
    muac DOUBLE,
    nutritional_status INT(11) DEFAULT NULL,
    last_menstrual_period DATE,
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(patient_id, visit_date)
  );

  SELECT "Successfully created etl_patient_triage table";

  -- ------------ create table etl_prep_behaviour_risk_assessment-----------------------

  CREATE TABLE kenyaemr_etl.etl_prep_behaviour_risk_assessment (
    uuid char(38),
    provider INT(11),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    date_created DATE,
    sexual_partner_hiv_status varchar(255),
    sexual_partner_on_art varchar(10),
    risk varchar(255),
    high_risk_partner varchar(10),
    sex_with_multiple_partners varchar(10),
    ipv_gbv varchar(10),
    transactional_sex varchar(10),
    recent_sti_infected varchar(10),
    recurrent_pep_use varchar(10),
    recurrent_sex_under_influence varchar(10),
    inconsistent_no_condom_use varchar(10),
    sharing_drug_needles varchar(255),
    assessment_outcome varchar(255),
    risk_education_offered varchar(10),
    risk_reduction varchar(10),
    willing_to_take_prep varchar(10),
    reason_not_willing varchar(255),
    risk_edu_offered varchar(10),
    risk_education varchar(255),
    referral_for_prevention_services varchar(255),
    referral_facility VARCHAR(255),
    time_partner_hiv_positive_known varchar(255),
    partner_enrolled_ccc varchar(255),
    partner_ccc_number varchar(255),
    partner_art_start_date DATE,
    serodiscordant_confirmation_date DATE,
    recent_unprotected_sex_with_positive_partner varchar(10),
    children_with_hiv_positive_partner varchar(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id)
  );
  SELECT "Successfully created etl_prep_behaviour_risk_assessment table";

  -- ------------ create table etl_prep_monthly_refill-----------------------

  CREATE TABLE kenyaemr_etl.etl_prep_monthly_refill (
    uuid char(38),
    provider INT(11),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    date_created DATE,
    risk_for_hiv_positive_partner  varchar(255),
    client_assessment  varchar(255),
    adherence_assessment varchar(255),
    poor_adherence_reasons varchar(255),
    other_poor_adherence_reasons varchar(255),
    adherence_counselling_done varchar(10),
    prep_status varchar(255),
    prescribed_prep_today varchar(10),
    prescribed_regimen varchar(10),
    prescribed_regimen_months varchar(10),
    prep_discontinue_reasons varchar(255),
    prep_discontinue_other_reasons varchar(255),
    appointment_given varchar(10),
    next_appointment DATE,
    remarks varchar(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id)

  );
  SELECT "Successfully created etl_prep_monthly_refill table";

-- ------------ create table etl_prep_discontinuation-----------------------

  CREATE TABLE kenyaemr_etl.etl_prep_discontinuation (
    uuid char(38),
    provider INT(11),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    date_created DATE,
    discontinue_reason VARCHAR(255),
    care_end_date DATE,
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(discontinue_reason),
    INDEX(care_end_date)

  );
  SELECT "Successfully created etl_prep_discontinuation table";

  -- ------------ create table etl_prep_enrollment-----------------------
  CREATE TABLE kenyaemr_etl.etl_prep_enrolment (
    uuid char(38),
    provider INT(11),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    date_created DATE,
    patient_type VARCHAR(255),
    transfer_in_entry_point VARCHAR(255),
    referred_from VARCHAR(255),
    transit_from VARCHAR(255),
    transfer_in_date DATE,
    transfer_from VARCHAR(255),
    initial_enrolment_date DATE,
    date_started_prep_trf_facility DATE,
    previously_on_prep VARCHAR(10),
    regimen VARCHAR(255),
    prep_last_date DATE,
    in_school VARCHAR(10),
    buddy_name VARCHAR(255),
    buddy_alias VARCHAR(255),
    buddy_relationship VARCHAR(255),
    buddy_phone VARCHAR(255),
    buddy_alt_phone VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id)

  );
  SELECT "Successfully created etl_prep_enrollment table";

   -- ------------ create table etl_prep_followup-----------------------

  CREATE TABLE kenyaemr_etl.etl_prep_followup (
    uuid char(38),
    provider INT(11),
    patient_id INT(11) NOT NULL,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    date_created DATE,
    sti_screened VARCHAR(10),
    genital_ulcer_desease VARCHAR(255),
    vaginal_discharge VARCHAR(255),
    cervical_discharge VARCHAR(255),
    pid VARCHAR(255),
    urethral_discharge VARCHAR(255),
    anal_discharge VARCHAR(255),
    other_sti_symptoms VARCHAR(255),
    sti_treated VARCHAR(10),
    vmmc_screened VARCHAR(10),
    vmmc_status VARCHAR(255),
    vmmc_referred VARCHAR(255),
    lmp DATE,
    pregnant VARCHAR(10),
    edd DATE,
    planned_pregnancy VARCHAR(10),
    wanted_pregnancy VARCHAR(10),
    breastfeeding VARCHAR(10),
    fp_status VARCHAR(255),
    fp_method VARCHAR(255),
    ended_pregnancy VARCHAR(255),
    pregnancy_outcome VARCHAR(10),
    outcome_date DATE,
    defects VARCHAR(10),
    has_chronic_illness VARCHAR(10),
    chronic_illness VARCHAR(255),
    chronic_illness_onset_date DATE,
    chronic_illness_drug VARCHAR(255),
    chronic_illness_dose VARCHAR(255),
    chronic_illness_units VARCHAR(255),
    chronic_illness_frequency VARCHAR(255),
    chronic_illness_duration VARCHAR(255),
    chronic_illness_duration_units VARCHAR(255),
    adverse_reactions VARCHAR(255),
    medicine_reactions VARCHAR(255),
    reaction VARCHAR(255),
    severity VARCHAR(255),
    action_taken VARCHAR(255),
    known_allergies VARCHAR(10),
    allergen VARCHAR(255),
    allergy_reaction VARCHAR(255),
    allergy_severity VARCHAR(255),
    allergy_date DATE,
    hiv_signs VARCHAR(10),
    adherence_counselled VARCHAR(10),
    prep_contraindicatios VARCHAR(255),
    treatment_plan VARCHAR(255),
    condoms_issued VARCHAR(10),
    number_of_condoms VARCHAR(10),
    appointment_given VARCHAR(10),
    appointment_date DATE,
    reason_no_appointment VARCHAR(255),
    clinical_notes VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id)
  );
  SELECT "Successfully created etl_prep_followup table";

   -- ------------ create table etl_progress_note-----------------------

  CREATE TABLE kenyaemr_etl.etl_progress_note (
    uuid char(38),
    provider INT(11),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    date_created DATE,
    notes VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id)

  );
  SELECT "Successfully created etl_progress_note table";

-- ------------ create table etl_ipt_initiation -----------------------
  CREATE TABLE kenyaemr_etl.etl_ipt_initiation (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    patient_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    encounter_provider INT(11),
    date_created DATE,
    ipt_indication INT(11),
    sub_county_reg_number VARCHAR(255),
    sub_county_reg_date DATE,
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(patient_id, visit_date)
  );

  SELECT "Successfully created etl_ipt_initiation table";
  -- ------------------- creating ipt followup table --------------------------

  /*CREATE TABLE kenyaemr_etl.etl_ipt_followup (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    patient_id INT(11) NOT NULL ,
    visit_id INT(11) DEFAULT NULL,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    encounter_provider INT(11),
    date_created DATE,
    ipt_due_date DATE,
    date_collected_ipt DATE,
    has_hepatoxicity INT(11),
    has_rash INT(11),
    has_peripheral_neuropathy INT(11),
    adherence INT(11),
    action_taken VARCHAR(100),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(adherence)
  );

  SELECT "Successfully created etl_ipt_followup table";
*/
  -- --------------------- creating ipt outcome table -------------------------------
  CREATE TABLE kenyaemr_etl.etl_ipt_outcome (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    patient_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    encounter_provider INT(11),
    date_created DATE,
    outcome INT(11),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(outcome),
    INDEX(patient_id, visit_date)
  );

  SELECT "Successfully created etl_ipt_outcome table";

  -- --------------------- creating hts tracing table -------------------------------
  CREATE TABLE kenyaemr_etl.etl_hts_linkage_tracing (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    patient_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    encounter_provider INT(11),
    date_created DATE,
    tracing_type INT(11),
    tracing_outcome INT(11),
    reason_not_contacted INT(11),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(tracing_type),
    INDEX(tracing_outcome),
    INDEX(reason_not_contacted),
    INDEX(patient_id, visit_date)
  );

  SELECT "Successfully created etl_hts_linkage_tracing table";

-- ------------------------ create patient program table ---------------------

CREATE TABLE kenyaemr_etl.etl_patient_program (
    uuid CHAR(38) NOT NULL PRIMARY KEY,
    patient_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    program VARCHAR(100) NOT NULL,
    date_enrolled DATE NOT NULL,
    date_completed DATE DEFAULT NULL,
    outcome INT(11),
    date_created DATE,
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(date_enrolled),
    INDEX(date_completed),
    INDEX(patient_id),
    INDEX(outcome)
  );

  -- ------------------------ create person address table ---------------------

  CREATE TABLE kenyaemr_etl.etl_person_address (
    uuid CHAR(38) NOT NULL PRIMARY KEY,
    patient_id INT(11) NOT NULL ,
    county VARCHAR(100) DEFAULT NULL,
    sub_county VARCHAR(100) DEFAULT NULL,
    location VARCHAR(100) DEFAULT NULL,
    ward VARCHAR(100) DEFAULT NULL,
    sub_location VARCHAR(100) DEFAULT NULL,
    village VARCHAR(100) DEFAULT NULL,
    postal_address VARCHAR(100) DEFAULT NULL,
    land_mark VARCHAR(100) DEFAULT NULL,
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(patient_id)
  );

   -- --------------------- creating OTZ activity table -------------------------------

  CREATE TABLE kenyaemr_etl.etl_otz_activity (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    patient_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    encounter_provider INT(11),
    date_created DATE,
    orientation VARCHAR(11) DEFAULT NULL,
    leadership VARCHAR(11) DEFAULT NULL,
    participation VARCHAR(11) DEFAULT NULL,
    treatment_literacy VARCHAR(11) DEFAULT NULL,
    transition_to_adult_care VARCHAR(11) DEFAULT NULL,
    making_decision_future VARCHAR(11) DEFAULT NULL,
    srh VARCHAR(11) DEFAULT NULL,
    beyond_third_ninety VARCHAR(11) DEFAULT NULL,
    attended_support_group VARCHAR(11) DEFAULT NULL,
    remarks VARCHAR(255) DEFAULT NULL,
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(patient_id, visit_date)
  );

  SELECT "Successfully created etl_otz_activity table";


   -- --------------------- creating OTZ enrollment table -------------------------------

  CREATE TABLE kenyaemr_etl.etl_otz_enrollment (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    patient_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    encounter_provider INT(11),
    date_created DATE,
    orientation VARCHAR(11) DEFAULT NULL,
    leadership VARCHAR(11) DEFAULT NULL,
    participation VARCHAR(11) DEFAULT NULL,
    treatment_literacy VARCHAR(11) DEFAULT NULL,
    transition_to_adult_care VARCHAR(11) DEFAULT NULL,
    making_decision_future VARCHAR(11) DEFAULT NULL,
    srh VARCHAR(11) DEFAULT NULL,
    beyond_third_ninety VARCHAR(11) DEFAULT NULL,
    transfer_in VARCHAR(11) DEFAULT NULL,
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(patient_id, visit_date)
  );

  SELECT "Successfully created etl_otz_enrollment table";

   -- --------------------- creating OVC enrollment table -------------------------------
  CREATE TABLE kenyaemr_etl.etl_ovc_enrolment (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    patient_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    encounter_provider INT(11),
    date_created DATE,
    caregiver_enrolled_here VARCHAR(11) DEFAULT NULL,
    caregiver_name VARCHAR(11) DEFAULT NULL,
    caregiver_gender VARCHAR(255) DEFAULT NULL,
    relationship_to_client VARCHAR(255) DEFAULT NULL,
    caregiver_phone_number VARCHAR(255) DEFAULT NULL,
    client_enrolled_cpims VARCHAR(11) DEFAULT NULL,
    partner_offering_ovc VARCHAR(255) DEFAULT NULL,
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(patient_id, visit_date)
  );

  SELECT "Successfully created etl_ovc_enrolment table";

       -- --------------------- creating Cervical cancer screening table -------------------------------
  CREATE TABLE kenyaemr_etl.etl_cervical_cancer_screening (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    encounter_provider INT(11),
    patient_id INT(11) NOT NULL,
    visit_id INT(11) DEFAULT NULL,
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    date_created DATE,
    screening_number INT(11),
    screening_method VARCHAR(255) DEFAULT NULL,
    screening_result VARCHAR(255) DEFAULT NULL,
    previous_screening_method VARCHAR(255) DEFAULT NULL,
    previous_screening_date DATE,
    previous_screening_result VARCHAR(255) DEFAULT NULL,
    encounter_type VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(screening_number),
    INDEX(patient_id),
    INDEX(patient_id, visit_date)
  );
  SELECT "Successfully created etl_cervical_cancer_screening table";

  -- --------------------- creating patient contact  table -------------------------------
  CREATE TABLE kenyaemr_etl.etl_patient_contact (
    id                     INT(11),
    uuid                   CHAR(38),
    date_created           DATE,
    first_name             VARCHAR(255),
    middle_name            VARCHAR(255),
    last_name              VARCHAR(255),
    sex                    VARCHAR(50),
    birth_date             DATETIME,
    physical_address       VARCHAR(255),
    phone_contact          VARCHAR(255),
    patient_related_to     INT(11),
    patient_id             INT(11),
    relationship_type      INT(11),
    appointment_date       DATETIME,
    baseline_hiv_status    VARCHAR(255),
    ipv_outcome            VARCHAR(255),
    marital_status         VARCHAR(100),
    living_with_patient    VARCHAR(100),
    pns_approach           VARCHAR(100),
    contact_listing_decline_reason   VARCHAR(255),
    consented_contact_listing   VARCHAR(100),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_related_to) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(date_created),
    INDEX(id),
    INDEX(id, date_created)
  );

  SELECT "Successfully created etl_patient_contact table";

  -- --------------------- creating client trace  table -------------------------------
  CREATE TABLE kenyaemr_etl.etl_client_trace (
    id                     INT(11),
    uuid                   CHAR(38),
    date_created           DATE,
    encounter_date         DATETIME,
    client_id              INT(11),
    contact_type           VARCHAR(255),
    status                 VARCHAR(255),
    unique_patient_no      VARCHAR(255),
    facility_linked_to     VARCHAR(255),
    health_worker_handed_to    VARCHAR(255),
    remarks                VARCHAR(255),
    appointment_date       DATETIME,
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kenyaemr_etl.etl_patient_contact(id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(date_created),
    INDEX(id),
    INDEX(id, date_created)
  );

  SELECT "Successfully created etl_client_trace table";


 -- --------------------- creating Viral Load table -------------------------------
  CREATE TABLE kenyaemr_etl.etl_viral_load (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    patient_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    order_date DATE ,
    date_of_result DATE ,
    order_reason VARCHAR(255) DEFAULT NULL ,
    previous_vl_result VARCHAR(50) DEFAULT NULL,
    current_vl_result VARCHAR(50) DEFAULT NULL,
    previous_vl_date DATE,
    previous_vl_reason VARCHAR(255) DEFAULT NULL,
    vl_months_since_hiv_enrollment INT(11) DEFAULT NULL,
    vl_months_since_otz_enrollment INT(11) DEFAULT NULL,
    eligibility VARCHAR(50) DEFAULT NULL,
    date_created DATE,
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(patient_id, visit_date)
  );

  SELECT "Successfully created etl_viral_load table";

    -- create table etl_client_registration
    create table kenyaemr_etl.etl_client_registration (
      client_id INT(11) not null primary key,
      registration_date DATE,
      given_name VARCHAR(255),
      middle_name VARCHAR(255),
      family_name VARCHAR(255),
      Gender VARCHAR(10),
      DOB DATE,
      alias_name VARCHAR(255),
      postal_address VARCHAR (255),
      county VARCHAR (255),
      sub_county VARCHAR (255),
      location VARCHAR (255),
      sub_location VARCHAR (255),
      village VARCHAR (255),
      phone_number VARCHAR (255)  DEFAULT NULL,
      alt_phone_number VARCHAR (255)  DEFAULT NULL,
      email_address VARCHAR (255)  DEFAULT NULL,
      national_id_number VARCHAR(50),
      passport_number VARCHAR(50)  DEFAULT NULL,
      dead INT(11),
      death_date DATE DEFAULT NULL,
      voided INT(11),
      index(client_id),
      index(Gender),
      index(registration_date),
      index(DOB)
    );

    SELECT "Successfully created etl_client_registration table";

    -- create table etl_contact
    create table kenyaemr_etl.etl_contact (
      uuid char(38) ,
      unique_identifier VARCHAR(50),
      client_id INT(11) NOT NULL,
      visit_id INT(11) DEFAULT NULL,
      visit_date DATE,
      location_id INT(11) DEFAULT NULL,
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      encounter_provider INT(11),
      date_created DATE,
      key_population_type VARCHAR(255),
      contacted_by_peducator VARCHAR(10),
      program_name VARCHAR(255),
      frequent_hotspot_name VARCHAR(255),
      frequent_hotspot_type VARCHAR(255),
      year_started_sex_work VARCHAR(10),
      year_started_sex_with_men VARCHAR(10),
      year_started_drugs VARCHAR(10),
      avg_weekly_sex_acts int(11),
      avg_weekly_anal_sex_acts int(11),
      avg_daily_drug_injections int(11),
      contact_person_name VARCHAR(255),
      contact_person_alias VARCHAR(255),
      contact_person_phone VARCHAR(255),
      voided INT(11),
      constraint foreign key(client_id) references kenyaemr_etl.etl_client_registration(client_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      index(client_id),
      index(unique_identifier),
      index(key_population_type)
    );

    SELECT "Successfully created etl_contact table";

    -- create table etl_client_enrollment

    create table kenyaemr_etl.etl_client_enrollment (
      uuid char(38) ,
      client_id INT(11) NOT NULL,
      visit_id INT(11) DEFAULT NULL,
      visit_date DATE,
      location_id INT(11) DEFAULT NULL,
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      encounter_provider INT(11),
      date_created DATE,
      contacted_for_prevention VARCHAR(10),
      has_regular_free_sex_partner VARCHAR(10),
      year_started_sex_work VARCHAR(10),
      year_started_sex_with_men VARCHAR(10),
      year_started_drugs VARCHAR(10),
      has_expereienced_sexual_violence VARCHAR(10),
      has_expereienced_physical_violence VARCHAR(10),
      ever_tested_for_hiv VARCHAR(10),
      test_type VARCHAR(255),
      share_test_results VARCHAR(100),
      willing_to_test VARCHAR(10),
      test_decline_reason VARCHAR(255),
      receiving_hiv_care VARCHAR(10),
      care_facility_name VARCHAR(100),
      ccc_number VARCHAR(100),
      vl_test_done VARCHAR(10),
      vl_results_date DATE,
      contact_for_appointment VARCHAR(10),
      contact_method VARCHAR(255),
      buddy_name VARCHAR(255),
      buddy_phone_number VARCHAR(255),
      voided INT(11),
      constraint foreign key(client_id) references kenyaemr_etl.etl_client_registration(client_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      index(client_id)
    );
    SELECT "Successfully created etl_client_enrollment table";

    -- create table etl_clinical_visit

    create table kenyaemr_etl.etl_clinical_visit (
      uuid char(38) ,
      client_id INT(11) NOT NULL,
      visit_id INT(11) DEFAULT NULL,
      visit_date DATE,
      location_id INT(11) DEFAULT NULL,
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      encounter_provider INT(11),
      date_created DATE,
      implementing_partner VARCHAR(255),
      type_of_visit VARCHAR(255),
      visit_reason VARCHAR(255),
      service_delivery_model VARCHAR(255),
      sti_screened VARCHAR(10),
      sti_results VARCHAR(255),
      sti_treated VARCHAR(10),
      sti_referred VARCHAR(10),
      sti_referred_text VARCHAR(255),
      tb_screened VARCHAR(10),
      tb_results VARCHAR(255),
      tb_treated VARCHAR(10),
      tb_referred VARCHAR(10),
      tb_referred_text VARCHAR(255),
      hepatitisB_screened VARCHAR(10),
      hepatitisB_results VARCHAR(255),
      hepatitisB_treated VARCHAR(10),
      hepatitisB_referred VARCHAR(10),
      hepatitisB_text VARCHAR(255),
      hepatitisC_screened VARCHAR(10),
      hepatitisC_results VARCHAR(255),
      hepatitisC_treated VARCHAR(10),
      hepatitisC_referred VARCHAR(10),
      hepatitisC_text VARCHAR(255),
      overdose_screened VARCHAR(10),
      overdose_results VARCHAR(255),
      overdose_treated VARCHAR(10),
      received_naloxone VARCHAR(10),
      overdose_referred VARCHAR(10),
      overdose_text VARCHAR(255),
      abscess_screened VARCHAR(10),
      abscess_results VARCHAR(255),
      abscess_treated VARCHAR(10),
      abscess_referred VARCHAR(10),
      abscess_text VARCHAR(255),
      alcohol_screened VARCHAR(10),
      alcohol_results VARCHAR(255),
      alcohol_treated VARCHAR(10),
      alcohol_referred VARCHAR(10),
      alcohol_text VARCHAR(255),
      cerv_cancer_screened VARCHAR(10),
      cerv_cancer_results VARCHAR(255),
      cerv_cancer_treated VARCHAR(10),
      cerv_cancer_referred VARCHAR(10),
      cerv_cancer_text VARCHAR(255),
      prep_screened VARCHAR(10),
      prep_results VARCHAR(255),
      prep_treated VARCHAR(10),
      prep_referred VARCHAR(10),
      prep_text VARCHAR(255),
      violence_screened VARCHAR(10),
      violence_results VARCHAR(255),
      violence_treated VARCHAR(10),
      violence_referred VARCHAR(10),
      violence_text VARCHAR(255),
      risk_red_counselling_screened VARCHAR(10),
      risk_red_counselling_eligibility VARCHAR(255),
      risk_red_counselling_support VARCHAR(10),
      risk_red_counselling_ebi_provided VARCHAR(10),
      risk_red_counselling_text VARCHAR(255),
      fp_screened VARCHAR(10),
      fp_eligibility VARCHAR(255),
      fp_treated VARCHAR(10),
      fp_referred VARCHAR(10),
      fp_text VARCHAR(255),
      mental_health_screened VARCHAR(10),
      mental_health_results VARCHAR(255),
      mental_health_support VARCHAR(100),
      mental_health_referred VARCHAR(10),
      mental_health_text VARCHAR(255),
      hiv_self_rep_status VARCHAR(50),
      last_hiv_test_setting VARCHAR(100),
      counselled_for_hiv VARCHAR(10),
      hiv_tested VARCHAR(10),
      test_frequency VARCHAR(100),
      received_results VARCHAR(10),
      test_results VARCHAR(100),
      linked_to_art VARCHAR(10),
      facility_linked_to VARCHAR(10),
      self_test_education VARCHAR(10),
      self_test_kits_given VARCHAR(100),
      self_use_kits VARCHAR (10),
      distribution_kits VARCHAR (10),
      self_tested VARCHAR(10),
      self_test_date DATE,
      self_test_frequency VARCHAR(100),
      self_test_results VARCHAR(100),
      test_confirmatory_results VARCHAR(100),
      confirmatory_facility VARCHAR(100),
      offsite_confirmatory_facility VARCHAR(100),
      self_test_linked_art VARCHAR(10),
      self_test_link_facility VARCHAR(255),
      hiv_care_facility VARCHAR(255),
      other_hiv_care_facility VARCHAR(255),
      initiated_art_this_month VARCHAR(10),
      active_art VARCHAR(10),
      eligible_vl VARCHAR(50),
      vl_test_done VARCHAR(100),
      vl_results VARCHAR(100),
      received_vl_results VARCHAR(100),
      condom_use_education VARCHAR(10),
      post_abortal_care VARCHAR(10),
      linked_to_psychosocial VARCHAR(10),
      male_condoms_no VARCHAR(10),
      female_condoms_no VARCHAR(10),
      lubes_no VARCHAR(10),
      syringes_needles_no VARCHAR(10),
      pep_eligible VARCHAR(10),
      exposure_type VARCHAR(100),
      other_exposure_type VARCHAR(100),
      clinical_notes VARCHAR(255),
      appointment_date DATE,
      voided INT(11),
      constraint foreign key(client_id) references kenyaemr_etl.etl_client_registration(client_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      index(client_id),
      index(client_id,visit_date)
    );
    SELECT "Successfully created etl_clinical_visit table";

    -- ------------ create table etl_peer_calendar-----------------------
    CREATE TABLE kenyaemr_etl.etl_peer_calendar (
      uuid CHAR(38),
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      client_id INT(11) NOT NULL ,
      location_id INT(11) DEFAULT NULL,
      visit_date DATE,
      visit_id INT(11),
      encounter_provider INT(11),
      date_created DATE,
      hotspot_name VARCHAR(255),
      typology VARCHAR(255),
      other_hotspots VARCHAR(255),
      weekly_sex_acts INT(10),
      monthly_condoms_required INT(10),
      weekly_anal_sex_acts INT(10),
      monthly_lubes_required INT(10),
      daily_injections INT(10),
      monthly_syringes_required INT(10),
      years_in_sexwork_drugs INT(10),
      experienced_violence VARCHAR(10),
      service_provided_within_last_month VARCHAR(255),
      monthly_n_and_s_distributed  INT(10),
      monthly_male_condoms_distributed  INT(10),
      monthly_lubes_distributed  INT(10),
      monthly_female_condoms_distributed  INT(10),
      monthly_self_test_kits_distributed INT(10),
      received_clinical_service VARCHAR(10),
      violence_reported VARCHAR(10),
      referred  VARCHAR(10),
      health_edu  VARCHAR(10),
      remarks VARCHAR(255),
      voided INT(11),
      CONSTRAINT FOREIGN KEY (client_id) REFERENCES kenyaemr_etl.etl_client_registration(client_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      INDEX(visit_date),
      INDEX(client_id, visit_date)
    );

    SELECT "Successfully created etl_peer_calendar table";

        -- ------------ create table etl_sti_treatment-----------------------
    CREATE TABLE kenyaemr_etl.etl_sti_treatment (
      uuid CHAR(38),
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      client_id INT(11) NOT NULL ,
      location_id INT(11) DEFAULT NULL,
      visit_date DATE,
      visit_id INT(11),
      encounter_provider INT(11),
      date_created DATE,
      visit_reason VARCHAR(255),
      syndrome VARCHAR(10),
      other_syndrome VARCHAR(255),
      drug_prescription VARCHAR(10),
      other_drug_prescription VARCHAR(255),
      genital_exam_done VARCHAR(10),
      lab_referral VARCHAR(10),
      lab_form_number VARCHAR(100),
      referred_to_facility VARCHAR(10),
      facility_name VARCHAR(255),
      partner_referral_done VARCHAR(10),
      given_lubes VARCHAR(10),
      no_of_lubes INT(10),
      given_condoms VARCHAR(10),
      no_of_condoms INT(10),
      provider_comments VARCHAR(255),
      provider_name VARCHAR(255),
      appointment_date DATE,
      voided INT(11),
      CONSTRAINT FOREIGN KEY (client_id) REFERENCES kenyaemr_etl.etl_client_registration(client_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      INDEX(visit_date),
      INDEX(encounter_id),
      INDEX(client_id),
      INDEX(visit_reason),
      INDEX(given_lubes),
      INDEX(given_condoms)
    );

  UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= script_id;

END$$






