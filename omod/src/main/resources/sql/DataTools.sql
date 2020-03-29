DROP PROCEDURE IF EXISTS create_datatools_tables$$
CREATE PROCEDURE create_datatools_tables()
BEGIN
DECLARE script_id INT(11);

-- Log start time
INSERT INTO kenyaemr_etl.etl_script_status(script_name, start_time) VALUES('KenyaEMR_Data_Tool', NOW());
SET script_id = LAST_INSERT_ID();

drop database if exists kenyaemr_datatools;
create database kenyaemr_datatools;

-- -------------------------- creating patient demographics --------------------------------------
-- populate patient_demographics table
create table kenyaemr_datatools.patient_demographics as
select 
patient_id,
given_name,
middle_name,
family_name,
Gender,
DOB,
national_id_no,
unique_patient_no,
patient_clinic_number,
Tb_no,
district_reg_no,
hei_no,
phone_number,
birth_place,
citizenship,
email_address,
next_of_kin,
next_of_kin_relationship,
marital_status,
education_level,
if(dead=1, "Yes", "NO") dead,
death_date,
voided
from kenyaemr_etl.etl_patient_demographics;

-- ADD INDICES

ALTER TABLE kenyaemr_datatools.patient_demographics ADD PRIMARY KEY(patient_id);
ALTER TABLE kenyaemr_datatools.patient_demographics ADD INDEX(Gender);
SELECT "Successfully created demographics table";


-- -------------------------------- create table laboratory_extract ------------------------------------------
create table kenyaemr_datatools.laboratory_extract as
select 
uuid,
encounter_id,
patient_id,
location_id,
visit_date,
visit_id,
(case lab_test when 5497 then "CD4 Count" when 730 then "CD4 PERCENT " when 654 then "ALT" when 790 then "Serum creatinine (umol/L)"
  when 856 then "HIV VIRAL LOAD" when 1305 then "HIV VIRAL LOAD" when 21 then "Hemoglobin (HGB)" else "" end) as lab_test,
urgency,
if(lab_test=299, (case test_result when 1228 then "REACTIVE" when 1229 then "NON-REACTIVE" when 1304 then "POOR SAMPLE QUALITY" end), 
if(lab_test=1030, (case test_result when 1138 then "INDETERMINATE" when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1304 then "POOR SAMPLE QUALITY" end), 
if(lab_test=302, (case test_result when 1115 then "Normal" when 1116 then "Abnormal" when 1067 then "Unknown" end), 
if(lab_test=32, (case test_result when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1138 then "INDETERMINATE" end), 
if(lab_test=1305, (case test_result when 1306 then "BEYOND DETECTABLE LIMIT" when 1301 then "DETECTED" when 1302 then "LDL" when 1304 then "POOR SAMPLE QUALITY" end), 
test_result ))))) AS test_result,
date_created,
created_by 
from kenyaemr_etl.etl_laboratory_extract;

ALTER TABLE kenyaemr_datatools.laboratory_extract ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.laboratory_extract ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.laboratory_extract ADD INDEX(lab_test);
ALTER TABLE kenyaemr_datatools.laboratory_extract ADD INDEX(test_result);

SELECT "Successfully created lab extract table";



-- create table patient_program_discontinuation
create table kenyaemr_datatools.patient_program_discontinuation as
select 
patient_id,
uuid,
visit_id,
visit_date,
program_uuid,
program_name,
encounter_id,
(case discontinuation_reason when 159492 then "Transferred Out" when 160034 then "Died" when 5240 then "Lost to Follow" when 819 then "Cannot afford Treatment"  
  when 5622 then "Other" when 1067 then "Unknown" else "" end) as discontinuation_reason,
date_died,
transfer_facility,
transfer_date
from kenyaemr_etl.etl_patient_program_discontinuation;

ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD INDEX(discontinuation_reason);


SELECT "Successfully created enhanced adherence table";


  -- create table triage
  create table kenyaemr_datatools.triage as
    select
      uuid,
      patient_id,
      visit_id,
      visit_date,
      location_id,
      encounter_id,
      encounter_provider,
      date_created,
      visit_reason,
      weight,
      height,
      systolic_pressure,
      diastolic_pressure,
      temperature,
      pulse_rate,
      respiratory_rate,
      oxygen_saturation,
      muac,
      (case nutritional_status when 1115 then "Normal" when 163302 then "Severe acute malnutrition" when 163303 then "Moderate acute malnutrition" when 114413 then "Overweight/Obese" else "" end) as nutritional_status,
      last_menstrual_period,
      voided
    from kenyaemr_etl.etl_patient_triage;

  ALTER TABLE kenyaemr_datatools.triage ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
  ALTER TABLE kenyaemr_datatools.triage ADD INDEX(visit_date);
  SELECT "Successfully created triage table";


  -- create table datatools_patient_contact
  create table kenyaemr_datatools.patient_contact as
    select
        id,
        uuid,
        date_created,
        first_name,
        middle_name,
        last_name,
        sex,
        birth_date,
        physical_address,
        phone_contact,
        patient_related_to,
        patient_id,
        (case relationship_type when 970 then "Mother" when 971 then "Father" when 1528 then "Child" when 973 then "Grandparent" when 972 then "Sibling" when 160639 then "Guardian" when 1527 then "Parent" when 5617 then "Spouse" when 162221 then "Co-wife" when 163565 then "Sexual partner" when 157351 then "Injectable drug user" when 5622 then "Other" else "" end) as relationship_type,
        appointment_date,
        baseline_hiv_status,
        ipv_outcome,
       (case marital_status when 1057 then "Single" when 5555 then "Married Monogamous" when 159715 then "Married Polygamous" when 1058 then "Divorced" when 1059 then "Widowed" else "" end) as marital_status,
       (case living_with_patient when 1065 then "Yes" when 1066 then "No" when 162570 then "Declined to Answer" else "" end) as living_with_patient,
       (case pns_approach when 162284 then "Dual referral" when 160551 then "Passive referral" when 161642 then "Contract referral" when 163096 then "Provider referral"  else "" end) as pns_approach,
        contact_listing_decline_reason,
       (case consented_contact_listing when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as consented_contact_listing,
        voided
    from kenyaemr_etl.etl_patient_contact;
  ALTER TABLE kenyaemr_datatools.patient_contact ADD PRIMARY KEY(id);
  ALTER TABLE kenyaemr_datatools.patient_contact ADD FOREIGN KEY (patient_related_to) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
  ALTER TABLE kenyaemr_datatools.patient_contact ADD INDEX(date_created);
  SELECT "Successfully created patient_contact table";

    -- create table datatools_client_trace
  create table kenyaemr_datatools.client_trace as
    select
      id,
      uuid,
      date_created,
      encounter_date,
      client_id,
      contact_type,
      status,
      unique_patient_no,
      facility_linked_to,
      health_worker_handed_to,
      remarks,
      appointment_date,
      voided
    from kenyaemr_etl.etl_client_trace;
  ALTER TABLE kenyaemr_datatools.client_trace ADD FOREIGN KEY (client_id) REFERENCES kenyaemr_datatools.patient_contact(id);
  ALTER TABLE kenyaemr_datatools.client_trace ADD INDEX(date_created);
  SELECT "Successfully created client_trace table";


CREATE TABLE kenyaemr_datatools.person_address as SELECT * from kenyaemr_etl.etl_person_address;
CREATE TABLE kenyaemr_datatools.etl_covid_19_case_enrolment as SELECT * from kenyaemr_etl.etl_covid_19_enrolment;


  UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= script_id;

END$$

