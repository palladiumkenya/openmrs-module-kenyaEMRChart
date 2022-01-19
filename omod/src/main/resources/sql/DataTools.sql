DROP PROCEDURE IF EXISTS create_datatools_tables$$
CREATE PROCEDURE create_datatools_tables()
BEGIN
DECLARE script_id INT(11);

-- Log start time
INSERT INTO kenyaemr_etl.etl_script_status(script_name, start_time) VALUES('KenyaEMR_Data_Tool', NOW());
SET script_id = LAST_INSERT_ID();

drop database if exists kenyaemr_datatools;
create database kenyaemr_datatools DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;

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
CPIMS_unique_identifier,
openmrs_id,
district_reg_no,
hei_no,
cwc_number,
phone_number,
birth_place,
citizenship,
email_address,
occupation,
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

-- --------------------------- populate patient_hiv_enrollment table ---------------------------------------------
create table kenyaemr_datatools.hiv_enrollment as
select 
patient_id,
uuid,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
date_last_modified,
patient_type,
date_first_enrolled_in_care,
(case entry_point when 159938 then "HBTC" when 160539 then "VCT Site" when 159937 then "MCH" when 160536 then "IPD-Adult" 
  when 160537 then "IPD-Child," when 160541 then "TB Clinic" when 160542 then "OPD" when 162050 then "CCC" 
  when 160551 then "Self Test," when 5622 then "Other(eg STI)" else "" end) as entry_point,
transfer_in_date,
facility_transferred_from,
district_transferred_from,
(case previous_regimen when 164968 then 'AZT/3TC/DTG'
when 164969 then 'TDF/3TC/DTG'
when 164970 then 'ABC/3TC/DTG'
when 164505 then 'TDF-3TC-EFV'
when 792 then 'D4T/3TC/NVP'
when 160124 then 'AZT/3TC/EFV'
when 160104 then 'D4T/3TC/EFV'
when 1652 then '3TC/NVP/AZT'
when 161361 then 'EDF/3TC/EFV'
when 104565 then 'EFV/FTC/TDF'
when 162201 then '3TC/LPV/TDF/r'
when 817 then 'ABC/3TC/AZT'
when 162199 then 'ABC/NVP/3TC'
when 162200 then '3TC/ABC/LPV/r'
when 162565 then '3TC/NVP/TDF'
when 1652 then '3TC/NVP/AZT'
when 162561 then '3TC/AZT/LPV/r'
when 164511 then 'AZT-3TC-ATV/r'
when 164512 then 'TDF-3TC-ATV/r'
when 162560 then '3TC/D4T/LPV/r'
when 162563 then '3TC/ABC/EFV'
when 162562 then 'ABC/LPV/R/TDF'
when 162559 then 'ABC/DDI/LPV/r' end) as previous_regimen,
date_started_art_at_transferring_facility,
date_confirmed_hiv_positive,
facility_confirmed_hiv_positive,
(case arv_status when 1 then "Yes" when 0 then "No" else "" end) as arv_status,
name_of_treatment_supporter,
(case relationship_of_treatment_supporter when 973 then "Grandparent" when 972 then "Sibling" when 160639 then "Guardian" when 1527 then "Parent" 
  when 5617 then "Spouse" when 163565 then "Partner" when 5622 then "Other" else "" end) as relationship_of_treatment_supporter,
treatment_supporter_telephone,
treatment_supporter_address,
(case in_school when 1 then 'Yes' when 2 then 'No' end) as in_school,
(case orphan when 1 then 'Yes' when 2 then 'No' end) as orphan,
date_of_discontinuation,
(case discontinuation_reason when 159492 then "Transferred Out" when 160034 then "Died" when 5240 then "Lost to Follow" when 819 then "Cannot afford Treatment"
  when 5622 then "Other" when 1067 then "Unknown" else "" end) as discontinuation_reason,
voided
from kenyaemr_etl.etl_hiv_enrollment;


ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD INDEX(arv_status);
ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD INDEX(date_confirmed_hiv_positive);
ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD INDEX(entry_point);

SELECT "Successfully created hiv enrollment table";

-- ----------------------------------- create table hiv_followup ----------------------------------------------
create table kenyaemr_datatools.hiv_followup as
select 
patient_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
(case visit_scheduled when 1 then "scheduled" when 2 then 'unscheduled' else "" end )as visit_scheduled,
(case person_present when 978 then "Self (SF)" when 161642 then "Treatment supporter (TS)" when 5622 then "Other" else "" end) as person_present,
weight,
systolic_pressure,
diastolic_pressure,
height,
temperature,
pulse_rate,
respiratory_rate,
oxygen_saturation,
muac,
(case nutritional_status when 1115 then "Normal" when 163302 then "Severe acute malnutrition" when 163303 then "Moderate acute malnutrition" when 114413 then "Overweight/Obese" else "" end) as nutritional_status,
(case population_type when 164928 then "General Population" when 164929 then "Key Population" else "" end) as population_type,
(case key_population_type when 105 then "People who inject drugs" when 160578 then "Men who have sex with men" when 160579 then "Female sex Worker" when 165100 then "Transgender" when 162277 then "People in prison and other closed settings" else "" end) as key_population_type,
IF(who_stage in (1204,1220),"WHO Stage1", IF(who_stage in (1205,1221),"WHO Stage2", IF(who_stage in (1206,1222),"WHO Stage3", IF(who_stage in (1207,1223),"WHO Stage4", "")))) as who_stage,
(case presenting_complaints when 1 then "Yes" when 0 then "No" else "" end) as presenting_complaints, 
clinical_notes,
(case on_anti_tb_drugs when 1065 then "Yes" when 1066 then "No" else "" end) as on_anti_tb_drugs,
(case on_ipt when 1065 then "Yes" when 1066 then "No" else "" end) as on_ipt,
(case ever_on_ipt when 1065 then "Yes" when 1066 then "No" else "" end) as ever_on_ipt,
(case cough when 159799 then "Yes" else "" end) as cough,
(case fever when 1494 then "Yes" else "" end) as fever,
(case weight_loss_poor_gain when 832 then "Yes" else "" end) as weight_loss_poor_gain,
(case night_sweats when 133027 then "Yes" else "" end) as night_sweats,
(case tb_case_contact when 124068 then "Yes" else "" end) as tb_case_contact,
(case lethargy when 116334 then "Yes"  else "" end) as lethargy,
 screened_for_tb,
(case spatum_smear_ordered when 307 then "Yes" when 1066 then "No" else "" end) as spatum_smear_ordered,
(case chest_xray_ordered when 12 then "Yes" when 1066 then "No" else "" end) as chest_xray_ordered,
(case genexpert_ordered when 162202 then "Yes" when 1066 then "No" else "" end) as genexpert_ordered,
(case spatum_smear_result when 703 then "POSITIVE" when 664 then "NEGATIVE" else "" end) as spatum_smear_result,
(case chest_xray_result when 1115 then "NORMAL" when 152526 then "ABNORMAL" else "" end) as chest_xray_result,
(case genexpert_result when 664 then "NEGATIVE" when 162203 then "Mycobacterium tuberculosis detected with rifampin resistance" when 162204 then "Mycobacterium tuberculosis detected without rifampin resistance" 
  when 164104 then "Mycobacterium tuberculosis detected with indeterminate rifampin resistance"  when 163611 then "Invalid" when 1138 then "INDETERMINATE" else "" end) as genexpert_result,
(case referral when 1065 then "Yes" when 1066 then "No" else "" end) as referral,
(case clinical_tb_diagnosis when 703 then "POSITIVE" when 664 then "NEGATIVE" else "" end) as clinical_tb_diagnosis,
(case contact_invitation when 1065 then "Yes" when 1066 then "No" else "" end) as contact_invitation,
(case evaluated_for_ipt when 1065 then "Yes" when 1066 then "No" else "" end) as evaluated_for_ipt,
(case has_known_allergies when 1 then "Yes" when 0 then "No" else "" end) as has_known_allergies,
(case has_chronic_illnesses_cormobidities when 1065 then "Yes" when 1066 then "No" else "" end) as has_chronic_illnesses_cormobidities,
(case has_adverse_drug_reaction when 1 then "Yes" when 0 then "No" else "" end) as has_adverse_drug_reaction,
(case pregnancy_status when 1065 then "Yes" when 1066 then "No" else "" end) as pregnancy_status,
(case breastfeeding when 1065 then "Yes" when 1066 then "No" else "" end) as breastfeeding,
(case wants_pregnancy when 1065 then "Yes" when 1066 then "No" else "" end) as wants_pregnancy,
(case pregnancy_outcome when 126127 then "Spontaneous abortion" when 125872 then "STILLBIRTH" when 1395 then "Term birth of newborn" when 129218 then "Preterm Delivery (Maternal Condition)" 
 when 159896 then "Therapeutic abortion procedure" when 151849 then "Liveborn, Unspecified Whether Single, Twin, or Multiple" when 1067 then "Unknown" else "" end) as pregnancy_outcome,
anc_number,
expected_delivery_date,
(case ever_had_menses when 1065 then "Yes" when 1066 then "No" when 1175 then "N/A" end) as ever_had_menses,
last_menstrual_period,
(case menopausal when 113928 then "Yes" end) as menopausal,
gravida,
parity,
full_term_pregnancies,
abortion_miscarriages,
(case family_planning_status when 965 then "On Family Planning" when 160652 then "Not using Family Planning" when 1360 then "Wants Family Planning" else "" end) as family_planning_status,
(case family_planning_method when 160570 then "Emergency contraceptive pills" when 780 then "Oral Contraceptives Pills" when 5279 then "Injectible" when 1359 then "Implant" 
when 5275 then "Intrauterine Device" when 136163 then "Lactational Amenorhea Method" when 5278 then "Diaphram/Cervical Cap" when 5277 then "Fertility Awareness" 
when 1472 then "Tubal Ligation" when 190 then "Condoms" when 1489 then "Vasectomy" when 162332 then "Undecided" else "" end) as family_planning_method,
(case reason_not_using_family_planning when 160572 then "Thinks can't get pregnant" when 160573 then "Not sexually active now" when 5622 then "Other" else "" end) as reason_not_using_family_planning,
(case tb_status when 1660 then "No TB Signs" when 142177 then "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "TB Screening Not Done"  else "" end) as tb_status,
tb_treatment_no,
general_examination,
(case system_examination when 1115 then 'Normal' when 1116 then 'Abnormal' end) as system_examination,
(case skin_findings when 150555 then 'Abscess' when 125201 then 'Swelling/Growth' when 135591 then 'Hair Loss' when 136455 then 'Itching' when 507 then 'Kaposi Sarcoma' when 1249 then 'Skin eruptions/Rashes' when 5244 then 'Oral sores' end) as skin_findings,
(case eyes_findings when 123074 then 'Visual Disturbance' when 140940 then 'Excessive tearing' when 131040 then 'Eye pain' when 127777 then 'Eye redness' when 140827 then 'Light sensitive' when 139100 then 'Itchy eyes' end) as eyes_findings,
(case ent_findings when 148517 then 'Apnea' when 139075 then 'Hearing disorder' when 119558 then 'Dental caries' when 118536 then 'Erythema' when 106 then 'Frequent colds' when 147230 then 'Gingival bleeding' when 135841 then 'Hairy cell leukoplakia' when 117698
then 'Hearing loss' when 138554 then 'Hoarseness' when 507 then 'Kaposi Sarcoma' when 152228 then 'Masses' when 128055 then 'Nasal discharge' when 133499 then 'Nosebleed' when 160285 then 'Pain' when 110099 then 'Post nasal discharge' when 126423 then 'Sinus problems' when 126318 then 'Snoring' when 158843 then 'Sore throat' when 5244 then 'Oral sores' when 5334 then 'Thrush' when 123588 then 'Tinnitus'
when 124601 then 'Toothache' when 123919 then 'Ulcers' when 111525 then 'Vertigo' end) as ent_findings,
(case chest_findings when 146893 then 'Bronchial breathing' when 127640 then 'Crackles' when 145712 then 'Dullness' when 164440 then 'Reduced breathing' when 127639 then 'Respiratory distress' when 5209 then 'Wheezing' end) as chest_findings,
(case cvs_findings when 140147 then 'Elevated blood pressure' when 136522 then 'Irregular heartbeat' when 562 then 'Cardiac murmur' when 130560 then 'Cardiac rub' end) as cvs_findings,
(case abdomen_findings when 150915 then 'Abdominal distension' when 5008 then 'Hepatomegaly' when 5103 then 'Abdominal mass' when 5009 then 'Splenomegaly' when 5105 then 'Abdominal tenderness' end) as abdomen_findings,
(case cns_findings when 118872 then 'Altered sensations' when 1836 then 'Bulging fontenelle' when 150817 then 'Abnormal reflexes' when 120345 then 'Confusion' when 157498 then 'Limb weakness' when 112721 then 'Stiff neck' when 136282 then 'Kernicterus' end) as cns_findings,
(case genitourinary_findings when 147241 then 'Bleeding' when 154311 then 'Rectal discharge' when 123529 then 'Urethral discharge' when 123396 then 'Vaginal discharge' when 124087 then 'Ulceration' end) as genitourinary_findings,
(case prophylaxis_given when 105281 then 'Cotrimoxazole' when 74250 then 'Dapsone' when 1107 then 'None' end) as prophylaxis_given,
(case ctx_adherence when 159405 then "Good" when 163794 then "Inadequate" when 159407 then "Poor" else "" end) as ctx_adherence,
(case ctx_dispensed when 1065 then "Yes" when 1066 then "No" when 1175 then "Not applicable" else "" end) as ctx_dispensed,
(case dapsone_adherence when 159405 then "Good" when 163794 then "Inadequate" when 159407 then "Poor" else "" end) as dapsone_adherence,
(case dapsone_dispensed when 1065 then "Yes" when 1066 then "No" when 1175 then "Not applicable" else "" end) as dapsone_dispensed,
(case inh_dispensed when 1065 then "Yes" when 1066 then "No" when 1175 then "Not applicable" else "" end) as inh_dispensed,
(case arv_adherence when 159405 then "Good" when 163794 then "Inadequate" when 159407 then "Poor" else "" end) as arv_adherence,
(case poor_arv_adherence_reason when 102 then "Toxicity, drug" when 121725 then "Alcohol abuse" when 119537 then "Depression" 
when 5622 then "Other" when 1754 then "Medications unavailable" when 1778 then "TREATMENT OR PROCEDURE NOT CARRIED OUT DUE TO FEAR OF SIDE EFFECTS" 
when 819 then "Cannot afford treatment" when 160583 then "Shares medications with others" when 160584 then "Lost or ran out of medication" 
when 160585 then "Felt too ill to take medication" when 160586 then "Felt better and stopped taking medication" when 160587 then "Forgot to take medication" 
when 160588 then "Pill burden" when 160589 then "Concerned about privacy/stigma" when 820 then "TRANSPORT PROBLEMS"  else "" end) as poor_arv_adherence_reason,
poor_arv_adherence_reason_other,
(case pwp_disclosure when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" when 1175 then "N/A" else "" end) as pwp_disclosure,
(case pwp_partner_tested when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" when 1175 then "N/A" else "" end) as pwp_partner_tested,
(case condom_provided when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" when 1175 then "N/A" else "" end) as condom_provided,
(case substance_abuse_screening when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as substance_abuse_screening,
(case screened_for_sti when 703 then "POSITIVE" when 664 then "NEGATIVE" when 1118 then "Not Done" when 1175 then "N/A" else "" end) as screened_for_sti,
(case cacx_screening when 703 then "POSITIVE" when 664 then "NEGATIVE" when 1118 then "Not Done" when 1175 then "N/A" else "" end) as cacx_screening,
(case sti_partner_notification when 1065 then "Yes" when 1066 then "No" else "" end) as sti_partner_notification,
(case at_risk_population when 105 then "People who inject drugs" when 160578 then "Men who have sex with men" when 160579 then "Female sex Worker" else "" end) as at_risk_population,
(case system_review_finding when 1115 then "NORMAL" when 1116 then "ABNORMAL" else "" end) as system_review_finding,
next_appointment_date,
refill_date,
(case next_appointment_reason when 160523 then "Follow up" when 1283 then "Lab tests" when 159382 then "Counseling" when 160521 then "Pharmacy Refill" when 5622 then "Other"  else "" end) as next_appointment_reason,
(case stability when 1 then "Yes" when 2 then "No" when 0 then "No" when 1175 then "Not applicable" else "" end) as stability,
(case differentiated_care when 164942 then "Standard Care" when 164943 then "Fast Track" when 164944 then "Community ART Distribution - HCW Led" when 164945 then "Community ART Distribution - Peer Led" 
when 164946 then "Facility ART Distribution Group" else "" end) as differentiated_care
from kenyaemr_etl.etl_patient_hiv_followup;

ALTER TABLE kenyaemr_datatools.hiv_followup ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(pregnancy_status);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(breastfeeding);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(family_planning_status);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(tb_status);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(ctx_dispensed);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(population_type);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(on_anti_tb_drugs);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(stability);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(differentiated_care);

SELECT "Successfully created hiv followup table";

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
(case order_reason when 843 then "Regimen failure" when 1259 then "CHANGE REGIMEN" when 1434 then "PREGNANCY"
when 159882 then "breastfeeding" when 160566 then "Immunologic failure" when 160569 then "Virologic failure"
when 161236 then "Routine" when 162080 then "Baseline" when 162081 then "Repeat" when 163523 then "Clinical failure"
else"" end) as order_reason,
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

-- create table pharmacy_extract
create table kenyaemr_datatools.pharmacy_extract as
select 
patient_id,
uuid,
visit_date,
visit_id,
encounter_id,
date_created,
encounter_name,
drug,
drug_name,
(case is_arv when 1 then "Yes" else "No" end) as is_arv,
(case is_ctx when 105281 then "SULFAMETHOXAZOLE / TRIMETHOPRIM (CTX)" else "" end) as is_ctx,
(case is_dapsone when 74250 then "DAPSONE" else "" end) as is_dapsone,
frequency,
duration,
duration_units,
voided,
date_voided,
dispensing_provider
from kenyaemr_etl.etl_pharmacy_extract;

ALTER TABLE kenyaemr_datatools.pharmacy_extract ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

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
transfer_date,
(case death_reason when 163324 then "HIV disease resulting in TB"
                   when 116030 then "HIV disease resulting in cancer"
                   when 160159 then "HIV disease resulting in other infectious and parasitic diseases"
                   when 160158 then "Other HIV disease resulting in other diseases or conditions leading to death"
                   when 145439 then "Other HIV disease resulting in other diseases or conditions leading to death"
                   when 133478 then "Other natural causes not directly related to HIV"
                   when 123812 then "Non-natural causes"
                   when 42917 then "Unknown cause" else "" end) as death_reason,
(case specific_death_cause
   when 165609 then "COVID-19 Complications"
   when 145439 then "Non-communicable diseases such as Diabetes and hypertension"
   when 156673 then "HIV disease resulting in mycobacterial infection"
   when 155010 then "HIV disease resulting in Kaposis sarcoma"
   when 156667 then "HIV disease resulting in Burkitts lymphoma"
   when 115195 then "HIV disease resulting in other types of non-Hodgkin lymphoma"
   when 157593 then "HIV disease resulting in other malignant neoplasms of lymphoid and haematopoietic and related tissue"
   when 156672 then "HIV disease resulting in multiple malignant neoplasms"
   when 159988 then "HIV disease resulting in other malignant neoplasms"
   when 5333 then "HIV disease resulting in other bacterial infections"
   when 116031 then "HIV disease resulting in unspecified malignant neoplasms"
   when 123122 then "HIV disease resulting in other viral infections"
   when 156669 then "HIV disease resulting in cytomegaloviral disease"
   when 156668 then "HIV disease resulting in candidiasis"
   when 5350 then "HIV disease resulting in other mycoses"
   when 882 then "HIV disease resulting in Pneumocystis jirovecii pneumonia - HIV disease resulting in Pneumocystis carinii pneumonia"
   when 156671 then "HIV disease resulting in multiple infections"
   when 160159 then "HIV disease resulting in other infectious and parasitic diseases"
   when 171 then "HIV disease resulting in unspecified infectious or parasitic disease - HIV disease resulting in infection NOS"
   when 156670 then "HIV disease resulting in other specified diseases including encephalopathy or lymphoid interstitial pneumonitis or wasting syndrome and others"
   when 160160 then "HIV disease resulting in other conditions including acute HIV infection syndrome or persistent generalized lymphadenopathy or hematological and immunological abnormalities and others"
   when 161548 then "HIV disease resulting in Unspecified HIV disease"
 else "" end) as specific_death_cause,
natural_causes,
non_natural_cause
from kenyaemr_etl.etl_patient_program_discontinuation;

ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD INDEX(discontinuation_reason);
SELECT "Successfully created pharmacy extract table";

  -- create table mch_enrollment
  create table kenyaemr_datatools.mch_enrollment as
    select
      patient_id,
      uuid,
      visit_id,
      visit_date,
      location_id,
      encounter_id,
      (case service_type when 1622 then 'ANC' when 164835 then 'Delivery' when 1623 then 'PNC' else '' end)as service_type,
      anc_number,
      first_anc_visit_date,
      gravida,
      parity,
      parity_abortion,
      age_at_menarche,
      lmp,
      lmp_estimated,
      edd_ultrasound,
      (case blood_group when 690 then "A POSITIVE" when 692 then "A NEGATIVE" when 694 then "B POSITIVE" when 696 then "B NEGATIVE" when 699 then "O POSITIVE"
       when 701 then "O NEGATIVE" when 1230 then "AB POSITIVE" when 1231 then "AB NEGATIVE" else "" end) as blood_group,
      (case serology when 1228 then "REACTIVE" when 1229 then "NON-REACTIVE" when 1304 then "POOR SAMPLE QUALITY" else "" end) as serology,
      (case tb_screening when 664 then "NEGATIVE" when 703 then "POSITIVE" else "" end) as tb_screening,
      (case bs_for_mps when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1138 then "INDETERMINATE" else "" end) as bs_for_mps,
      (case hiv_status when 664 then "HIV Negative" when 703 then "HIV Positive" when 1067 then "Unknown" when 1402 then "Not Tested" else "" end) as hiv_status,
      hiv_test_date,
      (case partner_hiv_status when 664 then "HIV Negative" when 703 then "HIV Positive" when 1067 then "Unknown" else "" end) as partner_hiv_status,
      partner_hiv_test_date,
      urine_microscopy,
      (case urinary_albumin when 664 then "Negative" when 1874 then "Trace - 15" when 1362 then "One Plus(+) - 30" when 1363 then "Two Plus(++) - 100" when 1364 then "Three Plus(+++) - 300" when 1365 then "Four Plus(++++) - 1000" else "" end) as urinary_albumin,
      (case glucose_measurement when 1115 then "Normal" when 1874 then "Trace" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" when 1365 then "Four Plus(++++)" else "" end) as glucose_measurement,
      urine_ph,
      urine_gravity,
      (case urine_nitrite_test when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" else "" end) as urine_nitrite_test,
      (case urine_leukocyte_esterace_test when 664 then "NEGATIVE" when 1874 then "Trace" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" else "" end) as urine_leukocyte_esterace_test,
      (case urinary_ketone when 664 then "NEGATIVE" when 1874 then "Trace - 5" when 1362 then "One Plus(+) - 15" when 1363 then "Two Plus(++) - 50" when 1364 then "Three Plus(+++) - 150" else "" end) as urinary_ketone,
      (case urine_bile_salt_test when 1115 then "Normal" when 1874 then "Trace - 1" when 1362 then "One Plus(+) - 4" when 1363 then "Two Plus(++) - 8" when 1364 then "Three Plus(+++) - 12" else "" end) as urine_bile_salt_test,
      (case urine_bile_pigment_test when 664 then "NEGATIVE" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" else "" end) as urine_bile_pigment_test,
      (case urine_colour when 162099 then "Colourless" when 127778 then "Red color" when 162097 then "Light yellow colour" when 162105 then "Yellow-green colour" when 162098 then "Dark yellow colour" when 162100 then "Brown color" else "" end) as urine_colour,
      (case urine_turbidity when 162102 then "Urine appears clear" when 162103 then "Cloudy urine" when 162104 then "Urine appears turbid" else "" end) as urine_turbidity,
      (case urine_dipstick_for_blood when 664 then "NEGATIVE" when 1874 then "Trace" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" else "" end) as urine_dipstick_for_blood,
      (case discontinuation_reason when 159492 then "Transferred out" when 1067 then "Unknown" when 160034 then "Died" when 5622 then "Other" when 819 then "819" else "" end) as discontinuation_reason
    from kenyaemr_etl.etl_mch_enrollment;

  ALTER TABLE kenyaemr_datatools.mch_enrollment ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
  ALTER TABLE kenyaemr_datatools.mch_enrollment ADD INDEX(visit_date);
  SELECT "Successfully created mch enrollment table";

  -- create table mch_antenatal_visit
  create table kenyaemr_datatools.mch_antenatal_visit as
    select
      patient_id,
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
      (case breast_exam_done when 1065 then "Yes" when 1066 then "No" else "" end) as breast_exam_done,
      (case pallor when 1065 then "Yes" when 1066 then "No" else "" end) as pallor,
      maturity,
      fundal_height,
      (case fetal_presentation when 139814 then "Frank Breech Presentation" when 160091 then "vertex presentation" when 144433 then "Compound Presentation" when 115808 then "Mentum Presentation of Fetus"
       when 118388 then "Face or Brow Presentation of Foetus" when 129192 then "Presentation of Cord" when 112259 then "Transverse or Oblique Fetal Presentation" when 164148 then "Occiput Anterior Position"
       when 164149 then "Brow Presentation" when 164150 then "Face Presentation" when 156352 then "footling breech presentation" else "" end) as fetal_presentation,
      (case lie when 132623 then "Oblique lie" when 162088 then "Longitudinal lie" when 124261 then "Transverse lie" else "" end) as lie,
      fetal_heart_rate,
      (case fetal_movement when 162090 then "Increased fetal movements" when 113377 then "Decreased fetal movements" when 1452 then "No fetal movements" when 162108 then "Fetal movements present" else "" end) as fetal_movement,
      (case who_stage when 1204 then "WHO Stage1" when 1205 then "WHO Stage2" when 1206 then "WHO Stage3" when 1207 then "WHO Stage4" else "" end) as who_stage,
      cd4,
      (case vl_sample_taken when 856 then 'Yes' when 1066 then 'No' end) as vl_sample_taken,
      viral_load,
      (case ldl when 1302 then "LDL"  else "" end) as ldl,
      (case arv_status when 1148 then "ARV Prophylaxis" when 1149 then "HAART" when 1175 then "NA" else "" end) as arv_status,
      final_test_result,
      patient_given_result,
      (case partner_hiv_tested when 1065 then "Yes" when 1066 then "No" else "" end) as partner_hiv_tested,
      (case partner_hiv_status when 664 then "HIV Negative" when 703 then "HIV Positive" when 1067 then "Unknown" else "" end) as partner_hiv_status,
      (case prophylaxis_given when 105281 then "Cotrimoxazole" when 74250 then "Dapsone" when 1107 then "None" else "" end) as prophylaxis_given,
      date_given_haart,
      (case baby_azt_dispensed when 160123 then "Yes" when 1066 then "No" when 1175 then "N/A" else "" end) as baby_azt_dispensed,
      (case baby_nvp_dispensed when 80586 then "Yes" when 1066 then "No" when 1175 then "N/A" else "" end) as baby_nvp_dispensed,
      TTT,
      IPT_malaria,
      iron_supplement,
      deworming,
      bed_nets,
      urine_microscopy,
      (case urinary_albumin when 664 then "Negative" when 1874 then "Trace - 15" when 1362 then "One Plus(+) - 30" when 1363 then "Two Plus(++) - 100" when 1364 then "Three Plus(+++) - 300" when 1365 then "Four Plus(++++) - 1000" else "" end) as urinary_albumin,
      (case glucose_measurement when 1115 then "Normal" when 1874 then "Trace" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" when 1365 then "Four Plus(++++)" else "" end) as glucose_measurement,
      urine_ph,
      urine_gravity,
      (case urine_nitrite_test when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" else "" end) as urine_nitrite_test,
      (case urine_leukocyte_esterace_test when 664 then "NEGATIVE" when 1874 then "Trace" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" else "" end) as urine_leukocyte_esterace_test,
      (case urinary_ketone when 664 then "NEGATIVE" when 1874 then "Trace - 5" when 1362 then "One Plus(+) - 15" when 1363 then "Two Plus(++) - 50" when 1364 then "Three Plus(+++) - 150" else "" end) as urinary_ketone,
      (case urine_bile_salt_test when 1115 then "Normal" when 1874 then "Trace - 1" when 1362 then "One Plus(+) - 4" when 1363 then "Two Plus(++) - 8" when 1364 then "Three Plus(+++) - 12" else "" end) as urine_bile_salt_test,
      (case urine_bile_pigment_test when 664 then "NEGATIVE" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" else "" end) as urine_bile_pigment_test,
      (case urine_colour when 162099 then "Colourless" when 127778 then "Red color" when 162097 then "Light yellow colour" when 162105 then "Yellow-green colour" when 162098 then "Dark yellow colour" when 162100 then "Brown color" else "" end) as urine_colour,
      (case urine_turbidity when 162102 then "Urine appears clear" when 162103 then "Cloudy urine" when 162104 then "Urine appears turbid" else "" end) as urine_turbidity,
      (case urine_dipstick_for_blood when 664 then "NEGATIVE" when 1874 then "Trace" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" else "" end) as urine_dipstick_for_blood,
      (case syphilis_test_status when 1229 then "Non Reactive" when 1228 then "Reactive" when 1402 then "Not Screened" when 1304 then "Poor Sample quality" else "" end) as syphilis_test_status,
      (case syphilis_treated_status when 1065 then "Yes" when 1066 then "No" else "" end) as syphilis_treated_status,
      (case bs_mps when 664 then "Negative" when 703 then "Positive" when 1138 then "Indeterminate" else "" end) as bs_mps,
      (case anc_exercises when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as anc_exercises,
      (case tb_screening when 1660 then "No TB signs" when 164128 then "No signs and started on INH" when 142177 then "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "Not done" when 1111 then "On TB Treatment"  else "" end) as tb_screening,
      (case cacx_screening when 703 then "POSITIVE" when 664 then "NEGATIVE" when 159393 then "Presumed" when 1118 then "Not Done" when 1175 then "N/A" else "" end) as cacx_screening,
      (case cacx_screening_method when 885 then "PAP Smear" when 162816 then "VIA" when 5622 then "Other" else "" end) as cacx_screening_method,
      (case has_other_illnes  when 1065 then "Yes" when 1066 then "No" else "" end) as has_other_illnes,
      (case counselled  when 1065 then "Yes" when 1066 then "No" else "" end) as counselled,
      (case counselled_on_birth_plans when 159758 then 'Yes' end) as counselled_on_birth_plans,
      (case counselled_on_danger_signs when 159857 then 'Yes' end) as counselled_on_danger_signs,
      (case counselled_on_family_planning when 156277 then 'Yes' end) as counselled_on_family_planning,
      (case counselled_on_hiv when 1914 then 'Yes' end) as counselled_on_hiv,
      (case counselled_on_supplimental_feeding when 159854 then 'Yes' end) as counselled_on_supplimental_feeding,
      (case counselled_on_breast_care when 159856 then 'Yes' end) as counselled_on_breast_care,
      (case counselled_on_infant_feeding when 161651 then 'Yes' end) as counselled_on_infant_feeding,
      (case counselled_on_treated_nets when 1381 then 'Yes' end) as counselled_on_treated_nets,
      (case referred_from when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_from,
      (case referred_to when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_to,
      next_appointment_date,
      clinical_notes

    from kenyaemr_etl.etl_mch_antenatal_visit;

  ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
  ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD INDEX(visit_date);
  SELECT "Successfully created mch antenatal visit table";

-- create table mch_delivery table
  create table kenyaemr_datatools.mch_delivery as
    select
      patient_id,
      uuid,
      provider,
      visit_id,
      visit_date,
      location_id,
      encounter_id,
      date_created,
      admission_number,
      duration_of_pregnancy,
      (case mode_of_delivery when 1170 then "Spontaneous vaginal delivery" when 1171 then "Cesarean section" when 1172 then "Breech delivery"
       when 118159 then "Forceps or Vacuum Extractor Delivery" when 159739 then "emergency caesarean section" when 159260 then "vacuum extractor delivery"
       when 5622 then "Other" when 1067 then "Unknown" else "" end) as mode_of_delivery,
      date_of_delivery,
      (case blood_loss when 1499 then "Moderate" when 1107 then "None" when 1498 then "Mild" when 1500 then "Severe" else "" end) as blood_loss,
      (case condition_of_mother when 160429 then "Alive" when 134612 then "Dead" else "" end) as condition_of_mother,
      (case delivery_outcome when 159913 then 'Single' when 159914 then 'Twins' when 159915 then 'Triplets' end) as delivery_outcome,
      apgar_score_1min,
      apgar_score_5min,
      apgar_score_10min,
      (case resuscitation_done when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as resuscitation_done,
      (case place_of_delivery when 1536 then "HOME" when 1588 then "HEALTH CLINIC/POST" when 1589 then "HOSPITAL"
       when 1601 then "EN ROUTE TO HEALTH FACILITY" when 159670 then "sub-district hospital" when 159671 then "Provincial hospital"
       when 159662 then "district hospital" when 159372 then "Primary Care Clinic" when 5622 then "Other" when 1067 then "Unknown" else "" end) as place_of_delivery,
       delivery_assistant,
      (case counseling_on_infant_feeding when 161651 then "Counseling about infant feeding practices" else "" end) as counseling_on_infant_feeding,
      (case counseling_on_exclusive_breastfeeding when 161096 then "Counseling for exclusive breastfeeding" else "" end) as counseling_on_exclusive_breastfeeding,
      (case counseling_on_infant_feeding_for_hiv_infected when 162091 then "Counseling for infant feeding practices to prevent HIV" else "" end) as counseling_on_infant_feeding_for_hiv_infected,
      (case mother_decision when 1173 then "EXPRESSED BREASTMILK" when 1152 then "WEANED" when 5254 then "Infant formula" when 1150 then "BREASTFED PREDOMINATELY"
       when 6046 then "Mixed feeding" when 5526 then "BREASTFED EXCLUSIVELY" when 968 then "COW MILK" when 1595 then "REPLACEMENT FEEDING"  else "" end) as mother_decision,
      (case placenta_complete when 163455 then "Complete placenta at delivery" when 163456 then "Incomplete placenta at delivery" else "" end) as placenta_complete,
      (case maternal_death_audited when 1065 then "Yes" when 1066 then "No" else "" end) as maternal_death_audited,
      (case cadre when 1574 then "CLINICAL OFFICER/DOCTOR" when 1578 then "Midwife" when 1577 then "NURSE" when 1575 then "TRADITIONAL BIRTH ATTENDANT" when 1555 then " COMMUNITY HEALTH CARE WORKER" when 5622 then "Other" else "" end) as cadre,
      (case delivery_complications when 1065 then "Yes" when 1066 then "No" else "" end) as delivery_complications,
      (case coded_delivery_complications when 118744 then "Eclampsia" when 113195 then "Ruptured Uterus" when 115036 then "Obstructed Labor" when 228 then "APH" when 230 then "PPH" when 130 then "Puerperal sepsis" when 1067 then "Unknown" else "" end) as coded_delivery_complications,
       other_delivery_complications,
       duration_of_labor,
      (case baby_sex when 1534 then "Male Gender" when 1535 then "Female gender" else "" end) as baby_sex,
      (case baby_condition when 135436 then "Macerated Stillbirth" when 159916 then "Fresh stillbirth" when 151849 then "Live birth"
       when 125872 then "STILLBIRTH" when 126127 then "Spontaneous abortion"
       when 164815 then "Live birth, died before arrival at facility"
       when 164816 then "Live birth, died after arrival or delivery in facility" else "" end) as baby_condition,
      (case teo_given when 84893 then "TETRACYCLINE" when 1066 then "No" when 1175 then "Not applicable" else "" end) as teo_given,
      birth_weight,
      (case bf_within_one_hour when 1065 then "Yes" when 1066 then "No" else "" end) as bf_within_one_hour,
      (case birth_with_deformity when 155871 then "deformity" when 1066 then "No"  when 1175 then "Not applicable" else "" end) as birth_with_deformity,
      final_test_result,
      patient_given_result,
      (case partner_hiv_tested when 1065 then "Yes" when 1066 then "No" else "" end) as partner_hiv_tested,
      (case partner_hiv_status when 664 then "HIV Negative" when 703 then "HIV Positive" when 1067 then "Unknown" else "" end) as partner_hiv_status,
      (case prophylaxis_given when 105281 then "SULFAMETHOXAZOLE / TRIMETHOPRIM" when 74250 then "DAPSONE"  when 1107 then "None" else "" end) as prophylaxis_given,
      (case baby_azt_dispensed when 160123 then "Zidovudine for PMTCT" when 1066 then "No" when 1175 then "Not Applicable" else "" end) as baby_azt_dispensed,
      (case baby_nvp_dispensed when 80586 then "NEVIRAPINE" when 1066 then "No" when 1175 then "Not Applicable" else "" end) as baby_nvp_dispensed,
      clinical_notes

    from kenyaemr_etl.etl_mchs_delivery;

  ALTER TABLE kenyaemr_datatools.mch_delivery ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
  ALTER TABLE kenyaemr_datatools.mch_delivery ADD INDEX(visit_date);
  SELECT "Successfully created mchs delivery table";

-- create table mch_discharge table
  create table kenyaemr_datatools.mch_discharge as
    select
      patient_id,
      uuid,
      provider,
      visit_id,
      visit_date,
      location_id,
      encounter_id,
      date_created,
      (case counselled_on_feeding when 1065 then "Yes" when 1066 then "No" else "" end) as counselled_on_feeding,
      (case baby_status when 163016 then "Alive" when 160432 then "Dead" else "" end) as baby_status,
      (case vitamin_A_dispensed when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as vitamin_A_dispensed,
      birth_notification_number,
      condition_of_mother,
      discharge_date,
      (case referred_from when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_from,
      (case referred_to when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_to,
      clinical_notes

    from kenyaemr_etl.etl_mchs_discharge;
  ALTER TABLE kenyaemr_datatools.mch_discharge ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
  ALTER TABLE kenyaemr_datatools.mch_discharge ADD INDEX(visit_date);

  SELECT "Successfully created mch_discharge table";

  -- create table mch_postnatal_visit
  create table kenyaemr_datatools.mch_postnatal_visit as
    select
      patient_id,
      uuid,
      visit_id,
      visit_date,
      location_id,
      encounter_id,
      provider,
      pnc_register_no,
      pnc_visit_no,
      delivery_date,
      (case mode_of_delivery when 1170 then "SVD" when 1171 then "C-Section" else "" end) as mode_of_delivery,
      (case place_of_delivery when 1589 then "Facility" when 1536 then "Home" when 5622 then "Other" else "" end) as place_of_delivery,
      (case delivery_outcome when 159913 then 'Single' when 159914 then 'Twins' when 159915 then 'Triplets' end) as delivery_outcome,
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
      (case arv_status when 1148 then "ARV Prophylaxis" when 1149 then "HAART" when 1175 then "NA" else "" end) as arv_status,
      (case general_condition when 1855 then "Good" when 162133 then "Fair" when 162132 then "Poor" else "" end) as general_condition,
      (case breast when 1855 then "Good" when 162133 then "Fair" when 162132 then "Poor" else "" end) as breast,    -- recheck
      (case cs_scar when 156794 then "infection of obstetric surgical wound" when 145776 then "Caesarean Wound Disruption" when 162129 then "Wound intact and healing" when 162130 then "Surgical wound healed" else "" end) as cs_scar,
      (case gravid_uterus when 162111 then "On exam, uterine fundus 12-16 week size" when 162112 then "On exam, uterine fundus 16-20 week size" when 162113 then "On exam, uterine fundus 20-24 week size" when 162114 then "On exam, uterine fundus 24-28 week size"
       when 162115 then "On exam, uterine fundus 28-32 week size" when 162116 then "On exam, uterine fundus 32-34 week size" when 162117 then "On exam, uterine fundus 34-36 week size" when 162118 then "On exam, uterine fundus 36-38 week size"
       when 162119 then "On exam, uterine fundus 38 weeks-term size" when 123427 then "Uterus Involuted"  else "" end) as gravid_uterus,
      (case episiotomy when 159842 then "repaired, episiotomy wound" when 159843 then "healed, episiotomy wound" when 159841 then "gap, episiotomy wound" when 113919 then "Postoperative Wound Infection" else "" end) as episiotomy,
      (case lochia when 159845 then "lochia excessive" when 159846 then "lochia foul smelling" when 159721 then "Lochia type" else "" end) as lochia,  -- recheck
      (case counselled_on_infant_feeding when 1065 then 'Yes' when 1066 then 'No' end) as counselled_on_infant_feeding,
      (case pallor when 1065 then "Yes" when 1066 then "No" when 1175 then "Not applicable" else "" end) as pallor,
      (case pph when 1065 then "Present" when 1066 then "Absent" else "" end) as pph,
      (case mother_hiv_status when 1067 then "Unknown" when 664 then "NEGATIVE" when 703 then "POSITIVE" else "" end) as mother_hiv_status,
      (case condition_of_baby when 1855 then "In good health" when 162132 then "Patient condition poor" when 1067 then "Unknown" when 162133 then "Patient condition fair/satisfactory" else "" end) as condition_of_baby,
      (case baby_feeding_method when 5526 then "BREASTFED EXCLUSIVELY" when 1595 then "REPLACEMENT FEEDING" when 6046 then "Mixed feeding" when 159418 then "Not at all sure" else "" end) as baby_feeding_method,
      (case umblical_cord when 162122 then "Neonatal umbilical stump clean" when 162123 then "Neonatal umbilical stump not clean" when 162124 then "Neonatal umbilical stump moist" when 159418 then "Not at all sure" else "" end) as umblical_cord,
      (case baby_immunization_started when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as baby_immunization_started,
      (case family_planning_counseling when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as family_planning_counseling,
      uterus_examination,
      uterus_cervix_examination,
      vaginal_examination,
      parametrial_examination,
      external_genitalia_examination,
      ovarian_examination,
      pelvic_lymph_node_exam,
      final_test_result,
      patient_given_result,
      (case partner_hiv_tested when 1065 then "Yes" when 1066 then "No" else "" end) as partner_hiv_tested,
      (case partner_hiv_status when 664 then "HIV Negative" when 703 then "HIV Positive" when 1067 then "Unknown" else "" end) as partner_hiv_status,
      (case prophylaxis_given when 105281 then "Cotrimoxazole" when 74250 then "Dapsone" when 1107 then "None" else "" end) as prophylaxis_given,
      (case baby_azt_dispensed when 160123 then "Yes" when 1066 then "No" when 1175 then "N/A" else "" end) as baby_azt_dispensed,
      (case baby_nvp_dispensed when 80586 then "Yes" when 1066 then "No" when 1175 then "N/A" else "" end) as baby_nvp_dispensed,
      (case pnc_exercises when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as pnc_exercises,
      (case maternal_condition when 130 then "Puerperal sepsis" when 114244 then "Perineal Laceration" when 1855 then "In good health" when 134612 then "Maternal Death" when 160429 then "Alive" when 162132 then "Patient condition poor" when 162133 then "Patient condition fair/satisfactory" else "" end) as maternal_condition,
      (case iron_supplementation when 1065 then "Yes" when 1066 then "No" else "" end) as iron_supplementation,
      (case fistula_screening when 1107 then "None" when 49 then "Vesicovaginal Fistula" when 127847 then "Rectovaginal fistula" when 1118 then "Not done"  else "" end) as fistula_screening,
      (case cacx_screening when 703 then "POSITIVE" when 664 then "NEGATIVE" when 159393 then "Presumed" when 1118 then "Not Done" when 1175 then "N/A" else "" end) as cacx_screening,
      (case cacx_screening_method when 885 then "PAP Smear" when 162816 then "VIA" when 5622 then "Other" else "" end) as cacx_screening_method,
      (case family_planning_status when 965 then "On Family Planning" when 160652 then "Not using Family Planning"  else "" end) as family_planning_status,
      (case family_planning_method when 160570 then "Emergency contraceptive pills" when 780 then "Oral Contraceptives Pills" when 5279 then "Injectible" when 1359 then "Implant"
       when 5275 then "Intrauterine Device" when 136163 then "Lactational Amenorhea Method" when 5278 then "Diaphram/Cervical Cap" when 5277 then "Fertility Awareness"
       when 1472 then "Tubal Ligation" when 190 then "Condoms" when 1489 then "Vasectomy" when 162332 then "Undecided" else "" end) as family_planning_method,
      (case referred_from when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_from,
      (case referred_to when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_to,
      clinical_notes,
      appointment_date

    from kenyaemr_etl.etl_mch_postnatal_visit;

  ALTER TABLE kenyaemr_datatools.mch_postnatal_visit ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
  ALTER TABLE kenyaemr_datatools.mch_postnatal_visit ADD INDEX(visit_date);
SELECT "Successfully created post natal visit table";

  -- create table hei_enrollment
  create table kenyaemr_datatools.hei_enrollment as
    select
      serial_no,
      patient_id,
      uuid,
      provider,
      visit_id,
      visit_date,
      location_id,
      encounter_id,
      (case child_exposed when 822 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as child_exposed,
      spd_number,
      birth_weight,
      gestation_at_birth,
      (case birth_type when 159913 then 'Single' when 159914 then 'Twins' when 159915 then 'Triplets' when 113450 then 'Quadruplets' when 113440 then 'Quintuplets' end) as birth_type,
      date_first_seen,
      birth_notification_number,
      birth_certificate_number,
      (case need_for_special_care when 161628 then "Yes" when 1066 then "No" else "" end) as need_for_special_care,
      (case reason_for_special_care when 116222 then "Birth weight less than 2.5 kg" when 162071 then "Birth less than 2 years after last birth" when 162072 then "Fifth or more child" when 162073 then "Teenage mother"
       when 162074 then "Brother or sisters undernourished" when 162075 then "Multiple births(Twins,triplets)" when 162076 then "Child in family dead" when 1174 then "Orphan"
       when 161599 then "Child has disability" when 1859 then "Parent HIV positive" when 123174 then "History/signs of child abuse/neglect" else "" end) as reason_for_special_care,
      (case referral_source when 160537 then "Paediatric" when 160542 then "OPD" when 160456 then "Maternity" when 162050 then "CCC"  when 160538 then "MCH/PMTCT" when 5622 then "Other" else "" end) as referral_source,
      (case transfer_in when 1065 then "Yes" when 1066 then "No" else "" end) as transfer_in,
      transfer_in_date,
      facility_transferred_from,
      district_transferred_from,
      date_first_enrolled_in_hei_care,
      (case mother_breastfeeding when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as mother_breastfeeding,
      (case TB_contact_history_in_household when 1065 then "Yes" when 1066 then "No" else "" end) as TB_contact_history_in_household,
      (case mother_alive when 1 then "Yes" when 0 then "No" else "" end) as mother_alive,
      (case mother_on_pmtct_drugs when 1065 then "Yes" when 1066 then "No" else "" end) as mother_on_pmtct_drugs,
      (case mother_on_drug when 80586 then "Sd NVP Only" when 1652 then "AZT+NVP+3TC" when 1149 then "HAART" when 1107 then "None" else "" end) as mother_on_drug,
      (case mother_on_art_at_infant_enrollment when 1065 then "Yes" when 1066 then "No" else "" end) as mother_on_art_at_infant_enrollment,
      (case mother_drug_regimen when 792 then "D4T/3TC/NVP" when 160124 then "AZT/3TC/EFV" when 160104 then "D4T/3TC/EFV" when 1652 then "3TC/NVP/AZT"
       when 161361 then "EDF/3TC/EFV" when 104565 then "EFV/FTC/TDF" when 162201 then "3TC/LPV/TDF/r" when 817 then "ABC/3TC/AZT"
       when 162199 then "ABC/NVP/3TC" when 162200 then "3TC/ABC/LPV/r" when 162565 then "3TC/NVP/TDF" when 1652 then "3TC/NVP/AZT"
       when 162561 then "3TC/AZT/LPV/r" when 164511 then "AZT-3TC-ATV/r" when 164512 then "TDF-3TC-ATV/r" when 162560 then "3TC/D4T/LPV/r"
       when 162563 then "3TC/ABC/EFV" when 162562 then "ABC/LPV/R/TDF" when 162559 then "ABC/DDI/LPV/r"  else "" end) as mother_drug_regimen,
      (case infant_prophylaxis when 80586 then "Sd NVP Only" when 1652 then "sd NVP+AZT+3TC" when 1149 then "NVP for 6 weeks(Mother on HAART)" when 1107 then "None" else "" end) as infant_prophylaxis,
      parent_ccc_number,
      (case mode_of_delivery when 1170 then "SVD" when 1171 then "C-Section" else "" end) as mode_of_delivery,
      (case place_of_delivery when 1589 then "Facility" when 1536 then "Home" when 5622 then "Other" else "" end) as place_of_delivery,
      birth_length,
      birth_order,
      health_facility_name,
      date_of_birth_notification,
      date_of_birth_registration,
      birth_registration_place,
      permanent_registration_serial,
      mother_facility_registered,
      exit_date,
     (case exit_reason when 1403 then "HIV Neg age greater 18 months" when 138571 then "Confirmed HIV Positive" when 5240 then "Lost" when 160432 then "Dead" when 159492 then "Transfer Out" else "" end) as exit_reason,
     hiv_status_at_exit
    from kenyaemr_etl.etl_hei_enrollment;

  ALTER TABLE kenyaemr_datatools.hei_enrollment ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
  ALTER TABLE kenyaemr_datatools.hei_enrollment ADD INDEX(visit_date);
  SELECT "Successfully created hei_enrollment";

  -- create table hei_follow_up_visit
  create table kenyaemr_datatools.hei_follow_up_visit as
    select
      patient_id,
      uuid,
      provider,
      visit_id,
      visit_date,
      location_id,
      encounter_id,
      weight,
      height,
      (case muac when 160909 then "Green" when 160910 then "Yellow" when 127778 then "Red" else "" end) as muac,
      (case primary_caregiver when 970 then "Mother" when 973 then "Guardian" when 972 then "Guardian" when 160639 then "Guardian" when 5622 then "Guardian" else "" end) as primary_caregiver,
      (case infant_feeding when 5526 then "Exclusive Breastfeeding(EBF)" when 1595 then "Exclusive Replacement(ERF)" when 6046 then "Mixed Feeding(MF)" else "" end) as infant_feeding,
      (case stunted when 164085 then "Yes" when 1115 then "No" else "" end) as stunted,
      (case tb_assessment_outcome when 1660 then "No TB Signs" when 142177 then "Presumed TB" when 1661 then "TB Confirmed" when 1662 then "TB Rx" when 1679 then "INH" when 160737 then "TB Screening Not Done" else "" end) as tb_assessment_outcome,
      (case social_smile_milestone when 162056 then "Social Smile" else "" end) as social_smile_milestone,
      (case head_control_milestone when 162057 then "Head Holding/Control" else "" end) as head_control_milestone,
      (case response_to_sound_milestone when 162058 then "Turns towards the origin of sound" else "" end) as response_to_sound_milestone,
      (case hand_extension_milestone when 162059 then "Extends hand to grasp a toy" else "" end) as hand_extension_milestone,
      (case sitting_milestone when 162061 then "Sitting" else "" end) as sitting_milestone,
      (case walking_milestone when 162063 then "Walking" else "" end) as walking_milestone,
      (case standing_milestone when 162062 then "Standing" else "" end) as standing_milestone,
      (case talking_milestone when 162060 then "Talking" else "" end) as talking_milestone,
      (case review_of_systems_developmental when 1115 then "Normal(N)" when 6022 then "Delayed(D)" when 6025 then "Regressed(R)" else "" end) as review_of_systems_developmental,
      (case weight_category when 123814 then "Underweight(UW)" when 126598 then "Severely Underweight(SUW)" when 114413 then "Overweight(OW)" when 115115 then "Obese(O)" when 1115 then "Normal(N)" else "" end) as weight_category,
      dna_pcr_sample_date,
      (case dna_pcr_contextual_status when 162081 then "Repeat" when 162083 then "Final test (end of pediatric window)" when 162082 then "Confirmation" when 162080 then "Initial" else "" end) as dna_pcr_contextual_status,
      (case dna_pcr_result when 1138 then "INDETERMINATE" when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1304 then "POOR SAMPLE QUALITY" else "" end) as dna_pcr_result,
      (case azt_given when 86663 then "Yes" else "No" end) as azt_given,
      (case nvp_given when 80586 then "Yes" else "No" end) as nvp_given,
      (case ctx_given when 105281 then "Yes" else "No" end) as ctx_given,
      (case multi_vitamin_given when 461 then "Yes" else "No" end) as multi_vitamin_given,
      (case first_antibody_result when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1304 then "POOR SAMPLE QUALITY" else "" end) as first_antibody_result,
      (case final_antibody_result when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1304 then "POOR SAMPLE QUALITY" else "" end) as final_antibody_result,
      (case tetracycline_ointment_given  when 1065 then "Yes" when 1066 then "No" else "" end) as tetracycline_ointment_given,
      (case pupil_examination when 162065 then "Black" when 1075 then "White" else "" end) as pupil_examination,
      (case sight_examination when 1065 then "Following Objects" when 1066 then "Not Following Objects" else "" end) as sight_examination,
      (case squint when 1065 then "Squint" when 1066 then "No Squint" else "" end) as squint,
      (case deworming_drug when 79413 then "Mebendazole" when 70439 then "Albendazole" else "" end) as deworming_drug,
      dosage,
      unit,
      (case vitaminA_given when 1065 then "Yes" when 1066 then "No" else "" end) as vitaminA_given,
      (case disability when 1065 then "Yes" when 1066 then "No" else "" end) as disability,
      (case referred_from when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_from,
      (case referred_to when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_to,
      (case counselled_on when 1914 then "HIV" when 1380 then "Nutrition" else "" end) as counselled_on,
     (case mnps_supplementation when 161649 then "Yes" when 1107 then "No" else "" end) as MNPS_Supplementation,
      comments,
      next_appointment_date
    from kenyaemr_etl.etl_hei_follow_up_visit;

  ALTER TABLE kenyaemr_datatools.hei_follow_up_visit ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

  ALTER TABLE kenyaemr_datatools.hei_follow_up_visit ADD INDEX(visit_date);
  ALTER TABLE kenyaemr_datatools.hei_follow_up_visit ADD INDEX(infant_feeding);

  SELECT "Successfully created hei_follow_up_visit";

  -- create table hei_immunization
  create table kenyaemr_datatools.hei_immunization as
    select
      patient_id,
			visit_date,
			created_by,
			date_created,
			encounter_id,
			BCG,
			OPV_birth,
			OPV_1,
			OPV_2,
			OPV_3,
			IPV,
			DPT_Hep_B_Hib_1,
			DPT_Hep_B_Hib_2,
			DPT_Hep_B_Hib_3,
			PCV_10_1,
			PCV_10_2,
			PCV_10_3,
			ROTA_1,
			ROTA_2,
			Measles_rubella_1,
			Measles_rubella_2,
			Yellow_fever,
			Measles_6_months,
			VitaminA_6_months,
			VitaminA_1_yr,
			VitaminA_1_and_half_yr,
			VitaminA_2_yr ,
			VitaminA_2_to_5_yr,
			fully_immunized
    from kenyaemr_etl.etl_hei_immunization;

  ALTER TABLE kenyaemr_datatools.hei_immunization ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
  SELECT "Successfully created hei_immunization table";


  -- create table tb_enrollment
  create table kenyaemr_datatools.tb_enrollment as
    select
      patient_id,
      uuid,
      provider,
      visit_id,
      visit_date,
      location_id,
      encounter_id,
      date_treatment_started,
      district,
      (case referred_by when 160539 then "VCT center" when 160631 then "HIV care clinic" when 160546 then "STI Clinic" when 161359 then "Home Based Care"
       when 160538 then "Antenatal/PMTCT Clinic" when 1725 then "Private Sector" when 1744 then "Chemist/pharmacist" when 160551 then "Self referral"
       when 1555 then "Community Health worker(CHW)" when 162050 then "CCC" when 164103 then "Diabetes Clinic" else "" end) as referred_by,
      referral_date,
      date_transferred_in,
      facility_transferred_from,
      district_transferred_from,
      date_first_enrolled_in_tb_care,
      weight,
      height,
      treatment_supporter,
      (case relation_to_patient when 973 then "Grandparent" when 972 then "Sibling" when 160639 then "Guardian" when 1527 then "Parent" when 5617 then "PARTNER OR SPOUSE"
       when 5622 then "Other" else "" end) as relation_to_patient,
      treatment_supporter_address,
      treatment_supporter_phone_contact,
      (case disease_classification when 42 then "Pulmonary TB" when 5042 then "Extra-Pulmonary TB" else "" end) as disease_classification,
      (case patient_classification when 159878 then "New" when 159877 then "Smear positive Relapse" when 159876 then "Smear negative Relapse" when 159874 then "Treatment after Failure"
       when 159873 then "Treatment resumed after defaulting" when 159872 then "Transfer in" when 163609 then "Previous treatment history unknown"  else "" end) as patient_classification,
      (case pulmonary_smear_result when 703 then "Smear Positive" when 664 then "Smear Negative" when 1118 then "Smear not done" else "" end) as pulmonary_smear_result,
      (case has_extra_pulmonary_pleurial_effusion when 130059 then "Pleural effusion" else "" end) as has_extra_pulmonary_pleurial_effusion,
      (case has_extra_pulmonary_milliary when 115753 then "Milliary" else "" end) as has_extra_pulmonary_milliary,
      (case has_extra_pulmonary_lymph_node when 111953 then "Lymph nodes" else "" end) as has_extra_pulmonary_lymph_node,
      (case has_extra_pulmonary_menengitis when 111967 then "Meningitis" else "" end) as has_extra_pulmonary_menengitis,
      (case has_extra_pulmonary_skeleton when 112116 then "Skeleton" else "" end) as has_extra_pulmonary_skeleton,
      (case has_extra_pulmonary_abdominal when 1350 then "Abdominal" else "" end) as has_extra_pulmonary_abdominal
    from kenyaemr_etl.etl_tb_enrollment;

  ALTER TABLE kenyaemr_datatools.tb_enrollment ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

  ALTER TABLE kenyaemr_datatools.tb_enrollment ADD INDEX(visit_date);
SELECT "Successfully created tb enrollment table";

-- create table tb_follow_up_visit
create table kenyaemr_datatools.tb_follow_up_visit as
select 
patient_id,
uuid,
provider,
visit_id,
visit_date ,
location_id,
encounter_id,
(case spatum_test when 160022 then "ZN Smear Microscopy" when 161880 then "Fluorescence Microscopy" else "" end) as spatum_test,
(case spatum_result when 159985 then "Scanty" when 1362 then "+" when 1363 then "++" when 1364 then "+++" when 664 then "Negative" else "" end) as spatum_result,
result_serial_number,
quantity ,
date_test_done,
(case bacterial_colonie_growth when 703 then "Growth" when 664 then "No growth" else "" end) as bacterial_colonie_growth,
number_of_colonies,
(case resistant_s when 84360 then "S" else "" end) as resistant_s,
(case resistant_r when 767 then "R" else "" end) as resistant_r,
(case resistant_inh when 78280 then "INH" else "" end) as resistant_inh,
(case resistant_e when 75948 then "E" else "" end) as resistant_e,
(case sensitive_s when 84360 then "S" else "" end) as sensitive_s,
(case sensitive_r when 767 then "R" else "" end) as sensitive_r,
(case sensitive_inh when 78280 then "INH" else "" end) as sensitive_inh,
(case sensitive_e when 75948 then "E" else "" end) as sensitive_e,
test_date,
(case hiv_status when 664 then "Negative" when 703 then "Positive" when 1067 then "Unknown" else "" end) as hiv_status,
next_appointment_date
from kenyaemr_etl.etl_tb_follow_up_visit;
 
ALTER TABLE kenyaemr_datatools.tb_follow_up_visit ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.tb_follow_up_visit ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.tb_follow_up_visit ADD INDEX(hiv_status);
SELECT "Successfully created tb followup table";


-- create table tb_screening
create table kenyaemr_datatools.tb_screening as
select
patient_id,
uuid,
provider,
visit_id,
visit_date,
encounter_id,
location_id,
(case cough_for_2wks_or_more when 159799 then "Yes" when 1066 then "No" else "" end) as cough_for_2wks_or_more,
(case confirmed_tb_contact when 124068 then "Yes" when 1066 then "No" else "" end) as confirmed_tb_contact,
(case fever_for_2wks_or_more when 1494 then "Yes" when 1066 then "No" else "" end) as fever_for_2wks_or_more,
(case noticeable_weight_loss when 832 then "Yes" when 1066 then "No" else "" end) as noticeable_weight_loss,
(case night_sweat_for_2wks_or_more when 133027 then "Yes" when 1066 then "No" else "" end) as night_sweat_for_2wks_or_more,
(case lethargy when 116334 then "Yes"  else "" end) as lethargy,
(case spatum_smear_ordered when 307 then "Yes" when 1066 then "No" else "" end) as spatum_smear_ordered,
(case chest_xray_ordered when 12 then "Yes" when 1066 then "No" else "" end) as chest_xray_ordered,
(case genexpert_ordered when 162202 then "Yes" when 1066 then "No" else "" end) as genexpert_ordered,
(case spatum_smear_result when 703 then "POSITIVE" when 664 then "NEGATIVE" else "" end) as spatum_smear_result,
(case chest_xray_result when 1115 then "NORMAL" when 152526 then "ABNORMAL" else "" end) as chest_xray_result,
(case genexpert_result when 664 then "NEGATIVE" when 162203 then "Mycobacterium tuberculosis detected with rifampin resistance" when 162204 then "Mycobacterium tuberculosis detected without rifampin resistance"
  when 164104 then "Mycobacterium tuberculosis detected with indeterminate rifampin resistance"  when 163611 then "Invalid" when 1138 then "INDETERMINATE" else "" end) as genexpert_result,
(case referral when 1065 then "Yes" when 1066 then "No" else "" end) as referral,
(case clinical_tb_diagnosis when 703 then "POSITIVE" when 664 then "NEGATIVE" else "" end) as clinical_tb_diagnosis,
(case contact_invitation when 1065 then "Yes" when 1066 then "No" else "" end) as contact_invitation,
(case evaluated_for_ipt when 1065 then "Yes" when 1066 then "No" else "" end) as evaluated_for_ipt,
(case resulting_tb_status when 1660 then "No TB Signs" when 142177 then "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "TB Screening Not Done" else "" end) as resulting_tb_status,
tb_treatment_start_date,
(case tb_prophylaxis when 105281 then 'Cotrimoxazole' when 74250 then 'Dapsone' when 1107 then 'None' end) as tb_prophylaxis,
notes,
(case person_present when 978 then 'Yes' else 'No' end) as person_present
from kenyaemr_etl.etl_tb_screening;

ALTER TABLE kenyaemr_datatools.tb_screening ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.tb_screening ADD INDEX(visit_date);
SELECT "Successfully created tb screening table";

 -- Table Datatools drug event
create table kenyaemr_datatools.drug_event as
    select
      uuid,
      patient_id,
      date_started,
      visit_date,
      provider,
      encounter_id,
      program,
      regimen,
      regimen_name,
      regimen_line,
      discontinued,
      (case regimen_stopped when 1260 then 'Yes' else 'No' end) as regimen_stopped,
      regimen_discontinued,
      date_discontinued,
      (case reason_discontinued when 102 then "Drug toxicity" when 160567 then "New diagnosis of Tuberculosis"  when 160569 then "Virologic failure"
       when 159598 then "Non-compliance with treatment or therapy" when 1754 then "Medications unavailable"
       when 1434 then "Currently pregnant"  when 1253 then "Completed PMTCT"  when 843 then "Regimen failure"
       when 5622 then "Other" when 160559 then "Risk of pregnancy" when 160561 then "New drug available" else "" end) as reason_discontinued,
      reason_discontinued_other
    from kenyaemr_etl.etl_drug_event;

ALTER TABLE kenyaemr_datatools.drug_event add FOREIGN KEY(patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

SELECT "Successfully created drug event table";


 -- create table art_preparation
     create table kenyaemr_datatools.art_preparation as
      select
     uuid,
     patient_id,
     visit_id,
     visit_date,
     location_id,
     encounter_id,
     provider,
     understands_hiv_art_benefits,
     screened_negative_substance_abuse,
     screened_negative_psychiatric_illness,
     HIV_status_disclosure,
     trained_drug_admin,
     caregiver_committed,
     adherance_barriers_identified,
     caregiver_location_contacts_known,
     ready_to_start_art,
     identified_drug_time,
     treatment_supporter_engaged,
     support_grp_meeting_awareness,
     enrolled_in_reminder_system
    from kenyaemr_etl.etl_ART_preparation;

    ALTER TABLE kenyaemr_datatools.art_preparation ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
    ALTER TABLE kenyaemr_datatools.art_preparation ADD INDEX(visit_date);
    ALTER TABLE kenyaemr_datatools.art_preparation ADD INDEX(ready_to_start_art);

SELECT "Successfully created art preparation table";

  -- create table enhanced_adherence
  create table kenyaemr_datatools.enhanced_adherence as
    select
      uuid,
      patient_id,
      visit_id,
      visit_date,
      location_id,
      encounter_id,
      provider,
      session_number,
      first_session_date,
      pill_count,
      arv_adherence,
      has_vl_results,
      vl_results_suppressed,
      vl_results_feeling,
      cause_of_high_vl,
      way_forward,
      patient_hiv_knowledge,
      patient_drugs_uptake,
      patient_drugs_reminder_tools,
      patient_drugs_uptake_during_travels,
      patient_drugs_side_effects_response,
      patient_drugs_uptake_most_difficult_times,
      patient_drugs_daily_uptake_feeling,
      patient_ambitions,
      patient_has_people_to_talk,
      patient_enlisting_social_support,
      patient_income_sources,
      patient_challenges_reaching_clinic,
      patient_worried_of_accidental_disclosure,
      patient_treated_differently,
      stigma_hinders_adherence,
      patient_tried_faith_healing,
      patient_adherence_improved,
      patient_doses_missed,
      review_and_barriers_to_adherence,
      other_referrals,
      appointments_honoured,
      referral_experience,
      home_visit_benefit,
      adherence_plan,
      next_appointment_date
    from kenyaemr_etl.etl_enhanced_adherence;
  ALTER TABLE kenyaemr_datatools.enhanced_adherence ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
  ALTER TABLE kenyaemr_datatools.enhanced_adherence ADD INDEX(visit_date);

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

SELECT "creating hts_test table";
create table kenyaemr_datatools.hts_test
  as select
  t.patient_id,
  t.visit_id,
  t.encounter_id,
  t.encounter_uuid,
  t.encounter_location,
  t.creator,
  t.date_created,
  t.date_last_modified,
  t.visit_date,
  t.test_type,
  t.population_type,
  t.key_population_type,
  t.ever_tested_for_hiv,
  t.months_since_last_test,
  t.patient_disabled,
  t.disability_type,
  t.patient_consented,
  t.client_tested_as,
  t.setting,
  t.approach,
(case  t.test_strategy
when 164163 then "HP: Hospital Patient Testing"
when 164953 then "NP: HTS for non-patients"
when 164954 then "VI:Integrated VCT Center"
when 164955 then "VS:Stand Alone VCT Center"
when 159938 then "HB:Home Based Testing"
when 159939 then "MO: Mobile Outreach HTS"
when 161557 then "Index testing"
when 166606 then "SNS - Social Networks"
when 5622 then "O:Other"
else ""  end ) as test_strategy,
(case  t.hts_entry_point
when 5485 then "In Patient Department(IPD)"
when 160542 then "Out Patient Department(OPD)"
when 162181 then "Peadiatric Clinic"
when 160552 then "Nutrition Clinic"
when 160538 then "PMTCT ANC"
when 160456 then "PMTCT MAT"
when 1623 then "PMTCT PNC"
when 160541 then "TB"
when 162050 then "CCC"
when 159940 then "VCT"
when 159938 then "Home Based Testing"
when 159939 then "Mobile Outreach"
when 162223 then "VMMC"
when 160546 then "STI Clinic"
when 160522 then "Emergency"
when 163096 then "Community Testing"
when 5622 then "Other"
else ""  end ) as hts_entry_point,
  t.test_1_kit_name,
  t.test_1_kit_lot_no,
  t.test_1_kit_expiry,
  t.test_1_result,
  t.test_2_kit_name,
  t.test_2_kit_lot_no,
  t.test_2_kit_expiry,
  t.test_2_result,
  t.final_test_result,
  t.patient_given_result,
  t.couple_discordant,
  t.referral_for,
  t.referral_facility,
  t.other_referral_facility,
  t.tb_screening,
  t.patient_had_hiv_self_test ,
  t.remarks,
  t.voided
from kenyaemr_etl.etl_hts_test t
inner join kenyaemr_etl.etl_patient_demographics d on d.patient_id = t.patient_id and d.voided=0;
ALTER TABLE kenyaemr_datatools.hts_test ADD FOREIGN KEY(patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.hts_test ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.hts_test ADD index(population_type);
ALTER TABLE kenyaemr_datatools.hts_test ADD index(final_test_result);

SELECT "Successfully created hts_test table";

create table kenyaemr_datatools.hts_referral_and_linkage
  as select l.* from kenyaemr_etl.etl_hts_referral_and_linkage l inner join kenyaemr_etl.etl_patient_demographics d on d.patient_id = l.patient_id and d.voided=0;
ALTER TABLE kenyaemr_datatools.hts_referral_and_linkage ADD FOREIGN KEY(patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.hts_referral_and_linkage ADD index(visit_date);

SELECT "Successfully created referral_and_linkage table";

create table kenyaemr_datatools.hts_referral
  as select r.* from kenyaemr_etl.etl_hts_referral r inner join kenyaemr_etl.etl_patient_demographics d on d.patient_id = r.patient_id and d.voided=0;

create table kenyaemr_datatools.current_in_care as select * from kenyaemr_etl.etl_current_in_care;
ALTER TABLE kenyaemr_datatools.current_in_care add FOREIGN KEY(patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

create table kenyaemr_datatools.ipt_followup as select * from kenyaemr_etl.etl_ipt_follow_up;
alter table kenyaemr_datatools.ipt_followup add FOREIGN KEY(patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

CREATE TABLE  kenyaemr_datatools.default_facility_info as SELECT * from kenyaemr_etl.etl_default_facility_info;
CREATE TABLE kenyaemr_datatools.person_address as SELECT * from kenyaemr_etl.etl_person_address;


  -- create table alcohol_drug_abuse_screening
create table kenyaemr_datatools.alcohol_drug_abuse_screening as
select
patient_id,
uuid,
provider,
visit_id,
visit_date,
encounter_id,
location_id,
(case alcohol_drinking_frequency when 1090 then 'Never' when 1091 then 'Monthly or less' when 1092 then '2 to 4 times a month' when 1093 then '2 to 3 times a week' when 1094 then '4 or More Times a Week' end) as alcohol_drinking_frequency,
(case smoking_frequency when 1090 then 'Never smoked' when 156358 then 'Former cigarette smoker' when 163197 then 'Current some day smoker' when 163196 then 'Current light tobacco smoker'
when 163195 then 'Current heavy tobacco smoker' when 163200 then 'Unknown if ever smoked' end) as smoking_frequency,
(case drugs_use_frequency when 1090 then 'Never' when 1091 then 'Monthly or less' when 1092 then '2 to 4 times a month' when 1093 then '2 to 3 times a week' when 1094 then '4 or More Times a Week' end) as drugs_use_frequency,
date_created,
date_last_modified,
voided
from kenyaemr_etl.etl_alcohol_drug_abuse_screening;

ALTER TABLE kenyaemr_datatools.alcohol_drug_abuse_screening ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.alcohol_drug_abuse_screening ADD INDEX(visit_date);
SELECT "Successfully created alcohol_drug_abuse_screening table";

/*Form collecting data has been discontinued
create table kenyaemr_datatools.gender_based_violence as select * from kenyaemr_etl.etl_gender_based_violence;
alter table kenyaemr_datatools.gender_based_violence add FOREIGN KEY(client_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.gender_based_violence ADD INDEX(visit_date);*/

-- create table gbv_screening
create table kenyaemr_datatools.gbv_screening as
select
patient_id,
uuid,
provider,
visit_id,
visit_date,
encounter_id,
location_id,
(case ipv when 1065 then 'Yes' when 1066 then 'No' end) as ipv,
(case physical_ipv when 158358 then 'Yes' when 1066 then 'No' end) as physical_ipv,
(case emotional_ipv when 118688 then 'Yes' when 1066 then 'No' end) as emotional_ipv,
(case sexual_ipv when 152370 then 'Yes' when 1066 then 'No' end) as sexual_ipv,
(case ipv_relationship when 1582 then 'Yes' when 1066 then 'No' end) as ipv_relationship,
date_created,
date_last_modified,
voided
from kenyaemr_etl.etl_gbv_screening;

ALTER TABLE kenyaemr_datatools.gbv_screening ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.gbv_screening ADD INDEX(visit_date);
SELECT "Successfully created gbv_screening table";

-- create table gbv_screening
create table kenyaemr_datatools.gbv_screening_action as
select
patient_id,
uuid,
provider,
visit_id,
visit_date,
obs_id,
location_id,
(case help_provider when 1589 THEN "Hospital" when 165284 then "Police" when 165037 then "Peer Educator" when 1560 then "Family" when 165294 then "Peers" when 5618 then "Friends"
                          when 165290 then "Religious Leader" when 165350 then "Dice" when 162690 then "Chief" when 5622 then "Other" else "" end) as help_provider,
(case action_taken when 1066 then "No action taken"
        when 165070 then "Counselling"
        when 160570 then "Emergency pills"
        when 1356 then "Hiv testing"
        when 130719 then "Investigation done"
        when 135914 then "Matter presented to court"
        when 165228 then "P3 form issued"
        when 165171 then "PEP given"
        when 165192 then "Perpetrator arrested"
        when 127910 then "Post rape care"
        when 165203 then "PrEP given"
        when 5618 then "Reconciliation"
        when 165093 then "Referred back to the family"
        when 165274 then "Referred to hospital"
        when 165180 then "Statement taken"
        when 165200 then "STI Prophylaxis"
        when 165184 then "Trauma counselling done"
        when 1185 then "Treatment"
        when 5622 then "Other"
        else "" end) as action_taken,
(case reason_for_not_reporting when 1067 then "Did not know where to report"
       when 1811 then "Distance"
       when 140923 then "Exhaustion/Lack of energy"
       when 163473 then "Fear shame"
       when 159418 then "Lack of faith in system"
       when 162951 then "Lack of knowledge"
       when 664 then "Negative attitude of the person reported to"
       when 143100 then "Not allowed culturally"
       when 165161 then "Perpetrator above the law"
       when 163475 then "Self blame"
       else "" end) as reason_for_not_reporting,
date_created,
date_last_modified,
voided
from kenyaemr_etl.etl_gbv_screening_action;

ALTER TABLE kenyaemr_datatools.gbv_screening_action ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.gbv_screening_action ADD INDEX(visit_date);
SELECT "Successfully created gbv_screening_action table";


-- create table depression_screening
create table kenyaemr_datatools.depression_screening as
select
patient_id,
uuid,
provider,
visit_id,
visit_date,
encounter_id,
location_id,
(case PHQ_9_rating when 1115 then 'Depression unlikely' when 157790 then 'Mild depression' when 134011 then 'Moderate depression' when 134017 then 'Moderate severe depression' when 126627 then 'Severe depression' end) as PHQ_9_rating,
date_created,
date_last_modified,
voided
from kenyaemr_etl.etl_depression_screening;

ALTER TABLE kenyaemr_datatools.depression_screening ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.depression_screening ADD INDEX(visit_date);
SELECT "Successfully created depression_screening table";

-- create table adverse_events
create table kenyaemr_datatools.adverse_events as
select
uuid,
provider,
patient_id,
visit_id,
visit_date,
location_id,
encounter_id,
obs_id,
(case cause when 70056 then 'Abicavir' when 162298 then 'ACE inhibitors' when 70878 then 'Allopurinol' when 155060 then 'Aminoglycosides' when 162299 then 'ARBs (angiotensin II receptor blockers)' when 103727 then 'Aspirin' when 71647 then 'Atazanavir' when 72822 then 'Carbamazepine' when 162301 then 'Cephalosporins' when 73300 then 'Chloroquine'  when 73667 then 'Codeine' when 74807 then 'Didanosine' when 75523 then 'Efavirenz' when 162302 then 'Erythromycins' when
75948 then 'Ethambutol' when 77164 then 'Griseofulvin' when 162305 then 'Heparins' when 77675 then 'Hydralazine' when 78280 then 'Isoniazid' when 794 then 'Lopinavir/ritonavir' when 80106 then 'Morphine' when 80586 then 'Nevirapine' when 80696 then 'Nitrofurans' when 162306 then 'Non-steroidal anti-inflammatory drugs' when 81723 then 'Penicillamine' when 81724 then 'Penicillin' when 81959 then 'Phenolphthaleins' when 82023 then 'Phenytoin' when
82559 then 'Procainamide' when 82900 then 'Pyrazinamide' when 83018 then 'Quinidine' when 767 then 'Rifampin' when 162307 then 'Statins' when 84309 then 'Stavudine'
when 162170 then 'Sulfonamides' when 84795 then 'Tenofovir' when 84893 then 'Tetracycline' when 86663 then 'Zidovudine' when 5622 then 'Other' end) as cause,
(case adverse_event when 1067 then 'Unknown' when  121629  then 'Anaemia' when 148888 then 'Anaphylaxis' when 148787 then 'Angioedema' when 120148 then 'Arrhythmia' when 108 then 'Bronchospasm' when 143264 then 'Cough' when 142412 then 'Diarrhea' when 118773 then 'Dystonia' when 140238 then 'Fever'
when 140039 then 'Flushing' when 139581 then 'GI upset' when 139084 then 'Headache' when 159098 then 'Hepatotoxicity' when 111061 then 'Hives' when 117399 then 'Hypertension' when 879 then 'Itching' when 121677 then 'Mental status change' when 159347 then 'Musculoskeletal pain'
when 121 then 'Myalgia' when 512 then 'Rash' when 5622 then 'Other' end ) as adverse_event,
(case severity when 1498 then 'Mild' when 1499 then 'Moderate' when 1500 then 'Severe' when 162819 then 'Fatal' when 1067 then 'Unknown' end) as severity,
start_date,
(case action_taken when 1257 then 'CONTINUE REGIMEN' when 1259 then 'SWITCHED REGIMEN'  when 981 then 'CHANGED DOSE'  when 1258 then 'SUBSTITUTED DRUG' when 1107 then 'NONE' when 1260 then 'STOP' when 5622 then 'Other' end) as action_taken,
date_created,
date_last_modified,
voided
from kenyaemr_etl.etl_adverse_events;

ALTER TABLE kenyaemr_datatools.adverse_events ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.adverse_events ADD INDEX(visit_date);
SELECT "Successfully created adverse_events table";

-- create table allergies_chronic_illnesses
create table kenyaemr_datatools.allergy_chronic_illness as
select
uuid,
provider,
patient_id,
visit_id,
visit_date,
location_id,
encounter_id,
obs_id,
(case chronic_illness when 149019 then 'Alzheimers Disease and other Dementias'
when 148432 then 'Arthritis'
when 153754 then 'Asthma'
when 159351 then 'Cancer'
when 119270 then 'Cardiovascular diseases'
when 120637 then 'Chronic Hepatitis'
when 145438 then 'Chronic Kidney Disease'
when 1295 then 'Chronic Obstructive Pulmonary Disease(COPD)'
when 120576 then 'Chronic Renal Failure'
when 119692 then 'Cystic Fibrosis'
when 120291 then 'Deafness and Hearing impairment'
when 119481 then 'Diabetes'
when 118631 then 'Endometriosis'
when 117855 then 'Epilepsy'
when 117789 then 'Glaucoma'
when 139071 then 'Heart Disease'
when 115728 then 'Hyperlipidaemia'
when 117399 then 'Hypertension'
when 117321 then 'Hypothyroidism'
when 151342 then 'Mental illness'
when 133687 then 'Multiple Sclerosis'
when 115115 then 'Obesity'
when 114662 then 'Osteoporosis'
when 117703 then 'Sickle Cell Anaemia'
when 118976 then 'Thyroid disease'
end) as chronic_illness,
chronic_illness_onset_date,
(case allergy_causative_agent when 162543 then 'Beef'
when 72609 then 'Caffeine'
when 162544 then 'Chocolate'
when 162545 then 'Dairy Food'
when 162171 then 'Eggs'
when 162546 then 'Fish'
when 162547 then 'Milk Protein'
when 162172 then 'Peanuts'
when 162175 then 'Shellfish'
when 162176 then 'Soy'
when 162548 then 'Strawberries'
when 162177 then 'Wheat'
when 162542 then 'Adhesive Tape'
when 162536 then 'Bee Stings'
when 162537 then 'Dust'
when 162538 then 'Latex'
when 162539 then 'Mold'
when 162540 then 'Pollen'
when 162541 then 'Ragweed'
when 5622 then 'Other' end) as allergy_causative_agent,
(case allergy_reaction when 1067 then 'Anaemia'
when 121629 then 'Anaphylaxis'
when 148888 then 'Angioedema'
when 148787 then 'Arrhythmia'
when 120148 then 'Bronchospasm'
when 108 then 'Cough'
when 143264 then 'Diarrhea'
when 142412 then 'Dystonia'
when 118773 then 'Fever'
when 140238 then 'Flushing'
when 140039 then 'GI upset'
when 139581 then 'Headache'
when 139084 then 'Hepatotoxicity'
when 159098 then 'Hives'
when 111061 then 'Hypertension'
when 117399 then 'Itching'
when 879 then 'Mental status change'
when 121677 then 'Musculoskeletal pain'
when 159347 then 'Myalgia'
when 121 then 'Rash'
when 512 then 'Other' end) as allergy_reaction,
(case allergy_severity when 160754 then 'Mild' when 160755 then 'Moderate' when 160756 then 'Severe' when 160758 then 'Fatal' when 1067 then 'Unknown' end) as allergy_severity,
allergy_onset_date,
voided,
date_created,
date_last_modified
from kenyaemr_etl.etl_allergy_chronic_illness;

ALTER TABLE kenyaemr_datatools.allergy_chronic_illness ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.allergy_chronic_illness ADD INDEX(visit_date);
SELECT "Successfully created allergy_chronic_illness table";

-- create table ipt_screening
create table kenyaemr_datatools.ipt_screening as
select
uuid,
provider,
patient_id,
visit_id,
visit_date,
location_id,
encounter_id,
obs_id,
(case cough when 159799 then 'Yes' when 1066 then 'No' end) as cough,
(case fever when 1494 then 'Yes' when 1066 then 'No' end) as fever,
(case weight_loss_poor_gain when 832 then 'Yes' when 1066 then 'No' end) as weight_loss_poor_gain,
(case night_sweats when 133027 then 'Yes' when 1066 then 'No' end) as night_sweats,
(case contact_with_tb_case when 124068 then 'Yes' when 1066 then 'No' end) as contact_with_tb_case,
(case lethargy when 116334 then 'Yes' when 1066 then 'No' end) as lethargy,
(case yellow_urine when 162311 then 'Yes' when 1066 then 'No' end) as yellow_urine,
(case numbness_bs_hands_feet when 132652 then 'Yes' when 1066 then 'No' end) as numbness_bs_hands_feet,
(case eyes_yellowness when 5192 then 'Yes' when 1066 then 'No' end) as eyes_yellowness,
(case upper_rightQ_abdomen_tenderness when 124994 then 'Yes' when 1066 then 'No' end) as upper_rightQ_abdomen_tenderness,
date_created,
date_last_modified,
voided
from kenyaemr_etl.etl_ipt_screening;

ALTER TABLE kenyaemr_datatools.ipt_screening ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.ipt_screening ADD INDEX(visit_date);
SELECT "Successfully created ipt_screening table";

-- create table pre_hiv_enrollment_ART
create table kenyaemr_datatools.pre_hiv_enrollment_art as
  select
         uuid,
         provider,
         patient_id,
         visit_id,
         visit_date,
         location_id,
         encounter_id,
         obs_id,
         (case PMTCT when 1065 then 'Yes' else '' end) as PMTCT,
         (case PMTCT_regimen when 164968 then 'AZT/3TC/DTG'
when 164969 then 'TDF/3TC/DTG'
when 164970 then 'ABC/3TC/DTG'
when 164505 then 'TDF-3TC-EFV'
when 792 then 'D4T/3TC/NVP'
when 160124 then 'AZT/3TC/EFV'
when 160104 then 'D4T/3TC/EFV'
when 1652 then '3TC/NVP/AZT'
when 161361 then 'EDF/3TC/EFV'
when 104565 then 'EFV/FTC/TDF'
when 162201 then '3TC/LPV/TDF/r'
when 817 then 'ABC/3TC/AZT'
when 162199 then 'ABC/NVP/3TC'
when 162200 then '3TC/ABC/LPV/r'
when 162565 then '3TC/NVP/TDF'
when 1652 then '3TC/NVP/AZT'
when 162561 then '3TC/AZT/LPV/r'
when 164511 then 'AZT-3TC-ATV/r'
when 164512 then 'TDF-3TC-ATV/r'
when 162560 then '3TC/D4T/LPV/r'
when 162563 then '3TC/ABC/EFV'
when 162562 then 'ABC/LPV/R/TDF'
when 162559 then 'ABC/DDI/LPV/r' end) as PMTCT_regimen,
         (case PEP when 1065 then 'Yes' else '' end) as PEP,
         (case PEP_regimen when 164968 then 'AZT/3TC/DTG'
when 164969 then 'TDF/3TC/DTG'
when 164970 then 'ABC/3TC/DTG'
when 164505 then 'TDF-3TC-EFV'
when 792 then 'D4T/3TC/NVP'
when 160124 then 'AZT/3TC/EFV'
when 160104 then 'D4T/3TC/EFV'
when 1652 then '3TC/NVP/AZT'
when 161361 then 'EDF/3TC/EFV'
when 104565 then 'EFV/FTC/TDF'
when 162201 then '3TC/LPV/TDF/r'
when 817 then 'ABC/3TC/AZT'
when 162199 then 'ABC/NVP/3TC'
when 162200 then '3TC/ABC/LPV/r'
when 162565 then '3TC/NVP/TDF'
when 1652 then '3TC/NVP/AZT'
when 162561 then '3TC/AZT/LPV/r'
when 164511 then 'AZT-3TC-ATV/r'
when 164512 then 'TDF-3TC-ATV/r'
when 162560 then '3TC/D4T/LPV/r'
when 162563 then '3TC/ABC/EFV'
when 162562 then 'ABC/LPV/R/TDF'
when 162559 then 'ABC/DDI/LPV/r' end) as PEP_regimen,
         (case PrEP when 1065 then 'Yes' else '' end) as PrEP,
         (case PrEP_regimen when 164968 then 'AZT/3TC/DTG'
when 164969 then 'TDF/3TC/DTG'
when 164970 then 'ABC/3TC/DTG'
when 164505 then 'TDF-3TC-EFV'
when 792 then 'D4T/3TC/NVP'
when 160124 then 'AZT/3TC/EFV'
when 160104 then 'D4T/3TC/EFV'
when 1652 then '3TC/NVP/AZT'
when 161361 then 'EDF/3TC/EFV'
when 104565 then 'EFV/FTC/TDF'
when 162201 then '3TC/LPV/TDF/r'
when 817 then 'ABC/3TC/AZT'
when 162199 then 'ABC/NVP/3TC'
when 162200 then '3TC/ABC/LPV/r'
when 162565 then '3TC/NVP/TDF'
when 1652 then '3TC/NVP/AZT'
when 162561 then '3TC/AZT/LPV/r'
when 164511 then 'AZT-3TC-ATV/r'
when 164512 then 'TDF-3TC-ATV/r'
when 162560 then '3TC/D4T/LPV/r'
when 162563 then '3TC/ABC/EFV'
when 162562 then 'ABC/LPV/R/TDF'
when 162559 then 'ABC/DDI/LPV/r' end) as PrEP_regimen,
         (case HAART when 1185 then 'Yes' else '' end) as HAART,
         (case HAART_regimen when 164968 then 'AZT/3TC/DTG'
when 164969 then 'TDF/3TC/DTG'
when 164970 then 'ABC/3TC/DTG'
when 164505 then 'TDF-3TC-EFV'
when 792 then 'D4T/3TC/NVP'
when 160124 then 'AZT/3TC/EFV'
when 160104 then 'D4T/3TC/EFV'
when 1652 then '3TC/NVP/AZT'
when 161361 then 'EDF/3TC/EFV'
when 104565 then 'EFV/FTC/TDF'
when 162201 then '3TC/LPV/TDF/r'
when 817 then 'ABC/3TC/AZT'
when 162199 then 'ABC/NVP/3TC'
when 162200 then '3TC/ABC/LPV/r'
when 162565 then '3TC/NVP/TDF'
when 1652 then '3TC/NVP/AZT'
when 162561 then '3TC/AZT/LPV/r'
when 164511 then 'AZT-3TC-ATV/r'
when 164512 then 'TDF-3TC-ATV/r'
when 162560 then '3TC/D4T/LPV/r'
when 162563 then '3TC/ABC/EFV'
when 162562 then 'ABC/LPV/R/TDF'
when 162559 then 'ABC/DDI/LPV/r' end) as HAART_regimen,
         date_created,
         date_last_modified,
         voided
  from kenyaemr_etl.etl_pre_hiv_enrollment_art;

ALTER TABLE kenyaemr_datatools.pre_hiv_enrollment_art ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.pre_hiv_enrollment_art ADD INDEX(visit_date);
SELECT "Successfully created pre_hiv_enrollment_art table";

-- create table covid_19_assessment
create table kenyaemr_datatools.covid_19_assessment as
select
       uuid,
       provider,
       patient_id,
       visit_id,
       visit_date,
       location_id,
       encounter_id,
       obs_id,
       (case ever_vaccinated when 1065 then 'Yes' when 1066 then 'No' end) as ever_vaccinated,
       (case first_vaccine_type when 166156 then 'Astrazeneca' when 166355 then 'Johnson and Johnson'
                                when 166154 then 'Moderna' when 166155 then 'Pfizer' when 166157 then 'Sputnik' when
           166379 then 'Sinopharm' when 1067 then 'Unknown' when 5622 then 'Other' end) as first_vaccine_type,
       (case second_vaccine_type when 166156 then 'Astrazeneca' when 166355 then 'Johnson and Johnson'
                                 when 166154 then 'Moderna' when 166155 then 'Pfizer' when 166157 then 'Sputnik' when
           166379 then 'Sinopharm' when 1067 then 'Unknown' when 5622 then 'Other(Specify)' end) as second_vaccine_type,
       first_dose,
       second_dose,
       first_dose_date,
       second_dose_date,
       (case first_vaccination_verified when 164134 then 'Yes' end ) as first_vaccination_verified,
       (case second_vaccination_verified when 164134 then 'Yes' end) as second_vaccination_verified,
       (case final_vaccination_status when 166192 then 'Partially Vaccinated' when 5585 then 'Fully Vaccinated' end) as final_vaccination_status,
       (case ever_received_booster when 1065 then 'Yes' when 1066 then 'No' end) as ever_received_booster,
       (case booster_vaccine_taken when 166156 then 'Astrazeneca' when 166355 then 'Johnson and Johnson'
                                   when 166154 then 'Moderna' when 166155 then 'Pfizer' when 166157 then 'Sputnik' when
           166379 then 'Sinopharm' when 1067 then 'Unknown' when 5622 then 'Other(Specify)' end) as booster_vaccine_taken,
       date_taken_booster_vaccine,
       booster_sequence,
       (case booster_dose_verified when 164134 then 'Yes' end) as booster_dose_verified,
       (case ever_tested_covid_19_positive when 703 then 'Yes' when 664 then 'No' when 1067 then 'Unknown' end) as ever_tested_covid_19_positive,
       (case symptomatic when 1068 then 'Yes' when 165912 then 'No' END) as symptomatic,
       date_tested_positive,
       (case hospital_admission when 1065 then 'Yes' when 1066 then 'No' end) as hospital_admission,
       admission_unit,
       (case on_ventillator when 1065 then 'Yes' when 1066 then 'No' end) as on_ventillator,
       (case on_oxygen_supplement when 1065 then 'Yes' when 1066 then 'No' end) as on_oxygen_supplement,
       date_created,
       date_last_modified,
       voided
from kenyaemr_etl.etl_covid19_assessment;

ALTER TABLE kenyaemr_datatools.covid_19_assessment ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.covid_19_assessment ADD INDEX(visit_date);
SELECT "Successfully created covid_19_assessment table";

-- Create table prep_enrolment
create table kenyaemr_datatools.prep_enrolment as
  select
         uuid,
         provider,
         patient_id,
         visit_id,
         visit_date,
         location_id,
         encounter_id,
         date_created,
         date_last_modified,
         patient_type,
         case population_type when 164928 then 'General Population' when 6096 then 'Discordant Couple' when 164929 then 'Key Population' end as population_type,
         case kp_type when 162277 then 'People in prison and other closed settings' when 165100 then 'Transgender' when 105 then 'PWID' when 160578 then 'MSM' when 165084 then 'MSW' when 160579 then 'FSW' end as kp_type,
         transfer_in_entry_point,
         referred_from,
         transit_from,
         transfer_in_date,
         transfer_from,
         initial_enrolment_date,
         date_started_prep_trf_facility,
         previously_on_prep,
         regimen,
         prep_last_date,
         case in_school when 1 then 'Yes' when 2 then 'No' end as in_school,
         buddy_name,
         buddy_alias,
         buddy_relationship,
         buddy_phone,
         buddy_alt_phone,
         voided
  from kenyaemr_etl.etl_prep_enrolment;

ALTER TABLE kenyaemr_datatools.prep_enrolment ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.prep_enrolment ADD INDEX(visit_date);
SELECT "Successfully created prep_enrolment table";

-- Create table cervical_cancer_screening
create table kenyaemr_datatools.cervical_cancer_screening as
  select
      uuid,
      encounter_id,
      encounter_provider,
      patient_id,
      visit_id,
      visit_date,
      location_id,
      date_created,
      date_last_modified,
      visit_type,
      screening_type,
      post_treatment_complication_cause,
      post_treatment_complication_other,
      screening_method,
      screening_result,
      treatment_method,
      treatment_method_other,
      referred_out,
      referral_facility,
      referral_reason,
      next_appointment_date,
      voided
  from kenyaemr_etl.etl_cervical_cancer_screening;

ALTER TABLE kenyaemr_datatools.cervical_cancer_screening ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.cervical_cancer_screening ADD INDEX(visit_date);
SELECT "Successfully created cervical_cancer_screening table";

-- Create table contact
create table kenyaemr_datatools.contact as
select
     uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    date_last_modified,
    key_population_type,
    contacted_by_peducator,
    program_name,
    frequent_hotspot_name,
    frequent_hotspot_type,
    year_started_sex_work,
    year_started_sex_with_men,
    year_started_drugs,
    avg_weekly_sex_acts,
    avg_weekly_anal_sex_acts,
    avg_daily_drug_injections,
    contact_person_name,
    contact_person_alias,
    contact_person_phone,
    voided
 from kenyaemr_etl.etl_contact;

ALTER TABLE kenyaemr_datatools.contact ADD FOREIGN KEY (client_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.contact ADD INDEX(visit_date);
SELECT "Successfully created contact table";

-- Create table client_enrollment
create table kenyaemr_datatools.client_enrollment as
select
  uuid,
  client_id,
  visit_id,
  visit_date,
  location_id,
  encounter_id,
  encounter_provider,
  date_created,
  date_last_modified,
  contacted_for_prevention,
  has_regular_free_sex_partner,
  year_started_sex_work,
  year_started_sex_with_men,
  year_started_drugs,
  has_expereienced_sexual_violence,
  has_expereienced_physical_violence,
  ever_tested_for_hiv,
  test_type,
  share_test_results,
  willing_to_test,
  test_decline_reason,
  receiving_hiv_care,
  care_facility_name,
  ccc_number,
  vl_test_done,
  vl_results_date,
  contact_for_appointment,
  contact_method,
  buddy_name,
  buddy_phone_number,
  voided
 from kenyaemr_etl.etl_client_enrollment;

ALTER TABLE kenyaemr_datatools.client_enrollment ADD FOREIGN KEY (client_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.client_enrollment ADD INDEX(visit_date);
SELECT "Successfully created client_enrollment table";

-- Create table clinical_visit
create table kenyaemr_datatools.clinical_visit as
select
    uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    date_last_modified,
    implementing_partner,
    type_of_visit,
    visit_reason,
    service_delivery_model,
    sti_screened,
    sti_results,
    sti_treated,
    sti_referred,
    sti_referred_text,
    tb_screened,
    tb_results,
    tb_treated,
    tb_referred,
    tb_referred_text,
    hepatitisB_screened,
    hepatitisB_results,
    hepatitisB_treated,
    hepatitisB_referred,
    hepatitisB_text,
    hepatitisC_screened,
    hepatitisC_results,
    hepatitisC_treated,
    hepatitisC_referred,
    hepatitisC_text,
    overdose_screened,
    overdose_results,
    overdose_treated,
    received_naloxone,
    overdose_referred,
    overdose_text,
    abscess_screened,
    abscess_results,
    abscess_treated,
    abscess_referred,
    abscess_text,
    alcohol_screened,
    alcohol_results,
    alcohol_treated,
    alcohol_referred,
    alcohol_text,
    cerv_cancer_screened,
    cerv_cancer_results,
    cerv_cancer_treated,
    cerv_cancer_referred,
    cerv_cancer_text,
    prep_screened,
    prep_results,
    prep_treated,
    prep_referred,
    prep_text,
    violence_screened,
    violence_results,
    violence_treated,
    violence_referred,
    violence_text,
    risk_red_counselling_screened,
    risk_red_counselling_eligibility,
    risk_red_counselling_support,
    risk_red_counselling_ebi_provided,
    risk_red_counselling_text,
    fp_screened,
    fp_eligibility,
    fp_treated,
    fp_referred,
    fp_text,
    mental_health_screened,
    mental_health_results,
    mental_health_support,
    mental_health_referred,
    mental_health_text,
    hiv_self_rep_status,
    last_hiv_test_setting,
    counselled_for_hiv,
    hiv_tested,
    test_frequency,
    received_results,
    test_results,
    linked_to_art,
    facility_linked_to,
    self_test_education,
    self_test_kits_given,
    self_use_kits,
    distribution_kits,
    self_tested,
    self_test_date,
    self_test_frequency,
    self_test_results,
    test_confirmatory_results,
    confirmatory_facility,
    offsite_confirmatory_facility,
    self_test_linked_art,
    self_test_link_facility,
    hiv_care_facility,
    other_hiv_care_facility,
    initiated_art_this_month,
    active_art,
    eligible_vl,
    vl_test_done,
    vl_results,
    received_vl_results,
    condom_use_education,
    post_abortal_care,
    linked_to_psychosocial,
    male_condoms_no,
    female_condoms_no,
    lubes_no,
    syringes_needles_no,
    pep_eligible,
    exposure_type,
    other_exposure_type,
    clinical_notes,
    appointment_date,
    voided
 from kenyaemr_etl.etl_clinical_visit;

ALTER TABLE kenyaemr_datatools.clinical_visit ADD FOREIGN KEY (client_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.clinical_visit ADD INDEX(visit_date);
SELECT "Successfully created clinical_visit table";

-- Create table peer_calendar
create table kenyaemr_datatools.peer_calendar as
select
    uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    date_last_modified,
    hotspot_name,
    typology,
    other_hotspots,
    weekly_sex_acts,
    monthly_condoms_required,
    weekly_anal_sex_acts,
    monthly_lubes_required,
    daily_injections,
    monthly_syringes_required,
    years_in_sexwork_drugs,
    experienced_violence,
    service_provided_within_last_month,
    monthly_n_and_s_distributed,
    monthly_male_condoms_distributed,
    monthly_lubes_distributed,
    monthly_female_condoms_distributed,
    monthly_self_test_kits_distributed,
    received_clinical_service,
    violence_reported,
    referred,
    health_edu,
    remarks,
    voided
 from kenyaemr_etl.etl_peer_calendar;

ALTER TABLE kenyaemr_datatools.peer_calendar ADD FOREIGN KEY (client_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.peer_calendar ADD INDEX(visit_date);
SELECT "Successfully created peer_calendar table";

-- Create table sti_treatment
create table kenyaemr_datatools.sti_treatment as
select
  uuid,
	client_id,
	visit_id,
	visit_date,
	location_id,
	encounter_id,
	encounter_provider,
	date_created,
	date_last_modified,
	visit_reason,
	syndrome,
	other_syndrome,
	drug_prescription,
	other_drug_prescription,
	genital_exam_done,
	lab_referral,
	lab_form_number,
	referred_to_facility,
	facility_name,
	partner_referral_done,
	given_lubes,
	no_of_lubes,
	given_condoms,
	no_of_condoms,
	provider_comments,
	provider_name,
	appointment_date,
	voided
 from kenyaemr_etl.etl_sti_treatment;

ALTER TABLE kenyaemr_datatools.sti_treatment ADD FOREIGN KEY (client_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.sti_treatment ADD INDEX(visit_date);
SELECT "Successfully created sti_treatment table";

-- Create table peer_tracking
create table kenyaemr_datatools.peer_tracking as
select
  uuid,
  provider,
  client_id,
  visit_id,
  visit_date,
  location_id,
  encounter_id,
  tracing_attempted,
  tracing_not_attempted_reason,
  attempt_number,
  tracing_date,
  tracing_type,
  tracing_outcome,
  is_final_trace,
  tracing_outcome_status,
  voluntary_exit_comment,
  status_in_program,
  source_of_information,
  other_informant,
  date_created,
  date_last_modified,
  voided
 from kenyaemr_etl.etl_peer_tracking;

ALTER TABLE kenyaemr_datatools.peer_tracking ADD FOREIGN KEY (client_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.peer_tracking ADD INDEX(visit_date);
SELECT "Successfully created peer_tracking table";

-- Create table treatment_verification
create table kenyaemr_datatools.treatment_verification as
select
  uuid,
  provider,
  client_id,
  visit_id,
  visit_date,
  location_id,
  encounter_id,
  date_diagnosed_with_hiv,
  art_health_facility,
  ccc_number,
  is_pepfar_site,
  date_initiated_art,
  current_regimen,
  information_source,
  cd4_test_date,
  cd4,
  vl_test_date,
  viral_load,
  disclosed_status,
  person_disclosed_to,
  other_person_disclosed_to,
  IPT_start_date,
  IPT_completion_date,
  on_diff_care,
  in_support_group,
  support_group_name,
  opportunistic_infection,
  oi_diagnosis_date,
  oi_treatment_start_date,
  oi_treatment_end_date,
  comment,
  date_created,
  date_last_modified,
  voided
 from kenyaemr_etl.etl_treatment_verification;

ALTER TABLE kenyaemr_datatools.treatment_verification ADD FOREIGN KEY (client_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.treatment_verification ADD INDEX(visit_date);
SELECT "Successfully created treatment_verification table";

-- Create table PrEP_verification
create table kenyaemr_datatools.PrEP_verification as
select
    uuid,
    provider,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    date_enrolled,
    health_facility_accessing_PrEP,
    is_pepfar_site,
    date_initiated_PrEP,
    PrEP_regimen,
    information_source,
    PrEP_status,
    verification_date,
    discontinuation_reason,
    other_discontinuation_reason,
    appointment_date,
    date_created,
    date_last_modified,
    voided
 from kenyaemr_etl.etl_PrEP_verification;

ALTER TABLE kenyaemr_datatools.PrEP_verification ADD FOREIGN KEY (client_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.PrEP_verification ADD INDEX(visit_date);
SELECT "Successfully created PrEP_verification table";

UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= script_id;

END$$

