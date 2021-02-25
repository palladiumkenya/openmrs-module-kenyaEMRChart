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
patient_type,
date_first_enrolled_in_care,
(case entry_point when 159938 then "HBTC" when 160539 then "VCT Site" when 159937 then "MCH" when 160536 then "IPD-Adult" 
  when 160537 then "IPD-Child," when 160541 then "TB Clinic" when 160542 then "OPD" when 162050 then "CCC" 
  when 160551 then "Self Test," when 5622 then "Other(eg STI)" else "" end) as entry_point,
transfer_in_date,
facility_transferred_from,
district_transferred_from,
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
(case orphan when 1 then 'Yes' when 2 then 'No' end) as orphan
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
(case visit_scheduled when 1246 then "visit_scheduled" else "" end )as visit_scheduled,
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
(case key_population_type when 105 then "People who inject drugs" when 160578 then "Men who have sex with men" when 160579 then "Female sex Worker" else "" end) as key_population_type,
IF(who_stage in (1204,1220),"WHO Stage1", IF(who_stage in (1205,1221),"WHO Stage2", IF(who_stage in (1206,1222),"WHO Stage3", IF(who_stage in (1207,1223),"WHO Stage4", "")))) as who_stage,
(case presenting_complaints when 1 then "Yes" when 0 then "No" else "" end) as presenting_complaints, 
clinical_notes,
(case on_anti_tb_drugs when 1065 then "Yes" when 1066 then "No" else "" end) as on_anti_tb_drugs,
(case on_ipt when 1065 then "Yes" when 1066 then "No" else "" end) as on_ipt,
(case ever_on_ipt when 1065 then "Yes" when 1066 then "No" else "" end) as ever_on_ipt,
(case spatum_smear_ordered when 1065 then "Yes" when 1066 then "No" else "" end) as spatum_smear_ordered,
(case chest_xray_ordered when 1065 then "Yes" when 1066 then "No" else "" end) as chest_xray_ordered,
(case genexpert_ordered when 1065 then "Yes" when 1066 then "No" else "" end) as genexpert_ordered,
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
transfer_date
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
      viral_load,
      (case ldl when 1302 then "LDL"  else "" end) as ldl,
      (case arv_status when 1148 then "ARV Prophylaxis" when 1149 then "HAART" when 1175 then "NA" else "" end) as arv_status,
      final_test_result,
      patient_given_result,
      (case partner_hiv_tested when 1065 then "Yes" when 1066 then "No" else "" end) as partner_hiv_tested,
      (case partner_hiv_status when 664 then "HIV Negative" when 703 then "HIV Positive" when 1067 then "Unknown" else "" end) as partner_hiv_status,
      (case prophylaxis_given when 105281 then "Cotrimoxazole" when 74250 then "Dapsone" when 1107 then "None" else "" end) as prophylaxis_given,
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
      clinical_notes

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
      (case primary_caregiver when 970 then "Mother" when 973 then "Guardian" when 972 then "Guardian" when 160639 then "Guardian" when 5622 then "Guardian" else "" end) as primary_caregiver,
      (case infant_feeding when 5526 then "Exclusive Breastfeeding(EBF)" when 1595 then "Exclusive Replacement(ERF)" when 6046 then "Mixed Feeding(MF)" else "" end) as infant_feeding,
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
      dna_pcr_sample_date,
      (case dna_pcr_contextual_status when 162081 then "Repeat" when 162083 then "Final test (end of pediatric window)" when 162082 then "Confirmation" when 162080 then "Initial" else "" end) as dna_pcr_contextual_status,
      (case dna_pcr_result when 1138 then "INDETERMINATE" when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1304 then "POOR SAMPLE QUALITY" else "" end) as dna_pcr_result,
      (case azt_given when 86663 then "Yes" else "No" end) as azt_given,
      (case nvp_given when 80586 then "Yes" else "No" end) as nvp_given,
      (case ctx_given when 105281 then "Yes" else "No" end) as ctx_given,
      (case first_antibody_result when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1304 then "POOR SAMPLE QUALITY" else "" end) as first_antibody_result,
      (case final_antibody_result when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1304 then "POOR SAMPLE QUALITY" else "" end) as final_antibody_result,
      (case tetracycline_ointment_given  when 1065 then "Yes" when 1066 then "No" else "" end) as tetracycline_ointment_given,
      (case pupil_examination when 162065 then "Black" when 1075 then "White" else "" end) as pupil_examination,
      (case sight_examination when 1065 then "Following Objects" when 1066 then "Not Following Objects" else "" end) as sight_examination,
      (case squint when 1065 then "Squint" when 1066 then "No Squint" else "" end) as squint,
      (case deworming_drug when 79413 then "Mebendazole" when 70439 then "Albendazole" else "" end) as deworming_drug,
      dosage,
      unit,
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
(case resulting_tb_status when 1660 then "No TB Signs" when 142177 then "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "TB Screening Not Done" else "" end) as resulting_tb_status,
tb_treatment_start_date,
notes
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
      regimen_discontinued,
      date_discontinued,
      (case reason_discontinued when 102 then "Drug toxicity" when 160567 then "New diagnosis of Tuberculosis"  when 160569 then "Virologic failure"
       when 159598 then "Non-compliance with treatment or therapy" when 1754 then "Medications unavailable"
       when 1434 then "Currently pregnant"  when 1253 then "Completed PMTCT"  when 843 then "Regimen failure"
       when 5622 then "Other"else "" end) as reason_discontinued,
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
  as select t.* from kenyaemr_etl.etl_hts_test t
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

create table kenyaemr_datatools.gender_based_violence as select * from kenyaemr_etl.etl_gender_based_violence;
alter table kenyaemr_datatools.gender_based_violence add FOREIGN KEY(client_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.gender_based_violence ADD INDEX(visit_date);

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

UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= script_id;

END$$

