DELIMITER $$

DROP PROCEDURE IF EXISTS create_datatools_tables $$
CREATE PROCEDURE create_datatools_tables()
BEGIN
    DECLARE script_id INT DEFAULT NULL;
    DECLARE target_table VARCHAR(300);
    DECLARE src_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @dynamic_sql = CONCAT('CREATE DATABASE IF NOT EXISTS ', @datatools_schema, ' DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci');
PREPARE stmt FROM @dynamic_sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @dynamic_sql = CONCAT('INSERT INTO ', @script_status_table, ' (script_name, start_time) VALUES (''KenyaEMR_Data_Tool'', NOW())');
PREPARE stmt FROM @dynamic_sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET script_id = LAST_INSERT_ID();

    SET target_table = CONCAT(@datatools_schema, '.`patient_demographics`');
    SET src_table = CONCAT(@etl_schema, '.`etl_patient_demographics`');

    SET @dynamic_sql = CONCAT('DROP TABLE IF EXISTS ', target_table);
PREPARE stmt FROM @dynamic_sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @dynamic_sql = CONCAT(
        'CREATE TABLE ', target_table, ' ENGINE=InnoDB AS ',
        'SELECT patient_id, uuid, given_name, middle_name, family_name, Gender, DOB, national_id_no, huduma_no, ',
        'unique_patient_no, national_unique_patient_identifier, nhif_number, sha_number, shif_number, ',
        'phone_number, birth_place, citizenship, email_address, occupation, next_of_kin, ',
        'next_of_kin_relationship, marital_status, education_level, ',
        'IF(dead=1, ''Yes'', ''No'') AS dead, death_date, voided ',
        'FROM ', src_table
    );
PREPARE stmt FROM @dynamic_sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @dynamic_sql = CONCAT('ALTER TABLE ', target_table, ' ADD PRIMARY KEY(patient_id)');
PREPARE stmt FROM @dynamic_sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- ---------------------------------------------------------
-- 5. TABLE: HIV Enrollment
-- ---------------------------------------------------------
SET target_table = CONCAT(@datatools_schema, '.`hiv_enrollment`');
    SET src_table = CONCAT(@etl_schema, '.`etl_hiv_enrollment`');
    SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_table);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
        'CREATE TABLE ', target_table, ' ENGINE=InnoDB AS ',
        'SELECT patient_id, uuid, visit_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, patient_type, ',
        'date_first_enrolled_in_care, entry_point, transfer_in_date, facility_transferred_from, district_transferred_from, ',
        'CASE previous_regimen ',
          'WHEN 164968 THEN ''AZT/3TC/DTG'' WHEN 164969 THEN ''TDF/3TC/DTG'' WHEN 164970 THEN ''ABC/3TC/DTG'' WHEN 164505 THEN ''TDF-3TC-EFV'' ',
          'WHEN 792 THEN ''D4T/3TC/NVP'' WHEN 160124 THEN ''AZT/3TC/EFV'' WHEN 160104 THEN ''D4T/3TC/EFV'' WHEN 1652 THEN ''3TC/NVP/AZT'' ',
          'WHEN 161361 THEN ''EDF/3TC/EFV'' WHEN 104565 THEN ''EFV/FTC/TDF'' WHEN 162201 THEN ''3TC/LPV/TDF/r'' WHEN 817 THEN ''ABC/3TC/AZT'' ',
          'WHEN 162199 THEN ''ABC/NVP/3TC'' WHEN 162200 THEN ''3TC/ABC/LPV/r'' WHEN 162565 THEN ''3TC/NVP/TDF'' WHEN 162561 THEN ''3TC/AZT/LPV/r'' ',
          'WHEN 164511 THEN ''AZT-3TC-ATV/r'' WHEN 164512 THEN ''TDF-3TC-ATV/r'' WHEN 162560 THEN ''3TC/D4T/LPV/r'' WHEN 162563 THEN ''3TC/ABC/EFV'' ',
          'WHEN 162562 THEN ''ABC/LPV/R/TDF'' WHEN 162559 THEN ''ABC/DDI/LPV/r'' ELSE NULL END AS previous_regimen, ',
        'date_started_art_at_transferring_facility, date_confirmed_hiv_positive, facility_confirmed_hiv_positive, ',
        'CASE arv_status WHEN 1 THEN ''Yes'' WHEN 0 THEN ''No'' ELSE '''' END AS arv_status, ',
        'CASE ever_on_pmtct WHEN 1065 THEN ''Yes'' ELSE '''' END AS ever_on_pmtct, ',
        'CASE ever_on_pep WHEN 1 THEN ''Yes'' ELSE '''' END AS ever_on_pep, ',
        'CASE ever_on_prep WHEN 1065 THEN ''Yes'' ELSE '''' END AS ever_on_prep, ',
        'CASE ever_on_haart WHEN 1185 THEN ''Yes'' ELSE '''' END AS ever_on_haart, ',
        'IF(who_stage IN (1204,1220), ''WHO Stage1'', IF(who_stage IN (1205,1221), ''WHO Stage2'', IF(who_stage IN (1206,1222), ''WHO Stage3'', IF(who_stage IN (1207,1223), ''WHO Stage4'', '''')))) AS who_stage, ',
        'name_of_treatment_supporter, ',
        'CASE relationship_of_treatment_supporter WHEN 973 THEN ''Grandparent'' WHEN 972 THEN ''Sibling'' WHEN 160639 THEN ''Guardian'' WHEN 1527 THEN ''Parent'' ',
          'WHEN 5617 THEN ''Spouse'' WHEN 163565 THEN ''Partner'' WHEN 5622 THEN ''Other'' ELSE '''' END AS relationship_of_treatment_supporter, ',
        'treatment_supporter_telephone, treatment_supporter_address, CASE in_school WHEN 1 THEN ''Yes'' WHEN 2 THEN ''No'' END AS in_school, ',
        'CASE orphan WHEN 1 THEN ''Yes'' WHEN 2 THEN ''No'' END AS orphan, date_of_discontinuation, ',
        'CASE discontinuation_reason WHEN 159492 THEN ''Transferred Out'' WHEN 160034 THEN ''Died'' WHEN 5240 THEN ''Lost to Follow'' WHEN 819 THEN ''Cannot afford Treatment'' ',
          'WHEN 5622 THEN ''Other'' WHEN 1067 THEN ''Unknown'' ELSE '''' END AS discontinuation_reason, voided ',
        'FROM ', src_table
    );
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', target_table, ' ADD PRIMARY KEY (encounter_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('UPDATE ', @script_status_table, ' SET stop_time = NOW() WHERE id = ', script_id);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT("Successfully created DataTools in ", @datatools_schema_raw) AS Result;


-- ----------------------------------- create table hiv_followup ----------------------------------------------
-- sql
SET @target_hiv_quoted = CONCAT('`', @datatools_schema, '`.`hiv_followup`');
SET @src_hiv_quoted = CONCAT('`', @etl_schema, '`.`etl_patient_hiv_followup`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_hiv_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_hiv_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, patient_id, visit_id, visit_date, location_id, encounter_id, encounter_provider, date_created, ',
    '(CASE visit_scheduled WHEN 1 THEN "scheduled" WHEN 2 THEN "unscheduled" ELSE "" END) AS visit_scheduled, ',
    '(CASE person_present WHEN 978 THEN "Self (SF)" WHEN 161642 THEN "Treatment supporter (TS)" WHEN 5622 THEN "Other" ELSE "" END) AS person_present, ',
    'weight, systolic_pressure, diastolic_pressure, height, temperature, pulse_rate, respiratory_rate, oxygen_saturation, muac, z_score_absolute, ',
    '(CASE z_score WHEN 1115 THEN "Normal (Median)" WHEN 123814 THEN "Mild (-1 SD)" WHEN 123815 THEN "Moderate (-2 SD)" WHEN 164131 THEN "Severe (-3 SD and -4 SD)" ELSE "" END) AS z_score, ',
    '(CASE nutritional_status WHEN 1115 THEN "Normal" WHEN 163302 THEN "Severe acute malnutrition" WHEN 163303 THEN "Moderate acute malnutrition" WHEN 114413 THEN "Overweight/Obese" ELSE "" END) AS nutritional_status, ',
    '(CASE population_type WHEN 164928 THEN "General Population" WHEN 164929 THEN "Key Population" ELSE "" END) AS population_type, ',
    '(CASE key_population_type WHEN 105 THEN "People who inject drugs" WHEN 160578 THEN "Men who have sex with men" WHEN 160579 THEN "Female sex Worker" WHEN 162277 THEN "People in prison and other closed settings" ELSE "" END) AS key_population_type, ',
    'IF(who_stage IN (1204,1220),"WHO Stage1", IF(who_stage IN (1205,1221),"WHO Stage2", IF(who_stage IN (1206,1222),"WHO Stage3", IF(who_stage IN (1207,1223),"WHO Stage4", "")))) AS who_stage, ',
    'who_stage_associated_oi, ',
    '(CASE presenting_complaints WHEN 1 THEN "Yes" WHEN 0 THEN "No" ELSE "" END) AS presenting_complaints, ',
    'clinical_notes, ',
    '(CASE on_anti_tb_drugs WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS on_anti_tb_drugs, ',
    '(CASE on_ipt WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS on_ipt, ',
    '(CASE ever_on_ipt WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS ever_on_ipt, ',
    '(CASE cough WHEN 159799 THEN "Yes" ELSE "" END) AS cough, ',
    '(CASE fever WHEN 1494 THEN "Yes" ELSE "" END) AS fever, ',
    '(CASE weight_loss_poor_gain WHEN 832 THEN "Yes" ELSE "" END) AS weight_loss_poor_gain, ',
    '(CASE night_sweats WHEN 133027 THEN "Yes" ELSE "" END) AS night_sweats, ',
    '(CASE tb_case_contact WHEN 124068 THEN "Yes" ELSE "" END) AS tb_case_contact, ',
    '(CASE lethargy WHEN 116334 THEN "Yes" ELSE "" END) AS lethargy, ',
    'screened_for_tb, ',
    '(CASE spatum_smear_ordered WHEN 307 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS spatum_smear_ordered, ',
    '(CASE chest_xray_ordered WHEN 12 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS chest_xray_ordered, ',
    '(CASE genexpert_ordered WHEN 162202 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS genexpert_ordered, ',
    '(CASE spatum_smear_result WHEN 703 THEN "POSITIVE" WHEN 664 THEN "NEGATIVE" ELSE "" END) AS spatum_smear_result, ',
    '(CASE chest_xray_result WHEN 1115 THEN "NORMAL" WHEN 152526 THEN "ABNORMAL" ELSE "" END) AS chest_xray_result, ',
    '(CASE genexpert_result WHEN 664 THEN "NEGATIVE" WHEN 162203 THEN "Mycobacterium tuberculosis detected with rifampin resistance" WHEN 162204 THEN "Mycobacterium tuberculosis detected without rifampin resistance" WHEN 164104 THEN "Mycobacterium TB with indeterminate rifampin resistance" WHEN 163611 THEN "Invalid" WHEN 1138 THEN "INDETERMINATE" ELSE "" END) AS genexpert_result, ',
    '(CASE referral WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS referral, ',
    '(CASE clinical_tb_diagnosis WHEN 703 THEN "POSITIVE" WHEN 664 THEN "NEGATIVE" ELSE "" END) AS clinical_tb_diagnosis, ',
    '(CASE contact_invitation WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS contact_invitation, ',
    '(CASE evaluated_for_ipt WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS evaluated_for_ipt, ',
    '(CASE has_known_allergies WHEN 1 THEN "Yes" WHEN 0 THEN "No" ELSE "" END) AS has_known_allergies, ',
    '(CASE has_chronic_illnesses_cormobidities WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS has_chronic_illnesses_cormobidities, ',
    '(CASE has_adverse_drug_reaction WHEN 1 THEN "Yes" WHEN 0 THEN "No" ELSE "" END) AS has_adverse_drug_reaction, ',
    '(CASE pregnancy_status WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS pregnancy_status, ',
    '(CASE breastfeeding WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS breastfeeding, ',
    '(CASE wants_pregnancy WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS wants_pregnancy, ',
    '(CASE pregnancy_outcome WHEN 126127 THEN "Spontaneous abortion" WHEN 125872 THEN "STILLBIRTH" WHEN 1395 THEN "Term birth of newborn" WHEN 129218 THEN "Preterm Delivery (Maternal Condition)" WHEN 159896 THEN "Therapeutic abortion procedure" WHEN 151849 THEN "Liveborn, Unspecified Whether Single, Twin, or Multiple" WHEN 1067 THEN "Unknown" ELSE "" END) AS pregnancy_outcome, ',
    'anc_number, expected_delivery_date, ',
    '(CASE ever_had_menses WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" WHEN 1175 THEN "N/A" END) AS ever_had_menses, ',
    'last_menstrual_period, ',
    '(CASE menopausal WHEN 113928 THEN "Yes" END) AS menopausal, ',
    'gravida, parity, full_term_pregnancies, abortion_miscarriages, ',
    '(CASE family_planning_status WHEN 965 THEN "On Family Planning" WHEN 160652 THEN "Not using Family Planning" WHEN 1360 THEN "Wants Family Planning" ELSE "" END) AS family_planning_status, ',
    '(CASE family_planning_method WHEN 160570 THEN "Emergency contraceptive pills" WHEN 780 THEN "Oral Contraceptives Pills" WHEN 5279 THEN "Injectible" WHEN 1359 THEN "Implant" WHEN 5275 THEN "Intrauterine Device" WHEN 136163 THEN "Lactational Amenorhea Method" WHEN 5278 THEN "Diaphram/Cervical Cap" WHEN 5277 THEN "Fertility Awareness" WHEN 1472 THEN "Tubal Ligation" WHEN 190 THEN "Condoms" WHEN 1489 THEN "Vasectomy" WHEN 162332 THEN "Undecided" ELSE "" END) AS family_planning_method, ',
    '(CASE reason_not_using_family_planning WHEN 160572 THEN "Thinks can not get pregnant" WHEN 160573 THEN "Not sexually active now" WHEN 5622 THEN "Other" ELSE "" END) AS reason_not_using_family_planning, ',
    '(CASE tb_status WHEN 1660 THEN "No TB Signs" WHEN 142177 THEN "Presumed TB" WHEN 1662 THEN "TB Confirmed" WHEN 160737 THEN "TB Screening Not Done" ELSE "" END) AS tb_status, ',
    'tb_treatment_no, general_examination, ',
    '(CASE system_examination WHEN 1115 THEN "NORMAL" WHEN 1116 THEN "ABNORMAL" END) AS system_examination, ',
    '(CASE skin_findings WHEN 150555 THEN "Abscess" WHEN 125201 THEN "Swelling/Growth" WHEN 135591 THEN "Hair Loss" WHEN 136455 THEN "Itching" WHEN 507 THEN "Kaposi Sarcoma" WHEN 1249 THEN "Skin eruptions/Rashes" WHEN 5244 THEN "Oral sores" END) AS skin_findings, ',
    '(CASE eyes_findings WHEN 123074 THEN "Visual Disturbance" WHEN 140940 THEN "Excessive tearing" WHEN 131040 THEN "Eye pain" WHEN 127777 THEN "Eye redness" WHEN 140827 THEN "Light sensitive" WHEN 139100 THEN "Itchy eyes" END) AS eyes_findings, ',
    '(CASE ent_findings WHEN 148517 THEN "Apnea" WHEN 139075 THEN "Hearing disorder" WHEN 119558 THEN "Dental caries" WHEN 118536 THEN "Erythema" WHEN 106 THEN "Frequent colds" WHEN 147230 THEN "Gingival bleeding" WHEN 135841 THEN "Hairy cell leukoplakia" WHEN 117698 THEN "Hearing loss" WHEN 138554 THEN "Hoarseness" WHEN 507 THEN "Kaposi Sarcoma" WHEN 152228 THEN "Masses" WHEN 128055 THEN "Nasal discharge" WHEN 133499 THEN "Nosebleed" WHEN 160285 THEN "Pain" WHEN 110099 THEN "Post nasal discharge" WHEN 126423 THEN "Sinus problems" WHEN 126318 THEN "Snoring" WHEN 158843 THEN "Sore throat" WHEN 5244 THEN "Oral sores" WHEN 5334 THEN "Thrush" WHEN 123588 THEN "Tinnitus" WHEN 124601 THEN "Toothache" WHEN 123919 THEN "Ulcers" WHEN 111525 THEN "Vertigo" END) AS ent_findings, ',
    '(CASE chest_findings WHEN 146893 THEN "Bronchial breathing" WHEN 127640 THEN "Crackles" WHEN 145712 THEN "Dullness" WHEN 164440 THEN "Reduced breathing" WHEN 127639 THEN "Respiratory distress" WHEN 5209 THEN "Wheezing" END) AS chest_findings, ',
    '(CASE cvs_findings WHEN 140147 THEN "Elevated blood pressure" WHEN 136522 THEN "Irregular heartbeat" WHEN 562 THEN "Cardiac murmur" WHEN 130560 THEN "Cardiac rub" END) AS cvs_findings, ',
    '(CASE abdomen_findings WHEN 150915 THEN "Abdominal distension" WHEN 5008 THEN "Hepatomegaly" WHEN 5103 THEN "Abdominal mass" WHEN 5009 THEN "Splenomegaly" WHEN 5105 THEN "Abdominal tenderness" END) AS abdomen_findings, ',
    '(CASE cns_findings WHEN 118872 THEN "Altered sensations" WHEN 1836 THEN "Bulging fontenelle" WHEN 150817 THEN "Abnormal reflexes" WHEN 120345 THEN "Confusion" WHEN 157498 THEN "Limb weakness" WHEN 112721 THEN "Stiff neck" WHEN 136282 THEN "Kernicterus" END) AS cns_findings, ',
    '(CASE genitourinary_findings WHEN 147241 THEN "Bleeding" WHEN 154311 THEN "Rectal discharge" WHEN 123529 THEN "Urethral discharge" WHEN 123396 THEN "Vaginal discharge" WHEN 124087 THEN "Ulceration" END) AS genitourinary_findings, ',
    '(CASE prophylaxis_given WHEN 105281 THEN "Cotrimoxazole" WHEN 74250 THEN "Dapsone" WHEN 1107 THEN "None" END) AS prophylaxis_given, ',
    '(CASE ctx_adherence WHEN 159405 THEN "Good" WHEN 163794 THEN "Inadequate" WHEN 159407 THEN "Poor" ELSE "" END) AS ctx_adherence, ',
    '(CASE ctx_dispensed WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" WHEN 1175 THEN "Not applicable" ELSE "" END) AS ctx_dispensed, ',
    '(CASE dapsone_adherence WHEN 159405 THEN "Good" WHEN 163794 THEN "Inadequate" WHEN 159407 THEN "Poor" ELSE "" END) AS dapsone_adherence, ',
    '(CASE dapsone_dispensed WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" WHEN 1175 THEN "Not applicable" ELSE "" END) AS dapsone_dispensed, ',
    '(CASE inh_dispensed WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" WHEN 1175 THEN "Not applicable" ELSE "" END) AS inh_dispensed, ',
    '(CASE arv_adherence WHEN 159405 THEN "Good" WHEN 163794 THEN "Inadequate" WHEN 159407 THEN "Poor" ELSE "" END) AS arv_adherence, ',
    '(CASE poor_arv_adherence_reason WHEN 102 THEN "Toxicity, drug" WHEN 121725 THEN "Alcohol abuse" WHEN 119537 THEN "Depression" WHEN 5622 THEN "Other" WHEN 1754 THEN "Medications unavailable" WHEN 1778 THEN "TREATMENT OR PROCEDURE NOT CARRIED OUT DUE TO FEAR OF SIDE EFFECTS" WHEN 819 THEN "Cannot afford treatment" WHEN 160583 THEN "Shares medications with others" WHEN 160584 THEN "Lost or ran out of medication" WHEN 160585 THEN "Felt too ill to take medication" WHEN 160586 THEN "Felt better and stopped taking medication" WHEN 160587 THEN "Forgot to take medication" WHEN 160588 THEN "Pill burden" WHEN 160589 THEN "Concerned about privacy/stigma" WHEN 820 THEN "TRANSPORT PROBLEMS" ELSE "" END) AS poor_arv_adherence_reason, ',
    'poor_arv_adherence_reason_other, ',
    '(CASE pwp_disclosure WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" WHEN 1067 THEN "Unknown" WHEN 1175 THEN "N/A" ELSE "" END) AS pwp_disclosure, ',
    '(CASE pwp_pead_disclosure WHEN 1066 THEN "No disclosure" WHEN 162979 THEN "Partial disclosure" WHEN 166982 THEN "Full disclosure" ELSE "" END) AS pwp_pead_disclosure, ',
    '(CASE pwp_partner_tested WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" WHEN 1067 THEN "Unknown" WHEN 1175 THEN "N/A" ELSE "" END) AS pwp_partner_tested, ',
    '(CASE condom_provided WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" WHEN 1067 THEN "Unknown" WHEN 1175 THEN "N/A" ELSE "" END) AS condom_provided, ',
    '(CASE substance_abuse_screening WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" WHEN 1067 THEN "Unknown" ELSE "" END) AS substance_abuse_screening, ',
    '(CASE screened_for_sti WHEN 703 THEN "POSITIVE" WHEN 664 THEN "NEGATIVE" WHEN 1118 THEN "Not Done" WHEN 1175 THEN "N/A" ELSE "" END) AS screened_for_sti, ',
    '(CASE cacx_screening WHEN 703 THEN "POSITIVE" WHEN 664 THEN "NEGATIVE" WHEN 1118 THEN "Not Done" WHEN 1175 THEN "N/A" ELSE "" END) AS cacx_screening, ',
    '(CASE sti_partner_notification WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS sti_partner_notification, ',
    '(CASE experienced_gbv WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" END) AS experienced_gbv, ',
    '(CASE depression_screening WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" END) AS depression_screening, ',
    '(CASE established_differentiated_care WHEN 164942 THEN "Standard Care" WHEN 164943 THEN "Fast Track" WHEN 166443 THEN "Health care worker Led facility ART group(HFAG)" WHEN 166444 THEN "Peer Led Facility ART Group(PFAG)" WHEN 1555 THEN "Health care worker Led Community ART group(HCAG)" WHEN 164945 THEN "Peer Led Community ART Group(PCAG)" WHEN 1000478 THEN "Community Pharmacy(CP)" WHEN 164944 THEN "Community ART Distribution Points(CAPD)" WHEN 166583 THEN "Individual patient ART Community Distribution(IACD)" END) AS established_differentiated_care, ',
    '(CASE at_risk_population WHEN 105 THEN "People who inject drugs" WHEN 160578 THEN "Men who have sex with men" WHEN 160579 THEN "Female sex Worker" ELSE "" END) AS at_risk_population, ',
    '(CASE system_review_finding WHEN 1115 THEN "NORMAL" WHEN 1116 THEN "ABNORMAL" ELSE "" END) AS system_review_finding, ',
    'next_appointment_date, refill_date, ',
    '(CASE appointment_consent WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE "" END) AS appointment_consent, ',
    '(CASE next_appointment_reason WHEN 160523 THEN "Follow up" WHEN 1283 THEN "Lab tests" WHEN 159382 THEN "Counseling" WHEN 160521 THEN "Pharmacy Refill" WHEN 5622 THEN "Other" ELSE "" END) AS next_appointment_reason, ',
    '(CASE stability WHEN 1 THEN "Yes" WHEN 2 THEN "No" WHEN 0 THEN "No" WHEN 1175 THEN "Not applicable" ELSE "" END) AS stability, ',
    '(CASE differentiated_care_group WHEN 1537 THEN "Facility ART distribution group" WHEN 163488 THEN "Community ART distribution group" END) AS differentiated_care_group, ',
    '(CASE differentiated_care WHEN 164942 THEN "Standard Care" WHEN 164943 THEN "Fast Track" WHEN 166443 THEN "Health care worker Led facility ART group(HFAG)" WHEN 166444 THEN "Peer Led Facility ART Group(PFAG)" WHEN 1555 THEN "Health care worker Led Community ART group(HCAG)" WHEN 164945 THEN "Peer Led Community ART Group(PCAG)" WHEN 1000478 THEN "Community Pharmacy(CP)" WHEN 164944 THEN "Community ART Distribution Points(CAPD)" WHEN 166583 THEN "Individual patient ART Community Distribution(IACD)" END) AS differentiated_care, ',
    '(CASE insurance_type WHEN 1917 THEN "NHIF" WHEN 1107 THEN "None" WHEN 5622 THEN "Other" ELSE "" END) AS insurance_type, ',
    'other_insurance_specify, ',
    '(CASE insurance_status WHEN 161636 THEN "Active" WHEN 1118 THEN "Inactive" ELSE "" END) AS insurance_status ',
  'FROM ', src_hiv_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hiv_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hiv_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hiv_quoted, ' ADD INDEX(pregnancy_status)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hiv_quoted, ' ADD INDEX(breastfeeding)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hiv_quoted, ' ADD INDEX(family_planning_status)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hiv_quoted, ' ADD INDEX(tb_status)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hiv_quoted, ' ADD INDEX(ctx_dispensed)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hiv_quoted, ' ADD INDEX(population_type)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hiv_quoted, ' ADD INDEX(on_anti_tb_drugs)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hiv_quoted, ' ADD INDEX(stability)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hiv_quoted, ' ADD INDEX(differentiated_care)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_hiv_quoted) AS message;


-- -------------------------------- create table laboratory_extract ------------------------------------------

SET @target_lab_quoted = CONCAT('`', @datatools_schema, '`.`laboratory_extract`');
SET @src_lab_quoted = CONCAT('`', @etl_schema, '`.`etl_laboratory_extract`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_lab_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_lab_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, encounter_id, patient_id, location_id, visit_date, visit_id, order_id, lab_test, urgency, order_reason, ',
    'order_test_name, obs_id, result_test_name, result_name, set_member_conceptId, test_result, ',
    'date_test_requested, date_test_result_received, test_requested_by, date_created, date_last_modified, created_by ',
  'FROM ', src_lab_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_lab_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_lab_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_lab_quoted, ' ADD INDEX(lab_test)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_lab_quoted, ' ADD INDEX(test_result)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_lab_quoted) AS message;


-- create table pharmacy_extract

SET @target_pharm_quoted = CONCAT('`', @datatools_schema, '`.`pharmacy_extract`');
SET @src_pharm_quoted = CONCAT('`', @etl_schema, '`.`etl_pharmacy_extract`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_pharm_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_pharm_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, uuid, visit_date, visit_id, encounter_id, date_created, encounter_name, drug, drug_name, ',
    '(CASE is_arv WHEN 1 THEN ''Yes'' ELSE ''No'' END) AS is_arv, ',
    '(CASE is_ctx WHEN 105281 THEN ''SULFAMETHOXAZOLE / TRIMETHOPRIM (CTX)'' ELSE '''' END) AS is_ctx, ',
    '(CASE is_dapsone WHEN 74250 THEN ''DAPSONE'' ELSE '''' END) AS is_dapsone, ',
    'frequency, duration, duration_units, voided, date_voided, dispensing_provider ',
  'FROM ', src_pharm_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_pharm_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_pharm_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_pharm_quoted, ' ADD INDEX(drug)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_pharm_quoted, ' ADD INDEX(is_arv)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_pharm_quoted) AS message;

-- create table patient_program_discontinuation

SET @target_ppd_quoted = CONCAT('`', @datatools_schema, '`.`patient_program_discontinuation`');
SET @src_ppd_quoted = CONCAT('`', @etl_schema, '`.`etl_patient_program_discontinuation`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_ppd_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_ppd_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, uuid, visit_id, visit_date, program_uuid, program_name, encounter_id, ',
    '(CASE discontinuation_reason WHEN 159492 THEN ''Transferred Out'' WHEN 160034 THEN ''Died'' WHEN 160432 THEN ''Died'' WHEN 5240 THEN ''Lost to Follow'' WHEN 819 THEN ''Cannot afford Treatment'' WHEN 5622 THEN ''Other'' WHEN 1067 THEN ''Unknown'' WHEN 164349 THEN ''Stopped Treatment'' ELSE '''' END) AS discontinuation_reason, ',
    'date_died, transfer_facility, transfer_date, ',
    '(CASE death_reason WHEN 163324 THEN ''HIV disease resulting in TB'' WHEN 116030 THEN ''HIV disease resulting in cancer'' WHEN 160159 THEN ''HIV disease resulting in other infectious and parasitic diseases'' WHEN 160158 THEN ''Other HIV disease resulting in other diseases or conditions leading to death'' WHEN 145439 THEN ''Other HIV disease resulting in other diseases or conditions leading to death'' WHEN 133478 THEN ''Other natural causes not directly related to HIV'' WHEN 123812 THEN ''Non-natural causes'' WHEN 42917 THEN ''Unknown cause'' ELSE '''' END) AS death_reason, ',
    '(CASE specific_death_cause ',
      'WHEN 165609 THEN ''COVID-19 Complications'' ',
      'WHEN 145439 THEN ''Non-communicable diseases such as Diabetes and hypertension'' ',
      'WHEN 156673 THEN ''HIV disease resulting in mycobacterial infection'' ',
      'WHEN 155010 THEN ''HIV disease resulting in Kaposis sarcoma'' ',
      'WHEN 156667 THEN ''HIV disease resulting in Burkitts lymphoma'' ',
      'WHEN 115195 THEN ''HIV disease resulting in other types of non-Hodgkin lymphoma'' ',
      'WHEN 157593 THEN ''HIV disease resulting in other malignant neoplasms of lymphoid and haematopoietic and related tissue'' ',
      'WHEN 156672 THEN ''HIV disease resulting in multiple malignant neoplasms'' ',
      'WHEN 159988 THEN ''HIV disease resulting in other malignant neoplasms'' ',
      'WHEN 5333 THEN ''HIV disease resulting in other bacterial infections'' ',
      'WHEN 116031 THEN ''HIV disease resulting in unspecified malignant neoplasms'' ',
      'WHEN 123122 THEN ''HIV disease resulting in other viral infections'' ',
      'WHEN 156669 THEN ''HIV disease resulting in cytomegaloviral disease'' ',
      'WHEN 156668 THEN ''HIV disease resulting in candidiasis'' ',
      'WHEN 5350 THEN ''HIV disease resulting in other mycoses'' ',
      'WHEN 882 THEN ''HIV disease resulting in Pneumocystis jirovecii pneumonia - HIV disease resulting in Pneumocystis carinii pneumonia'' ',
      'WHEN 156671 THEN ''HIV disease resulting in multiple infections'' ',
      'WHEN 160159 THEN ''HIV disease resulting in other infectious and parasitic diseases'' ',
      'WHEN 171 THEN ''HIV disease resulting in unspecified infectious or parasitic disease - HIV disease resulting in infection NOS'' ',
      'WHEN 156670 THEN ''HIV disease resulting in other specified diseases including encephalopathy or lymphoid interstitial pneumonitis or wasting syndrome and others'' ',
      'WHEN 160160 THEN ''HIV disease resulting in other conditions including acute HIV infection syndrome or persistent generalized lymphadenopathy or hematological and immunological abnormalities and others'' ',
      'WHEN 161548 THEN ''HIV disease resulting in Unspecified HIV disease'' ELSE '''' END) AS specific_death_cause, ',
    'natural_causes, non_natural_cause ',
  'FROM ', src_ppd_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ppd_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ppd_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ppd_quoted, ' ADD INDEX(discontinuation_reason)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_ppd_quoted) AS message;


-- create table mch_enrollment

SET @target_mch_quoted = CONCAT('`', @datatools_schema, '`.`mch_enrollment`');
SET @src_mch_quoted = CONCAT('`', @etl_schema, '`.`etl_mch_enrollment`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_mch_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_mch_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, uuid, visit_id, visit_date, location_id, encounter_id, ',
    '(CASE service_type WHEN 1622 THEN ''ANC'' WHEN 164835 THEN ''Delivery'' WHEN 1623 THEN ''PNC'' ELSE '''' END) AS service_type, ',
    'anc_number, first_anc_visit_date, gravida, parity, parity_abortion, age_at_menarche, lmp, lmp_estimated, edd_ultrasound, ',
    '(CASE blood_group WHEN 690 THEN ''A POSITIVE'' WHEN 692 THEN ''A NEGATIVE'' WHEN 694 THEN ''B POSITIVE'' WHEN 696 THEN ''B NEGATIVE'' WHEN 699 THEN ''O POSITIVE'' ',
      'WHEN 701 THEN ''O NEGATIVE'' WHEN 1230 THEN ''AB POSITIVE'' WHEN 1231 THEN ''AB NEGATIVE'' ELSE '''' END) AS blood_group, ',
    '(CASE serology WHEN 1228 THEN ''REACTIVE'' WHEN 1229 THEN ''NON-REACTIVE'' WHEN 1304 THEN ''POOR SAMPLE QUALITY'' ELSE '''' END) AS serology, ',
    '(CASE tb_screening WHEN 664 THEN ''NEGATIVE'' WHEN 703 THEN ''POSITIVE'' ELSE '''' END) AS tb_screening, ',
    '(CASE bs_for_mps WHEN 664 THEN ''NEGATIVE'' WHEN 703 THEN ''POSITIVE'' WHEN 1138 THEN ''INDETERMINATE'' ELSE '''' END) AS bs_for_mps, ',
    '(CASE hiv_status WHEN 164142 THEN ''Revisit'' WHEN 703 THEN ''Known Positive'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS hiv_status, ',
    'hiv_test_date, ',
    '(CASE partner_hiv_status WHEN 664 THEN ''HIV Negative'' WHEN 703 THEN ''HIV Positive'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS partner_hiv_status, ',
    'partner_hiv_test_date, ',
    'ti_date_started_art, ',
    '(CASE ti_current_regimen WHEN 164968 THEN ''AZT/3TC/DTG'' ',
       'WHEN 164969 THEN ''TDF/3TC/DTG'' WHEN 164970 THEN ''ABC/3TC/DTG'' WHEN 164505 THEN ''TDF-3TC-EFV'' ',
       'WHEN 792 THEN ''D4T/3TC/NVP'' WHEN 160124 THEN ''AZT/3TC/EFV'' WHEN 160104 THEN ''D4T/3TC/EFV'' WHEN 1652 THEN ''3TC/NVP/AZT'' ',
       'WHEN 161361 THEN ''EDF/3TC/EFV'' WHEN 104565 THEN ''EFV/FTC/TDF'' WHEN 162201 THEN ''3TC/LPV/TDF/r'' WHEN 817 THEN ''ABC/3TC/AZT'' ',
       'WHEN 162199 THEN ''ABC/NVP/3TC'' WHEN 162200 THEN ''3TC/ABC/LPV/r'' WHEN 162565 THEN ''3TC/NVP/TDF'' WHEN 162561 THEN ''3TC/AZT/LPV/r'' ',
       'WHEN 164511 THEN ''AZT-3TC-ATV/r'' WHEN 164512 THEN ''TDF-3TC-ATV/r'' WHEN 162560 THEN ''3TC/D4T/LPV/r'' WHEN 162563 THEN ''3TC/ABC/EFV'' ',
       'WHEN 162562 THEN ''ABC/LPV/R/TDF'' WHEN 162559 THEN ''ABC/DDI/LPV/r'' ELSE NULL END) AS ti_current_regimen, ',
    'ti_care_facility, urine_microscopy, ',
    '(CASE urinary_albumin WHEN 664 THEN ''Negative'' WHEN 1874 THEN ''Trace - 15'' WHEN 1362 THEN ''One Plus(+) - 30'' WHEN 1363 THEN ''Two Plus(++) - 100'' WHEN 1364 THEN ''Three Plus(+++) - 300'' WHEN 1365 THEN ''Four Plus(++++) - 1000'' ELSE '''' END) AS urinary_albumin, ',
    '(CASE glucose_measurement WHEN 1115 THEN ''Normal'' WHEN 1874 THEN ''Trace'' WHEN 1362 THEN ''One Plus(+)'' WHEN 1363 THEN ''Two Plus(++)'' WHEN 1364 THEN ''Three Plus(+++)'' WHEN 1365 THEN ''Four Plus(++++)'' ELSE '''' END) AS glucose_measurement, ',
    'urine_ph, urine_gravity, ',
    '(CASE urine_nitrite_test WHEN 664 THEN ''NEGATIVE'' WHEN 703 THEN ''POSITIVE'' WHEN 1362 THEN ''One Plus(+)'' WHEN 1363 THEN ''Two Plus(++)'' ELSE '''' END) AS urine_nitrite_test, ',
    '(CASE urine_leukocyte_esterace_test WHEN 664 THEN ''NEGATIVE'' WHEN 1874 THEN ''Trace'' WHEN 1362 THEN ''One Plus(+)'' WHEN 1363 THEN ''Two Plus(++)'' WHEN 1364 THEN ''Three Plus(+++)'' ELSE '''' END) AS urine_leukocyte_esterace_test, ',
    '(CASE urinary_ketone WHEN 664 THEN ''NEGATIVE'' WHEN 1874 THEN ''Trace - 5'' WHEN 1362 THEN ''One Plus(+) - 15'' WHEN 1363 THEN ''Two Plus(++) - 50'' WHEN 1364 THEN ''Three Plus(+++) - 150'' ELSE '''' END) AS urinary_ketone, ',
    '(CASE urine_bile_salt_test WHEN 1115 THEN ''Normal'' WHEN 1874 THEN ''Trace - 1'' WHEN 1362 THEN ''One Plus(+) - 4'' WHEN 1363 THEN ''Two Plus(++) - 8'' WHEN 1364 THEN ''Three Plus(+++) - 12'' ELSE '''' END) AS urine_bile_salt_test, ',
    '(CASE urine_bile_pigment_test WHEN 664 THEN ''NEGATIVE'' WHEN 1362 THEN ''One Plus(+)'' WHEN 1363 THEN ''Two Plus(++)'' WHEN 1364 THEN ''Three Plus(+++)'' ELSE '''' END) AS urine_bile_pigment_test, ',
    '(CASE urine_colour WHEN 162099 THEN ''Colourless'' WHEN 127778 THEN ''Red color'' WHEN 162097 THEN ''Light yellow colour'' WHEN 162105 THEN ''Yellow-green colour'' WHEN 162098 THEN ''Dark yellow colour'' WHEN 162100 THEN ''Brown color'' ELSE '''' END) AS urine_colour, ',
    '(CASE urine_turbidity WHEN 162102 THEN ''Urine appears clear'' WHEN 162103 THEN ''Cloudy urine'' WHEN 162104 THEN ''Urine appears turbid'' ELSE '''' END) AS urine_turbidity, ',
    '(CASE urine_dipstick_for_blood WHEN 664 THEN ''NEGATIVE'' WHEN 1874 THEN ''Trace'' WHEN 1362 THEN ''One Plus(+)'' WHEN 1363 THEN ''Two Plus(++)'' WHEN 1364 THEN ''Three Plus(+++)'' ELSE '''' END) AS urine_dipstick_for_blood, ',
    '(CASE discontinuation_reason WHEN 159492 THEN ''Transferred out'' WHEN 1067 THEN ''Unknown'' WHEN 160034 THEN ''Died'' WHEN 5622 THEN ''Other'' WHEN 819 THEN ''819'' ELSE '''' END) AS discontinuation_reason ',
  'FROM ', src_mch_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_mch_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_mch_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_mch_quoted) AS message;



-- create table mch_enrollment
SET @target_mch_ant_quoted = CONCAT('`', @datatools_schema, '`.`mch_antenatal_visit`');
SET @src_mch_ant_quoted = CONCAT('`', @etl_schema, '`.`etl_mch_antenatal_visit`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_mch_ant_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_mch_ant_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, uuid, visit_id, visit_date, location_id, encounter_id, provider, ',
    'anc_visit_number, temperature, pulse_rate, systolic_bp, diastolic_bp, respiratory_rate, oxygen_saturation, ',
    'weight, height, muac, hemoglobin, ',
    '(CASE breast_exam_done WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1115 THEN ''Normal'' WHEN 1116 THEN ''Abnormal'' ELSE '''' END) AS breast_exam_done, ',
    '(CASE pallor WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS pallor, ',
    'maturity, fundal_height, ',
    '(CASE fetal_presentation WHEN 139814 THEN ''Frank Breech Presentation'' WHEN 160091 THEN ''vertex presentation'' WHEN 144433 THEN ''Compound Presentation'' WHEN 115808 THEN ''Mentum Presentation of Fetus'' ',
      'WHEN 118388 THEN ''Face or Brow Presentation of Foetus'' WHEN 129192 THEN ''Presentation of Cord'' WHEN 112259 THEN ''Transverse or Oblique Fetal Presentation'' WHEN 164148 THEN ''Occiput Anterior Position'' ',
      'WHEN 164149 THEN ''Brow Presentation'' WHEN 164150 THEN ''Face Presentation'' WHEN 156352 THEN ''footling breech presentation'' ELSE '''' END) AS fetal_presentation, ',
    '(CASE lie WHEN 132623 THEN ''Oblique lie'' WHEN 162088 THEN ''Longitudinal lie'' WHEN 124261 THEN ''Transverse lie'' ELSE '''' END) AS lie, ',
    'fetal_heart_rate, ',
    '(CASE fetal_movement WHEN 162090 THEN ''Increased fetal movements'' WHEN 113377 THEN ''Decreased fetal movements'' WHEN 1452 THEN ''No fetal movements'' WHEN 162108 THEN ''Fetal movements present'' ELSE '''' END) AS fetal_movement, ',
    '(CASE who_stage WHEN 1204 THEN ''WHO Stage1'' WHEN 1205 THEN ''WHO Stage2'' WHEN 1206 THEN ''WHO Stage3'' WHEN 1207 THEN ''WHO Stage4'' ELSE '''' END) AS who_stage, ',
    'cd4, (CASE vl_sample_taken WHEN 856 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS vl_sample_taken, ',
    'viral_load, (CASE ldl WHEN 1302 THEN ''LDL'' ELSE '''' END) AS ldl, ',
    '(CASE arv_status WHEN 1148 THEN ''ARV Prophylaxis'' WHEN 1149 THEN ''HAART'' WHEN 1175 THEN ''NA'' ELSE '''' END) AS arv_status, ',
    'final_test_result, patient_given_result, ',
    '(CASE partner_hiv_tested WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS partner_hiv_tested, ',
    '(CASE partner_hiv_status WHEN 664 THEN ''HIV Negative'' WHEN 703 THEN ''HIV Positive'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS partner_hiv_status, ',
    '(CASE prophylaxis_given WHEN 105281 THEN ''Cotrimoxazole'' WHEN 74250 THEN ''Dapsone'' WHEN 1107 THEN ''None'' ELSE '''' END) AS prophylaxis_given, ',
    '(CASE haart_given WHEN 1 THEN ''Yes'' WHEN 2 THEN ''No'' ELSE '''' END) AS haart_given, ',
    'date_given_haart, ',
    '(CASE baby_azt_dispensed WHEN 160123 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS baby_azt_dispensed, ',
    '(CASE baby_nvp_dispensed WHEN 80586 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS baby_nvp_dispensed, ',
    'deworming_done_anc, IPT_dose_given_anc, TTT, IPT_malaria, iron_supplement, deworming, bed_nets, urine_microscopy, ',
    '(CASE urinary_albumin WHEN 664 THEN ''Negative'' WHEN 1874 THEN ''Trace - 15'' WHEN 1362 THEN ''One Plus(+) - 30'' WHEN 1363 THEN ''Two Plus(++) - 100'' WHEN 1364 THEN ''Three Plus(+++) - 300'' WHEN 1365 THEN ''Four Plus(++++) - 1000'' ELSE '''' END) AS urinary_albumin, ',
    '(CASE glucose_measurement WHEN 1115 THEN ''Normal'' WHEN 1874 THEN ''Trace'' WHEN 1362 THEN ''One Plus(+)'' WHEN 1363 THEN ''Two Plus(++)'' WHEN 1364 THEN ''Three Plus(+++)'' WHEN 1365 THEN ''Four Plus(++++)'' ELSE '''' END) AS glucose_measurement, ',
    'urine_ph, urine_gravity, ',
    '(CASE urine_nitrite_test WHEN 664 THEN ''NEGATIVE'' WHEN 703 THEN ''POSITIVE'' WHEN 1362 THEN ''One Plus(+)'' WHEN 1363 THEN ''Two Plus(++)'' ELSE '''' END) AS urine_nitrite_test, ',
    '(CASE urine_leukocyte_esterace_test WHEN 664 THEN ''NEGATIVE'' WHEN 1874 THEN ''Trace'' WHEN 1362 THEN ''One Plus(+)'' WHEN 1363 THEN ''Two Plus(++)'' WHEN 1364 THEN ''Three Plus(+++)'' ELSE '''' END) AS urine_leukocyte_esterace_test, ',
    '(CASE urinary_ketone WHEN 664 THEN ''NEGATIVE'' WHEN 1874 THEN ''Trace - 5'' WHEN 1362 THEN ''One Plus(+) - 15'' WHEN 1363 THEN ''Two Plus(++) - 50'' WHEN 1364 THEN ''Three Plus(+++) - 150'' ELSE '''' END) AS urinary_ketone, ',
    '(CASE urine_bile_salt_test WHEN 1115 THEN ''Normal'' WHEN 1874 THEN ''Trace - 1'' WHEN 1362 THEN ''One Plus(+) - 4'' WHEN 1363 THEN ''Two Plus(++) - 8'' WHEN 1364 THEN ''Three Plus(+++) - 12'' ELSE '''' END) AS urine_bile_salt_test, ',
    '(CASE urine_bile_pigment_test WHEN 664 THEN ''NEGATIVE'' WHEN 1362 THEN ''One Plus(+)'' WHEN 1363 THEN ''Two Plus(++)'' WHEN 1364 THEN ''Three Plus(+++)'' ELSE '''' END) AS urine_bile_pigment_test, ',
    '(CASE urine_colour WHEN 162099 THEN ''Colourless'' WHEN 127778 THEN ''Red color'' WHEN 162097 THEN ''Light yellow colour'' WHEN 162105 THEN ''Yellow-green colour'' WHEN 162098 THEN ''Dark yellow colour'' WHEN 162100 THEN ''Brown color'' ELSE '''' END) AS urine_colour, ',
    '(CASE urine_turbidity WHEN 162102 THEN ''Urine appears clear'' WHEN 162103 THEN ''Cloudy urine'' WHEN 162104 THEN ''Urine appears turbid'' ELSE '''' END) AS urine_turbidity, ',
    '(CASE urine_dipstick_for_blood WHEN 664 THEN ''NEGATIVE'' WHEN 1874 THEN ''Trace'' WHEN 1362 THEN ''One Plus(+)'' WHEN 1363 THEN ''Two Plus(++)'' WHEN 1364 THEN ''Three Plus(+++)'' ELSE '''' END) AS urine_dipstick_for_blood, ',
    '(CASE syphilis_test_status WHEN 1229 THEN ''Non Reactive'' WHEN 1228 THEN ''Reactive'' WHEN 1402 THEN ''Not Screened'' WHEN 1304 THEN ''Poor Sample quality'' ELSE '''' END) AS syphilis_test_status, ',
    '(CASE syphilis_treated_status WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS syphilis_treated_status, ',
    '(CASE bs_mps WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' WHEN 1138 THEN ''Indeterminate'' ELSE '''' END) AS bs_mps, ',
    '(CASE diabetes_test WHEN 664 THEN ''No Diabetes'' WHEN 703 THEN ''Has Diabetes'' WHEN 160737 THEN ''Not Done'' ELSE '''' END) AS diabetes_test, ',
    '(CASE intermittent_presumptive_treatment_given WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not Applicable'' END) AS intermittent_presumptive_treatment_given, ',
    '(CASE intermittent_presumptive_treatment_dose WHEN 1 THEN ''First Dose'' WHEN 2 THEN ''Second Dose'' WHEN 3 THEN ''Third Dose'' WHEN 4 THEN ''Fourth Dose'' WHEN 5 THEN ''Fifth Dose'' WHEN 6 THEN ''Sith Dose'' WHEN 7 THEN ''Seventh Dose'' WHEN 0 THEN ''No'' END) AS intermittent_presumptive_treatment_dose, ',
    '(CASE minimum_care_package WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS minimum_care_package, ',
    'minimum_package_of_care_services, ',
    '(CASE fgm_done WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS fgm_done, ',
    '(CASE fgm_complications WHEN 122949 THEN ''Scarring'' WHEN 136308 THEN ''Keloids'' WHEN 141615 THEN ''dyspaneuria'' WHEN 111633 THEN ''UTI'' ELSE '''' END) AS fgm_complications, ',
    '(CASE fp_method_postpartum WHEN 5275 THEN ''IUD'' WHEN 159589 THEN ''Implants'' WHEN 1472 THEN ''BTL'' ELSE '''' END) AS fp_method_postpartum, ',
    '(CASE anc_exercises WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS anc_exercises, ',
    '(CASE tb_screening WHEN 1660 THEN ''No TB signs'' WHEN 164128 THEN ''No signs and started on INH'' WHEN 142177 THEN ''Presumed TB'' WHEN 1662 THEN ''TB Confirmed'' WHEN 160737 THEN ''Not done'' WHEN 1111 THEN ''On TB Treatment'' ELSE '''' END) AS tb_screening, ',
    '(CASE cacx_screening WHEN 703 THEN ''POSITIVE'' WHEN 664 THEN ''NEGATIVE'' WHEN 159393 THEN ''Presumed'' WHEN 1118 THEN ''Not Done'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS cacx_screening, ',
    '(CASE cacx_screening_method WHEN 885 THEN ''PAP Smear'' WHEN 162816 THEN ''VIA'' WHEN 5622 THEN ''Other'' ELSE '''' END) AS cacx_screening_method, ',
    '(CASE hepatitis_b_screening WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 160737 THEN ''Not Done'' ELSE '''' END) AS hepatitis_b_screening, ',
    '(CASE hepatitis_b_treatment WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS hepatitis_b_treatment, ',
    '(CASE has_other_illnes WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS has_other_illnes, ',
    '(CASE counselled WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS counselled, ',
    '(CASE counselled_on_birth_plans WHEN 159758 THEN ''Yes'' END) AS counselled_on_birth_plans, ',
    '(CASE counselled_on_danger_signs WHEN 159857 THEN ''Yes'' END) AS counselled_on_danger_signs, ',
    '(CASE counselled_on_family_planning WHEN 156277 THEN ''Yes'' END) AS counselled_on_family_planning, ',
    '(CASE counselled_on_hiv WHEN 1914 THEN ''Yes'' END) AS counselled_on_hiv, ',
    '(CASE counselled_on_supplimental_feeding WHEN 159854 THEN ''Yes'' END) AS counselled_on_supplimental_feeding, ',
    '(CASE counselled_on_breast_care WHEN 159856 THEN ''Yes'' END) AS counselled_on_breast_care, ',
    '(CASE counselled_on_infant_feeding WHEN 161651 THEN ''Yes'' END) AS counselled_on_infant_feeding, ',
    '(CASE counselled_on_treated_nets WHEN 1381 THEN ''Yes'' END) AS counselled_on_treated_nets, ',
    '(CASE risk_reduction WHEN 165275 THEN ''Yes'' END) AS risk_reduction, ',
    '(CASE partner_testing WHEN 161557 THEN ''Yes'' END) AS partner_testing, ',
    '(CASE sti_screening WHEN 165190 THEN ''Yes'' END) AS sti_screening, ',
    '(CASE condom_provision WHEN 159777 THEN ''Yes'' END) AS condom_provision, ',
    '(CASE prep_adherence WHEN 165203 THEN ''Yes'' END) AS prep_adherence, ',
    '(CASE anc_visits_emphasis WHEN 165475 THEN ''Yes'' END) AS anc_visits_emphasis, ',
    '(CASE pnc_fp_counseling WHEN 1382 THEN ''Yes'' END) AS pnc_fp_counseling, ',
    '(CASE referral_vmmc WHEN 162223 THEN ''Yes'' END) AS referral_vmmc, ',
    '(CASE referral_dreams WHEN 165368 THEN ''Yes'' END) AS referral_dreams, ',
    '(CASE referred_from WHEN 1537 THEN ''Another Health Facility'' WHEN 163488 THEN ''Community Unit'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS referred_from, ',
    '(CASE referred_to WHEN 1537 THEN ''Another Health Facility'' WHEN 163488 THEN ''Community Unit'' WHEN 165093 THEN ''HIV Preventive services'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS referred_to, ',
    'next_appointment_date, clinical_notes ',
  'FROM ', src_mch_ant_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_mch_ant_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_mch_ant_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_mch_ant_quoted) AS message;

-- create table mch_delivery table
SET @target_mch_delivery_quoted = CONCAT('`', @datatools_schema, '`.`mch_delivery`');
SET @src_mch_delivery_quoted = CONCAT('`', @etl_schema, '`.`etl_mchs_delivery`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_mch_delivery_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_mch_delivery_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, date_created, admission_number, number_of_anc_visits, ',
    '(CASE vaginal_examination WHEN 132681 THEN ''Normal'' WHEN 5577 THEN ''Episiotomy'' WHEN 159264 THEN ''Vaginal Tear'' WHEN 118935 THEN ''FGM'' WHEN 139505 THEN ''Vaginal wart'' ELSE '''' END) AS vaginal_examination, ',
    '(CASE uterotonic_given WHEN 81369 THEN ''Oxytocin'' WHEN 104590 THEN ''Carbetocin'' WHEN 5622 THEN ''Other'' WHEN 1107 THEN ''None'' ELSE '''' END) AS uterotonic_given, ',
    '(CASE chlohexidine_applied_on_code_stump WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS chlohexidine_applied_on_code_stump, ',
    '(CASE vitamin_K_given WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS vitamin_K_given, ',
    '(CASE kangaroo_mother_care_given WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS kangaroo_mother_care_given, ',
    '(CASE testing_done_in_the_maternity_hiv_status WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1067 THEN ''Unknown'' WHEN 164817 THEN ''Known Positive'' ELSE '''' END) AS testing_done_in_the_maternity_hiv_status, ',
    '(CASE infant_provided_with_arv_prophylaxis WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''N/A'' ELSE '''' END) AS infant_provided_with_arv_prophylaxis, ',
    '(CASE mother_on_haart_during_anc WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''N/A'' ELSE '''' END) AS mother_on_haart_during_anc, ',
    '(CASE mother_started_haart_at_maternity WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS mother_started_haart_at_maternity, ',
    '(CASE vdrl_rpr_results WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1118 THEN ''Not Done'' ELSE '''' END) AS vdrl_rpr_results, ',
    'date_of_last_menstrual_period, estimated_date_of_delivery, reason_for_referral, duration_of_pregnancy, ',
    '(CASE mode_of_delivery WHEN 1170 THEN ''Spontaneous vaginal delivery'' WHEN 1171 THEN ''Cesarean section'' WHEN 1172 THEN ''Breech delivery'' WHEN 118159 THEN ''Forceps or Vacuum Extractor Delivery'' WHEN 159739 THEN ''emergency caesarean section'' WHEN 159260 THEN ''vacuum extractor delivery'' WHEN 5622 THEN ''Other'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS mode_of_delivery, ',
    'date_of_delivery, blood_loss, ',
    '(CASE condition_of_mother WHEN 160429 THEN ''Alive'' WHEN 134612 THEN ''Dead'' ELSE '''' END) AS condition_of_mother, ',
    '(CASE delivery_outcome WHEN 159913 THEN ''Single'' WHEN 159914 THEN ''Twins'' WHEN 159915 THEN ''Triplets'' END) AS delivery_outcome, ',
    'apgar_score_1min, apgar_score_5min, apgar_score_10min, ',
    '(CASE resuscitation_done WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS resuscitation_done, ',
    '(CASE place_of_delivery WHEN 1536 THEN ''HOME'' WHEN 1588 THEN ''HEALTH CLINIC/POST'' WHEN 1589 THEN ''HOSPITAL'' WHEN 1601 THEN ''EN ROUTE TO HEALTH FACILITY'' WHEN 159670 THEN ''sub-district hospital'' WHEN 159671 THEN ''Provincial hospital'' WHEN 159662 THEN ''district hospital'' WHEN 159372 THEN ''Primary Care Clinic'' WHEN 5622 THEN ''Other'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS place_of_delivery, ',
    'delivery_assistant, ',
    '(CASE counseling_on_infant_feeding WHEN 161651 THEN ''Counseling about infant feeding practices'' ELSE '''' END) AS counseling_on_infant_feeding, ',
    '(CASE counseling_on_exclusive_breastfeeding WHEN 161096 THEN ''Counseling for exclusive breastfeeding'' ELSE '''' END) AS counseling_on_exclusive_breastfeeding, ',
    '(CASE counseling_on_infant_feeding_for_hiv_infected WHEN 162091 THEN ''Counseling for infant feeding practices to prevent HIV'' ELSE '''' END) AS counseling_on_infant_feeding_for_hiv_infected, ',
    '(CASE mother_decision WHEN 1173 THEN ''EXPRESSED BREASTMILK'' WHEN 1152 THEN ''WEANED'' WHEN 5254 THEN ''Infant formula'' WHEN 1150 THEN ''BREASTFED PREDOMINATELY'' WHEN 6046 THEN ''Mixed feeding'' WHEN 5526 THEN ''BREASTFED EXCLUSIVELY'' WHEN 968 THEN ''COW MILK'' WHEN 1595 THEN ''REPLACEMENT FEEDING'' ELSE '''' END) AS mother_decision, ',
    '(CASE placenta_complete WHEN 703 THEN ''Yes'' WHEN 664 THEN ''No'' WHEN 1501 THEN ''Baby born before arrival'' ELSE '''' END) AS placenta_complete, ',
    '(CASE maternal_death_audited WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''NA'' ELSE '''' END) AS maternal_death_audited, ',
    '(CASE cadre WHEN 1574 THEN ''CLINICAL OFFICER/DOCTOR'' WHEN 1578 THEN ''Midwife'' WHEN 1577 THEN ''NURSE'' WHEN 1575 THEN ''TRADITIONAL BIRTH ATTENDANT'' WHEN 1555 THEN '' COMMUNITY HEALTH CARE WORKER'' WHEN 5622 THEN ''Other'' ELSE '''' END) AS cadre, ',
    '(CASE delivery_complications WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS delivery_complications, ',
    '(CASE coded_delivery_complications WHEN 118744 THEN ''Eclampsia'' WHEN 113195 THEN ''Ruptured Uterus'' WHEN 115036 THEN ''Obstructed Labor'' WHEN 228 THEN ''APH'' WHEN 230 THEN ''PPH'' WHEN 130 THEN ''Puerperal sepsis'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS coded_delivery_complications, ',
    'other_delivery_complications, duration_of_labor, ',
    '(CASE baby_sex WHEN 1534 THEN ''Male Gender'' WHEN 1535 THEN ''Female gender'' ELSE '''' END) AS baby_sex, ',
    '(CASE baby_condition WHEN 135436 THEN ''Macerated Stillbirth'' WHEN 159916 THEN ''Fresh stillbirth'' WHEN 151849 THEN ''Live birth'' WHEN 125872 THEN ''STILLBIRTH'' WHEN 126127 THEN ''Spontaneous abortion'' WHEN 164815 THEN ''Live birth, died before arrival at facility'' WHEN 164816 THEN ''Live birth, died after arrival or delivery in facility'' ELSE '''' END) AS baby_condition, ',
    '(CASE teo_given WHEN 84893 THEN ''TETRACYCLINE'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not applicable'' ELSE '''' END) AS teo_given, ',
    'birth_weight, ',
    '(CASE bf_within_one_hour WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS bf_within_one_hour, ',
    '(CASE birth_with_deformity WHEN 155871 THEN ''deformity'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not applicable'' ELSE '''' END) AS birth_with_deformity, ',
    '(CASE type_of_birth_deformity WHEN 143672 THEN ''Congenital syphilis'' WHEN 126208 THEN ''Spina bifida'' WHEN 117470 THEN ''Hydrocephalus'' WHEN 125048 THEN ''Talipes'' ELSE '''' END) AS type_of_birth_deformity, ',
    'final_test_result, patient_given_result, ',
    '(CASE partner_hiv_tested WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS partner_hiv_tested, ',
    '(CASE partner_hiv_status WHEN 664 THEN ''HIV Negative'' WHEN 703 THEN ''HIV Positive'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS partner_hiv_status, ',
    '(CASE prophylaxis_given WHEN 105281 THEN ''SULFAMETHOXAZOLE / TRIMETHOPRIM'' WHEN 74250 THEN ''DAPSONE'' WHEN 1107 THEN ''None'' ELSE '''' END) AS prophylaxis_given, ',
    '(CASE baby_azt_dispensed WHEN 160123 THEN ''Zidovudine for PMTCT'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not Applicable'' ELSE '''' END) AS baby_azt_dispensed, ',
    '(CASE baby_nvp_dispensed WHEN 80586 THEN ''NEVIRAPINE'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not Applicable'' ELSE '''' END) AS baby_nvp_dispensed, ',
    'clinical_notes, ',
    '(CASE stimulation_done WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS stimulation_done, ',
    '(CASE suction_done WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS suction_done, ',
    '(CASE oxygen_given WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS oxygen_given, ',
    '(CASE bag_mask_ventilation_provided WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS bag_mask_ventilation_provided, ',
    '(CASE induction_done WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS induction_done, ',
    '(CASE artificial_rapture_done WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS artificial_rapture_done ',
  'FROM ', @src_mch_delivery_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_mch_delivery_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_mch_delivery_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_mch_delivery_quoted) AS message;

-- create table mch_delivery table

SET @target_mch_delivery_quoted = CONCAT('`', @datatools_schema, '`.`mch_delivery`');
SET @src_mch_delivery_quoted = CONCAT('`', @etl_schema, '`.`etl_mchs_delivery`');

SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_mch_delivery_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_mch_delivery_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, date_created, admission_number, number_of_anc_visits, ',
    '(CASE vaginal_examination WHEN 132681 THEN ''Normal'' WHEN 5577 THEN ''Episiotomy'' WHEN 159264 THEN ''Vaginal Tear'' WHEN 118935 THEN ''FGM'' WHEN 139505 THEN ''Vaginal wart'' ELSE '''' END) AS vaginal_examination, ',
    '(CASE uterotonic_given WHEN 81369 THEN ''Oxytocin'' WHEN 104590 THEN ''Carbetocin'' WHEN 5622 THEN ''Other'' WHEN 1107 THEN ''None'' ELSE '''' END) AS uterotonic_given, ',
    '(CASE chlohexidine_applied_on_code_stump WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS chlohexidine_applied_on_code_stump, ',
    '(CASE vitamin_K_given WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS vitamin_K_given, ',
    '(CASE kangaroo_mother_care_given WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS kangaroo_mother_care_given, ',
    '(CASE testing_done_in_the_maternity_hiv_status WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1067 THEN ''Unknown'' WHEN 164817 THEN ''Known Positive'' ELSE '''' END) AS testing_done_in_the_maternity_hiv_status, ',
    '(CASE infant_provided_with_arv_prophylaxis WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''N/A'' ELSE '''' END) AS infant_provided_with_arv_prophylaxis, ',
    '(CASE mother_on_haart_during_anc WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''N/A'' ELSE '''' END) AS mother_on_haart_during_anc, ',
    '(CASE mother_started_haart_at_maternity WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS mother_started_haart_at_maternity, ',
    '(CASE vdrl_rpr_results WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1118 THEN ''Not Done'' ELSE '''' END) AS vdrl_rpr_results, ',
    'date_of_last_menstrual_period, estimated_date_of_delivery, reason_for_referral, duration_of_pregnancy, ',
    '(CASE mode_of_delivery WHEN 1170 THEN ''Spontaneous vaginal delivery'' WHEN 1171 THEN ''Cesarean section'' WHEN 1172 THEN ''Breech delivery'' WHEN 118159 THEN ''Forceps or Vacuum Extractor Delivery'' WHEN 159739 THEN ''emergency caesarean section'' WHEN 159260 THEN ''vacuum extractor delivery'' WHEN 5622 THEN ''Other'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS mode_of_delivery, ',
    'date_of_delivery, blood_loss, ',
    '(CASE condition_of_mother WHEN 160429 THEN ''Alive'' WHEN 134612 THEN ''Dead'' ELSE '''' END) AS condition_of_mother, ',
    '(CASE delivery_outcome WHEN 159913 THEN ''Single'' WHEN 159914 THEN ''Twins'' WHEN 159915 THEN ''Triplets'' END) AS delivery_outcome, ',
    'apgar_score_1min, apgar_score_5min, apgar_score_10min, ',
    '(CASE resuscitation_done WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS resuscitation_done, ',
    '(CASE place_of_delivery WHEN 1536 THEN ''HOME'' WHEN 1588 THEN ''HEALTH CLINIC/POST'' WHEN 1589 THEN ''HOSPITAL'' WHEN 1601 THEN ''EN ROUTE TO HEALTH FACILITY'' WHEN 159670 THEN ''sub-district hospital'' WHEN 159671 THEN ''Provincial hospital'' WHEN 159662 THEN ''district hospital'' WHEN 159372 THEN ''Primary Care Clinic'' WHEN 5622 THEN ''Other'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS place_of_delivery, ',
    'delivery_assistant, ',
    '(CASE counseling_on_infant_feeding WHEN 161651 THEN ''Counseling about infant feeding practices'' ELSE '''' END) AS counseling_on_infant_feeding, ',
    '(CASE counseling_on_exclusive_breastfeeding WHEN 161096 THEN ''Counseling for exclusive breastfeeding'' ELSE '''' END) AS counseling_on_exclusive_breastfeeding, ',
    '(CASE counseling_on_infant_feeding_for_hiv_infected WHEN 162091 THEN ''Counseling for infant feeding practices to prevent HIV'' ELSE '''' END) AS counseling_on_infant_feeding_for_hiv_infected, ',
    '(CASE mother_decision WHEN 1173 THEN ''EXPRESSED BREASTMILK'' WHEN 1152 THEN ''WEANED'' WHEN 5254 THEN ''Infant formula'' WHEN 1150 THEN ''BREASTFED PREDOMINATELY'' WHEN 6046 THEN ''Mixed feeding'' WHEN 5526 THEN ''BREASTFED EXCLUSIVELY'' WHEN 968 THEN ''COW MILK'' WHEN 1595 THEN ''REPLACEMENT FEEDING'' ELSE '''' END) AS mother_decision, ',
    '(CASE placenta_complete WHEN 703 THEN ''Yes'' WHEN 664 THEN ''No'' WHEN 1501 THEN ''Baby born before arrival'' ELSE '''' END) AS placenta_complete, ',
    '(CASE maternal_death_audited WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''NA'' ELSE '''' END) AS maternal_death_audited, ',
    '(CASE cadre WHEN 1574 THEN ''CLINICAL OFFICER/DOCTOR'' WHEN 1578 THEN ''Midwife'' WHEN 1577 THEN ''NURSE'' WHEN 1575 THEN ''TRADITIONAL BIRTH ATTENDANT'' WHEN 1555 THEN '' COMMUNITY HEALTH CARE WORKER'' WHEN 5622 THEN ''Other'' ELSE '''' END) AS cadre, ',
    '(CASE delivery_complications WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS delivery_complications, ',
    '(CASE coded_delivery_complications WHEN 118744 THEN ''Eclampsia'' WHEN 113195 THEN ''Ruptured Uterus'' WHEN 115036 THEN ''Obstructed Labor'' WHEN 228 THEN ''APH'' WHEN 230 THEN ''PPH'' WHEN 130 THEN ''Puerperal sepsis'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS coded_delivery_complications, ',
    'other_delivery_complications, duration_of_labor, ',
    '(CASE baby_sex WHEN 1534 THEN ''Male Gender'' WHEN 1535 THEN ''Female gender'' ELSE '''' END) AS baby_sex, ',
    '(CASE baby_condition WHEN 135436 THEN ''Macerated Stillbirth'' WHEN 159916 THEN ''Fresh stillbirth'' WHEN 151849 THEN ''Live birth'' WHEN 125872 THEN ''STILLBIRTH'' WHEN 126127 THEN ''Spontaneous abortion'' WHEN 164815 THEN ''Live birth, died before arrival at facility'' WHEN 164816 THEN ''Live birth, died after arrival or delivery in facility'' ELSE '''' END) AS baby_condition, ',
    '(CASE teo_given WHEN 84893 THEN ''TETRACYCLINE'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not applicable'' ELSE '''' END) AS teo_given, ',
    'birth_weight, ',
    '(CASE bf_within_one_hour WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS bf_within_one_hour, ',
    '(CASE birth_with_deformity WHEN 155871 THEN ''deformity'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not applicable'' ELSE '''' END) AS birth_with_deformity, ',
    '(CASE type_of_birth_deformity WHEN 143672 THEN ''Congenital syphilis'' WHEN 126208 THEN ''Spina bifida'' WHEN 117470 THEN ''Hydrocephalus'' WHEN 125048 THEN ''Talipes'' ELSE '''' END) AS type_of_birth_deformity, ',
    'final_test_result, patient_given_result, ',
    '(CASE partner_hiv_tested WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS partner_hiv_tested, ',
    '(CASE partner_hiv_status WHEN 664 THEN ''HIV Negative'' WHEN 703 THEN ''HIV Positive'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS partner_hiv_status, ',
    '(CASE prophylaxis_given WHEN 105281 THEN ''SULFAMETHOXAZOLE / TRIMETHOPRIM'' WHEN 74250 THEN ''DAPSONE'' WHEN 1107 THEN ''None'' ELSE '''' END) AS prophylaxis_given, ',
    '(CASE baby_azt_dispensed WHEN 160123 THEN ''Zidovudine for PMTCT'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not Applicable'' ELSE '''' END) AS baby_azt_dispensed, ',
    '(CASE baby_nvp_dispensed WHEN 80586 THEN ''NEVIRAPINE'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not Applicable'' ELSE '''' END) AS baby_nvp_dispensed, ',
    'clinical_notes, ',
    '(CASE stimulation_done WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS stimulation_done, ',
    '(CASE suction_done WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS suction_done, ',
    '(CASE oxygen_given WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS oxygen_given, ',
    '(CASE bag_mask_ventilation_provided WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS bag_mask_ventilation_provided, ',
    '(CASE induction_done WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS induction_done, ',
    '(CASE artificial_rapture_done WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS artificial_rapture_done ',
  'FROM ', src_mch_delivery_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_mch_delivery_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_mch_delivery_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('SELECT ''Successfully created '' , ', @target_mch_delivery_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;



-- create table mch_postnatal_visit-----

SET @target_mch_post_quoted = CONCAT('`', @datatools_schema, '`.`mch_postnatal_visit`');
SET @src_mch_post_quoted = CONCAT('`', @etl_schema, '`.`etl_mch_postnatal_visit`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_mch_post_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_mch_post_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, ',
    'uuid, ',
    'visit_id, ',
    'visit_date, ',
    'location_id, ',
    'encounter_id, ',
    'provider, ',
    'pnc_register_no, ',
    'pnc_visit_no, ',
    'delivery_date, ',
    '(CASE mode_of_delivery WHEN 1170 THEN ''SVD'' WHEN 1171 THEN ''C-Section'' WHEN 1172 THEN ''Breech delivery'' WHEN 118159 THEN ''Assisted vaginal delivery'' ELSE '''' END) AS mode_of_delivery, ',
    '(CASE place_of_delivery WHEN 1589 THEN ''Facility'' WHEN 1536 THEN ''Home'' WHEN 5622 THEN ''Other'' ELSE '''' END) AS place_of_delivery, ',
    '(CASE visit_timing_mother WHEN 1721 THEN ''0-48 Hours'' WHEN 1722 THEN ''3 days - 6 weeks'' WHEN 1723 THEN ''More than 6 weeks'' END) AS visit_timing_mother, ',
    '(CASE visit_timing_baby WHEN 167012 THEN ''0-48 Hours'' WHEN 167013 THEN ''3 days - 6 weeks'' WHEN 167015 THEN ''More than 6 weeks'' END) AS visit_timing_baby, ',
    '(CASE delivery_outcome WHEN 159913 THEN ''Single'' WHEN 159914 THEN ''Twins'' WHEN 159915 THEN ''Triplets'' END) AS delivery_outcome, ',
    'temperature, ',
    'pulse_rate, ',
    'systolic_bp, ',
    'diastolic_bp, ',
    'respiratory_rate, ',
    'oxygen_saturation, ',
    'weight, ',
    'height, ',
    'muac, ',
    'hemoglobin, ',
    '(CASE arv_status WHEN 1148 THEN ''ARV Prophylaxis'' WHEN 1149 THEN ''HAART'' WHEN 1175 THEN ''NA'' ELSE '''' END) AS arv_status, ',
    '(CASE general_condition WHEN 1855 THEN ''Good'' WHEN 162133 THEN ''Fair'' WHEN 162132 THEN ''Poor'' ELSE '''' END) AS general_condition, ',
    '(CASE breast WHEN 1855 THEN ''Good'' WHEN 162133 THEN ''Fair'' WHEN 162132 THEN ''Poor'' ELSE '''' END) AS breast, ',
    '(CASE cs_scar WHEN 156794 THEN ''infection of obstetric surgical wound'' WHEN 145776 THEN ''Caesarean Wound Disruption'' WHEN 162129 THEN ''Wound intact and healing'' WHEN 162130 THEN ''Surgical wound healed'' ELSE '''' END) AS cs_scar, ',
    '(CASE gravid_uterus WHEN 162111 THEN ''On exam, uterine fundus 12-16 week size'' WHEN 162112 THEN ''On exam, uterine fundus 16-20 week size'' WHEN 162113 THEN ''On exam, uterine fundus 20-24 week size'' WHEN 162114 THEN ''On exam, uterine fundus 24-28 week size'' WHEN 162115 THEN ''On exam, uterine fundus 28-32 week size'' WHEN 162116 THEN ''On exam, uterine fundus 32-34 week size'' WHEN 162117 THEN ''On exam, uterine fundus 34-36 week size'' WHEN 162118 THEN ''On exam, uterine fundus 36-38 week size'' WHEN 162119 THEN ''On exam, uterine fundus 38 weeks-term size'' WHEN 123427 THEN ''Uterus Involuted'' ELSE '''' END) AS gravid_uterus, ',
    '(CASE episiotomy WHEN 159842 THEN ''repaired, episiotomy wound'' WHEN 159843 THEN ''healed, episiotomy wound'' WHEN 159841 THEN ''gap, episiotomy wound'' WHEN 113919 THEN ''Postoperative Wound Infection'' ELSE '''' END) AS episiotomy, ',
    '(CASE lochia WHEN 159845 THEN ''lochia excessive'' WHEN 159846 THEN ''lochia foul smelling'' WHEN 159721 THEN ''Lochia type'' ELSE '''' END) AS lochia, ',
    '(CASE counselled_on_infant_feeding WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS counselled_on_infant_feeding, ',
    '(CASE pallor WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not applicable'' ELSE '''' END) AS pallor, ',
    '(CASE pallor_severity WHEN 1498 THEN ''Mild'' WHEN 1499 THEN ''Moderate'' WHEN 1500 THEN ''Severe'' ELSE '''' END) AS pallor_severity, ',
    '(CASE pph WHEN 1065 THEN ''Present'' WHEN 1066 THEN ''Absent'' ELSE '''' END) AS pph, ',
    '(CASE mother_hiv_status WHEN 1067 THEN ''Unknown'' WHEN 664 THEN ''NEGATIVE'' WHEN 703 THEN ''POSITIVE'' ELSE '''' END) AS mother_hiv_status, ',
    '(CASE condition_of_baby WHEN 1855 THEN ''In good health'' WHEN 162132 THEN ''Patient condition poor'' WHEN 1067 THEN ''Unknown'' WHEN 162133 THEN ''Patient condition fair/satisfactory'' ELSE '''' END) AS condition_of_baby, ',
    '(CASE baby_feeding_method WHEN 5526 THEN ''BREASTFED EXCLUSIVELY'' WHEN 1595 THEN ''REPLACEMENT FEEDING'' WHEN 6046 THEN ''Mixed feeding'' WHEN 159418 THEN ''Not at all sure'' ELSE '''' END) AS baby_feeding_method, ',
    '(CASE umblical_cord WHEN 162122 THEN ''Neonatal umbilical stump clean'' WHEN 162123 THEN ''Neonatal umbilical stump not clean'' WHEN 162124 THEN ''Neonatal umbilical stump moist'' WHEN 159418 THEN ''Not at all sure'' ELSE '''' END) AS umblical_cord, ',
    '(CASE baby_immunization_started WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS baby_immunization_started, ',
    '(CASE family_planning_counseling WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS family_planning_counseling, ',
    'other_maternal_complications, ',
    '(CASE uterus_examination WHEN 163750 THEN ''Contracted'' WHEN 148220 THEN ''Not contracted'' WHEN 5622 THEN ''Other'' ELSE '''' END) AS uterus_examination, ',
    'uterus_cervix_examination, ',
    'vaginal_examination, ',
    'parametrial_examination, ',
    'external_genitalia_examination, ',
    'ovarian_examination, ',
    'pelvic_lymph_node_exam, ',
    'final_test_result, ',
    '(CASE syphilis_results WHEN 1229 THEN ''Positive'' WHEN 1228 THEN ''Negative'' END) AS syphilis_results, ',
    'patient_given_result, ',
    '(CASE couple_counselled WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS couple_counselled, ',
    '(CASE partner_hiv_tested WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS partner_hiv_tested, ',
    '(CASE partner_hiv_status WHEN 664 THEN ''HIV Negative'' WHEN 703 THEN ''HIV Positive'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS partner_hiv_status, ',
    '(CASE pnc_hiv_test_timing_mother WHEN 162080 THEN ''Less than 6 weeks'' WHEN 162081 THEN ''Greater 6 weeks'' WHEN 1118 THEN ''Not Done'' END) AS pnc_hiv_test_timing_mother, ',
    '(CASE mother_haart_given WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''N/A'' WHEN 164142 THEN ''Revisit'' ELSE '''' END) AS mother_haart_given, ',
    '(CASE prophylaxis_given WHEN 105281 THEN ''Cotrimoxazole'' WHEN 74250 THEN ''Dapsone'' WHEN 1107 THEN ''None'' ELSE '''' END) AS prophylaxis_given, ',
    '(CASE infant_prophylaxis_timing WHEN 1065 THEN ''Less than 6 weeks'' WHEN 1066 THEN ''Greater 6 weeks'' END) AS infant_prophylaxis_timing, ',
    '(CASE baby_azt_dispensed WHEN 160123 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS baby_azt_dispensed, ',
    '(CASE baby_nvp_dispensed WHEN 80586 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS baby_nvp_dispensed, ',
    '(CASE pnc_exercises WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS pnc_exercises, ',
    '(CASE maternal_condition WHEN 130 THEN ''Puerperal sepsis'' WHEN 114244 THEN ''Perineal Laceration'' WHEN 1855 THEN ''In good health'' WHEN 134612 THEN ''Maternal Death'' WHEN 160429 THEN ''Alive'' WHEN 162132 THEN ''Patient condition poor'' WHEN 162133 THEN ''Patient condition fair/satisfactory'' ELSE '''' END) AS maternal_condition, ',
    '(CASE iron_supplementation WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS iron_supplementation, ',
    '(CASE fistula_screening WHEN 1107 THEN ''None'' WHEN 49 THEN ''Vesicovaginal Fistula'' WHEN 127847 THEN ''Rectovaginal fistula'' WHEN 1118 THEN ''Not done'' ELSE '''' END) AS fistula_screening, ',
    '(CASE cacx_screening WHEN 703 THEN ''POSITIVE'' WHEN 664 THEN ''NEGATIVE'' WHEN 159393 THEN ''Presumed'' WHEN 1118 THEN ''Not Done'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS cacx_screening, ',
    '(CASE cacx_screening_method WHEN 885 THEN ''PAP Smear'' WHEN 162816 THEN ''VIA'' WHEN 164977 THEN ''VILI'' WHEN 159859 THEN ''HPV'' WHEN 5622 THEN ''Other'' ELSE '''' END) AS cacx_screening_method, ',
    '(CASE family_planning_status WHEN 965 THEN ''On Family Planning'' WHEN 160652 THEN ''Not using Family Planning'' ELSE '''' END) AS family_planning_status, ',
    'family_planning_method, ',
    '(CASE referred_from WHEN 1537 THEN ''Another Health Facility'' WHEN 163488 THEN ''Community Unit'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS referred_from, ',
    '(CASE referred_to WHEN 1537 THEN ''Another Health Facility'' WHEN 163488 THEN ''Community Unit'' WHEN 165093 THEN ''HIV Preventive services'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS referred_to, ',
    'referral_reason, ',
    'clinical_notes, ',
    'appointment_date ',
  'FROM ', src_mch_post_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_mch_post_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_mch_post_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('SELECT ''Successfully created '' , ', @target_mch_post_quoted);

  -- ------------ create table etl_hei_enrollment-----------------------

SET @target_hei_quoted = CONCAT('`', @datatools_schema, '`.`hei_enrollment`');
SET @src_hei_quoted = CONCAT('`', @etl_schema, '`.`etl_hei_enrollment`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_hei_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_hei_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'serial_no, ',
    'patient_id, ',
    'uuid, ',
    'provider, ',
    'visit_id, ',
    'visit_date, ',
    'location_id, ',
    'encounter_id, ',
    '(CASE child_exposed WHEN 822 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS child_exposed, ',
    'spd_number, ',
    'birth_weight, ',
    'gestation_at_birth, ',
    '(CASE birth_type WHEN 159913 THEN ''Single'' WHEN 159914 THEN ''Twins'' WHEN 159915 THEN ''Triplets'' WHEN 113450 THEN ''Quadruplets'' WHEN 113440 THEN ''Quintuplets'' ELSE '''' END) AS birth_type, ',
    'date_first_seen, ',
    'birth_notification_number, ',
    'birth_certificate_number, ',
    '(CASE need_for_special_care WHEN 161628 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS need_for_special_care, ',
    '(CASE reason_for_special_care WHEN 116222 THEN ''Birth weight less than 2.5 kg'' WHEN 162071 THEN ''Birth less than 2 years after last birth'' WHEN 162072 THEN ''Fifth or more child'' WHEN 162073 THEN ''Teenage mother'' WHEN 162074 THEN ''Brother or sisters undernourished'' WHEN 162075 THEN ''Multiple births(Twins,triplets)'' WHEN 162076 THEN ''Child in family dead'' WHEN 1174 THEN ''Orphan'' WHEN 161599 THEN ''Child has disability'' WHEN 1859 THEN ''Parent HIV positive'' WHEN 123174 THEN ''History/signs of child abuse/neglect'' ELSE '''' END) AS reason_for_special_care, ',
    '(CASE referral_source WHEN 160537 THEN ''Paediatric'' WHEN 160542 THEN ''OPD'' WHEN 160456 THEN ''Maternity'' WHEN 162050 THEN ''CCC'' WHEN 160538 THEN ''MCH/PMTCT'' WHEN 5622 THEN ''Other'' ELSE '''' END) AS referral_source, ',
    '(CASE transfer_in WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS transfer_in, ',
    'transfer_in_date, ',
    'facility_transferred_from, ',
    'district_transferred_from, ',
    'date_first_enrolled_in_hei_care, ',
    '(CASE mother_breastfeeding WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS mother_breastfeeding, ',
    '(CASE TB_contact_history_in_household WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS TB_contact_history_in_household, ',
    '(CASE mother_alive WHEN 1 THEN ''Yes'' WHEN 0 THEN ''No'' ELSE '''' END) AS mother_alive, ',
    '(CASE mother_on_pmtct_drugs WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS mother_on_pmtct_drugs, ',
    '(CASE mother_on_drug WHEN 80586 THEN ''Sd NVP Only'' WHEN 1652 THEN ''AZT+NVP+3TC'' WHEN 1149 THEN ''HAART'' WHEN 1107 THEN ''None'' ELSE '''' END) AS mother_on_drug, ',
    '(CASE mother_on_art_at_infant_enrollment WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS mother_on_art_at_infant_enrollment, ',
    '(CASE mother_drug_regimen WHEN 164968 THEN ''AZT/3TC/DTG'' WHEN 164969 THEN ''TDF/3TC/DTG'' WHEN 164970 THEN ''ABC/3TC/DTG'' WHEN 164505 THEN ''TDF-3TC-EFV'' WHEN 792 THEN ''D4T/3TC/NVP'' WHEN 160124 THEN ''AZT/3TC/EFV'' WHEN 160104 THEN ''D4T/3TC/EFV'' WHEN 1652 THEN ''3TC/NVP/AZT'' WHEN 161361 THEN ''EDF/3TC/EFV'' WHEN 104565 THEN ''EFV/FTC/TDF'' WHEN 162201 THEN ''3TC/LPV/TDF/r'' WHEN 817 THEN ''ABC/3TC/AZT'' WHEN 162199 THEN ''ABC/NVP/3TC'' WHEN 162200 THEN ''3TC/ABC/LPV/r'' WHEN 162565 THEN ''3TC/NVP/TDF'' WHEN 162561 THEN ''3TC/AZT/LPV/r'' WHEN 164511 THEN ''AZT-3TC-ATV/r'' WHEN 164512 THEN ''TDF-3TC-ATV/r'' WHEN 162560 THEN ''3TC/D4T/LPV/r'' WHEN 162563 THEN ''3TC/ABC/EFV'' WHEN 162562 THEN ''ABC/LPV/R/TDF'' WHEN 162559 THEN ''ABC/DDI/LPV/r'' ELSE '''' END) AS mother_drug_regimen, ',
    '(CASE infant_prophylaxis WHEN 80586 THEN ''Sd NVP Only'' WHEN 1652 THEN ''AZT/NVP'' WHEN 162326 THEN ''NVP for 6 weeks(Mother on HAART)'' WHEN 160123 THEN ''AZT Liquid BD for 6 weeks'' WHEN 78643 THEN ''3TC Liquid BD'' WHEN 1149 THEN ''none'' WHEN 1107 THEN ''Other'' ELSE '''' END) AS infant_prophylaxis, ',
    'parent_ccc_number, ',
    '(CASE mode_of_delivery WHEN 1170 THEN ''SVD'' WHEN 1171 THEN ''C-Section'' WHEN 1172 THEN ''Breech delivery'' WHEN 118159 THEN ''Assisted vaginal delivery'' ELSE '''' END) AS mode_of_delivery, ',
    '(CASE place_of_delivery WHEN 1589 THEN ''Facility'' WHEN 1536 THEN ''Home'' WHEN 5622 THEN ''Other'' ELSE '''' END) AS place_of_delivery, ',
    'birth_length, ',
    'birth_order, ',
    'health_facility_name, ',
    'date_of_birth_notification, ',
    'date_of_birth_registration, ',
    'birth_registration_place, ',
    'permanent_registration_serial, ',
    'mother_facility_registered, ',
    'exit_date, ',
    '(CASE exit_reason WHEN 1403 THEN ''HIV Neg age greater 18 months'' WHEN 138571 THEN ''Confirmed HIV Positive'' WHEN 5240 THEN ''Lost'' WHEN 160432 THEN ''Dead'' WHEN 159492 THEN ''Transfer Out'' ELSE '''' END) AS exit_reason, ',
    'hiv_status_at_exit ',
  'FROM ', src_hei_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hei_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hei_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_hei_quoted) AS message;

-- create table hei_follow_up_visit

SET @target_hei_follow_quoted = CONCAT('`', @datatools_schema, '`.`hei_follow_up_visit`');
SET @src_hei_follow_quoted = CONCAT('`', @etl_schema, '`.`etl_hei_follow_up_visit`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_hei_follow_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_hei_follow_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, weight, height, ',
    '(CASE muac WHEN 160909 THEN ''Green'' WHEN 160910 THEN ''Yellow'' WHEN 127778 THEN ''Red'' ELSE '''' END) AS muac, ',
    '(CASE primary_caregiver WHEN 970 THEN ''Mother'' WHEN 973 THEN ''Guardian'' WHEN 972 THEN ''Guardian'' WHEN 160639 THEN ''Guardian'' WHEN 5622 THEN ''Guardian'' ELSE '''' END) AS primary_caregiver, ',
    '(CASE revisit_this_year WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS revisit_this_year, ',
    '(CASE height_length WHEN 1115 THEN ''Normal'' WHEN 164085 THEN ''Stunted'' WHEN 164086 THEN ''Severe Stunded'' END) AS height_length, ',
    '(CASE referred WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS referred, ',
    'referral_reason, ',
    '(CASE danger_signs WHEN 159861 THEN ''Unable to breastfeed'' WHEN 1983 THEN ''Unable to drink'' WHEN 164482 THEN ''Vomits everything'' WHEN 138868 THEN ''Bloody Diarrhea'' WHEN 460 THEN ''Has Oedema'' WHEN 164483 THEN ''Has convulsions'' END) AS danger_signs, ',
    '(CASE infant_feeding WHEN 5526 THEN ''Exclusive Breastfeeding(EBF)'' WHEN 1595 THEN ''Exclusive Replacement(ERF)'' WHEN 6046 THEN ''Mixed Feeding(MF)'' ELSE '''' END) AS infant_feeding, ',
    '(CASE stunted WHEN 164085 THEN ''Yes'' WHEN 1115 THEN ''No'' ELSE '''' END) AS stunted, ',
    '(CASE tb_assessment_outcome WHEN 1660 THEN ''No TB Signs'' WHEN 142177 THEN ''Presumed TB'' WHEN 1661 THEN ''TB Confirmed'' WHEN 1662 THEN ''TB Rx'' WHEN 1679 THEN ''INH'' WHEN 160737 THEN ''TB Screening Not Done'' ELSE '''' END) AS tb_assessment_outcome, ',
    '(CASE social_smile_milestone WHEN 162056 THEN ''Social Smile'' ELSE '''' END) AS social_smile_milestone, ',
    '(CASE head_control_milestone WHEN 162057 THEN ''Head Holding/Control'' ELSE '''' END) AS head_control_milestone, ',
    '(CASE response_to_sound_milestone WHEN 162058 THEN ''Turns towards the origin of sound'' ELSE '''' END) AS response_to_sound_milestone, ',
    '(CASE hand_extension_milestone WHEN 162059 THEN ''Extends hand to grasp a toy'' ELSE '''' END) AS hand_extension_milestone, ',
    '(CASE sitting_milestone WHEN 162061 THEN ''Sitting'' ELSE '''' END) AS sitting_milestone, ',
    '(CASE walking_milestone WHEN 162063 THEN ''Walking'' ELSE '''' END) AS walking_milestone, ',
    '(CASE standing_milestone WHEN 162062 THEN ''Standing'' ELSE '''' END) AS standing_milestone, ',
    '(CASE talking_milestone WHEN 162060 THEN ''Talking'' ELSE '''' END) AS talking_milestone, ',
    '(CASE review_of_systems_developmental WHEN 1115 THEN ''Normal(N)'' WHEN 6022 THEN ''Delayed(D)'' WHEN 6025 THEN ''Regressed(R)'' ELSE '''' END) AS review_of_systems_developmental, ',
    '(CASE weight_category WHEN 123814 THEN ''Underweight(UW)'' WHEN 126598 THEN ''Severely Underweight(SUW)'' WHEN 114413 THEN ''Overweight(OW)'' WHEN 115115 THEN ''Obese(O)'' WHEN 1115 THEN ''Normal(N)'' ELSE '''' END) AS weight_category, ',
    '(CASE followup_type WHEN 132636 THEN ''Marasmus'' WHEN 116474 THEN ''Kwashiorkor'' WHEN 115122 THEN ''Mulnutrition'' WHEN 5622 THEN ''Other'' ELSE '''' END) AS followup_type, ',
    'dna_pcr_sample_date, ',
    '(CASE dna_pcr_contextual_status WHEN 162081 THEN ''Repeat'' WHEN 162083 THEN ''Final test (end of pediatric window)'' WHEN 162082 THEN ''Confirmation'' WHEN 162080 THEN ''Initial'' ELSE '''' END) AS dna_pcr_contextual_status, ',
    '(CASE dna_pcr_result WHEN 1138 THEN ''INDETERMINATE'' WHEN 664 THEN ''NEGATIVE'' WHEN 703 THEN ''POSITIVE'' WHEN 1304 THEN ''POOR SAMPLE QUALITY'' ELSE '''' END) AS dna_pcr_result, ',
    '(CASE azt_given WHEN 86663 THEN ''Yes'' ELSE ''No'' END) AS azt_given, ',
    '(CASE nvp_given WHEN 80586 THEN ''Yes'' ELSE ''No'' END) AS nvp_given, ',
    '(CASE ctx_given WHEN 105281 THEN ''Yes'' ELSE ''No'' END) AS ctx_given, ',
    '(CASE multi_vitamin_given WHEN 461 THEN ''Yes'' ELSE ''No'' END) AS multi_vitamin_given, ',
    '(CASE first_antibody_result WHEN 664 THEN ''NEGATIVE'' WHEN 703 THEN ''POSITIVE'' WHEN 1304 THEN ''POOR SAMPLE QUALITY'' ELSE '''' END) AS first_antibody_result, ',
    '(CASE final_antibody_result WHEN 664 THEN ''NEGATIVE'' WHEN 703 THEN ''POSITIVE'' WHEN 1304 THEN ''POOR SAMPLE QUALITY'' ELSE '''' END) AS final_antibody_result, ',
    '(CASE tetracycline_ointment_given WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS tetracycline_ointment_given, ',
    '(CASE pupil_examination WHEN 162065 THEN ''Black'' WHEN 1075 THEN ''White'' ELSE '''' END) AS pupil_examination, ',
    '(CASE sight_examination WHEN 1065 THEN ''Following Objects'' WHEN 1066 THEN ''Not Following Objects'' ELSE '''' END) AS sight_examination, ',
    '(CASE squint WHEN 1065 THEN ''Squint'' WHEN 1066 THEN ''No Squint'' ELSE '''' END) AS squint, ',
    '(CASE deworming_drug WHEN 79413 THEN ''Mebendazole'' WHEN 70439 THEN ''Albendazole'' ELSE '''' END) AS deworming_drug, ',
    'dosage, unit, ',
    '(CASE vitaminA_given WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS vitaminA_given, ',
    '(CASE disability WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS disability, ',
    '(CASE referred_from WHEN 1537 THEN ''Another Health Facility'' WHEN 163488 THEN ''Community Unit'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS referred_from, ',
    '(CASE referred_to WHEN 1537 THEN ''Another Health Facility'' WHEN 163488 THEN ''Community Unit'' WHEN 165093 THEN ''HIV Preventive services'' WHEN 1175 THEN ''N/A'' ELSE '''' END) AS referred_to, ',
    '(CASE counselled_on WHEN 1914 THEN ''HIV'' WHEN 1380 THEN ''Nutrition'' ELSE '''' END) AS counselled_on, ',
    '(CASE mnps_supplementation WHEN 161649 THEN ''Yes'' WHEN 1107 THEN ''No'' ELSE '''' END) AS MNPS_Supplementation, ',
    '(CASE LLIN WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS LLIN, ',
    'comments, next_appointment_date ',
  'FROM ', src_hei_follow_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hei_follow_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hei_follow_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hei_follow_quoted, ' ADD INDEX(infant_feeding)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT CONCAT('Successfully created ', @target_hei_follow_quoted) AS message;

-- create table immunization

SET @target_immunization_quoted = CONCAT('`', @datatools_schema, '`.`immunization`');
SET @src_immunization_quoted = CONCAT('`', @etl_schema, '`.`etl_immunization`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_immunization_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_immunization_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, ',
    'visit_date, ',
    'created_by, ',
    'date_created, ',
    'encounter_id, ',
    'BCG, ',
    'OPV_birth, ',
    'OPV_1, ',
    'OPV_2, ',
    'OPV_3, ',
    'IPV, ',
    'DPT_Hep_B_Hib_1, ',
    'DPT_Hep_B_Hib_2, ',
    'DPT_Hep_B_Hib_3, ',
    'PCV_10_1, ',
    'PCV_10_2, ',
    'PCV_10_3, ',
    'ROTA_1, ',
    'ROTA_2, ',
    'ROTA_3, ',
    'Measles_rubella_1, ',
    'Measles_rubella_2, ',
    'Yellow_fever, ',
    'Measles_6_months, ',
    'VitaminA_6_months, ',
    'VitaminA_1_yr, ',
    'VitaminA_1_and_half_yr, ',
    'VitaminA_2_yr, ',
    'VitaminA_2_to_5_yr, ',
    'HPV_1, ',
    'HPV_2, ',
    'influenza, ',
    'sequence, ',
    'CASE fully_immunized WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS fully_immunized ',
  'FROM ', src_immunization_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_immunization_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_immunization_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_immunization_quoted, ' ADD INDEX(sequence)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_immunization_quoted) AS message;

-- create table tb_enrollment

-- sql
SET @target_tb_quoted = CONCAT('`', @datatools_schema, '`.`tb_enrollment`');
SET @src_tb_quoted = CONCAT('`', @etl_schema, '`.`etl_tb_enrollment`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_tb_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_tb_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, date_treatment_started, district, ',
    '(CASE referred_by WHEN 160539 THEN ''VCT center'' WHEN 160631 THEN ''HIV care clinic'' WHEN 160546 THEN ''STI Clinic'' WHEN 161359 THEN ''Home Based Care'' ',
      'WHEN 160538 THEN ''Antenatal/PMTCT Clinic'' WHEN 1725 THEN ''Private Sector'' WHEN 1744 THEN ''Chemist/pharmacist'' WHEN 160551 THEN ''Self referral'' ',
      'WHEN 1555 THEN ''Community Health worker(CHW)'' WHEN 162050 THEN ''CCC'' WHEN 164103 THEN ''Diabetes Clinic'' ELSE '''' END) AS referred_by, ',
    'referral_date, date_transferred_in, facility_transferred_from, district_transferred_from, date_first_enrolled_in_tb_care, ',
    'weight, height, treatment_supporter, ',
    '(CASE relation_to_patient WHEN 973 THEN ''Grandparent'' WHEN 972 THEN ''Sibling'' WHEN 160639 THEN ''Guardian'' WHEN 1527 THEN ''Parent'' WHEN 5617 THEN ''PARTNER OR SPOUSE'' WHEN 5622 THEN ''Other'' ELSE '''' END) AS relation_to_patient, ',
    'treatment_supporter_address, treatment_supporter_phone_contact, ',
    '(CASE disease_classification WHEN 42 THEN ''Pulmonary TB'' WHEN 5042 THEN ''Extra-Pulmonary TB'' ELSE '''' END) AS disease_classification, ',
    '(CASE patient_classification WHEN 159878 THEN ''New'' WHEN 159877 THEN ''Smear positive Relapse'' WHEN 159876 THEN ''Smear negative Relapse'' WHEN 159874 THEN ''Treatment after Failure'' ',
      'WHEN 159873 THEN ''Treatment resumed after defaulting'' WHEN 159872 THEN ''Transfer in'' WHEN 163609 THEN ''Previous treatment history unknown'' ELSE '''' END) AS patient_classification, ',
    '(CASE pulmonary_smear_result WHEN 703 THEN ''Smear Positive'' WHEN 664 THEN ''Smear Negative'' WHEN 1118 THEN ''Smear not done'' ELSE '''' END) AS pulmonary_smear_result, ',
    '(CASE has_extra_pulmonary_pleurial_effusion WHEN 130059 THEN ''Pleural effusion'' ELSE '''' END) AS has_extra_pulmonary_pleurial_effusion, ',
    '(CASE has_extra_pulmonary_milliary WHEN 115753 THEN ''Milliary'' ELSE '''' END) AS has_extra_pulmonary_milliary, ',
    '(CASE has_extra_pulmonary_lymph_node WHEN 111953 THEN ''Lymph nodes'' ELSE '''' END) AS has_extra_pulmonary_lymph_node, ',
    '(CASE has_extra_pulmonary_menengitis WHEN 111967 THEN ''Meningitis'' ELSE '''' END) AS has_extra_pulmonary_menengitis, ',
    '(CASE has_extra_pulmonary_skeleton WHEN 112116 THEN ''Skeleton'' ELSE '''' END) AS has_extra_pulmonary_skeleton, ',
    '(CASE has_extra_pulmonary_abdominal WHEN 1350 THEN ''Abdominal'' ELSE '''' END) AS has_extra_pulmonary_abdominal ',
  'FROM ', src_tb_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_tb_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_tb_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('SELECT ''Successfully created '' , ', @target_tb_quoted);

-- create table tb_follow_up_visit

-- sql
SET @target_tb_follow_follow_quoted = NULL; -- ensure variable not reused
SET @target_tb_follow_quoted = CONCAT('`', @datatools_schema, '`.`tb_follow_up_visit`');
SET @src_tb_follow_quoted = CONCAT('`', @etl_schema, '`.`etl_tb_follow_up_visit`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_tb_follow_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_tb_follow_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, ',
    'uuid, ',
    'provider, ',
    'visit_id, ',
    'visit_date, ',
    'location_id, ',
    'encounter_id, ',
    '(CASE spatum_test WHEN 160022 THEN ''ZN Smear Microscopy'' WHEN 161880 THEN ''Fluorescence Microscopy'' ELSE '''' END) AS spatum_test, ',
    '(CASE spatum_result WHEN 159985 THEN ''Scanty'' WHEN 1362 THEN ''+'' WHEN 1363 THEN ''++'' WHEN 1364 THEN ''+++'' WHEN 664 THEN ''Negative'' ELSE '''' END) AS spatum_result, ',
    'result_serial_number, ',
    'quantity, ',
    'date_test_done, ',
    '(CASE bacterial_colonie_growth WHEN 703 THEN ''Growth'' WHEN 664 THEN ''No growth'' ELSE '''' END) AS bacterial_colonie_growth, ',
    'number_of_colonies, ',
    '(CASE resistant_s WHEN 84360 THEN ''S'' ELSE '''' END) AS resistant_s, ',
    '(CASE resistant_r WHEN 767 THEN ''R'' ELSE '''' END) AS resistant_r, ',
    '(CASE resistant_inh WHEN 78280 THEN ''INH'' ELSE '''' END) AS resistant_inh, ',
    '(CASE resistant_e WHEN 75948 THEN ''E'' ELSE '''' END) AS resistant_e, ',
    '(CASE sensitive_s WHEN 84360 THEN ''S'' ELSE '''' END) AS sensitive_s, ',
    '(CASE sensitive_r WHEN 767 THEN ''R'' ELSE '''' END) AS sensitive_r, ',
    '(CASE sensitive_inh WHEN 78280 THEN ''INH'' ELSE '''' END) AS sensitive_inh, ',
    '(CASE sensitive_e WHEN 75948 THEN ''E'' ELSE '''' END) AS sensitive_e, ',
    'test_date, ',
    '(CASE hiv_status WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS hiv_status, ',
    'next_appointment_date ',
  'FROM ', @src_tb_follow_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_tb_follow_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_tb_follow_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_tb_follow_quoted, ' ADD INDEX(hiv_status)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('SELECT ''Successfully created '' , ', @target_tb_follow_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- create table tb_screening

-- sql
SET @target_tb_screen_quoted = CONCAT('`', @datatools_schema, '`.`tb_screening`');
SET @src_tb_screen_quoted = CONCAT('`', @etl_schema, '`.`etl_tb_screening`');

SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_tb_screen_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_tb_screen_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, ',
    'uuid, ',
    'provider, ',
    'visit_id, ',
    'visit_date, ',
    'encounter_id, ',
    'location_id, ',
    '(CASE cough_for_2wks_or_more WHEN 159799 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS cough_for_2wks_or_more, ',
    '(CASE confirmed_tb_contact WHEN 124068 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS confirmed_tb_contact, ',
    '(CASE fever_for_2wks_or_more WHEN 1494 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS fever_for_2wks_or_more, ',
    '(CASE noticeable_weight_loss WHEN 832 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS noticeable_weight_loss, ',
    '(CASE night_sweat_for_2wks_or_more WHEN 133027 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS night_sweat_for_2wks_or_more, ',
    '(CASE lethargy WHEN 116334 THEN ''Yes'' ELSE '''' END) AS lethargy, ',
    '(CASE spatum_smear_ordered WHEN 307 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS spatum_smear_ordered, ',
    '(CASE chest_xray_ordered WHEN 12 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS chest_xray_ordered, ',
    '(CASE genexpert_ordered WHEN 162202 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS genexpert_ordered, ',
    '(CASE spatum_smear_result WHEN 703 THEN ''POSITIVE'' WHEN 664 THEN ''NEGATIVE'' ELSE '''' END) AS spatum_smear_result, ',
    '(CASE chest_xray_result WHEN 1115 THEN ''NORMAL'' WHEN 152526 THEN ''ABNORMAL'' ELSE '''' END) AS chest_xray_result, ',
    '(CASE genexpert_result WHEN 664 THEN ''NEGATIVE'' WHEN 162203 THEN ''Mycobacterium tuberculosis detected with rifampin resistance'' WHEN 162204 THEN ''Mycobacterium tuberculosis detected without rifampin resistance'' ',
      'WHEN 164104 THEN ''Mycobacterium tuberculosis detected with indeterminate rifampin resistance'' WHEN 163611 THEN ''Invalid'' WHEN 1138 THEN ''INDETERMINATE'' ELSE '''' END) AS genexpert_result, ',
    '(CASE referral WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS referral, ',
    '(CASE clinical_tb_diagnosis WHEN 703 THEN ''POSITIVE'' WHEN 664 THEN ''NEGATIVE'' ELSE '''' END) AS clinical_tb_diagnosis, ',
    '(CASE contact_invitation WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS contact_invitation, ',
    '(CASE evaluated_for_ipt WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS evaluated_for_ipt, ',
    '(CASE resulting_tb_status WHEN 1660 THEN ''No TB Signs'' WHEN 142177 THEN ''Presumed TB'' WHEN 1662 THEN ''TB Confirmed'' WHEN 160737 THEN ''TB Screening Not Done'' ELSE '''' END) AS resulting_tb_status, ',
    'tb_treatment_start_date, ',
    '(CASE tb_prophylaxis WHEN 105281 THEN ''Cotrimoxazole'' WHEN 74250 THEN ''Dapsone'' WHEN 1107 THEN ''None'' ELSE '''' END) AS tb_prophylaxis, ',
    'notes, ',
    '(CASE person_present WHEN 978 THEN ''Yes'' ELSE ''No'' END) AS person_present ',
  'FROM ', @src_tb_screen_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT('ALTER TABLE ', @target_tb_screen_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT('ALTER TABLE ', @target_tb_screen_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT('SELECT ''Successfully created '' , ', @target_tb_screen_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;


-- -------- Table Datatools drug event ---

SET @target_drug_quoted = CONCAT('`', @datatools_schema, '`.`drug_event`');
SET @src_drug_quoted = CONCAT('`', @etl_schema, '`.`etl_drug_event`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_drug_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_drug_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, ',
    'patient_id, ',
    'date_started, ',
    'visit_date, ',
    'provider, ',
    'encounter_id, ',
    'program, ',
    'regimen, ',
    'regimen_name, ',
    'regimen_line, ',
    'discontinued, ',
    '(CASE regimen_stopped WHEN 1260 THEN ''Yes'' ELSE ''No'' END) AS regimen_stopped, ',
    'regimen_discontinued, ',
    'date_discontinued, ',
    '(CASE reason_discontinued ',
      'WHEN 102 THEN ''Drug toxicity'' ',
      'WHEN 160567 THEN ''New diagnosis of Tuberculosis'' ',
      'WHEN 160569 THEN ''Virologic failure'' ',
      'WHEN 159598 THEN ''Non-compliance with treatment or therapy'' ',
      'WHEN 1754 THEN ''Medications unavailable'' ',
      'WHEN 1434 THEN ''Currently pregnant'' ',
      'WHEN 1253 THEN ''Completed PMTCT'' ',
      'WHEN 843 THEN ''Regimen failure'' ',
      'WHEN 5622 THEN ''Other'' ',
      'WHEN 160559 THEN ''Risk of pregnancy'' ',
      'WHEN 160561 THEN ''New drug available'' ',
      'ELSE '''' END) AS reason_discontinued, ',
    'reason_discontinued_other ',
  'FROM ', src_drug_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_drug_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('SELECT ''Successfully created '' , ', @target_drug_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- --- HTS Table ------------------------

SET @target_hts_quoted = CONCAT('`', @datatools_schema, '`.`hts_test`');
SET @src_hts_quoted = CONCAT('`', @etl_schema, '`.`etl_hts_test`');
SET @src_pd_quoted  = CONCAT('`', @etl_schema, '`.`etl_patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_hts_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_hts_quoted, ' AS ',
  'SELECT ',
    't.patient_id, ',
    't.visit_id, ',
    't.encounter_id, ',
    't.encounter_uuid, ',
    't.encounter_location, ',
    't.creator, ',
    't.date_created, ',
    't.date_last_modified, ',
    'DATE(t.visit_date) AS visit_date, ',
    't.test_type, ',
    't.population_type, ',
    't.key_population_type, ',
    't.priority_population_type, ',
    't.ever_tested_for_hiv, ',
    't.months_since_last_test, ',
    't.patient_disabled, ',
    't.disability_type, ',
    't.patient_consented, ',
    't.client_tested_as, ',
    't.setting, ',
    't.approach, ',
    '(CASE t.test_strategy ',
      'WHEN 164163 THEN ''HP: Hospital Patient Testing'' ',
      'WHEN 164953 THEN ''NP: HTS for non-patients'' ',
      'WHEN 164954 THEN ''VI:Integrated VCT Center'' ',
      'WHEN 164955 THEN ''VS:Stand Alone VCT Center'' ',
      'WHEN 159938 THEN ''HB:Home Based Testing'' ',
      'WHEN 159939 THEN ''MO: Mobile Outreach HTS'' ',
      'WHEN 161557 THEN ''Index testing'' ',
      'WHEN 166606 THEN ''SNS - Social Networks'' ',
      'WHEN 5622   THEN ''O:Other'' ',
      'ELSE '''' END) AS test_strategy, ',
    '(CASE t.hts_entry_point ',
      'WHEN 5485   THEN ''In Patient Department(IPD)'' ',
      'WHEN 160542 THEN ''Out Patient Department(OPD)'' ',
      'WHEN 162181 THEN ''Peadiatric Clinic'' ',
      'WHEN 160552 THEN ''Nutrition Clinic'' ',
      'WHEN 160538 THEN ''PMTCT ANC'' ',
      'WHEN 160456 THEN ''PMTCT MAT'' ',
      'WHEN 1623   THEN ''PMTCT PNC'' ',
      'WHEN 160541 THEN ''TB'' ',
      'WHEN 162050 THEN ''CCC'' ',
      'WHEN 159940 THEN ''VCT'' ',
      'WHEN 159938 THEN ''Home Based Testing'' ',
      'WHEN 159939 THEN ''Mobile Outreach'' ',
      'WHEN 162223 THEN ''VMMC'' ',
      'WHEN 160546 THEN ''STI Clinic'' ',
      'WHEN 160522 THEN ''Emergency'' ',
      'WHEN 163096 THEN ''Community Testing'' ',
      'WHEN 5622   THEN ''Other'' ',
      'ELSE '''' END) AS hts_entry_point, ',
    't.hts_risk_category, t.hts_risk_score, ',
    't.test_1_kit_name, t.test_1_kit_lot_no, t.test_1_kit_expiry, t.test_1_result, ',
    't.test_2_kit_name, t.test_2_kit_lot_no, t.test_2_kit_expiry, t.test_2_result, ',
    't.test_3_kit_name, t.test_3_kit_lot_no, t.test_3_kit_expiry, t.test_3_result, ',
    't.final_test_result, t.syphillis_test_result, t.patient_given_result, t.couple_discordant, ',
    '(CASE t.referred WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END) AS referred, ',
    't.referral_for, t.referral_facility, t.other_referral_facility, t.neg_referral_for, t.neg_referral_specify, ',
    't.tb_screening, t.patient_had_hiv_self_test, t.remarks, t.voided ',
  'FROM ', @src_hts_quoted, ' t ',
  'INNER JOIN ', src_pd_quoted, ' d ON d.patient_id = t.patient_id AND d.voided = 0'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_quoted, ' ADD FOREIGN KEY(patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_quoted, ' ADD INDEX(population_type)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_quoted, ' ADD INDEX(final_test_result)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_hts_quoted) AS message;


-- ------------------------------------ POPULATE HTS LINKAGES AND REFERRALS -------------------------------

SET @target_hts_referral_quoted = CONCAT('`', @datatools_schema, '`.`hts_referral_and_linkage`');
SET @src_hts_referral_quoted    = CONCAT('`', @etl_schema, '`.`etl_hts_referral_and_linkage`');
SET @src_pd_quoted              = CONCAT('`', @etl_schema, '`.`etl_patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_hts_referral_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_hts_referral_quoted, ' AS ',
  'SELECT l.* FROM ', @src_hts_referral_quoted, ' l ',
  'INNER JOIN ', src_pd_quoted, ' d ON d.patient_id = l.patient_id AND d.voided = 0'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_referral_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_referral_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_hts_referral_quoted) AS message;


-- hts_referral
SET @target_hts_referral_quoted = CONCAT('`', @datatools_schema, '`.`hts_referral`');
SET @src_hts_referral_quoted    = CONCAT('`', @etl_schema, '`.`etl_hts_referral`');
SET @src_pd_quoted              = CONCAT('`', @etl_schema, '`.`etl_patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_hts_referral_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_hts_referral_quoted, ' AS ',
  'SELECT r.* FROM ', @src_hts_referral_quoted, ' r ',
  'INNER JOIN ', @src_pd_quoted, ' d ON d.patient_id = r.patient_id AND d.voided = 0'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_referral_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_hts_referral_quoted) AS message;


-- current_in_care
SET @target_current_quoted = CONCAT('`', @datatools_schema, '`.`current_in_care`');
SET @src_current_quoted   = CONCAT('`', @etl_schema, '`.`etl_current_in_care`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_current_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('CREATE TABLE ', @target_current_quoted, ' AS SELECT * FROM ', src_current_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_current_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_current_quoted) AS message;


-- ipt_followup
SET @target_ipt_quoted = CONCAT('`', @datatools_schema, '`.`ipt_followup`');
SET @src_ipt_quoted   = CONCAT('`', @etl_schema, '`.`etl_ipt_follow_up`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_ipt_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('CREATE TABLE ', @target_ipt_quoted, ' AS SELECT * FROM ', src_ipt_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ipt_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_ipt_quoted) AS message;


-- default_facility_info
SET @target_def_fac_quoted = CONCAT('`', @datatools_schema, '`.`default_facility_info`');
SET @src_def_fac_quoted    = CONCAT('`', @etl_schema, '`.`etl_default_facility_info`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_def_fac_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('CREATE TABLE ', target_def_fac_quoted, ' AS SELECT * FROM ', src_def_fac_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', target_def_fac_quoted) AS message;


-- person_address
SET @target_addr_quoted = CONCAT('`', @datatools_schema, '`.`person_address`');
SET @src_addr_quoted    = CONCAT('`', @etl_schema, '`.`etl_person_address`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_addr_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('CREATE TABLE ', target_addr_quoted, ' AS SELECT * FROM ', src_addr_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', target_addr_quoted) AS message;


-- create table ipt_screening

SET @target_ipt_quoted = CONCAT('`', @datatools_schema, '`.`ipt_screening`');
SET @src_ipt_quoted    = CONCAT('`', @etl_schema, '`.`etl_ipt_screening`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_ipt_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_ipt_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, ',
    'provider, ',
    'patient_id, ',
    'visit_id, ',
    'visit_date, ',
    'location_id, ',
    'encounter_id, ',
    'obs_id, ',
    '(CASE cough WHEN 159799 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS cough, ',
    '(CASE fever WHEN 1494 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS fever, ',
    '(CASE weight_loss_poor_gain WHEN 832 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS weight_loss_poor_gain, ',
    '(CASE night_sweats WHEN 133027 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS night_sweats, ',
    '(CASE contact_with_tb_case WHEN 124068 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS contact_with_tb_case, ',
    '(CASE lethargy WHEN 116334 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS lethargy, ',
    '(CASE yellow_urine WHEN 162311 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS yellow_urine, ',
    '(CASE numbness_bs_hands_feet WHEN 132652 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS numbness_bs_hands_feet, ',
    '(CASE eyes_yellowness WHEN 5192 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS eyes_yellowness, ',
    '(CASE upper_rightQ_abdomen_tenderness WHEN 124994 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS upper_rightQ_abdomen_tenderness, ',
    'date_created, ',
    'date_last_modified, ',
    'voided ',
  'FROM ', @src_ipt_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ipt_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ipt_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('SELECT ''Successfully created '' , ', @target_ipt_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Create table ccc_defaulter_tracing

-- sql
SET @target_ccc_quoted = CONCAT('`', @datatools_schema, '`.`ccc_defaulter_tracing`');
SET @src_ccc_quoted    = CONCAT('`', @etl_schema, '`.`etl_ccc_defaulter_tracing`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_ccc_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_ccc_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'f.uuid, ',
    'f.provider, ',
    'f.patient_id, ',
    'f.visit_id, ',
    'DATE(f.visit_date) AS visit_date, ',
    'f.location_id, ',
    'f.encounter_id, ',
    '(CASE WHEN f.tracing_type = 1650 THEN ''Client Called'' WHEN f.tracing_type = 164965 THEN ''Physical Tracing'' WHEN f.tracing_type = 161642 THEN ''Treatment supporter'' ELSE NULL END) AS tracing_type, ',
    '(CASE WHEN f.tracing_outcome = 1267 THEN ''Contact'' WHEN f.tracing_outcome = 1118 THEN ''No Contact'' ELSE NULL END) AS tracing_outcome, ',
    'DATE(f.missed_appointment_date) AS missed_appointment_date, ',
    '(CASE WHEN f.reason_for_missed_appointment = 165609 THEN ''Client has covid-19 infection'' WHEN f.reason_for_missed_appointment = 165610 THEN ''COVID-19 restrictions'' WHEN f.reason_for_missed_appointment = 164407 THEN ''Client refilled drugs from another facility'' WHEN f.reason_for_missed_appointment = 159367 THEN ''Client has enough drugs'' WHEN f.reason_for_missed_appointment = 162619 THEN ''Client travelled'' WHEN f.reason_for_missed_appointment = 126240 THEN ''Client could not get an off from work/school'' WHEN f.reason_for_missed_appointment = 160583 THEN ''Client is sharing drugs with partner'' WHEN f.reason_for_missed_appointment = 162192 THEN ''Client forgot clinic dates'' WHEN f.reason_for_missed_appointment = 164349 THEN ''Client stopped medications'' WHEN f.reason_for_missed_appointment = 1654 THEN ''Client sick at home/admitted'' WHEN f.reason_for_missed_appointment = 5622 THEN ''Other'' ELSE NULL END) AS reason_for_missed_appointment, ',
    'NULLIF(TRIM(f.non_coded_missed_appointment_reason), '''') AS non_coded_missed_appointment_reason, ',
    '(CASE WHEN f.reason_not_contacted = 166538 THEN ''No locator information'' WHEN f.reason_not_contacted = 165075 THEN ''Inaccurate locator information'' WHEN f.reason_not_contacted = 160034 THEN ''Died'' WHEN f.reason_not_contacted = 1302 THEN ''Calls not going through'' WHEN f.reason_not_contacted = 1567 THEN ''Not picking calls'' WHEN f.reason_not_contacted = 160415 THEN ''Migrated from reported location'' WHEN f.reason_not_contacted = 1706 THEN ''Not found at home'' WHEN f.reason_not_contacted = 5622 THEN ''Other'' ELSE NULL END) AS reason_not_contacted, ',
    'NULLIF(TRIM(f.attempt_number), '''') AS attempt_number, ',
    '(CASE WHEN f.is_final_trace = 1267 THEN ''Yes'' WHEN f.is_final_trace = 163339 THEN ''No'' ELSE NULL END) AS is_final_trace, ',
    '(CASE WHEN f.true_status = 160432 THEN ''Dead'' WHEN f.true_status = 1693 THEN ''Receiving ART from another clinic/Transferred'' WHEN f.true_status = 160037 THEN ''Still in care at CCC'' WHEN f.true_status = 5240 THEN ''Lost to follow up'' WHEN f.true_status = 164435 THEN ''Stopped treatment'' WHEN f.true_status = 142917 THEN ''Other'' ELSE NULL END) AS true_status, ',
    '(CASE WHEN f.cause_of_death = 165609 THEN ''Infection due to COVID-19'' WHEN f.cause_of_death = 162574 THEN ''Death related to HIV infection'' WHEN f.cause_of_death = 116030 THEN ''Cancer'' WHEN f.cause_of_death = 164500 THEN ''TB'' WHEN f.cause_of_death = 151522 THEN ''Other infectious and parasitic diseases'' WHEN f.cause_of_death = 133481 THEN ''Natural cause'' WHEN f.cause_of_death = 1603 THEN ''Unnatural Cause'' WHEN f.cause_of_death = 5622 THEN ''Unknown cause'' ELSE NULL END) AS cause_of_death, ',
    'NULLIF(TRIM(f.comments), '''') AS comments, ',
    'DATE(f.booking_date) AS booking_date, ',
    'f.date_created, f.date_last_modified ',
  'FROM ', src_ccc_quoted, ' f'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ccc_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ccc_quoted, ' ADD INDEX (visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ccc_quoted, ' ADD INDEX (patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ccc_quoted, ' ADD INDEX (missed_appointment_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ccc_quoted, ' ADD INDEX (true_status)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ccc_quoted, ' ADD INDEX (cause_of_death)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ccc_quoted, ' ADD INDEX (tracing_type)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('SELECT ''Successfully created '' , ', @target_ccc_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;


-- create table art_preparation

SET @target_art_quoted = CONCAT('`', @datatools_schema, '`.`art_preparation`');
SET @src_art_quoted = CONCAT('`', @etl_schema, '`.`etl_ART_preparation`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_art_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_art_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, ',
    'patient_id, ',
    'visit_id, ',
    'DATE(visit_date) AS visit_date, ',
    'location_id, ',
    'encounter_id, ',
    'provider, ',
    'understands_hiv_art_benefits, ',
    'screened_negative_substance_abuse, ',
    'screened_negative_psychiatric_illness, ',
    'HIV_status_disclosure, ',
    'trained_drug_admin, ',
    'caregiver_committed, ',
    'adherance_barriers_identified, ',
    'caregiver_location_contacts_known, ',
    'ready_to_start_art, ',
    'identified_drug_time, ',
    'treatment_supporter_engaged, ',
    'support_grp_meeting_awareness, ',
    'enrolled_in_reminder_system, ',
    'date_created, ',
    'date_last_modified ',
  'FROM ', src_art_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_art_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_art_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_art_quoted, ' ADD INDEX(ready_to_start_art)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('SELECT ''Successfully created '' , ', @target_art_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- create table enhanced_adherence ----

-- sql
SET @target_enhanced_quoted = CONCAT('`', @datatools_schema, '`.`enhanced_adherence`');
SET @src_enhanced_quoted = CONCAT('`', @etl_schema, '`.`etl_enhanced_adherence`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_enhanced_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_enhanced_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, ',
    'patient_id, ',
    'visit_id, ',
    'DATE(visit_date) AS visit_date, ',
    'location_id, ',
    'encounter_id, ',
    'provider, ',
    'session_number, ',
    'DATE(first_session_date) AS first_session_date, ',
    'pill_count, ',
    'MMAS4_1_forgets_to_take_meds, ',
    'MMAS4_2_careless_taking_meds, ',
    'MMAS4_3_stops_on_reactive_meds, ',
    'MMAS4_4_stops_meds_on_feeling_good, ',
    'MMSA8_1_took_meds_yesterday, ',
    'MMSA8_2_stops_meds_on_controlled_symptoms, ',
    'MMSA8_3_struggles_to_comply_tx_plan, ',
    'MMSA8_4_struggles_remembering_taking_meds, ',
    'arv_adherence, ',
    'has_vl_results, ',
    'vl_results_suppressed, ',
    'vl_results_feeling, ',
    'cause_of_high_vl, ',
    'way_forward, ',
    'patient_hiv_knowledge, ',
    'patient_drugs_uptake, ',
    'patient_drugs_reminder_tools, ',
    'patient_drugs_uptake_during_travels, ',
    'patient_drugs_side_effects_response, ',
    'patient_drugs_uptake_most_difficult_times, ',
    'patient_drugs_daily_uptake_feeling, ',
    'patient_ambitions, ',
    'patient_has_people_to_talk, ',
    'patient_enlisting_social_support, ',
    'patient_income_sources, ',
    'patient_challenges_reaching_clinic, ',
    'patient_worried_of_accidental_disclosure, ',
    'patient_treated_differently, ',
    'stigma_hinders_adherence, ',
    'patient_tried_faith_healing, ',
    'patient_adherence_improved, ',
    'patient_doses_missed, ',
    'review_and_barriers_to_adherence, ',
    'other_referrals, ',
    'appointments_honoured, ',
    'referral_experience, ',
    'home_visit_benefit, ',
    'adherence_plan, ',
    'DATE(next_appointment_date) AS next_appointment_date ',
  'FROM ', src_enhanced_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_enhanced_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_enhanced_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_enhanced_quoted) AS message;

-- --------- Create triage table ---------------------------


SET @target_triage_quoted = CONCAT('`', @datatools_schema, '`.`triage`');
SET @src_triage_quoted = CONCAT('`', @etl_schema, '`.`etl_patient_triage`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_triage_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_triage_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, ',
    'patient_id, ',
    'visit_id, ',
    'visit_date, ',
    'location_id, ',
    'encounter_id, ',
    'encounter_provider, ',
    'date_created, ',
    'visit_reason, ',
    'weight, ',
    'height, ',
    'systolic_pressure, ',
    'diastolic_pressure, ',
    'temperature, ',
    'CASE temperature_collection_mode ',
      'WHEN 5112 THEN ''Axillary'' ',
      'WHEN 166242 THEN ''Forehead thermometer gun'' ',
      'WHEN 160240 THEN ''Oral'' ',
      'ELSE NULL ',
    'END AS temperature_collection_mode, ',
    'pulse_rate, ',
    'respiratory_rate, ',
    'oxygen_saturation, ',
    'CASE oxygen_saturation_collection_mode ',
      'WHEN 162735 THEN ''Room air'' ',
      'WHEN 162738 THEN ''On supplemental oxygen'' ',
      'ELSE NULL ',
    'END AS oxygen_saturation_collection_mode, ',
    'muac, ',
    'z_score_absolute, ',
    'CASE z_score ',
      'WHEN 1115 THEN ''Normal (Median)'' ',
      'WHEN 123814 THEN ''Mild (-1 SD)'' ',
      'WHEN 123815 THEN ''Moderate (-2 SD)'' ',
      'WHEN 164131 THEN ''Severe (-3 SD and -4 SD)'' ',
      'ELSE NULL ',
    'END AS z_score, ',
    'CASE nutritional_status ',
      'WHEN 1115 THEN ''Normal'' ',
      'WHEN 163302 THEN ''Severe acute malnutrition'' ',
      'WHEN 163303 THEN ''Moderate acute malnutrition'' ',
      'WHEN 114413 THEN ''Overweight/Obese'' ',
      'WHEN 164125 THEN ''Nutritional wasting'' ',
      'WHEN 164131 THEN ''Severe (-3 SD and -4 SD)'' ',
      'WHEN 123815 THEN ''Moderate (-2 SD)'' ',
      'WHEN 123814 THEN ''Mild (-1 SD)'' ',
      'ELSE NULL ',
    'END AS nutritional_status, ',
    'CASE nutritional_intervention ',
      'WHEN 1380 THEN ''Nutritional counselling for a Normal Case'' ',
      'WHEN 159854 THEN ''MAM (Supplementary Feeding Program)'' ',
      'WHEN 161650 THEN ''SAM without complications (Outpatient Therapeutic Program)'' ',
      'WHEN 163302 THEN ''Inpatient Management for clients with SAM Complications'' ',
      'WHEN 983 THEN ''Weight Management follow up for clients with Overweight/Obesity'' ',
      'ELSE NULL ',
    'END AS nutritional_intervention, ',
    'last_menstrual_period, ',
    'CASE hpv_vaccinated WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END AS hpv_vaccinated, ',
    'voided ',
  'FROM ', src_triage_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_triage_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', CONCAT('`', @datatools_schema, '`.`patient_demographics`'), '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_triage_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_triage_quoted) AS message;


-- create table generalized_anxiety_disorder

-- sql
SET @target_gad_quoted = CONCAT('`', @datatools_schema, '`.`generalized_anxiety_disorder`');
SET @src_gad_quoted = CONCAT('`', @etl_schema, '`.`etl_generalized_anxiety_disorder`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_gad_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_gad_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, ',
    'patient_id, ',
    'visit_id, ',
    'visit_date, ',
    'location_id, ',
    'encounter_id, ',
    'encounter_provider, ',
    'date_created, ',
    '(CASE feeling_nervous_anxious WHEN 160215 THEN ''Not at all'' WHEN 167000 THEN ''Several days'' WHEN 167001 THEN ''More than half the days'' WHEN 167002 THEN ''Nearly every day'' ELSE NULL END) AS feeling_nervous_anxious, ',
    '(CASE control_worrying WHEN 160215 THEN ''Not at all'' WHEN 167000 THEN ''Several days'' WHEN 167001 THEN ''More than half the days'' WHEN 167002 THEN ''Nearly every day'' ELSE NULL END) AS control_worrying, ',
    '(CASE worrying_much WHEN 160215 THEN ''Not at all'' WHEN 167000 THEN ''Several days'' WHEN 167001 THEN ''More than half the days'' WHEN 167002 THEN ''Nearly every day'' ELSE NULL END) AS worrying_much, ',
    '(CASE trouble_relaxing WHEN 160215 THEN ''Not at all'' WHEN 167000 THEN ''Several days'' WHEN 167001 THEN ''More than half the days'' WHEN 167002 THEN ''Nearly every day'' ELSE NULL END) AS trouble_relaxing, ',
    '(CASE being_restless WHEN 160215 THEN ''Not at all'' WHEN 167000 THEN ''Several days'' WHEN 167001 THEN ''More than half the days'' WHEN 167002 THEN ''Nearly every day'' ELSE NULL END) AS being_restless, ',
    '(CASE feeling_bad WHEN 160215 THEN ''Not at all'' WHEN 167000 THEN ''Several days'' WHEN 167001 THEN ''More than half the days'' WHEN 167002 THEN ''Nearly every day'' ELSE NULL END) AS feeling_bad, ',
    '(CASE feeling_afraid WHEN 160215 THEN ''Not at all'' WHEN 167000 THEN ''Several days'' WHEN 167001 THEN ''More than half the days'' WHEN 167002 THEN ''Nearly every day'' ELSE NULL END) AS feeling_afraid, ',
    '(CASE assessment_outcome WHEN 159410 THEN ''Minimal Anxiety'' WHEN 1498 THEN ''Mild Anxiety'' WHEN 1499 THEN ''Moderate Anxiety'' WHEN 1500 THEN ''Severe Anxiety'' ELSE NULL END) AS assessment_outcome, ',
    'voided ',
  'FROM ', src_gad_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_gad_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_gad_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_gad_quoted) AS message;

-- Create table prep_monthly_refill

SET @target_prep_quoted = CONCAT('`', @datatools_schema, '`.`prep_monthly_refill`');
SET @src_prep_quoted    = CONCAT('`', @etl_schema, '`.`etl_prep_monthly_refill`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_prep_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_prep_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, provider, patient_id, visit_id, DATE(visit_date) AS visit_date, location_id, encounter_id, ',
    'date_created, date_last_modified, assessed_for_behavior_risk, risk_for_hiv_positive_partner, ',
    'client_assessment, adherence_assessment, poor_adherence_reasons, other_poor_adherence_reasons, ',
    'adherence_counselling_done, prep_status, switching_option, switching_date, prep_type, ',
    'prescribed_prep_today, prescribed_regimen, prescribed_regimen_months, number_of_condoms_issued, ',
    'prep_discontinue_reasons, prep_discontinue_other_reasons, appointment_given, next_appointment, remarks, voided ',
  'FROM ', src_prep_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_prep_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_prep_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_prep_quoted, ' ADD INDEX(encounter_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_prep_quoted, ' ADD INDEX(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_prep_quoted) AS message;

-- Create table prep_enrolment
-- sql
SET @target_prep_quoted = CONCAT('`', @datatools_schema, '`.`prep_enrolment`');
SET @src_prep_quoted    = CONCAT('`', @etl_schema, '`.`etl_prep_enrolment`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_prep_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_prep_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, ',
    'provider, ',
    'patient_id, ',
    'visit_id, ',
    'DATE(visit_date) AS visit_date, ',
    'location_id, ',
    'encounter_id, ',
    'date_created, ',
    'date_last_modified, ',
    'patient_type, ',
    'CASE population_type WHEN 164928 THEN ''General Population'' WHEN 6096 THEN ''Discordant Couple'' WHEN 164929 THEN ''Key Population'' ELSE NULL END AS population_type, ',
    'CASE kp_type WHEN 162277 THEN ''People in prison and other closed settings'' WHEN 105 THEN ''PWID'' WHEN 160578 THEN ''MSM'' WHEN 165084 THEN ''MSW'' WHEN 160579 THEN ''FSW'' ELSE NULL END AS kp_type, ',
    'transfer_in_entry_point, ',
    'referred_from, ',
    'transit_from, ',
    'transfer_in_date, ',
    'transfer_from, ',
    'initial_enrolment_date, ',
    'date_started_prep_trf_facility, ',
    'previously_on_prep, ',
    'prep_type, ',
    'regimen, ',
    'prep_last_date, ',
    'CASE in_school WHEN 1 THEN ''Yes'' WHEN 2 THEN ''No'' ELSE NULL END AS in_school, ',
    'buddy_name, ',
    'buddy_alias, ',
    'buddy_relationship, ',
    'buddy_phone, ',
    'buddy_alt_phone, ',
    'voided ',
  'FROM ', src_prep_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_prep_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_prep_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_prep_quoted, ' ADD INDEX(encounter_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_prep_quoted) AS message;


-- Create table cervical_cancer_screening

-- sql
SET @target_cervical_quoted = CONCAT('`', @datatools_schema, '`.`cervical_cancer_screening`');
SET @src_cervical_quoted    = CONCAT('`', @etl_schema, '`.`etl_cervical_cancer_screening`');
SET @target_pd_quoted       = CONCAT('`', @datatools_schema, '`.`patient_demographics`');

SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_cervical_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_cervical_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, encounter_id, encounter_provider, patient_id, visit_id, visit_date, location_id, ',
    'date_created, date_last_modified, visit_type, ',
    'screening_type, post_treatment_complication_cause, post_treatment_complication_other, ',
    'cervical_cancer, colposcopy_screening_method, hpv_screening_method, pap_smear_screening_method, via_vili_screening_method, ',
    'colposcopy_screening_result, hpv_screening_result, pap_smear_screening_result, via_vili_screening_result, ',
    'colposcopy_treatment_method, hpv_treatment_method, pap_smear_treatment_method, via_vili_treatment_method, ',
    'colorectal_cancer, fecal_occult_screening_method, colonoscopy_method, fecal_occult_screening_results, colonoscopy_method_results, ',
    'fecal_occult_screening_treatment, colonoscopy_method_treatment, ',
    'retinoblastoma_cancer, retinoblastoma_eua_screening_method, retinoblastoma_gene_method, ',
    'retinoblastoma_eua_screening_results, retinoblastoma_gene_method_results, retinoblastoma_eua_treatment, retinoblastoma_gene_treatment, ',
    'prostate_cancer, digital_rectal_prostate_examination, digital_rectal_prostate_results, digital_rectal_prostate_treatment, ',
    'prostatic_specific_antigen_test, prostatic_specific_antigen_results, prostatic_specific_antigen_treatment, ',
    'oral_cancer, oral_cancer_visual_exam_method, oral_cancer_cytology_method, oral_cancer_imaging_method, oral_cancer_biopsy_method, ',
    'oral_cancer_visual_exam_results, oral_cancer_cytology_results, oral_cancer_imaging_results, oral_cancer_biopsy_results, ',
    'oral_cancer_visual_exam_treatment, oral_cancer_cytology_treatment, oral_cancer_imaging_treatment, oral_cancer_biopsy_treatment, ',
    'breast_cancer, clinical_breast_examination_screening_method, ultrasound_screening_method, mammography_smear_screening_method, ',
    'clinical_breast_examination_screening_result, ultrasound_screening_result, mammography_screening_result, ',
    'clinical_breast_examination_treatment_method, ultrasound_treatment_method, breast_tissue_diagnosis, breast_tissue_diagnosis_date, ',
    'reason_tissue_diagnosis_not_done, mammography_treatment_method, ',
    'referred_out, ',
    'referral_facility, referral_reason, followup_date, ',
    'hiv_status, ',
    'smoke_cigarattes, ',
    'other_forms_tobacco, ',
    'take_alcohol, ',
    'previous_treatment, ',
    'signs_symptoms_specify, ',
    'family_history, ',
    'number_of_years_smoked, number_of_cigarette_per_day, clinical_notes, voided ',
  'FROM ', @src_cervical_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT('ALTER TABLE ', @target_cervical_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT('ALTER TABLE ', @target_cervical_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT CONCAT("Successfully created ", @target_cervical_quoted) AS message;


-- create table datatools_patient_contact

-- sql
SET @target_pc_quoted = CONCAT('`', @datatools_schema, '`.`patient_contact`');
SET @src_pc_quoted    = CONCAT('`', @etl_schema, '`.`etl_patient_contact`');
SET @target_pd_quoted = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_pc_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_pc_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'encounter_id, ',
    'patient_id, ',
    'patient_related_to, ',
    'CASE relationship_type ',
      'WHEN 970 THEN ''Mother'' ',
      'WHEN 971 THEN ''Father'' ',
      'WHEN 1528 THEN ''Child'' ',
      'WHEN 973 THEN ''Grandparent'' ',
      'WHEN 972 THEN ''Sibling'' ',
      'WHEN 160639 THEN ''Guardian'' ',
      'WHEN 1527 THEN ''Parent'' ',
      'WHEN 5617 THEN ''Spouse'' ',
      'WHEN 162221 THEN ''Co-wife'' ',
      'WHEN 163565 THEN ''Sexual partner'' ',
      'WHEN 157351 THEN ''Injectable drug user'' ',
      'WHEN 166606 THEN ''SNS'' ',
      'WHEN 5622 THEN ''Other'' ',
      'ELSE '''' ',
    'END AS relationship_type, ',
    'date_created, start_date, end_date, physical_address, baseline_hiv_status, reported_test_date, ',
    'CASE living_with_patient WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 162570 THEN ''Declined to Answer'' ELSE '''' END AS living_with_patient, ',
    'CASE pns_approach WHEN 162284 THEN ''Dual referral'' WHEN 160551 THEN ''Passive referral'' WHEN 161642 THEN ''Contract referral'' WHEN 163096 THEN ''Provider referral'' ELSE '''' END AS pns_approach, ',
    'appointment_date, ipv_outcome, contact_listing_decline_reason, ',
    'CASE consented_contact_listing WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Unknown'' ELSE '''' END AS consented_contact_listing, ',
    'encounter_provider, date_last_modified, location_id, uuid, voided ',
  'FROM ', src_pc_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_pc_quoted, ' ADD PRIMARY KEY(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_pc_quoted, ' ADD FOREIGN KEY (patient_related_to) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_pc_quoted, ' ADD INDEX(date_created)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_pc_quoted) AS message;

-- create table datatools_client_tracesql

SET @target_client_quoted = CONCAT('`', @datatools_schema, '`.`client_trace`');
SET @src_client_quoted    = CONCAT('`', @etl_schema, '`.`etl_client_trace`');
SET @target_pc_quoted     = CONCAT('`', @datatools_schema, '`.`patient_contact`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_client_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_client_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'id, uuid, date_created, date_last_modified, encounter_date, client_id, contact_type, status, unique_patient_no, ',
    'facility_linked_to, health_worker_handed_to, remarks, appointment_date, voided ',
  'FROM ', src_client_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', target_client_quoted, ' ADD FOREIGN KEY (client_id) REFERENCES ', @target_pc_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', target_client_quoted, ' ADD INDEX(date_created)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', target_client_quoted) AS message;

--  --- ---- Create  table kp contact ----------------------------------------

SET @target_kp_contact_quoted = CONCAT('`', @datatools_schema, '`.`kp_contact`');
SET @src_kp_contact_quoted    = CONCAT('`', @etl_schema, '`.`etl_contact`');
SET @target_pd_quoted        = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_kp_contact_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_kp_contact_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, ',
    'client_id, ',
    'visit_id, ',
    'visit_date, ',
    'location_id, ',
    'encounter_id, ',
    'encounter_provider, ',
    'date_created, ',
    'date_last_modified, ',
    'patient_type, ',
    'transfer_in_date, ',
    'date_first_enrolled_in_kp, ',
    'facility_transferred_from, ',
    'key_population_type, ',
    'priority_population_type, ',
    'implementation_county, ',
    'implementation_subcounty, ',
    'implementation_ward, ',
    'contacted_by_peducator, ',
    'program_name, ',
    'frequent_hotspot_name, ',
    'frequent_hotspot_type, ',
    'year_started_sex_work, ',
    'year_started_sex_with_men, ',
    'year_started_drugs, ',
    'avg_weekly_sex_acts, ',
    'avg_weekly_anal_sex_acts, ',
    'avg_daily_drug_injections, ',
    'contact_person_name, ',
    'contact_person_alias, ',
    'contact_person_phone, ',
    'voided ',
  'FROM ', @src_kp_contact_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kp_contact_quoted, ' ADD FOREIGN KEY (client_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kp_contact_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_kp_contact_quoted) AS message;

-- ----------------------- kp_client_enrollment ------------



SET @target_kp_client_quoted = CONCAT('`', @datatools_schema, '`.`kp_client_enrollment`');
SET @src_kp_client_quoted    = CONCAT('`', @etl_schema, '`.`etl_client_enrollment`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_kp_client_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_kp_client_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, client_id, visit_id, visit_date, location_id, encounter_id, encounter_provider, ',
    'date_created, date_last_modified, contacted_for_prevention, has_regular_free_sex_partner, ',
    'year_started_sex_work, year_started_sex_with_men, year_started_drugs, ',
    'has_expereienced_sexual_violence, has_expereienced_physical_violence, ever_tested_for_hiv, ',
    'test_type, share_test_results, willing_to_test, test_decline_reason, receiving_hiv_care, ',
    'care_facility_name, ccc_number, vl_test_done, vl_results_date, contact_for_appointment, ',
    'contact_method, buddy_name, buddy_phone_number, voided ',
  'FROM ', @src_kp_client_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kp_client_quoted, ' ADD FOREIGN KEY (client_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kp_client_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_kp_client_quoted) AS message;


-- ------ Create table kp_clinical_visit

-- sql
SET @target_kp_clinical_quoted = CONCAT('`', @datatools_schema, '`.`kp_clinical_visit`');
SET @src_kp_clinical_quoted = CONCAT('`', @etl_schema, '`.`etl_clinical_visit`');
SET @target_pd_quoted = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_kp_clinical_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_kp_clinical_quoted, ' AS SELECT ',
    'uuid,',
    'client_id,',
    'visit_id,',
    'visit_date,',
    'location_id,',
    'encounter_id,',
    'encounter_provider,',
    'date_created,',
    'date_last_modified,',
    'implementing_partner,',
    'type_of_visit,',
    'visit_reason,',
    'service_delivery_model,',
    'sti_screened,',
    'sti_results,',
    'sti_treated,',
    'sti_referred,',
    'sti_referred_text,',
    'tb_screened,',
    'tb_results,',
    'tb_treated,',
    'tb_referred,',
    'tb_referred_text,',
    'hepatitisB_screened,',
    'hepatitisB_results,',
    'hepatitisB_confirmatory_results,',
    'hepatitisB_vaccinated,',
    'hepatitisB_treated,',
    'hepatitisB_referred,',
    'hepatitisB_text,',
    'hepatitisC_screened,',
    'hepatitisC_results,',
    'hepatitisC_confirmatory_results,',
    'hepatitisC_treated,',
    'hepatitisC_referred,',
    'hepatitisC_text,',
    'overdose_screened,',
    'overdose_results,',
    'overdose_treated,',
    'received_naloxone,',
    'overdose_referred,',
    'overdose_text,',
    'abscess_screened,',
    'abscess_results,',
    'abscess_treated,',
    'abscess_referred,',
    'abscess_text,',
    'alcohol_screened,',
    'alcohol_results,',
    'alcohol_treated,',
    'alcohol_referred,',
    'alcohol_text,',
    'cerv_cancer_screened,',
    'cerv_cancer_results,',
    'cerv_cancer_treated,',
    'cerv_cancer_referred,',
    'cerv_cancer_text,',
    'anal_cancer_screened,',
    'anal_cancer_results,',
    'prep_screened,',
    'prep_results,',
    'prep_treated,',
    'prep_referred,',
    'prep_text,',
    'violence_screened,',
    'violence_results,',
    'violence_treated,',
    'violence_referred,',
    'violence_text,',
    'risk_red_counselling_screened,',
    'risk_red_counselling_eligibility,',
    'risk_red_counselling_support,',
    'risk_red_counselling_ebi_provided,',
    'risk_red_counselling_text,',
    'fp_screened,',
    'fp_eligibility,',
    'fp_treated,',
    'fp_referred,',
    'fp_text,',
    'mental_health_screened,',
    'mental_health_results,',
    'mental_health_support,',
    'mental_health_referred,',
    'mental_health_text,',
    'mat_screened,',
    'mat_results,',
    'mat_treated,',
    'mat_referred,',
    'mat_text,',
    'hiv_self_rep_status,',
    'last_hiv_test_setting,',
    'counselled_for_hiv,',
    'hiv_tested,',
    'test_frequency,',
    'received_results,',
    'test_results,',
    'linked_to_art,',
    'facility_linked_to,',
    'self_test_education,',
    'self_test_kits_given,',
    'self_use_kits,',
    'distribution_kits,',
    'self_tested,',
    'hiv_test_date,',
    'self_test_frequency,',
    'self_test_results,',
    'test_confirmatory_results,',
    'confirmatory_facility,',
    'offsite_confirmatory_facility,',
    'self_test_linked_art,',
    'self_test_link_facility,',
    'hiv_care_facility,',
    'other_hiv_care_facility,',
    'initiated_art_this_month,',
    'started_on_art,',
    'date_started_art,',
    'active_art,',
    'primary_care_facility_name,',
    'ccc_number,',
    'eligible_vl,',
    'vl_test_done,',
    'vl_results,',
    'vl_results_date,',
    'received_vl_results,',
    'condom_use_education,',
    'post_abortal_care,',
    'referral,',
    'linked_to_psychosocial,',
    'male_condoms_no,',
    'female_condoms_no,',
    'lubes_no,',
    'syringes_needles_no,',
    'pep_eligible,',
    'case pep_status when 166665 then ''Initiated'' when 164463 then ''Not Initiated'' when 1175 then ''Not applicable'' end as pep_status,',
    'exposure_type,',
    'other_exposure_type,',
    'case initiated_pep_within_72hrs when 1065 then ''Yes'' when 1066 then ''No'' end as initiated_pep_within_72hrs,',
    'clinical_notes,',
    'appointment_date,',
    'voided ',
  'FROM ', src_kp_clinical_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kp_clinical_quoted, ' ADD FOREIGN KEY (client_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kp_clinical_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_kp_clinical_quoted) AS message;

-- Create table kp_peer_calendar
-- sql
SET @target_kp_peer_quoted = CONCAT('`', @datatools_schema, '`.`kp_peer_calendar`');
SET @src_kp_peer_quoted    = CONCAT('`', @etl_schema, '`.`etl_peer_calendar`');
SET @target_pd_quoted      = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_kp_peer_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_kp_peer_quoted, ' AS SELECT ',
    'uuid,',
    'client_id,',
    'visit_id,',
    'visit_date,',
    'location_id,',
    'encounter_id,',
    'encounter_provider,',
    'date_created,',
    'date_last_modified,',
    'hotspot_name,',
    'typology,',
    'other_hotspots,',
    'weekly_sex_acts,',
    'monthly_condoms_required,',
    'weekly_anal_sex_acts,',
    'monthly_lubes_required,',
    'daily_injections,',
    'monthly_syringes_required,',
    'years_in_sexwork_drugs,',
    'experienced_violence,',
    'service_provided_within_last_month,',
    'monthly_n_and_s_distributed,',
    'monthly_male_condoms_distributed,',
    'monthly_lubes_distributed,',
    'monthly_female_condoms_distributed,',
    'monthly_self_test_kits_distributed,',
    'received_clinical_service,',
    'violence_reported,',
    'referred,',
    'health_edu,',
    'remarks,',
    'voided ',
  'FROM ', src_kp_peer_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kp_peer_quoted, ' ADD FOREIGN KEY (client_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kp_peer_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_kp_peer_quoted) AS message;

-- Create table kp_sti_treatment

-- sql
SET @target_kp_sti_quoted = CONCAT('`', @datatools_schema, '`.`kp_sti_treatment`');
SET @src_kp_sti_quoted    = CONCAT('`', @etl_schema, '`.`etl_sti_treatment`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_kp_sti_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_kp_sti_quoted, ' AS ',
  'SELECT ',
    'uuid,',
    'client_id,',
    'visit_id,',
    'visit_date,',
    'location_id,',
    'encounter_id,',
    'encounter_provider,',
    'date_created,',
    'date_last_modified,',
    'visit_reason,',
    'syndrome,',
    'other_syndrome,',
    'drug_prescription,',
    'other_drug_prescription,',
    'genital_exam_done,',
    'lab_referral,',
    'lab_form_number,',
    'referred_to_facility,',
    'facility_name,',
    'partner_referral_done,',
    'given_lubes,',
    'no_of_lubes,',
    'given_condoms,',
    'no_of_condoms,',
    'provider_comments,',
    'provider_name,',
    'appointment_date,',
    'voided ',
  'FROM ', src_kp_sti_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'ALTER TABLE ', @target_kp_sti_quoted,
  ' ADD FOREIGN KEY (client_id) REFERENCES ',
  CONCAT('`', @datatools_schema, '`.`patient_demographics`'),
  '(patient_id)'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kp_sti_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_kp_sti_quoted) AS message;


-- Create table kp_peer_tracking
SET @target_kp_peer_quoted = CONCAT('`', @datatools_schema, '`.`kp_peer_tracking`');
SET @src_kp_peer_quoted = CONCAT('`', @etl_schema, '`.`etl_peer_tracking`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_kp_peer_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_kp_peer_quoted, ' AS ',
  'SELECT ',
    'uuid, provider, client_id, visit_id, visit_date, location_id, encounter_id, ',
    'tracing_attempted, tracing_not_attempted_reason, attempt_number, tracing_date, tracing_type, ',
    'tracing_outcome, is_final_trace, tracing_outcome_status, voluntary_exit_comment, status_in_program, ',
    'source_of_information, other_informant, date_created, date_last_modified, voided ',
  'FROM ', src_kp_peer_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'ALTER TABLE ', @target_kp_peer_quoted,
  ' ADD FOREIGN KEY (client_id) REFERENCES ',
  CONCAT('`', @datatools_schema, '`.`patient_demographics`'),
  '(patient_id)'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kp_peer_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_kp_peer_quoted) AS message;

-- Create table kp_treatment_verification

SET @target_kp_treatment_quoted = CONCAT('`', @datatools_schema, '`.`kp_treatment_verification`');
SET @src_kp_treatment_quoted    = CONCAT('`', @etl_schema, '`.`etl_treatment_verification`');
SET @target_pd_quoted           = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_kp_treatment_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_kp_treatment_quoted, ' AS SELECT ',
    'uuid, provider, client_id, visit_id, visit_date, location_id, encounter_id, ',
    'date_diagnosed_with_hiv, art_health_facility, ccc_number, is_pepfar_site, ',
    'date_initiated_art, current_regimen, information_source, cd4_test_date, cd4, ',
    'vl_test_date, viral_load, disclosed_status, person_disclosed_to, other_person_disclosed_to, ',
    'IPT_start_date, IPT_completion_date, on_diff_care, in_support_group, support_group_name, ',
    'opportunistic_infection, oi_diagnosis_date, oi_treatment_start_date, oi_treatment_end_date, comment, ',
    'date_created, date_last_modified, voided ',
  'FROM ', src_kp_treatment_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kp_treatment_quoted, ' ADD FOREIGN KEY (client_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kp_treatment_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_kp_treatment_quoted) AS message;


-- create table alcohol_drug_abuse_screening

SET @target_adq_quoted = CONCAT('`', @datatools_schema, '`.`alcohol_drug_abuse_screening`');
SET @src_adq_quoted    = CONCAT('`', @etl_schema, '`.`etl_alcohol_drug_abuse_screening`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_adq_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_adq_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, uuid, provider, visit_id, visit_date, encounter_id, location_id, ',
    'CASE alcohol_drinking_frequency WHEN 1090 THEN ''Never'' WHEN 1091 THEN ''Monthly or less'' WHEN 1092 THEN ''2 to 4 times a month'' WHEN 1093 THEN ''2 to 3 times a week'' WHEN 1094 THEN ''4 or More Times a Week'' END AS alcohol_drinking_frequency, ',
    'CASE smoking_frequency WHEN 1090 THEN ''Never smoked'' WHEN 156358 THEN ''Former cigarette smoker'' WHEN 163197 THEN ''Current some day smoker'' WHEN 163196 THEN ''Current light tobacco smoker'' WHEN 163195 THEN ''Current heavy tobacco smoker'' WHEN 163200 THEN ''Unknown if ever smoked'' END AS smoking_frequency, ',
    'CASE drugs_use_frequency WHEN 1090 THEN ''Never'' WHEN 1091 THEN ''Monthly or less'' WHEN 1092 THEN ''2 to 4 times a month'' WHEN 1093 THEN ''2 to 3 times a week'' WHEN 1094 THEN ''4 or More Times a Week'' END AS drugs_use_frequency, ',
    'date_created, date_last_modified, voided ',
  'FROM ', src_adq_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @target_pd_quoted = IFNULL(target_pd_quoted, CONCAT('`', @datatools_schema, '`.`patient_demographics`'));
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_adq_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_adq_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_adq_quoted) AS message;

-- create table gbv_screening

-- sql
SET @target_gbv_quoted = CONCAT('`', @datatools_schema, '`.`gbv_screening`');
SET @src_gbv_quoted    = CONCAT('`', @etl_schema, '`.`etl_gbv_screening`');
SET @sql_stmt = CONCAT(
  'DROP TABLE IF EXISTS ', @target_gbv_quoted, '; ',
  'CREATE TABLE ', @target_gbv_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, uuid, provider, visit_id, visit_date, encounter_id, location_id, ',
    '(CASE ipv WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS ipv, ',
    '(CASE physical_ipv WHEN 158358 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS physical_ipv, ',
    '(CASE emotional_ipv WHEN 118688 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS emotional_ipv, ',
    '(CASE sexual_ipv WHEN 152370 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS sexual_ipv, ',
    '(CASE ipv_relationship WHEN 1582 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS ipv_relationship, ',
    'date_created, date_last_modified, voided ',
  'FROM ', src_gbv_quoted, ';'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @target_pd_quoted = IFNULL(target_pd_quoted, CONCAT('`', @datatools_schema, '`.`patient_demographics`'));
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_gbv_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_gbv_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('SELECT ''Successfully created '' , ', @target_gbv_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -------------- create table gbv_screening

SET @target_gbv_action_quoted = CONCAT('`', @datatools_schema, '`.`gbv_screening_action`');
SET @src_gbv_action_quoted = CONCAT('`', @etl_schema, '`.`etl_gbv_screening_action`');
SET @sql_stmt = CONCAT(
  'DROP TABLE IF EXISTS ', @target_gbv_action_quoted, '; ',
  'CREATE TABLE ', @target_gbv_action_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, uuid, encounter_id, provider, visit_id, visit_date, obs_id, location_id, ',
    '(CASE help_provider WHEN 1589 THEN ''Hospital'' WHEN 165284 THEN ''Police'' WHEN 165037 THEN ''Peer Educator'' WHEN 1560 THEN ''Family'' WHEN 165294 THEN ''Peers'' WHEN 5618 THEN ''Friends'' WHEN 165290 THEN ''Religious Leader'' WHEN 165350 THEN ''Dice'' WHEN 162690 THEN ''Chief'' WHEN 5622 THEN ''Other'' ELSE '''' END) AS help_provider, ',
    '(CASE action_taken WHEN 1066 THEN ''No action taken'' WHEN 165070 THEN ''Counselling'' WHEN 160570 THEN ''Emergency pills'' WHEN 1356 THEN ''Hiv testing'' WHEN 130719 THEN ''Investigation done'' WHEN 135914 THEN ''Matter presented to court'' WHEN 165228 THEN ''P3 form issued'' WHEN 165171 THEN ''PEP given'' WHEN 165192 THEN ''Perpetrator arrested'' WHEN 127910 THEN ''Post rape care'' WHEN 165203 THEN ''PrEP given'' WHEN 5618 THEN ''Reconciliation'' WHEN 165093 THEN ''Referred back to the family'' WHEN 165274 THEN ''Referred to hospital'' WHEN 165180 THEN ''Statement taken'' WHEN 165200 THEN ''STI Prophylaxis'' WHEN 165184 THEN ''Trauma counselling done'' WHEN 1185 THEN ''Treatment'' WHEN 5622 THEN ''Other'' ELSE '''' END) AS action_taken, ',
    'action_date AS action_date, ',
    '(CASE reason_for_not_reporting WHEN 1067 THEN ''Did not know where to report'' WHEN 1811 THEN ''Distance'' WHEN 140923 THEN ''Exhaustion/Lack of energy'' WHEN 163473 THEN ''Fear shame'' WHEN 159418 THEN ''Lack of faith in system'' WHEN 162951 THEN ''Lack of knowledge'' WHEN 664 THEN ''Negative attitude of the person reported to'' WHEN 143100 THEN ''Not allowed culturally'' WHEN 165161 THEN ''Perpetrator above the law'' WHEN 163475 THEN ''Self blame'' ELSE '''' END) AS reason_for_not_reporting, ',
    'date_created, date_last_modified, voided ',
  'FROM ', src_gbv_action_quoted, ';'
);

PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @target_pd_quoted = IFNULL(target_pd_quoted, CONCAT('`', @datatools_schema, '`.`patient_demographics`'));
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_gbv_action_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_gbv_action_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_gbv_action_quoted) AS message;


-- --------------- create table etl_violence_reporting ------------

SET @target_violence_quoted = CONCAT('`', @datatools_schema, '`.`violence_reporting`');
SET @src_violence_quoted    = CONCAT('`', @etl_schema, '`.`etl_violence_reporting`');
SET @target_pd_quoted       = CONCAT('`', @datatools_schema, '`.`patient_demographics`');

SET @sql_stmt = CONCAT(
  'DROP TABLE IF EXISTS ', @target_violence_quoted, '; ',
  'CREATE TABLE ', @target_violence_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, ',
    'place_of_incident, date_of_incident, ',
    '(CASE time_of_incident WHEN 165194 THEN ''AM'' WHEN 165195 THEN ''PM'' ELSE '''' END) AS time_of_incident, ',
    '(CASE abuse_against WHEN 165163 THEN ''Group'' WHEN 165162 THEN ''Individual'' ELSE '''' END) AS abuse_against, ',
    'form_of_incident, perpetrator, date_of_crisis_response, support_service, hiv_testing_duration, ',
    '(CASE hiv_testing_provided_within_5_days WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS hiv_testing_provided_within_5_days, ',
    'duration_on_emergency_contraception, ',
    '(CASE emergency_contraception_provided_within_5_days WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS emergency_contraception_provided_within_5_days, ',
    'psychosocial_trauma_counselling_duration, ',
    '(CASE psychosocial_trauma_counselling_provided_within_5_days WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS psychosocial_trauma_counselling_provided_within_5_days, ',
    'pep_provided_duration, ',
    '(CASE pep_provided_within_5_days WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS pep_provided_within_5_days, ',
    'sti_screening_and_treatment_duration, ',
    '(CASE sti_screening_and_treatment_provided_within_5_days WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS sti_screening_and_treatment_provided_within_5_days, ',
    'legal_support_duration, ',
    '(CASE legal_support_provided_within_5_days WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS legal_support_provided_within_5_days, ',
    'medical_examination_duration, ',
    '(CASE medical_examination_provided_within_5_days WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS medical_examination_provided_within_5_days, ',
    'prc_form_file_duration, ',
    '(CASE prc_form_file_provided_within_5_days WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS prc_form_file_provided_within_5_days, ',
    'other_services_provided, medical_services_and_care_duration, ',
    '(CASE medical_services_and_care_provided_within_5_days WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS medical_services_and_care_provided_within_5_days, ',
    'psychosocial_trauma_counselling_durationA, ',
    '(CASE psychosocial_trauma_counselling_provided_within_5_daysA WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS psychosocial_trauma_counselling_provided_within_5_daysA, ',
    'duration_of_none_sexual_legal_support, ',
    '(CASE duration_of_none_sexual_legal_support_within_5_days WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS duration_of_none_sexual_legal_support_within_5_days, ',
    '(CASE current_Location_of_person WHEN 1536 THEN ''Home'' WHEN 160432 THEN ''Dead'' WHEN 162277 THEN ''Imprisoned'' WHEN 1896 THEN ''Hospitalized'' WHEN 165227 THEN ''Safe place'' ELSE '''' END) AS current_Location_of_person, ',
    'follow_up_plan, resolution_date, date_created, date_last_modified, voided ',
  'FROM ', src_violence_quoted, ';'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_violence_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_violence_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_violence_quoted) AS message;



-- create table link_facility_tracking--------------------

SET @target_link_quoted = CONCAT('`', @datatools_schema, '`.`link_facility_tracking`');
SET @src_link_quoted = CONCAT('`', @etl_schema, '`.`etl_link_facility_tracking`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_link_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_link_quoted, ' AS ',
  'SELECT ',
    'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, ',
    'county, sub_county, ward, facility_name, ccc_number, date_diagnosed, date_initiated_art, ',
    '(CASE original_regimen ',
      'WHEN 162559 THEN ''ABC/DDI/LPV/r'' ',
      'WHEN 162562 THEN ''ABC/LPV/R/TDF'' ',
      'WHEN 161361 THEN ''EDF/3TC/EFV'' ',
      'WHEN 792 THEN ''D4T/3TC/NVP'' ',
      'WHEN 162200 THEN ''3TC/ABC/LPV/r'' ',
      'WHEN 164970 THEN ''ABC/3TC/DTG'' ',
      'WHEN 164511 THEN ''AZT-3TC-ATV/r'' ',
      'WHEN 162563 THEN ''3TC/ABC/EFV'' ',
      'WHEN 164968 THEN ''AZT/3TC/DTG'' ',
      'WHEN 164505 THEN ''TDF-3TC-EFV'' ',
      'WHEN 817 THEN ''ABC/3TC/AZT'' ',
      'WHEN 1652 THEN ''3TC/NVP/AZT'' ',
      'WHEN 162560 THEN ''3TC/D4T/LPV/r'' ',
      'WHEN 162199 THEN ''ABC/NVP/3TC'' ',
      'WHEN 164512 THEN ''TDF-3TC-ATV/r'' ',
      'WHEN 104565 THEN ''EFV/FTC/TDF'' ',
      'WHEN 162201 THEN ''3TC/LPV/TDF/r'' ',
      'WHEN 162565 THEN ''3TC/NVP/TDF'' ',
      'WHEN 162561 THEN ''3TC/AZT/LPV/r'' ',
      'WHEN 160124 THEN ''AZT/3TC/EFV'' ',
      'WHEN 160104 THEN ''D4T/3TC/EFV'' ',
    'ELSE '''' END) AS original_regimen, ',
    '(CASE current_regimen ',
      'WHEN 164968 THEN ''AZT/3TC/DTG'' ',
      'WHEN 164969 THEN ''TDF/3TC/DTG'' ',
      'WHEN 164970 THEN ''ABC/3TC/DTG'' ',
      'WHEN 164505 THEN ''TDF-3TC-EFV'' ',
      'WHEN 792 THEN ''D4T/3TC/NVP'' ',
      'WHEN 160124 THEN ''AZT/3TC/EFV'' ',
      'WHEN 160104 THEN ''D4T/3TC/EFV'' ',
      'WHEN 1652 THEN ''3TC/NVP/AZT'' ',
      'WHEN 161361 THEN ''EDF/3TC/EFV'' ',
      'WHEN 104565 THEN ''EFV/FTC/TDF'' ',
      'WHEN 162201 THEN ''3TC/LPV/TDF/r'' ',
      'WHEN 817 THEN ''ABC/3TC/AZT'' ',
      'WHEN 162199 THEN ''ABC/NVP/3TC'' ',
      'WHEN 162200 THEN ''3TC/ABC/LPV/r'' ',
      'WHEN 162565 THEN ''3TC/NVP/TDF'' ',
      'WHEN 162561 THEN ''3TC/AZT/LPV/r'' ',
      'WHEN 164511 THEN ''AZT-3TC-ATV/r'' ',
      'WHEN 164512 THEN ''TDF-3TC-ATV/r'' ',
      'WHEN 162560 THEN ''3TC/D4T/LPV/r'' ',
      'WHEN 162563 THEN ''3TC/ABC/EFV'' ',
      'WHEN 162562 THEN ''ABC/LPV/R/TDF'' ',
      'WHEN 162559 THEN ''ABC/DDI/LPV/r'' ',
    'ELSE '''' END) AS current_regimen, ',
    'date_switched, reason_for_switch, date_of_last_visit, date_viral_load_sample_collected, date_viral_load_results_received, ',
    '(CASE viral_load_results WHEN 167484 THEN ''LDL'' WHEN 167485 THEN ''Copies'' WHEN 1107 THEN ''None'' ELSE '''' END) AS viral_load_results, ',
    'viral_load_results_copies, date_of_next_visit, ',
    '(CASE enrolled_in_pssg WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS enrolled_in_pssg, ',
    '(CASE attended_pssg WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS attended_pssg, ',
    '(CASE on_pmtct WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS on_pmtct, ',
    'date_of_delivery, ',
    '(CASE tb_screening WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS tb_screening, ',
    '(CASE sti_treatment WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS sti_treatment, ',
    '(CASE trauma_counselling WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''NA'' ELSE '''' END) AS trauma_counselling, ',
    '(CASE cervical_cancer_screening WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''NA'' ELSE '''' END) AS cervical_cancer_screening, ',
    '(CASE family_planning WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''NA'' ELSE '''' END) AS family_planning, ',
    '(CASE currently_on_tb_treatment WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END) AS currently_on_tb_treatment, ',
    'date_initiated_tb_treatment, ',
    '(CASE tpt_status WHEN 1264 THEN ''On TPT'' WHEN 1267 THEN ''Completed'' WHEN 167156 THEN ''Declined'' WHEN 1090 THEN ''Never Initiated'' ELSE '''' END) AS tpt_status, ',
    'date_initiated_tpt, ',
    '(CASE data_collected_through WHEN 1502 THEN ''Visiting Facility'' WHEN 162189 THEN ''Calling Facility'' WHEN 978 THEN ''Self-reported'' ELSE '''' END) AS data_collected_through, ',
    'date_created, date_last_modified, voided ',
  'FROM ', src_link_quoted, ';'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_link_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', CONCAT('`', @datatools_schema, '`.`patient_demographics`'), '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_link_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_link_quoted) AS message;

-- create table depression_screening--------------------------------

SET @target_depr_quoted = CONCAT('`', @datatools_schema, '`.`depression_screening`');
SET @src_depr_quoted    = CONCAT('`', @etl_schema, '`.`etl_depression_screening`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_depr_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_depr_quoted, ' AS ',
  'SELECT ',
    'patient_id, uuid, provider, visit_id, visit_date, encounter_id, location_id, ',
    '(CASE little_interest WHEN 163733 THEN ''Not at all'' WHEN 163734 THEN ''Several days'' WHEN 163735 THEN ''More than half the days'' WHEN 163736 THEN ''Nearly every day'' END) AS little_interest, ',
    '(CASE feeling_down WHEN 163733 THEN ''Not at all'' WHEN 163734 THEN ''Several days'' WHEN 163735 THEN ''More than half the days'' WHEN 163736 THEN ''Nearly every day'' END) AS feeling_down, ',
    '(CASE trouble_sleeping WHEN 163733 THEN ''Not at all'' WHEN 163734 THEN ''Several days'' WHEN 163735 THEN ''More than half the days'' WHEN 163736 THEN ''Nearly every day'' END) AS trouble_sleeping, ',
    '(CASE feeling_tired WHEN 163733 THEN ''Not at all'' WHEN 163734 THEN ''Several days'' WHEN 163735 THEN ''More than half the days'' WHEN 163736 THEN ''Nearly every day'' END) AS feeling_tired, ',
    '(CASE poor_appetite WHEN 163733 THEN ''Not at all'' WHEN 163734 THEN ''Several days'' WHEN 163735 THEN ''More than half the days'' WHEN 163736 THEN ''Nearly every day'' END) AS poor_appetite, ',
    '(CASE feeling_bad WHEN 163733 THEN ''Not at all'' WHEN 163734 THEN ''Several days'' WHEN 163735 THEN ''More than half the days'' WHEN 163736 THEN ''Nearly every day'' END) AS feeling_bad, ',
    '(CASE trouble_concentrating WHEN 163733 THEN ''Not at all'' WHEN 163734 THEN ''Several days'' WHEN 163735 THEN ''More than half the days'' WHEN 163736 THEN ''Nearly every day'' END) AS trouble_concentrating, ',
    '(CASE moving_or_speaking_slowly WHEN 163733 THEN ''Not at all'' WHEN 163734 THEN ''Several days'' WHEN 163735 THEN ''More than half the days'' WHEN 163736 THEN ''Nearly every day'' END) AS moving_or_speaking_slowly, ',
    '(CASE self_hurtful_thoughts WHEN 163733 THEN ''Not at all'' WHEN 163734 THEN ''Several days'' WHEN 163735 THEN ''More than half the days'' WHEN 163736 THEN ''Nearly every day'' END) AS self_hurtful_thoughts, ',
    '(CASE PHQ_9_rating WHEN 1115 THEN ''Depression unlikely'' WHEN 157790 THEN ''Mild depression'' WHEN 134011 THEN ''Moderate depression'' WHEN 134017 THEN ''Moderate severe depression'' WHEN 126627 THEN ''Severe depression'' END) AS PHQ_9_rating, ',
    '(CASE pfa_offered WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS pfa_offered, ',
    '(CASE client_referred WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS client_referred, ',
    '(CASE facility_referred WHEN 163266 THEN ''This Facility'' WHEN 164407 THEN ''Other health facility'' END) AS facility_referred, ',
    'facility_name, services_referred_for, date_created, date_last_modified, voided ',
  'FROM ', src_depr_quoted, ';'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @target_pd_quoted = IFNULL(target_pd_quoted, CONCAT('`', @datatools_schema, '`.`patient_demographics`'));
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_depr_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_depr_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_depr_quoted) AS message;

-- create table adverse_events (tenant-aware)
SET @target_adv_quoted = CONCAT('`', @datatools_schema, '`.`adverse_events`');
SET @src_adv_quoted    = CONCAT('`', @etl_schema, '`.`etl_adverse_events`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_adv_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_adv_quoted, ' AS ',
  'SELECT ',
    'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, obs_id, ',
    '(CASE cause WHEN 70056 THEN ''Abicavir'' WHEN 162298 THEN ''ACE inhibitors'' WHEN 70878 THEN ''Allopurinol'' WHEN 155060 THEN ''Aminoglycosides'' WHEN 162299 THEN ''ARBs (angiotensin II receptor blockers)'' WHEN 103727 THEN ''Aspirin'' WHEN 71647 THEN ''Atazanavir'' WHEN 72822 THEN ''Carbamazepine'' WHEN 162301 THEN ''Cephalosporins'' WHEN 73300 THEN ''Chloroquine'' WHEN 73667 THEN ''Codeine'' WHEN 74807 THEN ''Didanosine'' WHEN 75523 THEN ''Efavirenz'' WHEN 162302 THEN ''Erythromycins'' WHEN 75948 THEN ''Ethambutol'' WHEN 77164 THEN ''Griseofulvin'' WHEN 162305 THEN ''Heparins'' WHEN 77675 THEN ''Hydralazine'' WHEN 78280 THEN ''Isoniazid'' WHEN 794 THEN ''Lopinavir/ritonavir'' WHEN 80106 THEN ''Morphine'' WHEN 80586 THEN ''Nevirapine'' WHEN 80696 THEN ''Nitrofurans'' WHEN 162306 THEN ''Non-steroidal anti-inflammatory drugs'' WHEN 81723 THEN ''Penicillamine'' WHEN 81724 THEN ''Penicillin'' WHEN 81959 THEN ''Phenolphthaleins'' WHEN 82023 THEN ''Phenytoin'' WHEN 82559 THEN ''Procainamide'' WHEN 82900 THEN ''Pyrazinamide'' WHEN 83018 THEN ''Quinidine'' WHEN 767 THEN ''Rifampin'' WHEN 162307 THEN ''Statins'' WHEN 84309 THEN ''Stavudine'' WHEN 162170 THEN ''Sulfonamides'' WHEN 84795 THEN ''Tenofovir'' WHEN 84893 THEN ''Tetracycline'' WHEN 86663 THEN ''Zidovudine'' WHEN 5622 THEN ''Other'' END) AS cause, ',
    '(CASE adverse_event WHEN 1067 THEN ''Unknown'' WHEN 121629 THEN ''Anaemia'' WHEN 148888 THEN ''Anaphylaxis'' WHEN 148787 THEN ''Angioedema'' WHEN 120148 THEN ''Arrhythmia'' WHEN 108 THEN ''Bronchospasm'' WHEN 143264 THEN ''Cough'' WHEN 142412 THEN ''Diarrhea'' WHEN 118773 THEN ''Dystonia'' WHEN 140238 THEN ''Fever'' WHEN 140039 THEN ''Flushing'' WHEN 139581 THEN ''GI upset'' WHEN 139084 THEN ''Headache'' WHEN 159098 THEN ''Hepatotoxicity'' WHEN 111061 THEN ''Hives'' WHEN 117399 THEN ''Hypertension'' WHEN 879 THEN ''Itching'' WHEN 121677 THEN ''Mental status change'' WHEN 159347 THEN ''Musculoskeletal pain'' WHEN 121 THEN ''Myalgia'' WHEN 512 THEN ''Rash'' WHEN 114403 THEN ''Pain'' WHEN 147241 THEN ''Bleeding'' WHEN 135693 THEN ''Anaesthetic Reaction'' WHEN 167126 THEN ''Excessive skin removed'' WHEN 156911 THEN ''Damage to the penis'' WHEN 152045 THEN ''Problems with appearance'' WHEN 156567 THEN ''Hematoma/Swelling'' WHEN 139510 THEN ''Infection/Swelling'' WHEN 118771 THEN ''Difficulty or pain when urinating'' WHEN 163799 THEN ''Wound disruption (without signs of hematoma or infection)'' WHEN 5622 THEN ''Other'' END) AS adverse_event, ',
    '(CASE severity WHEN 1498 THEN ''Mild'' WHEN 1499 THEN ''Moderate'' WHEN 1500 THEN ''Severe'' WHEN 162819 THEN ''Fatal'' WHEN 1067 THEN ''Unknown'' END) AS severity, ',
    'start_date, ',
    '(CASE action_taken WHEN 1257 THEN ''CONTINUE REGIMEN'' WHEN 1259 THEN ''SWITCHED REGIMEN'' WHEN 981 THEN ''CHANGED DOSE'' WHEN 1258 THEN ''SUBSTITUTED DRUG'' WHEN 1107 THEN ''NONE'' WHEN 1260 THEN ''STOP'' WHEN 5622 THEN ''Other'' END) AS action_taken, ',
    'date_created, date_last_modified, voided ',
  'FROM ', src_adv_quoted, ';'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_adv_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', CONCAT('`', @datatools_schema, '`.`patient_demographics`'), '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_adv_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_adv_quoted) AS message;


-- --------------------------------------
-- TABLE: pre_hiv_enrollment_art
-- Purpose: create tenant-aware datatools view of ETL pre_hiv_enrollment_art
-- Creates table, adds FK to patient_demographics and index on visit_date
-- --------------------------------------

SET @target_pre_hiv_quoted = CONCAT('`', @datatools_schema, '`.`pre_hiv_enrollment_art`');
SET @src_pre_hiv_quoted    = CONCAT('`', @etl_schema, '`.`etl_pre_hiv_enrollment_art`');

SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_pre_hiv_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_pre_hiv_quoted, ' AS ',
  'SELECT ',
    'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, obs_id, ',
    '(CASE PMTCT WHEN 1065 THEN ''Yes'' ELSE '' END) AS PMTCT, ',
    '(CASE PMTCT_regimen ',
      'WHEN 164968 THEN ''AZT/3TC/DTG'' ',
      'WHEN 164969 THEN ''TDF/3TC/DTG'' ',
      'WHEN 164970 THEN ''ABC/3TC/DTG'' ',
      'WHEN 164505 THEN ''TDF-3TC-EFV'' ',
      'WHEN 792    THEN ''D4T/3TC/NVP'' ',
      'WHEN 160124 THEN ''AZT/3TC/EFV'' ',
      'WHEN 160104 THEN ''D4T/3TC/EFV'' ',
      'WHEN 1652   THEN ''3TC/NVP/AZT'' ',
      'WHEN 161361 THEN ''EDF/3TC/EFV'' ',
      'WHEN 104565 THEN ''EFV/FTC/TDF'' ',
      'WHEN 162201 THEN ''3TC/LPV/TDF/r'' ',
      'WHEN 817    THEN ''ABC/3TC/AZT'' ',
      'WHEN 162199 THEN ''ABC/NVP/3TC'' ',
      'WHEN 162200 THEN ''3TC/ABC/LPV/r'' ',
      'WHEN 162565 THEN ''3TC/NVP/TDF'' ',
      'WHEN 162561 THEN ''3TC/AZT/LPV/r'' ',
      'WHEN 164511 THEN ''AZT-3TC-ATV/r'' ',
      'WHEN 164512 THEN ''TDF-3TC-ATV/r'' ',
      'WHEN 162560 THEN ''3TC/D4T/LPV/r'' ',
      'WHEN 162563 THEN ''3TC/ABC/EFV'' ',
      'WHEN 162562 THEN ''ABC/LPV/R/TDF'' ',
      'WHEN 162559 THEN ''ABC/DDI/LPV/r'' ',
    'ELSE '''' END) AS PMTCT_regimen, ',
    '(CASE PEP WHEN 1065 THEN ''Yes'' ELSE '' END) AS PEP, ',
    '(CASE PEP_regimen ',
      'WHEN 164968 THEN ''AZT/3TC/DTG'' ',
      'WHEN 164969 THEN ''TDF/3TC/DTG'' ',
      'WHEN 164970 THEN ''ABC/3TC/DTG'' ',
      'WHEN 164505 THEN ''TDF-3TC-EFV'' ',
      'WHEN 792    THEN ''D4T/3TC/NVP'' ',
      'WHEN 160124 THEN ''AZT/3TC/EFV'' ',
      'WHEN 160104 THEN ''D4T/3TC/EFV'' ',
      'WHEN 1652   THEN ''3TC/NVP/AZT'' ',
      'WHEN 161361 THEN ''EDF/3TC/EFV'' ',
      'WHEN 104565 THEN ''EFV/FTC/TDF'' ',
      'WHEN 162201 THEN ''3TC/LPV/TDF/r'' ',
      'WHEN 817    THEN ''ABC/3TC/AZT'' ',
      'WHEN 162199 THEN ''ABC/NVP/3TC'' ',
      'WHEN 162200 THEN ''3TC/ABC/LPV/r'' ',
      'WHEN 162565 THEN ''3TC/NVP/TDF'' ',
      'WHEN 162561 THEN ''3TC/AZT/LPV/r'' ',
      'WHEN 164511 THEN ''AZT-3TC-ATV/r'' ',
      'WHEN 164512 THEN ''TDF-3TC-ATV/r'' ',
      'WHEN 162560 THEN ''3TC/D4T/LPV/r'' ',
      'WHEN 162563 THEN ''3TC/ABC/EFV'' ',
      'WHEN 162562 THEN ''ABC/LPV/R/TDF'' ',
      'WHEN 162559 THEN ''ABC/DDI/LPV/r'' ',
    'ELSE '''' END) AS PEP_regimen, ',
    '(CASE PrEP WHEN 1065 THEN ''Yes'' ELSE '' END) AS PrEP, ',
    '(CASE PrEP_regimen ',
      'WHEN 164968 THEN ''AZT/3TC/DTG'' ',
      'WHEN 164969 THEN ''TDF/3TC/DTG'' ',
      'WHEN 164970 THEN ''ABC/3TC/DTG'' ',
      'WHEN 164505 THEN ''TDF-3TC-EFV'' ',
      'WHEN 792    THEN ''D4T/3TC/NVP'' ',
      'WHEN 160124 THEN ''AZT/3TC/EFV'' ',
      'WHEN 160104 THEN ''D4T/3TC/EFV'' ',
      'WHEN 1652   THEN ''3TC/NVP/AZT'' ',
      'WHEN 161361 THEN ''EDF/3TC/EFV'' ',
      'WHEN 104565 THEN ''EFV/FTC/TDF'' ',
      'WHEN 162201 THEN ''3TC/LPV/TDF/r'' ',
      'WHEN 817    THEN ''ABC/3TC/AZT'' ',
      'WHEN 162199 THEN ''ABC/NVP/3TC'' ',
      'WHEN 162200 THEN ''3TC/ABC/LPV/r'' ',
      'WHEN 162565 THEN ''3TC/NVP/TDF'' ',
      'WHEN 162561 THEN ''3TC/AZT/LPV/r'' ',
      'WHEN 164511 THEN ''AZT-3TC-ATV/r'' ',
      'WHEN 164512 THEN ''TDF-3TC-ATV/r'' ',
      'WHEN 162560 THEN ''3TC/D4T/LPV/r'' ',
      'WHEN 162563 THEN ''3TC/ABC/EFV'' ',
      'WHEN 162562 THEN ''ABC/LPV/R/TDF'' ',
      'WHEN 162559 THEN ''ABC/DDI/LPV/r'' ',
    'ELSE '''' END) AS PrEP_regimen, ',
    '(CASE HAART WHEN 1185 THEN ''Yes'' ELSE '' END) AS HAART, ',
    '(CASE HAART_regimen ',
      'WHEN 164968 THEN ''AZT/3TC/DTG'' ',
      'WHEN 164969 THEN ''TDF/3TC/DTG'' ',
      'WHEN 164970 THEN ''ABC/3TC/DTG'' ',
      'WHEN 164505 THEN ''TDF-3TC-EFV'' ',
      'WHEN 792    THEN ''D4T/3TC/NVP'' ',
      'WHEN 160124 THEN ''AZT/3TC/EFV'' ',
      'WHEN 160104 THEN ''D4T/3TC/EFV'' ',
      'WHEN 1652   THEN ''3TC/NVP/AZT'' ',
      'WHEN 161361 THEN ''EDF/3TC/EFV'' ',
      'WHEN 104565 THEN ''EFV/FTC/TDF'' ',
      'WHEN 162201 THEN ''3TC/LPV/TDF/r'' ',
      'WHEN 817    THEN ''ABC/3TC/AZT'' ',
      'WHEN 162199 THEN ''ABC/NVP/3TC'' ',
      'WHEN 162200 THEN ''3TC/ABC/LPV/r'' ',
      'WHEN 162565 THEN ''3TC/NVP/TDF'' ',
      'WHEN 162561 THEN ''3TC/AZT/LPV/r'' ',
      'WHEN 164511 THEN ''AZT-3TC-ATV/r'' ',
      'WHEN 164512 THEN ''TDF-3TC-ATV/r'' ',
      'WHEN 162560 THEN ''3TC/D4T/LPV/r'' ',
      'WHEN 162563 THEN ''3TC/ABC/EFV'' ',
      'WHEN 162562 THEN ''ABC/LPV/R/TDF'' ',
      'WHEN 162559 THEN ''ABC/DDI/LPV/r'' ',
    'ELSE '''' END) AS HAART_regimen, ',
    'date_created, date_last_modified, voided ',
  'FROM ', src_pre_hiv_quoted, ';'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_pre_hiv_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', CONCAT('`', @datatools_schema, '`.`patient_demographics`'), '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_pre_hiv_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_pre_hiv_quoted) AS message;


-- sql
-- --------------------------------------
-- Create tenant-aware datatools table: covid_19_assessment
-- Source: `@etl_schema`.`etl_covid19_assessment`
-- Target: `@datatools_schema`.`covid_19_assessment`
-- --------------------------------------

SET @target_covid_quoted = CONCAT('`', @datatools_schema, '`.`covid_19_assessment`');
SET @src_covid_quoted = CONCAT('`', @etl_schema, '`.`etl_covid19_assessment`');
SET @target_pd_quoted = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT(
  'DROP TABLE IF EXISTS ', @target_covid_quoted, ';'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_covid_quoted, ' AS ',
  'SELECT ',
    'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, obs_id, ',
    '(CASE ever_vaccinated WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS ever_vaccinated, ',
    '(CASE first_vaccine_type WHEN 166156 THEN ''Astrazeneca'' WHEN 166355 THEN ''Johnson and Johnson'' ',
      'WHEN 166154 THEN ''Moderna'' WHEN 166155 THEN ''Pfizer'' WHEN 166157 THEN ''Sputnik'' ',
      'WHEN 166379 THEN ''Sinopharm'' WHEN 1067 THEN ''Unknown'' WHEN 5622 THEN ''Other'' END) AS first_vaccine_type, ',
    '(CASE second_vaccine_type WHEN 166156 THEN ''Astrazeneca'' WHEN 166355 THEN ''Johnson and Johnson'' ',
      'WHEN 166154 THEN ''Moderna'' WHEN 166155 THEN ''Pfizer'' WHEN 166157 THEN ''Sputnik'' ',
      'WHEN 166379 THEN ''Sinopharm'' WHEN 1067 THEN ''Unknown'' WHEN 5622 THEN ''Other(Specify)'' END) AS second_vaccine_type, ',
    'first_dose, second_dose, first_dose_date, second_dose_date, ',
    '(CASE first_vaccination_verified WHEN 164134 THEN ''Yes'' END) AS first_vaccination_verified, ',
    '(CASE second_vaccination_verified WHEN 164134 THEN ''Yes'' END) AS second_vaccination_verified, ',
    '(CASE final_vaccination_status WHEN 166192 THEN ''Partially Vaccinated'' WHEN 5585 THEN ''Fully Vaccinated'' END) AS final_vaccination_status, ',
    '(CASE ever_received_booster WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS ever_received_booster, ',
    '(CASE booster_vaccine_taken WHEN 166156 THEN ''Astrazeneca'' WHEN 166355 THEN ''Johnson and Johnson'' ',
      'WHEN 166154 THEN ''Moderna'' WHEN 166155 THEN ''Pfizer'' WHEN 166157 THEN ''Sputnik'' ',
      'WHEN 166379 THEN ''Sinopharm'' WHEN 1067 THEN ''Unknown'' WHEN 5622 THEN ''Other(Specify)'' END) AS booster_vaccine_taken, ',
    'date_taken_booster_vaccine, booster_sequence, ',
    '(CASE booster_dose_verified WHEN 164134 THEN ''Yes'' END) AS booster_dose_verified, ',
    '(CASE ever_tested_covid_19_positive WHEN 703 THEN ''Yes'' WHEN 664 THEN ''No'' WHEN 1067 THEN ''Unknown'' END) AS ever_tested_covid_19_positive, ',
    '(CASE symptomatic WHEN 1068 THEN ''Yes'' WHEN 165912 THEN ''No'' END) AS symptomatic, ',
    'date_tested_positive, (CASE hospital_admission WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS hospital_admission, ',
    'admission_unit, (CASE on_ventillator WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS on_ventillator, ',
    '(CASE on_oxygen_supplement WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS on_oxygen_supplement, ',
    'date_created, date_last_modified, voided ',
  'FROM ', src_covid_quoted, ';'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'ALTER TABLE ', @target_covid_quoted,
  ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_covid_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_covid_quoted) AS message;


-- sql
-- --------------------------------------
-- Create tenant-aware datatools table: covid_19_assessment
-- Source: `@etl_schema`.`etl_covid19_assessment`
-- Target: `@datatools_schema`.`covid_19_assessment`
-- --------------------------------------

SET @target_covid_quoted = CONCAT('`', @datatools_schema, '`.`covid_19_assessment`');
SET @src_covid_quoted = CONCAT('`', @etl_schema, '`.`etl_covid19_assessment`');
SET @target_pd_quoted = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT(
  'DROP TABLE IF EXISTS ', @target_covid_quoted, ';'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_covid_quoted, ' AS ',
  'SELECT ',
    'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, obs_id, ',
    '(CASE ever_vaccinated WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS ever_vaccinated, ',
    '(CASE first_vaccine_type WHEN 166156 THEN ''Astrazeneca'' WHEN 166355 THEN ''Johnson and Johnson'' ',
      'WHEN 166154 THEN ''Moderna'' WHEN 166155 THEN ''Pfizer'' WHEN 166157 THEN ''Sputnik'' ',
      'WHEN 166379 THEN ''Sinopharm'' WHEN 1067 THEN ''Unknown'' WHEN 5622 THEN ''Other'' END) AS first_vaccine_type, ',
    '(CASE second_vaccine_type WHEN 166156 THEN ''Astrazeneca'' WHEN 166355 THEN ''Johnson and Johnson'' ',
      'WHEN 166154 THEN ''Moderna'' WHEN 166155 THEN ''Pfizer'' WHEN 166157 THEN ''Sputnik'' ',
      'WHEN 166379 THEN ''Sinopharm'' WHEN 1067 THEN ''Unknown'' WHEN 5622 THEN ''Other(Specify)'' END) AS second_vaccine_type, ',
    'first_dose, second_dose, first_dose_date, second_dose_date, ',
    '(CASE first_vaccination_verified WHEN 164134 THEN ''Yes'' END) AS first_vaccination_verified, ',
    '(CASE second_vaccination_verified WHEN 164134 THEN ''Yes'' END) AS second_vaccination_verified, ',
    '(CASE final_vaccination_status WHEN 166192 THEN ''Partially Vaccinated'' WHEN 5585 THEN ''Fully Vaccinated'' END) AS final_vaccination_status, ',
    '(CASE ever_received_booster WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS ever_received_booster, ',
    '(CASE booster_vaccine_taken WHEN 166156 THEN ''Astrazeneca'' WHEN 166355 THEN ''Johnson and Johnson'' ',
      'WHEN 166154 THEN ''Moderna'' WHEN 166155 THEN ''Pfizer'' WHEN 166157 THEN ''Sputnik'' ',
      'WHEN 166379 THEN ''Sinopharm'' WHEN 1067 THEN ''Unknown'' WHEN 5622 THEN ''Other(Specify)'' END) AS booster_vaccine_taken, ',
    'date_taken_booster_vaccine, booster_sequence, ',
    '(CASE booster_dose_verified WHEN 164134 THEN ''Yes'' END) AS booster_dose_verified, ',
    '(CASE ever_tested_covid_19_positive WHEN 703 THEN ''Yes'' WHEN 664 THEN ''No'' WHEN 1067 THEN ''Unknown'' END) AS ever_tested_covid_19_positive, ',
    '(CASE symptomatic WHEN 1068 THEN ''Yes'' WHEN 165912 THEN ''No'' END) AS symptomatic, ',
    'date_tested_positive, (CASE hospital_admission WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS hospital_admission, ',
    'admission_unit, (CASE on_ventillator WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS on_ventillator, ',
    '(CASE on_oxygen_supplement WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS on_oxygen_supplement, ',
    'date_created, date_last_modified, voided ',
  'FROM ', src_covid_quoted, ';'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'ALTER TABLE ', @target_covid_quoted,
  ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_covid_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_covid_quoted) AS message;

-- sql
-- --------------------------------------
-- Table: vmmc_enrolment
-- Purpose: create tenant-aware datatools view of ETL vmmc_enrolment
-- Source: `@etl_schema`.`etl_vmmc_enrolment`
-- Target: `@datatools_schema`.`vmmc_enrolment`
-- --------------------------------------

SET @target_vmmc_quoted = CONCAT('`', @datatools_schema, '`.`vmmc_enrolment`');
SET @src_vmmc_quoted    = CONCAT('`', @etl_schema, '`.`etl_vmmc_enrolment`');
SET @target_pd_quoted   = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_vmmc_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_vmmc_quoted, ' AS SELECT ',
    'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, ',
    '(CASE referee ',
      'WHEN 165650 THEN ''Self referral'' ',
      'WHEN 5619   THEN ''Health Care Worker'' ',
      'WHEN 1555   THEN ''Community Health Worker'' ',
      'WHEN 163488 THEN ''Community Health Volunteer'' ',
      'WHEN 1370   THEN ''HTS Counsellors'' ',
      'WHEN 5622   THEN ''Other'' ',
      'ELSE NULL END) AS referee, ',
    'other_referee, ',
    '(CASE source_of_vmmc_info ',
      'WHEN 167095 THEN ''Radio/Tv'' ',
      'WHEN 167096 THEN ''Print Media'' ',
      'WHEN 167098 THEN ''Road Show'' ',
      'WHEN 1555   THEN ''Mobilizer CHW'' ',
      'WHEN 160542 THEN ''OPD/MCH/HT'' ',
      'WHEN 167097 THEN ''Social Media'' ',
      'WHEN 5622   THEN ''Other'' ',
      'ELSE NULL END) AS source_of_vmmc_info, ',
    'other_source_of_vmmc_info, county_of_origin, date_created, date_last_modified, voided ',
  'FROM ', src_vmmc_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_quoted, ' ADD INDEX(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_quoted, ' ADD INDEX(encounter_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_vmmc_quoted) AS message;

-- sql
-- --------------------------------------
-- TABLE: vmmc_circumcision_procedure
-- Purpose: create tenant-aware datatools view of ETL etl_vmmc_circumcision_procedure
-- Source: `@etl_schema`.`etl_vmmc_circumcision_procedure`
-- Target: `@datatools_schema`.`vmmc_circumcision_procedure`
-- --------------------------------------

SET @target_vmmc_circum_quoted = CONCAT('`', @datatools_schema, '`.`vmmc_circumcision_procedure`');
SET @src_vmmc_circum_quoted    = CONCAT('`', @etl_schema, '`.`etl_vmmc_circumcision_procedure`');
SET @target_pd_quoted          = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_vmmc_circum_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_vmmc_circum_quoted, ' AS ',
  'SELECT ',
    'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, ',
    '(CASE circumcision_method WHEN 167119 THEN ''Conventional Surgical'' WHEN 167120 THEN ''Device Circumcision'' END) AS circumcision_method, ',
    '(CASE surgical_circumcision_method WHEN 167121 THEN ''Sleeve resection'' WHEN 167122 THEN ''Dorsal Slit'' WHEN 167123 THEN ''Forceps Guide'' WHEN 5622 THEN ''Other'' END) AS surgical_circumcision_method, ',
    'reason_circumcision_ineligible, ',
    '(CASE circumcision_device WHEN 167124 THEN ''Shangring'' WHEN 5622 THEN ''Other'' END) AS circumcision_device, ',
    'specific_other_device, device_size, lot_number, ',
    '(CASE anaesthesia_used WHEN 161914 THEN ''Local Anaesthesia'' WHEN 162797 THEN ''Topical Anaesthesia'' END) AS anaesthesia_type, ',
    '(CASE anaesthesia_used ',
         'WHEN 103960 THEN ''Lignocaine + Bupivacaine'' ',
         'WHEN 72505 THEN ''Bupivacaine'' ',
         'WHEN 104983 THEN ''Lignocaine + Prilocaine'' ',
         'WHEN 82514 THEN ''Prilocaine'' ',
         'WHEN 78849 THEN ''Lignocaine'' END) AS anaesthesia_used, ',
    'anaesthesia_concentration, anaesthesia_volume, ',
    'time_of_first_placement_cut, time_of_last_device_closure, ',
    '(CASE has_adverse_event WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS has_adverse_event, ',
    'adverse_event, severity, adverse_event_management, clinician_name, ',
    '(CASE clinician_cadre WHEN 162591 THEN ''MO'' WHEN 162592 THEN ''CO'' WHEN 1577 THEN ''Nurse'' END) AS clinician_cadre, ',
    'assist_clinician_name, ',
    '(CASE assist_clinician_cadre WHEN 162591 THEN ''MO'' WHEN 162592 THEN ''CO'' WHEN 1577 THEN ''Nurse'' END) AS assist_clinician_cadre, ',
    'theatre_number, date_created, date_last_modified, voided ',
  'FROM ', src_vmmc_circum_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_circum_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_circum_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_circum_quoted, ' ADD INDEX(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_circum_quoted, ' ADD INDEX(encounter_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_vmmc_circum_quoted) AS message;


-- sql
-- --------------------------------------
-- TABLE: vmmc_medical_history
-- Purpose: create tenant-aware datatools view of ETL etl_vmmc_medical_history
-- Source: `@etl_schema`.`etl_vmmc_medical_history`
-- Target: `@datatools_schema`.`vmmc_medical_history`
-- --------------------------------------

SET @target_vmmc_med_hist_quoted = CONCAT('`', @datatools_schema, '`.`vmmc_medical_history`');
SET @src_vmmc_med_hist_quoted    = CONCAT('`', @etl_schema, '`.`etl_vmmc_medical_history`');
SET @target_pd_quoted            = CONCAT('`', @datatools_schema, '`.`patient_demographics`');

SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_vmmc_med_hist_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_vmmc_med_hist_quoted, ' AS ',
  'SELECT ',
    'uuid, ',
    'provider, ',
    'patient_id, ',
    'visit_id, ',
    'visit_date, ',
    'location_id, ',
    'encounter_id, ',
    '(CASE assent_given WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS assent_given, ',
    '(CASE consent_given WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS consent_given, ',
    '(CASE hiv_status WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1067 THEN ''Unknown'' END) AS hiv_status, ',
    'hiv_test_date, ',
    'art_start_date, ',
    '(CASE current_regimen ',
      'WHEN 164968 THEN ''AZT/3TC/DTG'' ',
      'WHEN 164969 THEN ''TDF/3TC/DTG'' ',
      'WHEN 164970 THEN ''ABC/3TC/DTG'' ',
      'WHEN 164505 THEN ''TDF-3TC-EFV'' ',
      'WHEN 792 THEN ''D4T/3TC/NVP'' ',
      'WHEN 160124 THEN ''AZT/3TC/EFV'' ',
      'WHEN 160104 THEN ''D4T/3TC/EFV'' ',
      'WHEN 1652 THEN ''3TC/NVP/AZT'' ',
      'WHEN 161361 THEN ''EDF/3TC/EFV'' ',
      'WHEN 104565 THEN ''EFV/FTC/TDF'' ',
      'WHEN 162201 THEN ''3TC/LPV/TDF/r'' ',
      'WHEN 817 THEN ''ABC/3TC/AZT'' ',
      'WHEN 162199 THEN ''ABC/NVP/3TC'' ',
      'WHEN 162200 THEN ''3TC/ABC/LPV/r'' ',
      'WHEN 162565 THEN ''3TC/NVP/TDF'' ',
      'WHEN 162561 THEN ''3TC/AZT/LPV/r'' ',
      'WHEN 164511 THEN ''AZT-3TC-ATV/r'' ',
      'WHEN 164512 THEN ''TDF-3TC-ATV/r'' ',
      'WHEN 162560 THEN ''3TC/D4T/LPV/r'' ',
      'WHEN 162563 THEN ''3TC/ABC/EFV'' ',
      'WHEN 162562 THEN ''ABC/LPV/R/TDF'' ',
      'WHEN 162559 THEN ''ABC/DDI/LPV/r'' ',
    'END) AS current_regimen, ',
    'ccc_number, ',
    'next_appointment_date, ',
    '(CASE hiv_care_facility WHEN 163266 THEN ''This health facility'' WHEN 164407 THEN ''Other health facility'' END) AS hiv_care_facility, ',
    'hiv_care_facility_name, ',
    'vl, ',
    'cd4_count, ',
    '(CASE bleeding_disorder WHEN 147241 THEN ''Yes'' END) AS bleeding_disorder, ',
    '(CASE diabetes WHEN 119481 THEN ''Yes'' END) AS diabetes, ',
    'client_presenting_complaints, ',
    'other_complaints, ',
    'ongoing_treatment, ',
    'other_ongoing_treatment, ',
    'hb_level, ',
    'sugar_level, ',
    '(CASE has_known_allergies WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS has_known_allergies, ',
    '(CASE ever_had_surgical_operation WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS ever_had_surgical_operation, ',
    'specific_surgical_operation, ',
    '(CASE proven_tetanus_booster WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS proven_tetanus_booster, ',
    '(CASE ever_received_tetanus_booster WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS ever_received_tetanus_booster, ',
    'date_received_tetanus_booster, ',
    'blood_pressure, ',
    'pulse_rate, ',
    'temperature, ',
    '(CASE in_good_health WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS in_good_health, ',
    '(CASE counselled WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS counselled, ',
    'reason_ineligible, ',
    '(CASE circumcision_method_chosen WHEN 167119 THEN ''Conventional Surgical'' WHEN 167120 THEN ''Device Circumcision'' END) AS circumcision_method_chosen, ',
    '(CASE conventional_method_chosen WHEN 167121 THEN ''Sleeve resection'' WHEN 167122 THEN ''Dorsal Slit'' WHEN 167123 THEN ''Forceps Guide'' WHEN 5622 THEN ''Other'' END) AS conventional_method_chosen, ',
    'device_name, ',
    'device_size, ',
    'other_conventional_method_device_chosen, ',
    'services_referral, ',
    'date_created, ',
    'date_last_modified, ',
    'voided ',
  'FROM ', src_vmmc_med_hist_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;-- sql
-- --------------------------------------
-- TABLE: vmmc_client_followup
-- Purpose: tenant-aware datatools view of ETL etl_vmmc_client_followup
-- --------------------------------------

SET @target_vmmc_client_followup_quoted = CONCAT('`', @datatools_schema, '`.`vmmc_client_followup`');
SET @src_vmmc_client_followup_quoted = CONCAT('`', @etl_schema, '`.`etl_vmmc_client_followup`');
SET @target_pd_quoted = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_vmmc_client_followup_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_vmmc_client_followup_quoted, ' AS ',
  'SELECT ',
    'uuid, ',
    'provider, ',
    'patient_id, ',
    'visit_id, ',
    'visit_date, ',
    'location_id, ',
    'encounter_id, ',
    '(CASE visit_type WHEN 1246 THEN ''Scheduled'' WHEN 160101 THEN ''Unscheduled'' END) AS visit_type, ',
    'days_since_circumcision, ',
    '(CASE has_adverse_event WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS has_adverse_event, ',
    'adverse_event, ',
    'severity, ',
    'adverse_event_management, ',
    'medications_given, ',
    'other_medications_given, ',
    'clinician_name, ',
    '(CASE clinician_cadre WHEN 162591 THEN ''MO'' WHEN 162592 THEN ''CO'' WHEN 1577 THEN ''Nurse'' END) AS clinician_cadre, ',
    'clinician_notes, ',
    'date_created, ',
    'date_last_modified, ',
    'voided ',
  'FROM ', src_vmmc_client_followup_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_client_followup_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_client_followup_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_vmmc_client_followup_quoted) AS message;
SET @target_vmmc_circum_quoted = CONCAT('`', @datatools_schema, '`.`vmmc_circumcision_procedure`');
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_circum_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_circum_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully updated ', @target_vmmc_circum_quoted, ' with FK and index') AS message;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_med_hist_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_med_hist_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_vmmc_med_hist_quoted) AS message;

-- sql
-- --------------------------------------
-- TABLE: vmmc_post_operation_assessment
-- Purpose: tenant-aware datatools view of ETL etl_vmmc_post_operation_assessment
-- Source: `@etl_schema`.`etl_vmmc_post_operation_assessment`
-- Target: `@datatools_schema`.`vmmc_post_operation_assessment`
-- --------------------------------------

SET @target_vmmc_post_quoted = CONCAT('`', @datatools_schema, '`.`vmmc_post_operation_assessment`');
SET @src_vmmc_post_quoted    = CONCAT('`', @etl_schema, '`.`etl_vmmc_post_operation_assessment`');
SET @target_pd_quoted        = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_vmmc_post_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_vmmc_post_quoted, ' AS ',
  'SELECT ',
    'uuid, ',
    'provider, ',
    'patient_id, ',
    'visit_id, ',
    'visit_date, ',
    'location_id, ',
    'encounter_id, ',
    'blood_pressure, ',
    'pulse_rate, ',
    'temperature, ',
    'CASE penis_elevated WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS penis_elevated, ',
    'CASE given_post_procedure_instruction WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS given_post_procedure_instruction, ',
    'post_procedure_instructions, ',
    'CASE given_post_operation_medication WHEN 1107 THEN ''Yes'' ELSE NULL END AS given_post_operation_medication, ',
    'medication_given, ',
    'other_medication_given, ',
    'removal_date, ',
    'next_appointment_date, ',
    'discharged_by, ',
    'CASE cadre WHEN 162591 THEN ''MO'' WHEN 162592 THEN ''CO'' WHEN 1577 THEN ''Nurse'' END AS cadre, ',
    'date_created, ',
    'date_last_modified, ',
    'voided ',
  'FROM ', src_vmmc_post_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_post_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_vmmc_post_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_vmmc_post_quoted) AS message;


-- sql
-- --------------------------------------
-- TABLE: hts_eligibility_screening (tenant-aware)
-- Purpose: create tenant-aware datatools view of ETL etl_hts_eligibility_screening
-- Source: `@etl_schema`.`etl_hts_eligibility_screening`
-- Target: `@datatools_schema`.`hts_eligibility_screening`
-- --------------------------------------

SET @target_hts_quoted   = CONCAT('`', @datatools_schema, '`.`hts_eligibility_screening`');
SET @src_hts_quoted      = CONCAT('`', @etl_schema, '`.`etl_hts_eligibility_screening`');
SET @target_pd_quoted    = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_hts_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_hts_quoted, ' AS ',
  'SELECT ',
    'patient_id, ',
    'visit_id, ',
    'encounter_id, ',
    'uuid, ',
    'location_id, ',
    'provider, ',
    'visit_date, ',
    'CASE population_type WHEN 164928 THEN ''General Population'' WHEN 164929 THEN ''Key Population'' WHEN 138643 THEN ''Priority Population'' END AS population_type, ',
    'key_population_type, ',
    'priority_population_type, ',
    'CASE department WHEN 160542 THEN ''OPD:Out-patient department'' WHEN 5485 THEN ''IPD:In-patient department'' WHEN 160473 THEN ''Emergency'' WHEN 160538 THEN ''PMTCT'' WHEN 159940 THEN ''VCT'' END AS department, ',
    'CASE patient_type WHEN 164163 THEN ''HP:Hospital Patient'' WHEN 164953 THEN ''NP:Non-Hospital Patient'' END AS patient_type, ',
    '(CASE test_strategy WHEN 164163 THEN ''HP: Hospital Patient Testing'' WHEN 164953 THEN ''NP: HTS for non-patients'' WHEN 164954 THEN ''VI:Integrated VCT Center'' WHEN 164955 THEN ''VS:Stand Alone VCT Center'' WHEN 159938 THEN ''HB:Home Based Testing'' WHEN 159939 THEN ''MO: Mobile Outreach HTS'' WHEN 161557 THEN ''Index testing'' WHEN 166606 THEN ''SNS - Social Networks'' WHEN 5622 THEN ''O:Other'' ELSE '''' END) AS test_strategy, ',
    '(CASE hts_entry_point WHEN 5485 THEN ''In Patient Department(IPD)'' WHEN 160542 THEN ''Out Patient Department(OPD)'' WHEN 162181 THEN ''Peadiatric Clinic'' WHEN 160552 THEN ''Nutrition Clinic'' WHEN 160538 THEN ''PMTCT ANC'' WHEN 160456 THEN ''PMTCT MAT'' WHEN 1623 THEN ''PMTCT PNC'' WHEN 160541 THEN ''TB'' WHEN 162050 THEN ''CCC'' WHEN 159940 THEN ''VCT'' WHEN 159938 THEN ''Home Based Testing'' WHEN 159939 THEN ''Mobile Outreach'' WHEN 162223 THEN ''VMMC'' WHEN 160546 THEN ''STI Clinic'' WHEN 160522 THEN ''Emergency'' WHEN 163096 THEN ''Community Testing'' WHEN 5622 THEN ''Other'' ELSE '''' END) AS hts_entry_point, ',
    'hts_risk_category, ',
    'hts_risk_score, ',
    'patient_disabled, ',
    'disability_type, ',
    'recommended_test, ',
    'CASE is_health_worker WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS is_health_worker, ',
    'relationship_with_contact, ',
    'CASE mother_hiv_status WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1067 THEN ''Unknown'' END AS mother_hiv_status, ',
    'CASE tested_hiv_before WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS tested_hiv_before, ',
    'CASE who_performed_test WHEN 5619 THEN ''HTS Provider'' WHEN 164952 THEN ''Self Tested'' END AS who_performed_test, ',
    '(CASE test_results WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1067 THEN ''Unknown'' ELSE '''' END) AS test_results, ',
    'date_tested, ',
    'CASE started_on_art WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS started_on_art, ',
    'upn_number, ',
    'CASE child_defiled WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 162570 THEN ''Declined to answer'' END AS child_defiled, ',
    'CASE ever_had_sex WHEN 1 THEN ''Yes'' WHEN 0 THEN ''No'' END AS ever_had_sex, ',
    'sexually_active, ',
    'new_partner, ',
    'partner_hiv_status, ',
    'CASE couple_discordant WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS couple_discordant, ',
    'multiple_partners, ',
    'number_partners, ',
    'CASE alcohol_sex WHEN 1066 THEN ''Not at all'' WHEN 1385 THEN ''Sometimes'' WHEN 165027 THEN ''Always'' END AS alcohol_sex, ',
    'money_sex, ',
    'condom_burst, ',
    'unknown_status_partner, ',
    'known_status_partner, ',
    'experienced_gbv, ',
    'type_of_gbv, ',
    'service_received, ',
    'currently_on_prep, ',
    'recently_on_pep AS recently_on_pep, ',
    'recently_had_sti, ',
    'tb_screened, ',
    'CASE cough WHEN 159799 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS cough, ',
    'CASE fever WHEN 1494 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS fever, ',
    'CASE weight_loss WHEN 832 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS weight_loss, ',
    'CASE night_sweats WHEN 133027 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS night_sweats, ',
    'CASE contact_with_tb_case WHEN 124068 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS contact_with_tb_case, ',
    'CASE lethargy WHEN 116334 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS lethargy, ',
    'CASE tb_status WHEN 1660 THEN ''No TB signs'' WHEN 142177 THEN ''Presumed TB'' WHEN 1662 THEN ''TB Confirmed'' END AS tb_status, ',
    'shared_needle, ',
    'CASE needle_stick_injuries WHEN 153574 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS needle_stick_injuries, ',
    'CASE traditional_procedures WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS traditional_procedures, ',
    'child_reasons_for_ineligibility, ',
    'pregnant, ',
    'breastfeeding_mother, ',
    'CASE eligible_for_test WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS eligible_for_test, ',
    'CASE referred_for_testing WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS referred_for_testing, ',
    'reason_to_test, ',
    'reason_not_to_test, ',
    'reasons_for_ineligibility, ',
    'specific_reason_for_ineligibility, ',
    'date_created, ',
    'date_last_modified, ',
    'voided ',
  'FROM ', @src_hts_quoted
);

PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_quoted, ' ADD INDEX(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_quoted, ' ADD INDEX(visit_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_quoted, ' ADD INDEX(department)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_quoted, ' ADD INDEX(population_type)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_hts_quoted, ' ADD INDEX(eligible_for_test)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_hts_quoted) AS message;


-- sql
-- --------------------------------------
-- TABLE: drug_order
-- Purpose: create tenant-aware datatools view of ETL etl_drug_order
-- Source: `@etl_schema`.`etl_drug_order`
-- Target: `@datatools_schema`.`drug_order`
-- --------------------------------------

SET @target_drug_order_quoted = CONCAT('`', @datatools_schema, '`.`drug_order`');
SET @src_drug_order_quoted    = CONCAT('`', @etl_schema, '`.`etl_drug_order`');
SET @target_pd_quoted         = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_drug_order_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('CREATE TABLE ', target_drug_order_quoted, ' AS SELECT * FROM ', src_drug_order_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', target_drug_order_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', target_drug_order_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', target_drug_order_quoted, ' ADD INDEX(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', target_drug_order_quoted, ' ADD INDEX(encounter_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', target_drug_order_quoted, ' ADD INDEX(order_group_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('SELECT CONCAT(''Successfully created '', ', target_drug_order_quoted, ') AS message');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;


-- sql
-- --------------------------------------
-- TABLE: preventive_services (tenant-aware)
-- Source: `@etl_schema`.`etl_preventive_services`
-- Target: `@datatools_schema`.`preventive_services`
-- --------------------------------------

SET @target_preventive_quoted = CONCAT('`', @datatools_schema, '`.`preventive_services`');
SET @src_preventive_quoted    = CONCAT('`', @etl_schema, '`.`etl_preventive_services`');
SET @target_pd_quoted        = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_preventive_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_preventive_quoted, ' AS ',
  'SELECT ',
    'patient_id, ',
    'visit_date, ',
    'provider, ',
    'location_id, ',
    'encounter_id, ',
    'malaria_prophylaxis_1, ',
    'malaria_prophylaxis_2, ',
    'malaria_prophylaxis_3, ',
    'tetanus_taxoid_1, ',
    'tetanus_taxoid_2, ',
    'tetanus_taxoid_3, ',
    'tetanus_taxoid_4, ',
    'folate_iron_1, ',
    'folate_iron_2, ',
    'folate_iron_3, ',
    'folate_iron_4, ',
    'folate_1, ',
    'folate_2, ',
    'folate_3, ',
    'folate_4, ',
    'iron_1, ',
    'iron_2, ',
    'iron_3, ',
    'iron_4, ',
    'mebendazole, ',
    'long_lasting_insecticidal_net, ',
    'comment, ',
    'date_last_modified, ',
    'date_created, ',
    'voided ',
  'FROM ', src_preventive_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_preventive_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_preventive_quoted, ' ADD INDEX(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_preventive_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_preventive_quoted) AS message;

-- sql
-- --------------------------------------
-- TABLE: overdose_reporting
-- Purpose: tenant-aware datatools view of ETL etl_overdose_reporting
-- Source: `@etl_schema`.`etl_overdose_reporting`
-- Target: `@datatools_schema`.`overdose_reporting`
-- --------------------------------------

SET @target_overdose_quoted = CONCAT('`', @datatools_schema, '`.`overdose_reporting`');
SET @src_overdose_quoted = CONCAT('`', @etl_schema, '`.`etl_overdose_reporting`');
SET @target_pd_quoted = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_overdose_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_overdose_quoted, ' AS ',
  'SELECT ',
    'client_id, ',
    'visit_id, ',
    'encounter_id, ',
    'uuid, ',
    'provider, ',
    'location_id, ',
    'visit_date, ',
    'overdose_location, ',
    'overdose_date, ',
    '(CASE incident_type WHEN 165134 THEN ''New'' WHEN 165135 THEN ''Recurrent'' END) AS incident_type, ',
    'incident_site_name, ',
    '(CASE incident_site_type ',
      'WHEN 165011 THEN ''Street'' ',
      'WHEN 165012 THEN ''Injecting den'' ',
      'WHEN 165013 THEN ''Uninhabitable building'' ',
      'WHEN 165014 THEN ''Public Park'' ',
      'WHEN 165015 THEN ''Beach'' ',
      'WHEN 165016 THEN ''Casino'' ',
      'WHEN 165017 THEN ''Bar with lodging'' ',
      'WHEN 165018 THEN ''Bar without lodging'' ',
      'WHEN 165019 THEN ''Sex den'' ',
      'WHEN 165020 THEN ''Strip club'' ',
      'WHEN 165021 THEN ''Highway'' ',
      'WHEN 165022 THEN ''Brothel'' ',
      'WHEN 165023 THEN ''Guest house/hotel'' ',
      'WHEN 165025 THEN ''illicit brew den'' ',
      'WHEN 165026 THEN ''Barber shop/salon'' ',
    'END) AS incident_site_type, ',
    '(CASE naloxone_provided WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END) AS naloxone_provided, ',
    'risk_factors, ',
    'other_risk_factors, ',
    'drug, ',
    'other_drug, ',
    '(CASE outcome WHEN 1898 THEN ''Recovered'' WHEN 160034 THEN ''Died'' WHEN 1272 THEN ''Referred'' END) AS outcome, ',
    'remarks, ',
    'reported_by, ',
    'date_reported, ',
    'witness, ',
    'date_witnessed, ',
    'encounter, ',
    'date_created, ',
    'date_last_modified, ',
    'voided ',
  'FROM ', src_overdose_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_overdose_quoted, ' ADD FOREIGN KEY (client_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_overdose_quoted, ' ADD INDEX(client_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_overdose_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_overdose_quoted, ' ADD INDEX(naloxone_provided)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_overdose_quoted, ' ADD INDEX(outcome)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_overdose_quoted) AS message;


-- sql
-- --------------------------------------
-- TABLE: art_fast_track (tenant-aware)
-- Purpose: create tenant-aware datatools view of ETL etl_art_fast_track
-- Source: `@etl_schema`.`etl_art_fast_track`
-- Target: `@datatools_schema`.`art_fast_track`
-- --------------------------------------

SET @target_art_quoted = CONCAT('`', @datatools_schema, '`.`art_fast_track`');
SET @src_art_quoted    = CONCAT('`', @etl_schema, '`.`etl_art_fast_track`');
SET @target_pd_quoted  = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_art_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_art_quoted, ' AS ',
  'SELECT patient_id, ',
    'visit_date, ',
    'provider, ',
    'location_id, ',
    'encounter_id, ',
    'CASE art_refill_model ',
      'WHEN 1744 THEN ''Fast Track'' ',
      'WHEN 1555 THEN ''Community ART Distribution - HCW Led'' ',
      'WHEN 5618 THEN ''Community ART Distribution - Peer Led'' ',
      'WHEN 1537 THEN ''Facility ART Distribution Group'' ',
      'ELSE NULL END AS art_refill_model, ',
    'CASE ctx_dispensed WHEN 162229 THEN ''Yes'' ELSE NULL END AS ctx_dispensed, ',
    'CASE dapsone_dispensed WHEN 74250 THEN ''Yes'' ELSE NULL END AS dapsone_dispensed, ',
    'CASE oral_contraceptives_dispensed WHEN 780 THEN ''Yes'' ELSE NULL END AS oral_contraceptives_dispensed, ',
    'CASE condoms_distributed WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END AS condoms_distributed, ',
    'doses_missed, ',
    'CASE fatigue WHEN 162626 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END AS fatigue, ',
    'CASE cough WHEN 143264 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END AS cough, ',
    'CASE fever WHEN 140238 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END AS fever, ',
    'CASE rash WHEN 512 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END AS rash, ',
    'CASE nausea_vomiting WHEN 5978 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END AS nausea_vomiting, ',
    'CASE genital_sore_discharge WHEN 135462 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END AS genital_sore_discharge, ',
    'CASE diarrhea WHEN 142412 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END AS diarrhea, ',
    'CASE other_symptoms WHEN 5622 THEN ''Yes'' ELSE NULL END AS other_symptoms, ',
    'other_specific_symptoms, ',
    'CASE pregnant WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Not sure'' ELSE NULL END AS pregnant, ',
    'CASE family_planning_status WHEN 965 THEN ''On Family Planning'' WHEN 160652 THEN ''Not using Family Planning'' WHEN 1360 THEN ''Wants Family Planning'' ELSE NULL END AS family_planning_status, ',
    'family_planning_method, ',
    'reason_not_on_family_planning, ',
    'date_created, ',
    'date_last_modified, ',
    'voided ',
  'FROM ', @src_art_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_art_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_art_quoted, ' ADD INDEX(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_art_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_art_quoted) AS message;


-- --------------------------------------
-- TABLE: clinical_encounter (tenant-aware)
-- Purpose: create tenant-aware datatools view of ETL etl_clinical_encounter
-- Source: `@etl_schema`.`etl_clinical_encounter`
-- Target: `@datatools_schema`.`clinical_encounter`
-- --------------------------------------

SET @target_clinical_quoted = CONCAT('`', @datatools_schema, '`.`clinical_encounter`');
SET @src_clinical_quoted    = CONCAT('`', @etl_schema, '`.`etl_clinical_encounter`');
SET @target_pd_quoted       = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_clinical_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_clinical_quoted, ' AS ',
  'SELECT patient_id, ',
    'visit_id, encounter_id, location_id, provider, visit_date, visit_type, ',
    'therapy_ordered, other_therapy_ordered, counselling_ordered, other_counselling_ordered, ',
    'procedures_prescribed, procedures_ordered, ',
    'CASE patient_outcome WHEN 160429 THEN ''Released Home'' WHEN 1654 THEN ''Admit'' WHEN 1693 THEN ''Referral'' WHEN 159 THEN ''Deceased'' ELSE NULL END AS patient_outcome, ',
    'general_examination, admission_needed, date_of_patient_admission, admission_reason, admission_type, ',
    'priority_of_admission, admission_ward, hospital_stay, referral_needed, referral_ordered, referral_to, ',
    'other_facility, this_facility, voided ',
  'FROM ', src_clinical_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_clinical_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_clinical_quoted, ' ADD INDEX(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_clinical_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_clinical_quoted) AS message;


-- --------------------------------------
-- File: `src/main/resources/sql/hiv/DataTools.sql`
-- TABLE: kvp_clinical_enrollment
-- Purpose: create tenant-aware datatools view of ETL etl_kvp_clinical_enrollment
-- --------------------------------------

SET @target_kvp_quoted = CONCAT('`', @datatools_schema, '`.`kvp_clinical_enrollment`');
SET @src_kvp_quoted    = CONCAT('`', @etl_schema, '`.`etl_kvp_clinical_enrollment`');
SET @target_pd_quoted  = CONCAT('`', @datatools_schema, '`.`patient_demographics`');

SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_kvp_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_kvp_quoted, ' AS ',
  'SELECT ',
    'patient_id, ',
    'visit_id, ',
    'encounter_id, ',
    'uuid, ',
    'location_id, ',
    'provider, ',
    'visit_date, ',
    'CASE contacted_by_pe_for_health_services WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS contacted_by_pe_for_health_services, ',
    'CASE has_regular_non_paying_sexual_partner WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS has_regular_non_paying_sexual_partner, ',
    'number_of_sexual_partners, ',
    'year_started_fsw, ',
    'year_started_msm, ',
    'year_started_using_drugs, ',
    'trucker_duration_on_transit, ',
    'duration_working_as_trucker, ',
    'duration_working_as_fisherfolk, ',
    'year_tested_discordant_couple, ',
    'CASE ever_experienced_violence WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS ever_experienced_violence, ',
    'CASE type_of_violence_experienced WHEN 158358 THEN ''Physical'' WHEN 123160 THEN ''Sexual'' WHEN 117510 THEN ''Emotional'' END AS type_of_violence_experienced, ',
    'CASE ever_tested_for_hiv WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS ever_tested_for_hiv, ',
    'CASE latest_hiv_test_method WHEN 164952 THEN ''HIV Self Test'' WHEN 163722 THEN ''Rapid HIV Testing'' END AS latest_hiv_test_method, ',
    'CASE latest_hiv_test_results WHEN 703 THEN ''Yes I tested positive'' WHEN 664 THEN ''Yes I tested negative'' WHEN 1066 THEN ''No I do not want to share'' END AS latest_hiv_test_results, ',
    'CASE willing_to_test_for_hiv WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS willing_to_test_for_hiv, ',
    'reason_not_willing_to_test_for_hiv, ',
    'CASE receiving_hiv_care WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS receiving_hiv_care, ',
    'CASE hiv_care_facility WHEN 162723 THEN ''Elsewhere'' WHEN 163266 THEN ''Here'' END AS hiv_care_facility, ',
    'other_hiv_care_facility, ',
    'ccc_number, ',
    'CASE consent_followup WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS consent_followup, ',
    'date_created, ',
    'date_last_modified, ',
    'voided ',
  'FROM ', src_kvp_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kvp_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kvp_quoted, ' ADD INDEX (patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_kvp_quoted, ' ADD INDEX (visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_kvp_quoted) AS message;

-- sql
-- --------------------------------------
-- File: `src/main/resources/sql/hiv/DataTools.sql`
-- TABLE: high_iit_intervention
-- Purpose: create tenant-aware datatools view of ETL etl_high_iit_intervention
-- Source: `@etl_schema`.`etl_high_iit_intervention`
-- Target: `@datatools_schema`.`high_iit_intervention`
-- --------------------------------------

SET @target_high_quoted = CONCAT('`', @datatools_schema, '`.`high_iit_intervention`');
SET @src_high_quoted    = CONCAT('`', @etl_schema, '`.`etl_high_iit_intervention`');
SET @target_pd_quoted   = CONCAT('`', @datatools_schema, '`.`patient_demographics`');

SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_high_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_high_quoted, ' AS ',
  'SELECT ',
    'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, ',
    'interventions_offered, appointment_mgt_interventions, reminder_methods, ',
    'CASE enrolled_in_ushauri WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS enrolled_in_ushauri, ',
    'appointment_mngt_intervention_date, date_assigned_case_manager, ',
    'CASE eacs_recommended WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS eacs_recommended, ',
    'CASE enrolled_in_psychosocial_support_group WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS enrolled_in_psychosocial_support_group, ',
    'robust_literacy_interventions_date, ',
    'CASE expanding_differentiated_service_delivery_interventions WHEN 166443 THEN ''Offer options for community delivery of drugs including courier if eligible for MMD'' END AS expanding_differentiated_service_delivery_interventions, ',
    'CASE enrolled_in_nishauri WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS enrolled_in_nishauri, ',
    'expanded_differentiated_service_delivery_interventions_date, date_created, date_last_modified ',
  'FROM ', src_high_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT('ALTER TABLE ', @target_high_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT('ALTER TABLE ', @target_high_quoted, ' ADD INDEX (patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT('ALTER TABLE ', @target_high_quoted, ' ADD INDEX (visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT CONCAT('Successfully created ', @target_high_quoted) AS message;

-- sql
-- File: `src/main/resources/sql/hiv/DataTools.sql`
-- TABLE: home_visit_checklist (tenant-aware)
-- Source: `@etl_schema`.`etl_home_visit_checklist`
-- Target: `@datatools_schema`.`home_visit_checklist`

SET @target_home_quoted = CONCAT('`', @datatools_schema, '`.`home_visit_checklist`');
SET @src_home_quoted    = CONCAT('`', @etl_schema, '`.`etl_home_visit_checklist`');
SET @target_pd_quoted   = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_home_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_home_quoted, ' AS ',
  'SELECT ',
    'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, ',
    'independence_in_daily_activities, other_independence_activities, meeting_basic_needs, other_basic_needs, ',
    'CASE disclosure_to_sexual_partner WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS disclosure_to_sexual_partner, ',
    'CASE disclosure_to_household_members WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS disclosure_to_household_members, ',
    'disclosure_to, mode_of_storing_arv_drugs, arv_drugs_taking_regime, ',
    'CASE receives_household_social_support WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS receives_household_social_support, ',
    'household_social_support_given, ',
    'CASE receives_community_social_support WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS receives_community_social_support, ',
    'community_social_support_given, linked_to_other_services, ',
    'CASE has_mental_health_issues WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS has_mental_health_issues, ',
    'CASE suffering_stressful_situation WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS suffering_stressful_situation, ',
    'CASE uses_drugs_alcohol WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS uses_drugs_alcohol, ',
    'CASE has_side_medications_effects WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS has_side_medications_effects, ',
    'medication_side_effects, assessment_notes, date_created, date_last_modified ',
  'FROM ', src_home_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_home_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_home_quoted, ' ADD INDEX(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_home_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_home_quoted) AS message;

-- sql
-- File: `src/main/resources/sql/hiv/DataTools.sql`
-- --------------------------------------
-- TABLE: ncd_enrollment (tenant-aware)
-- Purpose: create tenant-aware datatools view of ETL `etl_ncd_enrollment`
-- Source: `@etl_schema`.`etl_ncd_enrollment`
-- Target: `@datatools_schema`.`ncd_enrollment`
-- Tenant-aware: builds quoted identifiers from `@datatools_schema` and `@etl_schema`
-- --------------------------------------

SET @target_ncd_quoted   = CONCAT('`', @datatools_schema, '`.`ncd_enrollment`');
SET @src_ncd_quoted      = CONCAT('`', @etl_schema, '`.`etl_ncd_enrollment`');
SET @target_pd_quoted    = CONCAT('`', @datatools_schema, '`.`patient_demographics`');

SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_ncd_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_ncd_quoted, ' AS ',
  'SELECT ',
    'patient_id, uuid, provider, visit_id, visit_date, encounter_id, location_id, visit_type, ',
    'referred_from, referred_from_department, referred_from_department_other, ',
    'CASE patient_complaint WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS patient_complaint, ',
    'specific_complaint, ',
    'CASE disease_type WHEN 142486 THEN ''Diabetes'' WHEN 117399 THEN ''Hypertension'' WHEN 166020 THEN ''Co-morbid'' END AS disease_type, ',
    'CASE diabetes_condition WHEN 1000488 THEN ''New DM patient'' WHEN 1000489 THEN ''Known DM patient'' END AS diabetes_condition, ',
    'CASE diabetes_type WHEN 142474 THEN ''Type 1 Diabetes Mellitus'' WHEN 2004524 THEN ''Type 2 Diabetes Mellitus'' WHEN 117807 THEN ''Gestational Diabetes Mellitus'' WHEN 126985 THEN ''Diabetes secondary to other causes'' END AS diabetes_type, ',
    'CASE hypertension_condition WHEN 1000490 THEN ''New HTN patient'' WHEN 1000491 THEN ''Known HTN patient'' END AS hypertension_condition, ',
    'hypertension_stage, hypertension_type, ',
    'CASE comorbid_condition WHEN 1000492 THEN ''New co-morbid patient'' WHEN 1000493 THEN ''Known Co-morbid patient'' END AS comorbid_condition, ',
    'diagnosis_date, ',
    'CASE hiv_status WHEN 664 THEN ''HIV Negative'' WHEN 703 THEN ''HIV Positive'' WHEN 1067 THEN ''Unknown'' END AS hiv_status, ',
    'CASE hiv_positive_on_art WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS hiv_positive_on_art, ',
    'CASE tb_screening WHEN 1660 THEN ''No TB Signs'' WHEN 142177 THEN ''Presumed TB'' WHEN 1662 THEN ''TB Confirmed'' WHEN 160737 THEN ''TB Screening Not Done'' END AS tb_screening, ',
    'CASE smoke_check WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 158939 THEN ''Stopped'' END AS smoke_check, ',
    'date_stopped_smoke, ',
    'CASE drink_alcohol WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 159452 THEN ''Stopped'' END AS drink_alcohol, ',
    'date_stopped_alcohol, ',
    'CASE cessation_counseling WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS cessation_counseling, ',
    'CASE physical_activity WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS physical_activity, ',
    'CASE diet_routine WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS diet_routine, ',
    'existing_complications, other_existing_complications, new_complications, other_new_complications, examination_findings, ',
    'CASE cardiovascular WHEN 1115 THEN ''Normal'' WHEN 1116 THEN ''Abnormal'' END AS cardiovascular, ',
    'CASE respiratory WHEN 1115 THEN ''Normal'' WHEN 1116 THEN ''Abnormal'' END AS respiratory, ',
    'CASE abdominal_pelvic WHEN 1115 THEN ''Normal'' WHEN 1116 THEN ''Abnormal'' END AS abdominal_pelvic, ',
    'CASE neurological WHEN 1115 THEN ''Normal'' WHEN 1116 THEN ''Abnormal'' END AS neurological, ',
    'CASE oral_exam WHEN 1115 THEN ''Normal'' WHEN 1116 THEN ''Abnormal'' END AS oral_exam, ',
    'CASE foot_risk WHEN 166674 THEN ''High Risk'' WHEN 166675 THEN ''Low Risk'' END AS foot_risk, ',
    'foot_low_risk, foot_high_risk, ',
    'CASE diabetic_foot WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS diabetic_foot, ',
    'describe_diabetic_foot_type, treatment_given, other_treatment_given, lifestyle_advice, nutrition_assessment, ',
    'CASE footcare_outcome WHEN 162130 THEN ''Ulcer healed'' WHEN 2001766 THEN ''Surgical debridement'' WHEN 164009 THEN ''Amputation'' WHEN 5240 THEN ''Loss to follow up'' WHEN 1654 THEN ''Admitted'' WHEN 1648 THEN ''Referred'' END AS footcare_outcome, ',
    'referred_to, reasons_for_referral, clinical_notes, date_created, date_last_modified, voided ',
  'FROM ', src_ncd_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ncd_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ncd_quoted, ' ADD INDEX (patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ncd_quoted, ' ADD INDEX (visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_ncd_quoted, ' table') AS message;



-- sql
-- --------------------------------------
-- TABLE: ncd_followup (tenant-aware)
-- Purpose: create tenant-aware datatools view of ETL `etl_ncd_followup`
-- Source: `@etl_schema`.`etl_ncd_followup`
-- Target: `@datatools_schema`.`ncd_followup`
-- --------------------------------------

SET @target_ncd_followup_quoted = CONCAT('`', @datatools_schema, '`.`ncd_followup`');
SET @src_ncd_followup_quoted    = CONCAT('`', @etl_schema, '`.`etl_ncd_followup`');
SET @target_pd_quoted           = CONCAT('`', @datatools_schema, '`.`patient_demographics`');
SET @sql_stmt = CONCAT('DROP TABLE IF EXISTS ', @target_ncd_followup_quoted);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'CREATE TABLE ', @target_ncd_followup_quoted, ' AS ',
  'SELECT ',
    'patient_id, ',
    'uuid, ',
    'provider, ',
    'visit_id, ',
    'visit_date, ',
    'encounter_id, ',
    'location_id, ',
    'visit_type, ',
    'CASE tobacco_use WHEN 159450 THEN ''Yes'' WHEN 159452 THEN ''Stopped'' WHEN 1090 THEN ''No'' END AS tobacco_use, ',
    'CASE drink_alcohol WHEN 159450 THEN ''Yes'' WHEN 159452 THEN ''Stopped'' WHEN 1090 THEN ''No'' END AS drink_alcohol, ',
    'CASE physical_activity WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS physical_activity, ',
    'CASE healthy_diet WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS healthy_diet, ',
    'CASE patient_complaint WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS patient_complaint, ',
    'specific_complaint, ',
    'other_specific_complaint, ',
    'examination_findings, ',
    'CASE cardiovascular WHEN 1115 THEN ''Normal'' WHEN 1116 THEN ''Abnormal'' END AS cardiovascular, ',
    'CASE abdominal_pelvic WHEN 1115 THEN ''Normal'' WHEN 1116 THEN ''Abnormal'' END AS abdominal_pelvic, ',
    'CASE neurological WHEN 1115 THEN ''Normal'' WHEN 1116 THEN ''Abnormal'' END AS neurological, ',
    'CASE oral_exam WHEN 1115 THEN ''Normal'' WHEN 1116 THEN ''Abnormal'' END AS oral_exam, ',
    'foot_exam, ',
    'CASE diabetic_foot WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END AS diabetic_foot, ',
    'foot_risk_assessment, ',
    'CASE diabetic_foot_risk WHEN 166675 THEN ''Low Risk'' WHEN 166674 THEN ''High Risk'' END AS diabetic_foot_risk, ',
    'CASE adhering_medication WHEN 159405 THEN ''Yes'' WHEN 159407 THEN ''No'' WHEN 1175 THEN ''N/A'' END AS adhering_medication, ',
    'referred_to, ',
    'CASE reasons_for_referral WHEN 159405 THEN ''Further management of HTN'' WHEN 159407 THEN ''Nutrition'' WHEN 1175 THEN ''Physiotherapy'' ',
      'WHEN 1666 THEN ''Surgical review'' WHEN 1112 THEN ''CVD review'' WHEN 222 THEN ''Renal review'' WHEN 6621 THEN ''Further management of DM'' END AS reasons_for_referral, ',
    'clinical_notes, ',
    'date_created, ',
    'date_last_modified, ',
    'voided ',
  'FROM ', src_ncd_followup_quoted
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT(
  'ALTER TABLE ', @target_ncd_followup_quoted,
  ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)'
);
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ncd_followup_quoted, ' ADD INDEX(patient_id)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql_stmt = CONCAT('ALTER TABLE ', @target_ncd_followup_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', @target_ncd_followup_quoted) AS message;
SET @sql_stmt = CONCAT('UPDATE `', @etl_schema, '`.etl_script_status SET stop_time=NOW() WHERE id= script_id');
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;


END $$
DELIMITER ;