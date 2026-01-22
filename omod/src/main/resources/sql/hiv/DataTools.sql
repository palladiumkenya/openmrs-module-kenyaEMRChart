-- sql
DELIMITER $$

SET @OLD_SQL_MODE=@@SQL_MODE $$
SET SQL_MODE='' $$

DROP PROCEDURE IF EXISTS sp_set_tenant_session_vars $$
CREATE PROCEDURE sp_set_tenant_session_vars()
BEGIN
    DECLARE current_schema VARCHAR(200);
    DECLARE tenant_suffix VARCHAR(100);
    DECLARE etl_schema VARCHAR(200);

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
    SET @etl_schema_quoted = CONCAT('`', etl_schema, '`');
    SET @script_status_table = CONCAT('`', etl_schema, '`.`etl_script_status`');
END $$
DROP PROCEDURE IF EXISTS create_datatools_tables $$
CREATE PROCEDURE create_datatools_tables()
BEGIN
    -- all DECLAREs must be at procedure start
    DECLARE script_id INT DEFAULT NULL;
    DECLARE sql_stmt TEXT;
    DECLARE datatools_schema VARCHAR(200);
    DECLARE src_pd_quoted, src_hiv_quoted, target_pd_quoted, target_hiv_quoted VARCHAR(300);

CALL sp_set_tenant_session_vars();

-- derive tenant datatools schema from current database
SET datatools_schema = IF(INSTR(DATABASE(), 'openmrs_') = 0,
                              'kenyaemr_datatools',
                              CONCAT('kenyaemr_datatools_', SUBSTRING_INDEX(DATABASE(), 'openmrs_', -1))
                             );

    -- record start in tenant etl_script_status
    SET sql_stmt = CONCAT('INSERT INTO ', @script_status_table, ' (script_name, start_time) VALUES (''KenyaEMR_Data_Tool'', NOW())');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT LAST_INSERT_ID() INTO script_id;

-- ensure datatools database exists
SET sql_stmt = CONCAT('CREATE DATABASE IF NOT EXISTS `', datatools_schema, '` DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

/* ----------------------------
   Demography group (patient_demographics)
   keep SETs and DDL for this table here
   ---------------------------- */
SET target_pd_quoted = CONCAT('`', datatools_schema, '`.`patient_demographics`');
    SET src_pd_quoted = CONCAT('`', @etl_schema, '`.`etl_patient_demographics`');

    SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_pd_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT(
      'CREATE TABLE ', target_pd_quoted, ' ENGINE=InnoDB AS ',
      'SELECT patient_id, uuid, given_name, middle_name, family_name, Gender, DOB, national_id_no, huduma_no, passport_no, ',
      'birth_certificate_no, unique_patient_no AS unique_patient_no, alien_no, driving_license_no, national_unique_patient_identifier, ',
      'hts_recency_id, nhif_number, patient_clinic_number, Tb_no, CPIMS_unique_identifier, openmrs_id, district_reg_no, hei_no, cwc_number, ',
      'phone_number, birth_place, citizenship, email_address, occupation, next_of_kin, next_of_kin_relationship, marital_status, education_level, ',
      'IF(dead=1, ''Yes'', ''NO'') AS dead, death_date, voided ',
      'FROM ', src_pd_quoted
    );
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT('ALTER TABLE ', target_pd_quoted, ' ADD PRIMARY KEY(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT('ALTER TABLE ', target_pd_quoted, ' ADD INDEX(`Gender`)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

/* ----------------------------
   HIV enrollment group (hiv_enrollment)
   keep SETs and DDL for this table here
   ---------------------------- */
SET target_hiv_quoted = CONCAT('`', datatools_schema, '`.`hiv_enrollment`');
    SET src_hiv_quoted = CONCAT('`', @etl_schema, '`.`etl_hiv_enrollment`');

    SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_hiv_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT(
      'CREATE TABLE ', target_hiv_quoted, ' ENGINE=InnoDB AS ',
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
      'FROM ', src_hiv_quoted
    );
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(arv_status)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(date_confirmed_hiv_positive)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(entry_point)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('UPDATE ', @script_status_table, ' SET stop_time = NOW() WHERE id = ', script_id);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT CONCAT('Successfully created ', target_pd_quoted, ' and ', target_hiv_quoted) AS message;


-- ----------------------------------- create table hiv_followup ----------------------------------------------
-- sql
SET target_hiv_quoted = CONCAT('`', datatools_schema, '`.`hiv_followup`');
SET src_hiv_quoted = CONCAT('`', @etl_schema, '`.`etl_patient_hiv_followup`');
SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_hiv_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_hiv_quoted, ' ENGINE=InnoDB AS ',
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
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(pregnancy_status)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(breastfeeding)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(family_planning_status)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(tb_status)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(ctx_dispensed)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(population_type)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(on_anti_tb_drugs)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(stability)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hiv_quoted, ' ADD INDEX(differentiated_care)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', target_hiv_quoted) AS message;


-- -------------------------------- create table laboratory_extract ------------------------------------------

SET target_lab_quoted = CONCAT('`', datatools_schema, '`.`laboratory_extract`');
SET src_lab_quoted = CONCAT('`', @etl_schema, '`.`etl_laboratory_extract`');
SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_lab_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_lab_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'uuid, encounter_id, patient_id, location_id, visit_date, visit_id, order_id, lab_test, urgency, order_reason, ',
    'order_test_name, obs_id, result_test_name, result_name, set_member_conceptId, test_result, ',
    'date_test_requested, date_test_result_received, test_requested_by, date_created, date_last_modified, created_by ',
  'FROM ', src_lab_quoted
);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_lab_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_lab_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_lab_quoted, ' ADD INDEX(lab_test)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_lab_quoted, ' ADD INDEX(test_result)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', target_lab_quoted) AS message;


-- create table pharmacy_extract

SET target_pharm_quoted = CONCAT('`', datatools_schema, '`.`pharmacy_extract`');
SET src_pharm_quoted = CONCAT('`', @etl_schema, '`.`etl_pharmacy_extract`');
SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_pharm_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_pharm_quoted, ' ENGINE=InnoDB AS ',
  'SELECT ',
    'patient_id, uuid, visit_date, visit_id, encounter_id, date_created, encounter_name, drug, drug_name, ',
    '(CASE is_arv WHEN 1 THEN ''Yes'' ELSE ''No'' END) AS is_arv, ',
    '(CASE is_ctx WHEN 105281 THEN ''SULFAMETHOXAZOLE / TRIMETHOPRIM (CTX)'' ELSE '''' END) AS is_ctx, ',
    '(CASE is_dapsone WHEN 74250 THEN ''DAPSONE'' ELSE '''' END) AS is_dapsone, ',
    'frequency, duration, duration_units, voided, date_voided, dispensing_provider ',
  'FROM ', src_pharm_quoted
);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_pharm_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_pharm_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_pharm_quoted, ' ADD INDEX(drug)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_pharm_quoted, ' ADD INDEX(is_arv)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', target_pharm_quoted) AS message;

-- create table patient_program_discontinuation

SET target_ppd_quoted = CONCAT('`', datatools_schema, '`.`patient_program_discontinuation`');
SET src_ppd_quoted = CONCAT('`', @etl_schema, '`.`etl_patient_program_discontinuation`');
SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_ppd_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_ppd_quoted, ' ENGINE=InnoDB AS ',
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
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_ppd_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_ppd_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_ppd_quoted, ' ADD INDEX(discontinuation_reason)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', target_ppd_quoted) AS message;


-- create table mch_enrollment

SET target_mch_quoted = CONCAT('`', datatools_schema, '`.`mch_enrollment`');
SET src_mch_quoted = CONCAT('`', @etl_schema, '`.`etl_mch_enrollment`');
SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_mch_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_mch_quoted, ' ENGINE=InnoDB AS ',
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
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_mch_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_mch_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', target_mch_quoted) AS message;


-- create table mch_enrollment
SET target_mch_ant_quoted = CONCAT('`', datatools_schema, '`.`mch_antenatal_visit`');
SET src_mch_ant_quoted = CONCAT('`', @etl_schema, '`.`etl_mch_antenatal_visit`');
SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_mch_ant_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_mch_ant_quoted, ' ENGINE=InnoDB AS ',
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
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_mch_ant_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_mch_ant_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', target_mch_ant_quoted) AS message;

-- create table mch_delivery table
SET target_mch_delivery_quoted = CONCAT('`', datatools_schema, '`.`mch_delivery`');
SET src_mch_delivery_quoted = CONCAT('`', @etl_schema, '`.`etl_mchs_delivery`');
SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_mch_delivery_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_mch_delivery_quoted, ' ENGINE=InnoDB AS ',
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
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_mch_delivery_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_mch_delivery_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', target_mch_delivery_quoted) AS message;

-- create table mch_delivery table

SET target_mch_delivery_quoted = CONCAT('`', datatools_schema, '`.`mch_delivery`');
SET src_mch_delivery_quoted = CONCAT('`', @etl_schema, '`.`etl_mchs_delivery`');

SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_mch_delivery_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_mch_delivery_quoted, ' ENGINE=InnoDB AS ',
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
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_mch_delivery_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_mch_delivery_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('SELECT ''Successfully created '' , ', target_mch_delivery_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- create table mch_postnatal_visit-----

SET target_mch_post_quoted = CONCAT('`', datatools_schema, '`.`mch_postnatal_visit`');
SET src_mch_post_quoted = CONCAT('`', @etl_schema, '`.`etl_mch_postnatal_visit`');
SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_mch_post_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_mch_post_quoted, ' ENGINE=InnoDB AS ',
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
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_mch_post_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_mch_post_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('SELECT ''Successfully created '' , ', target_mch_post_quoted);

  -- ------------ create table etl_hei_enrollment-----------------------

SET target_hei_quoted = CONCAT('`', datatools_schema, '`.`hei_enrollment`');
SET src_hei_quoted = CONCAT('`', @etl_schema, '`.`etl_hei_enrollment`');
SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_hei_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_hei_quoted, ' ENGINE=InnoDB AS ',
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
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hei_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hei_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', target_hei_quoted) AS message;

-- create table hei_follow_up_visit

SET target_hei_follow_quoted = CONCAT('`', datatools_schema, '`.`hei_follow_up_visit`');
SET src_hei_follow_quoted = CONCAT('`', @etl_schema, '`.`etl_hei_follow_up_visit`');
SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_hei_follow_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_hei_follow_quoted, ' ENGINE=InnoDB AS ',
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
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT('ALTER TABLE ', target_hei_follow_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hei_follow_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_hei_follow_quoted, ' ADD INDEX(infant_feeding)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT CONCAT('Successfully created ', target_hei_follow_quoted) AS message;

-- create table immunization

SET target_immunization_quoted = CONCAT('`', datatools_schema, '`.`immunization`');
SET src_immunization_quoted = CONCAT('`', @etl_schema, '`.`etl_immunization`');
SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_immunization_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_immunization_quoted, ' ENGINE=InnoDB AS ',
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
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_immunization_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_immunization_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_immunization_quoted, ' ADD INDEX(sequence)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Successfully created ', target_immunization_quoted) AS message;

-- create table tb_enrollment

-- sql
SET target_tb_quoted = CONCAT('`', datatools_schema, '`.`tb_enrollment`');
SET src_tb_quoted = CONCAT('`', @etl_schema, '`.`etl_tb_enrollment`');
SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_tb_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_tb_quoted, ' ENGINE=InnoDB AS ',
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
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_tb_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_tb_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('SELECT ''Successfully created '' , ', target_tb_quoted);

-- create table tb_follow_up_visit

-- sql
SET target_tb_follow_follow_quoted = NULL; -- ensure variable not reused
SET target_tb_follow_quoted = CONCAT('`', datatools_schema, '`.`tb_follow_up_visit`');
SET src_tb_follow_quoted = CONCAT('`', @etl_schema, '`.`etl_tb_follow_up_visit`');
SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_tb_follow_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_tb_follow_quoted, ' ENGINE=InnoDB AS ',
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
  'FROM ', src_tb_follow_quoted
);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_tb_follow_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_tb_follow_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('ALTER TABLE ', target_tb_follow_quoted, ' ADD INDEX(hiv_status)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT('SELECT ''Successfully created '' , ', target_tb_follow_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- create table tb_screening

-- sql
SET target_tb_screen_quoted = CONCAT('`', datatools_schema, '`.`tb_screening`');
SET src_tb_screen_quoted = CONCAT('`', @etl_schema, '`.`etl_tb_screening`');

SET sql_stmt = CONCAT('DROP TABLE IF EXISTS ', target_tb_screen_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT(
  'CREATE TABLE ', target_tb_screen_quoted, ' ENGINE=InnoDB AS ',
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
  'FROM ', src_tb_screen_quoted
);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT('ALTER TABLE ', target_tb_screen_quoted, ' ADD FOREIGN KEY (patient_id) REFERENCES ', target_pd_quoted, '(patient_id)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT('ALTER TABLE ', target_tb_screen_quoted, ' ADD INDEX(visit_date)');
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT('SELECT ''Successfully created '' , ', target_tb_screen_quoted);
PREPARE stmt FROM sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;




























END $$
SET SQL_MODE=@OLD_SQL_MODE $$
DELIMITER ;
