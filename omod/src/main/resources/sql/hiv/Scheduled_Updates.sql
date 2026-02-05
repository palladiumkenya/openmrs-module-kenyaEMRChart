SET @OLD_SQL_MODE = @SQL_MODE $$
SET SQL_MODE = '' $$
DROP PROCEDURE IF EXISTS sp_set_tenant_session_vars $$
CREATE PROCEDURE sp_set_tenant_session_vars()
BEGIN
    DECLARE current_db VARCHAR(200);
    DECLARE tenant_suffix VARCHAR(100);

    SET current_db = DATABASE();
    IF current_db IS NULL OR current_db = '' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No database selected.';
END IF;

    SET tenant_suffix = SUBSTRING_INDEX(current_db, 'openmrs_', -1);
    SET @etl_schema_raw = CONCAT('kenyaemr_etl_', tenant_suffix);
    SET @etl_schema = CONCAT('', @etl_schema_raw, '');
    SET @script_status_table_quoted = CONCAT(@etl_schema, '.etl_script_status');

    SET @create_db = CONCAT('CREATE DATABASE IF NOT EXISTS ', @etl_schema, ' DEFAULT CHARACTER SET utf8;');
PREPARE stmt FROM @create_db; EXECUTE stmt; DEALLOCATE PREPARE stmt;
END $$

- ---------------------------------------------------------
-- 2. Worker: Update Patient Demographics (Incremental)
-- ---------------------------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_patient_demographics $$
CREATE PROCEDURE sp_update_etl_patient_demographics(IN last_update_time DATETIME)
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table_quoted VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET target_table_quoted = CONCAT(@etl_schema, '.etl_patient_demographics');

    SET @last_ts = IFNULL(CONCAT('''', last_update_time, ''''), '''1900-01-01 00:00:00''');
    SET sql_stmt = CONCAT(
      'INSERT INTO ', @target_table_quoted, ' (patient_id, uuid, given_name, middle_name, family_name, Gender, DOB, dead, date_created, date_last_modified, voided, death_date) ',
      'SELECT p.person_id, p.uuid, p.given_name, p.middle_name, p.family_name, p.gender, p.birthdate, p.dead, p.date_created, ',
      'IF((p.date_last_modified = ''0000-00-00 00:00:00'' OR p.date_last_modified = p.date_created), NULL, p.date_last_modified), p.voided, p.death_date ',
      'FROM (',
        'SELECT p.person_id, p.uuid, pn.given_name, pn.middle_name, pn.family_name, p.gender, p.birthdate, p.dead, p.date_created, ',
        'GREATEST(IFNULL(p.date_changed, ''0000-00-00 00:00:00''), IFNULL(pn.date_changed, ''0000-00-00 00:00:00'')) AS date_last_modified, p.voided, p.death_date ',
        'FROM person p ',
        'INNER JOIN person_name pn ON pn.person_id = p.person_id AND pn.voided = 0 ',
        'WHERE (pn.date_created >= ', @last_ts, ' OR pn.date_changed >= ', @last_ts, ' OR p.date_created >= ', @last_ts, ' OR p.date_changed >= ', @last_ts, ') ',
        'GROUP BY p.person_id',
      ') p ',
      'ON DUPLICATE KEY UPDATE given_name = p.given_name, middle_name = p.middle_name, family_name = p.family_name, DOB = p.birthdate, dead = p.dead, voided = p.voided, death_date = p.death_date;'
    );
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET sql_stmt = CONCAT(
      'UPDATE ', @target_table_quoted, ' d ',
      'INNER JOIN (',
         'SELECT pa.person_id, ',
           'MAX(IF(pat.uuid = ''8d8718c2-c2cc-11de-8d13-0010c6dffd0f'', pa.value, NULL)) AS birthplace, ',
           'MAX(IF(pat.uuid = ''b2c38640-2603-4629-aebd-3b54f33f1e3a'', pa.value, NULL)) AS phone_number, ',
           'MAX(IF(pat.uuid = ''830bef6d-b01f-449d-9f8d-ac0fede8dbd3'', pa.value, NULL)) AS next_of_kin_name, ',
           'GREATEST(IFNULL(pa.date_changed, ''0000-00-00''), pa.date_created) AS latest_date ',
         'FROM person_attribute pa ',
         'INNER JOIN person_attribute_type pat ON pat.person_attribute_type_id = pa.person_attribute_type_id ',
         'WHERE (pa.date_created >= ', @last_ts, ' OR pa.date_changed >= ', @last_ts, ') ',
         'AND pa.voided = 0 AND pat.retired = 0 ',
         'GROUP BY pa.person_id',
      ') att ON att.person_id = d.patient_id ',
      'SET d.phone_number = att.phone_number, d.next_of_kin = att.next_of_kin_name, d.birth_place = att.birthplace, ',
          'd.date_last_modified = IF(att.latest_date > IFNULL(d.date_last_modified, ''0000-00-00''), att.latest_date, d.date_last_modified);'
    );
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
END $$

DROP PROCEDURE IF EXISTS sp_update_etl_hiv_enrollment $$
CREATE PROCEDURE sp_update_etl_hiv_enrollment(IN last_update_time DATETIME)
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table_quoted VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table_quoted = CONCAT(@etl_schema, '.etl_hiv_enrollment');

    SET sql_stmt = CONCAT(
      'INSERT INTO ', @target_table_quoted, ' (',
      'patient_id, uuid, visit_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, patient_type, date_first_enrolled_in_care, entry_point, ',
      'transfer_in_date, facility_transferred_from, district_transferred_from, previous_regimen, date_started_art_at_transferring_facility, date_confirmed_hiv_positive, facility_confirmed_hiv_positive, ',
      'arv_status, ever_on_pmtct, ever_on_pep, ever_on_prep, ever_on_haart, cd4_test_result, cd4_test_date, viral_load_test_result, viral_load_test_date, who_stage, name_of_treatment_supporter, ',
      'relationship_of_treatment_supporter, treatment_supporter_telephone, treatment_supporter_address, in_school, orphan, date_of_discontinuation, discontinuation_reason, voided',
      ') ',
      'SELECT ',
      'e.patient_id, e.uuid, e.visit_id, e.encounter_datetime AS visit_date, e.location_id, e.encounter_id, e.creator, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id IN (164932), o.value_coded, IF(o.concept_id = 160563 AND o.value_coded = 1065, 160563, NULL))) AS patient_type, ',
      'MAX(IF(o.concept_id = 160555, o.value_datetime, NULL)) AS date_first_enrolled_in_care, ',
      'MAX(IF(o.concept_id = 160540, o.value_coded, NULL)) AS entry_point, ',
      'MAX(IF(o.concept_id = 160534, o.value_datetime, NULL)) AS transfer_in_date, ',
      'MAX(IF(o.concept_id = 160535, LEFT(TRIM(o.value_text),100), NULL)) AS facility_transferred_from, ',
      'MAX(IF(o.concept_id = 161551, LEFT(TRIM(o.value_text),100), NULL)) AS district_transferred_from, ',
      'MAX(IF(o.concept_id = 164855, o.value_coded, NULL)) AS previous_regimen, ',
      'MAX(IF(o.concept_id = 159599, o.value_datetime, NULL)) AS date_started_art_at_transferring_facility, ',
      'MAX(IF(o.concept_id = 160554, o.value_datetime, NULL)) AS date_confirmed_hiv_positive, ',
      'MAX(IF(o.concept_id = 160632, LEFT(TRIM(o.value_text),100), NULL)) AS facility_confirmed_hiv_positive, ',
      'MAX(IF(o.concept_id = 160533, o.value_coded, NULL)) AS arv_status, ',
      'MAX(IF(o.concept_id = 1148, o.value_coded, NULL)) AS ever_on_pmtct, ',
      'MAX(IF(o.concept_id = 1691, o.value_coded, NULL)) AS ever_on_pep, ',
      'MAX(IF(o.concept_id = 165269, o.value_coded, NULL)) AS ever_on_prep, ',
      'MAX(IF(o.concept_id = 1181, o.value_coded, NULL)) AS ever_on_haart, ',
      'MAX(IF(o.concept_id = 5497, o.value_numeric, NULL)) AS cd4_test_result, ',
      'MAX(IF(o.concept_id = 159376, o.value_datetime, NULL)) AS cd4_test_date, ',
      'MAX(IF(o.concept_id = 1305 AND o.value_coded = 1302, ''LDL'', IF(o.concept_id = 162086, o.value_text, NULL))) AS viral_load_test_result, ',
      'MAX(IF(o.concept_id = 163281, DATE(o.value_datetime), NULL)) AS viral_load_test_date, ',
      'MAX(IF(o.concept_id = 5356, o.value_coded, NULL)) AS who_stage, ',
      'MAX(IF(o.concept_id = 160638, LEFT(TRIM(o.value_text),100), NULL)) AS name_of_treatment_supporter, ',
      'MAX(IF(o.concept_id = 160640, o.value_coded, NULL)) AS relationship_of_treatment_supporter, ',
      'MAX(IF(o.concept_id = 160642, LEFT(TRIM(o.value_text),100), NULL)) AS treatment_supporter_telephone, ',
      'MAX(IF(o.concept_id = 160641, LEFT(TRIM(o.value_text),100), NULL)) AS treatment_supporter_address, ',
      'MAX(IF(o.concept_id = 5629, o.value_coded, NULL)) AS in_school, ',
      'MAX(IF(o.concept_id = 1174, o.value_coded, NULL)) AS orphan, ',
      'MAX(IF(o.concept_id = 164384, o.value_datetime, NULL)) AS date_of_discontinuation, ',
      'MAX(IF(o.concept_id = 161555, o.value_coded, NULL)) AS discontinuation_reason, ',
      'e.voided ',
      'FROM encounter e ',
      'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid = ''de78a6be-bfc5-4634-adc3-5f1a280455cc'') et ON et.encounter_type_id = e.encounter_type ',
      'JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (160555,160540,160534,160535,161551,159599,160554,160632,160533,160638,160640,160642,160641,164932,160563,5629,1174,1088,161555,164855,164384,1148,1691,165269,1181,5356,5497,159376,1305,162086,163281) ',
      'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
            ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
            ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
            ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
            ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
      'GROUP BY e.patient_id, e.encounter_id ',
      'ON DUPLICATE KEY UPDATE visit_date = VALUES(visit_date), encounter_provider = VALUES(encounter_provider), patient_type = VALUES(patient_type), date_first_enrolled_in_care = VALUES(date_first_enrolled_in_care), entry_point = VALUES(entry_point), transfer_in_date = VALUES(transfer_in_date), ',
      'facility_transferred_from = VALUES(facility_transferred_from), district_transferred_from = VALUES(district_transferred_from), previous_regimen = VALUES(previous_regimen), date_started_art_at_transferring_facility = VALUES(date_started_art_at_transferring_facility), ',
      'date_confirmed_hiv_positive = VALUES(date_confirmed_hiv_positive), facility_confirmed_hiv_positive = VALUES(facility_confirmed_hiv_positive), arv_status = VALUES(arv_status), ever_on_pmtct = VALUES(ever_on_pmtct), ever_on_pep = VALUES(ever_on_pep), ever_on_prep = VALUES(ever_on_prep), ever_on_haart = VALUES(ever_on_haart), ',
      'who_stage = VALUES(who_stage), name_of_treatment_supporter = VALUES(name_of_treatment_supporter), relationship_of_treatment_supporter = VALUES(relationship_of_treatment_supporter), treatment_supporter_telephone = VALUES(treatment_supporter_telephone), treatment_supporter_address = VALUES(treatment_supporter_address), ',
      'in_school = VALUES(in_school), orphan = VALUES(orphan), voided = VALUES(voided), date_of_discontinuation = VALUES(date_of_discontinuation), discontinuation_reason = VALUES(discontinuation_reason), cd4_test_result = VALUES(cd4_test_result), cd4_test_date = VALUES(cd4_test_date), viral_load_test_result = VALUES(viral_load_test_result), viral_load_test_date = VALUES(viral_load_test_date);'
    );
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
END $$

-- ------------- update etl_hiv_followup--------------------------------
-- sql
DROP PROCEDURE IF EXISTS sp_update_etl_hiv_followup $$
CREATE PROCEDURE sp_update_etl_hiv_followup(IN last_update_time DATETIME)
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table_quoted VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table_quoted = CONCAT(@etl_schema, '.etl_patient_hiv_followup');

SET sql_stmt = CONCAT(
'INSERT INTO ', @target_table_quoted, ' (',
'uuid, patient_id, visit_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, ',
'visit_scheduled, person_present, weight, systolic_pressure, diastolic_pressure, height, temperature, pulse_rate, respiratory_rate, ',
'oxygen_saturation, muac, z_score_absolute, z_score, nutritional_status, population_type, key_population_type, who_stage, ',
'who_stage_associated_oi, presenting_complaints, clinical_notes, on_anti_tb_drugs, on_ipt, ever_on_ipt, cough, fever, ',
'weight_loss_poor_gain, night_sweats, tb_case_contact, lethargy, screened_for_tb, spatum_smear_ordered, chest_xray_ordered, genexpert_ordered, ',
'spatum_smear_result, chest_xray_result, genexpert_result, referral, clinical_tb_diagnosis, contact_invitation, evaluated_for_ipt, ',
'has_known_allergies, has_chronic_illnesses_cormobidities, has_adverse_drug_reaction, pregnancy_status, breastfeeding, wants_pregnancy, ',
'pregnancy_outcome, anc_number, expected_delivery_date, ever_had_menses, last_menstrual_period, menopausal, gravida, parity, ',
'full_term_pregnancies, abortion_miscarriages, family_planning_status, family_planning_method, reason_not_using_family_planning, ',
'tb_status, started_anti_TB, tb_rx_date, tb_treatment_no, general_examination, system_examination, skin_findings, eyes_findings, ',
'ent_findings, chest_findings, cvs_findings, abdomen_findings, cns_findings, genitourinary_findings, prophylaxis_given, ctx_adherence, ',
'ctx_dispensed, dapsone_adherence, dapsone_dispensed, inh_dispensed, arv_adherence, poor_arv_adherence_reason, poor_arv_adherence_reason_other, ',
'pwp_disclosure, pwp_pead_disclosure, pwp_partner_tested, condom_provided, substance_abuse_screening, screened_for_sti, cacx_screening, ',
'sti_partner_notification, experienced_gbv, depression_screening, at_risk_population, system_review_finding, next_appointment_date, refill_date, ',
'appointment_consent, next_appointment_reason, stability, differentiated_care_group, differentiated_care, established_differentiated_care, ',
'insurance_type, other_insurance_specify, insurance_status, voided',
') ',
'SELECT ',
'e.uuid, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id AS encounter_id, e.creator AS encounter_provider, ',
'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
'MAX(IF(o.concept_id = 1246, o.value_coded, NULL)) AS visit_scheduled, ',
'MAX(IF(o.concept_id = 161643, o.value_coded, NULL)) AS person_present, ',
'MAX(IF(o.concept_id = 5089, o.value_numeric, NULL)) AS weight, ',
'MAX(IF(o.concept_id = 5085, o.value_numeric, NULL)) AS systolic_pressure, ',
'MAX(IF(o.concept_id = 5086, o.value_numeric, NULL)) AS diastolic_pressure, ',
'MAX(IF(o.concept_id = 5090, o.value_numeric, NULL)) AS height, ',
'MAX(IF(o.concept_id = 5088, o.value_numeric, NULL)) AS temperature, ',
'MAX(IF(o.concept_id = 5087, o.value_numeric, NULL)) AS pulse_rate, ',
'MAX(IF(o.concept_id = 5242, o.value_numeric, NULL)) AS respiratory_rate, ',
'MAX(IF(o.concept_id = 5092, o.value_numeric, NULL)) AS oxygen_saturation, ',
'MAX(IF(o.concept_id = 1343, o.value_numeric, NULL)) AS muac, ',
'MAX(IF(o.concept_id = 162584, o.value_numeric, NULL)) AS z_score_absolute, ',
'MAX(IF(o.concept_id = 163515, o.value_coded, NULL)) AS z_score, ',
'MAX(IF(o.concept_id = 163300, o.value_coded, NULL)) AS nutritional_status, ',
'MAX(IF(o.concept_id = 164930, o.value_coded, NULL)) AS population_type, ',
'MAX(IF(o.concept_id = 160581, o.value_coded, NULL)) AS key_population_type, ',
'MAX(IF(o.concept_id = 5356, o.value_coded, NULL)) AS who_stage, ',
'CONCAT_WS('','', ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5006, ''Asymptomatic'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 130364, ''Persistent generalized lymphadenopathy)'' , '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 159214, ''Unexplained severe weight loss'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5330, ''Minor mucocutaneous manifestations'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 117543, ''Herpes zoster'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5012, ''Recurrent upper respiratory tract infections'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5018, ''Unexplained chronic diarrhoea'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5027, ''Unexplained persistent fever'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5337, ''Oral hairy leukoplakia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 42, ''Pulmonary tuberculosis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5333, ''Severe bacterial infections such as empyema or pyomyositis or meningitis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 133440, ''Acute necrotizing ulcerative stomatitis or gingivitis or periodontitis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 148849, ''Unexplained anaemia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 823, ''HIV wasting syndrome'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 137375, ''Pneumocystis jirovecipneumonia PCP'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 1215, ''Recurrent severe bacterial pneumonia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 1294, ''Cryptococcal meningitis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 990, ''Toxoplasmosis of the brain'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 143929, ''Chronic orolabial, genital or ano-rectal herpes simplex'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 110915, ''Kaposi sarcoma KS'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 160442, ''HIV encephalopathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5042, ''Extra pulmonary tuberculosis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 143110, ''Cryptosporidiosis with diarrhoea'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 136458, ''Isosporiasis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5033, ''Cryptococcosis extra pulmonary'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 160745, ''Disseminated non-tuberculous mycobacterial infection'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 154119, ''Cytomegalovirus CMV retinitis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5046, ''Progressive multifocal leucoencephalopathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 131357, ''Any disseminated mycosis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 146513, ''Candidiasis of the oesophagus or airways'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 160851, ''Non-typhoid salmonella NTS septicaemia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 155941, ''Lymphoma cerebral or B cell Non-Hodgkins Lymphoma'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 116023, ''Invasive cervical cancer'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 123084, ''Visceral leishmaniasis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 153701, ''Symptomatic HIV-associated nephropathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 825, ''Unexplained asymptomatic hepatosplenomegaly'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 1249, ''Papular pruritic eruptions'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 113116, ''Seborrheic dermatitis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 132387, ''Fungal nail infections'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 148762, ''Angular cheilitis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 159344, ''Linear gingival erythema'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 1212, ''Extensive HPV or molluscum infection'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 159912, ''Recurrent oral ulcerations'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 1210, ''Parotid enlargement'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 127784, ''Recurrent or chronic upper respiratory infection'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 134722, ''Unexplained moderate malnutrition'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 163282, ''Unexplained persistent fever'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 5334, ''Oral candidiasis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 160515, ''Severe recurrent bacterial pneumonia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 135458, ''Lymphoid interstitial pneumonitis (LIP)'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 163712, ''HIV-related cardiomyopathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 162331, ''Unexplained severe wasting or severe malnutrition'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 130021, ''Pneumocystis pneumonia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 146518, ''Candida of trachea, bronchi or lungs'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 167394 AND o.value_coded = 143744, ''Acquired recto-vesicular fistula'', '''')), '''') ',
') AS who_stage_associated_oi, ',
'MAX(IF(o.concept_id = 1154, o.value_coded, NULL)) AS presenting_complaints, ',
'NULL AS clinical_notes, ',
'MAX(IF(o.concept_id = 164948, o.value_coded, NULL)) AS on_anti_tb_drugs, ',
'MAX(IF(o.concept_id = 164949, o.value_coded, NULL)) AS on_ipt, ',
'MAX(IF(o.concept_id = 164950, o.value_coded, NULL)) AS ever_on_ipt, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded = 159799, o.value_coded, NULL)) AS cough, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded = 1494, o.value_coded, NULL)) AS fever, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded = 832, o.value_coded, NULL)) AS weight_loss_poor_gain, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded = 133027, o.value_coded, NULL)) AS night_sweats, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded = 124068, o.value_coded, NULL)) AS tb_case_contact, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded = 116334, o.value_coded, NULL)) AS lethargy, ',
'MAX(IF(o.concept_id = 1729 AND o.value_coded IN (159799,1494,832,133027,124068,116334,1066), ''Yes'', ''No'')) AS screened_for_tb, ',
'MAX(IF(o.concept_id = 1271 AND o.value_coded = 307, o.value_coded, NULL)) AS spatum_smear_ordered, ',
'MAX(IF(o.concept_id = 1271 AND o.value_coded = 12, o.value_coded, NULL)) AS chest_xray_ordered, ',
'MAX(IF(o.concept_id = 1271 AND o.value_coded = 162202, o.value_coded, NULL)) AS genexpert_ordered, ',
'MAX(IF(o.concept_id = 307, o.value_coded, NULL)) AS spatum_smear_result, ',
'MAX(IF(o.concept_id = 12, o.value_coded, NULL)) AS chest_xray_result, ',
'MAX(IF(o.concept_id = 162202, o.value_coded, NULL)) AS genexpert_result, ',
'MAX(IF(o.concept_id = 1272, o.value_coded, NULL)) AS referral, ',
'MAX(IF(o.concept_id = 163752, o.value_coded, NULL)) AS clinical_tb_diagnosis, ',
'MAX(IF(o.concept_id = 163414, o.value_coded, NULL)) AS contact_invitation, ',
'MAX(IF(o.concept_id = 162275, o.value_coded, NULL)) AS evaluated_for_ipt, ',
'MAX(IF(o.concept_id = 160557, o.value_coded, NULL)) AS has_known_allergies, ',
'MAX(IF(o.concept_id = 162747, o.value_coded, NULL)) AS has_chronic_illnesses_cormobidities, ',
'MAX(IF(o.concept_id = 121764, o.value_coded, NULL)) AS has_adverse_drug_reaction, ',
'MAX(IF(o.concept_id = 5272, o.value_coded, NULL)) AS pregnancy_status, ',
'MAX(IF(o.concept_id = 5632, o.value_coded, NULL)) AS breastfeeding, ',
'MAX(IF(o.concept_id = 164933, o.value_coded, NULL)) AS wants_pregnancy, ',
'MAX(IF(o.concept_id = 161033, o.value_coded, NULL)) AS pregnancy_outcome, ',
'MAX(IF(o.concept_id = 163530, o.value_text, NULL)) AS anc_number, ',
'MAX(IF(o.concept_id = 5596, DATE(o.value_datetime), NULL)) AS expected_delivery_date, ',
'MAX(IF(o.concept_id = 162877, o.value_coded, NULL)) AS ever_had_menses, ',
'MAX(IF(o.concept_id = 1427, DATE(o.value_datetime), NULL)) AS last_menstrual_period, ',
'MAX(IF(o.concept_id = 160596, o.value_coded, NULL)) AS menopausal, ',
'MAX(IF(o.concept_id = 5624, o.value_numeric, NULL)) AS gravida, ',
'MAX(IF(o.concept_id = 1053, o.value_numeric, NULL)) AS parity, ',
'MAX(IF(o.concept_id = 160080, o.value_numeric, NULL)) AS full_term_pregnancies, ',
'MAX(IF(o.concept_id = 1823, o.value_numeric, NULL)) AS abortion_miscarriages, ',
'MAX(IF(o.concept_id = 160653, o.value_coded, NULL)) AS family_planning_status, ',
'MAX(IF(o.concept_id = 374, o.value_coded, NULL)) AS family_planning_method, ',
'MAX(IF(o.concept_id = 160575, o.value_coded, NULL)) AS reason_not_using_family_planning, ',
'MAX(IF(o.concept_id = 1659, o.value_coded, NULL)) AS tb_status, ',
'MAX(IF(o.concept_id = 162309, o.value_coded, NULL)) AS started_anti_TB, ',
'MAX(IF(o.concept_id = 1113, o.value_datetime, NULL)) AS tb_rx_date, ',
'MAX(IF(o.concept_id = 161654, TRIM(o.value_text), NULL)) AS tb_treatment_no, ',
'CONCAT_WS('','', ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 1107, ''None'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 136443, ''Jaundice'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 460, ''Oedema'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 5334, ''Oral Thrush'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 5245, ''Pallor'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 140125, ''Finger Clubbing'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 126952, ''Lymph Node Axillary'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 143050, ''Cyanosis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 126939, ''Lymph Nodes Inguinal'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 823, ''Wasting'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 142630, ''Dehydration'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id = 162737 AND o.value_coded = 116334, ''Lethargic'', '''')), '''') ',
') AS general_examination, ',
'MAX(IF(o.concept_id = 159615, o.value_coded, NULL)) AS system_examination, ',
'MAX(IF(o.concept_id = 1120, o.value_coded, NULL)) AS skin_findings, ',
'MAX(IF(o.concept_id = 163309, o.value_coded, NULL)) AS eyes_findings, ',
'MAX(IF(o.concept_id = 164936, o.value_coded, NULL)) AS ent_findings, ',
'MAX(IF(o.concept_id = 1123, o.value_coded, NULL)) AS chest_findings, ',
'MAX(IF(o.concept_id = 1124, o.value_coded, NULL)) AS cvs_findings, ',
'MAX(IF(o.concept_id = 1125, o.value_coded, NULL)) AS abdomen_findings, ',
'MAX(IF(o.concept_id = 164937, o.value_coded, NULL)) AS cns_findings, ',
'MAX(IF(o.concept_id = 1126, o.value_coded, NULL)) AS genitourinary_findings, ',
'MAX(IF(o.concept_id = 1109, o.value_coded, NULL)) AS prophylaxis_given, ',
'MAX(IF(o.concept_id = 161652, o.value_coded, NULL)) AS ctx_adherence, ',
'MAX(IF(o.concept_id = 162229 OR (o.concept_id = 1282 AND o.value_coded = 105281), o.value_coded, NULL)) AS ctx_dispensed, ',
'MAX(IF(o.concept_id = 164941, o.value_coded, NULL)) AS dapsone_adherence, ',
'MAX(IF(o.concept_id = 164940 OR (o.concept_id = 1282 AND o.value_coded = 74250), o.value_coded, NULL)) AS dapsone_dispensed, ',
'MAX(IF(o.concept_id = 162230, o.value_coded, NULL)) AS inh_dispensed, ',
'MAX(IF(o.concept_id = 1658, o.value_coded, NULL)) AS arv_adherence, ',
'MAX(IF(o.concept_id = 160582, o.value_coded, NULL)) AS poor_arv_adherence_reason, ',
'MAX(IF(o.concept_id = 160632, TRIM(o.value_text), NULL)) AS poor_arv_adherence_reason_other, ',
'MAX(IF(o.concept_id = 159423, o.value_coded, NULL)) AS pwp_disclosure, ',
'MAX(IF(o.concept_id = 5616, o.value_coded, NULL)) AS pwp_pead_disclosure, ',
'MAX(IF(o.concept_id = 161557, o.value_coded, NULL)) AS pwp_partner_tested, ',
'MAX(IF(o.concept_id = 159777, o.value_coded, NULL)) AS condom_provided, ',
'MAX(IF(o.concept_id = 112603, o.value_coded, NULL)) AS substance_abuse_screening, ',
'MAX(IF(o.concept_id = 161558, o.value_coded, NULL)) AS screened_for_sti, ',
'MAX(IF(o.concept_id = 164934, o.value_coded, NULL)) AS cacx_screening, ',
'MAX(IF(o.concept_id = 164935, o.value_coded, NULL)) AS sti_partner_notification, ',
'MAX(IF(o.concept_id = 167161, o.value_coded, NULL)) AS experienced_gbv, ',
'MAX(IF(o.concept_id = 165086, o.value_coded, NULL)) AS depression_screening, ',
'MAX(IF(o.concept_id = 160581, o.value_coded, NULL)) AS at_risk_population, ',
'MAX(IF(o.concept_id = 159615, o.value_coded, NULL)) AS system_review_finding, ',
'NULL AS next_appointment_date, NULL AS refill_date, ',
'MAX(IF(o.concept_id = 166607, o.value_coded, NULL)) AS appointment_consent, ',
'MAX(IF(o.concept_id = 160288, o.value_coded, NULL)) AS next_appointment_reason, ',
'MAX(IF(o.concept_id = 1855, o.value_coded, NULL)) AS stability, ',
'MAX(IF(o.concept_id = 164947, o.value_coded, NULL)) AS differentiated_care_group, ',
'MAX(IF(o.concept_id IN (164946,165287), o.value_coded, NULL)) AS differentiated_care, ',
'MAX(IF(o.concept_id = 164946 OR o.concept_id = 165287, o.value_coded, NULL)) AS established_differentiated_care, ',
'MAX(IF(o.concept_id = 159356, o.value_coded, NULL)) AS insurance_type, ',
'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS other_insurance_specify, ',
'MAX(IF(o.concept_id = 165911, o.value_coded, NULL)) AS insurance_status, ',
'e.voided AS voided ',
'FROM encounter e ',
'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''a0034eee-1940-4e35-847f-97537a35d05e'',''465a92f2-baf8-42e9-9612-53064be868e8'')) et ON et.encounter_type_id = e.encounter_type ',
'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
'AND o.concept_id IN (1282,1246,161643,5089,5085,5086,5090,5088,5087,5242,5092,1343,162584,163515,5356,167394,5272,5632,161033,163530,5596,1427,5624,1053,160653,374,160575,1659,161654,161652,162229,162230,1658,160582,160632,159423,5616,161557,159777,112603,161558,160581,5096,163300,164930,160581,1154,160430,164948,164949,164950,1271,307,12,162202,1272,163752,163414,162275,160557,162747,121764,164933,160080,1823,164940,164934,164935,159615,160288,1855,164947,162549,162877,160596,1109,162309,1113,1729,162737,159615,1120,163309,164936,1123,1124,1125,164937,1126,166607,159356,161011,165911,167161,165086,164946,165287) ',
'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
'GROUP BY e.patient_id, visit_date ',
'ON DUPLICATE KEY UPDATE ',
'visit_date=VALUES(visit_date),encounter_provider=VALUES(encounter_provider),visit_scheduled=VALUES(visit_scheduled), ',
'person_present=VALUES(person_present),weight=VALUES(weight),systolic_pressure=VALUES(systolic_pressure),diastolic_pressure=VALUES(diastolic_pressure),height=VALUES(height),temperature=VALUES(temperature),pulse_rate=VALUES(pulse_rate),respiratory_rate=VALUES(respiratory_rate), ',
'oxygen_saturation=VALUES(oxygen_saturation),muac=VALUES(muac),z_score_absolute=VALUES(z_score_absolute),z_score=VALUES(z_score),nutritional_status=VALUES(nutritional_status),population_type=VALUES(population_type),key_population_type=VALUES(key_population_type),who_stage=VALUES(who_stage),who_stage_associated_oi=VALUES(who_stage_associated_oi),presenting_complaints=VALUES(presenting_complaints), ',
'clinical_notes=VALUES(clinical_notes),on_anti_tb_drugs=VALUES(on_anti_tb_drugs),on_ipt=VALUES(on_ipt),ever_on_ipt=VALUES(ever_on_ipt),cough=VALUES(cough),fever=VALUES(fever),weight_loss_poor_gain=VALUES(weight_loss_poor_gain),night_sweats=VALUES(night_sweats),tb_case_contact=VALUES(tb_case_contact),lethargy=VALUES(lethargy),screened_for_tb=VALUES(screened_for_tb), ',
'spatum_smear_ordered=VALUES(spatum_smear_ordered),chest_xray_ordered=VALUES(chest_xray_ordered),genexpert_ordered=VALUES(genexpert_ordered),spatum_smear_result=VALUES(spatum_smear_result),chest_xray_result=VALUES(chest_xray_result),genexpert_result=VALUES(genexpert_result),referral=VALUES(referral),clinical_tb_diagnosis=VALUES(clinical_tb_diagnosis),contact_invitation=VALUES(contact_invitation),evaluated_for_ipt=VALUES(evaluated_for_ipt), ',
'has_known_allergies=VALUES(has_known_allergies),has_chronic_illnesses_cormobidities=VALUES(has_chronic_illnesses_cormobidities),has_adverse_drug_reaction=VALUES(has_adverse_drug_reaction),pregnancy_status=VALUES(pregnancy_status),breastfeeding=VALUES(breastfeeding),wants_pregnancy=VALUES(wants_pregnancy),pregnancy_outcome=VALUES(pregnancy_outcome),anc_number=VALUES(anc_number),expected_delivery_date=VALUES(expected_delivery_date), ',
'last_menstrual_period=VALUES(last_menstrual_period),gravida=VALUES(gravida),parity=VALUES(parity),full_term_pregnancies=VALUES(full_term_pregnancies),abortion_miscarriages=VALUES(abortion_miscarriages),family_planning_status=VALUES(family_planning_status),family_planning_method=VALUES(family_planning_method),reason_not_using_family_planning=VALUES(reason_not_using_family_planning),tb_status=VALUES(tb_status),started_anti_TB=VALUES(started_anti_TB),tb_rx_date=VALUES(tb_rx_date),tb_treatment_no=VALUES(tb_treatment_no), ',
'general_examination=VALUES(general_examination),system_examination=VALUES(system_examination),skin_findings=VALUES(skin_findings),eyes_findings=VALUES(eyes_findings),ent_findings=VALUES(ent_findings),chest_findings=VALUES(chest_findings),cvs_findings=VALUES(cvs_findings),abdomen_findings=VALUES(abdomen_findings),cns_findings=VALUES(cns_findings),genitourinary_findings=VALUES(genitourinary_findings), ',
'ctx_adherence=VALUES(ctx_adherence),ctx_dispensed=VALUES(ctx_dispensed),dapsone_adherence=VALUES(dapsone_adherence),dapsone_dispensed=VALUES(dapsone_dispensed),inh_dispensed=VALUES(inh_dispensed),arv_adherence=VALUES(arv_adherence),poor_arv_adherence_reason=VALUES(poor_arv_adherence_reason),poor_arv_adherence_reason_other=VALUES(poor_arv_adherence_reason_other),pwp_disclosure=VALUES(pwp_disclosure),pwp_pead_disclosure=VALUES(pwp_pead_disclosure), ',
'pwp_partner_tested=VALUES(pwp_partner_tested),condom_provided=VALUES(condom_provided),substance_abuse_screening=VALUES(substance_abuse_screening),screened_for_sti=VALUES(screened_for_sti),cacx_screening=VALUES(cacx_screening),sti_partner_notification=VALUES(sti_partner_notification),experienced_gbv=VALUES(experienced_gbv),depression_screening=VALUES(depression_screening),at_risk_population=VALUES(at_risk_population),system_review_finding=VALUES(system_review_finding),next_appointment_date=VALUES(next_appointment_date),refill_date=VALUES(refill_date),appointment_consent=VALUES(appointment_consent), ',
'next_appointment_reason=VALUES(next_appointment_reason),stability=VALUES(stability),differentiated_care_group=VALUES(differentiated_care_group),differentiated_care=VALUES(differentiated_care),established_differentiated_care=VALUES(established_differentiated_care),insurance_type=VALUES(insurance_type),other_insurance_specify=VALUES(other_insurance_specify),insurance_status=VALUES(insurance_status),voided=VALUES(voided);'
);

PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END $$

-- ------------------------------------- laboratory updates ---------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_laboratory_extract $$
CREATE PROCEDURE sp_update_etl_laboratory_extract(IN last_update_time DATETIME)
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_laboratory_extract');

   SET @sql = CONCAT(
  'INSERT INTO ', @target_table, ' (',
    'uuid, encounter_id, patient_id, location_id, visit_date, visit_id, order_id, lab_test, urgency, order_reason, ',
    'order_test_name, obs_id, result_test_name, result_name, set_member_conceptId, test_result, ',
    'date_test_requested, date_test_result_received, date_created, date_last_modified, created_by',
  ') ',
  'WITH FilteredOrders AS (',
    'SELECT patient_id, encounter_id, order_id, concept_id, date_activated, urgency, order_reason ',
    'FROM orders ',
    'WHERE order_type_id = 3 AND order_action IN (''NEW'',''REVISE'') AND voided = 0 ',
    'GROUP BY patient_id, encounter_id, order_id, concept_id, date_activated, urgency, order_reason',
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
    'SELECT o.obs_id AS obs_id, o.order_id, o.concept_id, o.obs_datetime, o.date_created, o.value_numeric, n.name, n1.name AS test_name ',
    'FROM obs o ',
    'INNER JOIN concept c ON o.concept_id = c.concept_id ',
    'INNER JOIN concept_datatype cd ON c.datatype_id = cd.concept_datatype_id AND cd.name = ''Numeric'' ',
    'INNER JOIN concept_name n ON o.concept_id = n.concept_id AND n.locale = ''en'' AND n.concept_name_type = ''FULLY_SPECIFIED'' ',
    'LEFT JOIN concept_name n1 ON o.concept_id = n1.concept_id AND n1.locale = ''en'' AND n1.concept_name_type = ''FULLY_SPECIFIED'' ',
    'WHERE o.order_id IS NOT NULL',
  '), ',
  'TextLabOrderResults AS (',
    'SELECT o.obs_id AS obs_id, o.order_id, o.concept_id, o.obs_datetime, o.date_created, o.value_text, c.class_id, n.name, n1.name AS test_name ',
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
  'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR cr.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR nr.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR tr.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ') ',
  'GROUP BY o.order_id ',
  'ON DUPLICATE KEY UPDATE visit_date = VALUES(visit_date), lab_test = VALUES(lab_test), set_member_conceptId = VALUES(set_member_conceptId), test_result = VALUES(test_result);'
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
END $$

-- ------------- update etl_pharmacy_extract table--------------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_pharmacy_extract $$
CREATE PROCEDURE sp_update_etl_pharmacy_extract(IN last_update_time DATETIME)
BEGIN
    DECLARE sql_stmt TEXT;
    DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_pharmacy_extract');
    SET @sql = CONCAT(
      'INSERT INTO ', @target_table, ' (',
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
        'MAX(IF(o.concept_id = 1443, o.value_numeric, NULL)) AS frequency, ',
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
        'AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
          ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
          ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
          ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
          ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ') ',
      'GROUP BY o.obs_group_id, o.person_id, o.encounter_id ',
      'HAVING drug IS NOT NULL AND obs_group_id IS NOT NULL ',
      'ON DUPLICATE KEY UPDATE ',
        'visit_date = VALUES(visit_date), ',
        'encounter_name = VALUES(encounter_name), ',
        'is_arv = VALUES(is_arv), ',
        'is_ctx = VALUES(is_ctx), ',
        'is_dapsone = VALUES(is_dapsone), ',
        'frequency = VALUES(frequency), ',
        'duration = VALUES(duration), ',
        'duration_units = VALUES(duration_units), ',
        'voided = VALUES(voided), ',
        'date_voided = VALUES(date_voided);'
    );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Compute duration_in_days in tenant table
SET @sql = CONCAT(
      'UPDATE ', @target_table, ' SET duration_in_days = ',
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

END $$

-- ------------ create table etl_patient_treatment_event----------------------------------
-- sql
DROP PROCEDURE IF EXISTS sp_update_etl_program_discontinuation $$
CREATE PROCEDURE sp_update_etl_program_discontinuation(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_patient_program_discontinuation');

SELECT "Processing program discontinuations", CONCAT("Time: ", NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, uuid, visit_id, visit_date, program_uuid, program_name, encounter_id, ',
      'discontinuation_reason, effective_discontinuation_date, trf_out_verified, trf_out_verification_date, ',
      'date_died, transfer_facility, transfer_date, death_reason, specific_death_cause, natural_causes, non_natural_cause, ',
      'date_created, date_last_modified',
    ') ',
    'SELECT ',
      'q.patient_id, q.uuid, q.visit_id, q.encounter_datetime, q.program_uuid, q.program_name, q.encounter_id, ',
      'q.reason_discontinued, q.effective_discontinuation_date, q.trf_out_verified, q.trf_out_verification_date, ',
      'q.date_died, COALESCE(l.name, q.to_facility_raw) AS transfer_facility, q.to_date, q.death_reason, q.specific_death_cause, ',
      'q.natural_causes, q.non_natural_cause, q.date_created, q.date_last_modified ',
    'FROM (',
      'SELECT ',
        'e.patient_id, e.uuid, e.visit_id, e.encounter_datetime, et.uuid AS program_uuid, ',
        '(CASE et.uuid ',
          'WHEN ''2bdada65-4c72-4a48-8730-859890e25cee'' THEN ''HIV'' ',
          'WHEN ''d3e3d723-7458-4b4e-8998-408e8a551a84'' THEN ''TB'' ',
          'WHEN ''01894f88-dc73-42d4-97a3-0929118403fb'' THEN ''MCH Child HEI'' ',
          'WHEN ''5feee3f1-aa16-4513-8bd0-5d9b27ef1208'' THEN ''MCH Child'' ',
          'WHEN ''7c426cfc-3b47-4481-b55f-89860c21c7de'' THEN ''MCH Mother'' ',
          'WHEN ''162382b8-0464-11ea-9a9f-362b9e155667'' THEN ''OTZ'' ',
          'WHEN ''5cf00d9e-09da-11ea-8d71-362b9e155667'' THEN ''OVC'' ',
          'WHEN ''d7142400-2495-11e9-ab14-d663bd873d93'' THEN ''KP'' ',
        'END) AS program_name, ',
        'e.encounter_id, ',
        'COALESCE(MAX(IF(o.concept_id = 161555, o.value_coded, NULL)), MAX(IF(o.concept_id = 159786, o.value_coded, NULL))) AS reason_discontinued, ',
        'COALESCE(MAX(IF(o.concept_id = 164384, o.value_datetime, NULL)), MAX(IF(o.concept_id = 159787, o.value_datetime, NULL))) AS effective_discontinuation_date, ',
        'MAX(IF(o.concept_id = 1285, o.value_coded, NULL)) AS trf_out_verified, ',
        'MAX(IF(o.concept_id = 164133, o.value_datetime, NULL)) AS trf_out_verification_date, ',
        'MAX(IF(o.concept_id = 1543, o.value_datetime, NULL)) AS date_died, ',
        'MAX(IF(o.concept_id = 159495, LEFT(TRIM(o.value_text),100), NULL)) AS to_facility_raw, ',
        'MAX(IF(o.concept_id = 160649, o.value_datetime, NULL)) AS to_date, ',
        'MAX(IF(o.concept_id = 1599, o.value_coded, NULL)) AS death_reason, ',
        'MAX(IF(o.concept_id = 1748, o.value_coded, NULL)) AS specific_death_cause, ',
        'MAX(IF(o.concept_id = 162580, LEFT(TRIM(o.value_text),200), NULL)) AS natural_causes, ',
        'MAX(IF(o.concept_id = 160218, LEFT(TRIM(o.value_text),200), NULL)) AS non_natural_cause, ',
        'e.date_created AS date_created, ',
        'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
      'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (161555,159786,159787,164384,1543,159495,160649,165380,1285,164133,1599,1748,162580,160218) ',
      'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (',
        '''2bdada65-4c72-4a48-8730-859890e25cee'',',
        '''d3e3d723-7458-4b4e-8998-408e8a551a84'',',
        '''5feee3f1-aa16-4513-8bd0-5d9b27ef1208'',',
        '''7c426cfc-3b47-4481-b55f-89860c21c7de'',',
        '''01894f88-dc73-42d4-97a3-0929118403fb'',',
        '''162382b8-0464-11ea-9a9f-362b9e155667'',',
        '''5cf00d9e-09da-11ea-8d71-362b9e155667'',',
        '''d7142400-2495-11e9-ab14-d663bd873d93''',
      ')) et ON et.encounter_type_id = e.encounter_type ',
      'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
        ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ') ',
      'GROUP BY e.encounter_id',
    ') q ',
    'LEFT JOIN location l ON l.uuid = q.to_facility_raw ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), ',
      'discontinuation_reason = VALUES(discontinuation_reason), ',
      'date_died = VALUES(date_died), ',
      'transfer_facility = VALUES(transfer_facility), ',
      'transfer_date = VALUES(transfer_date), ',
      'trf_out_verified = VALUES(trf_out_verified), ',
      'trf_out_verification_date = VALUES(trf_out_verification_date), ',
      'death_reason = VALUES(death_reason), ',
      'specific_death_cause = VALUES(specific_death_cause);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT "Completed processing discontinuation data", CONCAT("Time: ", NOW());
END $$

-- ------------- update etl_mch_enrollment-----------------------

DROP PROCEDURE IF EXISTS sp_update_etl_mch_enrollment $$
CREATE PROCEDURE sp_update_etl_mch_enrollment(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_mch_enrollment');

SELECT "Processing MCH Enrollments", CONCAT("Time: ", NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, uuid, visit_id, visit_date, location_id, encounter_id, service_type, anc_number, ',
      'first_anc_visit_date, gravida, parity, parity_abortion, age_at_menarche, lmp, lmp_estimated, edd_ultrasound, ',
      'blood_group, serology, tb_screening, bs_for_mps, hiv_status, hiv_test_date, partner_hiv_status, partner_hiv_test_date, ',
      'ti_date_started_art, ti_current_regimen, ti_care_facility, urine_microscopy, urinary_albumin, glucose_measurement, ',
      'urine_ph, urine_gravity, urine_nitrite_test, urine_leukocyte_esterace_test, urinary_ketone, urine_bile_salt_test, ',
      'urine_bile_pigment_test, urine_colour, urine_turbidity, urine_dipstick_for_blood, discontinuation_reason, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
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
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (163530,163547,5624,160080,1823,160598,1427,162095,5596,300,299,160108,32,159427,160554,1436,160082,159599,164855,162724,56,1875,159734,161438,161439,161440,161441,161442,161444,161443,162106,162101,162096,161555,160478) ',
    'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''3ee036d8-7c13-4393-b5d6-036f2fe45126'')) et ON et.encounter_type_id = e.encounter_type ',
    'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date),service_type=VALUES(service_type),anc_number=VALUES(anc_number),first_anc_visit_date=VALUES(first_anc_visit_date),gravida=VALUES(gravida),parity=VALUES(parity),parity_abortion=VALUES(parity_abortion),age_at_menarche=VALUES(age_at_menarche),lmp=VALUES(lmp),lmp_estimated=VALUES(lmp_estimated),',
      'edd_ultrasound=VALUES(edd_ultrasound),blood_group=VALUES(blood_group),serology=VALUES(serology),tb_screening=VALUES(tb_screening),bs_for_mps=VALUES(bs_for_mps),hiv_status=VALUES(hiv_status),hiv_test_date=VALUES(hiv_test_date),partner_hiv_status=VALUES(partner_hiv_status),partner_hiv_test_date=VALUES(partner_hiv_test_date),',
      'ti_date_started_art=VALUES(ti_date_started_art),ti_current_regimen=VALUES(ti_current_regimen),ti_care_facility=VALUES(ti_care_facility),',
      'urine_microscopy=VALUES(urine_microscopy),urinary_albumin=VALUES(urinary_albumin),glucose_measurement=VALUES(glucose_measurement),urine_ph=VALUES(urine_ph),urine_gravity=VALUES(urine_gravity),urine_nitrite_test=VALUES(urine_nitrite_test),urine_leukocyte_esterace_test=VALUES(urine_leukocyte_esterace_test),urinary_ketone=VALUES(urinary_ketone),',
      'urine_bile_salt_test=VALUES(urine_bile_salt_test),urine_bile_pigment_test=VALUES(urine_bile_pigment_test),urine_colour=VALUES(urine_colour),urine_turbidity=VALUES(urine_turbidity),urine_dipstick_for_blood=VALUES(urine_dipstick_for_blood),discontinuation_reason=VALUES(discontinuation_reason);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT "Completed processing MCH Enrollments ", CONCAT("Time: ", NOW());
END $$

-- ------------- update etl_mch_antenatal_visit-------------------------

-- sql
DROP PROCEDURE IF EXISTS sp_update_etl_mch_antenatal_visit $$
CREATE PROCEDURE sp_update_etl_mch_antenatal_visit(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_mch_antenatal_visit');

SET sql_stmt = CONCAT(
  'INSERT INTO ', @target_table, ' (',
    'patient_id, uuid, visit_id, visit_date, location_id, encounter_id, provider, anc_visit_number, temperature, pulse_rate, systolic_bp, diastolic_bp, respiratory_rate, oxygen_saturation, weight,',
    'height, muac, hemoglobin, breast_exam_done, pallor, maturity, fundal_height, fetal_presentation, lie, fetal_heart_rate, fetal_movement, who_stage, cd4, vl_sample_taken, viral_load, ldl, arv_status,',
    'final_test_result, patient_given_result, partner_hiv_tested, partner_hiv_status, prophylaxis_given, haart_given, date_given_haart, baby_azt_dispensed, baby_nvp_dispensed, deworming_done_anc,',
    'IPT_dose_given_anc, TTT, IPT_malaria, iron_supplement, deworming, bed_nets, urine_microscopy, urinary_albumin, glucose_measurement, urine_ph, urine_gravity, urine_nitrite_test,',
    'urine_leukocyte_esterace_test, urinary_ketone, urine_bile_salt_test, urine_bile_pigment_test, urine_colour, urine_turbidity, urine_dipstick_for_blood, syphilis_test_status,',
    'syphilis_treated_status, bs_mps, diabetes_test, fgm_done, fgm_complications, fp_method_postpartum, anc_exercises, tb_screening, cacx_screening, cacx_screening_method, hepatitis_b_screening,',
    'hepatitis_b_treatment, has_other_illnes, counselled, counselled_on_birth_plans, counselled_on_danger_signs, counselled_on_family_planning, counselled_on_hiv, counselled_on_supplimental_feeding,',
    'counselled_on_breast_care, counselled_on_infant_feeding, counselled_on_treated_nets, intermittent_presumptive_treatment_given, intermittent_presumptive_treatment_dose, minimum_care_package,',
    'minimum_package_of_care_services, risk_reduction, partner_testing, sti_screening, condom_provision, prep_adherence, anc_visits_emphasis, pnc_fp_counseling, referral_vmmc, referral_dreams,',
    'referred_from, referred_to, clinical_notes, date_created, date_last_modified',
  ') ',
  'SELECT ',
    'e.patient_id, e.uuid, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.creator, ',
    'MAX(IF(o.concept_id=1425,o.value_numeric,NULL)) AS anc_visit_number, ',
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
    'MAX(IF(o.concept_id=163590,o.value_coded,NULL)) AS breast_exam_done, ',
    'MAX(IF(o.concept_id=5245,o.value_coded,NULL)) AS pallor, ',
    'MAX(IF(o.concept_id=1438,o.value_numeric,NULL)) AS maturity, ',
    'MAX(IF(o.concept_id=1439,o.value_numeric,NULL)) AS fundal_height, ',
    'MAX(IF(o.concept_id=160090,o.value_coded,NULL)) AS fetal_presentation, ',
    'MAX(IF(o.concept_id=162089,o.value_coded,NULL)) AS lie, ',
    'MAX(IF(o.concept_id=1440,o.value_numeric,NULL)) AS fetal_heart_rate, ',
    'MAX(IF(o.concept_id=162107,o.value_coded,NULL)) AS fetal_movement, ',
    'MAX(IF(o.concept_id=5356,o.value_coded,NULL)) AS who_stage, ',
    'MAX(IF(o.concept_id=5497,o.value_numeric,NULL)) AS cd4, ',
    'MAX(IF(o.concept_id=1271,o.value_coded,NULL)) AS vl_sample_taken, ',
    'MAX(IF(o.concept_id=856,o.value_numeric,NULL)) AS viral_load, ',
    'MAX(IF(o.concept_id=1305,o.value_coded,NULL)) AS ldl, ',
    'MAX(IF(o.concept_id=1147,o.value_coded,NULL)) AS arv_status, ',
    'MAX(IF(o.concept_id=159427,(CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1138 THEN ''Inconclusive'' ELSE '''' END),NULL)) AS final_test_result, ',
    'MAX(IF(o.concept_id=164848,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS patient_given_result, ',
    'MAX(IF(o.concept_id=161557,o.value_coded,NULL)) AS partner_hiv_tested, ',
    'MAX(IF(o.concept_id=1436,o.value_coded,NULL)) AS partner_hiv_status, ',
    'MAX(IF(o.concept_id=1109,o.value_coded,NULL)) AS prophylaxis_given, ',
    'MAX(IF(o.concept_id=5576,o.value_coded,NULL)) AS haart_given, ',
    'MAX(IF(o.concept_id=163784,o.value_datetime,NULL)) AS date_given_haart, ',
    'MAX(IF(o.concept_id=1282 AND o.value_coded = 160123,o.value_coded,NULL)) AS baby_azt_dispensed, ',
    'MAX(IF(o.concept_id=1282 AND o.value_coded = 80586,o.value_coded,NULL)) AS baby_nvp_dispensed, ',
    'MAX(IF(o.concept_id=159922,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''N/A'' ELSE '''' END),NULL)) AS deworming_done_anc, ',
    'MAX(IF(o.concept_id=1418,o.value_numeric,NULL)) AS IPT_dose_given_anc, ',
    'MAX(IF(o.concept_id=984,(CASE o.value_coded WHEN 84879 THEN ''Yes'' ELSE '''' END),NULL)) AS TTT, ',
    'MAX(IF(o.concept_id=984,(CASE o.value_coded WHEN 159610 THEN ''Yes'' ELSE '''' END),NULL)) AS IPT_malaria, ',
    'MAX(IF(o.concept_id=984,(CASE o.value_coded WHEN 104677 THEN ''Yes'' ELSE '''' END),NULL)) AS iron_supplement, ',
    'MAX(IF(o.concept_id=984,(CASE o.value_coded WHEN 79413 THEN ''Yes'' ELSE '''' END),NULL)) AS deworming, ',
    'MAX(IF(o.concept_id=984,(CASE o.value_coded WHEN 160428 THEN ''Yes'' ELSE '''' END),NULL)) AS bed_nets, ',
    'MAX(IF(o.concept_id=56,o.value_text,NULL)) AS urine_microscopy, ',
    'MAX(IF(o.concept_id=1875,o.value_coded,NULL)) AS urinary_albumin, ',
    'MAX(IF(o.concept_id=159734,o.value_coded,NULL)) AS glucose_measurement, ',
    'MAX(IF(o.concept_id=161438,o.value_numeric,NULL)) AS urine_ph, ',
    'MAX(IF(o.concept_id=161439,o.value_numeric,NULL)) AS urine_gravity, ',
    'MAX(IF(o.concept_id=161440,o.value_coded,NULL)) AS urine_nitrite_test, ',
    'MAX(IF(o.concept_id=161441,o.value_coded,NULL)) AS urine_leukocyte_esterace_test, ',
    'MAX(IF(o.concept_id=161442,o.value_coded,NULL)) AS urinary_ketone, ',
    'MAX(IF(o.concept_id=161444,o.value_coded,NULL)) AS urine_bile_salt_test, ',
    'MAX(IF(o.concept_id=161443,o.value_coded,NULL)) AS urine_bile_pigment_test, ',
    'MAX(IF(o.concept_id=162106,o.value_coded,NULL)) AS urine_colour, ',
    'MAX(IF(o.concept_id=162101,o.value_coded,NULL)) AS urine_turbidity, ',
    'MAX(IF(o.concept_id=162096,o.value_coded,NULL)) AS urine_dipstick_for_blood, ',
    'MAX(IF(o.concept_id=299,o.value_coded,NULL)) AS syphilis_test_status, ',
    'MAX(IF(o.concept_id=159918,o.value_coded,NULL)) AS syphilis_treated_status, ',
    'MAX(IF(o.concept_id=32,o.value_coded,NULL)) AS bs_mps, ',
    'MAX(IF(o.concept_id=119481,o.value_coded,NULL)) AS diabetes_test, ',
    'MAX(IF(o.concept_id=165099,o.value_coded,NULL)) AS fgm_done, ',
    'MAX(IF(o.concept_id=120198,o.value_coded,NULL)) AS fgm_complications, ',
    'MAX(IF(o.concept_id=374,o.value_coded,NULL)) AS fp_method_postpartum, ',
    'MAX(IF(o.concept_id=161074,o.value_coded,NULL)) AS anc_exercises, ',
    'MAX(IF(o.concept_id=1659,o.value_coded,NULL)) AS tb_screening, ',
    'MAX(IF(o.concept_id=164934,o.value_coded,NULL)) AS cacx_screening, ',
    'MAX(IF(o.concept_id=163589,o.value_coded,NULL)) AS cacx_screening_method, ',
    'MAX(IF(o.concept_id=165040,o.value_coded,NULL)) AS hepatitis_b_screening, ',
    'MAX(IF(o.concept_id=166665,o.value_coded,NULL)) AS hepatitis_b_treatment, ',
    'MAX(IF(o.concept_id=162747,o.value_coded,NULL)) AS has_other_illnes, ',
    'MAX(IF(o.concept_id=1912,o.value_coded,NULL)) AS counselled, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=159758,o.value_coded,NULL)) AS counselled_on_birth_plans, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=159857,o.value_coded,NULL)) AS counselled_on_danger_signs, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=156277,o.value_coded,NULL)) AS counselled_on_family_planning, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=1914,o.value_coded,NULL)) AS counselled_on_hiv, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=159854,o.value_coded,NULL)) AS counselled_on_supplimental_feeding, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=159856,o.value_coded,NULL)) AS counselled_on_breast_care, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=161651,o.value_coded,NULL)) AS counselled_on_infant_feeding, ',
    'MAX(IF(o.concept_id=159853 AND o.value_coded=1381,o.value_coded,NULL)) AS counselled_on_treated_nets, ',
    'MAX(IF(o.concept_id=1591,o.value_coded,NULL)) AS intermittent_presumptive_treatment_given, ',
    'MAX(IF(o.concept_id=1418,o.value_numeric,NULL)) AS intermittent_presumptive_treatment_dose, ',
    'MAX(IF(o.concept_id IN (165302,161595),o.value_coded,NULL)) AS minimum_care_package, ',
    'CONCAT_WS('','', ',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =165275,''Risk Reduction counselling'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =161557,''HIV Testing for the Partner'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =165190,''STI Screening and treatment'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =159777,''Condom Provision'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =165203,''PrEP with emphasis on adherence'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =165475,''Emphasize importance of follow up ANC Visits'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =1382,''Postnatal FP Counselling and support'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =162223,''Referrals for VMMC Services for partner'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =165368,''Referrals for OVC/DREAMS'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =166607,''Pre appointmnet SMS'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =166486,''Tartgeted home visits'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =1167,''Psychosocial and disclosure support'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =165002,''3-monthly Enhanced ART adherence assessments optimize TLD'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =163310,''Timely viral load monitoring, early ART switches'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =167410,''Complex case reviews in MDT/Consultation with clinical mentors'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =167079,''Enhanced longitudinal Mother-Infant Pair follow up'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =166563,''Early HEI case identification'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =160116,''Bi weekly random file audits to inform quality improvement'', '''')),'''') ,',
      'NULLIF(MAX(IF(o.concept_id=1592 AND o.value_coded =160031,''LTFU root cause audit and return to care plan default'', '''')),'''')',
    ') AS minimum_package_of_care_services, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=165275,o.value_coded,NULL)) AS risk_reduction, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=161557,o.value_coded,NULL)) AS partner_testing, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=165190,o.value_coded,NULL)) AS sti_screening, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=159777,o.value_coded,NULL)) AS condom_provision, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=165203,o.value_coded,NULL)) AS prep_adherence, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=165475,o.value_coded,NULL)) AS anc_visits_emphasis, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=1382,o.value_coded,NULL)) AS pnc_fp_counseling, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=162223,o.value_coded,NULL)) AS referral_vmmc, ',
    'MAX(IF(o.concept_id=1592 AND o.value_coded=165368,o.value_coded,NULL)) AS referral_dreams, ',
    'MAX(IF(o.concept_id=160481,o.value_coded,NULL)) AS referred_from, ',
    'MAX(IF(o.concept_id=163145,o.value_coded,NULL)) AS referred_to, ',
    'MAX(IF(o.concept_id=159395,o.value_text,NULL)) AS clinical_notes, ',
    'e.date_created AS date_created, ',
    'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
  'FROM encounter e ',
  'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
  'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
    'AND o.concept_id IN (1282,159922,984,1418,1425,5088,5087,5085,5086,5242,5092,5089,5090,1343,21,163590,5245,1438,1439,160090,162089,1440,162107,5356,5497,856,1305,1147,159427,164848,161557,1436,1109,5576,128256,1875,159734,161438,161439,161440,161441,161442,161444,161443,162106,162101,162096,299,159918,32,119481,165099,120198,374,161074,1659,164934,163589,165040,166665,162747,1912,160481,163145,5096,159395,163784,1271,159853,165302,1592,1591,1418,1592,161595) ',
  'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''e8f98494-af35-4bb8-9fc7-c409c8fed843'',''d3ea25c7-a3e8-4f57-a6a9-e802c3565a30'')) f ON f.form_id = e.form_id ',
  'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
  ') ',
  'GROUP BY e.patient_id, visit_date ',
  'ON DUPLICATE KEY UPDATE ',
    'visit_date=VALUES(visit_date),provider=VALUES(provider),anc_visit_number=VALUES(anc_visit_number),temperature=VALUES(temperature),pulse_rate=VALUES(pulse_rate),systolic_bp=VALUES(systolic_bp),diastolic_bp=VALUES(diastolic_bp),respiratory_rate=VALUES(respiratory_rate),',
    'oxygen_saturation=VALUES(oxygen_saturation),weight=VALUES(weight),height=VALUES(height),muac=VALUES(muac),hemoglobin=VALUES(hemoglobin),breast_exam_done=VALUES(breast_exam_done),pallor=VALUES(pallor),maturity=VALUES(maturity),fundal_height=VALUES(fundal_height),fetal_presentation=VALUES(fetal_presentation),lie=VALUES(lie),',
    'fetal_heart_rate=VALUES(fetal_heart_rate),fetal_movement=VALUES(fetal_movement),who_stage=VALUES(who_stage),cd4=VALUES(cd4),vl_sample_taken=VALUES(vl_sample_taken),viral_load=VALUES(viral_load),ldl=VALUES(ldl),arv_status=VALUES(arv_status),final_test_result=VALUES(final_test_result),',
    'patient_given_result=VALUES(patient_given_result),partner_hiv_tested=VALUES(partner_hiv_tested),partner_hiv_status=VALUES(partner_hiv_status),prophylaxis_given=VALUES(prophylaxis_given),haart_given=VALUES(haart_given),date_given_haart=VALUES(date_given_haart),baby_azt_dispensed=VALUES(baby_azt_dispensed),baby_nvp_dispensed=VALUES(baby_nvp_dispensed),deworming_done_anc=VALUES(deworming_done_anc),',
    'TTT=VALUES(TTT),IPT_dose_given_anc=VALUES(IPT_dose_given_anc),IPT_malaria=VALUES(IPT_malaria),iron_supplement=VALUES(iron_supplement),deworming=VALUES(deworming),bed_nets=VALUES(bed_nets),urine_microscopy=VALUES(urine_microscopy),urinary_albumin=VALUES(urinary_albumin),glucose_measurement=VALUES(glucose_measurement),urine_ph=VALUES(urine_ph),urine_gravity=VALUES(urine_gravity),',
    'urine_nitrite_test=VALUES(urine_nitrite_test),urine_leukocyte_esterace_test=VALUES(urine_leukocyte_esterace_test),urinary_ketone=VALUES(urinary_ketone),urine_bile_salt_test=VALUES(urine_bile_salt_test),urine_bile_pigment_test=VALUES(urine_bile_pigment_test),urine_colour=VALUES(urine_colour),urine_turbidity=VALUES(urine_turbidity),urine_dipstick_for_blood=VALUES(urine_dipstick_for_blood),syphilis_test_status=VALUES(syphilis_test_status),syphilis_treated_status=VALUES(syphilis_treated_status),',
    'bs_mps=VALUES(bs_mps),diabetes_test=VALUES(diabetes_test),fgm_done=VALUES(fgm_done),fgm_complications=VALUES(fgm_complications),fp_method_postpartum=VALUES(fp_method_postpartum),anc_exercises=VALUES(anc_exercises),tb_screening=VALUES(tb_screening),cacx_screening=VALUES(cacx_screening),cacx_screening_method=VALUES(cacx_screening_method),hepatitis_b_screening=VALUES(hepatitis_b_screening),hepatitis_b_treatment=VALUES(hepatitis_b_treatment),',
    'has_other_illnes=VALUES(has_other_illnes),counselled=VALUES(counselled),counselled_on_birth_plans=VALUES(counselled_on_birth_plans),counselled_on_danger_signs=VALUES(counselled_on_danger_signs),counselled_on_family_planning=VALUES(counselled_on_family_planning),counselled_on_hiv=VALUES(counselled_on_hiv),counselled_on_supplimental_feeding=VALUES(counselled_on_supplimental_feeding),counselled_on_breast_care=VALUES(counselled_on_breast_care),counselled_on_infant_feeding=VALUES(counselled_on_infant_feeding),counselled_on_treated_nets=VALUES(counselled_on_treated_nets),referred_from=VALUES(referred_from),',
    'minimum_care_package=VALUES(minimum_care_package),risk_reduction=VALUES(risk_reduction),partner_testing=VALUES(partner_testing),sti_screening=VALUES(sti_screening),condom_provision=VALUES(condom_provision),prep_adherence=VALUES(prep_adherence),anc_visits_emphasis=VALUES(anc_visits_emphasis),pnc_fp_counseling=VALUES(pnc_fp_counseling),referral_vmmc=VALUES(referral_vmmc),referral_dreams=VALUES(referral_dreams),referred_to=VALUES(referred_to),next_appointment_date=VALUES(next_appointment_date),clinical_notes=VALUES(clinical_notes),intermittent_presumptive_treatment_given=VALUES(intermittent_presumptive_treatment_given),intermittent_presumptive_treatment_dose=VALUES(intermittent_presumptive_treatment_dose),minimum_package_of_care_services=VALUES(minimum_package_of_care_services);'
);

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
END $$

-- ------------- update etl_mchs_delivery-------------------------

-- sql


DROP PROCEDURE IF EXISTS sp_update_etl_mch_delivery $$
CREATE PROCEDURE sp_update_etl_mch_delivery(IN last_update_time DATETIME)
BEGIN
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_mchs_delivery');
  SET @sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id,uuid,provider,visit_id,visit_date,location_id,encounter_id,date_created,date_last_modified,number_of_anc_visits,',
      'vaginal_examination,uterotonic_given,chlohexidine_applied_on_code_stump,vitamin_K_given,kangaroo_mother_care_given,',
      'testing_done_in_the_maternity_hiv_status,infant_provided_with_arv_prophylaxis,mother_on_haart_during_anc,mother_started_haart_at_maternity,vdrl_rpr_results,',
      'date_of_last_menstrual_period,estimated_date_of_delivery,reason_for_referral,admission_number,duration_of_pregnancy,mode_of_delivery,',
      'date_of_delivery,blood_loss,condition_of_mother,delivery_outcome,apgar_score_1min,apgar_score_5min,apgar_score_10min,resuscitation_done,',
      'place_of_delivery,delivery_assistant,counseling_on_infant_feeding,counseling_on_exclusive_breastfeeding,counseling_on_infant_feeding_for_hiv_infected,',
      'mother_decision,placenta_complete,maternal_death_audited,cadre,delivery_complications,coded_delivery_complications,other_delivery_complications,',
      'duration_of_labor,baby_sex,baby_condition,teo_given,birth_weight,bf_within_one_hour,birth_with_deformity,type_of_birth_deformity,',
      'final_test_result,patient_given_result,partner_hiv_tested,partner_hiv_status,prophylaxis_given,baby_azt_dispensed,baby_nvp_dispensed,',
      'clinical_notes,stimulation_done,suction_done,oxygen_given,bag_mask_ventilation_provided,induction_done,artificial_rapture_done',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_datetime, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id=1590,o.value_numeric,NULL)) AS number_of_anc_visits, ',
      'MAX(IF(o.concept_id=160704,o.value_coded,NULL)) AS vaginal_examination, ',
      'MAX(IF(o.concept_id=1282 AND o.value_coded IN (81369,104590,5622,1107),o.value_coded,NULL)) AS uterotonic_given, ',
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
      'MAX(IF(o.concept_id=159521,o.value_coded,NULL)) AS type_of_birth_deformity, ',
      'MAX(IF(o.concept_id=159427,(CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1138 THEN ''Inconclusive'' ELSE '''' END),NULL)) AS final_test_result, ',
      'MAX(IF(o.concept_id=164848,(CASE o.value_coded WHEN 1065 THEN \"Yes\" WHEN 1066 THEN \"No\" ELSE \"\" END),NULL)) AS patient_given_result, ',
      'MAX(IF(o.concept_id=161557,o.value_coded,NULL)) AS partner_hiv_tested, ',
      'MAX(IF(o.concept_id=1436,o.value_coded,NULL)) AS partner_hiv_status, ',
      'MAX(IF(o.concept_id=1109,o.value_coded,NULL)) AS prophylaxis_given, ',
      'MAX(IF(o.concept_id = 1282 AND o.value_coded = 160123,1,0)) AS baby_azt_dispensed, ',
      'MAX(IF(o.concept_id = 1282 AND o.value_coded = 80586,1,0)) AS baby_nvp_dispensed, ',
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
        'AND o.concept_id IN (162054,1590,160704,1282,159369,984,161094,1396,161930,163783,166665,299,1427,5596,164359,1789,5630,5599,161928,1856,159603,159604,159605,162131,1572,1473,1379,1151,163454,1602,1573,162093,1576,120216,159616,1587,159917,1282,5916,161543,164122,159521,159427,164848,161557,1436,1109,5576,159595,163784,159395,168751,1284,113316,165647,113602,163445,159949,1570) ',
      'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''496c7cc3-0eea-4e84-a04c-2292949e2f7f'')) f ON f.form_id = e.form_id ',
    'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider),visit_id=VALUES(visit_id),visit_date=VALUES(visit_date),encounter_id=VALUES(encounter_id),number_of_anc_visits=VALUES(number_of_anc_visits),',
      'vaginal_examination=VALUES(vaginal_examination),uterotonic_given=VALUES(uterotonic_given),chlohexidine_applied_on_code_stump=VALUES(chlohexidine_applied_on_code_stump),vitamin_K_given=VALUES(vitamin_K_given),',
      'kangaroo_mother_care_given=VALUES(kangaroo_mother_care_given),testing_done_in_the_maternity_hiv_status=VALUES(testing_done_in_the_maternity_hiv_status),infant_provided_with_arv_prophylaxis=VALUES(infant_provided_with_arv_prophylaxis),',
      'mother_on_haart_during_anc=VALUES(mother_on_haart_during_anc),mother_started_haart_at_maternity=VALUES(mother_started_haart_at_maternity),vdrl_rpr_results=VALUES(vdrl_rpr_results),',
      'date_of_last_menstrual_period=VALUES(date_of_last_menstrual_period),estimated_date_of_delivery=VALUES(estimated_date_of_delivery),reason_for_referral=VALUES(reason_for_referral),',
      'date_created=VALUES(date_created),admission_number=VALUES(admission_number),duration_of_pregnancy=VALUES(duration_of_pregnancy),mode_of_delivery=VALUES(mode_of_delivery),date_of_delivery=VALUES(date_of_delivery),',
      'blood_loss=VALUES(blood_loss),condition_of_mother=VALUES(condition_of_mother),apgar_score_1min=VALUES(apgar_score_1min),apgar_score_5min=VALUES(apgar_score_5min),apgar_score_10min=VALUES(apgar_score_10min),',
      'resuscitation_done=VALUES(resuscitation_done),place_of_delivery=VALUES(place_of_delivery),delivery_assistant=VALUES(delivery_assistant),counseling_on_infant_feeding=VALUES(counseling_on_infant_feeding),',
      'counseling_on_exclusive_breastfeeding=VALUES(counseling_on_exclusive_breastfeeding),counseling_on_infant_feeding_for_hiv_infected=VALUES(counseling_on_infant_feeding_for_hiv_infected),mother_decision=VALUES(mother_decision),',
      'placenta_complete=VALUES(placenta_complete),maternal_death_audited=VALUES(maternal_death_audited),cadre=VALUES(cadre),delivery_complications=VALUES(delivery_complications),',
      'coded_delivery_complications=VALUES(coded_delivery_complications),other_delivery_complications=VALUES(other_delivery_complications),duration_of_labor=VALUES(duration_of_labor),baby_sex=VALUES(baby_sex),',
      'baby_condition=VALUES(baby_condition),teo_given=VALUES(teo_given),birth_weight=VALUES(birth_weight),bf_within_one_hour=VALUES(bf_within_one_hour),birth_with_deformity=VALUES(birth_with_deformity),',
      'type_of_birth_deformity=VALUES(type_of_birth_deformity),final_test_result=VALUES(final_test_result),patient_given_result=VALUES(patient_given_result),partner_hiv_tested=VALUES(partner_hiv_tested),',
      'partner_hiv_status=VALUES(partner_hiv_status),prophylaxis_given=VALUES(prophylaxis_given),baby_azt_dispensed=VALUES(baby_azt_dispensed),baby_nvp_dispensed=VALUES(baby_nvp_dispensed),',
      'clinical_notes=VALUES(clinical_notes),stimulation_done=VALUES(stimulation_done),suction_done=VALUES(suction_done),oxygen_given=VALUES(oxygen_given),',
      'bag_mask_ventilation_provided=VALUES(bag_mask_ventilation_provided),induction_done=VALUES(induction_done),artificial_rapture_done=VALUES(artificial_rapture_done);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
END $$


-- ------------- populate etl_mchs_discharge-------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_mch_discharge $$
CREATE PROCEDURE sp_update_etl_mch_discharge(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_mchs_discharge');

SELECT "Processing MCH Discharge ", CONCAT("Time: ", NOW());

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, date_created, date_last_modified, ',
      'counselled_on_feeding, baby_status, vitamin_A_dispensed, birth_notification_number, condition_of_mother, discharge_date, ',
      'referred_from, referred_to, clinical_notes',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_datetime, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 161651, o.value_coded, NULL)) AS counselled_on_feeding, ',
      'MAX(IF(o.concept_id = 159926, o.value_coded, NULL)) AS baby_status, ',
      'MAX(IF(o.concept_id = 161534, o.value_coded, NULL)) AS vitamin_A_dispensed, ',
      'MAX(IF(o.concept_id = 162051, o.value_text, NULL)) AS birth_notification_number, ',
      'MAX(IF(o.concept_id = 162093, o.value_text, NULL)) AS condition_of_mother, ',
      'MAX(IF(o.concept_id = 1641, o.value_datetime, NULL)) AS discharge_date, ',
      'MAX(IF(o.concept_id = 160481, o.value_coded, NULL)) AS referred_from, ',
      'MAX(IF(o.concept_id = 163145, o.value_coded, NULL)) AS referred_to, ',
      'MAX(IF(o.concept_id = 159395, o.value_text, NULL)) AS clinical_notes ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
        'AND o.concept_id IN (161651,159926,161534,162051,162093,1641,160481,163145,159395) ',
      'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''af273344-a5f9-11e8-98d0-529269fb1459'')) f ON f.form_id = e.form_id ',
    'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), ',
      'provider = VALUES(provider), ',
      'counselled_on_feeding = VALUES(counselled_on_feeding), ',
      'baby_status = VALUES(baby_status), ',
      'vitamin_A_dispensed = VALUES(vitamin_A_dispensed), ',
      'birth_notification_number = VALUES(birth_notification_number), ',
      'condition_of_mother = VALUES(condition_of_mother), ',
      'discharge_date = VALUES(discharge_date), ',
      'referred_from = VALUES(referred_from), ',
      'referred_to = VALUES(referred_to), ',
      'clinical_notes = VALUES(clinical_notes);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT "Completed processing MCH Discharge visits", CONCAT("Time: ", NOW());
END $$

-- ------------- update etl_mch_postnatal_visit-------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_mch_postnatal_visit $$
CREATE PROCEDURE sp_update_etl_mch_postnatal_visit(IN last_update_time DATETIME)
BEGIN
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_mch_postnatal_visit');

SELECT "Processing MCH postnatal visits ", CONCAT("Time: ", NOW());

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, uuid, visit_id, visit_date, location_id, encounter_id, provider, pnc_register_no, pnc_visit_no, delivery_date, ',
      'mode_of_delivery, place_of_delivery, visit_timing_mother, visit_timing_baby, delivery_outcome, temperature, pulse_rate, ',
      'systolic_bp, diastolic_bp, respiratory_rate, oxygen_saturation, weight, height, muac, hemoglobin, arv_status, general_condition, ',
      'breast, cs_scar, gravid_uterus, episiotomy, lochia, counselled_on_infant_feeding, pallor, pallor_severity, pph, mother_hiv_status, ',
      'condition_of_baby, baby_feeding_method, umblical_cord, baby_immunization_started, family_planning_counseling, other_maternal_complications, ',
      'uterus_examination, uterus_cervix_examination, vaginal_examination, parametrial_examination, external_genitalia_examination, ovarian_examination, ',
      'pelvic_lymph_node_exam, final_test_result, syphilis_results, patient_given_result, couple_counselled, partner_hiv_tested, partner_hiv_status, ',
      'pnc_hiv_test_timing_mother, mother_haart_given, prophylaxis_given, infant_prophylaxis_timing, baby_azt_dispensed, baby_nvp_dispensed, ',
      'pnc_exercises, maternal_condition, iron_supplementation, fistula_screening, cacx_screening, cacx_screening_method, family_planning_status, ',
      'family_planning_method, referred_from, referred_to, referral_reason, clinical_notes, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.visit_id, e.encounter_datetime, e.location_id, e.encounter_id, e.creator, ',
      'MAX(IF(o.concept_id=1646, o.value_text, NULL)) AS pnc_register_no, ',
      'MAX(IF(o.concept_id=159893, o.value_numeric, NULL)) AS pnc_visit_no, ',
      'MAX(IF(o.concept_id=5599, o.value_datetime, NULL)) AS delivery_date, ',
      'MAX(IF(o.concept_id=5630, o.value_coded, NULL)) AS mode_of_delivery, ',
      'MAX(IF(o.concept_id=1572, o.value_coded, NULL)) AS place_of_delivery, ',
      'MAX(IF(o.concept_id=1724, o.value_coded, NULL)) AS visit_timing_mother, ',
      'MAX(IF(o.concept_id=167017, o.value_coded, NULL)) AS visit_timing_baby, ',
      'MAX(IF(o.concept_id=159949, o.value_coded, NULL)) AS delivery_outcome, ',
      'MAX(IF(o.concept_id=5088, o.value_numeric, NULL)) AS temperature, ',
      'MAX(IF(o.concept_id=5087, o.value_numeric, NULL)) AS pulse_rate, ',
      'MAX(IF(o.concept_id=5085, o.value_numeric, NULL)) AS systolic_bp, ',
      'MAX(IF(o.concept_id=5086, o.value_numeric, NULL)) AS diastolic_bp, ',
      'MAX(IF(o.concept_id=5242, o.value_numeric, NULL)) AS respiratory_rate, ',
      'MAX(IF(o.concept_id=5092, o.value_numeric, NULL)) AS oxygen_saturation, ',
      'MAX(IF(o.concept_id=5089, o.value_numeric, NULL)) AS weight, ',
      'MAX(IF(o.concept_id=5090, o.value_numeric, NULL)) AS height, ',
      'MAX(IF(o.concept_id=1343, o.value_numeric, NULL)) AS muac, ',
      'MAX(IF(o.concept_id=21, o.value_numeric, NULL)) AS hemoglobin, ',
      'MAX(IF(o.concept_id=1147, o.value_coded, NULL)) AS arv_status, ',
      'MAX(IF(o.concept_id=1856, o.value_coded, NULL)) AS general_condition, ',
      'MAX(IF(o.concept_id=159780, o.value_coded, NULL)) AS breast, ',
      'MAX(IF(o.concept_id=162128, o.value_coded, NULL)) AS cs_scar, ',
      'MAX(IF(o.concept_id=162110, o.value_coded, NULL)) AS gravid_uterus, ',
      'MAX(IF(o.concept_id=159840, o.value_coded, NULL)) AS episiotomy, ',
      'MAX(IF(o.concept_id=159844, o.value_coded, NULL)) AS lochia, ',
      'MAX(IF(o.concept_id=161651, o.value_coded, NULL)) AS counselled_on_infant_feeding, ',
      'MAX(IF(o.concept_id=5245, o.value_coded, NULL)) AS pallor, ',
      'MAX(IF(o.concept_id=162642, o.value_coded, NULL)) AS pallor_severity, ',
      'MAX(IF(o.concept_id=230, o.value_coded, NULL)) AS pph, ',
      'MAX(IF(o.concept_id=1396, o.value_coded, NULL)) AS mother_hiv_status, ',
      'MAX(IF(o.concept_id=162134, o.value_coded, NULL)) AS condition_of_baby, ',
      'MAX(IF(o.concept_id=1151, o.value_coded, NULL)) AS baby_feeding_method, ',
      'MAX(IF(o.concept_id=162121, o.value_coded, NULL)) AS umblical_cord, ',
      'MAX(IF(o.concept_id=162127, o.value_coded, NULL)) AS baby_immunization_started, ',
      'MAX(IF(o.concept_id=1382, o.value_coded, NULL)) AS family_planning_counseling, ',
      'MAX(IF(o.concept_id=160632, o.value_text, NULL)) AS other_maternal_complications, ',
      'MAX(IF(o.concept_id=163742, o.value_coded, NULL)) AS uterus_examination, ',
      'MAX(IF(o.concept_id=160968, o.value_text, NULL)) AS uterus_cervix_examination, ',
      'MAX(IF(o.concept_id=160969, o.value_text, NULL)) AS vaginal_examination, ',
      'MAX(IF(o.concept_id=160970, o.value_text, NULL)) AS parametrial_examination, ',
      'MAX(IF(o.concept_id=160971, o.value_text, NULL)) AS external_genitalia_examination, ',
      'MAX(IF(o.concept_id=160975, o.value_text, NULL)) AS ovarian_examination, ',
      'MAX(IF(o.concept_id=160972, o.value_text, NULL)) AS pelvic_lymph_node_exam, ',
      'MAX(IF(o.concept_id=159427, (CASE o.value_coded WHEN 703 THEN \"Positive\" WHEN 664 THEN \"Negative\" WHEN 1138 THEN \"Inconclusive\" ELSE \"\" END), NULL)) AS final_test_result, ',
      'MAX(IF(o.concept_id=299, o.value_coded, NULL)) AS syphilis_results, ',
      'MAX(IF(o.concept_id=164848, (CASE o.value_coded WHEN 1065 THEN \"Yes\" WHEN 1066 THEN \"No\" ELSE \"\" END), NULL)) AS patient_given_result, ',
      'MAX(IF(o.concept_id=165070, o.value_coded, NULL)) AS couple_counselled, ',
      'MAX(IF(o.concept_id=161557, o.value_coded, NULL)) AS partner_hiv_tested, ',
      'MAX(IF(o.concept_id=1436, o.value_coded, NULL)) AS partner_hiv_status, ',
      'MAX(IF(o.concept_id=165218, o.value_coded, NULL)) AS pnc_hiv_test_timing_mother, ',
      'MAX(IF(o.concept_id=163783, o.value_coded, NULL)) AS mother_haart_given, ',
      'MAX(IF(o.concept_id=1109, o.value_coded, NULL)) AS prophylaxis_given, ',
      'MAX(IF(o.concept_id=166665, o.value_coded, NULL)) AS infant_prophylaxis_timing, ',
      'MAX(IF(o.concept_id=1282 AND o.value_coded = 160123, o.value_coded, NULL)) AS baby_azt_dispensed, ',
      'MAX(IF(o.concept_id=1282 AND o.value_coded = 80586, o.value_coded, NULL)) AS baby_nvp_dispensed, ',
      'MAX(IF(o.concept_id=161074, o.value_coded, NULL)) AS pnc_exercises, ',
      'MAX(IF(o.concept_id=160085, o.value_coded, NULL)) AS maternal_condition, ',
      'MAX(IF(o.concept_id=161004, o.value_coded, NULL)) AS iron_supplementation, ',
      'MAX(IF(o.concept_id=159921, o.value_coded, NULL)) AS fistula_screening, ',
      'MAX(IF(o.concept_id=164934, o.value_coded, NULL)) AS cacx_screening, ',
      'MAX(IF(o.concept_id=163589, o.value_coded, NULL)) AS cacx_screening_method, ',
      'MAX(IF(o.concept_id=160653, o.value_coded, NULL)) AS family_planning_status, ',
      'CONCAT_WS('','', ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =160570, \"Emergency contraceptive pills\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =780, \"Oral Contraceptives Pills\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =5279, \"Injectible\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =1359, \"Implant\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =5275, \"Intrauterine Device\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =136163, \"Lactational Amenorhea Method\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =5278, \"Diaphram/Cervical Cap\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =5277, \"Fertility Awareness\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =1472, \"Tubal Ligation\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =190, \"Condoms\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =1489, \"Vasectomy\", '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=374 AND o.value_coded =162332, \"Undecided\", '''')), '''') ',
      ') AS family_planning_method, ',
      'MAX(IF(o.concept_id=160481, o.value_coded, NULL)) AS referred_from, ',
      'MAX(IF(o.concept_id=163145, o.value_coded, NULL)) AS referred_to, ',
      'MAX(IF(o.concept_id=164359, o.value_text, NULL)) AS referral_reason, ',
      'MAX(IF(o.concept_id=159395, o.value_text, NULL)) AS clinical_notes, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
        'AND o.concept_id IN (1646,159893,5599,5630,1572,5088,5087,5085,5086,5242,5092,5089,5090,1343,21,1147,1856,159780,162128,162110,159840,159844,5245,230,1396,162134,1151,162121,162127,1382,163742,160968,160969,160970,160971,160975,160972,159427,164848,161557,1436,1109,5576,159595,163784,1282,161074,160085,161004,159921,164934,163589,160653,374,160481,163145,159395,159949,5096,161651,165070,1724,167017,163783,162642,166665,165218,160632,299,159395) ',
      'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''72aa78e0-ee4b-47c3-9073-26f3b9ecc4a7'')) f ON f.form_id = e.form_id ',
    'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date), encounter_id=VALUES(encounter_id), provider=VALUES(provider), pnc_register_no=VALUES(pnc_register_no), ',
      'pnc_visit_no=VALUES(pnc_visit_no), delivery_date=VALUES(delivery_date), mode_of_delivery=VALUES(mode_of_delivery), place_of_delivery=VALUES(place_of_delivery), ',
      'visit_timing_mother=VALUES(visit_timing_mother), visit_timing_baby=VALUES(visit_timing_baby), delivery_outcome=VALUES(delivery_outcome), temperature=VALUES(temperature), ',
      'pulse_rate=VALUES(pulse_rate), systolic_bp=VALUES(systolic_bp), diastolic_bp=VALUES(diastolic_bp), respiratory_rate=VALUES(respiratory_rate), ',
      'oxygen_saturation=VALUES(oxygen_saturation), weight=VALUES(weight), height=VALUES(height), muac=VALUES(muac), hemoglobin=VALUES(hemoglobin), ',
      'arv_status=VALUES(arv_status), general_condition=VALUES(general_condition), breast=VALUES(breast), cs_scar=VALUES(cs_scar), gravid_uterus=VALUES(gravid_uterus), ',
      'episiotomy=VALUES(episiotomy), lochia=VALUES(lochia), pallor=VALUES(pallor), pph=VALUES(pph), mother_hiv_status=VALUES(mother_hiv_status), ',
      'condition_of_baby=VALUES(condition_of_baby), baby_feeding_method=VALUES(baby_feeding_method), umblical_cord=VALUES(umblical_cord), baby_immunization_started=VALUES(baby_immunization_started), ',
      'family_planning_counseling=VALUES(family_planning_counseling), uterus_examination=VALUES(uterus_examination), uterus_cervix_examination=VALUES(uterus_cervix_examination), ',
      'vaginal_examination=VALUES(vaginal_examination), parametrial_examination=VALUES(parametrial_examination), external_genitalia_examination=VALUES(external_genitalia_examination), ',
      'ovarian_examination=VALUES(ovarian_examination), pelvic_lymph_node_exam=VALUES(pelvic_lymph_node_exam), final_test_result=VALUES(final_test_result), ',
      'patient_given_result=VALUES(patient_given_result), couple_counselled=VALUES(couple_counselled), partner_hiv_tested=VALUES(partner_hiv_tested), ',
      'partner_hiv_status=VALUES(partner_hiv_status), mother_haart_given=VALUES(mother_haart_given), prophylaxis_given=VALUES(prophylaxis_given), ',
      'infant_prophylaxis_timing=VALUES(infant_prophylaxis_timing), baby_azt_dispensed=VALUES(baby_azt_dispensed), baby_nvp_dispensed=VALUES(baby_nvp_dispensed), ',
      'maternal_condition=VALUES(maternal_condition), iron_supplementation=VALUES(iron_supplementation), fistula_screening=VALUES(fistula_screening), ',
      'cacx_screening=VALUES(cacx_screening), cacx_screening_method=VALUES(cacx_screening_method), family_planning_status=VALUES(family_planning_status), ',
      'family_planning_method=VALUES(family_planning_method), referred_from=VALUES(referred_from), referred_to=VALUES(referred_to), referral_reason=VALUES(referral_reason), ',
      'clinical_notes=VALUES(clinical_notes), appointment_date=VALUES(appointment_date), counselled_on_infant_feeding=VALUES(counselled_on_infant_feeding), ',
      'pnc_hiv_test_timing_mother=VALUES(pnc_hiv_test_timing_mother), other_maternal_complications=VALUES(other_maternal_complications), syphilis_results=VALUES(syphilis_results);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT "Completed processing MCH postnatal visits ", CONCAT("Time: ", NOW());
END $$

-- ------------- update etl_hei_enrollment-------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_hei_enrolment $$
CREATE PROCEDURE sp_update_etl_hei_enrolment(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_hei_enrollment');
SELECT "Processing HEI Enrollments", CONCAT("Time: ", NOW());

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, child_exposed, ',
      'spd_number, birth_weight, gestation_at_birth, birth_type, date_first_seen, birth_notification_number, ',
      'birth_certificate_number, need_for_special_care, reason_for_special_care, referral_source, transfer_in, ',
      'transfer_in_date, facility_transferred_from, district_transferred_from, date_first_enrolled_in_hei_care, ',
      'mother_breastfeeding, TB_contact_history_in_household, mother_alive, mother_on_pmtct_drugs, mother_on_drug, ',
      'mother_on_art_at_infant_enrollment, mother_drug_regimen, infant_prophylaxis, parent_ccc_number, mode_of_delivery, ',
      'place_of_delivery, birth_length, birth_order, health_facility_name, date_of_birth_notification, ',
      'date_of_birth_registration, birth_registration_place, permanent_registration_serial, mother_facility_registered, ',
      'exit_date, exit_reason, hiv_status_at_exit, encounter_type, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id=5303, o.value_coded, NULL)) AS child_exposed, ',
      'MAX(IF(o.concept_id=162054, o.value_text, NULL)) AS spd_number, ',
      'MAX(IF(o.concept_id=5916, o.value_numeric, NULL)) AS birth_weight, ',
      'MAX(IF(o.concept_id=1409, o.value_numeric, NULL)) AS gestation_at_birth, ',
      'MAX(IF(o.concept_id=159949, o.value_coded, NULL)) AS birth_type, ',
      'MAX(IF(o.concept_id=162140, o.value_datetime, NULL)) AS date_first_seen, ',
      'MAX(IF(o.concept_id=162051, o.value_text, NULL)) AS birth_notification_number, ',
      'MAX(IF(o.concept_id=162052, o.value_text, NULL)) AS birth_certificate_number, ',
      'MAX(IF(o.concept_id=161630, o.value_coded, NULL)) AS need_for_special_care, ',
      'MAX(IF(o.concept_id=161601, o.value_coded, NULL)) AS reason_for_special_care, ',
      'MAX(IF(o.concept_id=160540, o.value_coded, NULL)) AS referral_source, ',
      'MAX(IF(o.concept_id=160563, o.value_coded, NULL)) AS transfer_in, ',
      'MAX(IF(o.concept_id=160534, o.value_datetime, NULL)) AS transfer_in_date, ',
      'MAX(IF(o.concept_id=160535, o.value_text, NULL)) AS facility_transferred_from, ',
      'MAX(IF(o.concept_id=161551, o.value_text, NULL)) AS district_transferred_from, ',
      'MAX(IF(o.concept_id=160555, o.value_datetime, NULL)) AS date_first_enrolled_in_hei_care, ',
      'MAX(IF(o.concept_id=159941, o.value_coded, NULL)) AS mother_breastfeeding, ',
      'MAX(IF(o.concept_id=152460, o.value_coded, NULL)) AS TB_contact_history_in_household, ',
      'MAX(IF(o.concept_id=160429, o.value_coded, NULL)) AS mother_alive, ',
      'MAX(IF(o.concept_id=1148, o.value_coded, NULL)) AS mother_on_pmtct_drugs, ',
      'MAX(IF(o.concept_id=1086, o.value_coded, NULL)) AS mother_on_drug, ',
      'MAX(IF(o.concept_id=162055, o.value_coded, NULL)) AS mother_on_art_at_infant_enrollment, ',
      'MAX(IF(o.concept_id=1088, o.value_coded, NULL)) AS mother_drug_regimen, ',
      'MAX(IF(o.concept_id=1282, o.value_coded, NULL)) AS infant_prophylaxis, ',
      'MAX(IF(o.concept_id=162053, o.value_numeric, NULL)) AS parent_ccc_number, ',
      'MAX(IF(o.concept_id=5630, o.value_coded, NULL)) AS mode_of_delivery, ',
      'MAX(IF(o.concept_id=1572, o.value_coded, NULL)) AS place_of_delivery, ',
      'MAX(IF(o.concept_id=1503, o.value_numeric, NULL)) AS birth_length, ',
      'MAX(IF(o.concept_id=163460, o.value_numeric, NULL)) AS birth_order, ',
      'MAX(IF(o.concept_id=162724, o.value_text, NULL)) AS health_facility_name, ',
      'MAX(IF(o.concept_id=164130, o.value_datetime, NULL)) AS date_of_birth_notification, ',
      'MAX(IF(o.concept_id=164129, o.value_datetime, NULL)) AS date_of_birth_registration, ',
      'MAX(IF(o.concept_id=164140, o.value_text, NULL)) AS birth_registration_place, ',
      'MAX(IF(o.concept_id=1646, o.value_text, NULL)) AS permanent_registration_serial, ',
      'MAX(IF(o.concept_id=162724, o.value_text, NULL)) AS mother_facility_registered, ',
      'MAX(IF(o.concept_id=160753, o.value_datetime, NULL)) AS exit_date, ',
      'MAX(IF(o.concept_id=161555, o.value_coded, NULL)) AS exit_reason, ',
      'MAX(IF(o.concept_id=159427, (CASE o.value_coded WHEN 703 THEN \"Positive\" WHEN 664 THEN \"Negative\" WHEN 1138 THEN \"Inconclusive\" ELSE \"\" END), NULL)) AS hiv_status_at_exit, ',
      'CASE et.uuid WHEN ''01894f88-dc73-42d4-97a3-0929118403fb'' THEN ''MCHCS_HEI_COMPLETION'' WHEN ''415f5136-ca4a-49a8-8db3-f994187c3af6'' THEN ''MCHCS_HEI_ENROLLMENT'' END AS encounter_type, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
        'AND o.concept_id IN (5303,162054,5916,1409,162140,162051,162052,161630,161601,160540,160563,160534,160535,161551,160555,1282,159941,1282,152460,160429,1148,1086,162055,1088,1282,162053,5630,1572,161555,159427,1503,163460,162724,164130,164129,164140,1646,160753,161555,159427,159949) ',
      'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''415f5136-ca4a-49a8-8db3-f994187c3af6'',''01894f88-dc73-42d4-97a3-0929118403fb'')) et ON et.encounter_type_id = e.encounter_type ',
    'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, visit_date ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider), visit_id=VALUES(visit_id), visit_date=VALUES(visit_date), child_exposed=VALUES(child_exposed), ',
      'spd_number=VALUES(spd_number), birth_weight=VALUES(birth_weight), gestation_at_birth=VALUES(gestation_at_birth), date_first_seen=VALUES(date_first_seen), ',
      'birth_notification_number=VALUES(birth_notification_number), birth_certificate_number=VALUES(birth_certificate_number), need_for_special_care=VALUES(need_for_special_care), ',
      'reason_for_special_care=VALUES(reason_for_special_care), referral_source=VALUES(referral_source), transfer_in=VALUES(transfer_in), transfer_in_date=VALUES(transfer_in_date), ',
      'facility_transferred_from=VALUES(facility_transferred_from), district_transferred_from=VALUES(district_transferred_from), date_first_enrolled_in_hei_care=VALUES(date_first_enrolled_in_hei_care), ',
      'mother_breastfeeding=VALUES(mother_breastfeeding), TB_contact_history_in_household=VALUES(TB_contact_history_in_household), mother_alive=VALUES(mother_alive), ',
      'mother_on_pmtct_drugs=VALUES(mother_on_pmtct_drugs), mother_on_drug=VALUES(mother_on_drug), mother_on_art_at_infant_enrollment=VALUES(mother_on_art_at_infant_enrollment), ',
      'mother_drug_regimen=VALUES(mother_drug_regimen), infant_prophylaxis=VALUES(infant_prophylaxis), parent_ccc_number=VALUES(parent_ccc_number), ',
      'mode_of_delivery=VALUES(mode_of_delivery), place_of_delivery=VALUES(place_of_delivery), birth_length=VALUES(birth_length), birth_order=VALUES(birth_order), ',
      'health_facility_name=VALUES(health_facility_name), date_of_birth_notification=VALUES(date_of_birth_notification), date_of_birth_registration=VALUES(date_of_birth_registration), ',
      'birth_registration_place=VALUES(birth_registration_place), permanent_registration_serial=VALUES(permanent_registration_serial), mother_facility_registered=VALUES(mother_facility_registered), ',
      'exit_date=VALUES(exit_date), exit_reason=VALUES(exit_reason), hiv_status_at_exit=VALUES(hiv_status_at_exit), birth_type=VALUES(birth_type);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT "Completed processing HEI Enrollments", CONCAT("Time: ", NOW());
END $$


-- ------------- update etl_hei_follow_up_visit-------------------------


DROP PROCEDURE IF EXISTS sp_populate_etl_hei_follow_up $$
CREATE PROCEDURE sp_populate_etl_hei_follow_up()
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_hei_follow_up_visit');

SELECT "Processing HEI Followup visits", CONCAT("Time: ", NOW());

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
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



-- ------------- update etl_hei_immunization-------------------------


DROP PROCEDURE IF EXISTS sp_populate_etl_hei_immunization $$
CREATE PROCEDURE sp_populate_etl_hei_immunization()
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_immunization');

SELECT "Processing hei_immunization data ", CONCAT("Time: ", NOW());

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
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


-- ------------- update etl_tb_enrollment-------------------------

-- sql
;
DROP PROCEDURE IF EXISTS sp_update_etl_tb_enrollment $$
CREATE PROCEDURE sp_update_etl_tb_enrollment(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_tb_enrollment');

SELECT "Processing TB Enrollments ", CONCAT("Time: ", NOW());

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, location_id, encounter_id, ',
      'date_treatment_started, district, referred_by, referral_date, date_transferred_in, ',
      'facility_transferred_from, district_transferred_from, date_first_enrolled_in_tb_care, ',
      'weight, height, treatment_supporter, relation_to_patient, treatment_supporter_address, ',
      'treatment_supporter_phone_contact, disease_classification, patient_classification, ',
      'pulmonary_smear_result, has_extra_pulmonary_pleurial_effusion, has_extra_pulmonary_milliary, ',
      'has_extra_pulmonary_lymph_node, has_extra_pulmonary_menengitis, has_extra_pulmonary_skeleton, ',
      'has_extra_pulmonary_abdominal, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id=1113, o.value_datetime, NULL)) AS date_treatment_started, ',
      'MAX(IF(o.concept_id=161564, LEFT(TRIM(o.value_text),100), NULL)) AS district, ',
      'MAX(IF(o.concept_id=160540, o.value_coded, NULL)) AS referred_by, ',
      'MAX(IF(o.concept_id=161561, o.value_datetime, NULL)) AS referral_date, ',
      'MAX(IF(o.concept_id=160534, o.value_datetime, NULL)) AS date_transferred_in, ',
      'MAX(IF(o.concept_id=160535, LEFT(TRIM(o.value_text),100), NULL)) AS facility_transferred_from, ',
      'MAX(IF(o.concept_id=161551, LEFT(TRIM(o.value_text),100), NULL)) AS district_transferred_from, ',
      'MAX(IF(o.concept_id=161552, o.value_datetime, NULL)) AS date_first_enrolled_in_tb_care, ',
      'MAX(IF(o.concept_id=5089, o.value_numeric, NULL)) AS weight, ',
      'MAX(IF(o.concept_id=5090, o.value_numeric, NULL)) AS height, ',
      'MAX(IF(o.concept_id=160638, LEFT(TRIM(o.value_text),100), NULL)) AS treatment_supporter, ',
      'MAX(IF(o.concept_id=160640, o.value_coded, NULL)) AS relation_to_patient, ',
      'MAX(IF(o.concept_id=160641, LEFT(TRIM(o.value_text),100), NULL)) AS treatment_supporter_address, ',
      'MAX(IF(o.concept_id=160642, LEFT(TRIM(o.value_text),100), NULL)) AS treatment_supporter_phone_contact, ',
      'MAX(IF(o.concept_id=160040, o.value_coded, NULL)) AS disease_classification, ',
      'MAX(IF(o.concept_id=159871, o.value_coded, NULL)) AS patient_classification, ',
      'MAX(IF(o.concept_id=159982, o.value_coded, NULL)) AS pulmonary_smear_result, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=130059, o.value_coded, NULL)) AS has_extra_pulmonary_pleurial_effusion, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=115753, o.value_coded, NULL)) AS has_extra_pulmonary_milliary, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=111953, o.value_coded, NULL)) AS has_extra_pulmonary_lymph_node, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=111967, o.value_coded, NULL)) AS has_extra_pulmonary_menengitis, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=112116, o.value_coded, NULL)) AS has_extra_pulmonary_skeleton, ',
      'MAX(IF(o.concept_id=161356 AND o.value_coded=1350, o.value_coded, NULL)) AS has_extra_pulmonary_abdominal, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'LEFT JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
    'AND o.concept_id IN (160540,161561,160534,160535,161551,161552,5089,5090,160638,160640,160641,160642,160040,159871,159982,161356) ',
    'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''9d8498a4-372d-4dc4-a809-513a2434621e'')) et ',
    'ON et.encounter_type_id = e.encounter_type ',
    'WHERE (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider),visit_id=VALUES(visit_id),visit_date=VALUES(visit_date),encounter_id=VALUES(encounter_id),',
      'date_treatment_started=VALUES(date_treatment_started),district=VALUES(district),referred_by=VALUES(referred_by),referral_date=VALUES(referral_date),',
      'date_transferred_in=VALUES(date_transferred_in),facility_transferred_from=VALUES(facility_transferred_from),',
      'district_transferred_from=VALUES(district_transferred_from),date_first_enrolled_in_tb_care=VALUES(date_first_enrolled_in_tb_care),',
      'weight=VALUES(weight),height=VALUES(height),treatment_supporter=VALUES(treatment_supporter),relation_to_patient=VALUES(relation_to_patient),',
      'treatment_supporter_address=VALUES(treatment_supporter_address),treatment_supporter_phone_contact=VALUES(treatment_supporter_phone_contact),',
      'disease_classification=VALUES(disease_classification),patient_classification=VALUES(patient_classification),',
      'pulmonary_smear_result=VALUES(pulmonary_smear_result),has_extra_pulmonary_pleurial_effusion=VALUES(has_extra_pulmonary_pleurial_effusion),',
      'has_extra_pulmonary_milliary=VALUES(has_extra_pulmonary_milliary),has_extra_pulmonary_lymph_node=VALUES(has_extra_pulmonary_lymph_node),',
      'has_extra_pulmonary_menengitis=VALUES(has_extra_pulmonary_menengitis),has_extra_pulmonary_skeleton=VALUES(has_extra_pulmonary_skeleton),',
      'has_extra_pulmonary_abdominal=VALUES(has_extra_pulmonary_abdominal);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT "Completed processing TB Enrollments ", CONCAT("Time: ", NOW());
END $$

-- ------------- update etl_tb_follow_up_visit-------------------------

-- sql
;
DROP PROCEDURE IF EXISTS sp_update_etl_tb_follow_up_visit $$
CREATE PROCEDURE sp_update_etl_tb_follow_up_visit(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_tb_follow_up_visit');

SELECT "Processing TB Followup visits ", CONCAT("Time: ", NOW());

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
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
      'MAX(IF(o.concept_id=159956 AND o.value_coded=84360,o.value_numeric,NULL)) AS resistant_s, ',
      'MAX(IF(o.concept_id=159956 AND o.value_coded=767,TRIM(o.value_text),NULL)) AS resistant_r, ',
      'MAX(IF(o.concept_id=159956 AND o.value_coded=78280,o.value_coded,NULL)) AS resistant_inh, ',
      'MAX(IF(o.concept_id=159956 AND o.value_coded=75948,TRIM(o.value_text),NULL)) AS resistant_e, ',
      'MAX(IF(o.concept_id=159958 AND o.value_coded=84360,TRIM(o.value_text),NULL)) AS sensitive_s, ',
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
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider),visit_id=VALUES(visit_id),visit_date=VALUES(visit_date),encounter_id=VALUES(encounter_id),',
      'spatum_test=VALUES(spatum_test),spatum_result=VALUES(spatum_result),result_serial_number=VALUES(result_serial_number),quantity=VALUES(quantity),',
      'date_test_done=VALUES(date_test_done),bacterial_colonie_growth=VALUES(bacterial_colonie_growth),number_of_colonies=VALUES(number_of_colonies),',
      'resistant_s=VALUES(resistant_s),resistant_r=VALUES(resistant_r),resistant_inh=VALUES(resistant_inh),resistant_e=VALUES(resistant_e),',
      'sensitive_s=VALUES(sensitive_s),sensitive_r=VALUES(sensitive_r),sensitive_inh=VALUES(sensitive_inh),sensitive_e=VALUES(sensitive_e),',
      'test_date=VALUES(test_date),hiv_status=VALUES(hiv_status);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT "Completed processing TB Followup visits ", CONCAT("Time: ", NOW());
END $$


-- ------------- update etl_tb_screening-------------------------

-- sql
DROP PROCEDURE IF EXISTS sp_update_etl_tb_screening $$
CREATE PROCEDURE sp_update_etl_tb_screening(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_tb_screening');

SELECT "Processing TB Screening data ", CONCAT("Time: ", NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, encounter_id, location_id, ',
      'cough_for_2wks_or_more, confirmed_tb_contact, fever_for_2wks_or_more, noticeable_weight_loss, ',
      'night_sweat_for_2wks_or_more, lethargy, spatum_smear_ordered, chest_xray_ordered, genexpert_ordered, ',
      'spatum_smear_result, chest_xray_result, genexpert_result, referral, clinical_tb_diagnosis, resulting_tb_status, ',
      'contact_invitation, evaluated_for_ipt, started_anti_TB, tb_treatment_start_date, tb_prophylaxis, notes, ',
      'person_present, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.encounter_id, e.location_id, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded = 159799, o.value_coded, NULL)) AS cough_for_2wks_or_more, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded = 124068, o.value_coded, NULL)) AS confirmed_tb_contact, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded = 1494, o.value_coded, NULL)) AS fever_for_2wks_or_more, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded = 832, o.value_coded, NULL)) AS noticeable_weight_loss, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded = 133027, o.value_coded, NULL)) AS night_sweat_for_2wks_or_more, ',
      'MAX(IF(o.concept_id=1729 AND o.value_coded = 116334, o.value_coded, NULL)) AS lethargy, ',
      'MAX(IF(o.concept_id=1271 AND o.value_coded = 307, o.value_coded, NULL)) AS spatum_smear_ordered, ',
      'MAX(IF(o.concept_id=1271 AND o.value_coded = 12, o.value_coded, NULL)) AS chest_xray_ordered, ',
      'MAX(IF(o.concept_id=1271 AND o.value_coded = 162202, o.value_coded, NULL)) AS genexpert_ordered, ',
      'MAX(IF(o.concept_id=307, o.value_coded, NULL)) AS spatum_smear_result, ',
      'MAX(IF(o.concept_id=12, o.value_coded, NULL)) AS chest_xray_result, ',
      'MAX(IF(o.concept_id=162202, o.value_coded, NULL)) AS genexpert_result, ',
      'MAX(IF(o.concept_id=1272, o.value_coded, NULL)) AS referral, ',
      'MAX(IF(o.concept_id=163752, o.value_coded, NULL)) AS clinical_tb_diagnosis, ',
      'MAX(IF(o.concept_id=1659, o.value_coded, NULL)) AS resulting_tb_status, ',
      'MAX(IF(o.concept_id=163414, o.value_coded, NULL)) AS contact_invitation, ',
      'MAX(IF(o.concept_id=162275, o.value_coded, NULL)) AS evaluated_for_ipt, ',
      'MAX(IF(o.concept_id=162309, o.value_coded, NULL)) AS started_anti_TB, ',
      'MAX(IF(o.concept_id=1113, DATE(o.value_datetime), NULL)) AS tb_treatment_start_date, ',
      'MAX(IF(o.concept_id=1109, o.value_coded, NULL)) AS tb_prophylaxis, ',
      'MAX(IF(o.concept_id=160632, o.value_text, NULL)) AS notes, ',
      'MAX(IF(o.concept_id=161643, o.value_coded, NULL)) AS person_present, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (',
      '''22c68f86-bbf0-49ba-b2d1-23fa7ccf0259'', ',
      '''59ed8e62-7f1f-40ae-a2e3-eabe350277ce'', ',
      '''23b4ebbd-29ad-455e-be0e-04aa6bc30798'', ',
      '''72aa78e0-ee4b-47c3-9073-26f3b9ecc4a7''',
    ') ',
    'INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (1659,1113,160632,161643,1729,1271,307,12,162202,1272,163752,163414,162275,162309,1109) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.patient_id, visit_date ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider), visit_id=VALUES(visit_id), visit_date=VALUES(visit_date), encounter_id=VALUES(encounter_id), ',
      'cough_for_2wks_or_more=VALUES(cough_for_2wks_or_more), confirmed_tb_contact=VALUES(confirmed_tb_contact), ',
      'fever_for_2wks_or_more=VALUES(fever_for_2wks_or_more), noticeable_weight_loss=VALUES(noticeable_weight_loss), ',
      'night_sweat_for_2wks_or_more=VALUES(night_sweat_for_2wks_or_more), lethargy=VALUES(lethargy), ',
      'spatum_smear_ordered=VALUES(spatum_smear_ordered), chest_xray_ordered=VALUES(chest_xray_ordered), genexpert_ordered=VALUES(genexpert_ordered), ',
      'spatum_smear_result=VALUES(spatum_smear_result), chest_xray_result=VALUES(chest_xray_result), genexpert_result=VALUES(genexpert_result), ',
      'referral=VALUES(referral), clinical_tb_diagnosis=VALUES(clinical_tb_diagnosis), resulting_tb_status=VALUES(resulting_tb_status), ',
      'contact_invitation=VALUES(contact_invitation), evaluated_for_ipt=VALUES(evaluated_for_ipt), started_anti_TB=VALUES(started_anti_TB), ',
      'tb_treatment_start_date=VALUES(tb_treatment_start_date), tb_prophylaxis=VALUES(tb_prophylaxis), notes=VALUES(notes);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT "Completed processing TB Screening data ", CONCAT("Time: ", NOW());
END $$

-- ------------------------------ update drug event -------------------------------------

sql
DROP PROCEDURE IF EXISTS sp_update_drug_event $$
CREATE PROCEDURE sp_update_drug_event(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_drug_event');
SELECT 'Processing Drug Event Data', CONCAT('Time: ', NOW());
SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, patient_id, date_started, visit_date, provider, encounter_id, program, regimen, regimen_name, regimen_line, discontinued, regimen_stopped, regimen_discontinued, date_discontinued, reason_discontinued, reason_discontinued_other, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.uuid, e.patient_id, DATE(e.encounter_datetime) AS date_started, DATE(e.encounter_datetime) AS visit_date, e.creator AS provider, e.encounter_id, ',
      'MAX(IF(o.concept_id=1255, ''HIV'', IF(o.concept_id=1268, ''TB'', NULL))) AS program, ',
      'MAX(IF(o.concept_id=1193, (CASE o.value_coded ',
        'WHEN 162565 THEN ''3TC/NVP/TDF'' ',
        'WHEN 164505 THEN ''TDF/3TC/EFV'' ',
        'WHEN 1652 THEN ''AZT/3TC/NVP'' ',
        'WHEN 160124 THEN ''AZT/3TC/EFV'' ',
        'WHEN 792 THEN ''D4T/3TC/NVP'' ',
        'WHEN 160104 THEN ''D4T/3TC/EFV'' ',
        'WHEN 164971 THEN ''TDF/3TC/AZT'' ',
        'WHEN 164968 THEN ''AZT/3TC/DTG'' ',
        'WHEN 164969 THEN ''TDF/3TC/DTG'' ',
        'WHEN 164970 THEN ''ABC/3TC/DTG'' ',
        'WHEN 162561 THEN ''AZT/3TC/LPV/r'' ',
        'WHEN 164511 THEN ''AZT/3TC/ATV/r'' ',
        'WHEN 162201 THEN ''TDF/3TC/LPV/r'' ',
        'WHEN 164512 THEN ''TDF/3TC/ATV/r'' ',
        'WHEN 162560 THEN ''D4T/3TC/LPV/r'' ',
        'WHEN 164972 THEN ''AZT/TDF/3TC/LPV/r'' ',
        'WHEN 164973 THEN ''ETR/RAL/DRV/RTV'' ',
        'WHEN 164974 THEN ''ETR/TDF/3TC/LPV/r'' ',
        'WHEN 162200 THEN ''ABC/3TC/LPV/r'' ',
        'WHEN 162199 THEN ''ABC/3TC/NVP'' ',
        'WHEN 162563 THEN ''ABC/3TC/EFV'' ',
        'WHEN 817 THEN ''AZT/3TC/ABC'' ',
        'WHEN 164975 THEN ''D4T/3TC/ABC'' ',
        'WHEN 162562 THEN ''TDF/ABC/LPV/r'' ',
        'WHEN 162559 THEN ''ABC/DDI/LPV/r'' ',
        'WHEN 164976 THEN ''ABC/TDF/3TC/LPV/r'' ',
        'WHEN 165375 THEN ''RAL+3TC+DRV+RTV'' ',
        'WHEN 165376 THEN ''RAL+3TC+DRV+RTV+AZT'' ',
        'WHEN 165377 THEN ''RAL+3TC+DRV+RTV+ABC'' ',
        'WHEN 165378 THEN ''ETV+3TC+DRV+RTV'' ',
        'WHEN 165379 THEN ''RAL+3TC+DRV+RTV+TDF'' ',
        'WHEN 165369 THEN ''TDF+3TC+DTG+DRV/r'' ',
        'WHEN 165370 THEN ''TDF+3TC+RAL+DRV/r'' ',
        'WHEN 165371 THEN ''TDF+3TC+DTG+EFV+DRV/r'' ',
        'WHEN 165372 THEN ''ABC+3TC+RAL'' ',
        'WHEN 165373 THEN ''AZT+3TC+RAL+DRV/r'' ',
        'WHEN 165374 THEN ''ABC+3TC+RAL+DRV/r'' ',
        'WHEN 167442 THEN ''AZT/3TC/DTG/DRV/r'' ',
        'WHEN 1675 THEN ''RHZE'' ',
        'WHEN 768 THEN ''RHZ'' ',
        'WHEN 1674 THEN ''SRHZE'' ',
        'WHEN 164978 THEN ''RfbHZE'' ',
        'WHEN 164979 THEN ''RfbHZ'' ',
        'WHEN 164980 THEN ''SRfbHZE'' ',
        'WHEN 84360 THEN ''S (1 gm vial)'' ',
        'WHEN 75948 THEN ''E'' ',
        'WHEN 1194 THEN ''RH'' ',
        'WHEN 159851 THEN ''RHE'' ',
        'WHEN 1108 THEN ''EH'' ',
        'WHEN 2001184 THEN ''TAF/3TC/DTG'' ',
        'ELSE '''' END), NULL)) AS regimen, ',
      'MAX(IF(o.concept_id=1193, (CASE o.value_coded ',
        'WHEN 162565 THEN ''3TC+NVP+TDF'' ',
        'WHEN 164505 THEN ''TDF+3TC+EFV'' ',
        'WHEN 1652 THEN ''AZT+3TC+NVP'' ',
        'WHEN 160124 THEN ''AZT+3TC+EFV'' ',
        'WHEN 792 THEN ''D4T+3TC+NVP'' ',
        'WHEN 160104 THEN ''D4T+3TC+EFV'' ',
        'WHEN 164971 THEN ''TDF+3TC+AZT'' ',
        'WHEN 164968 THEN ''AZT+3TC+DTG'' ',
        'WHEN 164969 THEN ''TDF+3TC+DTG'' ',
        'WHEN 164970 THEN ''ABC+3TC+DTG'' ',
        'WHEN 162561 THEN ''AZT+3TC+LPV/r'' ',
        'WHEN 164511 THEN ''AZT+3TC+ATV/r'' ',
        'WHEN 162201 THEN ''TDF+3TC+LPV/r'' ',
        'WHEN 164512 THEN ''TDF+3TC+ATV/r'' ',
        'WHEN 162560 THEN ''D4T+3TC+LPV/r'' ',
        'WHEN 164972 THEN ''AZT+TDF+3TC+LPV/r'' ',
        'WHEN 164973 THEN ''ETR+RAL+DRV+RTV'' ',
        'WHEN 164974 THEN ''ETR+TDF+3TC+LPV/r'' ',
        'WHEN 162200 THEN ''ABC+3TC+LPV/r'' ',
        'WHEN 162199 THEN ''ABC+3TC+NVP'' ',
        'WHEN 162563 THEN ''ABC+3TC+EFV'' ',
        'WHEN 817 THEN ''AZT+3TC+ABC'' ',
        'WHEN 164975 THEN ''D4T+3TC+ABC'' ',
        'WHEN 162562 THEN ''TDF+ABC+LPV/r'' ',
        'WHEN 162559 THEN ''ABC+DDI+LPV/r'' ',
        'WHEN 164976 THEN ''ABC+TDF+3TC+LPV/r'' ',
        'WHEN 165375 THEN ''RAL+3TC+DRV+RTV'' ',
        'WHEN 165376 THEN ''RAL+3TC+DRV+RTV+AZT'' ',
        'WHEN 165377 THEN ''RAL+3TC+DRV+RTV+ABC'' ',
        'WHEN 165378 THEN ''ETV+3TC+DRV+RTV'' ',
        'WHEN 165379 THEN ''RAL+3TC+DRV+RTV+TDF'' ',
        'WHEN 165369 THEN ''TDF+3TC+DTG+DRV/r'' ',
        'WHEN 165370 THEN ''TDF+3TC+RAL+DRV/r'' ',
        'WHEN 165371 THEN ''TDF+3TC+DTG+EFV+DRV/r'' ',
        'WHEN 165372 THEN ''ABC+3TC+RAL'' ',
        'WHEN 165373 THEN ''AZT+3TC+RAL+DRV/r'' ',
        'WHEN 165374 THEN ''ABC+3TC+RAL+DRV/r'' ',
        'WHEN 167442 THEN ''AZT/3TC/DTG/DRV/r'' ',
        'WHEN 1675 THEN ''RHZE'' ',
        'WHEN 768 THEN ''RHZ'' ',
        'WHEN 1674 THEN ''SRHZE'' ',
        'WHEN 164978 THEN ''RfbHZE'' ',
        'WHEN 164979 THEN ''RfbHZ'' ',
        'WHEN 164980 THEN ''SRfbHZE'' ',
        'WHEN 84360 THEN ''S (1 gm vial)'' ',
        'WHEN 75948 THEN ''E'' ',
        'WHEN 1194 THEN ''RH'' ',
        'WHEN 159851 THEN ''RHE'' ',
        'WHEN 1108 THEN ''EH'' ',
        'WHEN 164968 THEN ''AZT/3TC/DTG'' ',
        'WHEN 2001184 THEN ''TAF/3TC/DTG'' ',
        'ELSE '''' END), NULL)) AS regimen_name, ',
      'MAX(IF(o.concept_id=163104, (CASE o.value_text ',
        'WHEN ''AF'' THEN ''First line'' ',
        'WHEN ''AS'' THEN ''Second line'' ',
        'WHEN ''AT'' THEN ''Third line'' ',
        'WHEN ''CF'' THEN ''First line'' ',
        'WHEN ''CS'' THEN ''Second line'' ',
        'WHEN ''CT'' THEN ''Third line'' ',
        'ELSE '''' END), NULL)) AS regimen_line, ',
      'MAX(IF(o.concept_id=1191, (CASE WHEN o.value_datetime IS NULL THEN 0 ELSE 1 END), NULL)) AS discontinued, ',
      'MAX(IF(o.concept_id=1255 AND o.value_coded=1260, o.value_coded, NULL)) AS regimen_stopped, ',
      'NULL AS regimen_discontinued, ',
      'MAX(IF(o.concept_id=1191, o.value_datetime, NULL)) AS date_discontinued, ',
      'MAX(IF(o.concept_id=1252, o.value_coded, NULL)) AS reason_discontinued, ',
      'MAX(IF(o.concept_id=5622, o.value_text, NULL)) AS reason_discontinued_other, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 ',
        'AND o.concept_id IN (1193,1252,5622,1191,1255,1268,163104) ',
      'INNER JOIN (SELECT encounter_type, uuid, name FROM form WHERE uuid IN (''da687480-e197-11e8-9f32-f2801f1b9fd1'')) f ON f.encounter_type = e.encounter_type ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'date_started = VALUES(date_started), ',
      'regimen = VALUES(regimen), ',
      'regimen_name = VALUES(regimen_name), ',
      'regimen_line = VALUES(regimen_line), ',
      'discontinued = VALUES(discontinued), ',
      'regimen_stopped = VALUES(regimen_stopped), ',
      'regimen_discontinued = VALUES(regimen_discontinued), ',
      'date_discontinued = VALUES(date_discontinued), ',
      'reason_discontinued = VALUES(reason_discontinued), ',
      'reason_discontinued_other = VALUES(reason_discontinued_other), ',
      'date_last_modified = VALUES(date_last_modified);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing Drug Event Data', CONCAT('Time: ', NOW());
END $$


-- ---------------------------- Update HTS encounters ---------------------



DROP PROCEDURE IF EXISTS sp_update_hts_test $$
CREATE PROCEDURE sp_update_hts_test(IN last_update_time DATETIME)
BEGIN
    DECLARE target_table VARCHAR(300);
    DECLARE last_ts_str VARCHAR(100);

    -- 1. Initialize tenant variables
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_hts_test');

    -- 2. Handle the timestamp for the WHERE clause
    SET last_ts_str = IFNULL(CONCAT('''', last_update_time, ''''), '''1900-01-01 00:00:00''');

    -- 3. Prepare and Execute the dynamic SQL
    SET @sql_stmt = CONCAT(
        'INSERT INTO ', @target_table, ' (
            patient_id, visit_id, encounter_id, encounter_uuid, encounter_location, creator, date_created, date_last_modified,
            visit_date, test_type, population_type, key_population_type, priority_population_type, ever_tested_for_hiv, months_since_last_test,
            patient_disabled, disability_type, patient_consented, client_tested_as, setting, approach, test_strategy, hts_entry_point,
            hts_risk_category, hts_risk_score, test_1_kit_name, test_1_kit_lot_no, test_1_kit_expiry, test_1_result, test_2_kit_name,
            test_2_kit_lot_no, test_2_kit_expiry, test_2_result, test_3_kit_name, test_3_kit_lot_no, test_3_kit_expiry, test_3_result,
            final_test_result, syphillis_test_result, patient_given_result, couple_discordant, referred, referral_for, referral_facility,
            other_referral_facility, neg_referral_for, neg_referral_specify, tb_screening, patient_had_hiv_self_test, remarks, voided
        )
        SELECT
            e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator, e.date_created,
            IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified,
            DATE(e.encounter_datetime) AS visit_date,
            MAX(IF((o.concept_id=162084 AND o.value_coded=162082 AND f.uuid = "402dc5d7-46da-42d4-b2be-f43ea4ad87b0") OR (f.uuid = "b08471f6-0892-4bf7-ab2b-bf79797b8ea4"), 2, 1)) AS test_type,
            MAX(IF(o.concept_id=164930,(CASE o.value_coded WHEN 164928 THEN "General Population" WHEN 164929 THEN "Key and Vulnerable Population" WHEN 138643 THEN "Priority Population" ELSE NULL END),NULL)) AS population_type,
            MAX(IF((o.concept_id=160581 OR o.concept_id=165241) AND o.value_coded IN (105,160666,160578,165084,160579,165100,162277,167691,1142,163488,159674,162198,6096,5622,160549),
                (CASE o.value_coded
                    WHEN 105 THEN "People who inject drugs" WHEN 160666 THEN "People who use drugs" WHEN 160578 THEN "Men who have sex with men" WHEN 165084 THEN "Male Sex Worker"
                    WHEN 160579 THEN "Female sex worker" WHEN 162277 THEN "People in prison and other closed settings" WHEN 167691 THEN "Inmates" WHEN 1142 THEN "Prison Staff"
                    WHEN 163488 THEN "Prison Community" WHEN 159674 THEN "Fisher folk" WHEN 162198 THEN "Truck driver" WHEN 6096 THEN "Discordant" WHEN 160549 THEN "Adolescent and young girls" WHEN 5622 THEN "Other" ELSE NULL END),NULL)) AS key_population_type,
            MAX(IF(o.concept_id=160581 AND o.value_coded IN(159674,162198,160549,162277,1175,165192),
                (CASE o.value_coded WHEN 159674 THEN "Fisher folk" WHEN 162198 THEN "Truck driver" WHEN 160549 THEN "Adolescent and young girls" WHEN 162277 THEN "Prisoner" WHEN 1175 THEN "Not applicable" WHEN 165192 THEN "Military and other uniformed services" ELSE NULL END),NULL)) AS priority_population_type,
            MAX(IF(o.concept_id=164401,(CASE o.value_coded WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE NULL END),NULL)) AS ever_tested_for_hiv,
            FLOOR(CASE WHEN MAX(IF(o.concept_id=159813, o.value_numeric, NULL)) > 1200 THEN NULL ELSE MAX(IF(o.concept_id=159813, o.value_numeric, NULL)) END) AS months_since_last_test,
            MAX(IF(o.concept_id=164951,(CASE o.value_coded WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE NULL END),NULL)) AS patient_disabled,
            CONCAT_WS('','',
                MAX(IF(o.concept_id=162558 AND o.value_coded = 120291, "Hearing impairment", NULL)),
                MAX(IF(o.concept_id=162558 AND o.value_coded = 147215, "Visual impairment", NULL)),
                MAX(IF(o.concept_id=162558 AND o.value_coded = 151342, "Mentally Challenged", NULL)),
                MAX(IF(o.concept_id=162558 AND o.value_coded = 164538, "Physically Challenged", NULL)),
                MAX(IF(o.concept_id=162558 AND o.value_coded = 5622, "Other", NULL)),
                MAX(IF(o.concept_id=160632, o.value_text, NULL))
            ) AS disability_type,
            MAX(IF(o.concept_id=1710,(CASE o.value_coded WHEN 1 THEN "Yes" WHEN 0 THEN "No" ELSE NULL END),NULL)) AS patient_consented,
            MAX(IF(o.concept_id=164959,(CASE o.value_coded WHEN 164957 THEN "Individual" WHEN 164958 THEN "Couple" ELSE NULL END),NULL)) AS client_tested_as,
            MAX(IF(o.concept_id=165215,(CASE o.value_coded WHEN 1537 THEN "Facility" WHEN 163488 THEN "Community" ELSE "" END ),NULL)) AS setting,
            MAX(IF(o.concept_id=163556,(CASE o.value_coded WHEN 164163 THEN "Provider Initiated Testing(PITC)" WHEN 164953 THEN "Client Initiated Testing (CITC)" ELSE "" END ),NULL)) AS approach,
            MAX(IF(o.concept_id=164956,o.value_coded,NULL)) AS test_strategy,
            MAX(IF(o.concept_id=160540,o.value_coded,NULL)) AS hts_entry_point,
            MAX(IF(o.concept_id=167163,(CASE o.value_coded WHEN 1407 THEN "Low" WHEN 1499 THEN "Moderate" WHEN 1408 THEN "High" WHEN 167164 THEN "Very high" ELSE "" END),NULL)) AS hts_risk_category,
            MAX(IF(o.concept_id=167162,o.value_numeric,NULL)) AS hts_risk_score,
            MAX(IF(t.test_1_result IS NOT NULL, t.kit_name, NULL)) AS test_1_kit_name,
            MAX(IF(t.test_1_result IS NOT NULL, t.lot_no, NULL)) AS test_1_kit_lot_no,
            MAX(IF(t.test_1_result IS NOT NULL, t.expiry_date, NULL)) AS test_1_kit_expiry,
            MAX(IF(t.test_1_result IS NOT NULL, t.test_1_result, NULL)) AS test_1_result,
            MAX(IF(t.test_2_result IS NOT NULL, t.kit_name, NULL)) AS test_2_kit_name,
            MAX(IF(t.test_2_result IS NOT NULL, t.lot_no, NULL)) AS test_2_kit_lot_no,
            MAX(IF(t.test_2_result IS NOT NULL, t.expiry_date, NULL)) AS test_2_kit_expiry,
            MAX(IF(t.test_2_result IS NOT NULL, t.test_2_result, NULL)) AS test_2_result,
            MAX(IF(t.test_3_result IS NOT NULL, t.kit_name, NULL)) AS test_3_kit_name,
            MAX(IF(t.test_3_result IS NOT NULL, t.lot_no, NULL)) AS test_3_kit_lot_no,
            MAX(IF(t.test_3_result IS NOT NULL, t.expiry_date, NULL)) AS test_3_kit_expiry,
            MAX(IF(t.test_3_result IS NOT NULL, t.test_3_result, NULL)) AS test_3_result,
            MAX(IF(o.concept_id=159427,(CASE o.value_coded WHEN 703 THEN "Positive" WHEN 664 THEN "Negative" WHEN 1138 THEN "Inconclusive" WHEN 163611 THEN "Invalid" ELSE NULL END),NULL)) AS final_test_result,
            MAX(IF(o.concept_id=299,(CASE o.value_coded WHEN 1229 THEN "Positive" WHEN 1228 THEN "Negative" ELSE "" END),NULL)) AS syphillis_test_result,
            MAX(IF(o.concept_id=164848,(CASE o.value_coded WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE NULL END),NULL)) AS patient_given_result,
            MAX(IF(o.concept_id=6096,(CASE o.value_coded WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE NULL END),NULL)) AS couple_discordant,
            MAX(IF(o.concept_id=165093,o.value_coded,NULL)) AS referred,
            MAX(IF(o.concept_id=1887,(CASE o.value_coded WHEN 162082 THEN "Confirmatory test" WHEN 162050 THEN "Comprehensive care center" WHEN 164461 THEN "DBS for PCR" ELSE "" END),NULL)) AS referral_for,
            MAX(IF(o.concept_id=160481,(CASE o.value_coded WHEN 163266 THEN "This health facility" WHEN 164407 THEN "Other health facility" ELSE "" END),NULL)) AS referral_facility,
            MAX(IF(o.concept_id=161550,TRIM(o.value_text),NULL)) AS other_referral_facility,
            CONCAT_WS('','',
                MAX(IF(o.concept_id = 1272 AND o.value_coded = 165276, "Risk reduction counselling", NULL)),
                MAX(IF(o.concept_id = 1272 AND o.value_coded = 159612, "Safer sex practices", NULL)),
                MAX(IF(o.concept_id = 1272 AND o.value_coded = 162223, "VMMC", NULL)),
                MAX(IF(o.concept_id = 1272 AND o.value_coded = 190, "Condom use counselling", NULL)),
                MAX(IF(o.concept_id = 1272 AND o.value_coded = 1691, "Post-exposure prophylaxis", NULL)),
                MAX(IF(o.concept_id = 1272 AND o.value_coded = 167125, "Prevention and treatment of STIs", NULL)),
                MAX(IF(o.concept_id = 1272 AND o.value_coded = 118855, "Substance abuse and mental health treatment", NULL)),
                MAX(IF(o.concept_id = 1272 AND o.value_coded = 141814, "Prevention of violence", NULL)),
                MAX(IF(o.concept_id = 1272 AND o.value_coded = 1370, "HIV testing and re-testing", NULL)),
                MAX(IF(o.concept_id = 1272 AND o.value_coded = 166536, "Pre-Exposure Prophylaxis", NULL)),
                MAX(IF(o.concept_id = 1272 AND o.value_coded = 5622, "Other", NULL))
            ) AS neg_referral_for,
            MAX(IF(o.concept_id=164359,TRIM(o.value_text),NULL)) AS neg_referral_specify,
            MAX(IF(o.concept_id=1659,(CASE o.value_coded WHEN 1660 THEN "No TB signs" WHEN 142177 THEN "Presumed TB" WHEN 1662 THEN "TB Confirmed" WHEN 160737 THEN "Not done" WHEN 1111 THEN "On TB Treatment" ELSE NULL END),NULL)) AS tb_screening,
            MAX(IF(o.concept_id=164952,(CASE o.value_coded WHEN 1065 THEN "Yes" WHEN 1066 THEN "No" ELSE NULL END),NULL)) AS patient_had_hiv_self_test,
            MAX(IF(o.concept_id=163042,TRIM(o.value_text),NULL)) AS remarks,
            e.voided
        FROM encounter e
            INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0
            INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
            INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0
                AND o.concept_id IN (162084, 164930, 160581, 164401, 164951, 162558, 160632, 1710, 164959, 164956, 165241, 159427, 164848, 6096, 1659, 164952, 163042, 159813, 165215, 163556, 161550, 1887, 1272, 164359, 160481, 299, 167163, 167162, 165093)
            LEFT JOIN (
                SELECT o.person_id, o.encounter_id, o.obs_group_id,
                    MAX(IF(o.concept_id=1040, (CASE o.value_coded WHEN 703 THEN "Positive" WHEN 664 THEN "Negative" WHEN 163611 THEN "Invalid" ELSE NULL END), NULL)) AS test_1_result,
                    MAX(IF(o.concept_id=1326, (CASE o.value_coded WHEN 703 THEN "Positive" WHEN 664 THEN "Negative" WHEN 1175 THEN "N/A" ELSE NULL END), NULL)) AS test_2_result,
                    MAX(IF(o.concept_id=1000630, (CASE o.value_coded WHEN 703 THEN "Positive" WHEN 664 THEN "Negative" WHEN 1175 THEN "N/A" ELSE NULL END), NULL)) AS test_3_result,
                    MAX(IF(o.concept_id=164962, (CASE o.value_coded WHEN 164960 THEN "Determine" WHEN 164961 THEN "First Response" WHEN 165351 THEN "Dual Kit" WHEN 169126 THEN "One step" WHEN 169127 THEN "Trinscreen" ELSE NULL END), NULL)) AS kit_name,
                    MAX(IF(o.concept_id=164964, TRIM(o.value_text), NULL)) AS lot_no,
                    MAX(IF(o.concept_id=162502, DATE(o.value_datetime), NULL)) AS expiry_date
                FROM obs o
                WHERE o.concept_id IN (1040, 1326, 1000630, 164962, 164964, 162502) AND o.voided = 0
                GROUP BY e.encounter_id, o.obs_group_id
            ) t ON e.encounter_id = t.encounter_id
        WHERE e.voided = 0 AND (
            e.date_created >= ', last_ts_str, ' OR e.date_changed >= ', last_ts_str, ' OR
            o.date_created >= ', last_ts_str, '
        )
        GROUP BY e.encounter_id
        ON DUPLICATE KEY UPDATE
            visit_date=VALUES(visit_date), creator=VALUES(creator), test_type=VALUES(test_type), population_type=VALUES(population_type),
            key_population_type=VALUES(key_population_type), priority_population_type=VALUES(priority_population_type), ever_tested_for_hiv=VALUES(ever_tested_for_hiv),
            patient_disabled=VALUES(patient_disabled), disability_type=VALUES(disability_type), patient_consented=VALUES(patient_consented), client_tested_as=VALUES(client_tested_as),
            test_strategy=VALUES(test_strategy), hts_entry_point=VALUES(hts_entry_point), hts_risk_category=VALUES(hts_risk_category), hts_risk_score=VALUES(hts_risk_score),
            setting=VALUES(setting), approach=VALUES(approach), test_1_kit_name=VALUES(test_1_kit_name), test_1_kit_lot_no=VALUES(test_1_kit_lot_no),
            test_1_kit_expiry=VALUES(test_1_kit_expiry), test_1_result=VALUES(test_1_result), test_2_kit_name=VALUES(test_2_kit_name), test_2_kit_lot_no=VALUES(test_2_kit_lot_no),
            test_2_kit_expiry=VALUES(test_2_kit_expiry), test_2_result=VALUES(test_2_result), test_3_kit_name=VALUES(test_3_kit_name), test_3_kit_lot_no=VALUES(test_3_kit_lot_no),
            test_3_kit_expiry=VALUES(test_3_kit_expiry), test_3_result=VALUES(test_3_result), final_test_result=VALUES(final_test_result), patient_given_result=VALUES(patient_given_result),
            couple_discordant=VALUES(couple_discordant), referred=VALUES(referred), tb_screening=VALUES(tb_screening), neg_referral_for=VALUES(neg_referral_for),
            patient_had_hiv_self_test=VALUES(patient_had_hiv_self_test), remarks=VALUES(remarks), voided=VALUES(voided);'
    );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing HTS tests. Target: ', @target_table, ' Time: ', NOW()) AS message;
END $$


-- ------------------------------------ POPULATE HTS LINKAGES AND REFERRALS -------------------------------


DROP PROCEDURE IF EXISTS sp_update_hts_linkage_and_referral $$
CREATE PROCEDURE sp_update_hts_linkage_and_referral(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
  DECLARE where_clause TEXT;

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_hts_referral_and_linkage');

  SET where_clause = IF(
    last_update_time IS NULL,
    'e.voided = 0',
    CONCAT(
      '( (e.date_created >= ''', last_update_time, ''') OR (e.date_changed >= ''', last_update_time, ''') OR (e.date_voided >= ''', last_update_time, ''') OR (o.date_created >= ''', last_update_time, ''') OR (o.date_voided >= ''', last_update_time, ''')) AND e.voided = 0'
    )
  );

  SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, visit_id, encounter_id, encounter_uuid, encounter_location, creator, date_created, date_last_modified, visit_date, ',
      'tracing_type, tracing_status, referral_facility, facility_linked_to, enrollment_date, art_start_date, ccc_number, provider_handed_to, ',
      'cadre, remarks, voided) ',
    'SELECT ',
      'e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'DATE(e.encounter_datetime) AS visit_date, ',
      'MAX(IF(o.concept_id=164966, (CASE o.value_coded WHEN 1650 THEN ''Phone'' WHEN 164965 THEN ''Physical'' ELSE NULL END), NULL)) AS tracing_type, ',
      'MAX(IF(o.concept_id=159811, (CASE o.value_coded WHEN 1065 THEN ''Contacted and linked'' WHEN 1066 THEN ''Contacted but not linked'' ELSE NULL END), NULL)) AS tracing_status, ',
      'MAX(IF(o.concept_id=160481, (CASE o.value_coded WHEN 163266 THEN ''This health facility'' WHEN 164407 THEN ''Other health facility'' ELSE NULL END), NULL)) AS referral_facility, ',
      'MAX(IF(o.concept_id=162724, TRIM(o.value_text), NULL)) AS facility_linked_to, ',
      'MAX(IF(o.concept_id=160555, o.value_datetime, NULL)) AS enrollment_date, ',
      'MAX(IF(o.concept_id=159599, o.value_datetime, NULL)) AS art_start_date, ',
      'MAX(IF(o.concept_id=162053, o.value_numeric, NULL)) AS ccc_number, ',
      'MAX(IF(o.concept_id=1473, TRIM(o.value_text), NULL)) AS provider_handed_to, ',
      'MAX(IF(o.concept_id=162577, (CASE o.value_coded WHEN 1577 THEN ''Nurse'' WHEN 1574 THEN ''Clinical Officer/Doctor'' WHEN 1555 THEN ''Community Health Worker'' WHEN 1540 THEN ''Employee'' WHEN 5488 THEN ''Adherence counsellor'' WHEN 5622 THEN ''Other'' ELSE NULL END), NULL)) AS cadre, ',
      'MAX(IF(o.concept_id=163042, TRIM(o.value_text), NULL)) AS remarks, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''050a7f12-5c52-4cad-8834-863695af335d'', ''15ed03d2-c972-11e9-a32f-2a2ae2dbcce4'') ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (164966,159811,162724,160555,159599,162053,1473,162577,160481,163042) AND o.voided = 0 ',
    'WHERE ', where_clause, ' ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), creator = VALUES(creator), date_last_modified = VALUES(date_last_modified), ',
      'tracing_type = VALUES(tracing_type), tracing_status = VALUES(tracing_status), referral_facility = VALUES(referral_facility), ',
      'facility_linked_to = VALUES(facility_linked_to), enrollment_date = VALUES(enrollment_date), art_start_date = VALUES(art_start_date), ',
      'ccc_number = VALUES(ccc_number), provider_handed_to = VALUES(provider_handed_to), cadre = VALUES(cadre), remarks = VALUES(remarks), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing hts linkages', CONCAT('Time: ', NOW());
END $$


-- ------------------------ update hts referrals ------------------------------------




DROP PROCEDURE IF EXISTS sp_update_hts_referral $$
CREATE PROCEDURE sp_update_hts_referral(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
  DECLARE where_clause TEXT;
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_hts_referral');
    SET where_clause = IF(
      last_update_time IS NULL,
      'e.voided = 0',
      CONCAT(
        'e.voided = 0 AND (e.date_created >= ',
        QUOTE(last_update_time),
        ' OR e.date_changed >= ',
        QUOTE(last_update_time),
        ' OR e.date_voided >= ',
        QUOTE(last_update_time),
        ' OR o.date_created >= ',
        QUOTE(last_update_time),
        ' OR o.date_voided >= ',
        QUOTE(last_update_time),
        ')'
      )
    );
  SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, visit_id, encounter_id, encounter_uuid, encounter_location, creator, date_created, date_last_modified, ',
      'visit_date, facility_referred_to, date_to_enrol, remarks, voided',
    ') ',
    'SELECT ',
      'e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'DATE(e.encounter_datetime) AS visit_date, ',
      'MAX(IF(o.concept_id=161550, TRIM(o.value_text), NULL)) AS facility_referred_to, ',
      'MAX(IF(o.concept_id=161561, o.value_datetime, NULL)) AS date_to_enrol, ',
      'MAX(IF(o.concept_id=163042, TRIM(o.value_text), NULL)) AS remarks, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''9284828e-ce55-11e9-a32f-2a2ae2dbcce4'' ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (161550,161561,163042) AND o.voided = 0 ',
    'WHERE ', where_clause, ' ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), ',
      'creator = VALUES(creator), ',
      'date_last_modified = VALUES(date_last_modified), ',
      'facility_referred_to = VALUES(facility_referred_to), ',
      'date_to_enrol = VALUES(date_to_enrol), ',
      'remarks = VALUES(remarks), ',
      'voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing hts referrals', CONCAT('Time: ', NOW());
END $$


-- ------------- populate etl_ipt_screening-------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_ipt_screening $$
CREATE PROCEDURE sp_update_etl_ipt_screening(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
  DECLARE where_clause TEXT;

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_ipt_screening');

  SET where_clause = IF(
    last_update_time IS NULL,
    'e.voided = 0',
    CONCAT(
      'e.voided = 0 AND (e.date_created >= ', QUOTE(last_update_time),
      ' OR e.date_changed >= ', QUOTE(last_update_time),
      ' OR e.date_voided >= ', QUOTE(last_update_time),
      ' OR o.date_created >= ', QUOTE(last_update_time),
      ' OR o.date_voided >= ', QUOTE(last_update_time),
      ')'
    )
  );

  SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, encounter_id, location_id, ipt_started, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.encounter_id, e.location_id, ',
      'MAX(o.value_coded) AS ipt_started, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''22c68f86-bbf0-49ba-b2d1-23fa7ccf0259'', ''59ed8e62-7f1f-40ae-a2e3-eabe350277ce'') ',
      'INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id = 1265 AND o.voided = 0 ',
    'WHERE ', where_clause, ' ',
    'GROUP BY e.patient_id, visit_date ',
    'ON DUPLICATE KEY UPDATE visit_date = VALUES(visit_date), ipt_started = VALUES(ipt_started), date_last_modified = VALUES(date_last_modified);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT "Completed processing TPT screening forms", CONCAT("Time: ", NOW());
END $$


-- ------------- populate etl_ipt_followup-------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_ipt_follow_up $$
CREATE PROCEDURE sp_update_etl_ipt_follow_up(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
  DECLARE where_clause TEXT;

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_ipt_follow_up');

SELECT 'Processing TPT followup forms', CONCAT('Time: ', NOW());

SET where_clause = IF(
    last_update_time IS NULL,
    'e.voided = 0',
    CONCAT(
      'e.voided = 0 AND (e.date_created >= ', QUOTE(last_update_time),
      ' OR e.date_changed >= ', QUOTE(last_update_time),
      ' OR e.date_voided >= ', QUOTE(last_update_time),
      ' OR o.date_created >= ', QUOTE(last_update_time),
      ' OR o.date_voided >= ', QUOTE(last_update_time),
      ')'
    )
  );

  SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, uuid, provider, visit_id, visit_date, encounter_id, location_id, ',
      'ipt_due_date, date_collected_ipt, hepatotoxity, peripheral_neuropathy, rash, has_other_symptoms, adherence, action_taken, date_created, date_last_modified, voided',
    ') ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.encounter_id, e.location_id, ',
      'MAX(IF(o.concept_id = 164073, o.value_datetime, NULL)) AS ipt_due_date, ',
      'MAX(IF(o.concept_id = 164074, o.value_datetime, NULL)) AS date_collected_ipt, ',
      'MAX(IF(o.concept_id = 159098, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END, NULL)) AS hepatotoxity, ',
      'MAX(IF(o.concept_id = 118983, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END, NULL)) AS peripheral_neuropathy, ',
      'MAX(IF(o.concept_id = 512, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END, NULL)) AS rash, ',
      'MAX(IF(o.concept_id = 163190, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE NULL END, NULL)) AS has_other_symptoms, ',
      'MAX(IF(o.concept_id = 164075, CASE o.value_coded WHEN 159407 THEN ''Poor'' WHEN 159405 THEN ''Good'' WHEN 159406 THEN ''Fair'' WHEN 164077 THEN ''Very Good'' WHEN 164076 THEN ''Excellent'' WHEN 1067 THEN ''Unknown'' ELSE NULL END, NULL)) AS adherence, ',
      'MAX(IF(o.concept_id = 160632, TRIM(o.value_text), NULL)) AS action_taken, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN (SELECT encounter_type_id FROM encounter_type WHERE uuid IN (''aadeafbe-a3b1-4c57-bc76-8461b778ebd6'')) et ON et.encounter_type_id = e.encounter_type ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (164073,164074,159098,118983,512,164075,160632,163190) ',
    'WHERE ', where_clause, ' ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), provider = VALUES(provider), ipt_due_date = VALUES(ipt_due_date), ',
      'date_collected_ipt = VALUES(date_collected_ipt), hepatotoxity = VALUES(hepatotoxity), peripheral_neuropathy = VALUES(peripheral_neuropathy), ',
      'rash = VALUES(rash), has_other_symptoms = VALUES(has_other_symptoms), adherence = VALUES(adherence), action_taken = VALUES(action_taken), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing TPT followup forms', CONCAT('Time: ', NOW());
END $$



DROP PROCEDURE IF EXISTS sp_update_etl_ccc_defaulter_tracing $$
CREATE PROCEDURE sp_update_etl_ccc_defaulter_tracing(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_ccc_defaulter_tracing');
SELECT 'Processing ccc defaulter tracing form', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, tracing_type, missed_appointment_date, reason_for_missed_appointment, non_coded_missed_appointment_reason, tracing_outcome, reason_not_contacted, attempt_number, is_final_trace, true_status, cause_of_death, comments, booking_date, date_created, date_last_modified) ',
    'SELECT ',
      'e.uuid, e.creator AS provider, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id = 164966, o.value_coded, NULL)) AS tracing_type, ',
      'MAX(IF(o.concept_id = 164093, DATE(o.value_datetime), NULL)) AS missed_appointment_date, ',
      'MAX(IF(o.concept_id = 1801, o.value_coded, NULL)) AS reason_for_missed_appointment, ',
      'MAX(NULLIF(TRIM(IF(o.concept_id = 163513, o.value_text, NULL)), \'\')) AS non_coded_missed_appointment_reason, ',
      'MAX(IF(o.concept_id = 160721, o.value_coded, NULL)) AS tracing_outcome, ',
      'MAX(IF(o.concept_id = 166541, o.value_coded, NULL)) AS reason_not_contacted, ',
      'MAX(IF(o.concept_id = 1639, o.value_numeric, NULL)) AS attempt_number, ',
      'MAX(IF(o.concept_id = 163725, o.value_coded, NULL)) AS is_final_trace, ',
      'MAX(IF(o.concept_id = 160433, o.value_coded, NULL)) AS true_status, ',
      'MAX(IF(o.concept_id = 1599, o.value_coded, NULL)) AS cause_of_death, ',
      'MAX(NULLIF(TRIM(IF(o.concept_id = 160716, o.value_text, NULL)), \'\')) AS comments, ',
      'MAX(IF(o.concept_id = 163526, DATE(o.value_datetime), NULL)) AS booking_date, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''a1a62d1e-2def-11e9-b210-d663bd873d93'') ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (164966,164093,1801,163513,160721,1639,163725,160433,1599,160716,163526,166541) ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), ',
      'provider = VALUES(provider), ',
      'tracing_type = VALUES(tracing_type), ',
      'missed_appointment_date = VALUES(missed_appointment_date), ',
      'reason_for_missed_appointment = VALUES(reason_for_missed_appointment), ',
      'non_coded_missed_appointment_reason = VALUES(non_coded_missed_appointment_reason), ',
      'tracing_outcome = VALUES(tracing_outcome), ',
      'reason_not_contacted = VALUES(reason_not_contacted), ',
      'attempt_number = VALUES(attempt_number), ',
      'is_final_trace = VALUES(is_final_trace), ',
      'true_status = VALUES(true_status), ',
      'cause_of_death = VALUES(cause_of_death), ',
      'comments = VALUES(comments), ',
      'booking_date = VALUES(booking_date);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing CCC defaulter tracing forms', CONCAT('Time: ', NOW());
END $$


-- ------------- Update etl_ART_preparation-------------------------
;
DROP PROCEDURE IF EXISTS sp_update_etl_ART_preparation $$
CREATE PROCEDURE sp_update_etl_ART_preparation(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_ART_preparation');

SELECT 'Processing ART Preparation', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, patient_id, visit_id, visit_date, location_id, encounter_id, provider, ',
      'understands_hiv_art_benefits, screened_negative_substance_abuse, screened_negative_psychiatric_illness, ',
      'HIV_status_disclosure, trained_drug_admin, informed_drug_side_effects, caregiver_committed, adherance_barriers_identified, ',
      'caregiver_location_contacts_known, ready_to_start_art, identified_drug_time, treatment_supporter_engaged, support_grp_meeting_awareness, ',
      'enrolled_in_reminder_system, other_support_systems, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.uuid, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.creator, ',
      'NULLIF(MAX(IF(o.concept_id=1729, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS understands_hiv_art_benefits, ',
      'NULLIF(MAX(IF(o.concept_id=160246, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS screened_negative_substance_abuse, ',
      'NULLIF(MAX(IF(o.concept_id=159891, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS screened_negative_psychiatric_illness, ',
      'NULLIF(MAX(IF(o.concept_id=1048, CASE o.value_coded WHEN 1 THEN ''Yes'' WHEN 0 THEN ''No'' ELSE '' END, '''')), '''') AS HIV_status_disclosure, ',
      'NULLIF(MAX(IF(o.concept_id=164425, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS trained_drug_admin, ',
      'NULLIF(MAX(IF(o.concept_id=121764, CASE o.value_coded WHEN 1 THEN ''Yes'' WHEN 0 THEN ''No'' ELSE '' END, '''')), '''') AS informed_drug_side_effects, ',
      'NULLIF(MAX(IF(o.concept_id=5619, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS caregiver_committed, ',
      'NULLIF(MAX(IF(o.concept_id=159707, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS adherance_barriers_identified, ',
      'NULLIF(MAX(IF(o.concept_id=163089, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS caregiver_location_contacts_known, ',
      'NULLIF(MAX(IF(o.concept_id=162695, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS ready_to_start_art, ',
      'NULLIF(MAX(IF(o.concept_id=160119, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS identified_drug_time, ',
      'NULLIF(MAX(IF(o.concept_id=164886, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS treatment_supporter_engaged, ',
      'NULLIF(MAX(IF(o.concept_id=163766, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS support_grp_meeting_awareness, ',
      'NULLIF(MAX(IF(o.concept_id=163164, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS enrolled_in_reminder_system, ',
      'NULLIF(MAX(IF(o.concept_id=164360, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS other_support_systems, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 AND o.concept_id IN (1729,160246,159891,1048,164425,121764,5619,159707,163089,162695,160119,164886,163766,163164,164360) ',
      'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''782a4263-3ac9-4ce8-b316-534571233f12'')) f ON f.form_id = e.form_id ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), encounter_id = VALUES(encounter_id), provider = VALUES(provider), ',
      'understands_hiv_art_benefits = VALUES(understands_hiv_art_benefits), screened_negative_substance_abuse = VALUES(screened_negative_substance_abuse), ',
      'screened_negative_psychiatric_illness = VALUES(screened_negative_psychiatric_illness), HIV_status_disclosure = VALUES(HIV_status_disclosure), ',
      'trained_drug_admin = VALUES(trained_drug_admin), informed_drug_side_effects = VALUES(informed_drug_side_effects), caregiver_committed = VALUES(caregiver_committed), ',
      'adherance_barriers_identified = VALUES(adherance_barriers_identified), caregiver_location_contacts_known = VALUES(caregiver_location_contacts_known), ',
      'ready_to_start_art = VALUES(ready_to_start_art), identified_drug_time = VALUES(identified_drug_time), treatment_supporter_engaged = VALUES(treatment_supporter_engaged), ',
      'support_grp_meeting_awareness = VALUES(support_grp_meeting_awareness), enrolled_in_reminder_system = VALUES(enrolled_in_reminder_system), ',
      'other_support_systems = VALUES(other_support_systems);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing ART Preparation', CONCAT('Time: ', NOW());
END $$

-- ------------- update etl_enhanced_adherence-------------------------
 ;
DROP PROCEDURE IF EXISTS sp_update_etl_enhanced_adherence $$
CREATE PROCEDURE sp_update_etl_enhanced_adherence(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_enhanced_adherence');

SELECT 'Processing Enhanced Adherence ', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, patient_id, visit_id, visit_date, location_id, encounter_id, provider, ',
      'session_number, first_session_date, pill_count, ',
      'MMAS4_1_forgets_to_take_meds, MMAS4_2_careless_taking_meds, MMAS4_3_stops_on_reactive_meds, MMAS4_4_stops_meds_on_feeling_good, ',
      'MMSA8_1_took_meds_yesterday, MMSA8_2_stops_meds_on_controlled_symptoms, MMSA8_3_struggles_to_comply_tx_plan, MMSA8_4_struggles_remembering_taking_meds, ',
      'arv_adherence, has_vl_results, vl_results_suppressed, vl_results_feeling, cause_of_high_vl, way_forward, ',
      'patient_hiv_knowledge, patient_drugs_uptake, patient_drugs_reminder_tools, patient_drugs_uptake_during_travels, patient_drugs_side_effects_response, ',
      'patient_drugs_uptake_most_difficult_times, patient_drugs_daily_uptake_feeling, patient_ambitions, patient_has_people_to_talk, ',
      'patient_enlisting_social_support, patient_income_sources, patient_challenges_reaching_clinic, patient_worried_of_accidental_disclosure, ',
      'patient_treated_differently, stigma_hinders_adherence, patient_tried_faith_healing, patient_adherence_improved, patient_doses_missed, ',
      'review_and_barriers_to_adherence, other_referrals, appointments_honoured, referral_experience, home_visit_benefit, adherence_plan, ',
      'next_appointment_date, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.uuid, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.creator, ',
      'MAX(IF(o.concept_id=1639, o.value_numeric, NULL)) AS session_number, ',
      'MAX(IF(o.concept_id=164891, o.value_datetime, NULL)) AS first_session_date, ',
      'MAX(IF(o.concept_id=162846, o.value_numeric, NULL)) AS pill_count, ',
      'NULLIF(MAX(IF(o.concept_id=167321, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS MMAS4_1_forgets_to_take_meds, ',
      'NULLIF(MAX(IF(o.concept_id=163088, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS MMAS4_2_careless_taking_meds, ',
      'NULLIF(MAX(IF(o.concept_id=6098, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS MMAS4_3_stops_on_reactive_meds, ',
      'NULLIF(MAX(IF(o.concept_id=164998, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS MMAS4_4_stops_meds_on_feeling_good, ',
      'NULLIF(MAX(IF(o.concept_id=162736, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS MMSA8_1_took_meds_yesterday, ',
      'NULLIF(MAX(IF(o.concept_id=1743, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS MMSA8_2_stops_meds_on_controlled_symptoms, ',
      'NULLIF(MAX(IF(o.concept_id=1779, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '' END, '''')), '''') AS MMSA8_3_struggles_to_comply_tx_plan, ',
      'NULLIF(MAX(IF(o.concept_id=166365, CASE o.value_coded WHEN 1090 THEN ''Never/rarely'' WHEN 1358 THEN ''Once in a while'' WHEN 1385 THEN ''Sometimes'' WHEN 161236 THEN ''Usually'' WHEN 162135 THEN ''All the time'' ELSE '''' END, '''')), '''') AS MMSA8_4_struggles_remembering_taking_meds, ',
      'NULLIF(MAX(IF(o.concept_id=1658, CASE o.value_coded WHEN 159405 THEN ''Good'' WHEN 163794 THEN ''Inadequate'' WHEN 159407 THEN ''Poor'' ELSE '''' END, '''')), '''') AS arv_adherence, ',
      'NULLIF(MAX(IF(o.concept_id=164848, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END, '''')), '''') AS has_vl_results, ',
      'NULLIF(MAX(IF(o.concept_id=163310, CASE o.value_coded WHEN 1302 THEN ''Suppressed'' WHEN 1066 THEN ''Unsuppresed'' ELSE '''' END, '''')), '''') AS vl_results_suppressed, ',
      'NULLIF(MAX(IF(o.concept_id=164981, TRIM(o.value_text), NULL)), '''') AS vl_results_feeling, ',
      'NULLIF(MAX(IF(o.concept_id=164982, TRIM(o.value_text), NULL)), '''') AS cause_of_high_vl, ',
      'NULLIF(MAX(IF(o.concept_id=160632, TRIM(o.value_text), NULL)), '''') AS way_forward, ',
      'NULLIF(MAX(IF(o.concept_id=164983, TRIM(o.value_text), NULL)), '''') AS patient_hiv_knowledge, ',
      'NULLIF(MAX(IF(o.concept_id=164984, TRIM(o.value_text), NULL)), '''') AS patient_drugs_uptake, ',
      'NULLIF(MAX(IF(o.concept_id=164985, TRIM(o.value_text), NULL)), '''') AS patient_drugs_reminder_tools, ',
      'NULLIF(MAX(IF(o.concept_id=164986, TRIM(o.value_text), NULL)), '''') AS patient_drugs_uptake_during_travels, ',
      'NULLIF(MAX(IF(o.concept_id=164987, TRIM(o.value_text), NULL)), '''') AS patient_drugs_side_effects_response, ',
      'NULLIF(MAX(IF(o.concept_id=164988, TRIM(o.value_text), NULL)), '''') AS patient_drugs_uptake_most_difficult_times, ',
      'NULLIF(MAX(IF(o.concept_id=164989, TRIM(o.value_text), NULL)), '''') AS patient_drugs_daily_uptake_feeling, ',
      'NULLIF(MAX(IF(o.concept_id=164990, TRIM(o.value_text), NULL)), '''') AS patient_ambitions, ',
      'NULLIF(MAX(IF(o.concept_id=164991, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END, '''')), '''') AS patient_has_people_to_talk, ',
      'NULLIF(MAX(IF(o.concept_id=164992, TRIM(o.value_text), NULL)), '''') AS patient_enlisting_social_support, ',
      'NULLIF(MAX(IF(o.concept_id=164993, TRIM(o.value_text), NULL)), '''') AS patient_income_sources, ',
      'NULLIF(MAX(IF(o.concept_id=164994, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END, '''')), '''') AS patient_challenges_reaching_clinic, ',
      'NULLIF(MAX(IF(o.concept_id=164995, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END, '''')), '''') AS patient_worried_of_accidental_disclosure, ',
      'NULLIF(MAX(IF(o.concept_id=164996, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END, '''')), '''') AS patient_treated_differently, ',
      'NULLIF(MAX(IF(o.concept_id=164997, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END, '''')), '''') AS stigma_hinders_adherence, ',
      'NULLIF(MAX(IF(o.concept_id=164998, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END, '''')), '''') AS patient_tried_faith_healing, ',
      'NULLIF(MAX(IF(o.concept_id=1898, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END, '''')), '''') AS patient_adherence_improved, ',
      'NULLIF(MAX(IF(o.concept_id=160110, CASE o.value_coded WHEN 1 THEN ''Yes'' WHEN 0 THEN ''No'' ELSE '''' END, '''')), '''') AS patient_doses_missed, ',
      'NULLIF(MAX(IF(o.concept_id=163108, TRIM(o.value_text), NULL)), '''') AS review_and_barriers_to_adherence, ',
      'NULLIF(MAX(IF(o.concept_id=1272, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END, '''')), '''') AS other_referrals, ',
      'NULLIF(MAX(IF(o.concept_id=164999, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END, '''')), '''') AS appointments_honoured, ',
      'NULLIF(MAX(IF(o.concept_id=165000, TRIM(o.value_text), NULL)), '''') AS referral_experience, ',
      'NULLIF(MAX(IF(o.concept_id=165001, CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END, '''')), '''') AS home_visit_benefit, ',
      'NULLIF(MAX(IF(o.concept_id=165002, TRIM(o.value_text), NULL)), '''') AS adherence_plan, ',
      'MAX(IF(o.concept_id=5096, o.value_datetime, NULL)) AS next_appointment_date, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN obs o ON e.encounter_id = o.encounter_id AND o.voided = 0 AND o.concept_id IN (1639,164891,162846,1658,164848,163310,164981,164982,160632,164983,164984,164985,164986,164987,164988,164989,164990,164991,164992,164993,164994,164995,164996,164997,164998,1898,160110,163108,1272,164999,165000,165001,165002,5096,167321,163088,6098,162736,1743,1779,166365) ',
      'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''c483f10f-d9ee-4b0d-9b8c-c24c1ec24701'')) f ON f.form_id = e.form_id ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), encounter_id = VALUES(encounter_id), provider = VALUES(provider), ',
      'session_number = VALUES(session_number), first_session_date = VALUES(first_session_date), pill_count = VALUES(pill_count), ',
      'MMAS4_1_forgets_to_take_meds = VALUES(MMAS4_1_forgets_to_take_meds), MMAS4_2_careless_taking_meds = VALUES(MMAS4_2_careless_taking_meds), ',
      'MMAS4_3_stops_on_reactive_meds = VALUES(MMAS4_3_stops_on_reactive_meds), MMAS4_4_stops_meds_on_feeling_good = VALUES(MMAS4_4_stops_meds_on_feeling_good), ',
      'MMSA8_1_took_meds_yesterday = VALUES(MMSA8_1_took_meds_yesterday), MMSA8_2_stops_meds_on_controlled_symptoms = VALUES(MMSA8_2_stops_meds_on_controlled_symptoms), ',
      'MMSA8_3_struggles_to_comply_tx_plan = VALUES(MMSA8_3_struggles_to_comply_tx_plan), MMSA8_4_struggles_remembering_taking_meds = VALUES(MMSA8_4_struggles_remembering_taking_meds), ',
      'arv_adherence = VALUES(arv_adherence), has_vl_results = VALUES(has_vl_results), vl_results_suppressed = VALUES(vl_results_suppressed), vl_results_feeling = VALUES(vl_results_feeling), ',
      'cause_of_high_vl = VALUES(cause_of_high_vl), way_forward = VALUES(way_forward), patient_hiv_knowledge = VALUES(patient_hiv_knowledge), ',
      'patient_drugs_uptake = VALUES(patient_drugs_uptake), patient_drugs_reminder_tools = VALUES(patient_drugs_reminder_tools), ',
      'patient_drugs_uptake_during_travels = VALUES(patient_drugs_uptake_during_travels), patient_drugs_side_effects_response = VALUES(patient_drugs_side_effects_response), ',
      'patient_drugs_uptake_most_difficult_times = VALUES(patient_drugs_uptake_most_difficult_times), patient_drugs_daily_uptake_feeling = VALUES(patient_drugs_daily_uptake_feeling), ',
      'patient_ambitions = VALUES(patient_ambitions), patient_has_people_to_talk = VALUES(patient_has_people_to_talk), patient_enlisting_social_support = VALUES(patient_enlisting_social_support), ',
      'patient_income_sources = VALUES(patient_income_sources), patient_challenges_reaching_clinic = VALUES(patient_challenges_reaching_clinic), ',
      'patient_worried_of_accidental_disclosure = VALUES(patient_worried_of_accidental_disclosure), patient_treated_differently = VALUES(patient_treated_differently), ',
      'stigma_hinders_adherence = VALUES(stigma_hinders_adherence), patient_tried_faith_healing = VALUES(patient_tried_faith_healing), ',
      'patient_adherence_improved = VALUES(patient_adherence_improved), patient_doses_missed = VALUES(patient_doses_missed), ',
      'review_and_barriers_to_adherence = VALUES(review_and_barriers_to_adherence), other_referrals = VALUES(other_referrals), ',
      'appointments_honoured = VALUES(appointments_honoured), referral_experience = VALUES(referral_experience), home_visit_benefit = VALUES(home_visit_benefit), ',
      'adherence_plan = VALUES(adherence_plan), next_appointment_date = VALUES(next_appointment_date), date_created = VALUES(date_created), date_last_modified = VALUES(date_last_modified);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing Enhanced Adherence ', CONCAT('Time: ', NOW());
END $$

-- ------------- update etl_patient_triage-------------------------
sql
DROP PROCEDURE IF EXISTS sp_update_etl_patient_triage $$
CREATE PROCEDURE sp_update_etl_patient_triage(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_patient_triage');
SELECT 'Processing Patient Triage', CONCAT('Time: ', NOW());
SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, patient_id, visit_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, ',
      'visit_reason, complaint_today, complaint_duration, weight, height, systolic_pressure, diastolic_pressure, temperature, ',
      'temperature_collection_mode, pulse_rate, respiratory_rate, oxygen_saturation, oxygen_saturation_collection_mode, muac, ',
      'z_score_absolute, z_score, nutritional_status, nutritional_intervention, last_menstrual_period, hpv_vaccinated, voided',
    ') ',
    'SELECT ',
      'e.uuid, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id AS encounter_id, ',
      'e.creator AS encounter_provider, e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 160430, TRIM(o.value_text), NULL)) AS visit_reason, ',
      "NULLIF(MAX(IF(o.concept_id = 1154, CASE o.value_coded WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' ELSE NULL END, NULL)), '') AS complaint_today, ",
      'MAX(IF(o.concept_id = 159368, o.value_numeric, NULL)) AS complaint_duration, ',
      'MAX(IF(o.concept_id = 5089, o.value_numeric, NULL)) AS weight, ',
      'MAX(IF(o.concept_id = 5090, o.value_numeric, NULL)) AS height, ',
      'MAX(IF(o.concept_id = 5085, o.value_numeric, NULL)) AS systolic_pressure, ',
      'MAX(IF(o.concept_id = 5086, o.value_numeric, NULL)) AS diastolic_pressure, ',
      'MAX(IF(o.concept_id = 5088, o.value_numeric, NULL)) AS temperature, ',
      'MAX(IF(o.concept_id = 167231, o.value_coded, NULL)) AS temperature_collection_mode, ',
      'MAX(IF(o.concept_id = 5087, o.value_numeric, NULL)) AS pulse_rate, ',
      'MAX(IF(o.concept_id = 5242, o.value_numeric, NULL)) AS respiratory_rate, ',
      'MAX(IF(o.concept_id = 5092, o.value_numeric, NULL)) AS oxygen_saturation, ',
      'MAX(IF(o.concept_id = 165932, o.value_coded, NULL)) AS oxygen_saturation_collection_mode, ',
      'MAX(IF(o.concept_id = 1343, o.value_numeric, NULL)) AS muac, ',
      'MAX(IF(o.concept_id = 162584, o.value_numeric, NULL)) AS z_score_absolute, ',
      'MAX(IF(o.concept_id = 163515, o.value_coded, NULL)) AS z_score, ',
      'MAX(IF(o.concept_id IN (163515,167392), o.value_coded, NULL)) AS nutritional_status, ',
      'MAX(IF(o.concept_id = 163304, o.value_coded, NULL)) AS nutritional_intervention, ',
      'MAX(IF(o.concept_id = 1427, DATE(o.value_datetime), NULL)) AS last_menstrual_period, ',
      'MAX(IF(o.concept_id = 160325, o.value_coded, NULL)) AS hpv_vaccinated, ',
      'e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''d1059fb9-a079-4feb-a749-eedd709ae542'',''a0034eee-1940-4e35-847f-97537a35d05e'',''465a92f2-baf8-42e9-9612-53064be868e8'')) et ON et.encounter_type_id = e.encounter_type ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
      '  AND o.concept_id IN (160430,5089,5090,5085,5086,5088,5087,5242,5092,1343,163515,167392,1427,160325,162584,1154,159368,163304,167231,165932) ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), encounter_provider = VALUES(encounter_provider), weight = VALUES(weight), height = VALUES(height), ',
      'systolic_pressure = VALUES(systolic_pressure), diastolic_pressure = VALUES(diastolic_pressure), temperature = VALUES(temperature), ',
      'pulse_rate = VALUES(pulse_rate), respiratory_rate = VALUES(respiratory_rate), complaint_today = VALUES(complaint_today), ',
      'complaint_duration = VALUES(complaint_duration), oxygen_saturation = VALUES(oxygen_saturation), muac = VALUES(muac), ',
      'z_score = VALUES(z_score), nutritional_status = VALUES(nutritional_status), nutritional_intervention = VALUES(nutritional_intervention), ',
      'last_menstrual_period = VALUES(last_menstrual_period), hpv_vaccinated = VALUES(hpv_vaccinated), voided = VALUES(voided), ',
      'z_score_absolute = VALUES(z_score_absolute), oxygen_saturation_collection_mode = VALUES(oxygen_saturation_collection_mode), ',
      'temperature_collection_mode = VALUES(temperature_collection_mode);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing Patient Triage data', CONCAT('Time: ', NOW());
END $$


-- ------------ create table etl_generalized_anxiety_disorder-----------------------

DROP PROCEDURE IF EXISTS sp_update_etl_generalized_anxiety_disorder $$
CREATE PROCEDURE sp_update_etl_generalized_anxiety_disorder(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_generalized_anxiety_disorder');

SELECT 'Processing Generalized Anxiety Disorder', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, patient_id, visit_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, ',
      'feeling_nervous_anxious, control_worrying, worrying_much, trouble_relaxing, being_restless, feeling_bad, feeling_afraid, assessment_outcome, voided',
    ') ',
    'SELECT ',
      'e.uuid, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.creator AS encounter_provider, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 167003, o.value_coded, NULL)) AS feeling_nervous_anxious, ',
      'MAX(IF(o.concept_id = 167005, o.value_coded, NULL)) AS control_worrying, ',
      'MAX(IF(o.concept_id = 166482, o.value_coded, NULL)) AS worrying_much, ',
      'MAX(IF(o.concept_id = 167064, o.value_coded, NULL)) AS trouble_relaxing, ',
      'MAX(IF(o.concept_id = 167065, o.value_coded, NULL)) AS being_restless, ',
      'MAX(IF(o.concept_id = 167066, o.value_coded, NULL)) AS feeling_bad, ',
      'MAX(IF(o.concept_id = 167067, o.value_coded, NULL)) AS feeling_afraid, ',
      'MAX(IF(o.concept_id = 167267, o.value_coded, NULL)) AS assessment_outcome, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''524d078e-936a-4543-9ca6-7a8d9ed4db06'') ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (167003,167005,166482,167064,167065,167066,167067,167267) ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), encounter_provider = VALUES(encounter_provider), date_last_modified = VALUES(date_last_modified), ',
      'feeling_nervous_anxious = VALUES(feeling_nervous_anxious), control_worrying = VALUES(control_worrying), worrying_much = VALUES(worrying_much), ',
      'trouble_relaxing = VALUES(trouble_relaxing), being_restless = VALUES(being_restless), feeling_bad = VALUES(feeling_bad), feeling_afraid = VALUES(feeling_afraid), ',
      'assessment_outcome = VALUES(assessment_outcome), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing Generalized Anxiety Disorder', CONCAT('Time: ', NOW());
END $$

-- ------------- populate etl_prep_behaviour_risk_assessment-------------------------
-- sql
DROP PROCEDURE IF EXISTS sp_update_etl_prep_behaviour_risk_assessment $$
CREATE PROCEDURE sp_update_etl_prep_behaviour_risk_assessment(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_prep_behaviour_risk_assessment');

SELECT 'Processing Behaviour risk assessment', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, date_created, date_last_modified, ',
      'sexual_partner_hiv_status, sexual_partner_on_art, risk, high_risk_partner, sex_with_multiple_partners, ipv_gbv, transactional_sex, ',
      'recent_sti_infected, recurrent_pep_use, recurrent_sex_under_influence, inconsistent_no_condom_use, sharing_drug_needles, ',
      'other_reasons, other_reason_specify, risk_education_offered, risk_reduction, willing_to_take_prep, reason_not_willing, ',
      'risk_edu_offered, risk_education, referral_for_prevention_services, referral_facility, time_partner_hiv_positive_known, ',
      'partner_enrolled_ccc, partner_ccc_number, partner_art_start_date, serodiscordant_confirmation_date, HIV_serodiscordant_duration_months, ',
      'recent_unprotected_sex_with_positive_partner, children_with_hiv_positive_partner, voided',
    ') ',
    'SELECT ',
      'e.uuid, e.creator AS provider, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 1436, (CASE o.value_coded WHEN 703 THEN ''HIV Positive'' WHEN 664 THEN ''HIV Negative'' WHEN 1067 THEN ''Unknown'' ELSE '''' END), NULL)) AS sexual_partner_hiv_status, ',
      'MAX(IF(o.concept_id = 160119, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS sexual_partner_on_art, ',
      'CONCAT_WS('','', ',
        'MAX(IF(o.concept_id = 163310 AND o.value_coded = 162185, ''Detectable viral load'', NULL)), ',
        'MAX(IF(o.concept_id = 163310 AND o.value_coded = 160119, ''On ART for less than 6 months'', NULL)), ',
        'MAX(IF(o.concept_id = 163310 AND o.value_coded = 160571, ''Couple is trying to concieve'', NULL)), ',
        'MAX(IF(o.concept_id = 163310 AND o.value_coded = 159598, ''Suspected poor adherence'', NULL))',
      ') AS risk, ',
      'MAX(IF(o.concept_id = 160581, (CASE o.value_coded WHEN 1065 THEN ''High risk partner'' ELSE '''' END), NULL)) AS high_risk_partner, ',
      'MAX(IF(o.concept_id = 159385, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS sex_with_multiple_partners, ',
      'MAX(IF(o.concept_id = 160579, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS ipv_gbv, ',
      'MAX(IF(o.concept_id = 156660, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS transactional_sex, ',
      'MAX(IF(o.concept_id = 164845, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS recent_sti_infected, ',
      'MAX(IF(o.concept_id = 165088, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS recurrent_pep_use, ',
      'MAX(IF(o.concept_id = 165089, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS recurrent_sex_under_influence, ',
      'MAX(IF(o.concept_id = 165090, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS inconsistent_no_condom_use, ',
      'MAX(IF(o.concept_id = 165091, (CASE o.value_coded WHEN 138643 THEN ''Risk'' WHEN 1066 THEN ''No risk'' ELSE '''' END), NULL)) AS sharing_drug_needles, ',
      'MAX(IF(o.concept_id = 165241, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS other_reasons, ',
      'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS other_reason_specify, ',
      'MAX(IF(o.concept_id = 165053, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS risk_education_offered, ',
      'MAX(IF(o.concept_id = 165092, o.value_text, NULL)) AS risk_reduction, ',
      'MAX(IF(o.concept_id = 165094, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS willing_to_take_prep, ',
      'CONCAT_WS('','', ',
        'MAX(IF(o.concept_id = 1743 AND o.value_coded = 1107, ''None'', NULL)), ',
        'MAX(IF(o.concept_id = 1743 AND o.value_coded = 159935, ''Side effects(ADR)'', NULL)), ',
        'MAX(IF(o.concept_id = 1743 AND o.value_coded = 164997, ''Stigma'', NULL)), ',
        'MAX(IF(o.concept_id = 1743 AND o.value_coded = 160588, ''Pill burden'', NULL)), ',
        'MAX(IF(o.concept_id = 1743 AND o.value_coded = 164401, ''Too many HIV tests'', NULL)), ',
        'MAX(IF(o.concept_id = 1743 AND o.value_coded = 161888, ''Taking pills for a long time'', NULL))',
      ') AS reason_not_willing, ',
      'MAX(IF(o.concept_id = 161595, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS risk_edu_offered, ',
      'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS risk_education, ',
      'CONCAT_WS('','', ',
        'MAX(IF(o.concept_id = 165093 AND o.value_coded = 165276, ''Risk reduction counselling'', NULL)), ',
        'MAX(IF(o.concept_id = 165093 AND o.value_coded = 159612, ''Safer sex practices'', NULL)), ',
        'MAX(IF(o.concept_id = 165093 AND o.value_coded = 162223, ''vmmc-referral'', NULL)), ',
        'MAX(IF(o.concept_id = 165093 AND o.value_coded = 161594, ''Consistent and correct use of male and female Condom with compatible lubricant'', NULL)), ',
        'MAX(IF(o.concept_id = 165093 AND o.value_coded = 165149, ''Post-exposure prophylaxis'', NULL)), ',
        'MAX(IF(o.concept_id = 165093 AND o.value_coded = 164882, ''Prevention and treatment of STIs'', NULL)), ',
        'MAX(IF(o.concept_id = 165093 AND o.value_coded = 165151, ''Substance abuse and mental health treatment'', NULL)), ',
        'MAX(IF(o.concept_id = 165093 AND o.value_coded = 165273, ''Prevention of Violence'', NULL)), ',
        'MAX(IF(o.concept_id = 165093 AND o.value_coded = 1459, ''HIV testing and re-testing'', NULL)), ',
        'MAX(IF(o.concept_id = 165093 AND o.value_coded = 5622, ''Other'', NULL)), ',
        'MAX(IF(o.concept_id = 161550, o.value_text, NULL))',
      ') AS referral_for_prevention_services, ',
      'MAX(IF(o.concept_id = 161550, o.value_text, NULL)) AS referral_facility, ',
      'MAX(IF(o.concept_id = 160082, o.value_datetime, NULL)) AS time_partner_hiv_positive_known, ',
      'MAX(IF(o.concept_id = 165095, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS partner_enrolled_ccc, ',
      'MAX(IF(o.concept_id = 162053, o.value_numeric, NULL)) AS partner_ccc_number, ',
      'MAX(IF(o.concept_id = 159599, o.value_datetime, NULL)) AS partner_art_start_date, ',
      'MAX(IF(o.concept_id = 165096, o.value_datetime, NULL)) AS serodiscordant_confirmation_date, ',
      'MAX(IF(o.concept_id = 164393, o.value_numeric * 12, NULL)) + MAX(IF(o.concept_id = 165356, o.value_numeric, NULL)) AS HIV_serodiscordant_duration_months, ',
      'MAX(IF(o.concept_id = 165097, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS recent_unprotected_sex_with_positive_partner, ',
      'MAX(IF(o.concept_id = 1825, o.value_numeric, NULL)) AS children_with_hiv_positive_partner, ',
      'e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''40374909-05fc-4af8-b789-ed9c394ac785'') ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (1436,160119,163310,160581,159385,160579,156660,164845,165088,165089,165090,165241,160632,165091,165053,165092,165094,1743,161595,161011,165093,161550,160082,165095,162053,159599,165096,1825,164393,165356) ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), provider = VALUES(provider), date_last_modified = VALUES(date_last_modified), ',
      'sexual_partner_hiv_status = VALUES(sexual_partner_hiv_status), sexual_partner_on_art = VALUES(sexual_partner_on_art), risk = VALUES(risk), ',
      'high_risk_partner = VALUES(high_risk_partner), sex_with_multiple_partners = VALUES(sex_with_multiple_partners), ipv_gbv = VALUES(ipv_gbv), transactional_sex = VALUES(transactional_sex), ',
      'recent_sti_infected = VALUES(recent_sti_infected), recurrent_pep_use = VALUES(recurrent_pep_use), recurrent_sex_under_influence = VALUES(recurrent_sex_under_influence), inconsistent_no_condom_use = VALUES(inconsistent_no_condom_use), ',
      'sharing_drug_needles = VALUES(sharing_drug_needles), other_reasons = VALUES(other_reasons), other_reason_specify = VALUES(other_reason_specify), risk_education_offered = VALUES(risk_education_offered), ',
      'risk_reduction = VALUES(risk_reduction), willing_to_take_prep = VALUES(willing_to_take_prep), reason_not_willing = VALUES(reason_not_willing), risk_edu_offered = VALUES(risk_edu_offered), risk_education = VALUES(risk_education), ',
      'referral_for_prevention_services = VALUES(referral_for_prevention_services), referral_facility = VALUES(referral_facility), time_partner_hiv_positive_known = VALUES(time_partner_hiv_positive_known), ',
      'partner_enrolled_ccc = VALUES(partner_enrolled_ccc), partner_ccc_number = VALUES(partner_ccc_number), partner_art_start_date = VALUES(partner_art_start_date), ',
      'serodiscordant_confirmation_date = VALUES(serodiscordant_confirmation_date), HIV_serodiscordant_duration_months = VALUES(HIV_serodiscordant_duration_months), ',
      'recent_unprotected_sex_with_positive_partner = VALUES(recent_unprotected_sex_with_positive_partner), children_with_hiv_positive_partner = VALUES(children_with_hiv_positive_partner), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing Behaviour risk assessment', CONCAT('Time: ', NOW());
END $$

-- ------------- populate etl_prep_monthly_refill-------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_prep_monthly_refill $$
CREATE PROCEDURE sp_update_etl_prep_monthly_refill(IN last_update_time DATETIME)
BEGIN
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_prep_monthly_refill');

SELECT 'Processing monthly refill', CONCAT('Time: ', NOW());

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, date_created, date_last_modified, ',
      'assessed_for_behavior_risk, risk_for_hiv_positive_partner, client_assessment, adherence_assessment, ',
      'poor_adherence_reasons, other_poor_adherence_reasons, adherence_counselling_done, prep_status, switching_option, switching_date, ',
      'prep_type, prescribed_prep_today, prescribed_regimen, prescribed_regimen_months, number_of_condoms_issued, ',
      'prep_discontinue_reasons, prep_discontinue_other_reasons, appointment_given, remarks, voided',
    ') ',
    'SELECT ',
      'e.uuid, e.creator AS provider, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 138643, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' END), NULL)) AS assessed_for_behavior_risk, ',
      'MAX(IF(o.concept_id = 1169, (CASE o.value_coded WHEN 160571 THEN ''Couple is trying to conceive'' WHEN 159598 THEN ''Suspected poor adherence'' WHEN 160119 THEN ''On ART for less than 6 months'' WHEN 162854 THEN ''Not on ART'' ELSE '''' END), '''')) AS risk_for_hiv_positive_partner, ',
      'MAX(IF(o.concept_id = 162189, (CASE o.value_coded WHEN 159385 THEN ''Has Sex with more than one partner'' WHEN 1402 THEN ''Sex partner(s) at high risk for HIV and HIV status unknown'' WHEN 160579 THEN ''Transactional sex'' WHEN 165088 THEN ''Recurrent sex under influence of alcohol/recreational drugs'' WHEN 165089 THEN ''Inconsistent or no condom use'' WHEN 165090 THEN ''Injecting drug use with shared needles and/or syringes'' WHEN 164845 THEN ''Recurrent use of Post Exposure Prophylaxis (PEP)'' WHEN 112992 THEN ''Recent STI'' WHEN 141814 THEN ''Ongoing IPV/Violence'' ELSE '''' END), '''')) AS client_assessment, ',
      'MAX(IF(o.concept_id = 164075, (CASE o.value_coded WHEN 159405 THEN ''Good'' WHEN 159406 THEN ''Fair'' WHEN 159407 THEN ''Poor'' WHEN 1067 THEN ''Good,Fair,Poor,N/A(Did not pick PrEP at last'' ELSE '''' END), '''')) AS adherence_assessment, ',
      'MAX(IF(o.concept_id = 160582, (CASE o.value_coded WHEN 163293 THEN ''Sick'' WHEN 1107 THEN ''None'' WHEN 164997 THEN ''Stigma'' WHEN 160583 THEN ''Shared with others'' WHEN 1064 THEN ''No perceived risk'' WHEN 160588 THEN ''Pill burden'' WHEN 160584 THEN ''Lost/out of pills'' WHEN 1056 THEN ''Separated from HIV+'' WHEN 159935 THEN ''Side effects'' WHEN 160587 THEN ''Forgot'' WHEN 5622 THEN ''Other-specify'' ELSE '''' END), '''')) AS poor_adherence_reasons, ',
      'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS other_poor_adherence_reasons, ',
      'MAX(IF(o.concept_id = 164425, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS adherence_counselling_done, ',
      'MAX(IF(o.concept_id = 161641, (CASE o.value_coded WHEN 159836 THEN ''Discontinue'' WHEN 162904 THEN ''Restart'' WHEN 164515 THEN ''Switch'' WHEN 159835 THEN ''Continue'' ELSE '''' END), '''')) AS prep_status, ',
      'MAX(IF(o.concept_id = 167788, (CASE o.value_coded WHEN 159737 THEN ''Client Preference'' WHEN 160662 THEN ''Stock-out'' WHEN 121760 THEN ''Adverse Drug Reactions'' WHEN 141748 THEN ''Drug Interactions'' WHEN 167533 THEN ''Discontinuing Injection PrEP'' ELSE '''' END), '''')) AS switching_option, ',
      'MAX(IF(o.concept_id = 165144, o.value_datetime, NULL)) AS switching_date, ',
      'MAX(IF(o.concept_id = 166866, (CASE o.value_coded WHEN 165269 THEN ''Daily Oral PrEP'' WHEN 168050 THEN ''CAB-LA'' WHEN 168049 THEN ''Dapivirine ring'' WHEN 5424 THEN ''Event Driven'' ELSE '''' END), '''')) AS prep_type, ',
      'MAX(IF(o.concept_id = 1417, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS prescribed_prep_today, ',
      'MAX(IF(o.concept_id = 164515, (CASE o.value_coded WHEN 161364 THEN ''TDF/3TC'' WHEN 84795 THEN ''TDF'' WHEN 104567 THEN ''TDF/FTC(Preferred)'' ELSE '''' END), '''')) AS prescribed_regimen, ',
      'MAX(IF(o.concept_id = 164433, o.value_text, NULL)) AS prescribed_regimen_months, ',
      'MAX(IF(o.concept_id = 165055, o.value_numeric, NULL)) AS number_of_condoms_issued, ',
      'MAX(IF(o.concept_id = 161555, (CASE o.value_coded WHEN 138571 THEN ''HIV test is positive'' WHEN 113338 THEN ''Renal dysfunction'' WHEN 1302 THEN ''Viral suppression of HIV+'' WHEN 159598 THEN ''Not adherent to PrEP'' WHEN 164401 THEN ''Too many HIV tests'' WHEN 162696 THEN ''Client request'' WHEN 5622 THEN ''other'' ELSE '''' END), '''')) AS prep_discontinue_reasons, ',
      'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS prep_discontinue_other_reasons, ',
      'MAX(IF(o.concept_id = 164999, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS appointment_given, ',
      'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS remarks, ',
      'e.voided AS voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''291c03c8-a216-11e9-a2a3-2a2ae2dbcce4'') ',
    'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
    'AND o.concept_id IN (1169,162189,164075,160582,165144,166866,167788,160632,164425,161641,1417,164515,164433,161555,164999,161011,5096,138643,165055) ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date), provider=VALUES(provider), date_last_modified=VALUES(date_last_modified), ',
      'assessed_for_behavior_risk=VALUES(assessed_for_behavior_risk), risk_for_hiv_positive_partner=VALUES(risk_for_hiv_positive_partner), client_assessment=VALUES(client_assessment), adherence_assessment=VALUES(adherence_assessment), ',
      'poor_adherence_reasons=VALUES(poor_adherence_reasons), other_poor_adherence_reasons=VALUES(other_poor_adherence_reasons), adherence_counselling_done=VALUES(adherence_counselling_done), prep_status=VALUES(prep_status), switching_option=VALUES(switching_option), switching_date=VALUES(switching_date), ',
      'prep_type=VALUES(prep_type), prescribed_prep_today=VALUES(prescribed_prep_today), prescribed_regimen=VALUES(prescribed_regimen), prescribed_regimen_months=VALUES(prescribed_regimen_months), number_of_condoms_issued=VALUES(number_of_condoms_issued), ',
      'prep_discontinue_reasons=VALUES(prep_discontinue_reasons), prep_discontinue_other_reasons=VALUES(prep_discontinue_other_reasons), appointment_given=VALUES(appointment_given), remarks=VALUES(remarks), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing monthly refill', CONCAT('Time: ', NOW());
END $$

-- ------------- populate etl_prep_discontinuation-------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_prep_discontinuation $$
CREATE PROCEDURE sp_update_etl_prep_discontinuation(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table_quoted VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table_quoted = CONCAT(@etl_schema, '.etl_prep_discontinuation');

SELECT 'Processing PrEP discontinuation', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table_quoted, ' (',
      'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, date_created, date_last_modified, discontinue_reason, care_end_date, last_prep_dose_date, voided',
    ') ',
    'SELECT ',
      'e.uuid, e.creator AS provider, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 161555, (CASE o.value_coded ',
         'WHEN 138571 THEN ''HIV test is positive'' ',
         'WHEN 113338 THEN ''Renal dysfunction'' ',
         'WHEN 1302 THEN ''Viral suppression of HIV+'' ',
         'WHEN 159598 THEN ''Not adherent to PrEP'' ',
         'WHEN 164401 THEN ''Too many HIV tests'' ',
         'WHEN 162696 THEN ''Client request'' ',
         'WHEN 150506 THEN ''Intimate partner violence'' ',
         'WHEN 978 THEN ''Self Discontinuation'' ',
         'WHEN 160581 THEN ''Low risk of HIV'' ',
         'WHEN 121760 THEN ''Adverse drug reaction'' ',
         'WHEN 160034 THEN ''Died'' ',
         'WHEN 159492 THEN ''Transferred Out'' ',
         'WHEN 5240 THEN ''Defaulters (missed drugs pick ups)'' ',
         'WHEN 162479 THEN ''Partner Refusal'' ',
         'WHEN 5622 THEN ''Other'' ',
         'ELSE '''' END), '''')) AS discontinue_reason, ',
      'MAX(IF(o.concept_id = 164073, o.value_datetime, NULL)) AS care_end_date, ',
      'MAX(IF(o.concept_id = 162549, o.value_datetime, NULL)) AS last_prep_dose_date, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''467c4cc3-25eb-4330-9cf6-e41b9b14cc10'') ',
      'INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (161555,164073,162549) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), ',
      'provider = VALUES(provider), ',
      'discontinue_reason = VALUES(discontinue_reason), ',
      'care_end_date = VALUES(care_end_date), ',
      'last_prep_dose_date = VALUES(last_prep_dose_date), ',
      'voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing PrEP discontinuation', CONCAT('Time: ', NOW());
END $$


-- ------------- populate etl_prep_enrollment-------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_prep_enrolment $$
CREATE PROCEDURE sp_update_etl_prep_enrolment(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table_quoted VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table_quoted = CONCAT(@etl_schema, '.etl_prep_enrolment');
SELECT 'Processing PrEP enrolment', CONCAT('Time: ', NOW());
SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table_quoted, ' (',
      'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, date_created, date_last_modified, ',
      'patient_type, population_type, kp_type, transfer_in_entry_point, referred_from, transit_from, transfer_in_date, transfer_from, ',
      'initial_enrolment_date, date_started_prep_trf_facility, previously_on_prep, prep_type, regimen, prep_last_date, in_school, ',
      'buddy_name, buddy_alias, buddy_relationship, buddy_phone, buddy_alt_phone, voided',
    ') ',
    'SELECT ',
      'e.uuid, e.creator AS provider, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 164932, (CASE o.value_coded WHEN 164144 THEN ''New Patient'' WHEN 160563 THEN ''Transfer in'' WHEN 162904 THEN ''Restart'' ELSE '''' END), '''')) AS patient_type, ',
      'MAX(IF(o.concept_id = 164930, o.value_coded, NULL)) AS population_type, ',
      'MAX(IF(o.concept_id = 160581, o.value_coded, NULL)) AS kp_type, ',
      'MAX(IF(o.concept_id = 160540, (CASE o.value_coded WHEN 159938 THEN ''HBTC'' WHEN 160539 THEN ''VCT Site'' WHEN 159937 THEN ''MCH'' WHEN 160536 THEN ''IPD-Adult'' WHEN 160541 THEN ''TB Clinic'' WHEN 160542 THEN ''OPD'' WHEN 162050 THEN ''CCC'' WHEN 160551 THEN ''Self Test'' WHEN 5622 THEN ''Other'' ELSE '''' END), '''')) AS transfer_in_entry_point, ',
      'MAX(IF(o.concept_id = 162724, o.value_text, NULL)) AS referred_from, ',
      'MAX(IF(o.concept_id = 161550, o.value_text, NULL)) AS transit_from, ',
      'MAX(IF(o.concept_id = 160534, o.value_datetime, NULL)) AS transfer_in_date, ',
      'MAX(IF(o.concept_id = 160535, o.value_text, NULL)) AS transfer_from, ',
      'MAX(IF(o.concept_id = 160555, o.value_datetime, NULL)) AS initial_enrolment_date, ',
      'MAX(IF(o.concept_id = 159599, o.value_datetime, NULL)) AS date_started_prep_trf_facility, ',
      'MAX(IF(o.concept_id = 160533, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS previously_on_prep, ',
      'MAX(IF(o.concept_id = 166866, (CASE o.value_coded WHEN 165269 THEN ''Daily Oral PrEP'' WHEN 168050 THEN ''CAB-LA'' WHEN 168049 THEN ''Dapivirine ring'' WHEN 5424 THEN ''Event Driven'' ELSE '''' END), '''')) AS prep_type, ',
      'MAX(IF(o.concept_id = 1088, (CASE o.value_coded WHEN 104567 THEN ''TDF/FTC'' WHEN 84795 THEN ''TDF'' WHEN 161364 THEN ''TDF/3TC'' ELSE '''' END), '''')) AS regimen, ',
      'MAX(IF(o.concept_id = 162881, o.value_datetime, NULL)) AS prep_last_date, ',
      'MAX(IF(o.concept_id = 5629, o.value_coded, NULL)) AS in_school, ',
      'MAX(IF(o.concept_id = 160638, o.value_text, NULL)) AS buddy_name, ',
      'MAX(IF(o.concept_id = 165038, o.value_text, NULL)) AS buddy_alias, ',
      'MAX(IF(o.concept_id = 160640, (CASE o.value_coded WHEN 973 THEN ''Grandparent'' WHEN 972 THEN ''Sibling'' WHEN 160639 THEN ''Guardian'' WHEN 1527 THEN ''Parent'' WHEN 5617 THEN ''Spouse'' WHEN 163565 THEN ''Partner'' WHEN 5622 THEN ''Other'' ELSE '''' END), '''')) AS buddy_relationship, ',
      'MAX(IF(o.concept_id = 160642, o.value_text, NULL)) AS buddy_phone, ',
      'MAX(IF(o.concept_id = 160641, o.value_text, NULL)) AS buddy_alt_phone, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''d5ca78be-654e-4d23-836e-a934739be555'') ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (164932,160540,162724,161550,160534,160535,160555,159599,160533,1088,162881,5629,166866,160638,165038,160640,160642,160641,164930,160581) ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), provider = VALUES(provider), patient_type = VALUES(patient_type), population_type = VALUES(population_type), kp_type = VALUES(kp_type), ',
      'transfer_in_entry_point = VALUES(transfer_in_entry_point), referred_from = VALUES(referred_from), transit_from = VALUES(transit_from), transfer_in_date = VALUES(transfer_in_date), transfer_from = VALUES(transfer_from), ',
      'initial_enrolment_date = VALUES(initial_enrolment_date), date_started_prep_trf_facility = VALUES(date_started_prep_trf_facility), previously_on_prep = VALUES(previously_on_prep), prep_type = VALUES(prep_type), regimen = VALUES(regimen), ',
      'prep_last_date = VALUES(prep_last_date), in_school = VALUES(in_school), buddy_name = VALUES(buddy_name), buddy_alias = VALUES(buddy_alias), buddy_relationship = VALUES(buddy_relationship), ',
      'buddy_phone = VALUES(buddy_phone), buddy_alt_phone = VALUES(buddy_alt_phone), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing PrEP enrolment', CONCAT('Time: ', NOW());
END $$


-- ------------- populate etl_prep_followup-------------------------
-- sql

DROP PROCEDURE IF EXISTS sp_update_etl_prep_followup $$
CREATE PROCEDURE sp_update_etl_prep_followup(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table_quoted VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table_quoted = CONCAT(@etl_schema, '.etl_prep_followup');

SELECT 'Processing PrEP follow-up', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table_quoted, ' (',
      'uuid, form, provider, patient_id, visit_id, visit_date, location_id, encounter_id, date_created, date_last_modified, ',
      'sti_screened, genital_ulcer_disease, vaginal_discharge, cervical_discharge, pid, urethral_discharge, anal_discharge, other_sti_symptoms, sti_treated, ',
      'vmmc_screened, vmmc_status, vmmc_referred, lmp, menopausal_status, pregnant, edd, planned_pregnancy, wanted_pregnancy, breastfeeding, fp_status, fp_method, ',
      'ended_pregnancy, pregnancy_outcome, outcome_date, defects, has_chronic_illness, adverse_reactions, known_allergies, hepatitisB_vaccinated, hepatitisB_treated, ',
      'hepatitisC_vaccinated, hepatitisC_treated, hiv_signs, adherence_counselled, adherence_outcome, poor_adherence_reasons, other_poor_adherence_reasons, ',
      'prep_contraindications, treatment_plan, reason_for_starting_prep, switching_option, switching_date, prep_type, prescribed_PrEP, regimen_prescribed, ',
      'months_prescribed_regimen, condoms_issued, number_of_condoms, appointment_given, reason_no_appointment, clinical_notes, voided',
    ') ',
    'SELECT ',
      'e.uuid, ',
      '(CASE f.uuid WHEN ''1bfb09fc-56d7-4108-bd59-b2765fd312b8'' THEN ''prep-initial'' WHEN ''ee3e2017-52c0-4a54-99ab-ebb542fb8984'' THEN ''prep-consultation'' END) AS form, ',
      'e.creator AS provider, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 161558, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS sti_screened, ',
      'MAX(IF(o.concept_id = 165098 AND o.value_coded = 145762, ''GUD'', NULL)) AS genital_ulcer_disease, ',
      'MAX(IF(o.concept_id = 165098 AND o.value_coded = 121809, ''VG'', NULL)) AS vaginal_discharge, ',
      'MAX(IF(o.concept_id = 165098 AND o.value_coded = 116995, ''CD'', NULL)) AS cervical_discharge, ',
      'MAX(IF(o.concept_id = 165098 AND o.value_coded = 130644, ''PID'', NULL)) AS pid, ',
      'MAX(IF(o.concept_id = 165098 AND o.value_coded = 123529, ''UD'', NULL)) AS urethral_discharge, ',
      'MAX(IF(o.concept_id = 165098 AND o.value_coded = 148895, ''AD'', NULL)) AS anal_discharge, ',
      'MAX(IF(o.concept_id = 165098 AND o.value_coded = 5622, ''Other'', NULL)) AS other_sti_symptoms, ',
      'MAX(IF(o.concept_id = 165200, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS sti_treated, ',
      'MAX(IF(o.concept_id = 165308, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS vmmc_screened, ',
      'MAX(IF(o.concept_id = 165099, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Unknown'' ELSE '''' END), '''')) AS vmmc_status, ',
      'MAX(IF(o.concept_id = 1272, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS vmmc_referred, ',
      'MAX(IF(o.concept_id = 1472, o.value_datetime, NULL)) AS lmp, ',
      'MAX(IF(o.concept_id = 134346, o.value_coded, NULL)) AS menopausal_status, ',
      'MAX(IF(o.concept_id = 5272, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS pregnant, ',
      'MAX(IF(o.concept_id = 5596, o.value_datetime, NULL)) AS edd, ',
      'MAX(IF(o.concept_id = 1426, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS planned_pregnancy, ',
      'MAX(IF(o.concept_id = 164933, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS wanted_pregnancy, ',
      'MAX(IF(o.concept_id = 5632, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS breastfeeding, ',
      'MAX(IF(o.concept_id = 160653, (CASE o.value_coded WHEN 965 THEN ''On Family Planning'' WHEN 160652 THEN ''Not using Family Planning'' WHEN 1360 THEN ''Wants Family Planning'' ELSE '''' END), '''')) AS fp_status, ',
      'CONCAT_WS('','', ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 160570, ''Emergency contraceptive pills'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 780, ''Oral Contraceptives Pills'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 5279, ''Injectable'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 1359, ''Implant'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 136163, ''Lactational Amenorhea Method'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 5275, ''Intrauterine Device'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 5278, ''Diaphram/Cervical Cap'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 5277, ''Fertility Awareness'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 1472, ''Tubal Ligation/Female sterilization'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 190, ''Condoms'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 1489, ''Vasectomy(Partner)'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 162332, ''Undecided'', NULL))',
      ') AS fp_method, ',
      'MAX(IF(o.concept_id = 165103, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS ended_pregnancy, ',
      'MAX(IF(o.concept_id = 161033, (CASE o.value_coded WHEN 1395 THEN ''Term live'' WHEN 129218 THEN ''Preterm Delivery'' WHEN 125872 THEN ''Still birth'' WHEN 159896 THEN ''Induced abortion'' ELSE '''' END), '''')) AS pregnancy_outcome, ',
      'MAX(IF(o.concept_id = 1596, o.value_datetime, NULL)) AS outcome_date, ',
      'MAX(IF(o.concept_id = 164122, (CASE o.value_coded WHEN 155871 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Unknown'' ELSE '''' END), '''')) AS defects, ',
      'MAX(IF(o.concept_id = 162747, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS has_chronic_illness, ',
      'MAX(IF(o.concept_id = 121764, o.value_coded, NULL)) AS adverse_reactions, ',
      'MAX(IF(o.concept_id = 160557, o.value_coded, NULL)) AS known_allergies, ',
      'MAX(IF(o.concept_id = 1272, o.value_coded, NULL)) AS hepatitisB_vaccinated, ',
      'MAX(IF(o.concept_id = 1272, o.value_coded, NULL)) AS hepatitisB_treated, ',
      'MAX(IF(o.concept_id = 1272, o.value_coded, NULL)) AS hepatitisC_vaccinated, ',
      'MAX(IF(o.concept_id = 1272, o.value_coded, NULL)) AS hepatitisC_treated, ',
      'MAX(IF(o.concept_id = 165101, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS hiv_signs, ',
      'MAX(IF(o.concept_id = 165104, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS adherence_counselled, ',
      'MAX(IF(o.concept_id = 164075, (CASE o.value_coded WHEN 159405 THEN ''Good'' WHEN 159406 THEN ''Fair'' WHEN 159407 THEN ''Poor'' ELSE '''' END), '''')) AS adherence_outcome, ',
      'MAX(IF(o.concept_id = 160582, (CASE o.value_coded WHEN 163293 THEN ''Sick'' WHEN 1107 THEN ''None'' WHEN 164997 THEN ''Stigma'' WHEN 160583 THEN ''Shared with others'' WHEN 1064 THEN ''No perceived risk'' WHEN 160588 THEN ''Pill burden'' WHEN 160584 THEN ''Lost/out of pills'' WHEN 1056 THEN ''Separated from HIV+'' WHEN 159935 THEN ''Side effects'' WHEN 160587 THEN ''Forgot'' WHEN 5622 THEN ''Other-specify'' ELSE '''' END), '''')) AS poor_adherence_reasons, ',
      'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS other_poor_adherence_reasons, ',
      'CONCAT_WS('','', ',
        'MAX(IF(o.concept_id = 165106 AND o.value_coded = 1107, ''None'', NULL)), ',
        'MAX(IF(o.concept_id = 165106 AND o.value_coded = 138571, ''Confirmed HIV+'', NULL)), ',
        'MAX(IF(o.concept_id = 165106 AND o.value_coded = 155589, ''Renal impairment'', NULL)), ',
        'MAX(IF(o.concept_id = 165106 AND o.value_coded = 127750, ''Not willing'', NULL)), ',
        'MAX(IF(o.concept_id = 165106 AND o.value_coded = 165105, ''Less than 35ks and under 15 yrs'', NULL))',
      ') AS prep_contraindications, ',
      'MAX(IF(o.concept_id = 165109, (CASE o.value_coded WHEN 1256 THEN ''Start'' WHEN 1260 THEN ''Discontinue'' WHEN 162904 THEN ''Restart'' WHEN 164515 THEN ''Switch'' WHEN 1257 THEN ''Continue'' ELSE '''' END), '''')) AS treatment_plan, ',
      'MAX(IF(o.concept_id = 159623, o.value_coded, NULL)) AS reason_for_starting_prep, ',
      'MAX(IF(o.concept_id = 167788, (CASE o.value_coded WHEN 159737 THEN ''Client Preference'' WHEN 160662 THEN ''Stock-out'' WHEN 121760 THEN ''Adverse Drug Reactions'' WHEN 141748 THEN ''Drug Interactions'' WHEN 167533 THEN ''Discontinuing Injection PrEP'' ELSE '''' END), '''')) AS switching_option, ',
      'MAX(IF(o.concept_id = 165144, o.value_datetime, NULL)) AS switching_date, ',
      'MAX(IF(o.concept_id = 166866, (CASE o.value_coded WHEN 165269 THEN ''Daily Oral PrEP'' WHEN 168050 THEN ''CAB-LA'' WHEN 168049 THEN ''Dapivirine ring'' WHEN 5424 THEN ''Event Driven'' ELSE '''' END), '''')) AS prep_type, ',
      'MAX(IF(o.concept_id = 1417, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS prescribed_PrEP, ',
      'MAX(IF(o.concept_id = 164515, (CASE o.value_coded WHEN 161364 THEN ''TDF/3TC'' WHEN 84795 THEN ''TDF'' WHEN 104567 THEN ''TDF/FTC(Preferred)'' ELSE '''' END), '''')) AS regimen_prescribed, ',
      'MAX(IF(o.concept_id = 164433, o.value_text, NULL)) AS months_prescribed_regimen, ',
      'MAX(IF(o.concept_id = 159777, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS condoms_issued, ',
      'MAX(IF(o.concept_id = 165055, o.value_numeric, NULL)) AS number_of_condoms, ',
      'MAX(IF(o.concept_id = 165353, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), '''')) AS appointment_given, ',
      'MAX(IF(o.concept_id = 165354, (CASE o.value_coded WHEN 165053 THEN ''Risk will no longer exist'' WHEN 159492 THEN ''Intention to transfer out'' ELSE '''' END), '''')) AS reason_no_appointment, ',
      'MAX(IF(o.concept_id = 163042, o.value_text, NULL)) AS clinical_notes, ',
      'e.voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''ee3e2017-52c0-4a54-99ab-ebb542fb8984'', ''1bfb09fc-56d7-4108-bd59-b2765fd312b8'') ',
    'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
    'AND o.concept_id IN (161558,165098,165200,165308,165099,1272,1472,5272,5596,1426,164933,5632,160653,374,159623,165103,167788,165144,166866,161033,1596,164122,162747,1284,159948,1282,1443,1444,160855,159368,1732,121764,1193,159935,162760,1255,160557,160643,160753,165101,165104,165106,165109,159777,165055,165309,5096,165310,163042,134346,164075,160582,160632,1417,164515,164433,165354) ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date), provider=VALUES(provider), sti_screened=VALUES(sti_screened), genital_ulcer_disease=VALUES(genital_ulcer_disease), ',
      'vaginal_discharge=VALUES(vaginal_discharge), cervical_discharge=VALUES(cervical_discharge), pid=VALUES(pid), urethral_discharge=VALUES(urethral_discharge), ',
      'anal_discharge=VALUES(anal_discharge), other_sti_symptoms=VALUES(other_sti_symptoms), sti_treated=VALUES(sti_treated), vmmc_screened=VALUES(vmmc_screened), ',
      'vmmc_status=VALUES(vmmc_status), vmmc_referred=VALUES(vmmc_referred), lmp=VALUES(lmp), menopausal_status=VALUES(menopausal_status), pregnant=VALUES(pregnant), ',
      'edd=VALUES(edd), planned_pregnancy=VALUES(planned_pregnancy), wanted_pregnancy=VALUES(wanted_pregnancy), breastfeeding=VALUES(breastfeeding), ',
      'fp_status=VALUES(fp_status), fp_method=VALUES(fp_method), ended_pregnancy=VALUES(ended_pregnancy), pregnancy_outcome=VALUES(pregnancy_outcome), ',
      'outcome_date=VALUES(outcome_date), defects=VALUES(defects), has_chronic_illness=VALUES(has_chronic_illness), adverse_reactions=VALUES(adverse_reactions), ',
      'known_allergies=VALUES(known_allergies), hepatitisB_vaccinated=VALUES(hepatitisB_vaccinated), hepatitisB_treated=VALUES(hepatitisB_treated), ',
      'hepatitisC_vaccinated=VALUES(hepatitisC_vaccinated), hepatitisC_treated=VALUES(hepatitisC_treated), hiv_signs=VALUES(hiv_signs), adherence_counselled=VALUES(adherence_counselled), ',
      'adherence_outcome=VALUES(adherence_outcome), poor_adherence_reasons=VALUES(poor_adherence_reasons), other_poor_adherence_reasons=VALUES(other_poor_adherence_reasons), ',
      'prep_contraindications=VALUES(prep_contraindications), treatment_plan=VALUES(treatment_plan), reason_for_starting_prep=VALUES(reason_for_starting_prep), ',
      'switching_option=VALUES(switching_option), switching_date=VALUES(switching_date), prep_type=VALUES(prep_type), prescribed_PrEP=VALUES(prescribed_PrEP), ',
      'regimen_prescribed=VALUES(regimen_prescribed), months_prescribed_regimen=VALUES(months_prescribed_regimen), condoms_issued=VALUES(condoms_issued), ',
      'number_of_condoms=VALUES(number_of_condoms), appointment_given=VALUES(appointment_given), reason_no_appointment=VALUES(reason_no_appointment), clinical_notes=VALUES(clinical_notes), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing PrEP follow-up', CONCAT('Time: ', NOW());
END $$




-- ------------- populate etl_progress_note-------------------------

-- sql

DROP PROCEDURE IF EXISTS sp_update_etl_progress_note $$
CREATE PROCEDURE sp_update_etl_progress_note(IN last_update_time DATETIME)
BEGIN
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_progress_note');

SELECT 'Processing progress', CONCAT('Time: ', NOW());

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, date_created, date_last_modified, notes, voided',
    ') ',
    'SELECT ',
      'e.uuid, e.creator AS provider, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 159395, o.value_text, NULL)) AS notes, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''c48ed2a2-0a0f-4f4e-9fed-a79ca3e1a9b9'') ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (159395) ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date), provider=VALUES(provider), notes=VALUES(notes), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing progress', CONCAT('Time: ', NOW());
END $$


-- ------------------------------------- populate tpt initiation -----------------------------



DROP PROCEDURE IF EXISTS sp_update_etl_ipt_initiation $$
CREATE PROCEDURE sp_update_etl_ipt_initiation(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_ipt_initiation');

SELECT 'Processing TPT initiations ', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (patient_id, uuid, encounter_provider, visit_date, location_id, encounter_id, date_created, date_last_modified, ipt_indication, voided) ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 162276, o.value_coded, NULL)) AS ipt_indication, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN (SELECT encounter_type_id FROM encounter_type WHERE uuid IN (''de5cacd4-7d15-4ad0-a1be-d81c77b6c37d'')) et ON et.encounter_type_id = e.encounter_type ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id = 162276 ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date), encounter_provider=VALUES(encounter_provider), ipt_indication=VALUES(ipt_indication), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Completed Processing TPT Initiation ', CONCAT('Time: ', NOW());
END $$


-- ----------------------------------- process tpt outcome ---------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_ipt_outcome $$
CREATE PROCEDURE sp_update_etl_ipt_outcome(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_ipt_outcome');
SELECT 'Processing TPT outcome ', CONCAT('Time: ', NOW());
SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (patient_id, uuid, encounter_provider, visit_date, location_id, encounter_id, date_created, date_last_modified, outcome, voided) ',
    'SELECT ',
      'e.patient_id, e.uuid, e.creator AS encounter_provider, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 161555, o.value_coded, NULL)) AS outcome, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN (SELECT encounter_type_id FROM encounter_type WHERE uuid IN (''bb77c683-2144-48a5-a011-66d904d776c9'')) et ON et.encounter_type_id = e.encounter_type ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id = 161555 ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE visit_date = VALUES(visit_date), encounter_provider = VALUES(encounter_provider), outcome = VALUES(outcome), voided = VALUES(voided);'
  );
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Completed processing TPT outcome ', CONCAT('Time: ', NOW());
END $$

-- --------------------------------------- process HTS linkage tracing ------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_hts_linkage_tracing $$
CREATE PROCEDURE sp_update_etl_hts_linkage_tracing(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_hts_linkage_tracing');

SELECT 'Processing HTS Linkage tracing ', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, patient_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, tracing_type, tracing_outcome, reason_not_contacted, voided) ',
    'SELECT ',
      'e.uuid, e.patient_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id AS encounter_id, e.creator AS encounter_provider, e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 164966, o.value_coded, NULL)) AS tracing_type, ',
      'MAX(IF(o.concept_id = 159811, o.value_coded, NULL)) AS tracing_outcome, ',
      'MAX(IF(o.concept_id = 1779, o.value_coded, NULL)) AS reason_not_contacted, ',
      'e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN (SELECT form_id FROM form WHERE uuid IN (''15ed03d2-c972-11e9-a32f-2a2ae2dbcce4'')) f ON f.form_id = e.form_id ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (164966,159811,1779) ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE visit_date = VALUES(visit_date), encounter_provider = VALUES(encounter_provider), tracing_type = VALUES(tracing_type), tracing_outcome = VALUES(tracing_outcome), reason_not_contacted = VALUES(reason_not_contacted), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Completed processing HTS linkage tracing data ', CONCAT('Time: ', NOW());
END $$


-- ------------------------- process patient program ------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_patient_program $$
CREATE PROCEDURE sp_update_etl_patient_program(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_patient_program');

SELECT 'Updating patient program ', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, patient_id, location_id, program, date_enrolled, date_completed, outcome, date_created, date_last_modified, voided) ',
    'SELECT ',
      'pp.uuid, pp.patient_id, pp.location_id, ',
      'CASE p.uuid ',
        'WHEN ''9f144a34-3a4a-44a9-8486-6b7af6cc64f6'' THEN ''TB'' ',
        'WHEN ''dfdc6d40-2f2f-463d-ba90-cc97350441a8'' THEN ''HIV'' ',
        'WHEN ''c2ecdf11-97cd-432a-a971-cfd9bd296b83'' THEN ''MCH-Child Services'' ',
        'WHEN ''b5d9e05f-f5ab-4612-98dd-adb75438ed34'' THEN ''MCH-Mother Services'' ',
        'WHEN ''335517a1-04bc-438b-9843-1ba49fb7fcd9'' THEN ''TPT'' ',
        'WHEN ''24d05d30-0488-11ea-8d71-362b9e155667'' THEN ''OTZ'' ',
        'WHEN ''6eda83f0-09d9-11ea-8d71-362b9e155667'' THEN ''OVC'' ',
        'WHEN ''7447305a-18a7-11e9-ab14-d663bd873d93'' THEN ''KVP'' ',
        'WHEN ''e41c3d74-37c7-4001-9f19-ef9e35224b70'' THEN ''VIOLENCE SCREENING'' ',
        'WHEN ''228538f4-cad9-476b-84c3-ab0086150bcc'' THEN ''VMMC'' ',
        'WHEN ''4b898e20-9b2d-11ee-b9d1-0242ac120002'' THEN ''MAT'' ',
        'WHEN ''b2b2dd4a-3aa5-4c98-93ad-4970b06819ef'' THEN ''NimeCONFIRM'' ',
        'WHEN ''ffee43c4-9ccd-4e55-8a70-93194e7fafc6'' THEN ''NCD'' ',
        'WHEN ''8cd42506-2ebd-485f-89d6-4bb9ed328ccc'' THEN ''CPM'' ',
        'WHEN ''214cad1c-bb62-4d8e-b927-810a046daf62'' THEN ''PrEP'' ',
      'ELSE NULL END AS program, ',
      'pp.date_enrolled, pp.date_completed, pp.outcome_concept_id, pp.date_created, pp.date_changed AS date_last_modified, pp.voided ',
    'FROM patient_program pp ',
      'INNER JOIN program p ON p.program_id = pp.program_id AND p.retired = 0 ',
      'INNER JOIN person pt ON pt.person_id = pp.patient_id AND pt.voided = 0 ',
    'WHERE pp.date_created >= ''', DATE_FORMAT(last_update_time, '%Y-%m-%d %H:%i:%s'), ''' OR pp.date_changed >= ''', DATE_FORMAT(last_update_time, '%Y-%m-%d %H:%i:%s'), ''' OR pp.date_voided >= ''', DATE_FORMAT(last_update_time, '%Y-%m-%d %H:%i:%s'), ''' ',
    'GROUP BY pp.uuid ',
    'ON DUPLICATE KEY UPDATE ',
      'date_enrolled=VALUES(date_enrolled), ',
      'date_completed=VALUES(date_completed), ',
      'program=VALUES(program), ',
      'outcome=VALUES(outcome), ',
      'date_last_modified=VALUES(date_last_modified), ',
      'voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed updating patient program data ', CONCAT('Time: ', NOW());
END $$


-- ------------------------ create person address table ---------------------


DROP PROCEDURE IF EXISTS sp_update_etl_person_address $$
CREATE PROCEDURE sp_update_etl_person_address(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_person_address');
SELECT 'Processing person addresses ', CONCAT('Time: ', NOW());
SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, patient_id, county, sub_county, location, ward, sub_location, village, postal_address, land_mark, voided) ',
    'SELECT pa.uuid, pa.person_id, COALESCE(pa.country, pa.county_district) AS county, pa.state_province AS sub_county, pa.address6 AS location, pa.address4 AS ward, pa.address5 AS sub_location, pa.city_village AS village, pa.address1 AS postal_address, pa.address2 AS land_mark, pa.voided ',
    'FROM person_address pa ',
    'INNER JOIN person pt ON pt.person_id = pa.person_id AND pt.voided = 0 ',
    'WHERE pa.voided = 0 AND (pa.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR pa.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR pa.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'ON DUPLICATE KEY UPDATE ',
      'county = VALUES(county), sub_county = VALUES(sub_county), location = VALUES(location), ',
      'ward = VALUES(ward), sub_location = VALUES(sub_location), village = VALUES(village), ',
      'postal_address = VALUES(postal_address), land_mark = VALUES(land_mark), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing person_address data ', CONCAT('Time: ', NOW());
END $$


-- --------------------------------------- process OTZ Activity ------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_otz_activity $$
CREATE PROCEDURE sp_update_etl_otz_activity(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_otz_activity');
SELECT 'Processing OTZ Activity ', CONCAT('Time: ', NOW());
SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, patient_id, visit_date, visit_id, location_id, encounter_id, encounter_provider, date_created, date_last_modified, orientation, leadership, participation, treatment_literacy, transition_to_adult_care, making_decision_future, srh, beyond_third_ninety, attended_support_group, remarks, voided) ',
    'SELECT ',
      'e.uuid, e.patient_id, DATE(e.encounter_datetime) AS visit_date, e.visit_id, e.location_id, e.encounter_id AS encounter_id, e.creator AS encounter_provider, e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 165359, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS orientation, ',
      'MAX(IF(o.concept_id = 165361, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS leadership, ',
      'MAX(IF(o.concept_id = 165360, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS participation, ',
      'MAX(IF(o.concept_id = 165364, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS treatment_literacy, ',
      'MAX(IF(o.concept_id = 165363, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS transition_to_adult_care, ',
      'MAX(IF(o.concept_id = 165362, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS making_decision_future, ',
      'MAX(IF(o.concept_id = 165365, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS srh, ',
      'MAX(IF(o.concept_id = 165366, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS beyond_third_ninety, ',
      'MAX(IF(o.concept_id = 165302, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS attended_support_group, ',
      'MAX(IF(o.concept_id = 161011, TRIM(o.value_text), NULL)) AS remarks, ',
      'e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''3ae95d48-0464-11ea-8d71-362b9e155667'')) f ON f.form_id = e.form_id ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (165359,165361,165360,165364,165363,165362,165365,165366,165302,161011) ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.patient_id, e.encounter_id, visit_date ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), visit_id = VALUES(visit_id), location_id = VALUES(location_id), encounter_provider = VALUES(encounter_provider), date_last_modified = VALUES(date_last_modified), ',
      'orientation = VALUES(orientation), leadership = VALUES(leadership), participation = VALUES(participation), treatment_literacy = VALUES(treatment_literacy), transition_to_adult_care = VALUES(transition_to_adult_care), making_decision_future = VALUES(making_decision_future), ',
      'srh = VALUES(srh), beyond_third_ninety = VALUES(beyond_third_ninety), attended_support_group = VALUES(attended_support_group), remarks = VALUES(remarks), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing OTZ activity data ', CONCAT('Time: ', NOW());
END $$



-- --------------------------------------- process OTZ Enrollment ------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_otz_enrollment $$
CREATE PROCEDURE sp_update_etl_otz_enrollment(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_otz_enrollment');
SELECT 'Processing OTZ Enrollment ', CONCAT('Time: ', NOW());
SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, patient_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, orientation, leadership, participation, treatment_literacy, transition_to_adult_care, making_decision_future, srh, beyond_third_ninety, transfer_in, voided) ',
    'SELECT ',
      'e.uuid, e.patient_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id AS encounter_id, e.creator AS encounter_provider, e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 165359, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS orientation, ',
      'MAX(IF(o.concept_id = 165361, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS leadership, ',
      'MAX(IF(o.concept_id = 165360, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS participation, ',
      'MAX(IF(o.concept_id = 165364, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS treatment_literacy, ',
      'MAX(IF(o.concept_id = 165363, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS transition_to_adult_care, ',
      'MAX(IF(o.concept_id = 165362, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS making_decision_future, ',
      'MAX(IF(o.concept_id = 165365, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS srh, ',
      'MAX(IF(o.concept_id = 165366, (CASE o.value_coded WHEN 1065 THEN ''Yes'' ELSE '''' END), NULL)) AS beyond_third_ninety, ',
      'MAX(IF(o.concept_id = 160563, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS transfer_in, ',
      'e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''3ae95898-0464-11ea-8d71-362b9e155667'')) f ON f.form_id = e.form_id ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (165359,165361,165360,165364,165363,165362,165365,165366,160563) ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.patient_id, e.encounter_id, visit_date ',
    'ON DUPLICATE KEY UPDATE ',
      'uuid = VALUES(uuid), visit_date = VALUES(visit_date), location_id = VALUES(location_id), encounter_provider = VALUES(encounter_provider), date_last_modified = VALUES(date_last_modified), ',
      'orientation = VALUES(orientation), leadership = VALUES(leadership), participation = VALUES(participation), treatment_literacy = VALUES(treatment_literacy), transition_to_adult_care = VALUES(transition_to_adult_care), making_decision_future = VALUES(making_decision_future), ',
      'srh = VALUES(srh), beyond_third_ninety = VALUES(beyond_third_ninety), transfer_in = VALUES(transfer_in), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing OTZ enrollment data ', CONCAT('Time: ', NOW());
END $$


-- --------------------------------------- process OTZ Enrollment ------------------------

sql


DROP PROCEDURE IF EXISTS sp_update_etl_ovc_enrolment $$
CREATE PROCEDURE sp_update_etl_ovc_enrolment(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_ovc_enrolment');
SELECT 'Processing OVC Enrolment ', CONCAT('Time: ', NOW());
SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, patient_id, visit_date, location_id, visit_id, encounter_id, encounter_provider, date_created, date_last_modified, caregiver_enrolled_here, caregiver_name, caregiver_gender, relationship_to_client, caregiver_phone_number, client_enrolled_cpims, partner_offering_ovc, ovc_comprehensive_program, dreams_program, ovc_preventive_program, voided) ',
    'SELECT ',
      'e.uuid, e.patient_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.visit_id, e.encounter_id AS encounter_id, e.creator AS encounter_provider, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 163777, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS caregiver_enrolled_here, ',
      'MAX(IF(o.concept_id = 163258, o.value_text, NULL)) AS caregiver_name, ',
      'MAX(IF(o.concept_id = 1533, (CASE o.value_coded WHEN 1534 THEN ''Male'' WHEN 1535 THEN ''Female'' ELSE '''' END), NULL)) AS caregiver_gender, ',
      'MAX(IF(o.concept_id = 164352, (CASE o.value_coded WHEN 1527 THEN ''Parent'' WHEN 974 THEN ''Uncle'' WHEN 972 THEN ''Sibling'' WHEN 162722 THEN ''Childrens home'' WHEN 975 THEN ''Aunt'' ELSE '''' END), NULL)) AS relationship_to_client, ',
      'MAX(IF(o.concept_id = 160642, o.value_text, NULL)) AS caregiver_phone_number, ',
      'MAX(IF(o.concept_id = 163766, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS client_enrolled_cpims, ',
      'MAX(IF(o.concept_id = 165347, o.value_text, NULL)) AS partner_offering_ovc, ',
      'MAX(IF(o.concept_id = 163775 AND o.value_coded = 1141, ''Yes'', NULL)) AS ovc_comprehensive_program, ',
      'MAX(IF(o.concept_id = 163775 AND o.value_coded = 160549, ''Yes'', NULL)) AS dreams_program, ',
      'MAX(IF(o.concept_id = 163775 AND o.value_coded = 164128, ''Yes'', NULL)) AS ovc_preventive_program, ',
      'e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN (SELECT form_id, uuid, name FROM form WHERE uuid IN (''5cf01528-09da-11ea-8d71-362b9e155667'')) f ON f.form_id = e.form_id ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (163777,163258,1533,164352,160642,163766,165347,163775) ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.patient_id, e.encounter_id, visit_date ',
    'ON DUPLICATE KEY UPDATE ',
      'uuid = VALUES(uuid), visit_date = VALUES(visit_date), location_id = VALUES(location_id), visit_id = VALUES(visit_id), encounter_provider = VALUES(encounter_provider), date_last_modified = VALUES(date_last_modified), ',
      'caregiver_enrolled_here = VALUES(caregiver_enrolled_here), caregiver_name = VALUES(caregiver_name), caregiver_gender = VALUES(caregiver_gender), relationship_to_client = VALUES(relationship_to_client), caregiver_phone_number = VALUES(caregiver_phone_number), ',
      'client_enrolled_cpims = VALUES(client_enrolled_cpims), partner_offering_ovc = VALUES(partner_offering_ovc), ovc_comprehensive_program = VALUES(ovc_comprehensive_program), dreams_program = VALUES(dreams_program), ovc_preventive_program = VALUES(ovc_preventive_program), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing OVC enrolment data ', CONCAT('Time: ', NOW());
END $$



-- -------------Update etl_cervical_cancer_screening-------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_cervical_cancer_screening $$
CREATE PROCEDURE sp_update_etl_cervical_cancer_screening(
    IN last_update_time DATETIME
)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_cervical_cancer_screening');

SELECT 'Processing CAXC screening', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
   'uuid, encounter_id, encounter_provider, patient_id, visit_id, visit_date, location_id, date_created, date_last_modified, ',
   'visit_type, screening_type, post_treatment_complication_cause, post_treatment_complication_other, cervical_cancer, ',
   'colposcopy_screening_method, hpv_screening_method, pap_smear_screening_method, via_vili_screening_method, ',
   'colposcopy_screening_result, hpv_screening_result, pap_smear_screening_result, via_vili_screening_result, ',
   'colposcopy_treatment_method, hpv_treatment_method, pap_smear_treatment_method, via_vili_treatment_method, ',
   'colorectal_cancer, fecal_occult_screening_method, colonoscopy_method, fecal_occult_screening_results, colonoscopy_method_results, ',
   'fecal_occult_screening_treatment, colonoscopy_method_treatment, retinoblastoma_cancer, retinoblastoma_eua_screening_method, ',
   'retinoblastoma_gene_method, retinoblastoma_eua_screening_results, retinoblastoma_gene_method_results, retinoblastoma_eua_treatment, ',
   'retinoblastoma_gene_treatment, prostate_cancer, digital_rectal_prostate_examination, digital_rectal_prostate_results, ',
   'digital_rectal_prostate_treatment, prostatic_specific_antigen_test, prostatic_specific_antigen_results, prostatic_specific_antigen_treatment, ',
   'oral_cancer, oral_cancer_visual_exam_method, oral_cancer_cytology_method, oral_cancer_imaging_method, oral_cancer_biopsy_method, ',
   'oral_cancer_visual_exam_results, oral_cancer_cytology_results, oral_cancer_imaging_results, oral_cancer_biopsy_results, ',
   'oral_cancer_visual_exam_treatment, oral_cancer_cytology_treatment, oral_cancer_imaging_treatment, oral_cancer_biopsy_treatment, ',
   'breast_cancer, clinical_breast_examination_screening_method, ultrasound_screening_method, mammography_smear_screening_method, ',
   'clinical_breast_examination_screening_result, ultrasound_screening_result, mammography_screening_result, clinical_breast_examination_treatment_method, ',
   'ultrasound_treatment_method, breast_tissue_diagnosis, breast_tissue_diagnosis_date, reason_tissue_diagnosis_not_done, mammography_treatment_method, ',
   'referred_out, referral_facility, referral_reason, followup_date, hiv_status, smoke_cigarattes, other_forms_tobacco, take_alcohol, ',
   'previous_treatment, previous_treatment_specify, signs_symptoms, signs_symptoms_specify, family_history, number_of_years_smoked, number_of_cigarette_per_day, clinical_notes, voided',
    ') ',
  'SELECT ',
  'e.uuid, e.encounter_id, e.creator AS provider, e.patient_id, e.visit_id, e.encounter_datetime AS visit_date, e.location_id, e.date_created, ',
  'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
  'MAX(IF(o.concept_id = 160288, CASE o.value_coded WHEN 162080 THEN ''Initial visit'' WHEN 161236 THEN ''Routine visit'' WHEN 165381 THEN ''Post treatment visit'' WHEN 1185 THEN ''Treatment visit'' WHEN 165382 THEN ''Post treatment complication'' ELSE '''' END, NULL)) AS visit_type, ',
  'MAX(IF(o.concept_id = 164181, CASE o.value_coded WHEN 164180 THEN ''First time screening'' WHEN 160530 THEN ''Rescreening'' ELSE '''' END, NULL)) AS screening_type, ',
  'MAX(IF(o.concept_id = 1000154, CASE o.value_coded WHEN 1000078 THEN ''Counsel on negative findings'' WHEN 1000121 THEN ''Referred for further evaluation'' ELSE '''' END, NULL)) AS post_treatment_complication_cause, ',
  'max(if(o.concept_id=163042,o.value_text,null)) as post_treatment_complication_other,',
  'max(if(o.concept_id = 116030 and o.value_coded = 116023, ''Yes'', null))as cervical_cancer,',
  'max(if(o.concept_id = 163589 and f.uuid = "0c93b93c-bfef-4d2a-9fbe-16b59ee366e7" and o.value_coded=160705, ''Colposcopy(for positive HPV,VIA or PAP smear)'', if(t.colposcopy_screening_method is not null and f.uuid="be5c5602-0a1d-11eb-9e20-37d2e56925ee", t.colposcopy_screening_method, null))) as colposcopy_screening_method,',
  'max(if(o.concept_id = 163589 and f.uuid = "0c93b93c-bfef-4d2a-9fbe-16b59ee366e7" and o.value_coded=159859, ''HPV'', if(t.hpv_screening_method is not null and f.uuid="be5c5602-0a1d-11eb-9e20-37d2e56925ee", t.hpv_screening_method, null))) as hpv_screening_method,',
  'max(if(o.concept_id = 163589 and f.uuid = "0c93b93c-bfef-4d2a-9fbe-16b59ee366e7" and o.value_coded=885, ''Pap Smear'', if(t.pap_smear_screening_method is not null and f.uuid="be5c5602-0a1d-11eb-9e20-37d2e56925ee", t.pap_smear_screening_method, null))) as pap_smear_screening_method,',
  'max(if(o.concept_id = 163589 and f.uuid = "0c93b93c-bfef-4d2a-9fbe-16b59ee366e7" and o.value_coded in (164805,164977), ''VIA'', if(t.via_vili_screening_method is not null and f.uuid="be5c5602-0a1d-11eb-9e20-37d2e56925ee", t.via_vili_screening_method, null))) as via_vili_screening_method,',
  'max(if(t3.colposcopy_screening_result is not null and f.uuid = "0c93b93c-bfef-4d2a-9fbe-16b59ee366e7", t3.colposcopy_screening_result, if(t.colposcopy_screening_result is not null and f.uuid="be5c5602-0a1d-11eb-9e20-37d2e56925ee", t.colposcopy_screening_result, null))) as colposcopy_screening_result,',
  'max(if(t2.hpv_screening_result is not null and f.uuid = "0c93b93c-bfef-4d2a-9fbe-16b59ee366e7", t2.hpv_screening_result, if(t.hpv_screening_result is not null and f.uuid="be5c5602-0a1d-11eb-9e20-37d2e56925ee", t.hpv_screening_result, null))) as hpv_screening_result,',
  'max(if(t4.pap_smear_screening_result is not null and f.uuid = "0c93b93c-bfef-4d2a-9fbe-16b59ee366e7", t4.pap_smear_screening_result, if(t.pap_smear_screening_result is not null and f.uuid="be5c5602-0a1d-11eb-9e20-37d2e56925ee", t.pap_smear_screening_result, null))) as pap_smear_screening_result,',
  'max(if(t1.via_vili_screening_result is not null and f.uuid = "0c93b93c-bfef-4d2a-9fbe-16b59ee366e7", t1.via_vili_screening_result, if(t.via_vili_screening_result is not null and f.uuid="be5c5602-0a1d-11eb-9e20-37d2e56925ee", t.via_vili_screening_result, null))) as via_vili_screening_result,',
  'max(if(t3.colposcopy_treatment_method is not null and f.uuid = "0c93b93c-bfef-4d2a-9fbe-16b59ee366e7", t3.colposcopy_treatment_method, if(t.colposcopy_treatment_method is not null and f.uuid="be5c5602-0a1d-11eb-9e20-37d2e56925ee", t.colposcopy_treatment_method, null))) as colposcopy_treatment_method,',
  'max(if(t2.hpv_treatment_method is not null and f.uuid = "0c93b93c-bfef-4d2a-9fbe-16b59ee366e7", t2.hpv_treatment_method, if(t.hpv_treatment_method is not null and f.uuid="be5c5602-0a1d-11eb-9e20-37d2e56925ee", t.hpv_treatment_method, null))) as hpv_treatment_method,',
  'max(if(t4.pap_smear_treatment_method is not null and f.uuid = "0c93b93c-bfef-4d2a-9fbe-16b59ee366e7", t4.pap_smear_treatment_method, if(t.pap_smear_treatment_method is not null and f.uuid="be5c5602-0a1d-11eb-9e20-37d2e56925ee", t.pap_smear_treatment_method, null))) as pap_smear_treatment_method,',
  'max(if(t1.via_vili_treatment_method is not null and f.uuid = "0c93b93c-bfef-4d2a-9fbe-16b59ee366e7", t1.via_vili_treatment_method, if(t.via_vili_treatment_method is not null and f.uuid="be5c5602-0a1d-11eb-9e20-37d2e56925ee", t.via_vili_treatment_method, null))) as via_vili_treatment_method,',
  'max(if(o.concept_id = 116030 and o.value_coded = 133350, ''Yes'', null))as colorectal_cancer,',
  'max(if(o.concept_id = 164959 and o.value_coded = 159362, ''fecal occult'', null))as fecal_occult_screening_method,',
  'max(if(o.concept_id = 164959 and o.value_coded = 1000148, ''colonoscopy'', null))as colonoscopy_method,',
  'max(if(o.concept_id=166664, (case o.value_coded when 703 then ''Positive'' when 664 then ''Negative'' else '''' end),null)) as fecal_occult_screening_results ,',
  'max(if(o.concept_id=166664, (case o.value_coded when 1115 then ''No abnormality'' when 148910 then ''Polyps'' when 133350 then ''Suspicious for cancer'' when 118606 then ''Inflammation'' when 5622 then ''Other abnormalities'' else '''' end),null)) as colonoscopy_method_results,',
  'max(if(o.concept_id = 1000147, (case o.value_coded when 1000078 then ''Counsel on negative findings'' when 1000102 then ''Referred for colonoscopy'' else '''' end),null)) as fecal_occult_screening_treatment,',
  'max(if(o.concept_id = 1000148, (case o.value_coded when 1000078 then ''Counsel on negative findings'' when 1000143 then ''Refer for biopsy'' when 1000103 then ''Referred for further management'' when 162907 then ''Refer to surgical resection'' else '''' end),null)) as colonoscopy_method_treatment,',
  'max(if(o.concept_id = 116030 and o.value_coded = 127527, ''Yes'', null))as retinoblastoma_cancer,',
  'max(if(o.concept_id = 163589 and o.value_coded = 1000149, ''EUA(Examination Under Anesthesia)'', null))as retinoblastoma_eua_screening_method,',
  'max(if(o.concept_id = 163589 and o.value_coded = 1000105, ''Retinoblastoma gene (RB1 gene)'', null))as retinoblastoma_gene_method,',
  'max(if(o.concept_id=1000149, (case o.value_coded when 1115 then ''Normal'' when 1116 then ''Abnormal'' else '''' end),null)) as retinoblastoma_eua_screening_results ,',
  'max(if(o.concept_id=1000105, (case o.value_coded when 703 then ''Negative'' when 664 then ''Positive'' else '''' end),null)) as retinoblastoma_gene_method_results,',
  'max(if(o.concept_id = 1000149, (case o.value_coded when 1000078 then ''Counsel on negative findings'' when 1000121 then ''Referred for further evaluation'' else '''' end),null)) as retinoblastoma_eua_treatment,',
      'max(if(o.concept_id = 1000150, (case o.value_coded when 1000078 then ''Counsel on negative findings'' when 1000121 then ''Referred for further evaluation'' else '''' end),null)) as retinoblastoma_gene_treatment,',
  'max(if(o.concept_id = 116030 and o.value_coded = 146221, ''Yes'', null)) as prostate_cancer,',
  'max(if(o.concept_id = 1000107, ''Yes'',null)) as digital_rectal_prostate_examination,',
  'max(if(o.concept_id = 1000107, case o.value_coded when 1115 then ''Normal'' when 1000108 then ''Enlarged'' when 1000109 then ''Hard/lampy'' end, null)) as  digital_rectal_prostate_results,',
  'concat_ws('','',max(if(o.concept_id = 1000111 and o.value_coded = 1712,''Patient education'',null)), max(if(o.concept_id = 1000111 and o.value_coded = 1000078,''Counseled on -ve findings, after (DRE, PSA test and TRUS)'',null)), max(if(o.concept_id = 1000111 and o.value_coded = 1000121,''Performed/reffered for further evaluation(PSA,TRUS Biopsy)'',null))) as digital_rectal_prostate_treatment,',
  'max(if(o.concept_id = 1169, ''Yes'',null)) as prostatic_specific_antigen_test,',
  'max(if(o.concept_id = 1169, case o.value_coded when 1000113 then ''0-4ng/ml'' when 1000114 then ''4-10ng/ml'' when 1000115 then ''>10ng/ml'' end, null)) as prostatic_specific_antigen_results,',
  'max(if(o.concept_id = 1000111 and o.value_coded in (1000081,1000121,1000143), case o.value_coded when 1000081 then ''Routine follow up after 2 years'' when 1000121 then ''Further evaluation'' when 1000121 then ''Further evaluation'' when 1000143 then ''Perform/refer for biopsy'' end, null)) as prostatic_specific_antigen_treatment,',
  'max(if(o.concept_id = 116030 and o.value_coded = 115355, ''Yes'', null))as oral_cancer,',
  'max(if(o.concept_id = 163308, ''Yes'', null))as oral_cancer_visual_exam_method,',
  'max(if(o.concept_id = 167139, ''Yes'', null))as oral_cancer_cytology_method,',
  'max(if(o.concept_id = 1000135, ''Yes'', null))as oral_cancer_imaging_method,',
  'max(if(o.concept_id = 1000136,''Yes'', null))as oral_cancer_biopsy_method,',
  'max(if(o.concept_id = 163308, (case o.value_coded when 1115 then ''Normal'' when 1116 then ''Abnormal'' else '''' end),null)) as oral_cancer_visual_exam_results,',
  'max(if(o.concept_id = 167139, (case o.value_coded when 703 then ''Positive'' when 664 then ''Negative'' else '''' end),null)) as oral_cancer_cytology_results,',
  'max(if(o.concept_id = 1000135, (case o.value_coded when 1115 then ''Normal'' when 1116 then ''Abnormal'' else '''' end),null)) as oral_cancer_imaging_results,',
  'max(if(o.concept_id = 1000136, (case o.value_coded when 1115 then ''Normal'' when 1116 then ''Abnormal'' else '''' end),null)) as oral_cancer_biopsy_results,',
  'max(if(o.concept_id = 1000151, (case o.value_coded when 1000078 then ''Counsel on negative findings'' when 1000121 then ''Referred for further evaluation'' else '''' end),null)) as oral_cancer_visual_exam_treatment,',
  'max(if(o.concept_id = 1000152, (case o.value_coded when 1000078 then ''Counsel on negative findings'' when 1000121 then ''Referred for further evaluation'' else '''' end),null)) as oral_cancer_cytology_treatment,',
  'max(if(o.concept_id = 1000153, (case o.value_coded when 1000078 then ''Counsel on negative findings'' when 1000121 then ''Referred for further evaluation'' else '''' end),null)) as oral_cancer_imaging_treatment,',
  'max(if(o.concept_id = 1000154, (case o.value_coded when 1000078 then ''Counsel on negative findings'' when 1000121 then ''Referred for further evaluation'' else '''' end),null)) as oral_cancer_biopsy_treatment,',
  'max(if(o.concept_id = 116030 and o.value_coded = 116026, ''Yes'', null))as breast_cancer,',
  'max(if(o.concept_id = 1000090 and o.value_coded = 1065, ''clinical breast examination'', null))as clinical_breast_examination_screening_method,',
  'max(if(o.concept_id = 1000090 and o.value_coded = 1000092, ''ultrasound'', null))as ultrasound_screening_method,',
  'max(if(o.concept_id = 159780 and o.value_coded = 163591, ''mammography'', null))as mammography_smear_screening_method,',
  'max(if(o.concept_id=166664, (case o.value_coded when 1115 then ''Normal'' when 1116 then ''Abnormal'' else '''' end),null)) as clinical_breast_examination_screening_result,',
  'max(if(o.concept_id=166664, (case o.value_coded when 1000094 then ''BIRADS 0(Incomplete Need additional imaging evaluation)'' when 1000093 then ''BIRADS 1(Negative),BIRADS 2(Benign)'' when 1000095 then ''BIRADS 2(Benign),'' when 1000096 then ''BIRADS 3(Probably Benign)'' when 1000097 then ''BIRADS 4(Suspicious)'' when 1000098 then ''BIRADS 5(Highly Suggestive of Malignancy)'' when 1000099 then ''BIRADS 6(Known Biopsy-Proven Malignancy)'' else '''' end),null)) as ultrasound_screening_result,',
  'max(if(o.concept_id=166664, (case o.value_coded when 1000094 then ''BIRADS 0(Incomplete Need additional imaging evaluation)'' when 1000093 then ''BIRADS 1(Negative),BIRADS 2(Benign)'' when 1000095 then ''BIRADS 2(Benign),'' when 1000096 then ''BIRADS 3(Probably Benign)'' when 1000097 then ''BIRADS 4(Suspicious)'' when 1000098 then ''BIRADS 5(Highly Suggestive of Malignancy)'' when 1000099 then ''BIRADS 6(Known Biopsy-Proven Malignancy)'' else '''' end),null)) as mammography_screening_result,',
  'max(if(o.concept_id = 1000091,(case o.value_coded when 1000078 then ''Counsel on negative findings'' when 1609 then ''refer for triple assessment'' else '''' end),null))as clinical_breast_examination_treatment_method,',
  'max(if(o.concept_id = 1000145, (case o.value_coded when 1609 then ''Recall for additional imaging'' when 432 then ''Routine mammography screening'' when 164080 then ''Short-interval(6 months) follow-up'' when 136785 then ''Tissue Diagnosis(U/S guided biopsy)'' when 1000103 then ''Referred for further management'' else '''' end),null)) as ultrasound_treatment_method,',
  'max(if(o.concept_id = 1000516, case o.value_coded when 1267 then ''Done'' when 1118 then ''Not done'' end, null)) as breast_tissue_diagnosis,',
  'max(if(o.concept_id = 1000088, o.value_datetime, null)) as breast_tissue_diagnosis_date,',
  'max(if(o.concept_id = 160632, o.value_text, null)) as reason_tissue_diagnosis_not_done,',
  'max(if(o.concept_id = 1000145, (case o.value_coded when 1609 then ''Recall for additional imaging'' when 432 then ''Routine Ultra sound screening'' when 164080 then ''Short-interval(6 months) follow-up'' when 136785 then ''Tissue Diagnosis(U/S guided biopsy)'' when 159619 then ''Surgical excision when clinically appropriate)'' when 1000103 then ''Referred for further management'' else '''' end),null)) as mammography_treatment_method,',
  'max(if(o.concept_id in (1788,165267),(case o.value_coded when 1065 then ''Yes'' when 1066 then ''No'' else '''' end),null)) as referred_out,',
  'max(if(o.concept_id=165268,o.value_text,null)) as referral_facility,',
   'max(if(o.concept_id = 1887, (case o.value_coded when 165388 then ''Site does not have cryotherapy machine'' when 159008 then ''Large lesion, Suspect cancer'' when 1000103 then ''Further evaluation'' when 161194 then ''Diagnostic work up'' when 1185 then ''Treatment'' when 5622 then ''Other'' else '''' end), '''' )) as referral_reason,',
  'max(if(o.concept_id=5096,o.value_datetime,null)) as followup_date,',
  'max(if(o.concept_id=1169,(case o.value_coded when 703 then ''Positive'' when 664 then ''Negative'' when 1067 then ''Unknown'' else '''' end),null)) as hiv_status,',
  'max(if(o.concept_id=163201,(case o.value_coded when 1065 then ''Yes'' when 1066 then ''No'' when 158939 then ''Stopped'' else '''' end),null)) as smoke_cigarattes,',
  'max(if(o.concept_id=163731,(case o.value_coded when 1065 then ''Yes'' when 1066 then ''No'' when 158939 then ''Stopped'' else '''' end),null)) as other_forms_tobacco,',
  'max(if(o.concept_id=159449,(case o.value_coded when 1065 then ''Yes'' when 1066 then ''No'' when 167155 then ''Stopped'' else '''' end),null)) as take_alcohol,',
  'concat_ws('','', max(if(o.concept_id = 162964 and o.value_coded = 1107, ''None'', null)), max(if(o.concept_id = 162964 and o.value_coded = 166917, ''Chemotherapy'', null)), max(if(o.concept_id = 162964 and o.value_coded = 16117, ''Radiotherapy'', null)), max(if(o.concept_id = 162964 and o.value_coded = 160345, ''Hormonal therapy'', null)), max(if(o.concept_id = 162964 and o.value_coded = 5622, ''Other'', null)), max(if(o.concept_id = 162964 and o.value_coded = 159619, ''Surgery'', null))) as previous_treatment,',
  'max(if(o.concept_id=160632,trim(o.value_text),null)) as previous_treatment_specify,',
  'concat_ws('','', max(if(o.concept_id = 1729 and o.value_coded = 1107, ''None'', null)), max(if(o.concept_id = 1729 and o.value_coded = 111, ''Dyspepsia'', null)), max(if(o.concept_id = 1729 and o.value_coded = 117671, ''Blood in stool'', null)), max(if(o.concept_id = 1729 and o.value_coded = 5192, ''Yellow eyes'', null)), max(if(o.concept_id = 1729 and o.value_coded = 840, ''Blood in urine'', null)), max(if(o.concept_id = 1729 and o.value_coded = 132667, ''Nose Bleeding'', null)), max(if(o.concept_id = 1729 and o.value_coded = 5954, ''Difficulty in swallowing'', null)), max(if(o.concept_id = 1729 and o.value_coded = 832, ''Weight loss'', null)), max(if(o.concept_id = 1729 and o.value_coded = 140501, ''Easy fatigability'', null)), max(if(o.concept_id = 1729 and o.value_coded = 150802, ''Abnormal vaginal bleeding'', null)), max(if(o.concept_id = 1729 and o.value_coded = 115844, ''Changing/enlarging skin moles'', null)), max(if(o.concept_id = 1729 and o.value_coded = 120551, ''Chronic skin ulcers'', null)), max(if(o.concept_id = 1729 and o.value_coded = 115919, ''Lumps/swellings'', null)), max(if(o.concept_id = 1729 and o.value_coded = 145455, ''Chronic cough'', null)), max(if(o.concept_id = 1729 and o.value_coded = 115779, ''Persistent headaches'', null)), max(if(o.concept_id = 1729 and o.value_coded = 129452, ''Post-coital bleeding'', null)), max(if(o.concept_id = 1729 and o.value_coded = 151903, ''Changing bowel habits'', null)), max(if(o.concept_id = 1729 and o.value_coded = 5622, ''Other'', null))) as signs_symptoms,',
  'max(if(o.concept_id=161011,trim(o.value_text),null)) as signs_symptoms_specify,',
  'max(if(o.concept_id=160592,(case o.value_coded when 1065 then \"Yes\" when 1066 then \"No\" else \"\" end),null)) as family_history,',
  'max(if(o.concept_id=159931,o.value_numeric,null)) as number_of_years_smoked,',
  'max(if(o.concept_id=1546,o.value_numeric,null)) as number_of_cigarette_per_day,',
  'MAX(IF(o.concept_id = 164879, TRIM(o.value_text), NULL)) AS clinical_notes, ',
  'e.voided ',
  'FROM encounter e ',
  '  INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
  '  INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''be5c5602-0a1d-11eb-9e20-37d2e56925ee'',''0c93b93c-bfef-4d2a-9fbe-16b59ee366e7'') ',
  '  INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (1169,1392,1546,1729,1788,1887,5096,116030,132679,159449,159780,159931,160049,1000516,1000088,160288,160592,160632,161011,162737,162964,163042,163201,163308,163589,163731,164181,164879,164959,165267,165268,165383,166664,167139,1000090,1000091,1000105,1000107,1000111,1000135,1000136,1000145,1000147,1000148,1000149,1000150,1000151,1000152,1000153,1000154) ',
  '  INNER JOIN (',
  '    SELECT',
  '      o.person_id,',
  '      o.encounter_id,',
  '      o.obs_group_id,',
  '      MAX(IF(o.concept_id = 163589, (CASE o.value_coded WHEN 159859 THEN ''HPV'' ELSE '''' END), NULL)) AS hpv_screening_method,',
  '      MAX(IF(o.concept_id = 164934, (CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 159393 THEN ''Suspicious for Cancer'' ELSE '''' END), NULL)) AS hpv_screening_result,',
  '      MAX(IF(o.concept_id IN (165070,160705), (CASE o.value_coded WHEN 1065 THEN ''Counseled on negative results'' WHEN 703 THEN ''Do a VIA or colposcopy'' ELSE '''' END), NULL)) AS hpv_treatment_method,',
  '      MAX(IF(o.concept_id = 163589, (CASE o.value_coded WHEN 164977 THEN ''VIA'' ELSE '''' END), NULL)) AS via_vili_screening_method,',
  '      MAX(IF(o.concept_id = 164934, (CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 159008 THEN ''Suspicious for Cancer'' ELSE '''' END), NULL)) AS via_vili_screening_result,',
  '      MAX(IF(o.concept_id IN (165070,165266,166937), (CASE o.value_coded WHEN 1065 THEN ''Counseled on negative results'' WHEN 165385 THEN ''Cryotherapy performed (SVA)'' WHEN 165381 THEN ''Cryotherapy postponed'' WHEN 165386 THEN ''Cryotherapy performed (previously postponed)'' WHEN 1648 THEN ''Referred for cryotherapy'' WHEN 162810 THEN ''LEEP performed'' WHEN 165396 THEN ''Cold knife cone'' WHEN 165395 THEN ''Thermal ablation performed (SVA)'' WHEN 159837 THEN ''Hysterectomy'' WHEN 165391 THEN ''Referred for cancer treatment'' WHEN 161826 THEN ''Perform biopsy and/or refer for further management'' ELSE '''' END), NULL)) AS via_vili_treatment_method,',
  '      MAX(IF(o.concept_id = 163589, (CASE o.value_coded WHEN 885 THEN ''Pap Smear'' ELSE '''' END), NULL)) AS pap_smear_screening_method,',
  '      MAX(IF(o.concept_id = 164934, (CASE o.value_coded WHEN 1115 THEN ''Normal'' WHEN 145808 THEN ''Low grade lesion'' WHEN 145805 THEN ''High grade lesion'' WHEN 155424 THEN ''Invasive Cancer'' WHEN 145822 THEN ''Atypical squamous cells(ASC-US/ASC-H)'' WHEN 155208 THEN ''AGUS'' ELSE '''' END), NULL)) AS pap_smear_screening_result,',
  '      MAX(IF(o.concept_id IN (165070,1272), (CASE o.value_coded WHEN 1065 THEN ''Counseled on negative results'' WHEN 160705 THEN ''Refer for colposcopy'' WHEN 161826 THEN ''Refer for biopsy'' ELSE '''' END), NULL)) AS pap_smear_treatment_method,',
  '      MAX(IF(o.concept_id = 163589, (CASE o.value_coded WHEN 160705 THEN ''Colposcopy(for positive HPV,VIA or PAP smear)'' ELSE '''' END), NULL)) AS colposcopy_screening_method,',
  '      MAX(IF(o.concept_id = 164934, (CASE o.value_coded WHEN 1115 THEN ''Normal'' WHEN 1116 THEN ''Abnormal'' WHEN 159008 THEN ''Suspicious for Cancer'' ELSE '''' END), NULL)) AS colposcopy_screening_result,',
  '      MAX(IF(o.concept_id IN (165070,166665,160705,165266), (CASE o.value_coded WHEN 1065 THEN ''Counseled on negative results'' WHEN 162812 THEN ''Cryotherapy'' WHEN 165395 THEN ''Thermal ablation'' WHEN 166620 THEN ''Loop electrosurgical excision'' WHEN 1000103 THEN ''Refer for appropriate diagnosis and management'' WHEN 165385 THEN ''Cryotherapy performed (SVA)'' WHEN 165381 THEN ''Cryotherapy postponed'' WHEN 162810 THEN ''Cryotherapy performed (previously postponed)'' WHEN 1648 THEN ''Referred for cryotherapy'' WHEN 165396 THEN ''LEEP performed'' WHEN 165395 THEN ''Thermal ablation performed (SVA)'' WHEN 159837 THEN ''Hysterectomy'' WHEN 165391 THEN ''Referred for cancer treatment'' WHEN 165995 THEN ''Other treatment'' ELSE '''' END), NULL)) AS colposcopy_treatment_method',
  '    FROM obs o',
  '    INNER JOIN encounter e ON e.encounter_id = o.encounter_id',
  '    INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''be5c5602-0a1d-11eb-9e20-37d2e56925ee'',''0c93b93c-bfef-4d2a-9fbe-16b59ee366e7'') ',
  '    WHERE o.concept_id IN (163589,164934,116030,165070,160705,165266,166937,1272,166665,159362,1000148,163591,159780,1000092,1000149,1000105,116030,163589,162737,132679,1392,166664,160049) AND o.voided = 0 ',
  '    GROUP BY e.encounter_id, o.obs_group_id',
  '  ) t ON e.encounter_id = t.encounter_id ',
  '  LEFT JOIN (',
  '    SELECT',
  '      o.person_id,',
  '      o.encounter_id,',
  '      MAX(IF(o1.concept_id = 164934, (CASE o1.value_coded WHEN 703 THEN ''Positive'' WHEN 1116 THEN ''Positive'' WHEN 145805 THEN ''Positive'' WHEN 155424 THEN ''Positive'' WHEN 145808 THEN ''Presumed'' WHEN 159393 THEN ''Presumed'' WHEN 159008 THEN ''Presumed'' WHEN 5622 THEN ''Other'' WHEN 1115 THEN ''Negative'' WHEN 664 THEN ''Negative'' ELSE NULL END), '''')) AS via_vili_screening_result,',
  '      MAX(IF(o1.concept_id = 165266, (CASE o1.value_coded WHEN 165381 THEN ''Cryotherapy postponed'' WHEN 165386 THEN ''Cryotherapy performed'' WHEN 162810 THEN ''LEEP'' WHEN 165396 THEN ''Cold knife cone'' WHEN 165395 THEN ''Thermocoagulation'' WHEN 165385 THEN ''Cryotherapy performed (single Visit)'' WHEN 159837 THEN ''Hysterectomy'' WHEN 165391 THEN ''Referred for cancer treatment'' WHEN 1107 THEN ''None'' WHEN 5622 THEN ''Other'' ELSE '''' END), '''')) AS via_vili_treatment_method',
  '    FROM obs o',
  '    INNER JOIN encounter e ON e.encounter_id = o.encounter_id',
  '    INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''0c93b93c-bfef-4d2a-9fbe-16b59ee366e7'')',
  '    LEFT JOIN obs o1 ON e.encounter_id = o1.encounter_id AND o1.concept_id IN (165266,164934)',
  '    WHERE o.concept_id = 163589 AND o.value_coded IN (164805,164977) AND o.voided = 0',
  '    GROUP BY e.encounter_id ',
  '  ) t1 ON e.encounter_id = t1.encounter_id ',
  '  LEFT JOIN (',
  '    SELECT',
  '      o.person_id,',
  '      o.encounter_id,',
  '      MAX(IF(o1.concept_id = 164934, (CASE o1.value_coded WHEN 703 THEN ''Positive'' WHEN 1116 THEN ''Positive'' WHEN 145805 THEN ''Positive'' WHEN 155424 THEN ''Positive'' WHEN 145808 THEN ''Presumed'' WHEN 159393 THEN ''Presumed'' WHEN 159008 THEN ''Presumed'' WHEN 5622 THEN ''Other'' WHEN 1115 THEN ''Negative'' WHEN 664 THEN ''Negative'' ELSE NULL END), '''')) AS hpv_screening_result,',
  '      MAX(IF(o1.concept_id = 165266, (CASE o1.value_coded WHEN 165381 THEN ''Cryotherapy postponed'' WHEN 165386 THEN ''Cryotherapy performed'' WHEN 162810 THEN ''LEEP'' WHEN 165396 THEN ''Cold knife cone'' WHEN 165395 THEN ''Thermocoagulation'' WHEN 165385 THEN ''Cryotherapy performed (single Visit)'' WHEN 159837 THEN ''Hysterectomy'' WHEN 165391 THEN ''Referred for cancer treatment'' WHEN 1107 THEN ''None'' WHEN 5622 THEN ''Other'' ELSE '''' END), '''')) AS hpv_treatment_method',
  '    FROM obs o',
  '    INNER JOIN encounter e ON e.encounter_id = o.encounter_id',
  '    INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''0c93b93c-bfef-4d2a-9fbe-16b59ee366e7'')',
  '    LEFT JOIN obs o1 ON e.encounter_id = o1.encounter_id AND o1.concept_id IN (165266,164934)',
  '    WHERE o.concept_id = 163589 AND o.value_coded = 159859 AND o.voided = 0',
  '    GROUP BY e.encounter_id ',
  '  ) t2 ON e.encounter_id = t2.encounter_id ',
  '  LEFT JOIN (',
  '    SELECT',
  '      o.person_id,',
  '      o.encounter_id,',
  '      MAX(IF(o1.concept_id = 164934, (CASE o1.value_coded WHEN 703 THEN ''Positive'' WHEN 1116 THEN ''Positive'' WHEN 145805 THEN ''Positive'' WHEN 155424 THEN ''Positive'' WHEN 145808 THEN ''Presumed'' WHEN 159393 THEN ''Presumed'' WHEN 159008 THEN ''Presumed'' WHEN 5622 THEN ''Other'' WHEN 1115 THEN ''Negative'' WHEN 664 THEN ''Negative'' ELSE NULL END), '''')) AS colposcopy_screening_result,',
  '      MAX(IF(o1.concept_id = 165266, (CASE o1.value_coded WHEN 165381 THEN ''Cryotherapy postponed'' WHEN 165386 THEN ''Cryotherapy performed'' WHEN 162810 THEN ''LEEP'' WHEN 165396 THEN ''Cold knife cone'' WHEN 165395 THEN ''Thermocoagulation'' WHEN 165385 THEN ''Cryotherapy performed (single Visit)'' WHEN 159837 THEN ''Hysterectomy'' WHEN 165391 THEN ''Referred for cancer treatment'' WHEN 1107 THEN ''None'' WHEN 5622 THEN ''Other'' ELSE '''' END), '''')) AS colposcopy_treatment_method',
  '    FROM obs o',
  '    INNER JOIN encounter e ON e.encounter_id = o.encounter_id',
  '    INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''0c93b93c-bfef-4d2a-9fbe-16b59ee366e7'')',
  '    LEFT JOIN obs o1 ON e.encounter_id = o1.encounter_id AND o1.concept_id IN (165266,164934)',
  '    WHERE o.concept_id = 163589 AND o.value_coded = 160705 AND o.voided = 0',
  '    GROUP BY e.encounter_id ',
  '  ) t3 ON e.encounter_id = t3.encounter_id ',
  '  LEFT JOIN (',
  '    SELECT',
  '      o.person_id,',
  '      o.encounter_id,',
  '      MAX(IF(o1.concept_id = 164934, (CASE o1.value_coded WHEN 703 THEN ''Positive'' WHEN 1116 THEN ''Positive'' WHEN 145805 THEN ''Positive'' WHEN 155424 THEN ''Positive'' WHEN 145808 THEN ''Presumed'' WHEN 159393 THEN ''Presumed'' WHEN 159008 THEN ''Presumed'' WHEN 5622 THEN ''Other'' WHEN 1115 THEN ''Negative'' WHEN 664 THEN ''Negative'' ELSE NULL END), '''')) AS pap_smear_screening_result,',
  '      MAX(IF(o1.concept_id = 165266, (CASE o1.value_coded WHEN 165381 THEN ''Cryotherapy postponed'' WHEN 165386 THEN ''Cryotherapy performed'' WHEN 162810 THEN ''LEEP'' WHEN 165396 THEN ''Cold knife cone'' WHEN 165395 THEN ''Thermocoagulation'' WHEN 165385 THEN ''Cryotherapy performed (single Visit)'' WHEN 159837 THEN ''Hysterectomy'' WHEN 165391 THEN ''Referred for cancer treatment'' WHEN 1107 THEN ''None'' WHEN 5622 THEN ''Other'' ELSE '''' END), '''')) AS pap_smear_treatment_method',
  '    FROM obs o',
  '    INNER JOIN encounter e ON e.encounter_id = o.encounter_id',
  '    INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''0c93b93c-bfef-4d2a-9fbe-16b59ee366e7'')',
  '    LEFT JOIN obs o1 ON e.encounter_id = o1.encounter_id AND o1.concept_id IN (165266,164934)',
  '    WHERE o.concept_id = 163589 AND o.value_coded = 885 AND o.voided = 0',
  '    GROUP BY e.encounter_id ',
  '  ) t4 ON e.encounter_id = t4.encounter_id ',
  'WHERE e.voided = 0 AND ',
  '  (e.date_created >= last_update_time ',
  '    OR e.date_changed >= last_update_time ',
  '    OR e.date_voided >= last_update_time ',
  '    OR o.date_created >= last_update_time ',
  '    OR o.date_voided >= last_update_time)',
  'GROUP BY e.encounter_id ',
'ON DUPLICATE KEY UPDATE ',
'visit_date=VALUES(visit_date), encounter_provider=VALUES(encounter_provider), ',
'visit_type=VALUES(visit_type), screening_type=VALUES(screening_type), ',
'post_treatment_complication_cause=VALUES(post_treatment_complication_cause), post_treatment_complication_other=VALUES(post_treatment_complication_other), cervical_cancer=VALUES(cervical_cancer), ',
'colposcopy_screening_method=VALUES(colposcopy_screening_method), hpv_screening_method=VALUES(hpv_screening_method), ',
'pap_smear_screening_method=VALUES(pap_smear_screening_method), via_vili_screening_method=VALUES(via_vili_screening_method), ',
'colposcopy_screening_result=VALUES(colposcopy_screening_result), hpv_screening_result=VALUES(hpv_screening_result), ',
'pap_smear_screening_result=VALUES(pap_smear_screening_result), via_vili_screening_result=VALUES(via_vili_screening_result), ',
'colposcopy_treatment_method=VALUES(colposcopy_treatment_method), hpv_treatment_method=VALUES(hpv_treatment_method), ',
'pap_smear_treatment_method=VALUES(pap_smear_treatment_method), via_vili_treatment_method=VALUES(via_vili_treatment_method), ',
'referred_out=VALUES(referred_out), ',
'retinoblastoma_cancer=VALUES(retinoblastoma_cancer), ',
'retinoblastoma_eua_screening_method=VALUES(retinoblastoma_eua_screening_method), ',
'retinoblastoma_gene_method=VALUES(retinoblastoma_gene_method), ',
'retinoblastoma_eua_screening_results=VALUES(retinoblastoma_eua_screening_results), ',
'retinoblastoma_gene_method_results=VALUES(retinoblastoma_gene_method_results), ',
'retinoblastoma_eua_treatment=VALUES(retinoblastoma_eua_treatment), ',
'retinoblastoma_gene_treatment=VALUES(retinoblastoma_gene_treatment), ',
'oral_cancer=VALUES(oral_cancer), ',
'oral_cancer_visual_exam_method=VALUES(oral_cancer_visual_exam_method), ',
'oral_cancer_cytology_method=VALUES(oral_cancer_cytology_method), ',
'oral_cancer_imaging_method=VALUES(oral_cancer_imaging_method), ',
'oral_cancer_biopsy_method=VALUES(oral_cancer_biopsy_method), ',
'oral_cancer_visual_exam_results=VALUES(oral_cancer_visual_exam_results), ',
'oral_cancer_cytology_results=VALUES(oral_cancer_cytology_results), ',
'oral_cancer_imaging_results=VALUES(oral_cancer_imaging_results), ',
'oral_cancer_biopsy_results=VALUES(oral_cancer_biopsy_results), ',
'oral_cancer_visual_exam_treatment=VALUES(oral_cancer_visual_exam_treatment), ',
'oral_cancer_cytology_treatment=VALUES(oral_cancer_cytology_treatment), ',
'oral_cancer_imaging_treatment=VALUES(oral_cancer_imaging_treatment), ',
'oral_cancer_biopsy_treatment=VALUES(oral_cancer_biopsy_treatment), ',
'prostate_cancer=VALUES(prostate_cancer), ',
'digital_rectal_prostate_examination=VALUES(digital_rectal_prostate_examination), ',
'digital_rectal_prostate_results=VALUES(digital_rectal_prostate_results), ',
'digital_rectal_prostate_treatment=VALUES(digital_rectal_prostate_treatment), ',
'prostatic_specific_antigen_test=VALUES(prostatic_specific_antigen_test), ',
'prostatic_specific_antigen_results=VALUES(prostatic_specific_antigen_results), ',
'prostatic_specific_antigen_treatment=VALUES(prostatic_specific_antigen_treatment), ',
'referral_facility=VALUES(referral_facility), ',
'colorectal_cancer=VALUES(colorectal_cancer), fecal_occult_screening_method=VALUES(fecal_occult_screening_method), ',
'colonoscopy_method=VALUES(colonoscopy_method), fecal_occult_screening_results=VALUES(fecal_occult_screening_results), ',
'colonoscopy_method_results=VALUES(colonoscopy_method_results), fecal_occult_screening_treatment=VALUES(fecal_occult_screening_treatment), ',
'colonoscopy_method_treatment=VALUES(colonoscopy_method_treatment), breast_cancer=VALUES(breast_cancer), ',
'clinical_breast_examination_screening_method=VALUES(clinical_breast_examination_screening_method), ultrasound_screening_method=VALUES(ultrasound_screening_method), ',
'mammography_smear_screening_method=VALUES(mammography_smear_screening_method), clinical_breast_examination_screening_result=VALUES(clinical_breast_examination_screening_result), ',
'ultrasound_screening_result=VALUES(ultrasound_screening_result), mammography_screening_result=VALUES(mammography_screening_result), ',
'clinical_breast_examination_treatment_method=VALUES(clinical_breast_examination_treatment_method), ultrasound_treatment_method=VALUES(ultrasound_treatment_method), ',
'breast_tissue_diagnosis=VALUES(breast_tissue_diagnosis), breast_tissue_diagnosis_date=VALUES(breast_tissue_diagnosis_date), reason_tissue_diagnosis_not_done=VALUES(reason_tissue_diagnosis_not_done), ',
'mammography_treatment_method=VALUES(mammography_treatment_method), ',
'referral_reason=VALUES(referral_reason), followup_date=VALUES(followup_date), hiv_status=VALUES(hiv_status), ',
'smoke_cigarattes=VALUES(smoke_cigarattes), other_forms_tobacco=VALUES(other_forms_tobacco), ',
'take_alcohol=VALUES(take_alcohol), previous_treatment=VALUES(previous_treatment), ',
'previous_treatment_specify=VALUES(previous_treatment_specify), signs_symptoms=VALUES(signs_symptoms), ',
'signs_symptoms_specify=VALUES(signs_symptoms_specify), family_history=VALUES(family_history), ',
'number_of_years_smoked=VALUES(number_of_years_smoked), number_of_cigarette_per_day=VALUES(number_of_cigarette_per_day), ',
'clinical_notes=VALUES(clinical_notes), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing Cervical Cancer Screening', CONCAT('Time: ', NOW());
SELECT 'Completed processing CAXC screening', CONCAT('Time: ', NOW());
END $$


		--------------------------- process patient contact ------------------------
;
DROP PROCEDURE IF EXISTS sp_update_etl_patient_contact $$
CREATE PROCEDURE sp_update_etl_patient_contact(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_patient_contact');
SELECT 'Processing patient contact ', CONCAT('Time: ', NOW());
SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid,date_created,encounter_id,encounter_provider,location_id,patient_id,patient_related_to,relationship_type,start_date,end_date,date_last_modified,voided',
    ') ',
    'SELECT ',
      'p.uuid, DATE(e.encounter_datetime) AS date_created, e.encounter_id, e.creator AS encounter_provider, e.location_id, ',
      'r.patient_contact AS patient_id, r.patient_related_to, r.relationship AS relationship_type, r.start_date, r.end_date, e.date_changed AS date_last_modified, e.voided ',
    'FROM encounter e ',
    'LEFT JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid = ''de1f9d67-b73e-4e1b-90d0-036166fc6995'') et ON et.encounter_type_id = e.encounter_type ',
    'INNER JOIN (',
      'SELECT r.person_a AS patient_related_to, r.person_b AS patient_contact, r.relationship, r.start_date, r.end_date, r.date_created, r.date_changed, r.voided ',
      'FROM relationship r ',
      'INNER JOIN relationship_type t ON r.relationship = t.relationship_type_id ',
      'INNER JOIN person pr ON pr.person_id = r.person_a AND pr.voided = 0',
    ') r ON e.patient_id = r.patient_contact AND r.voided = 0 AND (r.end_date IS NULL OR r.end_date > CURRENT_DATE) ',
    'INNER JOIN person p ON p.person_id = r.patient_contact AND p.voided = 0 ',
    'LEFT JOIN (SELECT person_id FROM person_attribute pa JOIN person_attribute_type t ON pa.person_attribute_type_id = t.person_attribute_type_id AND t.uuid = ''7c94bd35-fba7-4ef7-96f5-29c89a318fcf'') pt ON e.patient_id = pt.person_id ',
    'WHERE e.voided = 0 ',
    'GROUP BY patient_contact ',
    'ON DUPLICATE KEY UPDATE ',
      'start_date = VALUES(start_date), end_date = VALUES(end_date), patient_related_to = VALUES(patient_related_to), relationship_type = VALUES(relationship_type);'
  );
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT(
    'UPDATE ', @target_table, ' c JOIN (',
      'SELECT pa.person_id, ',
        'MAX(IF(pat.uuid = ''3ca03c84-632d-4e53-95ad-91f1bd9d96d6'', CASE pa.value WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1067 THEN ''Unknown'' END, NULL)) AS baseline_hiv_status, ',
        'MAX(IF(pat.uuid = ''35a08d84-9f80-4991-92b4-c4ae5903536e'', pa.value, NULL)) AS living_with_patient, ',
        'MAX(IF(pat.uuid = ''59d1b886-90c8-4f7f-9212-08b20a9ee8cf'', pa.value, NULL)) AS pns_approach, ',
        'MAX(IF(pat.uuid = ''49c543c2-a72a-4b0a-8cca-39c375c0726f'', pa.value, NULL)) AS ipv_outcome ',
      'FROM person_attribute pa ',
      'INNER JOIN person p ON p.person_id = pa.person_id AND p.voided = 0 ',
      'INNER JOIN (SELECT person_attribute_type_id, name, uuid FROM person_attribute_type WHERE retired = 0) pat ON pat.person_attribute_type_id = pa.person_attribute_type_id ',
      'AND pat.uuid IN (''3ca03c84-632d-4e53-95ad-91f1bd9d96d6'',''35a08d84-9f80-4991-92b4-c4ae5903536e'',''59d1b886-90c8-4f7f-9212-08b20a9ee8cf'',''49c543c2-a72a-4b0a-8cca-39c375c0726f'') ',
      'WHERE pa.voided = 0 AND (pa.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR pa.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
      'GROUP BY p.person_id',
    ') att ON att.person_id = c.patient_id ',
    'SET c.baseline_hiv_status = att.baseline_hiv_status, c.living_with_patient = att.living_with_patient, c.pns_approach = att.pns_approach, c.ipv_outcome = att.ipv_outcome;'
  );
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET sql_stmt = CONCAT(
    'UPDATE ', @target_table, ' c LEFT JOIN (',
      'SELECT pa.person_id, MAX(pa.address1) AS physical_address, MAX(pa.date_changed) AS date_last_modified, MAX(pa.date_created) AS date_created ',
      'FROM person_address pa ',
      'WHERE pa.voided = 0 AND (pa.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR pa.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
      'GROUP BY pa.person_id',
    ') pa ON c.patient_id = pa.person_id ',
    'SET c.physical_address = pa.physical_address;'
  );
PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Completed processing Patient contact', CONCAT('Time: ', NOW());
END $$

----- UPdate kp contact

DROP PROCEDURE IF EXISTS sp_update_etl_kp_contact $$
CREATE PROCEDURE sp_update_etl_kp_contact(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_contact');

SELECT 'Processing client contact data ', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, client_id, visit_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, ',
      'patient_type, transfer_in_date, date_first_enrolled_in_kp, facility_transferred_from, key_population_type, priority_population_type, ',
      'implementation_county, implementation_subcounty, implementation_ward, contacted_by_peducator, program_name, frequent_hotspot_name, ',
      'frequent_hotspot_type, year_started_sex_work, year_started_sex_with_men, year_started_drugs, avg_weekly_sex_acts, avg_weekly_anal_sex_acts, ',
      'avg_daily_drug_injections, contact_person_name, contact_person_alias, contact_person_phone, voided',
    ') ',
    'SELECT ',
      'e.uuid, e.patient_id, e.visit_id, e.encounter_datetime AS visit_date, e.location_id, e.encounter_id, e.creator AS encounter_provider, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id=164932,(CASE o.value_coded WHEN 164144 THEN ''New Patient'' WHEN 160563 THEN ''Transfer in'' ELSE '''' END),NULL)) AS patient_type, ',
      'MAX(IF(o.concept_id=160534,o.value_datetime,NULL)) AS transfer_in_date, ',
      'MAX(IF(o.concept_id=160555,o.value_datetime,NULL)) AS date_first_enrolled_in_kp, ',
      'MAX(IF(o.concept_id=160535,LEFT(TRIM(o.value_text),100),NULL)) AS facility_transferred_from, ',
      'COALESCE(',
        'MAX(IF(o.concept_id=165241,(CASE o.value_coded WHEN 162277 THEN ''Prison Inmate'' WHEN 1142 THEN ''Prison Staff'' WHEN 163488 THEN ''Prison Community'' END),NULL)),',
        'MAX(IF(o.concept_id=164929,(CASE o.value_coded WHEN 166513 THEN ''FSW'' WHEN 160578 THEN ''MSM'' WHEN 165084 THEN ''MSW'' WHEN 165085 THEN ''PWUD'' WHEN 105 THEN ''PWID'' WHEN 162277 THEN ''People in prison and other closed settings'' WHEN 159674 THEN ''Fisher Folk'' WHEN 162198 THEN ''Truck Driver'' WHEN 6096 THEN ''Discordant Couple'' WHEN 1175 THEN ''Not applicable'' ELSE '''' END),NULL))',
      ') AS key_population_type, ',
      'MAX(IF(o.concept_id=138643,(CASE o.value_coded WHEN 159674 THEN ''Fisher Folk'' WHEN 162198 THEN ''Truck Driver'' WHEN 160549 THEN ''Adolescent and Young Girls'' WHEN 162277 THEN ''Prisoner'' ELSE '''' END),NULL)) AS priority_population_type, ',
      'MAX(IF(o.concept_id=167131,o.value_text,NULL)) AS implementation_county, ',
      'MAX(IF(o.concept_id=161551,o.value_text,NULL)) AS implementation_subcounty, ',
      'MAX(IF(o.concept_id=161550,o.value_text,NULL)) AS implementation_ward, ',
      'MAX(IF(o.concept_id=165004,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS contacted_by_peducator, ',
      'MAX(IF(o.concept_id=165137,o.value_text,NULL)) AS program_name, ',
      'MAX(IF(o.concept_id=165006,o.value_text,NULL)) AS frequent_hotspot_name, ',
      'MAX(IF(o.concept_id=165005,(CASE o.value_coded ',
        'WHEN 165011 THEN ''Street'' WHEN 165012 THEN ''Injecting den'' WHEN 165013 THEN ''Uninhabitable building'' WHEN 165014 THEN ''Public Park'' WHEN 1536 THEN ''Homes'' ',
        'WHEN 165015 THEN ''Beach'' WHEN 165016 THEN ''Casino'' WHEN 165017 THEN ''Bar with lodging'' WHEN 165018 THEN ''Bar without lodging'' WHEN 165019 THEN ''Sex den'' ',
        'WHEN 165020 THEN ''Strip club'' WHEN 165021 THEN ''Highway'' WHEN 165022 THEN ''Brothel'' WHEN 165023 THEN ''Guest house/hotel'' WHEN 165024 THEN ''Massage parlor'' ',
        'WHEN 165025 THEN ''illicit brew den'' WHEN 165026 THEN ''Barber shop/salon'' WHEN 165297 THEN ''Virtual Space'' WHEN 5622 THEN ''Other'' ELSE '''' END),NULL)) AS frequent_hotspot_type, ',
      'MAX(IF(o.concept_id=165030,o.value_numeric,NULL)) AS year_started_sex_work, ',
      'MAX(IF(o.concept_id=165031,o.value_numeric,NULL)) AS year_started_sex_with_men, ',
      'MAX(IF(o.concept_id=165032,o.value_numeric,NULL)) AS year_started_drugs, ',
      'MAX(IF(o.concept_id=165007,o.value_numeric,NULL)) AS avg_weekly_sex_acts, ',
      'MAX(IF(o.concept_id=165008,o.value_numeric,NULL)) AS avg_weekly_anal_sex_acts, ',
      'MAX(IF(o.concept_id=165009,o.value_numeric,NULL)) AS avg_daily_drug_injections, ',
      'MAX(IF(o.concept_id=160638,o.value_text,NULL)) AS contact_person_name, ',
      'MAX(IF(o.concept_id=165038,o.value_text,NULL)) AS contact_person_alias, ',
      'MAX(IF(o.concept_id=160642,o.value_text,NULL)) AS contact_person_phone, ',
      'e.voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid = ''ea68aad6-4655-4dc5-80f2-780e33055a9e'') et ON et.encounter_type_id = e.encounter_type ',
    'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (164932,160534,160555,160535,164929,138643,167131,161551,161550,165004,165137,165006,165005,165030,165031,165032,165007,165008,165009,160638,165038,160642,165241) ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, e.encounter_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), encounter_provider = VALUES(encounter_provider), patient_type = VALUES(patient_type), transfer_in_date = VALUES(transfer_in_date), ',
      'date_first_enrolled_in_kp = VALUES(date_first_enrolled_in_kp), facility_transferred_from = VALUES(facility_transferred_from), key_population_type = VALUES(key_population_type), ',
      'priority_population_type = VALUES(priority_population_type), implementation_county = VALUES(implementation_county), implementation_subcounty = VALUES(implementation_subcounty), ',
      'implementation_ward = VALUES(implementation_ward), contacted_by_peducator = VALUES(contacted_by_peducator), program_name = VALUES(program_name), frequent_hotspot_name = VALUES(frequent_hotspot_name), ',
      'frequent_hotspot_type = VALUES(frequent_hotspot_type), year_started_sex_work = VALUES(year_started_sex_work), year_started_sex_with_men = VALUES(year_started_sex_with_men), ',
      'year_started_drugs = VALUES(year_started_drugs), avg_weekly_sex_acts = VALUES(avg_weekly_sex_acts), avg_weekly_anal_sex_acts = VALUES(avg_weekly_anal_sex_acts), ',
      'avg_daily_drug_injections = VALUES(avg_daily_drug_injections), contact_person_name = VALUES(contact_person_name), contact_person_alias = VALUES(contact_person_alias), ',
      'contact_person_phone = VALUES(contact_person_phone), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT 'Completed processing KP contact data', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'UPDATE ', @target_table, ' c ',
    'JOIN (',
      'SELECT pi.patient_id, MAX(IF(pit.uuid = ''b7bfefd0-239b-11e9-ab14-d663bd873d93'', pi.identifier, NULL)) AS unique_identifier ',
      'FROM patient_identifier pi ',
      'JOIN patient_identifier_type pit ON pi.identifier_type = pit.patient_identifier_type_id ',
      'WHERE pi.voided = 0 ',
      'GROUP BY pi.patient_id',
    ') pid ON pid.patient_id = c.client_id ',
    'SET c.unique_identifier = pid.unique_identifier;'
  );

PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END $$

------------ update  kP client enrollment data -------

sql
DROP PROCEDURE IF EXISTS sp_update_etl_kp_client_enrollment $$
CREATE PROCEDURE sp_update_etl_kp_client_enrollment(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_client_enrollment');

SELECT 'Processing client enrollment data ', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, client_id, visit_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, ',
      'contacted_for_prevention, has_regular_free_sex_partner, year_started_sex_work, year_started_sex_with_men, year_started_drugs, ',
      'has_expereienced_sexual_violence, has_expereienced_physical_violence, ever_tested_for_hiv, test_type, share_test_results, ',
      'willing_to_test, test_decline_reason, receiving_hiv_care, care_facility_name, ccc_number, vl_test_done, vl_results_date, ',
      'contact_for_appointment, contact_method, buddy_name, buddy_phone_number, voided) ',
    'SELECT ',
      'e.uuid, e.patient_id, e.visit_id, e.encounter_datetime AS visit_date, e.location_id, e.encounter_id, e.creator, e.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 165004, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS contacted_for_prevention, ',
      'MAX(IF(o.concept_id = 165027, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS has_regular_free_sex_partner, ',
      'MAX(IF(o.concept_id = 165030, o.value_numeric, NULL)) AS year_started_sex_work, ',
      'MAX(IF(o.concept_id = 165031, o.value_numeric, NULL)) AS year_started_sex_with_men, ',
      'MAX(IF(o.concept_id = 165032, o.value_numeric, NULL)) AS year_started_drugs, ',
      'MAX(IF(o.concept_id = 123160, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS has_expereienced_sexual_violence, ',
      'MAX(IF(o.concept_id = 165034, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS has_expereienced_physical_violence, ',
      'MAX(IF(o.concept_id = 164401, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS ever_tested_for_hiv, ',
      'MAX(IF(o.concept_id = 164956, (CASE o.value_coded WHEN 163722 THEN ''Rapid HIV Testing'' WHEN 164952 THEN ''Self Test'' ELSE '''' END), NULL)) AS test_type, ',
      'MAX(IF(o.concept_id = 165153, (CASE o.value_coded WHEN 703 THEN ''Yes I tested positive'' WHEN 664 THEN ''Yes I tested negative'' WHEN 1066 THEN ''No I do not want to share'' ELSE '''' END), NULL)) AS share_test_results, ',
      'MAX(IF(o.concept_id = 165154, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS willing_to_test, ',
      'MAX(IF(o.concept_id = 159803, o.value_text, NULL)) AS test_decline_reason, ',
      'MAX(IF(o.concept_id = 159811, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS receiving_hiv_care, ',
      'MAX(IF(o.concept_id = 162724, o.value_text, NULL)) AS care_facility_name, ',
      'MAX(IF(o.concept_id = 162053, o.value_numeric, NULL)) AS ccc_number, ',
      'MAX(IF(o.concept_id = 164437, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS vl_test_done, ',
      'MAX(IF(o.concept_id = 163281, o.value_datetime, NULL)) AS vl_results_date, ',
      'MAX(IF(o.concept_id = 165036, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS contact_for_appointment, ',
      'MAX(IF(o.concept_id = 164966, (CASE o.value_coded WHEN 161642 THEN ''Treatment supporter'' WHEN 165037 THEN ''Peer educator'' WHEN 1555 THEN ''Outreach worker'' WHEN 159635 THEN ''Phone number'' ELSE '''' END), NULL)) AS contact_method, ',
      'MAX(IF(o.concept_id = 160638, o.value_text, NULL)) AS buddy_name, ',
      'MAX(IF(o.concept_id = 160642, o.value_text, NULL)) AS buddy_phone_number, ',
      'e.voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid = ''c7f47a56-207b-11e9-ab14-d663bd873d93'') et ON et.encounter_type_id = e.encounter_type ',
    'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
    '  AND o.concept_id IN (165004,165027,165030,165031,165032,123160,165034,164401,164956,165153,165154,159803,159811,162724,162053,164437,163281,165036,164966,160638,160642) ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, e.encounter_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
      'encounter_provider = VALUES(encounter_provider), visit_date = VALUES(visit_date), has_expereienced_sexual_violence = VALUES(has_expereienced_sexual_violence), ',
      'has_expereienced_physical_violence = VALUES(has_expereienced_physical_violence), ever_tested_for_hiv = VALUES(ever_tested_for_hiv), test_type = VALUES(test_type), ',
      'share_test_results = VALUES(share_test_results), willing_to_test = VALUES(willing_to_test), test_decline_reason = VALUES(test_decline_reason), ',
      'receiving_hiv_care = VALUES(receiving_hiv_care), care_facility_name = VALUES(care_facility_name), ccc_number = VALUES(ccc_number), ',
      'vl_test_done = VALUES(vl_test_done), vl_results_date = VALUES(vl_results_date), contact_for_appointment = VALUES(contact_for_appointment), ',
      'contact_method = VALUES(contact_method), buddy_name = VALUES(buddy_name), buddy_phone_number = VALUES(buddy_phone_number), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT 'Completed processing KP client enrollment data', CONCAT('Time: ', NOW());

END $$


-- ------------- populate etl_kp_clinical_visit--------------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_kp_clinical_visit $$
CREATE PROCEDURE sp_update_etl_kp_clinical_visit(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_clinical_visit');

SELECT 'Processing Clinical Visit ', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, client_id, visit_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, ',
      'implementing_partner, type_of_visit, visit_reason, service_delivery_model, sti_screened, sti_results, sti_treated, sti_referred, sti_referred_text, ',
      'tb_screened, tb_results, tb_treated, tb_referred, tb_referred_text, hepatitisB_screened, hepatitisB_results, hepatitisB_confirmatory_results, hepatitisB_vaccinated, ',
      'hepatitisB_treated, hepatitisB_referred, hepatitisB_text, hepatitisC_screened, hepatitisC_results, hepatitisC_confirmatory_results, hepatitisC_treated, hepatitisC_referred, hepatitisC_text, ',
      'overdose_screened, overdose_results, overdose_treated, received_naloxone, overdose_referred, overdose_text, abscess_screened, abscess_results, abscess_treated, abscess_referred, abscess_text, ',
      'alcohol_screened, alcohol_results, alcohol_treated, alcohol_referred, alcohol_text, cerv_cancer_screened, cerv_cancer_results, cerv_cancer_treated, cerv_cancer_referred, cerv_cancer_text, ',
      'anal_cancer_screened, anal_cancer_results, prep_screened, prep_results, prep_treated, prep_referred, prep_text, violence_screened, violence_results, violence_treated, violence_referred, violence_text, ',
      'risk_red_counselling_screened, risk_red_counselling_eligibility, risk_red_counselling_support, risk_red_counselling_ebi_provided, risk_red_counselling_text, fp_screened, fp_eligibility, fp_treated, fp_referred, fp_text, ',
      'mental_health_screened, mental_health_results, mental_health_support, mental_health_referred, mental_health_text, mat_screened, mat_results, mat_treated, mat_referred, mat_text, hiv_self_rep_status, last_hiv_test_setting, ',
      'counselled_for_hiv, hiv_tested, test_frequency, received_results, test_results, linked_to_art, facility_linked_to, self_test_education, self_test_kits_given, self_use_kits, distribution_kits, self_tested, hiv_test_date, ',
      'self_test_frequency, self_test_results, test_confirmatory_results, confirmatory_facility, offsite_confirmatory_facility, self_test_linked_art, self_test_link_facility, hiv_care_facility, other_hiv_care_facility, initiated_art_this_month, ',
      'started_on_art, date_started_art, active_art, primary_care_facility_name, ccc_number, eligible_vl, vl_test_done, vl_results, vl_results_date, received_vl_results, condom_use_education, post_abortal_care, referral, linked_to_psychosocial, ',
      'male_condoms_no, female_condoms_no, lubes_no, syringes_needles_no, pep_eligible, pep_status, exposure_type, other_exposure_type, initiated_pep_within_72hrs, clinical_notes, voided',
    ') ',
      'SELECT ',
      'e.uuid, e.patient_id, e.visit_id, e.encounter_datetime AS visit_date, e.location_id, e.encounter_id, e.creator, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id = 165347, o.value_text, NULL)) AS implementing_partner, ',
      'MAX(IF(o.concept_id = 164181, (CASE o.value_coded WHEN 162080 THEN ''Initial'' WHEN 164142 THEN ''Revisit'' ELSE '''' END), NULL)) AS type_of_visit, ',
      'MAX(IF(o.concept_id = 164082, (CASE o.value_coded WHEN 5006 THEN ''Asymptomatic'' WHEN 1068 THEN ''Symptomatic'' WHEN 165348 THEN ''Quarterly Screening checkup'' WHEN 160523 THEN ''Follow up'' ELSE '''' END), NULL)) AS visit_reason, ',
      'MAX(IF(o.concept_id=160540,(CASE o.value_coded WHEN 161235 THEN ''Static'' WHEN 160545 THEN ''Outreach'' ELSE '''' END),NULL)) AS service_delivery_model, ',
      'MAX(IF(o.concept_id=161558,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' ELSE '''' END),NULL)) AS sti_screened, ',
      'MAX(IF(o.concept_id=165199,(CASE o.value_coded WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' ELSE '''' END),NULL)) AS sti_results, ',
      'MAX(IF(o.concept_id=165200,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS sti_treated, ',
      'MAX(IF(o.concept_id=165249,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS sti_referred, ',
      'MAX(IF(o.concept_id=165250,o.value_text,NULL)) AS sti_referred_text, ',
      'MAX(IF(o.concept_id=165197,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' ELSE '''' END),NULL)) AS tb_screened, ',
      'MAX(IF(o.concept_id=165198,(CASE o.value_coded WHEN 1660 THEN ''No signs'' WHEN 142177 THEN ''Presumptive'' WHEN 1661 THEN ''Diagnosed with TB'' WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' ELSE '''' END),NULL)) AS tb_results, ',
      'MAX(IF(o.concept_id=1111,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' ELSE ''NA'' END),NULL)) AS tb_treated, ',
      'MAX(IF(o.concept_id=162310,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS tb_referred, ',
      'MAX(IF(o.concept_id=163323,o.value_text,NULL)) AS tb_referred_text, ',
      'MAX(IF(o.concept_id=165040,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' ELSE '''' END),NULL)) AS hepatitisB_screened, ',
      'MAX(IF(o.concept_id=1322,(CASE o.value_coded WHEN 664 THEN ''N'' WHEN 703 THEN ''P'' ELSE '''' END),NULL)) AS hepatitisB_results, ',
      'MAX(IF(o.concept_id=159430,(CASE o.value_coded WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' WHEN 1118 THEN ''Not done'' ELSE '''' END),NULL)) AS hepatitisB_confirmatory_results, ',
      'MAX(IF(o.concept_id=165251,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''NA'' ELSE '''' END),NULL)) AS hepatitisB_vaccinated, ',
      'MAX(IF(o.concept_id=166665,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' ELSE '''' END),NULL)) AS hepatitisB_treated, ',
      'MAX(IF(o.concept_id=165252,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS hepatitisB_referred, ',
      'MAX(IF(o.concept_id=165253,o.value_text,NULL)) AS hepatitisB_text, ',
      'MAX(IF(o.concept_id=165041,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' ELSE '''' END),NULL)) AS hepatitisC_screened, ',
      'MAX(IF(o.concept_id=161471,(CASE o.value_coded WHEN 664 THEN ''N'' WHEN 703 THEN ''P'' ELSE '''' END),NULL)) AS hepatitisC_results, ',
      'MAX(IF(o.concept_id=167786,(CASE o.value_coded WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' ELSE '''' END),NULL)) AS hepatitisC_confirmatory_results, ',
      'MAX(IF(o.concept_id=165254,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' WHEN 1175 THEN ''NA'' ELSE '''' END),NULL)) AS hepatitisC_treated, ',
      'MAX(IF(o.concept_id=165255,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS hepatitisC_referred, ',
      'MAX(IF(o.concept_id=165256,o.value_text,NULL)) AS hepatitisC_text, ',
      'MAX(IF(o.concept_id=165042,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS overdose_screened, ',
      'MAX(IF(o.concept_id=165046,(CASE o.value_coded WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' ELSE '''' END),NULL)) AS overdose_results, ',
      'MAX(IF(o.concept_id=165257,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS overdose_treated, ',
      'MAX(IF(o.concept_id=165201,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS received_naloxone, ',
      'MAX(IF(o.concept_id=165258,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS overdose_referred, ',
      'MAX(IF(o.concept_id=165259,o.value_text,NULL)) AS overdose_text, ',
      'MAX(IF(o.concept_id=165044,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS abscess_screened, ',
      'MAX(IF(o.concept_id=165051,(CASE o.value_coded WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' ELSE '''' END),NULL)) AS abscess_results, ',
      'MAX(IF(o.concept_id=165260,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS abscess_treated, ',
      'MAX(IF(o.concept_id=165261,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS abscess_referred, ',
      'MAX(IF(o.concept_id=165262,o.value_text,NULL)) AS abscess_text, ',
      'MAX(IF(o.concept_id=165043,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' ELSE '''' END),NULL)) AS alcohol_screened, ',
      'MAX(IF(o.concept_id=165047,(CASE o.value_coded WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' ELSE '''' END),NULL)) AS alcohol_results, ',
      'MAX(IF(o.concept_id=165263,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS alcohol_treated, ',
      'MAX(IF(o.concept_id=165264,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS alcohol_referred, ',
      'MAX(IF(o.concept_id=165265,o.value_text,NULL)) AS alcohol_text, ',
      'MAX(IF(o.concept_id=164934,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS cerv_cancer_screened, ',
      'MAX(IF(o.concept_id=165196,(CASE o.value_coded WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' ELSE '''' END),NULL)) AS cerv_cancer_results, ',
      'MAX(IF(o.concept_id=165266,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS cerv_cancer_treated, ',
      'MAX(IF(o.concept_id=165267,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS cerv_cancer_referred, ',
      'MAX(IF(o.concept_id=165268,o.value_text,NULL)) AS cerv_cancer_text, ',
      'MAX(IF(o.concept_id=116030,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''NA'' ELSE '''' END),NULL)) AS anal_cancer_screened, ',
      'MAX(IF(o.concept_id=166664,(CASE o.value_coded WHEN 162743 THEN ''Suspected'' WHEN 1302 THEN ''Not Suspected'' ELSE '''' END),NULL)) AS anal_cancer_results, ',
      'MAX(IF(o.concept_id=165076,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 165080 THEN ''Ongoing'' ELSE '''' END),NULL)) AS prep_screened, ',
      'MAX(IF(o.concept_id=165202,(CASE o.value_coded WHEN 165087 THEN ''Eligible'' WHEN 165078 THEN ''Not eligible'' ELSE '''' END),NULL)) AS prep_results, ',
      'MAX(IF(o.concept_id=165203,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' ELSE '''' END),NULL)) AS prep_treated, ',
      'MAX(IF(o.concept_id=165270,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS prep_referred, ',
      'MAX(IF(o.concept_id=165271,o.value_text,NULL)) AS prep_text, ',
      'MAX(IF(o.concept_id=165204,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS violence_screened, ',
      'CONCAT_WS('''','''', MAX(IF(o.concept_id = 165205 AND o.value_coded IN (165206, 165207, 123007, 126312), ''Emotional & Psychological'', NULL)), ',
      '           MAX(IF(o.concept_id = 165205 AND o.value_coded = 121387, ''Physical'', NULL)), ',
      '           MAX(IF(o.concept_id = 165205 AND o.value_coded = 127910, ''Rape/Sexual assault'', NULL)), ',
      '           MAX(IF(o.concept_id = 165205 AND o.value_coded = 141537, ''Economical'', NULL)), ',
      '           MAX(IF(o.concept_id = 165205 AND o.value_coded = 5622, ''Other'', NULL))) AS violence_results, ',
      'MAX(IF(o.concept_id=165208,(CASE o.value_coded WHEN 1065 THEN ''Supported'' WHEN 1066 THEN ''Not supported'' ELSE '''' END),NULL)) AS violence_treated, ',
      'MAX(IF(o.concept_id=165273,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS violence_referred, ',
      'MAX(IF(o.concept_id=165274,o.value_text,NULL)) AS violence_text, ',
      'MAX(IF(o.concept_id=165045,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS risk_red_counselling_screened, ',
      'MAX(IF(o.concept_id=165050,(CASE o.value_coded WHEN 165087 THEN ''Eligible'' WHEN 165078 THEN ''Not eligible'' ELSE '''' END),NULL)) AS risk_red_counselling_eligibility, ',
      'MAX(IF(o.concept_id=165053,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' ELSE '''' END),NULL)) AS risk_red_counselling_support, ',
      'MAX(IF(o.concept_id=161595,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' ELSE '''' END),NULL)) AS risk_red_counselling_ebi_provided, ',
      'MAX(IF(o.concept_id=165277,o.value_text,NULL)) AS risk_red_counselling_text, ',
      'MAX(IF(o.concept_id=1382,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS fp_screened, ',
      'MAX(IF(o.concept_id=165209,(CASE o.value_coded WHEN 165087 THEN ''Eligible'' WHEN 165078 THEN ''Not eligible'' ELSE '''' END),NULL)) AS fp_eligibility, ',
      'MAX(IF(o.concept_id=160653,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' WHEN 965 THEN ''On-going'' ELSE '''' END),NULL)) AS fp_treated, ',
      'MAX(IF(o.concept_id=165279,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS fp_referred, ',
      'MAX(IF(o.concept_id=165280,o.value_text,NULL)) AS fp_text, ',
      'MAX(IF(o.concept_id=165210,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS mental_health_screened, ',
      'MAX(IF(o.concept_id=165211,(CASE o.value_coded WHEN 165212 THEN ''Depression unlikely'' WHEN 157790 THEN ''Mild depression'' WHEN 134017 THEN ''Moderate depression'' WHEN 134011 THEN ''Moderate-severe depression'' WHEN 126627 THEN ''Severe Depression'' ELSE '''' END),NULL)) AS mental_health_results, ',
      'MAX(IF(o.concept_id=165213,(CASE o.value_coded WHEN 1065 THEN ''Supported'' WHEN 1066 THEN ''Not supported'' ELSE '''' END),NULL)) AS mental_health_support, ',
      'MAX(IF(o.concept_id=165281,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS mental_health_referred, ',
      'MAX(IF(o.concept_id=165282,o.value_text,NULL)) AS mental_health_text, ',
      'MAX(IF(o.concept_id=166663,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS mat_screened, ',
      'MAX(IF(o.concept_id=166664,(CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' ELSE '''' END),NULL)) AS mat_results, ',
      'MAX(IF(o.concept_id=165052,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS mat_treated, ',
      'MAX(IF(o.concept_id=165093,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS mat_referred, ',
      'MAX(IF(o.concept_id=166637,o.value_text,NULL)) AS mat_text, ',
      'MAX(IF(o.concept_id=165214,(CASE o.value_coded WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' WHEN 1067 THEN ''Unknown'' ELSE '''' END),NULL)) AS hiv_self_rep_status, ',
      'MAX(IF(o.concept_id=165215,(CASE o.value_coded WHEN 165216 THEN ''Universal HTS'' WHEN 165217 THEN ''Self-testing'' WHEN 1402 THEN ''Never tested'' ELSE '''' END),NULL)) AS last_hiv_test_setting, ',
      'MAX(IF(o.concept_id=159382,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS counselled_for_hiv, ',
      'MAX(IF(o.concept_id=164401,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not Applicable'' WHEN 162570 THEN ''Declined'' WHEN 1788 THEN ''Referred for testing'' ELSE '''' END),NULL)) AS hiv_tested, ',
      'MAX(IF(o.concept_id=165218,(CASE o.value_coded WHEN 162080 THEN ''Initial'' WHEN 162081 THEN ''Repeat'' WHEN 1175 THEN ''Not Applicable'' ELSE '''' END),NULL)) AS test_frequency, ',
      'MAX(IF(o.concept_id=164848,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Not Applicable'' ELSE '''' END),NULL)) AS received_results, ',
      'MAX(IF(o.concept_id=159427,(CASE o.value_coded WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' WHEN 165232 THEN ''Inconclusive'' WHEN 138571 THEN ''Known Positive'' WHEN 1118 THEN ''Not done'' ELSE '''' END),NULL)) AS test_results, ',
      'MAX(IF(o.concept_id=1648,(CASE o.value_coded WHEN 1 THEN ''Yes'' WHEN 0 THEN ''No'' ELSE '''' END),NULL)) AS linked_to_art, ',
      'MAX(IF(o.concept_id=163042,o.value_text,NULL)) AS facility_linked_to, ',
      'MAX(IF(o.concept_id=165220,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS self_test_education, ',
      'MAX(IF(o.concept_id=165221,(CASE o.value_coded WHEN 165222 THEN ''Self use'' WHEN 165223 THEN ''Distribution'' ELSE '''' END),NULL)) AS self_test_kits_given, ',
      'MAX(IF(o.concept_id=165222,o.value_numeric,NULL)) AS self_use_kits, ',
      'MAX(IF(o.concept_id=165223,o.value_numeric,NULL)) AS distribution_kits, ',
      'MAX(IF(o.concept_id=164952,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' ELSE '''' END),NULL)) AS self_tested, ',
      'MAX(IF(o.concept_id=164400,o.value_datetime,NULL)) AS hiv_test_date, ',
      'MAX(IF(o.concept_id=165231,(CASE o.value_coded WHEN 162080 THEN ''Initial'' WHEN 162081 THEN ''Repeat'' ELSE '''' END),NULL)) AS self_test_frequency, ',
      'MAX(IF(o.concept_id=165233,(CASE o.value_coded WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' WHEN 165232 THEN ''Inconclusive'' ELSE '''' END),NULL)) AS self_test_results, ',
      'MAX(IF(o.concept_id=165234,(CASE o.value_coded WHEN 664 THEN ''Negative'' WHEN 703 THEN ''Positive'' WHEN 1118 THEN ''Not done'' ELSE '''' END),NULL)) AS test_confirmatory_results, ',
      'MAX(IF(o.concept_id=165237,o.value_text,NULL)) AS confirmatory_facility, ',
      'MAX(IF(o.concept_id=162724,o.value_text,NULL)) AS offsite_confirmatory_facility, ',
      'MAX(IF(o.concept_id=165238,(CASE o.value_coded WHEN 1 THEN ''Yes'' WHEN 0 THEN ''No'' ELSE '''' END),NULL)) AS self_test_linked_art, ',
      'MAX(IF(o.concept_id=161562,o.value_text,NULL)) AS self_test_link_facility, ',
      'MAX(IF(o.concept_id=165239,(CASE o.value_coded WHEN 163266 THEN ''Provided here'' WHEN 162723 THEN ''Provided elsewhere'' WHEN 160563 THEN ''Referred'' ELSE '''' END),NULL)) AS hiv_care_facility, ',
      'MAX(IF(o.concept_id=163042,o.value_text,NULL)) AS other_hiv_care_facility, ',
      'MAX(IF(o.concept_id=165240,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not Applicable'' ELSE '''' END),NULL)) AS initiated_art_this_month, ',
      'MAX(IF(o.concept_id=167790,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS started_on_art, ',
      'MAX(IF(o.concept_id=159599,o.value_datetime,NULL)) AS date_started_art, ',
      'MAX(IF(o.concept_id=160119,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not Applicable'' ELSE '''' END),NULL)) AS active_art, ',
      'MAX(IF(o.concept_id=162724,o.value_text,NULL)) AS primary_care_facility_name, ',
      'MAX(IF(o.concept_id=162053,o.value_numeric,NULL)) AS ccc_number, ',
      'MAX(IF(o.concept_id=165242,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1175 THEN ''Not Applicable'' ELSE '''' END),NULL)) AS eligible_vl, ',
      'MAX(IF(o.concept_id=165243,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' WHEN 1175 THEN ''Not Applicable'' ELSE '''' END),NULL)) AS vl_test_done, ',
      'COALESCE(MAX(IF(o.concept_id=165246,(CASE o.value_coded WHEN 167484 THEN ''LDL'' WHEN 1107 THEN ''None'' ELSE NULL END), NULL)), MAX(IF(o.concept_id=856,o.value_numeric,NULL))) AS vl_results, ',
      'MAX(IF(o.concept_id = 163281, o.value_datetime, NULL)) AS vl_results_date, ',
      'MAX(IF(o.concept_id=165246,(CASE o.value_coded WHEN 164369 THEN ''N'' ELSE ''Y'' END),NULL)) AS received_vl_results, ',
      'MAX(IF(o.concept_id=165247,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS condom_use_education, ',
      'MAX(IF(o.concept_id=164820,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS post_abortal_care, ',
      'MAX(IF(o.concept_id=165302,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS referral, ',
      'MAX(IF(o.concept_id=163766,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS linked_to_psychosocial, ',
      'MAX(IF(o.concept_id=165055,o.value_numeric,NULL)) AS male_condoms_no, ',
      'MAX(IF(o.concept_id=165056,o.value_numeric,NULL)) AS female_condoms_no, ',
      'MAX(IF(o.concept_id=165057,o.value_numeric,NULL)) AS lubes_no, ',
      'MAX(IF(o.concept_id=165058,o.value_numeric,NULL)) AS syringes_needles_no, ',
      'MAX(IF(o.concept_id=164845,(CASE o.value_coded WHEN 1065 THEN ''Y'' WHEN 1066 THEN ''N'' ELSE ''NA'' END),NULL)) AS pep_eligible, ',
      'MAX(IF(o.concept_id=65911,o.value_coded,NULL)) AS pep_status, ',
      'CONCAT_WS('''','''', MAX(IF(o.concept_id = 165060 AND o.value_coded = 127910, ''Rape'', NULL)), ',
      '          MAX(IF(o.concept_id = 165060 AND o.value_coded = 165045, ''Condom burst'', NULL)), ',
      '          MAX(IF(o.concept_id = 165060 AND o.value_coded = 5622, ''Others'', NULL))) AS exposure_type, ',
      'MAX(IF(o.concept_id=163042,o.value_text,NULL)) AS other_exposure_type, ',
      'MAX(IF(o.concept_id=165171,o.value_coded,NULL)) AS initiated_pep_within_72hrs, ',
      'MAX(IF(o.concept_id=165248,o.value_text,NULL)) AS clinical_notes, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''92e03f22-9686-11e9-bc42-526af7764f64'')) et ON et.encounter_type_id = e.encounter_type ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
        'AND o.concept_id IN (165347,164181,164082,160540,161558,165199,165200,165249,165250,165197,165198,1111,162310,163323,165040,1322,165251,165252,165253,165041,161471,165254,165255,165256,165042,165046,165257,165201,165258,165259,165044,165051,165260,165261,165262,165043,165047,165263,165264,165265,164934,165196,165266,165267,165268,116030,165076,165202,165203,165270,165271,165204,165205,165208,165273,165274,165045,165050,165053,161595,165277,1382,165209,160653,165279,165280,165210,165211,165213,165281,165282,166663,166664,165052,166637,165093,165214,165215,159382,164401,165218,164848,159427,1648,163042,165220,165221,165222,165223,164952,164400,165231,165233,165234,165237,162724,165238,161562,165239,163042,165240,160119,165242,165243,165246,165247,164820,165302,163766,165055,165056,165057,165058,164845,165248,5096,164142,856,159599,167790,162724,162053,163281,159430,165251,167786,65911,165171,165060) ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, e.encounter_id ',
    'ORDER BY e.patient_id ',
      'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), encounter_provider = VALUES(encounter_provider), implementing_partner = VALUES(implementing_partner), type_of_visit = VALUES(type_of_visit), ',
      'visit_reason = VALUES(visit_reason), service_delivery_model = VALUES(service_delivery_model), sti_screened = VALUES(sti_screened), sti_results = VALUES(sti_results), sti_treated = VALUES(sti_treated), ',
      'sti_referred = VALUES(sti_referred), sti_referred_text = VALUES(sti_referred_text), tb_screened = VALUES(tb_screened), tb_results = VALUES(tb_results), tb_treated = VALUES(tb_treated), ',
      'tb_referred = VALUES(tb_referred), tb_referred_text = VALUES(tb_referred_text), hepatitisB_screened = VALUES(hepatitisB_screened), hepatitisB_results = VALUES(hepatitisB_results), hepatitisB_confirmatory_results = VALUES(hepatitisB_confirmatory_results), ',
      'hepatitisB_vaccinated = VALUES(hepatitisB_vaccinated), hepatitisB_treated = VALUES(hepatitisB_treated), hepatitisB_referred = VALUES(hepatitisB_referred), hepatitisB_text = VALUES(hepatitisB_text), hepatitisC_screened = VALUES(hepatitisC_screened), ',
      'hepatitisC_results = VALUES(hepatitisC_results), hepatitisC_confirmatory_results = VALUES(hepatitisC_confirmatory_results), hepatitisC_treated = VALUES(hepatitisC_treated), hepatitisC_referred = VALUES(hepatitisC_referred), hepatitisC_text = VALUES(hepatitisC_text), ',
      'overdose_screened = VALUES(overdose_screened), overdose_results = VALUES(overdose_results), overdose_treated = VALUES(overdose_treated), received_naloxone = VALUES(received_naloxone), overdose_referred = VALUES(overdose_referred), overdose_text = VALUES(overdose_text), ',
      'abscess_screened = VALUES(abscess_screened), abscess_results = VALUES(abscess_results), abscess_treated = VALUES(abscess_treated), abscess_referred = VALUES(abscess_referred), abscess_text = VALUES(abscess_text), ',
      'alcohol_screened = VALUES(alcohol_screened), alcohol_results = VALUES(alcohol_results), alcohol_treated = VALUES(alcohol_treated), alcohol_referred = VALUES(alcohol_referred), alcohol_text = VALUES(alcohol_text), ',
      'cerv_cancer_screened = VALUES(cerv_cancer_screened), cerv_cancer_results = VALUES(cerv_cancer_results), cerv_cancer_treated = VALUES(cerv_cancer_treated), cerv_cancer_referred = VALUES(cerv_cancer_referred), cerv_cancer_text = VALUES(cerv_cancer_text), ',
      'anal_cancer_screened = VALUES(anal_cancer_screened), anal_cancer_results = VALUES(anal_cancer_results), prep_screened = VALUES(prep_screened), prep_results = VALUES(prep_results), prep_treated = VALUES(prep_treated), prep_referred = VALUES(prep_referred), ',
      'prep_text = VALUES(prep_text), violence_screened = VALUES(violence_screened), violence_results = VALUES(violence_results), violence_treated = VALUES(violence_treated), violence_referred = VALUES(violence_referred), violence_text = VALUES(violence_text), ',
      'risk_red_counselling_screened = VALUES(risk_red_counselling_screened), risk_red_counselling_eligibility = VALUES(risk_red_counselling_eligibility), risk_red_counselling_support = VALUES(risk_red_counselling_support), risk_red_counselling_ebi_provided = VALUES(risk_red_counselling_ebi_provided), risk_red_counselling_text = VALUES(risk_red_counselling_text), ',
      'fp_screened = VALUES(fp_screened), fp_eligibility = VALUES(fp_eligibility), fp_treated = VALUES(fp_treated), fp_referred = VALUES(fp_referred), fp_text = VALUES(fp_text), mental_health_screened = VALUES(mental_health_screened), mental_health_results = VALUES(mental_health_results), mental_health_support = VALUES(mental_health_support), mental_health_referred = VALUES(mental_health_referred), mental_health_text = VALUES(mental_health_text), mat_screened = VALUES(mat_screened), mat_results = VALUES(mat_results), mat_treated = VALUES(mat_treated), mat_referred = VALUES(mat_referred), mat_text = VALUES(mat_text), hiv_self_rep_status = VALUES(hiv_self_rep_status), last_hiv_test_setting = VALUES(last_hiv_test_setting), counselled_for_hiv = VALUES(counselled_for_hiv), hiv_tested = VALUES(hiv_tested), test_frequency = VALUES(test_frequency), received_results = VALUES(received_results), test_results = VALUES(test_results), linked_to_art = VALUES(linked_to_art), facility_linked_to = VALUES(facility_linked_to), self_test_education = VALUES(self_test_education), self_test_kits_given = VALUES(self_test_kits_given), self_use_kits = VALUES(self_use_kits), distribution_kits = VALUES(distribution_kits), self_tested = VALUES(self_tested), hiv_test_date = VALUES(hiv_test_date), self_test_frequency = VALUES(self_test_frequency), self_test_results = VALUES(self_test_results), test_confirmatory_results = VALUES(test_confirmatory_results), confirmatory_facility = VALUES(confirmatory_facility), offsite_confirmatory_facility = VALUES(offsite_confirmatory_facility), self_test_linked_art = VALUES(self_test_linked_art), self_test_link_facility = VALUES(self_test_link_facility), hiv_care_facility = VALUES(hiv_care_facility), other_hiv_care_facility = VALUES(other_hiv_care_facility), initiated_art_this_month = VALUES(initiated_art_this_month), started_on_art = VALUES(started_on_art), date_started_art = VALUES(date_started_art), active_art = VALUES(active_art), primary_care_facility_name = VALUES(primary_care_facility_name), ccc_number = VALUES(ccc_number), eligible_vl = VALUES(eligible_vl), vl_test_done = VALUES(vl_test_done), vl_results = VALUES(vl_results), vl_results_date = VALUES(vl_results_date), received_vl_results = VALUES(received_vl_results), condom_use_education = VALUES(condom_use_education), post_abortal_care = VALUES(post_abortal_care), referral = VALUES(referral), linked_to_psychosocial = VALUES(linked_to_psychosocial), male_condoms_no = VALUES(male_condoms_no), female_condoms_no = VALUES(female_condoms_no), lubes_no = VALUES(lubes_no), syringes_needles_no = VALUES(syringes_needles_no), pep_eligible = VALUES(pep_eligible), pep_status = VALUES(pep_status), exposure_type = VALUES(exposure_type), other_exposure_type = VALUES(other_exposure_type), initiated_pep_within_72hrs = VALUES(initiated_pep_within_72hrs), clinical_notes = VALUES(clinical_notes), appointment_date = VALUES(appointment_date), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing KP clinical visit', CONCAT('Time: ', NOW());
END $$


-- ------------- populate etl_kp_peer_calendar--------------------------------


-- sql

DROP PROCEDURE IF EXISTS sp_update_etl_kp_peer_calendar $$
CREATE PROCEDURE sp_update_etl_kp_peer_calendar(IN last_update_time DATETIME)
BEGIN
  DECLARE target_table_quoted VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table_quoted = CONCAT(@etl_schema, '.etl_peer_calendar');

SELECT CONCAT('Processing Peer calendar ', CONCAT('Time: ', NOW()));

SET @sql = CONCAT(
'INSERT INTO ', @target_table_quoted, ' (uuid, client_id, visit_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, hotspot_name, typology, other_hotspots, weekly_sex_acts, monthly_condoms_required, weekly_anal_sex_acts, monthly_lubes_required, daily_injections, monthly_syringes_required, years_in_sexwork_drugs, experienced_violence, service_provided_within_last_month, monthly_n_and_s_distributed, monthly_male_condoms_distributed, monthly_lubes_distributed, monthly_female_condoms_distributed, monthly_self_test_kits_distributed, received_clinical_service, violence_reported, referred, health_edu, remarks, voided) ',
'SELECT e.uuid, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id AS encounter_id, e.creator AS encounter_provider, e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
'MAX(IF(o.concept_id=165006, o.value_text, NULL)) AS hotspot_name, ',
'MAX(IF(o.concept_id=165005, (CASE o.value_coded WHEN 165011 THEN ''Street'' WHEN 165012 THEN ''Injecting den'' WHEN 165013 THEN ''Uninhabitable building'' WHEN 165014 THEN ''Park'' WHEN 1536 THEN ''Homes'' WHEN 165015 THEN ''Beach'' WHEN 165016 THEN ''Casino'' WHEN 165017 THEN ''Bar with lodging'' WHEN 165018 THEN ''Bar without lodging'' WHEN 165019 THEN ''Sex den'' WHEN 165020 THEN ''Strip club'' WHEN 165021 THEN ''Highways'' WHEN 165022 THEN ''Brothel'' WHEN 165023 THEN ''Guest house/Hotels/Lodgings'' WHEN 165024 THEN ''Massage parlor'' WHEN 165025 THEN ''Chang''aa den'' WHEN 165026 THEN ''Barbershop/Salon'' WHEN 165297 THEN ''Virtual Space'' WHEN 5622 THEN ''Other (Specify)'' ELSE '''' END), NULL)) AS typology, ',
'MAX(IF(o.concept_id=165298, o.value_text, NULL)) AS other_hotspots, ',
'MAX(IF(o.concept_id=165007, o.value_numeric, NULL)) AS weekly_sex_acts, ',
'MAX(IF(o.concept_id=165299, o.value_numeric, NULL)) AS monthly_condoms_required, ',
'MAX(IF(o.concept_id=165008, o.value_numeric, NULL)) AS weekly_anal_sex_acts, ',
'MAX(IF(o.concept_id=165300, o.value_numeric, NULL)) AS monthly_lubes_required, ',
'MAX(IF(o.concept_id=165009, o.value_numeric, NULL)) AS daily_injections, ',
'MAX(IF(o.concept_id=165308, o.value_numeric, NULL)) AS monthly_syringes_required, ',
'MAX(IF(o.concept_id=165301, o.value_numeric, NULL)) AS years_in_sexwork_drugs, ',
'MAX(IF(o.concept_id=123160, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS experienced_violence, ',
'MAX(IF(o.concept_id=165302, (CASE o.value_coded WHEN 159777 THEN ''Condoms'' WHEN 165303 THEN ''Needles and Syringes'' WHEN 165004 THEN ''Contact'' WHEN 161643 THEN ''Visited Clinic'' ELSE '''' END), NULL)) AS service_provided_within_last_month, ',
'MAX(IF(o.concept_id=165341, o.value_numeric, NULL)) AS monthly_n_and_s_distributed, ',
'MAX(IF(o.concept_id=165343, o.value_numeric, NULL)) AS monthly_male_condoms_distributed, ',
'MAX(IF(o.concept_id=165057, o.value_numeric, NULL)) AS monthly_lubes_distributed, ',
'MAX(IF(o.concept_id=165344, o.value_numeric, NULL)) AS monthly_female_condoms_distributed, ',
'MAX(IF(o.concept_id=165345, o.value_numeric, NULL)) AS monthly_self_test_kits_distributed, ',
'MAX(IF(o.concept_id=1774, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS received_clinical_service, ',
'MAX(IF(o.concept_id=165272, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS violence_reported, ',
'MAX(IF(o.concept_id=1749, o.value_numeric, NULL)) AS referred, ',
'MAX(IF(o.concept_id=165346, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS health_edu, ',
'MAX(IF(o.concept_id=160632, o.value_text, NULL)) AS remarks, ',
'e.voided AS voided ',
'FROM encounter e ',
'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''c4f9db39-2c18-49a6-bf9b-b243d673c64d'')) et ON et.encounter_type_id = e.encounter_type ',
'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (165006,165005,165298,165007,165299,165008,165301,165302,165341,165343,165057,165344,165345,1774,123160,1749,165346,160632,165272) ',
'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
'GROUP BY e.patient_id, e.encounter_id ',
'ORDER BY e.patient_id ',
'ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date), encounter_provider=VALUES(encounter_provider), hotspot_name=VALUES(hotspot_name), typology=VALUES(typology), other_hotspots=VALUES(other_hotspots), weekly_sex_acts=VALUES(weekly_sex_acts), monthly_condoms_required=VALUES(monthly_condoms_required), weekly_anal_sex_acts=VALUES(weekly_anal_sex_acts), monthly_lubes_required=VALUES(monthly_lubes_required), daily_injections=VALUES(daily_injections), monthly_syringes_required=VALUES(monthly_syringes_required), years_in_sexwork_drugs=VALUES(years_in_sexwork_drugs), experienced_violence=VALUES(experienced_violence), service_provided_within_last_month=VALUES(service_provided_within_last_month), monthly_n_and_s_distributed=VALUES(monthly_n_and_s_distributed), monthly_male_condoms_distributed=VALUES(monthly_male_condoms_distributed), monthly_lubes_distributed=VALUES(monthly_lubes_distributed), monthly_female_condoms_distributed=VALUES(monthly_female_condoms_distributed), monthly_self_test_kits_distributed=VALUES(monthly_self_test_kits_distributed), received_clinical_service=VALUES(received_clinical_service), violence_reported=VALUES(violence_reported), referred=VALUES(referred), health_edu=VALUES(health_edu), remarks=VALUES(remarks), voided=VALUES(voided);'
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing Peer calendar data ', CONCAT('Time: ', NOW()));
END $$


-- ------------- populate etl_kp_sti_treatment--------------------------------
-- sql

DROP PROCEDURE IF EXISTS sp_update_etl_kp_sti_treatment $$
CREATE PROCEDURE sp_update_etl_kp_sti_treatment(IN last_update_time DATETIME)
BEGIN
  DECLARE target_table_quoted VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table_quoted = CONCAT(@etl_schema, '.etl_sti_treatment');
SELECT CONCAT('Processing STI Treatment ', CONCAT('Time: ', NOW()));
SET @sql = CONCAT(
    'INSERT INTO ', @target_table_quoted, ' (',
      'uuid, client_id, visit_id, visit_date, location_id, encounter_id, encounter_provider, date_created, date_last_modified, ',
      'visit_reason, syndrome, other_syndrome, drug_prescription, other_drug_prescription, genital_exam_done, lab_referral, lab_form_number, ',
      'referred_to_facility, facility_name, partner_referral_done, given_lubes, no_of_lubes, given_condoms, no_of_condoms, provider_comments, ',
      'provider_name, appointment_date, voided',
    ') ',
    'SELECT ',
      'e.uuid, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id AS encounter_id, e.creator AS encounter_provider, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(IF(o.concept_id=164082, (CASE o.value_coded WHEN 1068 THEN ''Symptomatic'' WHEN 5006 THEN ''Asymptomatic'' WHEN 163139 THEN ''Quartely Screening'' WHEN 160523 THEN ''Follow up'' ELSE '''' END), NULL)) AS visit_reason, ',
      'MAX(IF(o.concept_id=1169, (CASE o.value_coded WHEN 1065 THEN ''Positive'' WHEN 1066 THEN ''Negative'' ELSE '''' END), NULL)) AS syndrome, ',
      'MAX(IF(o.concept_id=165138, o.value_text, NULL)) AS other_syndrome, ',
      'MAX(IF(o.concept_id=165200, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS drug_prescription, ',
      'MAX(IF(o.concept_id=163101, o.value_text, NULL)) AS other_drug_prescription, ',
      'MAX(IF(o.concept_id=163743, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS genital_exam_done, ',
      'MAX(IF(o.concept_id=1272, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS lab_referral, ',
      'MAX(IF(o.concept_id=163042, o.value_text, NULL)) AS lab_form_number, ',
      'MAX(IF(o.concept_id=1788, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS referred_to_facility, ',
      'MAX(IF(o.concept_id=162724, o.value_text, NULL)) AS facility_name, ',
      'MAX(IF(o.concept_id=165128, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS partner_referral_done, ',
      'MAX(IF(o.concept_id=165127, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS given_lubes, ',
      'MAX(IF(o.concept_id=163169, o.value_numeric, NULL)) AS no_of_lubes, ',
      'MAX(IF(o.concept_id=159777, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS given_condoms, ',
      'MAX(IF(o.concept_id=165055, o.value_numeric, NULL)) AS no_of_condoms, ',
      'MAX(IF(o.concept_id=162749, o.value_text, NULL)) AS provider_comments, ',
      'MAX(IF(o.concept_id=1473, o.value_text, NULL)) AS provider_name, ',
      'MAX(IF(o.concept_id=5096, o.value_datetime, NULL)) AS appointment_date, ',
      'e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      '(SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''2cc8c535-bbfa-4668-98c7-b12e3550ee7b'')) et_sub ',
      'INNER JOIN (SELECT encounter_type_id FROM encounter_type WHERE uuid IN (''2cc8c535-bbfa-4668-98c7-b12e3550ee7b'')) et ON et.encounter_type_id = e.encounter_type ',
      'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
      'AND o.concept_id IN (164082,1169,165138,165200,163101,163743,1272,163042,1788,162724,165128,165127,163169,159777,165055,162749,1473,5096) ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.patient_id, e.encounter_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date), encounter_provider=VALUES(encounter_provider), visit_reason=VALUES(visit_reason), syndrome=VALUES(syndrome), ',
      'other_syndrome=VALUES(other_syndrome), drug_prescription=VALUES(drug_prescription), other_drug_prescription=VALUES(other_drug_prescription), ',
      'genital_exam_done=VALUES(genital_exam_done), lab_referral=VALUES(lab_referral), lab_form_number=VALUES(lab_form_number), referred_to_facility=VALUES(referred_to_facility), ',
      'facility_name=VALUES(facility_name), partner_referral_done=VALUES(partner_referral_done), given_lubes=VALUES(given_lubes), no_of_lubes=VALUES(no_of_lubes), ',
      'given_condoms=VALUES(given_condoms), no_of_condoms=VALUES(no_of_condoms), provider_comments=VALUES(provider_comments), provider_name=VALUES(provider_name), ',
      'appointment_date=VALUES(appointment_date), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT CONCAT('Completed processing STI Treatment data ', CONCAT('Time: ', NOW()));
END $$



- ------------- populate kp peer tracking-------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_kp_peer_tracking $$
CREATE PROCEDURE sp_update_etl_kp_peer_tracking(IN last_update_time DATETIME)
BEGIN
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_peer_tracking');
SELECT CONCAT('Processing kp peer tracking form ', CONCAT('Time: ', NOW()));
SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, provider, client_id, visit_id, visit_date, location_id, encounter_id, tracing_attempted, tracing_not_attempted_reason, attempt_number, tracing_date, tracing_type, tracing_outcome, is_final_trace, tracing_outcome_status, voluntary_exit_comment, status_in_program, source_of_information, other_informant, date_created, date_last_modified, voided) ',
    'SELECT ',
      'e.uuid, e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id=165004, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS tracing_attempted, ',
      'MAX(IF(o.concept_id=165071, (CASE o.value_coded WHEN 165078 THEN ''Contact information illegible'' WHEN 165073 THEN ''Location listed too general to make tracking possible'' WHEN 165072 THEN ''Contact information missing'' WHEN 163777 THEN ''Cohort register or peer outreach calendar reviewed and client not lost to follow up'' WHEN 5622 THEN ''other'' ELSE '''' END), NULL)) AS tracing_not_attempted_reason, ',
      'MAX(IF(o.concept_id=1639, o.value_numeric, NULL)) AS attempt_number, ',
      'MAX(IF(o.concept_id=160753, o.value_datetime, NULL)) AS tracing_date, ',
      'MAX(IF(o.concept_id=164966, (CASE o.value_coded WHEN 1650 THEN ''Phone'' WHEN 164965 THEN ''Physical'' ELSE '''' END), NULL)) AS tracing_type, ',
      'MAX(IF(o.concept_id=160721, (CASE o.value_coded WHEN 160718 THEN ''KP reached'' WHEN 160717 THEN ''KP not reached but other informant reached'' WHEN 160720 THEN ''KP not reached'' ELSE '''' END), NULL)) AS tracing_outcome, ',
      'MAX(IF(o.concept_id=163725, (CASE o.value_coded WHEN 1267 THEN ''Yes'' WHEN 163339 THEN ''No'' ELSE '''' END), NULL)) AS is_final_trace, ',
      'MAX(IF(o.concept_id=160433, (CASE o.value_coded WHEN 160432 THEN ''Dead'' WHEN 160415 THEN ''Relocated'' WHEN 165219 THEN ''Voluntary exit'' WHEN 134236 THEN ''Enrolled in MAT (applicable to PWIDS only)'' WHEN 165067 THEN ''Untraceable'' WHEN 162752 THEN ''Bedridden'' WHEN 156761 THEN ''Imprisoned'' WHEN 162632 THEN ''Found'' ELSE '''' END), NULL)) AS tracing_outcome_status, ',
      'MAX(IF(o.concept_id=160716, o.value_text, NULL)) AS voluntary_exit_comment, ',
      'MAX(IF(o.concept_id=161641, (CASE o.value_coded WHEN 5240 THEN ''Lost to follow up'' WHEN 160031 THEN ''Defaulted'' WHEN 161636 THEN ''Active'' WHEN 160432 THEN ''Dead'' ELSE '''' END), NULL)) AS status_in_program, ',
      'MAX(IF(o.concept_id=162568, (CASE o.value_coded WHEN 164929 THEN ''KP'' WHEN 165037 THEN ''PE'' WHEN 5622 THEN ''Other'' ELSE '''' END), NULL)) AS source_of_information, ',
      'MAX(IF(o.concept_id=160632, o.value_text, NULL)) AS other_informant, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''63917c60-3fea-11e9-b210-d663bd873d93'') ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (165004,165071,1639,160753,164966,160721,163725,160433,160716,161641,162568,160632) ',
    'WHERE e.voided = 0 AND (',
      IFNULL(CONCAT('e.date_created >= ''', last_update_time, ''''), 'NULL'),
      ' OR ', IFNULL(CONCAT('e.date_changed >= ''', last_update_time, ''''), 'NULL'),
      ' OR ', IFNULL(CONCAT('e.date_voided >= ''', last_update_time, ''''), 'NULL'),
      ' OR ', IFNULL(CONCAT('o.date_created >= ''', last_update_time, ''''), 'NULL'),
      ' OR ', IFNULL(CONCAT('o.date_voided >= ''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, e.encounter_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date), provider=VALUES(provider), tracing_attempted=VALUES(tracing_attempted), tracing_not_attempted_reason=VALUES(tracing_not_attempted_reason), attempt_number=VALUES(attempt_number), tracing_date=VALUES(tracing_date), tracing_type=VALUES(tracing_type), tracing_outcome=VALUES(tracing_outcome), is_final_trace=VALUES(is_final_trace), tracing_outcome_status=VALUES(tracing_outcome_status), voluntary_exit_comment=VALUES(voluntary_exit_comment), status_in_program=VALUES(status_in_program), source_of_information=VALUES(source_of_information), other_informant=VALUES(other_informant), date_created=VALUES(date_created), date_last_modified=VALUES(date_last_modified), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT CONCAT('Completed processing kp peer tracking form ', CONCAT('Time: ', NOW()));
END $$



-- sql

DROP PROCEDURE IF EXISTS sp_update_etl_kp_treatment_verification $$
CREATE PROCEDURE sp_update_etl_kp_treatment_verification(IN last_update_time DATETIME)
BEGIN
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_treatment_verification');
SELECT CONCAT('Processing kp treatment verification form ', CONCAT('Time: ', NOW()));
SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, provider, client_id, visit_id, visit_date, location_id, encounter_id, ',
      'date_diagnosed_with_hiv, art_health_facility, ccc_number, is_pepfar_site, date_initiated_art, ',
      'current_regimen, information_source, cd4_test_date, cd4, vl_test_date, viral_load, disclosed_status, ',
      'person_disclosed_to, other_person_disclosed_to, IPT_start_date, IPT_completion_date, on_diff_care, ',
      'in_support_group, support_group_name, opportunistic_infection, oi_diagnosis_date, oi_treatment_start_date, ',
      'oi_treatment_end_date, comment, date_created, date_last_modified, voided) ',
    'SELECT ',
      'e.uuid, e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id = 159948, o.value_datetime, NULL)) AS date_diagnosed_with_hiv, ',
      'MAX(IF(o.concept_id = 162724, o.value_text, \"\")) AS art_health_facility, ',
      'MAX(IF(o.concept_id = 162053, o.value_numeric, \"\")) AS ccc_number, ',
      'MAX(IF(o.concept_id = 1768, (CASE o.value_coded WHEN 1065 THEN \"Yes\" WHEN 1066 THEN \"No\" ELSE \"\" END), NULL)) AS is_pepfar_site, ',
      'MAX(IF(o.concept_id = 159599, o.value_datetime, NULL)) AS date_initiated_art, ',
      'MAX(IF(o.concept_id = 164515, (CASE o.value_coded ',
        'WHEN 162565 THEN \"TDF/3TC/NVP\" WHEN 164505 THEN \"TDF/3TC/EFV\" WHEN 1652 THEN \"AZT/3TC/NVP\" WHEN 160124 THEN \"AZT/3TC/EFV\" ',
        'WHEN 792 THEN \"D4T/3TC/NVP\" WHEN 160104 THEN \"D4T/3TC/EFV\" WHEN 162561 THEN \"AZT/3TC/LPV/r\" WHEN 164511 THEN \"AZT/3TC/ATV/r\" ',
        'WHEN 164512 THEN \"TDF/3TC/ATV/r\" WHEN 162201 THEN \"TDF/3TC/LPV/r\" WHEN 162560 THEN \"D4T/3TC/LPV/r\" WHEN 162200 THEN \"ABC/3TC/LPV/r\" ',
        'WHEN 164971 THEN \"TDF/3TC/AZT\" WHEN 164968 THEN \"AZT/3TC/DTG\" WHEN 164969 THEN \"TDF/3TC/DTG\" WHEN 164970 THEN \"ABC/3TC/DTG\" ',
        'WHEN 164972 THEN \"AZT/TDF/3TC/LPV/r\" WHEN 164973 THEN \"ETR/RAL/DRV/RTV\" WHEN 164974 THEN \"ETR/TDF/3TC/LPV/r\" WHEN 165357 THEN \"ABC/3TC/ATV/r\" ',
        'WHEN 165375 THEN \"RAL/3TC/DRV/RTV\" WHEN 165376 THEN \"RAL/3TC/DRV/RTV/AZT\" WHEN 165379 THEN \"RAL/3TC/DRV/RTV/TDF\" WHEN 165378 THEN \"ETV/3TC/DRV/RTV\" ',
        'WHEN 165369 THEN \"TDF/3TC/DTG/DRV/r\" WHEN 165370 THEN \"TDF/3TC/RAL/DRV/r\" WHEN 165371 THEN \"TDF/3TC/DTG/EFV/DRV/r\" ELSE \"\" END), NULL)) AS current_regimen, ',
      'MAX(IF(o.concept_id = 162568, (CASE o.value_coded WHEN 162969 THEN \"SMS\" WHEN 163787 THEN \"Verbal report\" WHEN 1238 THEN \"Written record\" ',
        'WHEN 162189 THEN \"Phone call\" WHEN 160526 THEN \"EID Dashboard\" WHEN 165048 THEN \"Appointment card\" ELSE \"\" END), NULL)) AS information_source, ',
      'MAX(IF(o.concept_id = 160103, o.value_datetime, NULL)) AS cd4_test_date, ',
      'MAX(IF(o.concept_id = 5497, o.value_numeric, \"\")) AS cd4, ',
      'MAX(IF(o.concept_id = 163281, o.value_datetime, NULL)) AS vl_test_date, ',
      'MAX(IF(o.concept_id = 160632, o.value_numeric, \"\")) AS viral_load, ',
      'MAX(IF(o.concept_id = 163524, (CASE o.value_coded WHEN 1065 THEN \"Yes\" WHEN 1066 THEN \"No\" ELSE \"\" END), NULL)) AS disclosed_status, ',
      'MAX(IF(o.concept_id = 5616, (CASE o.value_coded WHEN 159423 THEN \"Sexual Partner\" WHEN 1560 THEN \"Family member\" WHEN 161642 THEN \"Treatment partner\" ',
        'WHEN 160639 THEN \"Spiritual Leader\" WHEN 5622 THEN \"Other\" ELSE \"\" END), NULL)) AS person_disclosed_to, ',
      'MAX(IF(o.concept_id = 163101, o.value_text, \"\")) AS other_person_disclosed_to, ',
      'MAX(IF(o.concept_id = 162320, o.value_datetime, NULL)) AS IPT_start_date, ',
      'MAX(IF(o.concept_id = 162279, o.value_datetime, NULL)) AS IPT_completion_date, ',
      'MAX(IF(o.concept_id = 164947, (CASE o.value_coded WHEN 1065 THEN \"Yes\" WHEN 1066 THEN \"No\" ELSE \"\" END), NULL)) AS on_diff_care, ',
      'MAX(IF(o.concept_id = 165302, (CASE o.value_coded WHEN 1065 THEN \"Yes\" WHEN 1066 THEN \"No\" ELSE \"\" END), NULL)) AS in_support_group, ',
      'MAX(IF(o.concept_id = 165137, o.value_text, \"\")) AS support_group_name, ',
      'MAX(IF(o.concept_id = 162634, (CASE o.value_coded WHEN 112141 THEN \"Tuberculosis\" WHEN 990 THEN \"Toxoplasmosis\" WHEN 130021 THEN \"Pneumocystosis carinii pneumonia\" ',
        'WHEN 114100 THEN \"Pneumonia\" WHEN 136326 THEN \"Kaposi Sarcoma\" WHEN 123118 THEN \"HIV encephalitis\" WHEN 117543 THEN \"Herpes Zoster\" WHEN 154119 THEN \"Cytomegalovirus (CMV)\" ',
        'WHEN 1219 THEN \"Cryptococcosis\" WHEN 120939 THEN \"Candidiasis\" WHEN 116104 THEN \"Lymphoma\" WHEN 5622 THEN \"Other\" ELSE \"\" END), NULL)) AS opportunistic_infection, ',
      'MAX(IF(o.concept_id = 159948, o.value_datetime, NULL)) AS oi_diagnosis_date, ',
      'MAX(IF(o.concept_id = 160753, o.value_datetime, NULL)) AS oi_treatment_start_date, ',
      'MAX(IF(o.concept_id = 162868, o.value_datetime, NULL)) AS oi_treatment_end_date, ',
      'MAX(IF(o.concept_id = 161011, o.value_text, \"\")) AS comment, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'e.voided AS voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''a70a1132-75b3-11ea-bc55-0242ac130003'') ',
    'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
    'AND o.concept_id IN (159948,162724,162053,1768,159599,164515,162568,657,5497,163281,160632,163524,5616,160716,161641,163101,162320,162279,164947,165302,165137,162634,160753,162868,161011) ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, e.encounter_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date), provider=VALUES(provider), date_diagnosed_with_hiv=VALUES(date_diagnosed_with_hiv), ',
      'art_health_facility=VALUES(art_health_facility), ccc_number=VALUES(ccc_number), is_pepfar_site=VALUES(is_pepfar_site), ',
      'date_initiated_art=VALUES(date_initiated_art), current_regimen=VALUES(current_regimen), information_source=VALUES(information_source), ',
      'cd4_test_date=VALUES(cd4_test_date), cd4=VALUES(cd4), vl_test_date=VALUES(vl_test_date), viral_load=VALUES(viral_load), ',
      'disclosed_status=VALUES(disclosed_status), person_disclosed_to=VALUES(person_disclosed_to), other_person_disclosed_to=VALUES(other_person_disclosed_to), ',
      'IPT_start_date=VALUES(IPT_start_date), IPT_completion_date=VALUES(IPT_completion_date), on_diff_care=VALUES(on_diff_care), ',
      'in_support_group=VALUES(in_support_group), support_group_name=VALUES(support_group_name), opportunistic_infection=VALUES(opportunistic_infection), ',
      'oi_diagnosis_date=VALUES(oi_diagnosis_date), oi_treatment_start_date=VALUES(oi_treatment_start_date), oi_treatment_end_date=VALUES(oi_treatment_end_date), ',
      'comment=VALUES(comment), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT CONCAT('Completed processing kp treatment verification form ', CONCAT('Time: ', NOW()));
END $$


-- ------------- Update kp PrEP verification-------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_PrEP_verification $$
CREATE PROCEDURE sp_update_etl_PrEP_verification(IN last_update_time DATETIME)
BEGIN
  DECLARE target_table_quoted VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table_quoted = CONCAT(@etl_schema, '.etl_PrEP_verification');

SELECT CONCAT('Processing PrEP verification form ', CONCAT('Time: ', NOW()));

SET @sql = CONCAT(
    'INSERT INTO ', @target_table_quoted, ' (uuid, provider, client_id, visit_id, visit_date, location_id, encounter_id, date_enrolled, health_facility_accessing_PrEP, is_pepfar_site, date_initiated_PrEP, PrEP_regimen, information_source, PrEP_status, verification_date, discontinuation_reason, other_discontinuation_reason, appointment_date, date_created, date_last_modified, voided) ',
    'SELECT ',
      'e.uuid, e.creator AS provider, e.patient_id AS client_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id = 163526, o.value_datetime, NULL)) AS date_enrolled, ',
      'MAX(IF(o.concept_id = 162724, o.value_text, \"\")) AS health_facility_accessing_PrEP, ',
      'MAX(IF(o.concept_id = 1768, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' WHEN 1067 THEN ''Unknown'' ELSE '''' END), NULL)) AS is_pepfar_site, ',
      'MAX(IF(o.concept_id = 160555, o.value_datetime, NULL)) AS date_initiated_PrEP, ',
      'MAX(IF(o.concept_id = 164515, (CASE o.value_coded WHEN 161364 THEN ''TDF/3TC'' WHEN 84795 THEN ''TDF'' WHEN 104567 THEN ''TDF/FTC(Preferred)'' ELSE '''' END), NULL)) AS PrEP_regimen, ',
      'MAX(IF(o.concept_id = 162568, (CASE o.value_coded WHEN 163787 THEN ''Verbal report'' WHEN 162969 THEN ''SMS'' WHEN 1662 THEN ''Appointment card'' WHEN 1650 THEN ''Phone call'' WHEN 1238 THEN ''Written record'' WHEN 160526 THEN ''EID Dashboard'' ELSE '''' END), NULL)) AS information_source, ',
      'MAX(IF(o.concept_id = 165109, (CASE o.value_coded WHEN 1256 THEN ''Start'' WHEN 1257 THEN ''Continue'' WHEN 162904 THEN ''Restart'' WHEN 1260 THEN ''Discontinue'' ELSE '''' END), NULL)) AS PrEP_status, ',
      'MAX(IF(o.concept_id = 162079, o.value_datetime, NULL)) AS verification_date, ',
      'MAX(IF(o.concept_id = 161555, (CASE o.value_coded WHEN 138571 THEN ''HIV test is positive'' WHEN 1302 THEN ''Viral suppression of HIV+ Partner'' WHEN 159598 THEN ''Not adherent to PrEP'' WHEN 164401 THEN ''Too many HIV tests'' WHEN 162696 THEN ''Client request'' WHEN 5622 THEN ''Other'' ELSE '''' END), NULL)) AS discontinuation_reason, ',
      'MAX(IF(o.concept_id = 165230, o.value_text, \"\")) AS other_discontinuation_reason, ',
      'MAX(IF(o.concept_id = 5096, o.value_datetime, NULL)) AS appointment_date, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''5c64e61a-7fdc-11ea-bc55-0242ac130003'') ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (163526,162724,1768,160555,164515,162568,162079,165109,161555,165230,5096) ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, e.encounter_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date), provider=VALUES(provider), date_enrolled=VALUES(date_enrolled), health_facility_accessing_PrEP=VALUES(health_facility_accessing_PrEP), is_pepfar_site=VALUES(is_pepfar_site), date_initiated_PrEP=VALUES(date_initiated_PrEP), PrEP_regimen=VALUES(PrEP_regimen), information_source=VALUES(information_source), PrEP_status=VALUES(PrEP_status), verification_date=VALUES(verification_date), discontinuation_reason=VALUES(discontinuation_reason), other_discontinuation_reason=VALUES(other_discontinuation_reason), appointment_date=VALUES(appointment_date), date_created=VALUES(date_created), date_last_modified=VALUES(date_last_modified), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT CONCAT('Completed processing PrEP verification form ', CONCAT('Time: ', NOW()));
END $$


-- ------------- populate etl_alcohol_drug_abuse_screening-------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_alcohol_drug_abuse_screening $$
CREATE PROCEDURE sp_update_etl_alcohol_drug_abuse_screening(IN last_update_time DATETIME)
BEGIN
  DECLARE target_table_quoted VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table_quoted = CONCAT(@etl_schema, '.etl_alcohol_drug_abuse_screening');
SELECT 'Processing Alcohol and Drug Abuse Screening(CAGE-AID/CRAFFT)', CONCAT('Time: ', NOW());

SET @sql = CONCAT(
    'INSERT INTO ', @target_table_quoted, ' (patient_id, uuid, provider, visit_id, visit_date, encounter_id, location_id, alcohol_drinking_frequency, smoking_frequency, drugs_use_frequency, date_created, date_last_modified, voided) ',
    'SELECT e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.encounter_id, e.location_id, ',
    'MAX(IF(o.concept_id = 159449, o.value_coded, NULL)) AS alcohol_drinking_frequency, ',
    'MAX(IF(o.concept_id = 163201, o.value_coded, NULL)) AS smoking_frequency, ',
    'MAX(IF(o.concept_id = 112603, o.value_coded, NULL)) AS drugs_use_frequency, ',
    'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, e.voided AS voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''7b1ec2d5-a4ad-4ffc-a0d3-ff1ea68e293c'') ',
    'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (159449, 163201, 112603) ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.encounter_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date), provider=VALUES(provider), alcohol_drinking_frequency=VALUES(alcohol_drinking_frequency), smoking_frequency=VALUES(smoking_frequency), drugs_use_frequency=VALUES(drugs_use_frequency), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing Alcohol and Drug Abuse Screening(CAGE-AID/CRAFFT) data', CONCAT('Time: ', NOW());
END $$


  --------- update table etl_gbv_screening -----------
 ;
DROP PROCEDURE IF EXISTS sp_update_etl_gbv_screening $$
CREATE PROCEDURE sp_update_etl_gbv_screening(IN last_update_time DATETIME)
BEGIN
  DECLARE target_table_quoted VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table_quoted = CONCAT(@etl_schema, '.etl_gbv_screening');
SELECT 'Processing GBV Screening', CONCAT('Time: ', NOW());
SET @sql = CONCAT(
    'INSERT INTO ', @target_table_quoted, ' (',
      'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, ',
      'ipv, physical_ipv, emotional_ipv, sexual_ipv, ipv_relationship, date_created, date_last_modified, voided',
    ') ',
    'SELECT ',
      'e.uuid, e.creator AS provider, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id = 165116, o.value_coded, NULL)) AS ipv, ',
      'MAX(IF(o.concept_id = 165117, o.value_coded, NULL)) AS physical_ipv, ',
      'MAX(IF(o.concept_id = 165034, o.value_coded, NULL)) AS emotional_ipv, ',
      'MAX(IF(o.concept_id = 165070, o.value_coded, NULL)) AS sexual_ipv, ',
      'MAX(IF(o.concept_id = 165045, o.value_coded, NULL)) AS ipv_relationship, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''03767614-1384-4ce3-aea9-27e2f4e67d01'', ''94eec122-83a1-11ea-bc55-0242ac130003'') ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 AND o.concept_id IN (160658,165116,165117,165034,165070,165045,141814) ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date), ',
      'provider=VALUES(provider), ',
      'ipv=VALUES(ipv), physical_ipv=VALUES(physical_ipv), emotional_ipv=VALUES(emotional_ipv), sexual_ipv=VALUES(sexual_ipv), ',
      'ipv_relationship=VALUES(ipv_relationship), ',
      'date_created=VALUES(date_created), date_last_modified=VALUES(date_last_modified), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing GBV Screening', CONCAT('Time: ', NOW());
END $$

------updating etl_gbv_screening_action table-------
-- sql

DROP PROCEDURE IF EXISTS sp_update_etl_gbv_screening_action $$
CREATE PROCEDURE sp_update_etl_gbv_screening_action(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_gbv_screening_action');
SELECT 'Processing GBV Screening Action', CONCAT('Time: ', NOW());
SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, provider, patient_id, encounter_id, visit_id, visit_date, location_id, obs_id, ',
      'help_provider, action_taken, action_date, reason_for_not_reporting, date_created, date_last_modified, voided',
    ') ',
    'SELECT ',
      'e.uuid, e.creator AS provider, e.patient_id, e.encounter_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, o.child_obs_id AS obs_id, ',
      'MAX(IF(o.obs_group = 1562 AND o.child_concept_id = 162886, o.child_value_coded, NULL)) AS help_provider, ',
      'MAX(IF(o.obs_group = 159639 AND o.child_concept_id = 162875, o.child_value_coded, NULL)) AS action_taken, ',
      'MAX(IF(o.obs_group = 1562 AND o.child_concept_id = 160753, o.child_value_datetime, NULL)) AS action_date, ',
      'MAX(IF(o.obs_group = 1743 AND o.child_concept_id = 6098, o.child_value_coded, NULL)) AS reason_for_not_reporting, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.child_date_created) > MIN(e.date_created), MAX(o.child_date_created), NULL) AS date_last_modified, ',
      'e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''03767614-1384-4ce3-aea9-27e2f4e67d01'', ''94eec122-83a1-11ea-bc55-0242ac130003'') ',
      'INNER JOIN (',
        'SELECT o.encounter_id AS encounter_id, o.person_id AS person_id, o.obs_id AS parent_obs_id, ',
               'o1.obs_id AS child_obs_id, o.concept_id AS obs_group, o1.concept_id AS child_concept_id, ',
               'o1.value_coded AS child_value_coded, o1.value_datetime AS child_value_datetime, ',
               'o1.date_created AS child_date_created, o1.date_voided AS child_date_voided, o1.voided AS child_voided ',
        'FROM obs o ',
        'JOIN obs o1 ON o.obs_id = o1.obs_group_id AND o1.concept_id IN (162871,162886,162875,6098,160753) ',
        'WHERE o.concept_id IN (1562,159639,1743)',
      ') o ON o.encounter_id = e.encounter_id AND o.child_voided = 0 ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.child_date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.child_date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY o.child_obs_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), ',
      'provider = VALUES(provider), ',
      'help_provider = VALUES(help_provider), ',
      'action_taken = VALUES(action_taken), ',
      'action_date = VALUES(action_date), ',
      'reason_for_not_reporting = VALUES(reason_for_not_reporting), ',
      'date_created = VALUES(date_created), ',
      'date_last_modified = VALUES(date_last_modified), ',
      'voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing GBV screening action data', CONCAT('Time: ', NOW());
END $$


-- Update Violence Reporting
-- sql

DROP PROCEDURE IF EXISTS sp_update_etl_violence_reporting $$
CREATE PROCEDURE sp_update_etl_violence_reporting(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_violence_reporting');
SELECT 'Processing violence reporting', CONCAT('Time: ', NOW());
SET sql_stmt = CONCAT(
'INSERT INTO ', @target_table, ' (',
'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, place_of_incident, date_of_incident, time_of_incident, abuse_against, form_of_incident, perpetrator, date_of_crisis_response, support_service, hiv_testing_duration, hiv_testing_provided_within_5_days, duration_on_emergency_contraception, emergency_contraception_provided_within_5_days, psychosocial_trauma_counselling_duration, psychosocial_trauma_counselling_provided_within_5_days, pep_provided_duration, pep_provided_within_5_days, sti_screening_and_treatment_duration, sti_screening_and_treatment_provided_within_5_days, legal_support_duration, legal_support_provided_within_5_days, medical_examination_duration, medical_examination_provided_within_5_days, prc_form_file_duration, prc_form_file_provided_within_5_days, other_services_provided, medical_services_and_care_duration, medical_services_and_care_provided_within_5_days, psychosocial_trauma_counselling_durationA, psychosocial_trauma_counselling_provided_within_5_daysA, duration_of_none_sexual_legal_support, duration_of_none_sexual_legal_support_within_5_days, current_Location_of_person, follow_up_plan, resolution_date, date_created, date_last_modified, voided',
') ',
'SELECT ',
'e.uuid,e.creator,e.patient_id,e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
'MAX(IF(o.concept_id = 162725, o.value_text, NULL)) AS place_of_incident, ',
'MAX(IF(o.concept_id = 160753, o.value_datetime, NULL)) AS date_of_incident, ',
'MAX(IF(o.concept_id = 161244, o.value_coded, NULL)) AS time_of_incident, ',
'MAX(IF(o.concept_id = 165164, o.value_coded, NULL)) AS abuse_against, ',
'CONCAT_WS('','', ',
'MAX(IF(o.concept_id = 165228 AND o.value_coded = 165161, ''Harrasment'', NULL)),',
'MAX(IF(o.concept_id = 165228 AND o.value_coded = 123007, ''Verbal Abuse'', NULL)),',
'MAX(IF(o.concept_id = 165228 AND o.value_coded = 126312, ''Discrimination'', NULL)),',
'MAX(IF(o.concept_id = 165228 AND o.value_coded = 152292, ''Assault/physical abuse'', NULL)),',
'MAX(IF(o.concept_id = 165228 AND o.value_coded = 152370, ''Rape/Sexual Assault'', NULL)),',
'MAX(IF(o.concept_id = 165228 AND o.value_coded = 156761, ''Illegal arrest'', NULL)),',
'MAX(IF(o.concept_id = 165228 AND o.value_coded = 141537, ''Economic'', NULL)),',
'MAX(IF(o.concept_id = 165228 AND o.value_coded = 5622, ''Other'', NULL))',
') AS form_of_incident, ',
'CONCAT_WS('','', ',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 165283, ''Local Gang'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 165284, ''Police/Prison Officers'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 165285, ''General Public'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 165286, ''Clients'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 165193, ''Local Authority'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 163488, ''Community Members'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 165291, ''Drug Peddler'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 165290, ''Religious Group'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 165292, ''Pimp/Madam'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 165293, ''Bar Owner/Manager'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 162277, ''Inmates'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 1560, ''Family'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 165294, ''Partner'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 5619, ''Health Provider'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 165289, ''Education institution'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 165295, ''Neighbor'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 165296, ''Employer'', NULL)),',
'MAX(IF(o.concept_id = 165229 AND o.value_coded = 5622, ''Other'', NULL))',
') AS perpetrator, ',
'MAX(IF(o.concept_id = 165349, o.value_datetime, NULL)) AS date_of_crisis_response, ',
'MAX(IF(o.concept_id = 165225, o.value_text, NULL)) AS support_service, ',
'MAX(IF(o.concept_id = 159368, IF(o.value_numeric > 10000, 10000, o.value_numeric), NULL)) AS hiv_testing_duration, ',
'MAX(IF(o.concept_id = 165165, o.value_coded, NULL)) AS hiv_testing_provided_within_5_days, ',
'MAX(IF(o.concept_id = 165166, o.value_numeric, NULL)) AS duration_on_emergency_contraception, ',
'MAX(IF(o.concept_id = 165167, o.value_coded, NULL)) AS emergency_contraception_provided_within_5_days, ',
'MAX(IF(o.concept_id = 165168, o.value_numeric, NULL)) AS psychosocial_trauma_counselling_duration, ',
'MAX(IF(o.concept_id = 165169, o.value_coded, NULL)) AS psychosocial_trauma_counselling_provided_within_5_days, ',
'MAX(IF(o.concept_id = 165170, o.value_numeric, NULL)) AS pep_provided_duration, ',
'MAX(IF(o.concept_id = 165171, o.value_coded, NULL)) AS pep_provided_within_5_days, ',
'MAX(IF(o.concept_id = 165190, o.value_numeric, NULL)) AS sti_screening_and_treatment_duration, ',
'MAX(IF(o.concept_id = 165172, o.value_coded, NULL)) AS sti_screening_and_treatment_provided_within_5_days, ',
'MAX(IF(o.concept_id = 165173, o.value_numeric, NULL)) AS legal_support_duration, ',
'MAX(IF(o.concept_id = 165174, o.value_coded, NULL)) AS legal_support_provided_within_5_days, ',
'MAX(IF(o.concept_id = 165175, o.value_numeric, NULL)) AS medical_examination_duration, ',
'MAX(IF(o.concept_id = 165176, o.value_coded, NULL)) AS medical_examination_provided_within_5_days, ',
'MAX(IF(o.concept_id = 165178, o.value_numeric, NULL)) AS prc_form_file_duration, ',
'MAX(IF(o.concept_id = 165177, o.value_coded, NULL)) AS prc_form_file_provided_within_5_days, ',
'MAX(IF(o.concept_id = 163108, o.value_text, NULL)) AS other_services_provided, ',
'MAX(IF(o.concept_id = 165181, o.value_numeric, NULL)) AS medical_services_and_care_duration, ',
'MAX(IF(o.concept_id = 165182, o.value_coded, NULL)) AS medical_services_and_care_provided_within_5_days, ',
'MAX(IF(o.concept_id = 165183, o.value_numeric, NULL)) AS psychosocial_trauma_counselling_durationA, ',
'MAX(IF(o.concept_id = 165184, o.value_coded, NULL)) AS psychosocial_trauma_counselling_provided_within_5_daysA, ',
'MAX(IF(o.concept_id = 165187, o.value_numeric, NULL)) AS duration_of_none_sexual_legal_support, ',
'MAX(IF(o.concept_id = 165188, o.value_coded, NULL)) AS duration_of_none_sexual_legal_support_within_5_days, ',
'MAX(IF(o.concept_id = 165189, o.value_coded, NULL)) AS current_Location_of_person, ',
'MAX(IF(o.concept_id = 164378, o.value_text, NULL)) AS follow_up_plan, ',
'MAX(IF(o.concept_id = 165224, o.value_datetime, NULL)) AS resolution_date, ',
'e.date_created AS date_created, ',
'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
'e.voided AS voided ',
'FROM encounter e ',
'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''10cd2ca0-8d25-4876-b97c-b568a912957e'') ',
'INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (162725,160753,161244,165164,165228,165229,165206,165349,165225,159368,165165,165166,165167,165168,165169,165170,165171,165190,165172,165173,165174,165175,165176,165178,165177,163108,165181,165182,165183,165184,165187,165188,165189,164378,165224) AND o.voided = 0 ',
'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
'GROUP BY e.encounter_id ',
'ORDER BY e.patient_id ',
'ON DUPLICATE KEY UPDATE ',
'visit_date = VALUES(visit_date), provider = VALUES(provider), place_of_incident = VALUES(place_of_incident), date_of_incident = VALUES(date_of_incident), time_of_incident = VALUES(time_of_incident), abuse_against = VALUES(abuse_against), form_of_incident = VALUES(form_of_incident), perpetrator = VALUES(perpetrator), date_of_crisis_response = VALUES(date_of_crisis_response), support_service = VALUES(support_service), hiv_testing_duration = VALUES(hiv_testing_duration), hiv_testing_provided_within_5_days = VALUES(hiv_testing_provided_within_5_days), duration_on_emergency_contraception = VALUES(duration_on_emergency_contraception), emergency_contraception_provided_within_5_days = VALUES(emergency_contraception_provided_within_5_days), psychosocial_trauma_counselling_duration = VALUES(psychosocial_trauma_counselling_duration), psychosocial_trauma_counselling_provided_within_5_days = VALUES(psychosocial_trauma_counselling_provided_within_5_days), pep_provided_duration = VALUES(pep_provided_duration), pep_provided_within_5_days = VALUES(pep_provided_within_5_days), sti_screening_and_treatment_duration = VALUES(sti_screening_and_treatment_duration), sti_screening_and_treatment_provided_within_5_days = VALUES(sti_screening_and_treatment_provided_within_5_days), legal_support_duration = VALUES(legal_support_duration), legal_support_provided_within_5_days = VALUES(legal_support_provided_within_5_days), medical_examination_duration = VALUES(medical_examination_duration), medical_examination_provided_within_5_days = VALUES(medical_examination_provided_within_5_days), prc_form_file_duration = VALUES(prc_form_file_duration), prc_form_file_provided_within_5_days = VALUES(prc_form_file_provided_within_5_days), other_services_provided = VALUES(other_services_provided), medical_services_and_care_duration = VALUES(medical_services_and_care_duration), medical_services_and_care_provided_within_5_days = VALUES(medical_services_and_care_provided_within_5_days), psychosocial_trauma_counselling_durationA = VALUES(psychosocial_trauma_counselling_durationA), psychosocial_trauma_counselling_provided_within_5_daysA = VALUES(psychosocial_trauma_counselling_provided_within_5_daysA), duration_of_none_sexual_legal_support = VALUES(duration_of_none_sexual_legal_support), duration_of_none_sexual_legal_support_within_5_days = VALUES(duration_of_none_sexual_legal_support_within_5_days), current_Location_of_person = VALUES(current_Location_of_person), follow_up_plan = VALUES(follow_up_plan), resolution_date = VALUES(resolution_date), date_created = VALUES(date_created), date_last_modified = VALUES(date_last_modified), voided = VALUES(voided);'
  );
PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing violence reporting data', CONCAT('Time: ', NOW());
END $$


-- Update Link facility tracking form-----------
-- sql


DROP PROCEDURE IF EXISTS sp_update_etl_link_facility_tracking $$
CREATE PROCEDURE sp_update_etl_link_facility_tracking(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_link_facility_tracking');
SELECT 'Processing link facility tracking form', CONCAT('Time: ', NOW());
SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
    'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, ',
    'county, sub_county, ward, facility_name, ccc_number, date_diagnosed, date_initiated_art, ',
    'original_regimen, current_regimen, date_switched, reason_for_switch, date_of_last_visit, ',
    'date_viral_load_sample_collected, date_viral_load_results_received, viral_load_results, viral_load_results_copies, ',
    'date_of_next_visit, enrolled_in_pssg, attended_pssg, on_pmtct, date_of_delivery, tb_screening, ',
    'sti_treatment, trauma_counselling, cervical_cancer_screening, family_planning, currently_on_tb_treatment, ',
    'date_initiated_tb_treatment, tpt_status, date_initiated_tpt, data_collected_through, date_created, date_last_modified, voided',
    ') ',
    'SELECT ',
    'e.uuid, e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
    'MAX(IF(o.concept_id = 167992, o.value_text, NULL)) AS county, ',
    'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS sub_county, ',
    'MAX(IF(o.concept_id = 165137, o.value_text, NULL)) AS ward, ',
    'MAX(IF(o.concept_id = 162724, o.value_text, NULL)) AS facility_name, ',
    'MAX(IF(o.concept_id = 162053, o.value_text, NULL)) AS ccc_number, ',
    'MAX(IF(o.concept_id = 159948, o.value_datetime, NULL)) AS date_diagnosed, ',
    'MAX(IF(o.concept_id = 159599, o.value_datetime, NULL)) AS date_initiated_art, ',
    'MAX(IF(o.concept_id = 164855, o.value_coded, NULL)) AS original_regimen, ',
    'MAX(IF(o.concept_id = 164432, o.value_coded, NULL)) AS current_regimen, ',
    'MAX(IF(o.concept_id = 164516, o.value_datetime, NULL)) AS date_switched, ',
    'MAX(IF(o.concept_id = 162725, o.value_text, NULL)) AS reason_for_switch, ',
    'MAX(IF(o.concept_id = 164093, o.value_datetime, NULL)) AS date_of_last_visit, ',
    'MAX(IF(o.concept_id = 162078, o.value_datetime, NULL)) AS date_viral_load_sample_collected, ',
    'MAX(IF(o.concept_id = 163281, o.value_datetime, NULL)) AS date_viral_load_results_received, ',
    'MAX(IF(o.concept_id = 165236, o.value_coded, NULL)) AS viral_load_results, ',
    'MAX(IF(o.concept_id = 856, o.value_numeric, NULL)) AS viral_load_results_copies, ',
    'MAX(IF(o.concept_id = 5096, o.value_datetime, NULL)) AS date_of_next_visit, ',
    'MAX(IF(o.concept_id = 165163, o.value_coded, NULL)) AS enrolled_in_pssg, ',
    'MAX(IF(o.concept_id = 164999, o.value_coded, NULL)) AS attended_pssg, ',
    'MAX(IF(o.concept_id = 163532, o.value_coded, NULL)) AS on_pmtct, ',
    'MAX(IF(o.concept_id = 5599, o.value_datetime, NULL)) AS date_of_delivery, ',
    'MAX(IF(o.concept_id = 166663, o.value_coded, NULL)) AS tb_screening, ',
    'MAX(IF(o.concept_id = 166665, o.value_coded, NULL)) AS sti_treatment, ',
    'MAX(IF(o.concept_id = 165184, o.value_coded, NULL)) AS trauma_counselling, ',
    'MAX(IF(o.concept_id = 165086, o.value_coded, NULL)) AS cervical_cancer_screening, ',
    'MAX(IF(o.concept_id = 160653, o.value_coded, NULL)) AS family_planning, ',
    'MAX(IF(o.concept_id = 162309, o.value_coded, NULL)) AS currently_on_tb_treatment, ',
    'MAX(IF(o.concept_id = 1113, o.value_datetime, NULL)) AS date_initiated_tb_treatment, ',
    'MAX(IF(o.concept_id = 162230, o.value_coded, NULL)) AS tpt_status, ',
    'MAX(IF(o.concept_id = 162320, o.value_datetime, NULL)) AS date_initiated_tpt, ',
    'MAX(IF(o.concept_id = 162568, o.value_coded, NULL)) AS data_collected_through, ',
    'e.date_created AS date_created, ',
    'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
    'e.voided AS voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''052ede51-ddda-4f04-aa25-754ff40abf37'') ',
    'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.voided = 0 ',
    'AND o.concept_id IN (167992,160632,165137,162724,162053,159948,159599,164855,164432,164516,162725,164093,162078,163281,165236,856,5096,165163,164999,163532,5599,166663,166665,165184,165086,160653,162309,1113,162230,162320,162568) ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
    'visit_date = VALUES(visit_date), ',
    'provider = VALUES(provider), ',
    'county = VALUES(county), sub_county = VALUES(sub_county), ward = VALUES(ward), facility_name = VALUES(facility_name), ccc_number = VALUES(ccc_number), ',
    'date_diagnosed = VALUES(date_diagnosed), date_initiated_art = VALUES(date_initiated_art), original_regimen = VALUES(original_regimen), current_regimen = VALUES(current_regimen), ',
    'date_switched = VALUES(date_switched), reason_for_switch = VALUES(reason_for_switch), date_of_last_visit = VALUES(date_of_last_visit), ',
    'date_viral_load_sample_collected = VALUES(date_viral_load_sample_collected), date_viral_load_results_received = VALUES(date_viral_load_results_received), viral_load_results = VALUES(viral_load_results), viral_load_results_copies = VALUES(viral_load_results_copies), ',
    'date_of_next_visit = VALUES(date_of_next_visit), enrolled_in_pssg = VALUES(enrolled_in_pssg), attended_pssg = VALUES(attended_pssg), on_pmtct = VALUES(on_pmtct), ',
    'date_of_delivery = VALUES(date_of_delivery), tb_screening = VALUES(tb_screening), sti_treatment = VALUES(sti_treatment), trauma_counselling = VALUES(trauma_counselling), ',
    'cervical_cancer_screening = VALUES(cervical_cancer_screening), family_planning = VALUES(family_planning), currently_on_tb_treatment = VALUES(currently_on_tb_treatment), ',
    'date_initiated_tb_treatment = VALUES(date_initiated_tb_treatment), tpt_status = VALUES(tpt_status), date_initiated_tpt = VALUES(date_initiated_tpt), data_collected_through = VALUES(data_collected_through), ',
    'voided = VALUES(voided)'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing link facility tracking form', CONCAT('Time: ', NOW());
END $$


-- Update Depression Screening


DROP PROCEDURE IF EXISTS sp_update_etl_depression_screening $$
CREATE PROCEDURE sp_update_etl_depression_screening(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_depression_screening');
SELECT 'Processing depression screening', CONCAT('Time: ', NOW());
SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, ',
      'little_interest, feeling_down, trouble_sleeping, feeling_tired, poor_appetite, feeling_bad, ',
      'trouble_concentrating, moving_or_speaking_slowly, self_hurtful_thoughts, phq_9_rating, pfa_offered, ',
      'client_referred, facility_referred, facility_name, services_referred_for, date_created, date_last_modified, voided) ',
    'SELECT ',
      'e.uuid, e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id = 167006, o.value_coded, NULL)) AS little_interest, ',
      'MAX(IF(o.concept_id = 167007, o.value_coded, NULL)) AS feeling_down, ',
      'MAX(IF(o.concept_id = 167068, o.value_coded, NULL)) AS trouble_sleeping, ',
      'MAX(IF(o.concept_id = 167069, o.value_coded, NULL)) AS feeling_tired, ',
      'MAX(IF(o.concept_id = 167070, o.value_coded, NULL)) AS poor_appetite, ',
      'MAX(IF(o.concept_id = 167071, o.value_coded, NULL)) AS feeling_bad, ',
      'MAX(IF(o.concept_id = 167072, o.value_coded, NULL)) AS trouble_concentrating, ',
      'MAX(IF(o.concept_id = 167073, o.value_coded, NULL)) AS moving_or_speaking_slowly, ',
      'MAX(IF(o.concept_id = 167074, o.value_coded, NULL)) AS self_hurtful_thoughts, ',
      'MAX(IF(o.concept_id = 165110, o.value_coded, NULL)) AS phq_9_rating, ',
      'MAX(IF(o.concept_id = 165302, o.value_coded, NULL)) AS pfa_offered, ',
      'MAX(IF(o.concept_id = 166656, o.value_coded, NULL)) AS client_referred, ',
      'MAX(IF(o.concept_id = 166636, o.value_coded, NULL)) AS facility_referred, ',
      'MAX(IF(o.concept_id = 162724, o.value_text, NULL)) AS facility_name, ',
      'GROUP_CONCAT(IF(o.concept_id = 168146, (CASE o.value_coded WHEN 167061 THEN ''Psychiatric service'' WHEN 163312 THEN ''Psychotherapy service'' ELSE NULL END), NULL) SEPARATOR '' | '') AS services_referred_for, ',
      'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''5fe533ee-0c40-4a1f-a071-dc4d0fbb0c17'') ',
      'INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (167006,167007,167068,167069,167070,167071,167072,167073,167074,165110,165302,166656,166636,162724,168146) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE visit_date = VALUES(visit_date), provider = VALUES(provider), phq_9_rating = VALUES(phq_9_rating), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing depression screening data', CONCAT('Time: ', NOW());
END $$


-- Update Adverse events ---------------------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_adverse_events $$
CREATE PROCEDURE sp_update_etl_adverse_events(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_adverse_events');
SELECT 'Processing adverse events', CONCAT('Time: ', NOW());
SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, form, provider, patient_id, visit_id, visit_date, location_id, encounter_id, obs_id, cause, adverse_event, severity, start_date, action_taken, date_created, date_last_modified, voided) ',
    'SELECT ',
      'e.uuid, ',
      '(CASE f.uuid ',
        'WHEN ''22c68f86-bbf0-49ba-b2d1-23fa7ccf0259'' THEN ''greencard'' ',
        'WHEN ''1bfb09fc-56d7-4108-bd59-b2765fd312b8'' THEN ''prep-initial'' ',
        'WHEN ''ee3e2017-52c0-4a54-99ab-ebb542fb8984'' THEN ''prep-consultation'' ',
        'WHEN ''5ee93f48-960b-11ec-b909-0242ac120002'' THEN ''vmmc-procedure'' ',
        'WHEN ''08873f91-7161-4f90-931d-65b131f2b12b'' THEN ''vmmc-followup'' ',
      'END) AS form, ',
      'e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, o1.obs_id, ',
      'MAX(IF(o1.obs_group = 121760 AND o1.concept_id = 1193, o1.value_coded, NULL)) AS cause, ',
      'MAX(IF(o1.obs_group = 121760 AND o1.concept_id IN (159935,162875), o1.value_coded, NULL)) AS adverse_event, ',
      'MAX(IF(o1.obs_group = 121760 AND o1.concept_id = 162760, o1.value_coded, NULL)) AS severity, ',
      'MAX(IF(o1.obs_group = 121760 AND o1.concept_id = 160753, DATE(o1.value_datetime), NULL)) AS start_date, ',
      'MAX(IF(o1.obs_group = 121760 AND o1.concept_id = 1255, o1.value_coded, NULL)) AS action_taken, ',
      'e.date_created AS date_created, IF(MAX(o1.date_created) > MIN(e.date_created), MAX(o1.date_created), NULL) AS date_last_modified, e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.retired = 0 ',
      'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid IN (''a0034eee-1940-4e35-847f-97537a35d05e'',''c4a2be28-6673-4c36-b886-ea89b0a42116'',''706a8b12-c4ce-40e4-aec3-258b989bf6d3'',''35c6fcc2-960b-11ec-b909-0242ac120002'',''2504e865-638e-4a63-bf08-7e8f03a376f3'')) et ON et.encounter_type_id = e.encounter_type ',
      'INNER JOIN (SELECT o.person_id, o1.encounter_id, o.obs_id, o.concept_id AS obs_group, o1.concept_id AS concept_id, o1.value_coded, o1.value_datetime, o1.date_created AS date_created, o1.date_voided AS date_voided, o1.voided FROM obs o JOIN obs o1 ON o.obs_id = o1.obs_group_id AND o1.concept_id IN (1193,159935,162875,162760,160753,1255) AND o.concept_id = 121760) o1 ON o1.encounter_id = e.encounter_id AND o1.voided = 0 ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o1.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o1.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY o1.obs_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date), provider=VALUES(provider), cause=VALUES(cause), adverse_event=VALUES(adverse_event), severity=VALUES(severity), start_date=VALUES(start_date), action_taken=VALUES(action_taken), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing adverse events data', CONCAT('Time: ', NOW());
END $$




-- --------------------------------------
-- PROCEDURE: sp_update_etl_pre_hiv_enrollment_art
-- Purpose: populate tenant-aware etl_pre_hiv_enrollment_art
-- Tenant-aware: uses sp_set_tenant_session_vars() and dynamic INSERT target
-- --------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_pre_hiv_enrollment_art $$
CREATE PROCEDURE sp_update_etl_pre_hiv_enrollment_art(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_pre_hiv_enrollment_art');

SELECT 'Processing pre_hiv enrollment ART', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, obs_id, ',
      'PMTCT, PMTCT_regimen, PEP, PEP_regimen, PrEP, PrEP_regimen, HAART, HAART_regimen, ',
      'date_created, date_last_modified, voided',
    ') ',
    'SELECT ',
      'e.uuid, e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, o1.obs_id, ',
      'MAX(IF(o1.obs_group = 160741 AND o1.concept_id = 1148, o1.value_coded, NULL)) AS PMTCT, ',
      'MAX(IF(o1.obs_group = 160741 AND o1.concept_id = 966, o1.value_coded, NULL)) AS PMTCT_regimen, ',
      'MAX(IF(o1.obs_group = 160741 AND o1.concept_id = 1691, o1.value_coded, NULL)) AS PEP, ',
      'MAX(IF(o1.obs_group = 160741 AND o1.concept_id = 1088, o1.value_coded, NULL)) AS PEP_regimen, ',
      'MAX(IF(o1.obs_group = 160741 AND o1.concept_id = 165269, o1.value_coded, NULL)) AS PrEP, ',
      'MAX(IF(o1.obs_group = 160741 AND o1.concept_id = 1087, o1.value_coded, NULL)) AS PrEP_regimen, ',
      'MAX(IF(o1.obs_group = 1085 AND o1.concept_id = 1181, o1.value_coded, NULL)) AS HAART, ',
      'MAX(IF(o1.obs_group = 1085 AND o1.concept_id = 1088, o1.value_coded, NULL)) AS HAART_regimen, ',
      'e.date_created AS date_created, IF(MAX(o1.date_created) > MIN(e.date_created), MAX(o1.date_created), NULL) AS date_last_modified, e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid = ''de78a6be-bfc5-4634-adc3-5f1a280455cc'') et ON et.encounter_type_id = e.encounter_type ',
      'INNER JOIN (',
        'SELECT o.person_id, o1.encounter_id, o.obs_id, o.concept_id AS obs_group, o1.concept_id AS concept_id, o1.value_coded, o1.value_datetime, o1.date_created, o1.date_voided, o1.voided ',
        'FROM obs o JOIN obs o1 ON o.obs_id = o1.obs_group_id ',
        'AND o1.concept_id IN (1148,966,1691,1088,1087,1181,165269) AND o1.voided = 0 ',
        'AND o.concept_id IN (160741,1085)',
      ') o1 ON o1.encounter_id = e.encounter_id ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o1.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o1.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY o1.obs_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), provider = VALUES(provider), PMTCT = VALUES(PMTCT), PMTCT_regimen = VALUES(PMTCT_regimen), ',
      'PEP = VALUES(PEP), PEP_regimen = VALUES(PEP_regimen), PrEP = VALUES(PrEP), PrEP_regimen = VALUES(PrEP_regimen), ',
      'HAART = VALUES(HAART), HAART_regimen = VALUES(HAART_regimen), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing pre_hiv enrollment ART data', CONCAT('Time: ', NOW());
END $$





-- --------------------------------------
-- PROCEDURE: sp_update_etl_covid_19_assessment
-- Purpose: update tenant-aware etl_covid19_assessment
-- Notes: calls sp_set_tenant_session_vars() to resolve etl_schema then
--        builds and executes a dynamic INSERT ... ON DUPLICATE KEY UPDATE
-- --------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_covid_19_assessment $$
CREATE PROCEDURE sp_update_etl_covid_19_assessment(IN last_update_time DATETIME)
BEGIN
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT('', etl_schema, '.etl_covid19_assessment');

SELECT 'Processing etl_covid19_assessment', CONCAT('Time: ', NOW());

SET @sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, obs_id, ever_vaccinated, first_vaccine_type, second_vaccine_type, first_dose, second_dose, first_dose_date, second_dose_date, first_vaccination_verified, second_vaccination_verified, final_vaccination_status, ever_received_booster, booster_vaccine_taken, date_taken_booster_vaccine, booster_sequence, booster_dose_verified, ever_tested_covid_19_positive, symptomatic, date_tested_positive, hospital_admission, admission_unit, on_ventillator, on_oxygen_supplement, date_created, date_last_modified, voided) ',
    'SELECT o3.uuid AS uuid, o3.creator AS provider, o3.person_id AS patient_id, o3.visit_id AS visit_id, o3.visit_date AS visit_date, o3.location_id AS location_id, o3.encounter_id AS encounter_id, o1.obs_group AS obs_id, ',
    'MAX(IF(o3.concept_id = 163100, o3.value_coded, NULL)) AS ever_vaccinated, ',
    'MAX(IF(dose = 1 AND o1.concept_id = 984 AND o1.obs_group = 1421, vaccine_type, \"\")) AS first_vaccine_type, ',
    'MAX(IF(dose = 2 AND o1.concept_id = 984 AND o1.obs_group = 1421, vaccine_type, \"\")) AS second_vaccine_type, ',
    'MAX(IF(dose = 1 AND o1.concept_id = 1418 AND o1.obs_group = 1421, dose, \"\")) AS first_dose, ',
    'MAX(IF(dose = 2 AND o1.concept_id = 1418 AND o1.obs_group = 1421, dose, \"\")) AS second_dose, ',
    'MAX(IF(y.dose = 1 AND o1.concept_id = 1410 AND y.obs_group = 1421, DATE(y.date_given), \"\")) AS first_dose_date, ',
    'MAX(IF(y.dose = 2 AND o1.concept_id = 1410 AND y.obs_group = 1421, DATE(y.date_given), \"\")) AS second_dose_date, ',
    'MAX(IF(dose = 1 AND o1.concept_id = 164464 AND o1.obs_group = 1421, verified, \"\")) AS first_vaccination_verified, ',
    'MAX(IF(dose = 2 AND o1.concept_id = 164464 AND o1.obs_group = 1421, verified, \"\")) AS second_vaccination_verified, ',
    'MAX(IF(o3.concept_id = 164134, o3.value_coded, NULL)) AS final_vaccination_status, ',
    'MAX(IF(o3.concept_id = 166063, o3.value_coded, NULL)) AS ever_received_booster, ',
    'MAX(IF(o1.concept_id = 984 AND o1.obs_group = 1184, o1.value_coded, \"\")) AS booster_vaccine_taken, ',
    'MAX(IF(o1.concept_id = 1410 AND o1.obs_group = 1184, DATE(o1.value_datetime), NULL)) AS date_taken_booster_vaccine, ',
    'MAX(IF(o1.concept_id = 1418 AND o1.obs_group = 1184, o1.value_numeric, \"\")) AS booster_sequence, ',
    'MAX(IF(o1.concept_id = 164464 AND o1.obs_group = 1184, o1.value_coded, \"\")) AS booster_dose_verified, ',
    'MAX(IF(o3.concept_id = 166638, o3.value_coded, NULL)) AS ever_tested_covid_19_positive, ',
    'MAX(IF(o3.concept_id = 159640, o3.value_coded, NULL)) AS symptomatic, ',
    'MAX(IF(o3.concept_id = 159948, DATE(o3.value_datetime), NULL)) AS date_tested_positive, ',
    'MAX(IF(o3.concept_id = 162477, o3.value_coded, NULL)) AS hospital_admission, ',
    'CONCAT_WS('','', MAX(IF(o3.concept_id = 161010 AND o3.value_coded = 165994, ''Isolation'', NULL)), MAX(IF(o3.concept_id = 161010 AND o3.value_coded = 165995, ''HDU'', NULL)), MAX(IF(o3.concept_id = 161010 AND o3.value_coded = 161936, ''ICU'', NULL))) AS admission_unit, ',
    'MAX(IF(o3.concept_id = 165932, o3.value_coded, NULL)) AS on_ventillator, ',
    'MAX(IF(o3.concept_id = 165864, o3.value_coded, NULL)) AS on_oxygen_supplement, ',
    'o3.date_created AS date_created, o3.date_last_modified AS date_last_modified, COALESCE(o3.enc_voided, o3.voided) AS voided ',
    'FROM (',
      'SELECT e.uuid, e.creator, o.person_id, o.encounter_id, DATE(e.encounter_datetime) AS visit_date, e.visit_id, e.location_id, o.obs_id, o.concept_id AS obs_group, o.concept_id AS concept_id, o.value_coded, o.value_datetime, o.value_numeric, o.date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, o.voided, o.date_voided, e.date_voided AS enc_voided, e.date_voided AS enc_voided_date, e.date_changed AS enc_changed_date, e.date_created AS enc_created_date ',
      'FROM obs o INNER JOIN encounter e ON e.encounter_id = o.encounter_id INNER JOIN person p ON p.person_id = o.person_id AND p.voided = 0 ',
      'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid = ''86709cfc-1490-11ec-82a8-0242ac130003'') et ON et.encounter_type_id = e.encounter_type ',
      'WHERE o.concept_id IN (163100, 984, 1418, 1410, 164464, 164134, 166063, 166638, 159948, 162477, 161010, 165864, 165932, 159640) AND o.voided = 0 ',
      'GROUP BY o.obs_id',
    ') o3 ',
    'LEFT JOIN (',
      'SELECT person_id AS patient_id, DATE(encounter_datetime) AS visit_date, creator, obs_id, DATE(t.date_created) AS date_created, t.date_last_modified AS date_last_modified, encounter_id, name AS encounter_type, t.uuid, ',
      'MAX(IF(t.concept_id = 984, t.value_coded, \"\")) AS vaccine_type, MAX(IF(t.concept_id = 1418, value_numeric, \"\")) AS dose, MAX(IF(t.concept_id = 164464, value_coded, \"\")) AS verified, MAX(IF(t.concept_id = 1410, date_given, \"\")) AS date_given, t.concept_id AS concept_id, t.obs_group AS obs_group, obs_group_id, t.visit_id, t.location_id, t.voided ',
      'FROM (',
        'SELECT e.uuid, o2.person_id, o2.obs_id, o.concept_id AS obs_group, e.encounter_datetime, e.creator, e.date_created, IF(MAX(o2.date_created) != MIN(o2.date_created), MAX(o2.date_created), NULL) AS date_last_modified, o2.voided AS voided, o2.concept_id, o2.value_coded, o2.value_numeric, DATE(o2.value_datetime) date_given, o2.obs_group_id, o2.encounter_id, et.name, e.visit_id, e.location_id ',
        'FROM obs o INNER JOIN encounter e ON e.encounter_id = o.encounter_id INNER JOIN person p ON p.person_id = o.person_id AND p.voided = 0 ',
        'INNER JOIN (SELECT encounter_type_id, uuid, name FROM encounter_type WHERE uuid = ''86709cfc-1490-11ec-82a8-0242ac130003'') et ON et.encounter_type_id = e.encounter_type ',
        'INNER JOIN obs o2 ON o.obs_id = o2.obs_group_id WHERE o2.concept_id IN (984, 1418, 1410, 164464) AND o2.voided = 0 GROUP BY o2.obs_id',
      ') t GROUP BY obs_group_id HAVING vaccine_type != \"\"',
    ') y ON o3.encounter_id = y.encounter_id ',
    'LEFT JOIN (',
      'SELECT o.person_id, o1.encounter_id, o.obs_id, o.concept_id AS obs_group, o1.concept_id AS concept_id, o1.value_coded, o1.value_datetime, o1.value_numeric, o1.date_created, o1.voided ',
      'FROM obs o JOIN obs o1 ON o.obs_id = o1.obs_group_id INNER JOIN person p ON p.person_id = o.person_id AND p.voided = 0 ',
      'WHERE o1.concept_id IN (163100, 984, 1418, 1410, 164464, 164134, 166063, 166638, 159948, 162477, 161010, 165864, 165932) AND o1.voided = 0 AND o.concept_id IN (1421, 1184) ORDER BY o1.obs_id',
    ') o1 ON o1.encounter_id = y.encounter_id ',
    'WHERE ( (o3.enc_created_date >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o3.enc_changed_date >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o3.enc_voided_date >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o3.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o3.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ) ',
    'GROUP BY o3.visit_id ',
    'ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date), provider=VALUES(provider), ever_vaccinated=VALUES(ever_vaccinated), first_vaccine_type=VALUES(first_vaccine_type), second_vaccine_type=VALUES(second_vaccine_type), first_dose=VALUES(first_dose), second_dose=VALUES(second_dose), first_dose_date=VALUES(first_dose_date), second_dose_date=VALUES(second_dose_date), first_vaccination_verified=VALUES(first_vaccination_verified), second_vaccination_verified=VALUES(second_vaccination_verified), final_vaccination_status=VALUES(final_vaccination_status), ever_received_booster=VALUES(ever_received_booster), booster_vaccine_taken=VALUES(booster_vaccine_taken), date_taken_booster_vaccine=VALUES(date_taken_booster_vaccine), booster_sequence=VALUES(booster_sequence), booster_dose_verified=VALUES(booster_dose_verified), ever_tested_covid_19_positive=VALUES(ever_tested_covid_19_positive), symptomatic=VALUES(symptomatic), date_tested_positive=VALUES(date_tested_positive), hospital_admission=VALUES(hospital_admission), admission_unit=VALUES(admission_unit), on_ventillator=VALUES(on_ventillator), on_oxygen_supplement=VALUES(on_oxygen_supplement), date_created=VALUES(date_created), date_last_modified=VALUES(date_last_modified), voided=VALUES(voided);'
  );
PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing etl_covid19_assessment', CONCAT('Time: ', NOW());
END $$


-- sql
-- --------------------------------------
-- PROCEDURE: sp_update_etl_vmmc_enrolment
-- Purpose: update tenant-aware etl_vmmc_enrolment
-- Tenant-aware: calls sp_set_tenant_session_vars() and uses dynamic INSERT target
-- --------------------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_vmmc_enrolment $$
CREATE PROCEDURE sp_update_etl_vmmc_enrolment(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

  -- resolve tenant schema
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_vmmc_enrolment');

SELECT 'Processing VMMC enrolment', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, referee, other_referee, source_of_vmmc_info, other_source_of_vmmc_info, county_of_origin, date_created, date_last_modified, voided) ',
    'SELECT e.uuid, e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
    'MAX(IF(o.concept_id = 160482, o.value_coded, NULL)) AS referee, ',
    'MAX(IF(o.concept_id = 165143, o.value_text, NULL)) AS other_referee, ',
    'MAX(IF(o.concept_id = 167094, o.value_coded, NULL)) AS source_of_vmmc_info, ',
    'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS other_source_of_vmmc_info, ',
    'MAX(IF(o.concept_id = 167131, o.value_text, NULL)) AS county_of_origin, ',
    'e.date_created AS date_created, ',
    'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
    'e.voided AS voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''a74e3e4a-9e2a-41fb-8e64-4ba8a71ff984'') ',
    'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (160482,165143,167094,160632,167131) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.encounter_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date), provider=VALUES(provider), referee=VALUES(referee), other_referee=VALUES(other_referee), source_of_vmmc_info=VALUES(source_of_vmmc_info), other_source_of_vmmc_info=VALUES(other_source_of_vmmc_info), county_of_origin=VALUES(county_of_origin), date_created=VALUES(date_created), date_last_modified=VALUES(date_last_modified), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing VMMC enrolment data for ', @etl_schema, '.etl_vmmc_enrolment Time: ', NOW()) AS status;
END $$


-- sql

-- --------------------------------------
-- PROCEDURE: sp_update_etl_vmmc_circumcision_procedure
-- Purpose: update tenant-aware etl_vmmc_circumcision_procedure
-- Notes: calls sp_set_tenant_session_vars() then builds and executes dynamic INSERT ... ON DUPLICATE KEY UPDATE
-- --------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_vmmc_circumcision_procedure $$
CREATE PROCEDURE sp_update_etl_vmmc_circumcision_procedure(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_vmmc_circumcision_procedure');

SELECT CONCAT('Processing VMMC circumcision procedure for ', @etl_schema, '.etl_vmmc_circumcision_procedure Time: ', NOW()) AS status;

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, circumcision_method, surgical_circumcision_method, reason_circumcision_ineligible, circumcision_device, specific_other_device, device_size, lot_number, anaesthesia_type, anaesthesia_used, anaesthesia_concentration, anaesthesia_volume, time_of_first_placement_cut, time_of_last_device_closure, has_adverse_event, adverse_event, severity, adverse_event_management, clinician_name, clinician_cadre, assist_clinician_name, assist_clinician_cadre, theatre_number, date_created, date_last_modified, voided) ',
    'SELECT e.uuid, e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
    'MAX(IF(o.concept_id = 167118, o.value_coded, NULL)) AS circumcision_method, ',
    'MAX(IF(o.concept_id = 167119, o.value_coded, NULL)) AS surgical_circumcision_method, ',
    'MAX(IF(o.concept_id = 163042, o.value_text, NULL)) AS reason_circumcision_ineligible, ',
    'MAX(IF(o.concept_id = 167120, o.value_coded, NULL)) AS circumcision_device, ',
    'MAX(IF(o.concept_id = 163042, o.value_text, NULL)) AS specific_other_device, ',
    'MAX(IF(o.concept_id = 163049, o.value_text, NULL)) AS device_size, ',
    'MAX(IF(o.concept_id = 164964, o.value_text, NULL)) AS lot_number, ',
    'MAX(IF(o.concept_id = 164254, o.value_coded, NULL)) AS anaesthesia_type, ',
    'MAX(IF(o.concept_id = 165139, o.value_coded, NULL)) AS anaesthesia_used, ',
    'MAX(IF(o.concept_id = 1444, o.value_text, NULL)) AS anaesthesia_concentration, ',
    'MAX(IF(o.concept_id = 166650, o.value_numeric, NULL)) AS anaesthesia_volume, ',
    'MAX(IF(o.concept_id = 160715, o.value_datetime, NULL)) AS time_of_first_placement_cut, ',
    'MAX(IF(o.concept_id = 167132, o.value_datetime, NULL)) AS time_of_last_device_closure, ',
    'MAX(IF(o.concept_id = 162871, o.value_coded, NULL)) AS has_adverse_event, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 162875 AND o.value_coded = 147241, ''Bleeding'', NULL)), ',
      'MAX(IF(o.concept_id = 162875 AND o.value_coded = 135693, ''Anaesthetic Reaction'', NULL)), ',
      'MAX(IF(o.concept_id = 162875 AND o.value_coded = 167126, ''Excessive skin removed'', NULL)), ',
      'MAX(IF(o.concept_id = 162875 AND o.value_coded = 156911, ''Damage to the penis'', NULL)), ',
      'MAX(IF(o.concept_id = 162875 AND o.value_coded = 114403, ''Pain'', NULL))',
    ') AS adverse_event, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 162760 AND o.value_coded = 1500, ''Severe'', NULL)), ',
      'MAX(IF(o.concept_id = 162760 AND o.value_coded = 1499, ''Moderate'', NULL)), ',
      'MAX(IF(o.concept_id = 162760 AND o.value_coded = 1498, ''Mild'', NULL))',
    ') AS severity, ',
    'MAX(IF(o.concept_id = 162749, o.value_text, NULL)) AS adverse_event_management, ',
    'MAX(IF(o.concept_id = 1473, o.value_text, NULL)) AS clinician_name, ',
    'MAX(IF(o.concept_id = 163556, o.value_coded, NULL)) AS clinician_cadre, ',
    'MAX(IF(o.concept_id = 164141, o.value_text, NULL)) AS assist_clinician_name, ',
    'MAX(IF(o.concept_id = 166014, o.value_coded, NULL)) AS assist_clinician_cadre, ',
    'MAX(IF(o.concept_id = 167133, o.value_text, NULL)) AS theatre_number, ',
    'e.date_created AS date_created, ',
    'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
    'e.voided AS voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''5ee93f48-960b-11ec-b909-0242ac120002'') ',
    'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (167118,167119,163042,167120,163049,164964,164254,1444,166650,160715,167132,162871,162875,162760,162749,1473,163556,164141,166014,167133,165139) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
    'visit_date = VALUES(visit_date), provider = VALUES(provider), circumcision_method = VALUES(circumcision_method), surgical_circumcision_method = VALUES(surgical_circumcision_method), reason_circumcision_ineligible = VALUES(reason_circumcision_ineligible), circumcision_device = VALUES(circumcision_device), specific_other_device = VALUES(specific_other_device), device_size = VALUES(device_size), lot_number = VALUES(lot_number), anaesthesia_type = VALUES(anaesthesia_type), anaesthesia_used = VALUES(anaesthesia_used), anaesthesia_concentration = VALUES(anaesthesia_concentration), anaesthesia_volume = VALUES(anaesthesia_volume), time_of_first_placement_cut = VALUES(time_of_first_placement_cut), time_of_last_device_closure = VALUES(time_of_last_device_closure), has_adverse_event = VALUES(has_adverse_event), adverse_event = VALUES(adverse_event), severity = VALUES(severity), adverse_event_management = VALUES(adverse_event_management), clinician_name = VALUES(clinician_name), clinician_cadre = VALUES(clinician_cadre), assist_clinician_name = VALUES(assist_clinician_name), assist_clinician_cadre = VALUES(assist_clinician_cadre), theatre_number = VALUES(theatre_number), date_last_modified = VALUES(date_last_modified), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing VMMC circumcision procedure data for ', @etl_schema, '.etl_vmmc_circumcision_procedure Time: ', NOW()) AS status;
END $$



-- sql
-- --------------------------------------
-- PROCEDURE: sp_update_etl_vmmc_medical_history
-- Purpose: update tenant-aware etl_vmmc_medical_history
-- Tenant-aware: calls sp_set_tenant_session_vars() and uses dynamic INSERT target
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- --------------------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_vmmc_medical_history $$
CREATE PROCEDURE sp_update_etl_vmmc_medical_history(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_vmmc_medical_history');

SELECT 'Processing VMMC medical history examination', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
    'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, assent_given, consent_given, hiv_status, hiv_test_date, art_start_date, current_regimen, ccc_number, next_appointment_date, hiv_care_facility, hiv_care_facility_name, vl, cd4_count, bleeding_disorder, diabetes, client_presenting_complaints, other_complaints, ongoing_treatment, other_ongoing_treatment, hb_level, sugar_level, has_known_allergies, ever_had_surgical_operation, specific_surgical_operation, proven_tetanus_booster, ever_received_tetanus_booster, date_received_tetanus_booster, blood_pressure, pulse_rate, temperature, in_good_health, counselled, reason_ineligible, circumcision_method_chosen, conventional_method_chosen, device_name, device_size, other_conventional_method_device_chosen, services_referral, date_created, date_last_modified, voided',
    ') ',
    'SELECT ',
    'e.uuid, e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
    'MAX(IF(o.concept_id = 167093, o.value_coded, NULL)) AS assent_given, ',
    'MAX(IF(o.concept_id = 1710, o.value_coded, NULL)) AS consent_given, ',
    'MAX(IF(o.concept_id = 159427, o.value_coded, NULL)) AS hiv_status, ',
    'MAX(IF(o.concept_id = 160554, o.value_datetime, NULL)) AS hiv_test_date, ',
    'MAX(IF(o.concept_id = 159599, o.value_datetime, NULL)) AS art_start_date, ',
    'MAX(IF(o.concept_id = 164855, o.value_coded, NULL)) AS current_regimen, ',
    'MAX(IF(o.concept_id = 162053, o.value_numeric, NULL)) AS ccc_number, ',
    'MAX(IF(o.concept_id = 5096, o.value_datetime, NULL)) AS next_appointment_date, ',
    'MAX(IF(o.concept_id = 165239, o.value_coded, NULL)) AS hiv_care_facility, ',
    'MAX(IF(o.concept_id = 161550, o.value_text, NULL)) AS hiv_care_facility_name, ',
    'MAX(IF(o.concept_id = 856, o.value_numeric, NULL)) AS vl, ',
    'MAX(IF(o.concept_id = 1305, ''LDL'', NULL)) AS vl_ldl_flag, ',
    'MAX(IF(o.concept_id = 5497, o.value_numeric, NULL)) AS cd4_count, ',
    'MAX(IF(o.concept_id = 165241 AND o.value_coded = 147241, o.value_coded, NULL)) AS bleeding_disorder, ',
    'MAX(IF(o.concept_id = 165241 AND o.value_coded = 119481, o.value_coded, NULL)) AS diabetes, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 1728 AND o.value_coded = 123529, ''Urethral Discharge'', NULL)), ',
      'MAX(IF(o.concept_id = 1728 AND o.value_coded = 118990, ''Genital Sore'', NULL)), ',
      'MAX(IF(o.concept_id = 1728 AND o.value_coded = 163606, ''Pain on Urination'', NULL)), ',
      'MAX(IF(o.concept_id = 1728 AND o.value_coded = 125203, ''Swelling of the scrotum'', NULL)), ',
      'MAX(IF(o.concept_id = 1728 AND o.value_coded = 163831, ''Difficulty in retracting foreskin'', NULL)), ',
      'MAX(IF(o.concept_id = 1728 AND o.value_coded = 130845, ''Difficulty in returning foreskin to normal'', NULL)), ',
      'MAX(IF(o.concept_id = 1728 AND o.value_coded = 116123, ''Concerns about erection/sexual function'', NULL)), ',
      'MAX(IF(o.concept_id = 1728 AND o.value_coded = 163813, ''Epispadia'', NULL)), ',
      'MAX(IF(o.concept_id = 1728 AND o.value_coded = 138010, ''Hypospadia'', NULL)), ',
      'MAX(IF(o.concept_id = 1728 AND o.value_coded = 5622, ''Other'', NULL))',
    ') AS client_presenting_complaints, ',
    'MAX(IF(o.concept_id = 163047, o.value_text, NULL)) AS other_complaints, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 1794 AND o.value_coded = 121629, ''Anaemia'', NULL)), ',
      'MAX(IF(o.concept_id = 1794 AND o.value_coded = 142484, ''Diabetes'', NULL)), ',
      'MAX(IF(o.concept_id = 1794 AND o.value_coded = 138571, ''HIV/AIDS'', NULL)), ',
      'MAX(IF(o.concept_id = 1794 AND o.value_coded = 5622, ''Other'', NULL))',
    ') AS ongoing_treatment, ',
    'MAX(IF(o.concept_id = 163104, o.value_text, NULL)) AS other_ongoing_treatment, ',
    'MAX(IF(o.concept_id = 21, o.value_numeric, NULL)) AS hb_level, ',
    'MAX(IF(o.concept_id = 887, o.value_numeric, NULL)) AS sugar_level, ',
    'MAX(IF(o.concept_id = 160557, o.value_coded, NULL)) AS has_known_allergies, ',
    'MAX(IF(o.concept_id = 164896, o.value_coded, NULL)) AS ever_had_surgical_operation, ',
    'MAX(IF(o.concept_id = 163393, o.value_text, NULL)) AS specific_surgical_operation, ',
    'MAX(IF(o.concept_id = 54, o.value_coded, NULL)) AS proven_tetanus_booster, ',
    'MAX(IF(o.concept_id = 161536, o.value_coded, NULL)) AS ever_received_tetanus_booster, ',
    'MAX(IF(o.concept_id = 1410, o.value_datetime, NULL)) AS date_received_tetanus_booster, ',
    'CONCAT_WS(''/'', MAX(IF(o.concept_id = 5085, o.value_numeric, NULL)), MAX(IF(o.concept_id = 5086, o.value_numeric, NULL))) AS blood_pressure, ',
    'MAX(IF(o.concept_id = 5242, o.value_numeric, NULL)) AS pulse_rate, ',
    'MAX(IF(o.concept_id = 5088, o.value_numeric, NULL)) AS temperature, ',
    'MAX(IF(o.concept_id = 1855, o.value_coded, NULL)) AS in_good_health, ',
    'MAX(IF(o.concept_id = 165070, o.value_coded, NULL)) AS counselled, ',
    'MAX(IF(o.concept_id = 162169, o.value_text, NULL)) AS reason_ineligible, ',
    'MAX(IF(o.concept_id = 167118, o.value_coded, NULL)) AS circumcision_method_chosen, ',
    'MAX(IF(o.concept_id = 167119, o.value_coded, NULL)) AS conventional_method_chosen, ',
    'MAX(IF(o.concept_id = 167120, o.value_coded, NULL)) AS device_name, ',
    'MAX(IF(o.concept_id = 163049, o.value_text, NULL)) AS device_size, ',
    'MAX(IF(o.concept_id = 163042, o.value_text, NULL)) AS other_conventional_method_device_chosen, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 1272 AND o.value_coded = 167125, ''STI Treatment'', NULL)), ',
      'MAX(IF(o.concept_id = 1272 AND o.value_coded = 166536, ''PrEP Services'', NULL)), ',
      'MAX(IF(o.concept_id = 1272 AND o.value_coded = 190, ''Condom dispensing'', NULL))',
    ') AS services_referral, ',
    'e.date_created AS date_created, ',
    'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
    'e.voided AS voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''d42aeb3d-d5d2-4338-a154-f75ddac78b59'') ',
    'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (167093,1710,159427,160554,164855,159599,162053,5096,165239,161550,856,1305,5497,165241,1728,163047,1794,163104,21,887,160557,164896,163393,54,161536,1410,5085,5086,5242,5088,1855,165070,162169,167118,167119,167120,163049,163042,1272) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (',
      IFNULL(CONCAT('e.date_created >= ''', last_update_time, ''''), 'NULL'),
      ' OR ',
      IFNULL(CONCAT('e.date_changed >= ''', last_update_time, ''''), 'NULL'),
      ' OR ',
      IFNULL(CONCAT('e.date_voided >= ''', last_update_time, ''''), 'NULL'),
      ' OR ',
      IFNULL(CONCAT('o.date_created >= ''', last_update_time, ''''), 'NULL'),
      ' OR ',
      IFNULL(CONCAT('o.date_voided >= ''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
    'visit_date = VALUES(visit_date), ',
    'provider = VALUES(provider), ',
    'assent_given = VALUES(assent_given), ',
    'consent_given = VALUES(consent_given), ',
    'hiv_status = VALUES(hiv_status), ',
    'hiv_test_date = VALUES(hiv_test_date), ',
    'art_start_date = VALUES(art_start_date), ',
    'current_regimen = VALUES(current_regimen), ',
    'ccc_number = VALUES(ccc_number), ',
    'next_appointment_date = VALUES(next_appointment_date), ',
    'hiv_care_facility = VALUES(hiv_care_facility), ',
    'hiv_care_facility_name = VALUES(hiv_care_facility_name), ',
    'vl = VALUES(vl), ',
    'cd4_count = VALUES(cd4_count), ',
    'bleeding_disorder = VALUES(bleeding_disorder), ',
    'diabetes = VALUES(diabetes), ',
    'client_presenting_complaints = VALUES(client_presenting_complaints), ',
    'other_complaints = VALUES(other_complaints), ',
    'ongoing_treatment = VALUES(ongoing_treatment), ',
    'other_ongoing_treatment = VALUES(other_ongoing_treatment), ',
    'hb_level = VALUES(hb_level), ',
    'sugar_level = VALUES(sugar_level), ',
    'has_known_allergies = VALUES(has_known_allergies), ',
    'ever_had_surgical_operation = VALUES(ever_had_surgical_operation), ',
    'specific_surgical_operation = VALUES(specific_surgical_operation), ',
    'proven_tetanus_booster = VALUES(proven_tetanus_booster), ',
    'ever_received_tetanus_booster = VALUES(ever_received_tetanus_booster), ',
    'date_received_tetanus_booster = VALUES(date_received_tetanus_booster), ',
    'blood_pressure = VALUES(blood_pressure), ',
    'pulse_rate = VALUES(pulse_rate), ',
    'temperature = VALUES(temperature), ',
    'in_good_health = VALUES(in_good_health), ',
    'counselled = VALUES(counselled), ',
    'reason_ineligible = VALUES(reason_ineligible), ',
    'circumcision_method_chosen = VALUES(circumcision_method_chosen), ',
    'conventional_method_chosen = VALUES(conventional_method_chosen), ',
    'device_name = VALUES(device_name), ',
    'device_size = VALUES(device_size), ',
    'other_conventional_method_device_chosen = VALUES(other_conventional_method_device_chosen), ',
    'services_referral = VALUES(services_referral), ',
    'date_created = VALUES(date_created), ',
    'date_last_modified = VALUES(date_last_modified), ',
    'voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing VMMC medical history examination data for ', @etl_schema, '.etl_vmmc_medical_history Time: ', NOW()) AS status;
END $$




-- --------------------------------------
-- PROCEDURE: sp_update_etl_vmmc_client_followup
-- Purpose: populate tenant-aware etl_vmmc_client_followup
-- Tenant-aware: calls sp_set_tenant_session_vars() and uses dynamic INSERT target
-- --------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_vmmc_client_followup $$
CREATE PROCEDURE sp_update_etl_vmmc_client_followup(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_vmmc_client_followup');

SELECT 'Processing VMMC client followup', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, visit_type, days_since_circumcision, has_adverse_event, adverse_event, severity, adverse_event_management, medications_given, other_medications_given, clinician_name, clinician_cadre, clinician_notes, date_created, date_last_modified, voided) ',
    'SELECT e.uuid, e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
    'MAX(IF(o.concept_id = 164181, o.value_coded, NULL)) AS visit_type, ',
    'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS days_since_circumcision, ',
    'MAX(IF(o.concept_id = 162871, o.value_coded, NULL)) AS has_adverse_event, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 162875 AND o.value_coded = 114403, ''Pain'', NULL)), ',
      'MAX(IF(o.concept_id = 162875 AND o.value_coded = 147241, ''Bleeding'', NULL)), ',
      'MAX(IF(o.concept_id = 162875 AND o.value_coded = 152045, ''Problems with appearance'', NULL)), ',
      'MAX(IF(o.concept_id = 162875 AND o.value_coded = 156567, ''Hematoma'', NULL)), ',
      'MAX(IF(o.concept_id = 162875 AND o.value_coded = 139510, ''Infection'', NULL)), ',
      'MAX(IF(o.concept_id = 162875 AND o.value_coded = 118771, ''Difficulty urinating'', NULL)), ',
      'MAX(IF(o.concept_id = 162875 AND o.value_coded = 163799, ''Wound disruption'', NULL))',
    ') AS adverse_event, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 162760 AND o.value_coded = 1500, ''Severe'', NULL)), ',
      'MAX(IF(o.concept_id = 162760 AND o.value_coded = 1499, ''Moderate'', NULL)), ',
      'MAX(IF(o.concept_id = 162760 AND o.value_coded = 1498, ''Mild'', NULL))',
    ') AS severity, ',
    'MAX(IF(o.concept_id = 162749, o.value_text, NULL)) AS adverse_event_management, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 159369 AND o.value_coded = 1107, ''None'', NULL)), ',
      'MAX(IF(o.concept_id = 159369 AND o.value_coded = 103294, ''Analgesic'', NULL)), ',
      'MAX(IF(o.concept_id = 159369 AND o.value_coded = 1195, ''Antibiotics'', NULL)), ',
      'MAX(IF(o.concept_id = 159369 AND o.value_coded = 84879, ''TTCV'', NULL)), ',
      'MAX(IF(o.concept_id = 159369 AND o.value_coded = 5622, ''Other'', NULL))',
    ') AS medications_given, ',
    'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS other_medications_given, ',
    'MAX(IF(o.concept_id = 1473, o.value_text, NULL)) AS clinician_name, ',
    'MAX(IF(o.concept_id = 1542, o.value_coded, NULL)) AS clinician_cadre, ',
    'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS clinician_notes, ',
    'e.date_created AS date_created, ',
    'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
    'e.voided AS voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''08873f91-7161-4f90-931d-65b131f2b12b'') ',
    'INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (164181,162871,162875,162760,162749,159369,161011,1473,1542,160632) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (',
      IFNULL(CONCAT('e.date_created >= ''', last_update_time, ''''), 'NULL'),
    ' OR ',
      IFNULL(CONCAT('e.date_changed >= ''', last_update_time, ''''), 'NULL'),
    ' OR ',
      IFNULL(CONCAT('e.date_voided >= ''', last_update_time, ''''), 'NULL'),
    ' OR ',
      IFNULL(CONCAT('o.date_created >= ''', last_update_time, ''''), 'NULL'),
    ' OR ',
      IFNULL(CONCAT('o.date_voided >= ''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
    'visit_date=VALUES(visit_date), provider=VALUES(provider), visit_type=VALUES(visit_type), days_since_circumcision=VALUES(days_since_circumcision), has_adverse_event=VALUES(has_adverse_event), adverse_event=VALUES(adverse_event), severity=VALUES(severity), adverse_event_management=VALUES(adverse_event_management), medications_given=VALUES(medications_given), other_medications_given=VALUES(other_medications_given), clinician_name=VALUES(clinician_name), clinician_cadre=VALUES(clinician_cadre), clinician_notes=VALUES(clinician_notes), date_created=VALUES(date_created), date_last_modified=VALUES(date_last_modified), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing VMMC client followup data ', @target_table, ' Time: ', NOW()) AS status;
END $$



-- Update VMMC post operation assessment ----



DROP PROCEDURE IF EXISTS sp_update_etl_vmmc_post_operation_assessment $$
CREATE PROCEDURE sp_update_etl_vmmc_post_operation_assessment(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_vmmc_post_operation_assessment');

SELECT "Processing post vmmc operation assessment", CONCAT("Time: ", NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, blood_pressure, pulse_rate, temperature, penis_elevated, given_post_procedure_instruction, post_procedure_instructions, given_post_operation_medication, medication_given, other_medication_given, removal_date, next_appointment_date, discharged_by, cadre, date_created, date_last_modified, voided) ',
    'SELECT e.uuid, e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'CONCAT_WS(\'/\', MAX(IF(o.concept_id = 5085, o.value_numeric, NULL)), MAX(IF(o.concept_id = 5086, o.value_numeric, NULL))) AS blood_pressure, ',
      'MAX(IF(o.concept_id = 5087, o.value_numeric, NULL)) AS pulse_rate, ',
      'MAX(IF(o.concept_id = 5088, o.value_numeric, NULL)) AS temperature, ',
      'MAX(IF(o.concept_id = 162871, o.value_coded, NULL)) AS penis_elevated, ',
      'MAX(IF(o.concept_id = 166639, o.value_coded, NULL)) AS given_post_procedure_instruction, ',
      'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS post_procedure_instructions, ',
      'MAX(IF(o.concept_id = 159369 AND o.value_coded = 1107, o.value_coded, NULL)) AS given_post_operation_medication, ',
        'CONCAT_WS('','', ',
          'MAX(IF(o.concept_id = 159369 AND o.value_coded = 103294, ''Analgesic'', NULL)), ',
          'MAX(IF(o.concept_id = 159369 AND o.value_coded = 1195, ''Antibiotics'', NULL)), ',
          'MAX(IF(o.concept_id = 159369 AND o.value_coded = 84879, ''TTCV'', NULL)), ',
          'MAX(IF(o.concept_id = 159369 AND o.value_coded = 5622, ''Other'', NULL))',
        ') AS medication_given, ',
      'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS other_medication_given, ',
      'MAX(IF(o.concept_id = 160753, o.value_datetime, NULL)) AS removal_date, ',
      'MAX(IF(o.concept_id = 5096, o.value_datetime, NULL)) AS next_appointment_date, ',
      'MAX(IF(o.concept_id = 1473, o.value_text, NULL)) AS discharged_by, ',
      'MAX(IF(o.concept_id = 1542, o.value_coded, NULL)) AS cadre, ',
      'e.date_created AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'e.voided AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''620b3404-9ae5-11ec-b909-0242ac120002'') ',
      'INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (5085,5086,5087,5088,162871,160632,159369,161011,160753,5096,1473,1542) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), provider = VALUES(provider), blood_pressure = VALUES(blood_pressure), pulse_rate = VALUES(pulse_rate), temperature = VALUES(temperature), penis_elevated = VALUES(penis_elevated), given_post_procedure_instruction = VALUES(given_post_procedure_instruction), post_procedure_instructions = VALUES(post_procedure_instructions), given_post_operation_medication = VALUES(given_post_operation_medication), medication_given = VALUES(medication_given), other_medication_given = VALUES(other_medication_given), removal_date = VALUES(removal_date), next_appointment_date = VALUES(next_appointment_date), discharged_by = VALUES(discharged_by), cadre = VALUES(cadre), date_created = VALUES(date_created), date_last_modified = VALUES(date_last_modified), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT "Completed processing VMMC post operation assessment data ", CONCAT("Time: ", NOW());
END $$



-- --------------------------------------
-- PROCEDURE: sp_update_etl_hts_eligibility_screening
-- Purpose: populate tenant-aware etl_hts_eligibility_screening
-- Tenant-aware: calls sp_set_tenant_session_vars() and inserts into dynamic target
-- --------------------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_hts_eligibility_screening $$
CREATE PROCEDURE sp_update_etl_hts_eligibility_screening(IN last_update_time DATETIME)
BEGIN
  DECLARE sql_stmt TEXT;
  DECLARE target_table VARCHAR(300);

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_hts_eligibility_screening');

SELECT 'Processing hts screening', CONCAT('Time: ', NOW());

SET sql_stmt = CONCAT(
'INSERT INTO ', @target_table, ' (
    patient_id, visit_id, encounter_id, uuid, location_id, provider, visit_date,
    population_type, key_population_type, priority_population_type, patient_disabled,
    disability_type, department, patient_type, is_health_worker, recommended_test,
    test_strategy, hts_entry_point, hts_risk_category, hts_risk_score,
    relationship_with_contact, mother_hiv_status, tested_hiv_before, who_performed_test,
    test_results, date_tested, started_on_art, upn_number, child_defiled, ever_had_sex,
    sexually_active, new_partner, partner_hiv_status, couple_discordant, multiple_partners,
    number_partners, alcohol_sex, money_sex, condom_burst, unknown_status_partner,
    known_status_partner, experienced_gbv, type_of_gbv, service_received,
    currently_on_prep, recently_on_pep, recently_had_sti, tb_screened, cough,
    fever, weight_loss, night_sweats, contact_with_tb_case, lethargy, tb_status,
    shared_needle, needle_stick_injuries, traditional_procedures, child_reasons_for_ineligibility,
    pregnant, breastfeeding_mother, eligible_for_test, referred_for_testing, reason_to_test,
    reason_not_to_test, reasons_for_ineligibility, specific_reason_for_ineligibility,
    date_created, date_last_modified, voided
) ',
'SELECT
    e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator,
    DATE(e.encounter_datetime) AS visit_date,
    MAX(IF(o.concept_id=164930,o.value_coded,NULL)) AS population_type,
    MAX(IF(o.concept_id=160581,(CASE o.value_coded WHEN 105 THEN ''People who inject drugs''
                                                   WHEN 160578 THEN ''Men who have sex with men''
                                                   WHEN 160579 THEN ''Female sex worker''
                                                   WHEN 162277 THEN ''People in prison and other closed settings''
                                                   WHEN 5622 THEN ''Other'' ELSE '''' END),NULL)) AS key_population_type,
    MAX(IF(o.concept_id=138643,(CASE o.value_coded WHEN 159674 THEN ''Fisher folk''
                                                   WHEN 162198 THEN ''Truck driver''
                                                   WHEN 160549 THEN ''Adolescent and young girls''
                                                   WHEN 162277 THEN ''Prisoner''
                                                   WHEN 165192 THEN ''Military and other uniformed services'' ELSE '''' END),NULL)) AS priority_population_type,
    MAX(IF(o.concept_id=164951,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS patient_disabled,
    CONCAT_WS('','',
      NULLIF(MAX(IF(o.concept_id=162558 AND o.value_coded = 120291,''Hearing impairment'','''')),''),
      NULLIF(MAX(IF(o.concept_id=162558 AND o.value_coded = 147215,''Visual impairment'','''')),''),
      NULLIF(MAX(IF(o.concept_id=162558 AND o.value_coded = 151342,''Mentally Challenged'','''')),''),
      NULLIF(MAX(IF(o.concept_id=162558 AND o.value_coded = 164538,''Physically Challenged'','''')),''),
      NULLIF(MAX(IF(o.concept_id=162558 AND o.value_coded = 5622,''Other'','''')),''),
      NULLIF(MAX(IF(o.concept_id=160632,o.value_text,'''')),'')) AS disability_type,
    MAX(IF(o.concept_id=159936,o.value_coded,NULL)) AS department,
    MAX(IF(o.concept_id=164932,o.value_coded,NULL)) AS patient_type,
    MAX(IF(o.concept_id=5619,o.value_coded,NULL)) AS is_health_worker,
    MAX(IF(o.concept_id=167229,(CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END),NULL)) AS recommended_test,
    MAX(IF(o.concept_id=164956,o.value_coded,NULL)) AS test_strategy,
    MAX(IF(o.concept_id=160540,o.value_coded,NULL)) AS hts_entry_point,
    MAX(IF(o.concept_id=167163,(CASE o.value_coded WHEN 1407 THEN ''Low'' WHEN 1499 THEN ''Moderate'' WHEN 1408 THEN ''High'' WHEN 167164 THEN ''Very high'' ELSE '''' END),NULL)) AS hts_risk_category,
    MAX(IF(o.concept_id=167162,o.value_numeric,NULL)) AS hts_risk_score,
    CONCAT_WS('','',
      MAX(IF(o.concept_id = 166570 AND o.value_coded = 163565, ''Sexual Contact'', NULL)),
      MAX(IF(o.concept_id = 166570 AND o.value_coded = 166606, ''Social Contact'', NULL)),
      MAX(IF(o.concept_id = 166570 AND o.value_coded = 166517, ''Needle sharing'', NULL)),
      MAX(IF(o.concept_id = 166570 AND o.value_coded = 1107, ''None'', NULL))
    ) AS relationship_with_contact,
    MAX(IF(o.concept_id=1396,o.value_coded,NULL)) AS mother_hiv_status,
    MAX(IF(o.concept_id=164401,o.value_coded,NULL)) AS tested_hiv_before,
    MAX(IF(o.concept_id=165215,o.value_coded,NULL)) AS who_performed_test,
    MAX(IF(o.concept_id=159427,o.value_coded,NULL)) AS test_results,
    MAX(IF(o.concept_id=164400,o.value_datetime,NULL)) AS date_tested,
    MAX(IF(o.concept_id=165240,o.value_coded,NULL)) AS started_on_art,
    MAX(IF(o.concept_id=162053,o.value_numeric,NULL)) AS upn_number,
    MAX(IF(o.concept_id=160109,o.value_coded,NULL)) AS child_defiled,
    MAX(IF(o.concept_id=5569,o.value_coded,NULL)) AS ever_had_sex,
    MAX(IF(o.concept_id=160109,(CASE o.value_coded WHEN 1065 THEN ''YES'' WHEN 1066 THEN ''NO'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS sexually_active,
    MAX(IF(o.concept_id=167144,(CASE o.value_coded WHEN 1065 THEN ''YES'' WHEN 1066 THEN ''NO'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS new_partner,
    MAX(IF(o.concept_id=1436,(CASE o.value_coded WHEN 703 THEN ''Positive'' WHEN 664 THEN ''Negative'' WHEN 1067 THEN ''Unknown'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS partner_hiv_status,
    MAX(IF(o.concept_id=6096,o.value_coded,NULL)) AS couple_discordant,
    MAX(IF(o.concept_id=5568,(CASE o.value_coded WHEN 1 THEN ''YES'' WHEN 2 THEN ''NO'' END),NULL)) AS multiple_partners,
    MAX(IF(o.concept_id=5570,o.value_numeric,NULL)) AS number_partners,
    MAX(IF(o.concept_id=165088,o.value_coded,NULL)) AS alcohol_sex,
    MAX(IF(o.concept_id=160579,(CASE o.value_coded WHEN 1065 THEN ''YES'' WHEN 1066 THEN ''NO'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS money_sex,
    MAX(IF(o.concept_id=166559,(CASE o.value_coded WHEN 1065 THEN ''YES'' WHEN 1066 THEN ''NO'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS condom_burst,
    MAX(IF(o.concept_id=159218,(CASE o.value_coded WHEN 1065 THEN ''YES'' WHEN 1066 THEN ''NO'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS unknown_status_partner,
    MAX(IF(o.concept_id=163568,(CASE o.value_coded WHEN 163289 THEN ''YES'' WHEN 1066 THEN ''NO'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS known_status_partner,
    MAX(IF(o.concept_id=167161,(CASE o.value_coded WHEN 1065 THEN ''YES'' WHEN 1066 THEN ''NO'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS experienced_gbv,
    CONCAT_WS('','',
      MAX(IF(o.concept_id=167145 AND o.value_coded = 1065, ''Sexual violence'', NULL)),
      MAX(IF(o.concept_id=160658 AND o.value_coded = 1065, ''Emotional abuse'', NULL)),
      MAX(IF(o.concept_id=165205 AND o.value_coded = 1065, ''Physical violence'', NULL))
    ) AS type_of_gbv,
    CONCAT_WS('','',
      MAX(IF(o.concept_id=164845 AND o.value_coded = 1065, ''PEP'', NULL)),
      MAX(IF(o.concept_id=165269 AND o.value_coded = 1065, ''PrEP'', NULL)),
      MAX(IF(o.concept_id=165098 AND o.value_coded = 1065, ''STI'', NULL)),
      MAX(IF(o.concept_id=112141 AND o.value_coded = 1065, ''TB'', NULL))
    ) AS service_received,
    MAX(IF(o.concept_id=165203,(CASE o.value_coded WHEN 1065 THEN ''YES'' WHEN 1066 THEN ''NO'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS currently_on_prep,
    MAX(IF(o.concept_id=1691,(CASE o.value_coded WHEN 1 THEN ''YES'' WHEN 2 THEN ''NO'' END),NULL)) AS recently_on_pep,
    MAX(IF(o.concept_id=165200,(CASE o.value_coded WHEN 1065 THEN ''YES'' WHEN 1066 THEN ''NO'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS recently_had_sti,
    MAX(IF(o.concept_id=165197,(CASE o.value_coded WHEN 1065 THEN ''YES'' WHEN 1066 THEN ''NO'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS tb_screened,
    MAX(IF(o.concept_id=1729 AND o.value_coded = 159799,o.value_coded,1066)) AS cough,
    MAX(IF(o.concept_id=1729 AND o.value_coded = 1494,o.value_coded,1066)) AS fever,
    MAX(IF(o.concept_id=1729 AND o.value_coded = 832,o.value_coded,1066)) AS weight_loss,
    MAX(IF(o.concept_id=1729 AND o.value_coded = 133027,o.value_coded,1066)) AS night_sweats,
    MAX(IF(o.concept_id=1729 AND o.value_coded = 124068,o.value_coded,1066)) AS contact_with_tb_case,
    MAX(IF(o.concept_id=1729 AND o.value_coded = 116334,o.value_coded,1066)) AS lethargy,
    MAX(IF(o.concept_id=1659,o.value_coded,NULL)) AS tb_status,
    MAX(IF(o.concept_id=165090,(CASE o.value_coded WHEN 1065 THEN ''YES'' WHEN 1066 THEN ''NO'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS shared_needle,
    MAX(IF(o.concept_id=165060,o.value_coded,NULL)) AS needle_stick_injuries,
    MAX(IF(o.concept_id=166365,o.value_coded,NULL)) AS traditional_procedures,
    CONCAT_WS('','',
      MAX(IF(o.concept_id = 165908 AND o.value_coded = 115122, ''Malnutrition'', NULL)),
      MAX(IF(o.concept_id = 165908 AND o.value_coded = 5050, ''Failure to thrive'', NULL)),
      MAX(IF(o.concept_id = 165908 AND o.value_coded = 127833, ''Recurrent infections'', NULL)),
      MAX(IF(o.concept_id = 165908 AND o.value_coded = 112141, ''TB'', NULL)),
      MAX(IF(o.concept_id = 165908 AND o.value_coded = 1174, ''Orphaned'', NULL)),
      MAX(IF(o.concept_id = 165908 AND o.value_coded = 163718, ''Parents tested HIV positive'', NULL)),
      MAX(IF(o.concept_id = 165908 AND o.value_coded = 140238, ''Prolonged fever'', NULL)),
      MAX(IF(o.concept_id = 165908 AND o.value_coded = 5632, ''Child breastfeeding'', NULL))
    ) AS child_reasons_for_ineligibility,
    MAX(IF(o.concept_id=5272,(CASE o.value_coded WHEN 1065 THEN ''YES'' WHEN 1066 THEN ''NO'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS pregnant,
    MAX(IF(o.concept_id=5632,(CASE o.value_coded WHEN 1065 THEN ''YES'' WHEN 1066 THEN ''NO'' WHEN 162570 THEN ''Declined to answer'' ELSE '''' END),NULL)) AS breastfeeding_mother,
    MAX(IF(o.concept_id=162699,o.value_coded,NULL)) AS eligible_for_test,
    MAX(IF(o.concept_id=1788,o.value_coded,NULL)) AS referred_for_testing,
    MAX(IF(o.concept_id=164082,(CASE o.value_coded WHEN 165087 THEN ''HCW Provider Discretion'' WHEN 165091 THEN ''Based on Risk screening findings'' WHEN 1163 THEN ''ML Risk category'' WHEN 163510 THEN ''HTS Guidelines'' ELSE '''' END),NULL)) AS reason_to_test,
    MAX(IF(o.concept_id=160416,(CASE o.value_coded WHEN 165087 THEN ''HCW Provider Discretion'' WHEN 165091 THEN ''Based on Risk screening findings'' WHEN 1163 THEN ''ML Risk category'' WHEN 163510 THEN ''HTS Guidelines'' ELSE '''' END),NULL)) AS reason_not_to_test,
    CONCAT_WS('','',
      MAX(IF(o.concept_id = 159803 AND o.value_coded = 167156, ''Declined testing'', NULL)),
      MAX(IF(o.concept_id = 159803 AND o.value_coded = 165029, ''Wants to test with partner'', NULL)),
      MAX(IF(o.concept_id = 159803 AND o.value_coded = 160589, ''Stigma related issues'', NULL)),
      MAX(IF(o.concept_id = 159803 AND o.value_coded = 141814, ''Fear of violent partner'', NULL)),
      MAX(IF(o.concept_id = 159803 AND o.value_coded = 155974, ''No counselor to test'', NULL)),
      MAX(IF(o.concept_id = 159803 AND o.value_coded = 158948, ''High workload for the staff'', NULL)),
      MAX(IF(o.concept_id = 159803 AND o.value_coded = 163293, ''Too sick'', NULL)),
      MAX(IF(o.concept_id = 159803 AND o.value_coded = 160352, ''Lack of test kits'', NULL)),
      MAX(IF(o.concept_id = 159803 AND o.value_coded = 5622, ''Other'', NULL))
    ) AS reasons_for_ineligibility,
    MAX(IF(o.concept_id=160632,o.value_text,NULL)) AS specific_reason_for_ineligibility,
    e.date_created,
    IF(MAX(o.date_created) > MIN(e.date_created),MAX(o.date_created),NULL) AS date_last_modified,
    e.voided
FROM encounter e
  INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0
  INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''04295648-7606-11e8-adc0-fa7ae01bbebc''
  LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (164930,160581,138643,159936,164932,5619,166570,164401,165215,159427,164400,165240,162053,160109,167144,1436,6096,5568,5570,165088,160579,166559,159218,163568,167161,1396,167145,160658,165205,164845,165269,112141,165203,1691,165200,165197,1729,1659,165090,165060,166365,165908,165098,5272,5632,162699,1788,159803,160632,164126,159803,167229,160540,167163,167162,160416,1396,5569,164956,160109) AND o.voided=0
WHERE e.voided=0 AND (
  (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ')
  OR (e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ')
  OR (e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ')
  OR (o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ')
  OR (o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ')
)
GROUP BY e.patient_id, DATE(e.encounter_datetime)
ORDER BY e.patient_id
ON DUPLICATE KEY UPDATE
  provider=VALUES(provider),
  visit_date=VALUES(visit_date),
  population_type=VALUES(population_type),
  key_population_type=VALUES(key_population_type),
  priority_population_type=VALUES(priority_population_type),
  patient_disabled=VALUES(patient_disabled),
  disability_type=VALUES(disability_type),
  department=VALUES(department),
  patient_type=VALUES(patient_type),
  is_health_worker=VALUES(is_health_worker),
  recommended_test=VALUES(recommended_test),
  test_strategy=VALUES(test_strategy),
  hts_entry_point=VALUES(hts_entry_point),
  hts_risk_category=VALUES(hts_risk_category),
  hts_risk_score=VALUES(hts_risk_score),
  relationship_with_contact=VALUES(relationship_with_contact),
  mother_hiv_status=VALUES(mother_hiv_status),
  tested_hiv_before=VALUES(tested_hiv_before),
  who_performed_test=VALUES(who_performed_test),
  test_results=VALUES(test_results),
  date_tested=VALUES(date_tested),
  started_on_art=VALUES(started_on_art),
  upn_number=VALUES(upn_number),
  child_defiled=VALUES(child_defiled),
  ever_had_sex=VALUES(ever_had_sex),
  sexually_active=VALUES(sexually_active),
  new_partner=VALUES(new_partner),
  partner_hiv_status=VALUES(partner_hiv_status),
  couple_discordant=VALUES(couple_discordant),
  multiple_partners=VALUES(multiple_partners),
  number_partners=VALUES(number_partners),
  alcohol_sex=VALUES(alcohol_sex),
  money_sex=VALUES(money_sex),
  condom_burst=VALUES(condom_burst),
  unknown_status_partner=VALUES(unknown_status_partner),
  known_status_partner=VALUES(known_status_partner),
  experienced_gbv=VALUES(experienced_gbv),
  type_of_gbv=VALUES(type_of_gbv),
  service_received=VALUES(service_received),
  currently_on_prep=VALUES(currently_on_prep),
  recently_on_pep=VALUES(recently_on_pep),
  recently_had_sti=VALUES(recently_had_sti),
  tb_screened=VALUES(tb_screened),
  cough=VALUES(cough),
  fever=VALUES(fever),
  weight_loss=VALUES(weight_loss),
  night_sweats=VALUES(night_sweats),
  contact_with_tb_case=VALUES(contact_with_tb_case),
  lethargy=VALUES(lethargy),
  tb_status=VALUES(tb_status),
  shared_needle=VALUES(shared_needle),
  needle_stick_injuries=VALUES(needle_stick_injuries),
  traditional_procedures=VALUES(traditional_procedures),
  child_reasons_for_ineligibility=VALUES(child_reasons_for_ineligibility),
  pregnant=VALUES(pregnant),
  breastfeeding_mother=VALUES(breastfeeding_mother),
  eligible_for_test=VALUES(eligible_for_test),
  referred_for_testing=VALUES(referred_for_testing),
  reason_to_test=VALUES(reason_to_test),
  reason_not_to_test=VALUES(reason_not_to_test),
  reasons_for_ineligibility=VALUES(reasons_for_ineligibility),
  specific_reason_for_ineligibility=VALUES(specific_reason_for_ineligibility),
  date_created=VALUES(date_created),
  date_last_modified=VALUES(date_last_modified),
  voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing hts eligibility screening data ', CONCAT('Time: ', NOW());
END $$




-- sql

-- --------------------------------------
-- PROCEDURE: sp_update_etl_patient_appointments
-- Purpose: populate tenant-aware etl_patient_appointment
-- Tenant-aware: calls sp_set_tenant_session_vars() and uses dynamic INSERT target
-- --------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_patient_appointments $$
CREATE PROCEDURE sp_update_etl_patient_appointments(IN last_update_time DATETIME)
BEGIN
  DECLARE target_table VARCHAR(255);
  DECLARE sql_stmt TEXT;
  -- resolve tenant schema
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_patient_appointment');
SELECT 'Processing Patient appointment updates', CONCAT('Target: ', @target_table, ' Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (patient_appointment_id, provider_id, patient_id, visit_date, start_date_time, end_date_time, appointment_service_id, status, location_id, date_created) ',
    'SELECT patient_appointment_id, provider_id, patient_id, DATE(date_appointment_scheduled) AS visit_date, start_date_time, end_date_time, appointment_service_id, status, location_id, date_created ',
    'FROM patient_appointment ',
    'WHERE voided = 0 AND (date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ' OR date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY patient_id ',
    'ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date), start_date_time=VALUES(start_date_time), end_date_time=VALUES(end_date_time), appointment_service_id=VALUES(appointment_service_id), status=VALUES(status), date_created=VALUES(date_created);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SELECT 'Completed processing Patient appointment', CONCAT('Time: ', NOW());
END $$


-- sql
-- --------------------------------------
-- PROCEDURE: sp_update_etl_drug_order
-- Purpose: populate tenant-aware etl_drug_order
-- Tenant-aware: calls sp_set_tenant_session_vars() and inserts into dynamic target
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- --------------------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_drug_order $$
CREATE PROCEDURE sp_update_etl_drug_order(IN last_update_time DATETIME)
BEGIN
  DECLARE target_table VARCHAR(300);
  DECLARE sql_stmt TEXT;

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_drug_order');

SELECT 'Processing Drug orders', CONCAT('Target: ', @target_table, ' Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (uuid, encounter_id, order_group_id, patient_id, location_id, visit_date, visit_id, provider, order_id, urgency, drug_id, drug_concept_id, drug_name, frequency, enc_name, dose, dose_units, quantity, quantity_units, dosing_instructions, duration, duration_units, instructions, route, voided, date_voided, date_created, date_last_modified) ',
    'SELECT e.uuid, e.encounter_id, o.order_group_id, e.patient_id, e.location_id, DATE(e.encounter_datetime) AS visit_date, e.visit_id, e.creator AS provider, do.order_id, o.urgency, d.drug_id, GROUP_CONCAT(DISTINCT o.concept_id SEPARATOR ''|'') AS drug_concept_id, GROUP_CONCAT(DISTINCT LEFT(d.name,255) SEPARATOR ''+'') AS drug_name, ',
    'COALESCE(GROUP_CONCAT(DISTINCT CASE do.frequency WHEN 1 THEN ''Once daily, in the evening'' WHEN 2 THEN ''Once daily, in the morning'' WHEN 3 THEN ''Twice daily'' WHEN 4 THEN ''Once daily, at bedtime'' WHEN 5 THEN ''Once daily'' WHEN 6 THEN ''Thrice daily'' ELSE ''Unknown'' END SEPARATOR ''|''), ''Unknown'') AS frequency, ',
    'et.name AS enc_name, GROUP_CONCAT(DISTINCT do.dose SEPARATOR ''|'') AS dose, do.dose_units AS dose_units, do.quantity AS quantity, do.quantity_units AS quantity_units, do.dosing_instructions, do.duration, ',
    'CASE do.duration_units WHEN 1072 THEN ''DAYS'' WHEN 1073 THEN ''WEEKS'' WHEN 1074 THEN ''MONTHS'' ELSE ''UNKNOWN'' END AS duration_units, o.instructions, do.route AS route, o.voided, o.date_voided, e.date_created, e.date_changed AS date_last_modified ',
    'FROM orders o INNER JOIN drug_order do ON o.order_id = do.order_id INNER JOIN drug d ON do.drug_inventory_id = d.drug_id INNER JOIN encounter e ON e.encounter_id = o.encounter_id AND e.voided = 0 AND e.patient_id = o.patient_id INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 LEFT JOIN encounter_type et ON et.encounter_type_id = e.encounter_type ',
    'WHERE o.voided = 0 AND o.order_type_id = 2 AND (COALESCE(o.order_action, '''') = ''NEW'' OR COALESCE(o.order_reason_non_coded, '''') = ''previously existing orders'') AND e.voided = 0 ',
    'GROUP BY o.order_group_id, o.patient_id, o.encounter_id ',
    'ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date), provider=VALUES(provider), order_id=VALUES(order_id), urgency=VALUES(urgency), drug_concept_id=VALUES(drug_concept_id), drug_id=VALUES(drug_id), drug_name=VALUES(drug_name), frequency=VALUES(frequency), enc_name=VALUES(enc_name), dose=VALUES(dose), dose_units=VALUES(dose_units), quantity=VALUES(quantity), quantity_units=VALUES(quantity_units), dosing_instructions=VALUES(dosing_instructions), duration=VALUES(duration), duration_units=VALUES(duration_units), instructions=VALUES(instructions), route=VALUES(route), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing drug orders data', CONCAT('Time: ', NOW());
END $$




-- sql

-- --------------------------------------
-- PROCEDURE: sp_update_etl_preventive_services
-- Purpose: populate tenant-aware etl_preventive_services
-- Tenant-aware: calls sp_set_tenant_session_vars() and uses dynamic INSERT target
-- --------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_preventive_services $$
CREATE PROCEDURE sp_update_etl_preventive_services(IN last_update_time DATETIME)
BEGIN
  DECLARE target_table VARCHAR(300);
  DECLARE sql_stmt TEXT;

CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_preventive_services');

SELECT 'Processing Preventive services', CONCAT('Target: ', @target_table, ' Time: ', NOW());

SET sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, visit_date, provider, location_id, encounter_id, obs_group_id, ',
      'malaria_prophylaxis_1, malaria_prophylaxis_2, malaria_prophylaxis_3, ',
      'tetanus_taxoid_1, tetanus_taxoid_2, tetanus_taxoid_3, tetanus_taxoid_4, ',
      'folate_iron_1, folate_iron_2, folate_iron_3, folate_iron_4, ',
      'folate_1, folate_2, folate_3, folate_4, ',
      'iron_1, iron_2, iron_3, iron_4, ',
      'mebendazole, long_lasting_insecticidal_net, comment, date_last_modified, date_created, voided) ',
    'SELECT ',
      'y.patient_id, y.visit_date, y.provider, y.location_id, y.encounter_id, y.obs_group_id, ',
      'MAX(IF(vaccine = ''Malarial prophylaxis'' AND sequence = 1, date_given, NULL)) AS malaria_prophylaxis_1, ',
      'MAX(IF(vaccine = ''Malarial prophylaxis'' AND sequence = 2, date_given, NULL)) AS malaria_prophylaxis_2, ',
      'MAX(IF(vaccine = ''Malarial prophylaxis'' AND sequence = 3, date_given, NULL)) AS malaria_prophylaxis_3, ',
      'MAX(IF(vaccine = ''Tetanus Toxoid'' AND sequence = 1, date_given, NULL)) AS tetanus_taxoid_1, ',
      'MAX(IF(vaccine = ''Tetanus Toxoid'' AND sequence = 2, date_given, NULL)) AS tetanus_taxoid_2, ',
      'MAX(IF(vaccine = ''Tetanus Toxoid'' AND sequence = 3, date_given, NULL)) AS tetanus_taxoid_3, ',
      'MAX(IF(vaccine = ''Tetanus Toxoid'' AND sequence = 4, date_given, NULL)) AS tetanus_taxoid_4, ',
      'MAX(IF(vaccine = ''Folate/Iron'' AND sequence = 1, date_given, NULL)) AS folate_iron_1, ',
      'MAX(IF(vaccine = ''Folate/Iron'' AND sequence = 2, date_given, NULL)) AS folate_iron_2, ',
      'MAX(IF(vaccine = ''Folate/Iron'' AND sequence = 3, date_given, NULL)) AS folate_iron_3, ',
      'MAX(IF(vaccine = ''Folate/Iron'' AND sequence = 4, date_given, NULL)) AS folate_iron_4, ',
      'MAX(IF(vaccine = ''Folate'' AND sequence = 1, date_given, NULL)) AS folate_1, ',
      'MAX(IF(vaccine = ''Folate'' AND sequence = 2, date_given, NULL)) AS folate_2, ',
      'MAX(IF(vaccine = ''Folate'' AND sequence = 3, date_given, NULL)) AS folate_3, ',
      'MAX(IF(vaccine = ''Folate'' AND sequence = 4, date_given, NULL)) AS folate_4, ',
      'MAX(IF(vaccine = ''Iron'' AND sequence = 1, date_given, NULL)) AS iron_1, ',
      'MAX(IF(vaccine = ''Iron'' AND sequence = 2, date_given, NULL)) AS iron_2, ',
      'MAX(IF(vaccine = ''Iron'' AND sequence = 3, date_given, NULL)) AS iron_3, ',
      'MAX(IF(vaccine = ''Iron'' AND sequence = 4, date_given, NULL)) AS iron_4, ',
      'MAX(IF(vaccine = ''Mebendazole'', date_given, NULL)) AS mebendazole, ',
      'MAX(IF(vaccine = ''Long-lasting insecticidal net'', date_given, NULL)) AS long_lasting_insecticidal_net, ',
      'y.comment, y.date_last_modified, y.date_created, y.voided ',
    'FROM (',
      '  SELECT person_id AS patient_id, visit_id, DATE(encounter_datetime) AS visit_date, creator AS provider, ',
      '         location_id, encounter_id, ',
      '         MAX(IF(concept_id = 984, (CASE WHEN value_coded = 84879 THEN ''Tetanus Toxoid'' WHEN value_coded = 159610 THEN ''Malarial prophylaxis'' WHEN value_coded = 104677 THEN ''Folate/Iron'' WHEN value_coded = 79413 THEN ''Mebendazole'' WHEN value_coded = 160428 THEN ''Long-lasting insecticidal net'' WHEN value_coded = 76609 THEN ''Folate'' WHEN value_coded = 78218 THEN ''Iron'' END), NULL)) AS vaccine, ',
      '         MAX(IF(concept_id = 1418, value_numeric, NULL)) AS sequence, ',
      '         MAX(IF(concept_id = 161011, value_text, NULL)) AS comment, ',
      '         MAX(IF(concept_id = 1410, date_given, NULL)) AS date_given, ',
      '         DATE(date_created) AS date_created, date_last_modified, voided, obs_group_id ',
      '  FROM (',
      '    SELECT o.person_id, e.visit_id, o.concept_id, e.encounter_datetime, e.creator, e.date_created, ',
      '           IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      '           o.value_coded, o.value_numeric, o.value_text, DATE(o.value_datetime) AS date_given, o.obs_group_id, o.encounter_id, e.voided, e.location_id ',
      '    FROM obs o ',
      '    INNER JOIN encounter e ON e.encounter_id = o.encounter_id ',
      '    INNER JOIN person p ON p.person_id = o.person_id AND p.voided = 0 ',
      '    INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''d3ea25c7-a3e8-4f57-a6a9-e802c3565a30'',''e8f98494-af35-4bb8-9fc7-c409c8fed843'') ',
      '    WHERE concept_id IN (984,1418,161011,1410,5096) AND o.voided = 0 ',
      '      AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
      '    GROUP BY o.obs_group_id, o.concept_id, e.encounter_datetime ',
      '  ) t ',
      '  GROUP BY t.obs_group_id ',
      '  HAVING vaccine != '''' ',
    ') y ',
    'GROUP BY y.obs_group_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date = VALUES(visit_date), provider = VALUES(provider), ',
      'malaria_prophylaxis_1 = VALUES(malaria_prophylaxis_1), malaria_prophylaxis_2 = VALUES(malaria_prophylaxis_2), malaria_prophylaxis_3 = VALUES(malaria_prophylaxis_3), ',
      'tetanus_taxoid_1 = VALUES(tetanus_taxoid_1), tetanus_taxoid_2 = VALUES(tetanus_taxoid_2), tetanus_taxoid_3 = VALUES(tetanus_taxoid_3), tetanus_taxoid_4 = VALUES(tetanus_taxoid_4), ',
      'folate_iron_1 = VALUES(folate_iron_1), folate_iron_2 = VALUES(folate_iron_2), folate_iron_3 = VALUES(folate_iron_3), folate_iron_4 = VALUES(folate_iron_4), ',
      'folate_1 = VALUES(folate_1), folate_2 = VALUES(folate_2), folate_3 = VALUES(folate_3), folate_4 = VALUES(folate_4), ',
      'iron_1 = VALUES(iron_1), iron_2 = VALUES(iron_2), iron_3 = VALUES(iron_3), iron_4 = VALUES(iron_4), ',
      'mebendazole = VALUES(mebendazole), long_lasting_insecticidal_net = VALUES(long_lasting_insecticidal_net), comment = VALUES(comment), voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 'Completed processing Preventive services data', CONCAT('Target: ', @target_table, ' Time: ', NOW());
END $$



-- sql


-- --------------------------------------
-- PROCEDURE: sp_update_etl_overdose_reporting
-- Purpose: populate tenant-aware etl_overdose_reporting
-- Tenant-aware: calls sp_set_tenant_session_vars() and uses dynamic INSERT target
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- --------------------------------------


DROP PROCEDURE IF EXISTS sp_update_etl_overdose_reporting $$
CREATE PROCEDURE sp_update_etl_overdose_reporting(IN last_update_time DATETIME)
BEGIN
    -- 1. Declare local variables for table names
    DECLARE target_table VARCHAR(300);
    DECLARE last_ts_str VARCHAR(100);

    -- 2. Set tenant vars (sets @etl_schema)
CALL sp_set_tenant_session_vars();

-- 3. Define the target table and the safe timestamp string
SET @target_table = CONCAT(@etl_schema, '.etl_overdose_reporting');
    SET last_ts_str = IFNULL(CONCAT('''', last_update_time, ''''), '''1900-01-01 00:00:00''');

    -- 4. Build the SQL statement using the @ session variable
    SET @sql_stmt = CONCAT(
        'INSERT INTO ', @target_table, ' (',
          'client_id, visit_id, encounter_id, uuid, provider, location_id, visit_date, ',
          'overdose_location, overdose_date, incident_type, incident_site_name, incident_site_type, ',
          'naloxone_provided, risk_factors, other_risk_factors, drug, other_drug, outcome, remarks, ',
          'reported_by, date_reported, witness, date_witnessed, encounter, date_created, date_last_modified, voided',
        ') ',
        'SELECT ',
          'e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.creator AS provider, e.location_id, DATE(e.encounter_datetime) AS visit_date, ',
          'MAX(IF(o.concept_id=162725,o.value_text,NULL)) AS overdose_location, ',
          'MAX(IF(o.concept_id=165146,o.value_datetime,NULL)) AS overdose_date, ',
          'MAX(IF(o.concept_id=165133,o.value_coded,NULL)) AS incident_type, ',
          'MAX(IF(o.concept_id=165006,o.value_text,NULL)) AS incident_site_name, ',
          'MAX(IF(o.concept_id=165005,o.value_coded,NULL)) AS incident_site_type, ',
          'MAX(IF(o.concept_id=165136,o.value_coded,NULL)) AS naloxone_provided, ',
          'CONCAT_WS('','', ',
            'MAX(IF(o.concept_id=165140 AND o.value_coded=989,''Age'',NULL)), ',
            'MAX(IF(o.concept_id=165140 AND o.value_coded=162747,''Comorbidity'',NULL)), ',
            'MAX(IF(o.concept_id=165140 AND o.value_coded=131779,''Abstinence from opioid use'',NULL)), ',
            'MAX(IF(o.concept_id=165140 AND o.value_coded=129754,''Mixing'',NULL)), ',
            'MAX(IF(o.concept_id=165140 AND o.value_coded=134236,''MAT induction/Re-induction'',NULL)), ',
            'MAX(IF(o.concept_id=165140 AND o.value_coded=5622,''Other'',NULL))',
          ') AS risk_factors, ',
          'MAX(IF(o.concept_id=165145,o.value_text,NULL)) AS other_risk_factors, ',
          'CONCAT_WS('','', ',
            'MAX(IF(o.concept_id=1193 AND o.value_coded=79661,''Methadone'',NULL)), ',
            'MAX(IF(o.concept_id=1193 AND o.value_coded=121725,''Alcohol'',NULL)), ',
            'MAX(IF(o.concept_id=1193 AND o.value_coded=146504,''Cannabis'',NULL)), ',
            'MAX(IF(o.concept_id=1193 AND o.value_coded=73650,''Cocaine'',NULL)), ',
            'MAX(IF(o.concept_id=1193 AND o.value_coded=76511,''Flunitrazepam (Tap tap, Bugizi)'',NULL)), ',
            'MAX(IF(o.concept_id=1193 AND o.value_coded=77443,''Heroine'',NULL)), ',
            'MAX(IF(o.concept_id=1193 AND o.value_coded=5622,''Other'',NULL))',
          ') AS drug, ',
          'MAX(IF(o.concept_id=163101,o.value_text,NULL)) AS other_drug, ',
          'MAX(IF(o.concept_id=165141,o.value_coded,NULL)) AS outcome, ',
          'MAX(IF(o.concept_id=160632,o.value_text,NULL)) AS remarks, ',
          'MAX(IF(o.concept_id=1473,o.value_text,NULL)) AS reported_by, ',
          'MAX(IF(o.concept_id=165144,o.value_datetime,NULL)) AS date_reported, ',
          'MAX(IF(o.concept_id=165143,o.value_text,NULL)) AS witness, ',
          'MAX(IF(o.concept_id=160753,o.value_datetime,NULL)) AS date_witnessed, ',
          'CASE f.uuid WHEN ''92fd9c5a-c84a-483b-8d78-d4d7a600db30'' THEN ''Peer Overdose'' WHEN ''d753bab3-0bbb-43f5-9796-5e95a5d641f3'' THEN ''HCW overdose'' END AS encounter, ',
          'MIN(e.date_created) AS date_created, ',
          'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
          'MAX(e.voided) AS voided ',
        'FROM encounter e ',
          'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
          'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (''92fd9c5a-c84a-483b-8d78-d4d7a600db30'',''d753bab3-0bbb-43f5-9796-5e95a5d641f3'') ',
          'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (162725,165146,165133,165006,165005,165136,165140,1193,163101,165141,160632,1473,165144,165143,160753) AND o.voided = 0 ',
        'WHERE e.voided = 0 AND (',
          'e.date_created >= ', last_ts_str,
          ' OR e.date_changed >= ', last_ts_str,
          ' OR o.date_created >= ', last_ts_str, ') ',
        'GROUP BY e.encounter_id ',
        'ON DUPLICATE KEY UPDATE ',
          'visit_date = VALUES(visit_date), provider = VALUES(provider), overdose_location = VALUES(overdose_location), overdose_date = VALUES(overdose_date), ',
          'incident_type = VALUES(incident_type), incident_site_name = VALUES(incident_site_name), incident_site_type = VALUES(incident_site_type), ',
          'naloxone_provided = VALUES(naloxone_provided), risk_factors = VALUES(risk_factors), other_risk_factors = VALUES(other_risk_factors), ',
          'drug = VALUES(drug), other_drug = VALUES(other_drug), outcome = VALUES(outcome), remarks = VALUES(remarks), ',
          'reported_by = VALUES(reported_by), date_reported = VALUES(date_reported), witness = VALUES(witness), date_witnessed = VALUES(date_witnessed), encounter = VALUES(encounter), ',
          'date_created = VALUES(date_created), date_last_modified = VALUES(date_last_modified), voided = VALUES(voided);'
    );

    -- 5. Prepare and Execute
PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing overdose reporting. Target: ', @target_table, ' Time: ', NOW()) AS message;
END $$


-- --------------------------------------
-- PROCEDURE: sp_update_etl_art_fast_track
-- Purpose: populate tenant-aware etl_art_fast_track
-- Notes: calls sp_set_tenant_session_vars() to resolve etl_schema then
--        builds and executes a dynamic INSERT ... ON DUPLICATE KEY UPDATE
-- --------------------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_art_fast_track $$
CREATE PROCEDURE sp_update_etl_art_fast_track(IN last_update_time DATETIME)
BEGIN
  -- set tenant vars (must exist in DB)
CALL sp_set_tenant_session_vars();

-- build target table name for tenant
SET @target_table = CONCAT('', etl_schema, '.etl_art_fast_track');

  SET @sql_stmt = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, ',
      'art_refill_model, ctx_dispensed, dapsone_dispensed, oral_contraceptives_dispensed, condoms_distributed, ',
      'doses_missed, fatigue, cough, fever, rash, nausea_vomiting, genital_sore_discharge, diarrhea, ',
      'other_symptoms, other_specific_symptoms, pregnant, family_planning_status, family_planning_method, ',
      'reason_not_on_family_planning, referred_to_clinic, return_visit_date, date_created, date_last_modified, voided',
    ') ',
    'SELECT ',
      'e.uuid, e.creator AS provider, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'MAX(IF(o.concept_id = 1758, o.value_coded, NULL)) AS art_refill_model, ',
      'MAX(IF(o.concept_id = 1282 AND o.value_coded = 162229, o.value_coded, NULL)) AS ctx_dispensed, ',
      'MAX(IF(o.concept_id = 1282 AND o.value_coded = 74250, o.value_coded, NULL)) AS dapsone_dispensed, ',
      'MAX(IF(o.concept_id = 1282 AND o.value_coded = 780, o.value_coded, NULL)) AS oral_contraceptives_dispensed, ',
      'MAX(IF(o.concept_id = 159777, o.value_coded, NULL)) AS condoms_distributed, ',
      'MAX(IF(o.concept_id = 162878, o.value_numeric, NULL)) AS doses_missed, ',
      'MAX(IF(o.concept_id = 1284 AND o.value_coded = 162626, o.value_coded, NULL)) AS fatigue, ',
      'MAX(IF(o.concept_id = 1284 AND o.value_coded = 143264, o.value_coded, NULL)) AS cough, ',
      'MAX(IF(o.concept_id = 1284 AND o.value_coded = 140238, o.value_coded, NULL)) AS fever, ',
      'MAX(IF(o.concept_id = 1284 AND o.value_coded = 512, o.value_coded, NULL)) AS rash, ',
      'MAX(IF(o.concept_id = 1284 AND o.value_coded = 5978, o.value_coded, NULL)) AS nausea_vomiting, ',
      'MAX(IF(o.concept_id = 1284 AND o.value_coded = 135462, o.value_coded, NULL)) AS genital_sore_discharge, ',
      'MAX(IF(o.concept_id = 1284 AND o.value_coded = 142412, o.value_coded, NULL)) AS diarrhea, ',
      'MAX(IF(o.concept_id = 1284 AND o.value_coded = 5622, o.value_coded, NULL)) AS other_symptoms, ',
      'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS other_specific_symptoms, ',
      'MAX(IF(o.concept_id = 5272, o.value_coded, NULL)) AS pregnant, ',
      'MAX(IF(o.concept_id = 160653, o.value_coded, NULL)) AS family_planning_status, ',
      'CONCAT_WS('','', ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 160570, ''Emergency contraceptive pills'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 780, ''Oral Contraceptives Pills'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 5279, ''Injectible'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 1359, ''Implant'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 5275, ''Intrauterine Device'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 136163, ''Lactational Amenorhea Method'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 5278, ''Diaphram/Cervical Cap'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 159524, ''Fertility Awareness'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 1472, ''Tubal Ligation'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 190, ''Condoms'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 1489, ''Vasectomy(Partner)'', NULL)), ',
        'MAX(IF(o.concept_id = 374 AND o.value_coded = 1175, ''Undecided'', NULL))',
      ') AS family_planning_method, ',
      'CONCAT_WS('','', ',
        'MAX(IF(o.concept_id = 160575 AND o.value_coded = 160572, ''Thinks cannot get pregnant'', NULL)), ',
        'MAX(IF(o.concept_id = 160575 AND o.value_coded = 160573, ''Not sexually active now'', NULL))',
      ') AS reason_not_on_family_planning, ',
      'MAX(IF(o.concept_id = 512, o.value_coded, NULL)) AS referred_to_clinic, ',
      'MAX(IF(o.concept_id = 2096, o.value_datetime, NULL)) AS return_visit_date, ',
      'MIN(e.date_created) AS date_created, ',
      'IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, ',
      'MAX(e.voided) AS voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''83fb6ab2-faec-4d87-a714-93e77a28a201'' ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (1758,1282,159777,162878,1284,5272,160653,374,160575,512,2096) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.encounter_id ',
    'ORDER BY e.patient_id ',
    'ON DUPLICATE KEY UPDATE ',
      'visit_date=VALUES(visit_date), provider=VALUES(provider), art_refill_model=VALUES(art_refill_model), ',
      'ctx_dispensed=VALUES(ctx_dispensed), dapsone_dispensed=VALUES(dapsone_dispensed), oral_contraceptives_dispensed=VALUES(oral_contraceptives_dispensed), ',
      'condoms_distributed=VALUES(condoms_distributed), doses_missed=VALUES(doses_missed), fatigue=VALUES(fatigue), cough=VALUES(cough), ',
      'fever=VALUES(fever), rash=VALUES(rash), nausea_vomiting=VALUES(nausea_vomiting), genital_sore_discharge=VALUES(genital_sore_discharge), ',
      'diarrhea=VALUES(diarrhea), other_symptoms=VALUES(other_symptoms), other_specific_symptoms=VALUES(other_specific_symptoms), ',
      'pregnant=VALUES(pregnant), family_planning_status=VALUES(family_planning_status), family_planning_method=VALUES(family_planning_method), ',
      'reason_not_on_family_planning=VALUES(reason_not_on_family_planning), referred_to_clinic=VALUES(referred_to_clinic), return_visit_date=VALUES(return_visit_date), ',
      'date_created=VALUES(date_created), date_last_modified=VALUES(date_last_modified), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing ART fast track. Target: ', @target_table, ' Time: ', NOW()) AS message;
END $$



--------------- create etl_clinical_encounter -------------------


DROP PROCEDURE IF EXISTS sp_update_etl_clinical_encounter $$
CREATE PROCEDURE sp_update_etl_clinical_encounter(IN last_update_time DATETIME)
BEGIN
  -- set tenant/session vars (expects sp_set_tenant_session_vars to populate etl_schema)
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_clinical_encounter');

SELECT 'Processing Clinical Encounter' AS status;

SET @sql = CONCAT(
'INSERT INTO ', @target_table, ' (',
'patient_id, visit_id, encounter_id, uuid, location_id, provider, visit_date, visit_type, ',
'therapy_ordered, other_therapy_ordered, counselling_ordered, other_counselling_ordered, ',
'procedures_prescribed, procedures_ordered, patient_outcome, diagnosis_category, general_examination, ',
'admission_needed, date_of_patient_admission, admission_reason, admission_type, priority_of_admission, ',
'admission_ward, hospital_stay, referral_needed, referral_ordered, referral_to, other_facility, this_facility, voided',
') ',
'SELECT ',
'e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator, DATE(e.encounter_datetime) AS visit_date, ',
'MAX(IF(o.concept_id=164181, (CASE o.value_coded WHEN 164180 THEN ''New visit'' WHEN 160530 THEN ''Revisit'' WHEN 160563 THEN ''Transfer in'' ELSE '''' END), NULL)) AS visit_type, ',
'CONCAT_WS('','', ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1107, ''None'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 165225, ''Support service provided'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 163319, ''Behavioural activation therapy'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1000209, ''Occupational Therapy'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 131022, ''Pain Management'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1000579, ''Physiotherapy'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 135797, ''Lifestyle Modification Programs'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1000528, ''Respiratory Therapy'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 5622, ''Other'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174, o.value_text, '''')),'''')) AS therapy_ordered, ',
'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS other_therapy_ordered, ',
'CONCAT_WS('','', ',
'  NULLIF(MAX(IF(o.concept_id=165104 AND o.value_coded = 1107, ''None'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=165104 AND o.value_coded = 5490, ''Psychosocial therapy'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=165104 AND o.value_coded = 165151, ''Substance Abuse Counseling'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=165104 AND o.value_coded = 1380, ''Nutritional and Dietary'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=165104 AND o.value_coded = 156277, ''Family Counseling'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=165104 AND o.value_coded = 5622, ''Other'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=165104, o.value_text, '''')),'''')) AS counselling_ordered, ',
'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS other_counselling_ordered, ',
'MAX(IF(o.concept_id=1651, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS procedures_prescribed, ',
'CONCAT_WS('','', ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 166715, ''Incision and Drainage(I&D)'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 527, ''Splinting and Casting'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1000254, ''Nebulization'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1000361, ''Nasogastric Tube Insertion'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 113223, ''Ear Irrigation'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1000238, ''Urethral Catheterization'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 161797, ''Suprapubic Catheterization'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1000359, ''Nasal Cauterization'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1000248, ''Gastric Lavage'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1000247, ''Removal of Foreign Body'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 166941, ''Thoraxic Drainage'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 162809, ''Batholin Gland marsupialization'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 166741, ''Intra-articular Injection'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 161773, ''Haemorrhoids Injections'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 164913, ''Joints Aspiration'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 149977, ''Release of trigger finger'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 161623, ''Surgical toilet and suturing'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1935, ''Wound Dressing'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 159728, ''Manual Vacuum Aspiration'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1637, ''Dilatation and Curetage'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 159842, ''Episiotomy Repair'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 161663, ''Skin Lesion Excision'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1000136, ''Biopsy'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 161803, ''Circumcision'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 161284, ''Paracentesis'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 165893, ''Cannulation'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 162651, ''Iv fluids management'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 441, ''Bandaging'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 1000222, ''Enema Administration'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174 AND o.value_coded = 127896, ''Lumbar Puncture'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=164174, o.value_text, '''')),'''')) AS procedures_ordered, ',
'MAX(IF(o.concept_id=160433, o.value_coded, NULL)) AS patient_outcome, ',
'MAX(IF(o.concept_id=2031533, (CASE o.value_coded WHEN 1687 THEN ''New'' WHEN 2031534 THEN ''Existing'' ELSE '''' END), NULL)) AS diagnosis_category, ',
'CONCAT_WS('','', ',
'  NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded = 1107, ''None'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded = 136443, ''Jaundice'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded = 460, ''Oedema'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded = 5334, ''Oral Thrush'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded = 5245, ''Pallor'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded = 140125, ''Finger Clubbing'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded = 126952, ''Lymph Node Axillary'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded = 143050, ''Cyanosis'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded = 126939, ''Lymph Nodes Inguinal'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded = 823, ''Wasting'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded = 142630, ''Dehydration'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded = 116334, ''Lethargic'', '''')),'''')) AS general_examination, ',
'MAX(IF(o.concept_id=1651, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS admission_needed, ',
'MAX(IF(o.concept_id = 1640, o.value_datetime, NULL)) AS date_of_patient_admission, ',
'MAX(IF(o.concept_id=164174, o.value_text, NULL)) AS admission_reason, ',
'MAX(IF(o.concept_id=162477, (CASE o.value_coded WHEN 164180 THEN ''New'' WHEN 159833 THEN ''Readmission'' ELSE '''' END), NULL)) AS admission_type, ',
'MAX(IF(o.concept_id=1655, (CASE o.value_coded WHEN 160473 THEN ''Emergency'' WHEN 159310 THEN ''Direct'' WHEN 1000139 THEN ''Scheduled'' ELSE '''' END), NULL)) AS priority_of_admission, ',
'CONCAT_WS('','', ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 1000039, ''Female medical'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 1000035, ''Female Surgical'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 1000040, ''Male Medical'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 1000041, ''Male Surgical'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 1000032, ''Maternity Ward'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 1000061, ''Pediatric Ward'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 1000038, ''Child Ward'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 164835, ''Labor and Delivery Unit'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 161629, ''Observation Ward'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 162680, ''Recovery Ward'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 1000054, ''Psychiatric Ward'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 1000056, ''Isolation Ward'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 161936, ''Intensive Care Unit'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 1000199, ''Amenity Ward'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 1000201, ''Gynaecological Ward'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 1000059, ''Nursery Unit/Newborn Unit'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 167396, ''High Dependecy Unit'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075 AND o.value_coded = 165644, ''Neonatal Intensive Care Unit'', '''')),''''), ',
'  NULLIF(MAX(IF(o.concept_id=1000075, o.value_text, '''')),'''')) AS admission_ward, ',
'MAX(IF(o.concept_id=1896, (CASE o.value_coded WHEN 1072 THEN ''Daycase'' WHEN 161018 THEN ''Overnight'' WHEN 1275 THEN ''Longer stay'' ELSE '''' END), NULL)) AS hospital_stay, ',
'MAX(IF(o.concept_id=1272, (CASE o.value_coded WHEN 1065 THEN ''Yes'' WHEN 1066 THEN ''No'' ELSE '''' END), NULL)) AS referral_needed, ',
'MAX(IF(o.concept_id=160632, o.value_text, NULL)) AS referral_ordered, ',
'MAX(IF(o.concept_id=163145, (CASE o.value_coded WHEN 164407 THEN ''Other health facility'' WHEN 163266 THEN ''This health facility'' ELSE '''' END), NULL)) AS referral_to, ',
'MAX(IF(o.concept_id=159495, o.value_text, NULL)) AS other_facility, ',
'MAX(IF(o.concept_id=162724, o.value_text, NULL)) AS this_facility, ',
'e.voided ',
'FROM encounter e ',
'  INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
'  INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''e958f902-64df-4819-afd4-7fb061f59308'' ',
'  LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (164174,160632,165104,162737,1651,1640,162477,1655,1000075,1896,1272,162724,160433,164181,163145,159495,2031533) AND o.voided = 0 ',
'WHERE e.voided = 0 AND (',
' e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
' e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
' e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
' o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
' o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
') ',
'GROUP BY e.encounter_id ',
'ORDER BY e.patient_id ',
'ON DUPLICATE KEY UPDATE ',
' visit_date=VALUES(visit_date), provider=VALUES(provider), visit_type=VALUES(visit_type), therapy_ordered=VALUES(therapy_ordered), other_therapy_ordered=VALUES(other_therapy_ordered), ',
' counselling_ordered=VALUES(counselling_ordered), other_counselling_ordered=VALUES(other_counselling_ordered), procedures_prescribed=VALUES(procedures_prescribed), procedures_ordered=VALUES(procedures_ordered), ',
' patient_outcome=VALUES(patient_outcome), diagnosis_category=VALUES(diagnosis_category), general_examination=VALUES(general_examination), admission_needed=VALUES(admission_needed), date_of_patient_admission=VALUES(date_of_patient_admission), ',
' admission_reason=VALUES(admission_reason), admission_type=VALUES(admission_type), priority_of_admission=VALUES(priority_of_admission), admission_ward=VALUES(admission_ward), hospital_stay=VALUES(hospital_stay), ',
' referral_needed=VALUES(referral_needed), referral_ordered=VALUES(referral_ordered), referral_to=VALUES(referral_to), other_facility=VALUES(other_facility), this_facility=VALUES(this_facility), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing Clinical Encounter. Target: ', @target_table, ' Time: ', NOW()) AS status;
END $$



-- sql

-- --------------------------------------
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- PROCEDURE: sp_update_etl_pep_management_survivor
-- Purpose: update tenant-aware etl_pep_management_survivor
-- Tenant-aware: calls sp_set_tenant_session_vars() and uses dynamic INSERT target
-- --------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_pep_management_survivor $$
CREATE PROCEDURE sp_update_etl_pep_management_survivor(IN last_update_time DATETIME)
BEGIN
CALL sp_set_tenant_session_vars();

SET @target_table = CONCAT(@etl_schema, '.etl_pep_management_survivor');

  SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, visit_id, encounter_id, uuid, location_id, provider, visit_date, prc_number, incident_reporting_date, type_of_violence, disabled, other_type_of_violence, type_of_assault, other_type_of_assault, incident_date, perpetrator_identity, survivor_relation_to_perpetrator, perpetrator_compulsory_HIV_test_done, perpetrator_compulsory_HIV_test_result, perpetrator_file_number, survivor_state, clothing_state, genitalia_examination, other_injuries, high_vaginal_or_anal_swab, rpr_vdrl, survivor_hiv_test_result, given_pep, referred_to_psc, pdt, emergency_contraception_issued, reason_emergency_contraception_not_issued, sti_prophylaxis_and_treatment, reason_sti_prophylaxis_not_issued, pep_regimen_issued, reason_pep_regimen_not_issued, starter_pack_given, date_given_pep, HBsAG_result, LFTs_ALT, RFTs_creatinine, other_tests, voided',
    ') ',
    'SELECT ',
      'e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator, DATE(e.encounter_datetime) AS visit_date, ',
      'MAX(IF(o.concept_id = 1646, o.value_text, NULL)) AS prc_number, ',
      'MAX(IF(o.concept_id = 166848, o.value_datetime, NULL)) AS incident_reporting_date, ',
      'MAX(IF(o.concept_id = 165205, o.value_coded, NULL)) AS type_of_violence, ',
      'MAX(IF(o.concept_id = 162558, o.value_coded, NULL)) AS disabled, ',
      'MAX(IF(o.concept_id = 165138, o.value_text, NULL)) AS other_type_of_violence, ',
      'CONCAT_WS('','', NULLIF(MAX(IF(o.concept_id=123160 AND o.value_coded = 166060, ''Oral'', '''')), ''''), NULLIF(MAX(IF(o.concept_id=123160 AND o.value_coded = 123385, ''Vaginal'', '''')), ''''), NULLIF(MAX(IF(o.concept_id=123160 AND o.value_coded = 148895, ''Anal'', '''')), ''''), NULLIF(MAX(IF(o.concept_id=123160 AND o.value_coded = 5622, ''Other'', '''')), '''')) AS type_of_assault, ',
      'MAX(IF(o.concept_id = 164879, o.value_text, NULL)) AS other_type_of_assault, ',
      'MAX(IF(o.concept_id = 165349, o.value_datetime, NULL)) AS incident_date, ',
      'MAX(IF(o.concept_id = 165230, o.value_text, NULL)) AS perpetrator_identity, ',
      'MAX(IF(o.concept_id = 1530, o.value_coded, NULL)) AS survivor_relation_to_perpetrator, ',
      'MAX(IF(o.concept_id = 164848, o.value_coded, NULL)) AS perpetrator_compulsory_HIV_test_done, ',
      'MAX(IF(o.concept_id = 159427, o.value_coded, NULL)) AS perpetrator_compulsory_HIV_test_result, ',
      'MAX(IF(o.concept_id = 1639, o.value_numeric, NULL)) AS perpetrator_file_number, ',
      'MAX(IF(o.concept_id = 163042, o.value_text, NULL)) AS survivor_state, ',
      'MAX(IF(o.concept_id = 163045, o.value_text, NULL)) AS clothing_state, ',
      'MAX(IF(o.concept_id = 160971, o.value_text, NULL)) AS genitalia_examination, ',
      'MAX(IF(o.concept_id = 165092, o.value_text, NULL)) AS other_injuries, ',
      'MAX(IF(o.concept_id = 166364, o.value_text, NULL)) AS high_vaginal_or_anal_swab, ',
      'MAX(IF(o.concept_id = 299, o.value_coded, NULL)) AS rpr_vdrl, ',
      'MAX(IF(o.concept_id = 163760, o.value_coded, NULL)) AS survivor_hiv_test_result, ',
      'MAX(IF(o.concept_id = 165171, o.value_coded, NULL)) AS given_pep, ',
      'MAX(IF(o.concept_id = 165270, o.value_coded, NULL)) AS referred_to_psc, ',
      'MAX(IF(o.concept_id = 167229, o.value_coded, NULL)) AS pdt, ',
      'MAX(IF(o.concept_id = 165167, o.value_coded, NULL)) AS emergency_contraception_issued, ',
      'MAX(IF(o.concept_id = 160138, o.value_text, NULL)) AS reason_emergency_contraception_not_issued, ',
      'MAX(IF(o.concept_id = 165200, o.value_coded, NULL)) AS sti_prophylaxis_and_treatment, ',
      'MAX(IF(o.concept_id = 160953, o.value_text, NULL)) AS reason_sti_prophylaxis_not_issued, ',
      'MAX(IF(o.concept_id = 164845, o.value_coded, NULL)) AS pep_regimen_issued, ',
      'MAX(IF(o.concept_id = 160954, o.value_text, NULL)) AS reason_pep_regimen_not_issued, ',
      'MAX(IF(o.concept_id = 1263, o.value_coded, NULL)) AS starter_pack_given, ',
      'MAX(IF(o.concept_id = 166865, o.value_datetime, NULL)) AS date_given_pep, ',
      'MAX(IF(o.concept_id = 161472, o.value_coded, NULL)) AS HBsAG_result, ',
      'MAX(IF(o.concept_id = 654, o.value_datetime, NULL)) AS LFTs_ALT, ',
      'MAX(IF(o.concept_id = 790, o.value_numeric, NULL)) AS RFTs_creatinine, ',
      'MAX(IF(o.concept_id = 160987, o.value_text, NULL)) AS other_tests, ',
      'e.voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''f44b2405-226b-47c4-b98f-b826ea4725ae'' ',
    'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (1646,166848,165205,165138,123160,164879,165349,165230,1530,164848,159427,1639,163042,163045,160971,162558,165092,166364,299,163760,165171,165270,167229,165167,160138,165200,160953,164845,160954,1263,166865,161472,654,790,160987) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider), visit_date=VALUES(visit_date), prc_number=VALUES(prc_number), incident_reporting_date=VALUES(incident_reporting_date), ',
      'type_of_violence=VALUES(type_of_violence), disabled=VALUES(disabled), other_type_of_violence=VALUES(other_type_of_violence), type_of_assault=VALUES(type_of_assault), other_type_of_assault=VALUES(other_type_of_assault), ',
      'incident_date=VALUES(incident_date), perpetrator_identity=VALUES(perpetrator_identity), survivor_relation_to_perpetrator=VALUES(survivor_relation_to_perpetrator), perpetrator_compulsory_HIV_test_done=VALUES(perpetrator_compulsory_HIV_test_done), perpetrator_compulsory_HIV_test_result=VALUES(perpetrator_compulsory_HIV_test_result), perpetrator_file_number=VALUES(perpetrator_file_number), ',
      'survivor_state=VALUES(survivor_state), clothing_state=VALUES(clothing_state), genitalia_examination=VALUES(genitalia_examination), other_injuries=VALUES(other_injuries), high_vaginal_or_anal_swab=VALUES(high_vaginal_or_anal_swab), rpr_vdrl=VALUES(rpr_vdrl), survivor_hiv_test_result=VALUES(survivor_hiv_test_result), ',
      'given_pep=VALUES(given_pep), referred_to_psc=VALUES(referred_to_psc), pdt=VALUES(pdt), emergency_contraception_issued=VALUES(emergency_contraception_issued), reason_emergency_contraception_not_issued=VALUES(reason_emergency_contraception_not_issued), sti_prophylaxis_and_treatment=VALUES(sti_prophylaxis_and_treatment), reason_sti_prophylaxis_not_issued=VALUES(reason_sti_prophylaxis_not_issued), ',
      'pep_regimen_issued=VALUES(pep_regimen_issued), reason_pep_regimen_not_issued=VALUES(reason_pep_regimen_not_issued), starter_pack_given=VALUES(starter_pack_given), date_given_pep=VALUES(date_given_pep), HBsAG_result=VALUES(HBsAG_result), LFTs_ALT=VALUES(LFTs_ALT), RFTs_creatinine=VALUES(RFTs_creatinine), other_tests=VALUES(other_tests), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed updating PEP management survivor. Target: ', @target_table, ' Time: ', NOW()) AS status;
END $$




sql
-- --------------------------------------
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- PROCEDURE: sp_update_etl_sgbv_pep_followup
-- Purpose: populate tenant-aware etl_sgbv_pep_followup
-- Tenant-aware: calls sp_set_tenant_session_vars() and uses dynamic INSERT target
-- --------------------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_sgbv_pep_followup $$
CREATE PROCEDURE sp_update_etl_sgbv_pep_followup(IN last_update_time DATETIME)
BEGIN
  -- resolve tenant schema (expects sp_set_tenant_session_vars to set @etl_schema)
CALL sp_set_tenant_session_vars();

SET @target_table = CONCAT(@etl_schema, '.etl_sgbv_pep_followup');

SELECT CONCAT('Processing SGBV PEP followup -> ', @target_table) AS status;

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, visit_id, encounter_id, uuid, location_id, provider, visit_date, visit_number, ',
      'pep_completed, reason_pep_not_completed, hiv_test_done, hiv_test_result, pdt_test_done, pdt_test_result, ',
      'HBsAG_test_done, HBsAG_test_result, lfts_alt, rfts_creatinine, three_month_post_exposure_HIV_serology_result, ',
      'patient_assessment, voided',
    ') ',
    'SELECT ',
      'e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator, DATE(e.encounter_datetime) AS visit_date, ',
      'MAX(IF(o.concept_id = 1724, o.value_coded, NULL)) AS visit_number, ',
      'MAX(IF(o.concept_id = 165171, o.value_coded, NULL)) AS pep_completed, ',
      'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS reason_pep_not_completed, ',
      'MAX(IF(o.concept_id = 1356, o.value_coded, NULL)) AS hiv_test_done, ',
      'MAX(IF(o.concept_id = 159427, o.value_coded, NULL)) AS hiv_test_result, ',
      'MAX(IF(o.concept_id = 163951, o.value_coded, NULL)) AS pdt_test_done, ',
      'MAX(IF(o.concept_id = 167229, o.value_coded, NULL)) AS pdt_test_result, ',
      'MAX(IF(o.concept_id = 161472 AND o.value_coded IN (1065,1066), o.value_coded, NULL)) AS HBsAG_test_done, ',
      'MAX(IF(o.concept_id = 165384, o.value_coded, NULL)) AS HBsAG_test_result, ',
      'MAX(IF(o.concept_id = 654, o.value_numeric, NULL)) AS lfts_alt, ',
      'MAX(IF(o.concept_id = 790, o.value_coded, NULL)) AS rfts_creatinine, ',
      'MAX(IF(o.concept_id = 161472 AND o.value_coded IN (703,664), o.value_coded, NULL)) AS three_month_post_exposure_HIV_serology_result, ',
      'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS patient_assessment, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''155ccbe2-a33f-4a58-8ce6-57a7372071ee'' ',
      'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (1724,165171,161011,1356,159427,163951,167229,161472,165384,654,790,160632) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider), ',
      'visit_date=VALUES(visit_date), ',
      'visit_number=VALUES(visit_number), ',
      'pep_completed=VALUES(pep_completed), ',
      'reason_pep_not_completed=VALUES(reason_pep_not_completed), ',
      'hiv_test_done=VALUES(hiv_test_done), ',
      'hiv_test_result=VALUES(hiv_test_result), ',
      'pdt_test_done=VALUES(pdt_test_done), ',
      'pdt_test_result=VALUES(pdt_test_result), ',
      'HBsAG_test_done=VALUES(HBsAG_test_done), ',
      'HBsAG_test_result=VALUES(HBsAG_test_result), ',
      'lfts_alt=VALUES(lfts_alt), ',
      'rfts_creatinine=VALUES(rfts_creatinine), ',
      'three_month_post_exposure_HIV_serology_result=VALUES(three_month_post_exposure_HIV_serology_result), ',
      'patient_assessment=VALUES(patient_assessment), ',
      'voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed updating SGBV PEP followup -> ', @target_table, ' Time: ', NOW()) AS status;
END $$





-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- PROCEDURE: sp_update_etl_sgbv_post_rape_care
-- Purpose: populate tenant-aware etl_sgbv_post_rape_care
-- Tenant-aware: calls sp_set_tenant_session_vars() and uses dynamic INSERT target
DROP PROCEDURE IF EXISTS sp_update_etl_sgbv_post_rape_care $$
CREATE PROCEDURE sp_update_etl_sgbv_post_rape_care(IN last_update_time DATETIME)
BEGIN
  -- resolve tenant schema variable @etl_schema
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_sgbv_post_rape_care');

SELECT CONCAT('Processing SGBV post rape care -> ', @target_table) AS status;

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id,visit_id,encounter_id,uuid,location_id,provider,visit_date,examination_date,incident_date,number_of_perpetrators,',
      'is_perpetrator_known,survivor_relation_to_perpetrator,county,sub_county,landmark,observation_on_chief_complaint,chief_complaint_report,',
      'circumstances_around_incident,type_of_sexual_violence,other_type_of_sexual_violence,use_of_condoms,prior_attendance_to_health_facility,',
      'attended_health_facility_name,date_attended_health_facility,treated_at_facility,given_referral_notes,incident_reported_to_police,',
      'police_station_name,police_report_date,medical_or_surgical_history,additional_info_from_survivor,physical_examination,parity_term,',
      'parity_abortion,on_contraception,known_pregnancy,date_of_last_consensual_sex,systolic,diastolic,demeanor,changed_clothes,',
      'state_of_clothes,means_clothes_transported,details_about_clothes_transport,clothes_handed_to_police,survivor_went_to_toilet,',
      'survivor_bathed,bath_details,survivor_left_marks_on_perpetrator,details_of_marks_on_perpetrator,physical_injuries,details_outer_genitalia,',
      'details_vagina,details_hymen,details_anus,significant_orifice,pep_first_dose,ecp_given,stitching_done,stitching_notes,treated_for_sti,',
      'sti_treatment_remarks,other_medications,referred_to,web_prep_microscopy,samples_packed,examining_officer,voided) ',
    'SELECT ',
      'e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator, DATE(e.encounter_datetime) AS visit_date, ',
      'MAX(IF(o.concept_id = 159948, o.value_datetime, NULL)) AS examination_date, ',
      'MAX(IF(o.concept_id = 162869, o.value_datetime, NULL)) AS incident_date, ',
      'MAX(IF(o.concept_id = 1639, o.value_numeric, NULL)) AS number_of_perpetrators, ',
      'MAX(IF(o.concept_id = 165229, o.value_coded, NULL)) AS is_perpetrator_known, ',
      'MAX(IF(o.concept_id = 167214, o.value_text, NULL)) AS survivor_relation_to_perpetrator, ',
      'MAX(IF(o.concept_id = 167131, o.value_text, NULL)) AS county, ',
      'MAX(IF(o.concept_id = 161564, o.value_text, NULL)) AS sub_county, ',
      'MAX(IF(o.concept_id = 159942, o.value_text, NULL)) AS landmark, ',
      'MAX(IF(o.concept_id = 160945, o.value_text, NULL)) AS observation_on_chief_complaint, ',
      'MAX(IF(o.concept_id = 166846, o.value_text, NULL)) AS chief_complaint_report, ',
      'MAX(IF(o.concept_id = 160303, o.value_text, NULL)) AS circumstances_around_incident, ',
      'CONCAT_WS('','', NULLIF(MAX(IF(o.concept_id=123160 AND o.value_coded = 166060, ''Oral'', '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=123160 AND o.value_coded = 123385, ''Vaginal'', '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=123160 AND o.value_coded = 148895, ''Anal'', '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=123160 AND o.value_coded = 5622, ''Other'', '''')), '''')) AS type_of_sexual_violence, ',
      'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS other_type_of_sexual_violence, ',
      'MAX(IF(o.concept_id = 1357, o.value_coded, NULL)) AS use_of_condoms, ',
      'MAX(IF(o.concept_id = 166484, o.value_coded, NULL)) AS prior_attendance_to_health_facility, ',
      'MAX(IF(o.concept_id = 162724, o.value_text, NULL)) AS attended_health_facility_name, ',
      'MAX(IF(o.concept_id = 164093, o.value_datetime, NULL)) AS date_attended_health_facility, ',
      'MAX(IF(o.concept_id = 165052, o.value_coded, NULL)) AS treated_at_facility, ',
      'MAX(IF(o.concept_id = 165152, o.value_coded, NULL)) AS given_referral_notes, ',
      'MAX(IF(o.concept_id = 165193, o.value_coded, NULL)) AS incident_reported_to_police, ',
      'MAX(IF(o.concept_id = 161550, o.value_text, NULL)) AS police_station_name, ',
      'MAX(IF(o.concept_id = 165144, o.value_datetime, NULL)) AS police_report_date, ',
      'MAX(IF(o.concept_id = 160221, o.value_text, NULL)) AS medical_or_surgical_history, ',
      'MAX(IF(o.concept_id = 163677, o.value_text, NULL)) AS additional_info_from_survivor, ',
      'MAX(IF(o.concept_id = 1391, o.value_text, NULL)) AS physical_examination, ',
      'MAX(IF(o.concept_id = 160080, o.value_numeric, NULL)) AS parity_term, ',
      'MAX(IF(o.concept_id = 1823, o.value_numeric, NULL)) AS parity_abortion, ',
      'MAX(IF(o.concept_id = 163400, o.value_coded, NULL)) AS on_contraception, ',
      'MAX(IF(o.concept_id = 5272, o.value_coded, NULL)) AS known_pregnancy, ',
      'MAX(IF(o.concept_id = 160753, o.value_datetime, NULL)) AS date_of_last_consensual_sex, ',
      'MAX(IF(o.concept_id = 5085, o.value_numeric, NULL)) AS systolic, ',
      'MAX(IF(o.concept_id = 5086, o.value_numeric, NULL)) AS diastolic, ',
      'MAX(IF(o.concept_id = 62056, o.value_coded, NULL)) AS demeanor, ',
      'MAX(IF(o.concept_id = 165171, o.value_coded, NULL)) AS changed_clothes, ',
      'MAX(IF(o.concept_id = 163104, o.value_text, NULL)) AS state_of_clothes, ',
      'MAX(IF(o.concept_id = 165171, o.value_coded, NULL)) AS means_clothes_transported, ',
      'MAX(IF(o.concept_id = 166363, o.value_text, NULL)) AS details_about_clothes_transport, ',
      'MAX(IF(o.concept_id = 165180, o.value_coded, NULL)) AS clothes_handed_to_police, ',
      'MAX(IF(o.concept_id = 160258, o.value_coded, NULL)) AS survivor_went_to_toilet, ',
      'MAX(IF(o.concept_id = 162997, o.value_coded, NULL)) AS survivor_bathed, ',
      'MAX(IF(o.concept_id = 163048, o.value_text, NULL)) AS bath_details, ',
      'MAX(IF(o.concept_id = 165241, o.value_coded, NULL)) AS survivor_left_marks_on_perpetrator, ',
      'MAX(IF(o.concept_id = 161031, o.value_text, NULL)) AS details_of_marks_on_perpetrator, ',
      'MAX(IF(o.concept_id = 165035, o.value_text, NULL)) AS physical_injuries, ',
      'MAX(IF(o.concept_id = 160971, o.value_text, NULL)) AS details_outer_genitalia, ',
      'MAX(IF(o.concept_id = 160969, o.value_text, NULL)) AS details_vagina, ',
      'MAX(IF(o.concept_id = 160981, o.value_text, NULL)) AS details_hymen, ',
      'MAX(IF(o.concept_id = 160962, o.value_text, NULL)) AS details_anus, ',
      'MAX(IF(o.concept_id = 160943, o.value_text, NULL)) AS significant_orifice, ',
      'MAX(IF(o.concept_id = 165060, o.value_coded, NULL)) AS pep_first_dose, ',
      'MAX(IF(o.concept_id = 374, o.value_coded, NULL)) AS ecp_given, ',
      'MAX(IF(o.concept_id = 1670, o.value_coded, NULL)) AS stitching_done, ',
      'MAX(IF(o.concept_id = 165440, o.value_text, NULL)) AS stitching_notes, ',
      'MAX(IF(o.concept_id = 165200, o.value_coded, NULL)) AS treated_for_sti, ',
      'MAX(IF(o.concept_id = 167214, o.value_text, NULL)) AS sti_treatment_remarks, ',
      'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS other_medications, ',
      'CONCAT_WS('','', NULLIF(MAX(IF(o.concept_id=160632 AND o.value_coded = 165192, ''Police Station'', '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=160632 AND o.value_coded = 167254, ''Safe Shelter'', '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=160632 AND o.value_coded = 5460, ''Trauma Counselling'', '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=160632 AND o.value_coded = 160542, ''OPD/CCC/HIV clinic'', '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=160632 AND o.value_coded = 1370, ''HIV Test'', '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=160632 AND o.value_coded = 164422, ''Laboratory'', '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=160632 AND o.value_coded = 135914, ''Legal'', '''')), ''''), ',
        'NULLIF(MAX(IF(o.concept_id=160632 AND o.value_coded = 5622, ''Other'', '''')), '''')) AS referred_to, ',
      'MAX(IF(o.concept_id = 164217, o.value_coded, NULL)) AS web_prep_microscopy, ',
      'MAX(IF(o.concept_id = 165435, o.value_text, NULL)) AS samples_packed, ',
      'MAX(IF(o.concept_id = 165225, o.value_text, NULL)) AS examining_officer, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''c46aa4fd-8a5a-4675-90a7-a6f2119f61d8'' ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (159948,162869,1639,165229,167214,167131,161564,159942,160945,166846,160303,123160,161011,1357,166484,162724,164093,165052,165152,165193,161550,165144,160221,163677,1391,160080,1823,163400,5272,160753,5085,5086,162056,165171,163104,161239,166363,165180,160258,162997,163048,165241,161031,165035,160971,160969,160981,160962,160943,165060,374,1670,165440,165200,167214,160632,1272,164217,165435,165225) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND ( ',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' ) ',
    'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider), visit_date=VALUES(visit_date), examination_date=VALUES(examination_date), incident_date=VALUES(incident_date), ',
      'number_of_perpetrators=VALUES(number_of_perpetrators), is_perpetrator_known=VALUES(is_perpetrator_known), survivor_relation_to_perpetrator=VALUES(survivor_relation_to_perpetrator), ',
      'county=VALUES(county), sub_county=VALUES(sub_county), landmark=VALUES(landmark), observation_on_chief_complaint=VALUES(observation_on_chief_complaint), ',
      'chief_complaint_report=VALUES(chief_complaint_report), circumstances_around_incident=VALUES(circumstances_around_incident), type_of_sexual_violence=VALUES(type_of_sexual_violence), ',
      'other_type_of_sexual_violence=VALUES(other_type_of_sexual_violence), use_of_condoms=VALUES(use_of_condoms), prior_attendance_to_health_facility=VALUES(prior_attendance_to_health_facility), ',
      'attended_health_facility_name=VALUES(attended_health_facility_name), date_attended_health_facility=VALUES(date_attended_health_facility), treated_at_facility=VALUES(treated_at_facility), ',
      'given_referral_notes=VALUES(given_referral_notes), incident_reported_to_police=VALUES(incident_reported_to_police), police_station_name=VALUES(police_station_name), ',
      'police_report_date=VALUES(police_report_date), medical_or_surgical_history=VALUES(medical_or_surgical_history), additional_info_from_survivor=VALUES(additional_info_from_survivor), ',
      'physical_examination=VALUES(physical_examination), parity_term=VALUES(parity_term), parity_abortion=VALUES(parity_abortion), on_contraception=VALUES(on_contraception), ',
      'known_pregnancy=VALUES(known_pregnancy), date_of_last_consensual_sex=VALUES(date_of_last_consensual_sex), systolic=VALUES(systolic), diastolic=VALUES(diastolic), ',
      'demeanor=VALUES(demeanor), changed_clothes=VALUES(changed_clothes), state_of_clothes=VALUES(state_of_clothes), means_clothes_transported=VALUES(means_clothes_transported), ',
      'details_about_clothes_transport=VALUES(details_about_clothes_transport), clothes_handed_to_police=VALUES(clothes_handed_to_police), survivor_went_to_toilet=VALUES(survivor_went_to_toilet), ',
      'survivor_bathed=VALUES(survivor_bathed), bath_details=VALUES(bath_details), survivor_left_marks_on_perpetrator=VALUES(survivor_left_marks_on_perpetrator), ',
      'details_of_marks_on_perpetrator=VALUES(details_of_marks_on_perpetrator), physical_injuries=VALUES(physical_injuries), details_outer_genitalia=VALUES(details_outer_genitalia), ',
      'details_vagina=VALUES(details_vagina), details_hymen=VALUES(details_hymen), details_anus=VALUES(details_anus), significant_orifice=VALUES(significant_orifice), ',
      'pep_first_dose=VALUES(pep_first_dose), ecp_given=VALUES(ecp_given), stitching_done=VALUES(stitching_done), stitching_notes=VALUES(stitching_notes), ',
      'treated_for_sti=VALUES(treated_for_sti), sti_treatment_remarks=VALUES(sti_treatment_remarks), other_medications=VALUES(other_medications), referred_to=VALUES(referred_to), ',
      'web_prep_microscopy=VALUES(web_prep_microscopy), samples_packed=VALUES(samples_packed), examining_officer=VALUES(examining_officer), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed updating SGBV post rape care -> ', @target_table, ' Time: ', NOW()) AS status;
END $$



-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- PROCEDURE: sp_update_etl_gbv_physical_emotional_abuse
-- Purpose: populate tenant-aware etl_gbv_physical_emotional_abuse
-- Tenant-aware: calls sp_set_tenant_session_vars() and uses dynamic INSERT target
DROP PROCEDURE IF EXISTS sp_update_etl_gbv_physical_emotional_abuse $$
CREATE PROCEDURE sp_update_etl_gbv_physical_emotional_abuse(IN last_update_time DATETIME)
BEGIN
  -- resolve tenant schema; implementation must set @etl_schema
CALL sp_set_tenant_session_vars();

SET @target_table = CONCAT(@etl_schema, '.etl_gbv_physical_emotional_abuse');

  SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (patient_id, visit_id, encounter_id, uuid, location_id, provider, visit_date, gbv_number, referred_from, entry_point, other_referral_source, type_of_violence, date_of_incident, trauma_counselling, trauma_counselling_comments, referred_to, other_referral, voided) ',
    'SELECT e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator, DATE(e.encounter_datetime) AS visit_date, ',
    'MAX(IF(o.concept_id = 1646, o.value_text, NULL)) AS gbv_number, ',
    'MAX(IF(o.concept_id = 1272, o.value_coded, NULL)) AS referred_from, ',
    'MAX(IF(o.concept_id = 160540, o.value_coded, NULL)) AS entry_point, ',
    'MAX(IF(o.concept_id = 165092, o.value_text, NULL)) AS other_referral_source, ',
    'CONCAT_WS('','', NULLIF(MAX(IF(o.concept_id = 165205 AND o.value_coded = 167243, ''Intimate Partner Violence (IPV)'','''')),''''), NULLIF(MAX(IF(o.concept_id = 165205 AND o.value_coded = 117510, ''Emotional Violence'','''')),''''), NULLIF(MAX(IF(o.concept_id = 165205 AND o.value_coded = 158358, ''Physical Violence'','''')),'''')) AS type_of_violence, ',
    'MAX(IF(o.concept_id = 162869, o.value_datetime, NULL)) AS date_of_incident, ',
    'MAX(IF(o.concept_id = 165184, o.value_coded, NULL)) AS trauma_counselling, ',
    'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS trauma_counselling_comments, ',
    'CONCAT_WS('','', ',
      'NULLIF(MAX(IF(o.concept_id = 1272 AND o.value_coded = 165192, ''Police Station'','''')),''''), ',
      'NULLIF(MAX(IF(o.concept_id = 1272 AND o.value_coded = 165227, ''Safe Space'','''')),''''), ',
      'NULLIF(MAX(IF(o.concept_id = 1272 AND o.value_coded = 1691, ''Post-exposure Prophylaxis'','''')),''''), ',
      'NULLIF(MAX(IF(o.concept_id = 1272 AND o.value_coded = 1459, ''HIV Testing/Re-testing services'','''')),''''), ',
      'NULLIF(MAX(IF(o.concept_id = 1272 AND o.value_coded = 1610, ''Care and Treatment'','''')),''''), ',
      'NULLIF(MAX(IF(o.concept_id = 1272 AND o.value_coded = 5486, ''Alternative dispute resolution'','''')),''''), ',
      'NULLIF(MAX(IF(o.concept_id = 1272 AND o.value_coded = 160546, ''STI Screening/treatment'','''')),''''), ',
      'NULLIF(MAX(IF(o.concept_id = 1272 AND o.value_coded = 1606, ''Radiology services (X-ray)'','''')),''''), ',
      'NULLIF(MAX(IF(o.concept_id = 1272 AND o.value_coded = 5490, ''Psychosocial Support'','''')),''''), ',
      'NULLIF(MAX(IF(o.concept_id = 1272 AND o.value_coded = 135914, ''Legal Support'','''')),''''), ',
      'NULLIF(MAX(IF(o.concept_id = 1272 AND o.value_coded = 5622, ''Other'','''')),'''')) AS referred_to, ',
    'MAX(IF(o.concept_id = 60632, o.value_text, NULL)) AS other_referral, ',
    'e.voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''a0943862-f0fe-483d-9f11-44f62abae063'' ',
    'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (1646,1272,160540,165092,165205,162869,165184,161011,60632) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
    'ON DUPLICATE KEY UPDATE provider=VALUES(provider), visit_date=VALUES(visit_date), gbv_number=VALUES(gbv_number), referred_from=VALUES(referred_from), entry_point=VALUES(entry_point), other_referral_source=VALUES(other_referral_source), type_of_violence=VALUES(type_of_violence), date_of_incident=VALUES(date_of_incident), trauma_counselling=VALUES(trauma_counselling), trauma_counselling_comments=VALUES(trauma_counselling_comments), referred_to=VALUES(referred_to), other_referral=VALUES(other_referral), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed updating GBV physical and emotional abuse -> ', @target_table, ' Time: ', NOW()) AS status;
END $$


-- sql
-- --------------------------------------
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- PROCEDURE: sp_update_etl_family_planning
-- Purpose: populate tenant-aware etl_family_planning
-- Tenant-aware: calls sp_set_tenant_session_vars() and builds dynamic INSERT target
-- --------------------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_family_planning $$
CREATE PROCEDURE sp_update_etl_family_planning(IN last_update_time DATETIME)
BEGIN
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_family_planning');

SELECT CONCAT('Processing Family planning -> ', @target_table) AS status;

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, visit_id, encounter_id, uuid, location_id, provider, visit_date, ',
      'first_user_of_contraceptive, counselled_on_fp, contraceptive_dispensed, ',
      'type_of_visit_for_method, type_of_service, quantity_dispensed, reasons_for_larc_removal, ',
      'other_reasons_for_larc_removal, counselled_on_natural_fp, circle_beads_given, ',
      'receiving_postpartum_fp, experienced_intimate_partner_violence, referred_for_fp, ',
      'referred_to, referred_from, reasons_for_referral, voided',
    ') ',
    'SELECT ',
      'e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator, DATE(e.encounter_datetime) AS visit_date, ',
      'MAX(IF(o.concept_id = 160653, o.value_coded, NULL)) AS first_user_of_contraceptive, ',
      'MAX(IF(o.concept_id = 1382, o.value_coded, NULL)) AS counselled_on_fp, ',
      'MAX(IF(o.concept_id = 374, o.value_coded, NULL)) AS contraceptive_dispensed, ',
      'MAX(IF(o.concept_id = 167523, o.value_coded, NULL)) AS type_of_visit_for_method, ',
      'MAX(IF(o.concept_id = 1386, o.value_coded, NULL)) AS type_of_service, ',
      'MAX(IF(o.concept_id = 166864, o.value_numeric, NULL)) AS quantity_dispensed, ',
      'MAX(IF(o.concept_id = 164901, o.value_coded, NULL)) AS reasons_for_larc_removal, ',
      'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS other_reasons_for_larc_removal, ',
      'MAX(IF(o.concept_id = 1379, o.value_coded, NULL)) AS counselled_on_natural_fp, ',
      'MAX(IF(o.concept_id = 166866, o.value_coded, NULL)) AS circle_beads_given, ',
      'MAX(IF(o.concept_id = 1177, o.value_coded, NULL)) AS receiving_postpartum_fp, ',
      'MAX(IF(o.concept_id = 167255, o.value_coded, NULL)) AS experienced_intimate_partner_violence, ',
      'MAX(IF(o.concept_id = 166515, o.value_coded, NULL)) AS referred_for_fp, ',
      'MAX(IF(o.concept_id = 163145, o.value_coded, NULL)) AS referred_to, ',
      'MAX(IF(o.concept_id = 160481, o.value_coded, NULL)) AS referred_from, ',
      'MAX(IF(o.concept_id = 164359, o.value_text, NULL)) AS reasons_for_referral, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''a52c57d4-110f-4879-82ae-907b0d90add6'' ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (160653,1382,374,167523,1386,166864,164901,160632,1379,166866,1177,167255,163145,160481,164359) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider), ',
      'visit_date=VALUES(visit_date), ',
      'first_user_of_contraceptive=VALUES(first_user_of_contraceptive), ',
      'counselled_on_fp=VALUES(counselled_on_fp), ',
      'contraceptive_dispensed=VALUES(contraceptive_dispensed), ',
      'type_of_visit_for_method=VALUES(type_of_visit_for_method), ',
      'type_of_service=VALUES(type_of_service), ',
      'quantity_dispensed=VALUES(quantity_dispensed), ',
      'reasons_for_larc_removal=VALUES(reasons_for_larc_removal), ',
      'other_reasons_for_larc_removal=VALUES(other_reasons_for_larc_removal), ',
      'counselled_on_natural_fp=VALUES(counselled_on_natural_fp), ',
      'circle_beads_given=VALUES(circle_beads_given), ',
      'receiving_postpartum_fp=VALUES(receiving_postpartum_fp), ',
      'experienced_intimate_partner_violence=VALUES(experienced_intimate_partner_violence), ',
      'referred_for_fp=VALUES(referred_for_fp), ',
      'referred_to=VALUES(referred_to), ',
      'referred_from=VALUES(referred_from), ',
      'reasons_for_referral=VALUES(reasons_for_referral), ',
      'voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing Family planning -> ', @target_table, ' Time: ', NOW()) AS status;
END $$


-- --------------------------------------
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- PROCEDURE: sp_update_etl_physiotherapy
-- Purpose: populate tenant-aware etl_physiotherapy
-- Tenant-aware: calls sp_set_tenant_session_vars() and builds dynamic INSERT target


-- --------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_physiotherapy $$
CREATE PROCEDURE sp_update_etl_physiotherapy(IN last_update_time DATETIME)
BEGIN
  -- initialize tenant variables
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(@etl_schema, '.etl_physiotherapy');

  SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'patient_id, visit_id, encounter_id, uuid, location_id, provider, visit_date, visit_type, ',
      'referred_from, referred_from_department, referred_from_department_other, number_of_sessions, referral_reason, disorder_category, other_disorder_category, clinical_notes, pin_scale, affected_region, range_of_motion, strength_test, functional_assessment, assessment_finding, goals, planned_interventions, other_interventions, sessions_per_week, patient_outcome, referred_for, referred_to, transfer_to_facility, services_referred_for, date_of_admission, reason_for_admission, type_of_admission, priority_of_admission, admission_ward, duration_of_hospital_stay, voided',
    ') ',
    'SELECT ',
      'e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator, DATE(e.encounter_datetime) AS visit_date, ',
      'MAX(IF(o.concept_id = 164181, o.value_coded, NULL)) AS visit_type, ',
      'MAX(IF(o.concept_id = 160338, o.value_coded, NULL)) AS referred_from, ',
      'MAX(IF(o.concept_id = 160478, o.value_coded, NULL)) AS referred_from_department, ',
      'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS referred_from_department_other, ',
      'MAX(IF(o.concept_id = 164812, o.value_numeric, NULL)) AS number_of_sessions, ',
      'MAX(IF(o.concept_id = 162725, o.value_text, NULL)) AS referral_reason, ',
      'MAX(IF(o.concept_id = 1000485, o.value_coded, NULL)) AS disorder_category, ',
      'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS other_disorder_category, ',
      'MAX(IF(o.concept_id = 160629, o.value_text, NULL)) AS clinical_notes, ',
      'MAX(IF(o.concept_id = 1475, o.value_numeric, NULL)) AS pin_scale, ',
      'MAX(IF(o.concept_id = 160629, o.value_text, NULL)) AS affected_region, ',
      'MAX(IF(o.concept_id = 602, o.value_coded, NULL)) AS range_of_motion, ',
      'MAX(IF(o.concept_id = 165241, o.value_coded, NULL)) AS strength_test, ',
      'MAX(IF(o.concept_id = 163580, o.value_coded, NULL)) AS functional_assessment, ',
      'MAX(IF(o.concept_id = 165002, o.value_text, NULL)) AS assessment_finding, ',
      'MAX(IF(o.concept_id = 165250, o.value_text, NULL)) AS goals, ',
      'MAX(IF(o.concept_id = 163304, o.value_coded, NULL)) AS planned_interventions, ',
      'MAX(IF(o.concept_id = 165250, o.value_text, NULL)) AS other_interventions, ',
      'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS sessions_per_week, ',
      'MAX(IF(o.concept_id = 160433, o.value_coded, NULL)) AS patient_outcome, ',
      'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS referred_for, ',
      'MAX(IF(o.concept_id = 163145, o.value_text, NULL)) AS referred_to, ',
      'MAX(IF(o.concept_id = 159495, o.value_text, NULL)) AS transfer_to_facility, ',
      'MAX(IF(o.concept_id = 162724, o.value_text, NULL)) AS services_referred_for, ',
      'MAX(IF(o.concept_id = 1640, o.value_datetime, NULL)) AS date_of_admission, ',
      'MAX(IF(o.concept_id = 162879, o.value_text, NULL)) AS reason_for_admission, ',
      'MAX(IF(o.concept_id = 162477, o.value_coded, NULL)) AS type_of_admission, ',
      'MAX(IF(o.concept_id = 1655, o.value_coded, NULL)) AS priority_of_admission, ',
      'MAX(IF(o.concept_id = 1000075, o.value_coded, NULL)) AS admission_ward, ',
      'MAX(IF(o.concept_id = 1896, o.value_coded, NULL)) AS duration_of_hospital_stay, ',
      'e.voided ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''18c209ac-0787-4b51-b9aa-aa8b1581239c'' ',
      'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (164181,160338,160478,160632,164812,162725,1000485,160632,160629,1475,160629,602,165241,163580,165002,165250,163304,165250,161011,160433,163145,159495,162724,1640,162879,162477,1655,1000075,1896) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
      ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
    'ON DUPLICATE KEY UPDATE ',
      'provider = VALUES(provider), ',
      'visit_date = VALUES(visit_date), ',
      'visit_type = VALUES(visit_type), ',
      'referred_from = VALUES(referred_from), ',
      'referred_from_department = VALUES(referred_from_department), ',
      'referred_from_department_other = VALUES(referred_from_department_other), ',
      'number_of_sessions = VALUES(number_of_sessions), ',
      'referral_reason = VALUES(referral_reason), ',
      'disorder_category = VALUES(disorder_category), ',
      'other_disorder_category = VALUES(other_disorder_category), ',
      'clinical_notes = VALUES(clinical_notes), ',
      'pin_scale = VALUES(pin_scale), ',
      'affected_region = VALUES(affected_region), ',
      'range_of_motion = VALUES(range_of_motion), ',
      'strength_test = VALUES(strength_test), ',
      'functional_assessment = VALUES(functional_assessment), ',
      'assessment_finding = VALUES(assessment_finding), ',
      'goals = VALUES(goals), ',
      'planned_interventions = VALUES(planned_interventions), ',
      'other_interventions = VALUES(other_interventions), ',
      'sessions_per_week = VALUES(sessions_per_week), ',
      'patient_outcome = VALUES(patient_outcome), ',
      'referred_for = VALUES(referred_for), ',
      'referred_to = VALUES(referred_to), ',
      'transfer_to_facility = VALUES(transfer_to_facility), ',
      'services_referred_for = VALUES(services_referred_for), ',
      'date_of_admission = VALUES(date_of_admission), ',
      'reason_for_admission = VALUES(reason_for_admission), ',
      'type_of_admission = VALUES(type_of_admission), ',
      'priority_of_admission = VALUES(priority_of_admission), ',
      'admission_ward = VALUES(admission_ward), ',
      'duration_of_hospital_stay = VALUES(duration_of_hospital_stay), ',
      'voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing Physiotherapy -> ', @target_table, ' Time: ', NOW()) AS status;
END $$


-- --------------------------------------
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- PROCEDURE: sp_update_etl_psychiatry
-- Purpose: populate tenant-aware etl_psychiatry
-- Tenant-aware: calls sp_set_tenant_session_vars() and uses dynamic INSERT target
-- --------------------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_psychiatry $$
CREATE PROCEDURE sp_update_etl_psychiatry(IN last_update_time DATETIME)
BEGIN
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT('', etl_schema, '.etl_psychiatry');

  SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (patient_id, visit_id, encounter_id, uuid, location_id, provider, visit_date, visit_type, referred_from, referred_from_department, presenting_allegations, other_allegations, contact_with_TB_case, history_of_present_illness, surgical_history, type_of_surgery, surgery_date, on_medication, childhood_mistreatment, persistent_cruelty_meanness, physically_abused, sexually_abused, patient_occupation_history, reproductive_history, lmp_date, general_examination_findings, mental_status, attitude_and_behaviour, speech, mood, illusions, attention_concentration, memory_recall, judgement, insight, affect, thought_process, thought_content, hallucinations, orientation_status, management_plan, counselling_prescribed, patient_outcome, referred_to, facility_transferred_to, date_of_admission, reason_for_admission, type_of_admission, priority_of_admission, admission_ward, duration_of_hospital_stay, voided) ',
    'SELECT e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator, DATE(e.encounter_datetime) AS visit_date, ',
    'MAX(IF(o.concept_id = 164181, o.value_coded, NULL)) AS visit_type, ',
    'MAX(IF(o.concept_id = 160338, o.value_coded, NULL)) AS referred_from, ',
    'MAX(IF(o.concept_id = 160478, o.value_coded, NULL)) AS referred_from_department, ',
    'MAX(IF(o.concept_id = 5219, o.value_coded, NULL)) AS presenting_allegations, ',
    'MAX(IF(o.concept_id = 165250, o.value_text, NULL)) AS other_allegations, ',
    'MAX(IF(o.concept_id = 124068, o.value_coded, NULL)) AS contact_with_TB_case, ',
    'MAX(IF(o.concept_id = 1390, o.value_text, NULL)) AS history_of_present_illness, ',
    'MAX(IF(o.concept_id = 168148, o.value_coded, NULL)) AS surgical_history, ',
    'MAX(IF(o.concept_id = 166635, o.value_text, NULL)) AS type_of_surgery, ',
    'MAX(IF(o.concept_id = 160715, o.value_datetime, NULL)) AS surgery_date, ',
    'MAX(IF(o.concept_id = 159367, o.value_coded, NULL)) AS on_medication, ',
    'MAX(IF(o.concept_id = 165206, o.value_coded, NULL)) AS childhood_mistreatment, ',
    'MAX(IF(o.concept_id = 165241, o.value_coded, NULL)) AS persistent_cruelty_meanness, ',
    'MAX(IF(o.concept_id = 165034, o.value_coded, NULL)) AS physically_abused, ',
    'MAX(IF(o.concept_id = 123160, o.value_coded, NULL)) AS sexually_abused, ',
    'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS patient_occupation_history, ',
    'MAX(IF(o.concept_id = 160598, o.value_text, NULL)) AS reproductive_history, ',
    'MAX(IF(o.concept_id = 1427, o.value_datetime, NULL)) AS lmp_date, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 162737 AND o.value_coded = 1107, ''None'', NULL)), ',
      'MAX(IF(o.concept_id = 162737 AND o.value_coded = 136443, ''Jaundice'', NULL)), ',
      'MAX(IF(o.concept_id = 162737 AND o.value_coded = 460, ''Oedema'', NULL)), ',
      'MAX(IF(o.concept_id = 162737 AND o.value_coded = 5334, ''Oral thrush'', NULL)), ',
      'MAX(IF(o.concept_id = 162737 AND o.value_coded = 5245, ''Pallor'', NULL)), ',
      'MAX(IF(o.concept_id = 162737 AND o.value_coded = 140125, ''Finger Clubbing'', NULL)), ',
      'MAX(IF(o.concept_id = 162737 AND o.value_coded = 126952, ''Lymph Node Axillary'', NULL)), ',
      'MAX(IF(o.concept_id = 162737 AND o.value_coded = 143050, ''Cyanosis'', NULL)), ',
      'MAX(IF(o.concept_id = 162737 AND o.value_coded = 126939, ''Lymph Nodes Inguinal'', NULL)), ',
      'MAX(IF(o.concept_id = 162737 AND o.value_coded = 823, ''Wasting'', NULL)), ',
      'MAX(IF(o.concept_id = 162737 AND o.value_coded = 116334, ''Lethargic'', NULL))',
    ') AS general_examination_findings, ',
    'MAX(IF(o.concept_id = 167092, o.value_coded, NULL)) AS mental_status, ',
    'MAX(IF(o.concept_id = 167193, o.value_coded, NULL)) AS attitude_and_behaviour, ',
    'MAX(IF(o.concept_id = 167201, o.value_coded, NULL)) AS speech, ',
    'MAX(IF(o.concept_id = 167099, o.value_coded, NULL)) AS mood, ',
    'MAX(IF(o.concept_id = 167526, o.value_coded, NULL)) AS illusions, ',
    'MAX(IF(o.concept_id = 167203, o.value_coded, NULL)) AS attention_concentration, ',
    'MAX(IF(o.concept_id = 167321, o.value_coded, NULL)) AS memory_recall, ',
    'MAX(IF(o.concept_id = 167116, o.value_coded, NULL)) AS judgement, ',
    'MAX(IF(o.concept_id = 167115, o.value_coded, NULL)) AS insight, ',
    'MAX(IF(o.concept_id = 167101, o.value_coded, NULL)) AS affect, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 167106 AND o.value_coded = 16732, ''Logical'', NULL)), ',
      'MAX(IF(o.concept_id = 167106 AND o.value_coded = 167319, ''Illogical'', NULL)), ',
      'MAX(IF(o.concept_id = 167106 AND o.value_coded = 167318, ''Pressured'', NULL)), ',
      'MAX(IF(o.concept_id = 167106 AND o.value_coded = 167137, ''Disorganized'', NULL))',
    ') AS thought_process, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 167112 AND o.value_coded = 1115, ''Normal'', NULL)), ',
      'MAX(IF(o.concept_id = 167112 AND o.value_coded = 142600, ''Delusions'', NULL)), ',
      'MAX(IF(o.concept_id = 167112 AND o.value_coded = 125562, ''Suicidal'', NULL)), ',
      'MAX(IF(o.concept_id = 167112 AND o.value_coded = 114164, ''Phobias'', NULL)), ',
      'MAX(IF(o.concept_id = 167112 AND o.value_coded = 132613, ''Obssessions'', NULL)), ',
      'MAX(IF(o.concept_id = 167112 AND o.value_coded = 167117, ''Homicidal'', NULL))',
    ') AS thought_content, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 167181 AND o.value_coded = 148126, ''Auditory hallucinations'', NULL)), ',
      'MAX(IF(o.concept_id = 167181 AND o.value_coded = 132427, ''Olfactory Hallucinations'', NULL)), ',
      'MAX(IF(o.concept_id = 167181 AND o.value_coded = 125058, ''Tactile Hallucinations'', NULL)), ',
      'MAX(IF(o.concept_id = 167181 AND o.value_coded = 123069, ''Visual Hallucinations'', NULL)), ',
      'MAX(IF(o.concept_id = 167181 AND o.value_coded = 163747, ''Absent'', NULL))',
    ') AS hallucinations, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 167084 AND o.value_coded = 167083, ''Oriented to time'', NULL)), ',
      'MAX(IF(o.concept_id = 167084 AND o.value_coded = 167082, ''Oriented to place'', NULL)), ',
      'MAX(IF(o.concept_id = 167084 AND o.value_coded = 167081, ''Oriented to person'', NULL)), ',
      'MAX(IF(o.concept_id = 167084 AND o.value_coded = 163747, ''Absent'', NULL))',
    ') AS orientation_status, ',
    'MAX(IF(o.concept_id = 163104, o.value_text, NULL)) AS management_plan, ',
    'CONCAT_WS('','', ',
      'MAX(IF(o.concept_id = 165104 AND o.value_coded = 1107, ''None'', NULL)), ',
      'MAX(IF(o.concept_id = 165104 AND o.value_coded = 156277, ''Family Counseling'', NULL)), ',
      'MAX(IF(o.concept_id = 165104 AND o.value_coded = 1380, ''Nutritional and Dietary'', NULL)), ',
      'MAX(IF(o.concept_id = 165104 AND o.value_coded = 5490, ''Psychosocial therapy'', NULL)), ',
      'MAX(IF(o.concept_id = 165104 AND o.value_coded = 165151, ''Substance Abuse Counseling'', NULL)), ',
      'MAX(IF(o.concept_id = 165104 AND o.value_coded = 5622, ''Other'', NULL))',
    ') AS counselling_prescribed, ',
    'MAX(IF(o.concept_id = 160433, o.value_coded, NULL)) AS patient_outcome, ',
    'MAX(IF(o.concept_id = 163145, o.value_coded, NULL)) AS referred_to, ',
    'MAX(IF(o.concept_id = 159495 OR o.concept_id = 162724, o.value_text, NULL)) AS facility_transferred_to, ',
    'MAX(IF(o.concept_id = 1640, o.value_datetime, NULL)) AS date_of_admission, ',
    'MAX(IF(o.concept_id = 162879, o.value_text, NULL)) AS reason_for_admission, ',
    'MAX(IF(o.concept_id = 162477, o.value_coded, NULL)) AS type_of_admission, ',
    'MAX(IF(o.concept_id = 1655, o.value_coded, NULL)) AS priority_of_admission, ',
    'MAX(IF(o.concept_id = 1000075, o.value_coded, NULL)) AS admission_ward, ',
    'MAX(IF(o.concept_id = 1896, o.value_coded, NULL)) AS duration_of_hospital_stay, ',
    'e.voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''1fbd26f1-0478-437c-be1e-b8468bd03ffa'' ',
    'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (164181,160338,160478,5219,165250,124068,1390,168148,166635,160715,159367,165206,165241,165034,123160,161011,160598,1427,162737,167092,167193,167201,167099,167526,167203,167321,167116,167115,167101,167106,167112,167181,167084,163104,165104,160433,163145,159495,162724,1640,162879,162477,1655,1000075,1896) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider), visit_date=VALUES(visit_date), visit_type=VALUES(visit_type), referred_from=VALUES(referred_from), referred_from_department=VALUES(referred_from_department), presenting_allegations=VALUES(presenting_allegations), other_allegations=VALUES(other_allegations), contact_with_TB_case=VALUES(contact_with_TB_case), history_of_present_illness=VALUES(history_of_present_illness), surgical_history=VALUES(surgical_history), type_of_surgery=VALUES(type_of_surgery), surgery_date=VALUES(surgery_date), on_medication=VALUES(on_medication), childhood_mistreatment=VALUES(childhood_mistreatment), persistent_cruelty_meanness=VALUES(persistent_cruelty_meanness), physically_abused=VALUES(physically_abused), sexually_abused=VALUES(sexually_abused), patient_occupation_history=VALUES(patient_occupation_history), reproductive_history=VALUES(reproductive_history), lmp_date=VALUES(lmp_date), general_examination_findings=VALUES(general_examination_findings), mental_status=VALUES(mental_status), attitude_and_behaviour=VALUES(attitude_and_behaviour), speech=VALUES(speech), mood=VALUES(mood), illusions=VALUES(illusions), attention_concentration=VALUES(attention_concentration), memory_recall=VALUES(memory_recall), judgement=VALUES(judgement), insight=VALUES(insight), affect=VALUES(affect), thought_process=VALUES(thought_process), thought_content=VALUES(thought_content), hallucinations=VALUES(hallucinations), orientation_status=VALUES(orientation_status), management_plan=VALUES(management_plan), counselling_prescribed=VALUES(counselling_prescribed), patient_outcome=VALUES(patient_outcome), referred_to=VALUES(referred_to), facility_transferred_to=VALUES(facility_transferred_to), date_of_admission=VALUES(date_of_admission), reason_for_admission=VALUES(reason_for_admission), type_of_admission=VALUES(type_of_admission), priority_of_admission=VALUES(priority_of_admission), admission_ward=VALUES(admission_ward), duration_of_hospital_stay=VALUES(duration_of_hospital_stay), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing Psychiatry -> ', @target_table, ' Time: ', NOW()) AS status;
END $$





-- --------------------------------------
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- PROCEDURE: sp_update_etl_kvp_clinical_enrollment
-- Purpose: update tenant-aware etl_kvp_clinical_enrollment
-- Notes: calls sp_set_tenant_session_vars() to resolve etl_schema then
--        builds and executes a dynamic INSERT ... ON DUPLICATE KEY UPDATE
-- --------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_kvp_clinical_enrollment $$
CREATE PROCEDURE sp_update_etl_kvp_clinical_enrollment(IN last_update_time DATETIME)
BEGIN
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT('', etl_schema, '.etl_kvp_clinical_enrollment');

  SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (patient_id, visit_id, encounter_id, uuid, location_id, provider, visit_date, contacted_by_pe_for_health_services, has_regular_non_paying_sexual_partner, number_of_sexual_partners, year_started_fsw, year_started_msm, year_started_using_drugs, trucker_duration_on_transit, duration_working_as_trucker, duration_working_as_fisherfolk, year_tested_discordant_couple, ever_experienced_violence, type_of_violence_experienced, ever_tested_for_hiv, latest_hiv_test_method, latest_hiv_test_results, willing_to_test_for_hiv, reason_not_willing_to_test_for_hiv, receiving_hiv_care, hiv_care_facility, other_hiv_care_facility, ccc_number, consent_followup, date_created, date_last_modified, voided) ',
    'SELECT e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator, DATE(e.encounter_datetime) AS visit_date, ',
    'MAX(IF(o.concept_id = 165004, o.value_coded, NULL)) AS contacted_by_pe_for_health_services, ',
    'MAX(IF(o.concept_id = 165027, o.value_coded, NULL)) AS has_regular_non_paying_sexual_partner, ',
    'MAX(IF(o.concept_id = 5570, o.value_numeric, NULL)) AS number_of_sexual_partners, ',
    'MAX(IF(o.concept_id = 165030, o.value_numeric, NULL)) AS year_started_fsw, ',
    'MAX(IF(o.concept_id = 165031, o.value_numeric, NULL)) AS year_started_msm, ',
    'MAX(IF(o.concept_id = 165032, o.value_numeric, NULL)) AS year_started_using_drugs, ',
    'MAX(IF(o.concept_id = 165032, o.value_numeric, NULL)) AS trucker_duration_on_transit, ',
    'MAX(IF(o.concept_id = 163191, o.value_numeric, NULL)) AS duration_working_as_trucker, ',
    'MAX(IF(o.concept_id = 169043, o.value_numeric, NULL)) AS duration_working_as_fisherfolk, ',
    'MAX(IF(o.concept_id = 159813, o.value_numeric, NULL)) AS year_tested_discordant_couple, ',
    'MAX(IF(o.concept_id = 123160, o.value_coded, NULL)) AS ever_experienced_violence, ',
    'MAX(IF(o.concept_id = 165205, o.value_coded, NULL)) AS type_of_violence_experienced, ',
    'MAX(IF(o.concept_id = 164401, o.value_coded, NULL)) AS ever_tested_for_hiv, ',
    'MAX(IF(o.concept_id = 164956, o.value_coded, NULL)) AS latest_hiv_test_method, ',
    'MAX(IF(o.concept_id = 165153, o.value_coded, NULL)) AS latest_hiv_test_results, ',
    'MAX(IF(o.concept_id = 165154, o.value_coded, NULL)) AS willing_to_test_for_hiv, ',
    'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS reason_not_willing_to_test_for_hiv, ',
    'MAX(IF(o.concept_id = 159811, o.value_coded, NULL)) AS receiving_hiv_care, ',
    'MAX(IF(o.concept_id = 165239, o.value_coded, NULL)) AS hiv_care_facility, ',
    'MAX(IF(o.concept_id = 162724, o.value_text, NULL)) AS other_hiv_care_facility, ',
    'MAX(IF(o.concept_id = 162053, o.value_numeric, NULL)) AS ccc_number, ',
    'MAX(IF(o.concept_id = 165036, o.value_numeric, NULL)) AS consent_followup, ',
    'e.date_created, e.date_changed, e.voided ',
    'FROM encounter e ',
    'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''c7f47cea-207b-11e9-ab14-d663bd873d93'' ',
    'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (165004,165027,5570,165030,165031,165032,163191,169043,159813,123160,165205,164401,164956,165153,165154,160632,159811,165239,162724,162053,165036) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ') ',
    'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
    'ON DUPLICATE KEY UPDATE provider=VALUES(provider), visit_date=VALUES(visit_date), contacted_by_pe_for_health_services=VALUES(contacted_by_pe_for_health_services), has_regular_non_paying_sexual_partner=VALUES(has_regular_non_paying_sexual_partner), number_of_sexual_partners=VALUES(number_of_sexual_partners), year_started_fsw=VALUES(year_started_fsw), year_started_msm=VALUES(year_started_msm), year_started_using_drugs=VALUES(year_started_using_drugs), trucker_duration_on_transit=VALUES(trucker_duration_on_transit), duration_working_as_trucker=VALUES(duration_working_as_trucker), duration_working_as_fisherfolk=VALUES(duration_working_as_fisherfolk), year_tested_discordant_couple=VALUES(year_tested_discordant_couple), ever_experienced_violence=VALUES(ever_experienced_violence), type_of_violence_experienced=VALUES(type_of_violence_experienced), ever_tested_for_hiv=VALUES(ever_tested_for_hiv), latest_hiv_test_method=VALUES(latest_hiv_test_method), latest_hiv_test_results=VALUES(latest_hiv_test_results), willing_to_test_for_hiv=VALUES(willing_to_test_for_hiv), reason_not_willing_to_test_for_hiv=VALUES(reason_not_willing_to_test_for_hiv), receiving_hiv_care=VALUES(receiving_hiv_care), hiv_care_facility=VALUES(hiv_care_facility), other_hiv_care_facility=VALUES(other_hiv_care_facility), ccc_number=VALUES(ccc_number), consent_followup=VALUES(consent_followup), date_created=VALUES(date_created), date_last_modified=VALUES(date_last_modified), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing KVP Clinical enrollment -> ', @target_table, ' Time: ', NOW()) AS status;
END $$


-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- PROCEDURE: sp_update_etl_special_clinics
-- Purpose: update tenant-aware etl_special_clinics
-- Tenant-aware: calls sp_set_tenant_session_vars() and inserts into dynamic target

DROP PROCEDURE IF EXISTS sp_update_etl_special_clinics $$
CREATE PROCEDURE sp_update_etl_special_clinics(IN last_update_time DATETIME)
BEGIN
  -- initialize tenant session vars (must set etl_schema)
CALL sp_set_tenant_session_vars();

SET @target_table = CONCAT('', etl_schema, '.etl_special_clinics');

SELECT CONCAT('Processing ', @target_table) AS status;

SET @sql = CONCAT(
'INSERT INTO ', @target_table, ' (',
'patient_id, visit_id, encounter_id, uuid, location_id, provider, visit_date, visit_type, pregnantOrLactating, referred_from, eye_assessed, acuity_finding, referred_to, ot_intervention, assistive_technology, enrolled_in_school, patient_with_disability, patient_has_edema, nutritional_status, patient_pregnant, sero_status, nutritional_intervention, postnatal, patient_on_arv, anaemia_level, metabolic_disorders, critical_nutrition_practices, maternal_nutrition, therapeutic_food, supplemental_food, micronutrients, referral_status, criteria_for_admission, type_of_admission, cadre, neuron_developmental_findings, neurodiversity_conditions, learning_findings, screening_site, communication_mode, neonatal_risk_factor, presence_of_comobidities, first_screening_date, first_screening_outcome, second_screening_outcome, symptoms_for_otc, nutritional_details, first_0_6_months, second_6_12_months, disability_classification, treatment_intervention, area_of_service, diagnosis_category, orthopaedic_patient_no, patient_outcome, special_clinic, special_clinic_form_uuid, date_created, date_last_modified',
') ',
'SELECT ',
'e.patient_id, e.visit_id, e.encounter_id, e.uuid, e.location_id, e.creator, DATE(e.encounter_datetime) AS visit_date, ',
'MAX(IF(o.concept_id = 164181, o.value_coded, NULL)) AS visit_type, ',
'MAX(IF(o.concept_id = 5272, o.value_coded, NULL)) AS pregnantOrLactating, ',
'MAX(IF(o.concept_id = 161643, o.value_coded, NULL)) AS referred_from, ',
'MAX(IF(o.concept_id = 160348, o.value_coded, NULL)) AS eye_assessed, ',
'MAX(IF(o.concept_id = 164448, o.value_coded, NULL)) AS acuity_finding, ',
'MAX(IF(o.concept_id IN (163145,1788), o.value_coded, NULL)) AS referred_to, ',
'CONCAT_WS('','', ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 1107, ''None'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 164806, ''Neonatal Screening'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 168287, ''Initial Assessment'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 168031, ''Neonatal Screening'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 169149, ''Environmental Assessment'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 527, ''Splinting'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 163318, ''Developmental Skills Training'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 1000534, ''Multi sensory screening'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 2002026, ''Therapeutic Activities'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 2000823, ''Sensory Stimulation'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 160130, ''Vocational Training'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 164518, ''Bladder and Bowel Management'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 164872, ''Environmental Adaptations'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 2002045, ''OT Screening'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 167696, ''Individual Psychotherapy'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 166724, ''Scar Management'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 167695, ''Group Psychotherapy'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 167809, ''Health Education/ Patient Education'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 165001, ''Home Visits (Interventions)'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 167277, ''Recreation Therapy'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 161625, ''OT in critical care'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 167813, ''OT Sexual health'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 160351, ''Teletherapy'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 163579, ''Fine and gross motor skills training'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 160563, ''Referrals IN'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 159492, ''Referrals OUT'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 1692, ''Discharge on recovery'', NULL)), ',
'  MAX(IF(o.concept_id = 165302 AND o.value_coded = 5622, ''Others(specify)'', NULL))',
') AS ot_intervention, ',
'   CONCAT_WS('','',max(if(o.concept_id = 164204 and o.value_coded = 1107,  ''None'',NULL)),
                     max(if(o.concept_id = 164204 and o.value_coded = 2000819,  ''Communication'',NULL)),
                     max(if(o.concept_id = 164204 and o.value_coded = 165151,  ''Self Care'',NULL)),
                     max(if(o.concept_id = 164204 and o.value_coded = 168812,  ''Physical'',NULL)),
                     max(if(o.concept_id = 164204 and o.value_coded = 165424,  ''Cognitive/Intellectual'',NULL)),
                     max(if(o.concept_id = 164204 and o.value_coded = 2000976,  ''Hearing Devices'',NULL))) as assistive_technology, ',
'           max(if(o.concept_id = 160336, o.value_coded, null))                         as enrolled_in_school, ',
'           max(if(o.concept_id = 162558, o.value_coded, null))                         as patient_with_disability, ',
'           max(if(o.concept_id = 163894, o.value_coded, null))                         as patient_has_edema, ',
'           max(if(o.concept_id = 160205,o.value_coded,null))                           as nutritional_status, ',
'           max(if(o.concept_id  = 5272,o.value_coded,null))                            as patient_pregnant, ',
'           max(if(o.concept_id  = 1169,o.value_coded,null))                            as sero_status, ',
'           max(if(o.concept_id  = 162696,o.value_coded,null))                           as nutritional_intervention, ',
'           max(if(o.concept_id = 168734,o.value_coded,null))                          as postnatal, ',
'           max(if(o.concept_id  =1149,o.value_coded,null))                            as patient_on_arv, ',
'           max(if(o.concept_id  = 156625,o.value_coded,null))                          as anaemia_level, ',
'              CONCAT_WS('','',max(if(o.concept_id = 163304 and o.value_coded = 1107,  ''None'',NULL)), ',
'                     max(if(o.concept_id = 163304 and o.value_coded = 135761,  ''Lypodystrophy'',NULL)), ',
'                     max(if(o.concept_id = 163304 and o.value_coded = 141623,  ''Dyslipidemia'',NULL)), ',
'                     max(if(o.concept_id = 163304 and o.value_coded = 142473,  ''Type II Diabetes'',NULL))) as metabolic_disorders, ',
'             CONCAT_WS('','',max(if(o.concept_id = 161005 and o.value_coded = 1107,  ''None'',NULL)), ',
'                     max(if(o.concept_id = 161005 and o.value_coded = 163300,  ''Nutrition status assessment'',NULL)), ',
'                     max(if(o.concept_id = 161005 and o.value_coded = 161648,  ''Dietary/Energy needs'',NULL)), ',
'                     max(if(o.concept_id = 161005 and o.value_coded = 1906,  ''Sanitation'',NULL)), ',
'                     max(if(o.concept_id = 161005 and o.value_coded = 135797,  ''Positive living behaviour'',NULL)), ',
'                     max(if(o.concept_id = 161005 and o.value_coded = 159364,  ''Exercise'',NULL)), ',
'                     max(if(o.concept_id = 161005 and o.value_coded = 154358,  ''Safe drinking water'',NULL)), ',
'                     max(if(o.concept_id = 161005 and o.value_coded = 1611,  ''Prompt treatment for Opportunistic Infections'',NULL)), ',
'                     max(if(o.concept_id = 161005 and o.value_coded = 164377,  ''Drug food interactions side effects'',NULL))) as critical_nutrition_practices, ',
'             max(if(o.concept_id = 163300, o.value_coded, null))                                            as maternal_nutrition, ',
'             CONCAT_WS('','',max(if(o.concept_id = 161648 and o.value_coded = 1107,  ''None'',NULL)), ',
'                     max(if(o.concept_id = 161648 and o.value_coded = 163394,  ''RUTF'',NULL)), ',
'                     max(if(o.concept_id = 161648 and o.value_coded = 163404,  ''F-75'',NULL)), ',
'                     max(if(o.concept_id = 161648 and o.value_coded = 167247,  ''F-100'',NULL)), ',
'                     max(if(o.concept_id = 161648 and o.value_coded = 159854,  ''Fiesmol'',NULL)), ',
'                     max(if(o.concept_id = 161648 and o.value_coded = 159364,  ''Exercise'',NULL)), ',
'                     max(if(o.concept_id = 161648 and o.value_coded = 5622,  ''Others'',NULL))) as therapeutic_food, ',
'             CONCAT_WS('','',max(if(o.concept_id = 159854 and o.value_coded = 1107,  ''None'',NULL)), ',
'                     max(if(o.concept_id = 159854 and o.value_coded = 159597,  ''FBF'',NULL)), ',
'                     max(if(o.concept_id = 159854 and o.value_coded = 162758,  ''CSB'',NULL)), ',
'                     max(if(o.concept_id = 159854 and o.value_coded = 166382,  ''RUSF'',NULL)), ',
'                     max(if(o.concept_id = 159854 and o.value_coded = 165577,  ''Liquid nutrition supplements'',NULL)), ',
'                     max(if(o.concept_id = 159854 and o.value_coded = 5622,  ''Others'',NULL))) as supplemental_food, ',
'             CONCAT_WS('','',max(if(o.concept_id = 5484 and o.value_coded = 1107,  ''None'',NULL)), ',
'                     max(if(o.concept_id = 5484 and o.value_coded = 86339,  ''Vitamin A'',NULL)), ',
'                     max(if(o.concept_id = 5484 and o.value_coded = 86343,  ''B6'',NULL)), ',
'                     max(if(o.concept_id = 5484 and o.value_coded = 461,  ''Multi-vitamins'',NULL)), ',
'                     max(if(o.concept_id = 5484 and o.value_coded = 104677,  ''Iron-folate'',NULL)), ',
'                     max(if(o.concept_id = 5484 and o.value_coded = 86672,  ''Zinc'',NULL)), ',
'                     max(if(o.concept_id = 5484 and o.value_coded = 161649,  ''Multiple Micronutrients'',NULL)), ',
'                     max(if(o.concept_id = 5484 and o.value_coded = 5622,  ''Others'',NULL))) as micronutrients, ',
'        max(if(o.concept_id  =   1788,o.value_coded,null))                          as referral_status, ',
'        max(if(o.concept_id  =  167381,o.value_coded,null))                         as criteria_for_admission, ',
'        max(if(o.concept_id  =  162477,o.value_coded,null))                         as type_of_admission, ',
'        max(if(o.concept_id  =  5619,o.value_coded,null))                           as cadre, ',
'        CONCAT_WS('','',max(if(o.concept_id = 167273 and o.value_coded = 152492,  ''Cerebral palsy'',NULL)), ',
'                    max(if(o.concept_id = 167273 and o.value_coded = 144481,  ''Down syndrome'',NULL)), ',
'                    max(if(o.concept_id = 167273 and o.value_coded = 117470,  ''Hydrocephalus'',NULL)), ',
'                    max(if(o.concept_id = 167273 and o.value_coded = 126208,  ''Spina bifida'',NULL))) as neuron_developmental_findings, ',
'        CONCAT_WS('','',max(if(o.concept_id = 165911 and o.value_coded = 121317,  ''ADHD(Attention deficit hyperactivity disorder)'',NULL)), ',
'                          max(if(o.concept_id = 165911 and o.value_coded = 121303,  ''Autism'',NULL))) as neurodiversity_conditions, ',
'        CONCAT_WS('','',max(if(o.concept_id = 165241 and o.value_coded = 118795,  ''Dyslexia'',NULL)), ',
'                      max(if(o.concept_id = 165241 and o.value_coded = 118800,  ''Dysgraphia'',NULL)), ',
'                      max(if(o.concept_id = 165241 and o.value_coded = 141644,  ''Dyscalculia'',NULL)), ',
'                      max(if(o.concept_id = 165241 and o.value_coded = 153271,  ''Auditory processing'',NULL)), ',
'                      max(if(o.concept_id = 165241 and o.value_coded = 121529,  ''Language processing disorder'',NULL)), ',
'                      max(if(o.concept_id = 165241 and o.value_coded = 155205,  ''Nonverbal learning disabilities'',NULL)), ',
'                      max(if(o.concept_id = 165241 and o.value_coded = 126456,  ''Visual perceptual/visual motor deficit'',NULL))) as learning_findings, ',
'        max(if(o.concept_id = 1000494, o.value_coded, null))                         as screening_site, ',
'        max(if(o.concept_id = 164209, o.value_coded, null))                         as communication_mode, ',
'        max(if(o.concept_id = 165430, o.value_coded, null))                         as neonatal_risk_factor, ',
'        CONCAT_WS('','',max(if(o.concept_id = 162747 and o.value_coded = 117086,  ''Recurrent ear infections'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 117087,  ''Chronic ear disease'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 140903,  ''Noise exposure'',NULL)), ',
'                     max(if(o.concept_id = 162747 and (o.value_coded = 119481 OR o.value_coded = 127706),  ''Diabetes'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 117399,  ''Hypertension'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 148117,  ''Autoimmune diseases'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 116838,  ''Head injury'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 114698,  ''Osteoarthritis'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 114662,  ''Osteoporosis'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 127417,  ''Rheumatoid arthritis'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 115115,  ''Obesity'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 117762,  ''Gout'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 153690,  ''Lupus'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 119955,  ''Hip dysplasia'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 113125,  ''Scoliosis'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 5622,  ''Other'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 1169,  ''HIV'',NULL)), ',
'                     max(if(o.concept_id = 162747 and o.value_coded = 112141,  ''TB'',NULL))) as presence_of_comobidities, ',
'        max(if(o.concept_id = 1000088, o.value_datetime, null))                     as first_screening_date, ',
'        max(if(o.concept_id = 162737, o.value_coded, null))                         as first_screening_outcome, ',
'        max(if(o.concept_id = 166663, o.value_coded, null))                         as second_screening_outcome, ',
'        CONCAT_WS('','',max(if(o.concept_id = 5219 and o.value_coded = 114403,  ''Pain'',NULL)), ',
'                     max(if(o.concept_id = 5219 and o.value_coded = 130842,  ''Immobility'',NULL)), ',
'                     max(if(o.concept_id = 5219 and o.value_coded = 140468,  ''Muscle tenseness'',NULL)), ',
'                     max(if(o.concept_id = 5219 and o.value_coded = 119775,  ''Muscle spasms'',NULL)), ',
'                     max(if(o.concept_id = 5219 and o.value_coded = 163894,  ''Swelling'',NULL)), ',
'                     max(if(o.concept_id = 5219 and o.value_coded = 111525,  ''Loss of function'',NULL)), ',
'                     max(if(o.concept_id = 5219 and o.value_coded = 116554,  ''Joint stiffness'',NULL)), ',
'                     max(if(o.concept_id = 5219 and o.value_coded = 5622,  ''Other'',NULL))) as symptoms_for_otc, ',
'        max(if(o.concept_id = 159402, o.value_coded, null))                         as nutritional_details, ',
'        max(if(o.concept_id = 985, o.value_coded, null))                         as first_0_6_months, ',
'        max(if(o.concept_id = 1151, o.value_coded, null))                         as second_6_12_months, ',
'        CONCAT_WS('','',max(if(o.concept_id = 1069 and o.value_coded = 167078,  ''Neurodevelopmental'',NULL)), ',
'                    max(if(o.concept_id = 1069 and o.value_coded = 153343,  ''learning'',NULL)), ',
'                    max(if(o.concept_id = 1069 and o.value_coded = 160176,  ''Neurodiversity conditions'',NULL)), ',
'                    max(if(o.concept_id = 1069 and o.value_coded = 156923,  ''Intelectual disability'',NULL)), ',
'                    max(if(o.concept_id = 1069 and o.value_coded = 142616,  ''Delayed developmental milestone'',NULL)), ',
'                    max(if(o.concept_id = 1069 and o.value_coded = 155205,  ''Nonverbal learning disabilities'',NULL)), ',
'                    max(if(o.concept_id = 1069 and o.value_coded = 5622,  ''Others(specify)'',NULL))) as disability_classification, ',
'        CONCAT_WS('','',max(if(o.concept_id = 165531 and o.value_coded = 167004,  ''Assessing'',NULL)), ',
'                    max(if(o.concept_id = 165531 and o.value_coded = 2001239,  ''Counselling Delivery'',NULL)), ',
'                    max(if(o.concept_id = 165531 and o.value_coded = 162308,  ''Measurement taking'',NULL)), ',
'                    max(if(o.concept_id = 165531 and o.value_coded = 119758,  ''Fabrication'',NULL)), ',
'                    max(if(o.concept_id = 165531 and o.value_coded = 159630,  ''Fitting'',NULL)), ',
'                    max(if(o.concept_id = 165531 and o.value_coded = 5622,  ''Assistive Technology Training'',NULL)), ',
'                    max(if(o.concept_id = 165531 and o.value_coded = 2001627,  ''Casting'',NULL)), ',
'                    max(if(o.concept_id = 165531 and o.value_coded = 1000474,  ''PWD assessment & Categorization'',NULL)), ',
'                    max(if(o.concept_id = 165531 and o.value_coded = 160068,  ''Referral'',NULL)), ',
'                    max(if(o.concept_id = 165531 and o.value_coded = 142608,  ''Delivery'',NULL))) as treatment_intervention, ',
'                    max(if(o.concept_id = 168146, o.value_coded, null))                               as area_of_service, ',
'	            	max(if(o.concept_id=2031533,(case o.value_coded when 1687 then ''New'' when 2031534 THEN ''Existing'' else '''' end),null)) as diagnosis_category, ',
'                    max(if(o.concept_id = 159893, o.value_numeric, null))                            as orthopaedic_patient_no, ',
'MAX(IF(o.concept_id = 160433, o.value_coded, NULL)) AS patient_outcome, ',
'CASE f.uuid ',
' WHEN ''c5055956-c3bb-45f2-956f-82e114c57aa7'' THEN ''ENT'' ',
' WHEN ''1fbd26f1-0478-437c-be1e-b8468bd03ffa'' THEN ''Psychiatry'' ',
' WHEN ''235900ff-4d4a-4575-9759-96f325f5e291'' THEN ''Ophthamology'' ',
' WHEN ''beec83df-6606-4019-8223-05a54a52f2b0'' THEN ''Orthopaedic'' ',
' WHEN ''35ab0825-33af-49e7-ac01-bb0b05753732'' THEN ''Obstetric'' ',
' WHEN ''062a24b5-728b-4639-8176-197e8f458490'' THEN ''Occupational Therapy'' ',
' WHEN ''18c209ac-0787-4b51-b9aa-aa8b1581239c'' THEN ''Physiotherapy'' ',
' WHEN ''b8357314-0f6a-4fc9-a5b7-339f47095d62'' THEN ''Nutrition'' ',
' WHEN ''31a371c6-3cfe-431f-94db-4acadad8d209'' THEN ''Oncology'' ',
' WHEN ''d9f74419-e179-426e-9aff-ec97f334a075'' THEN ''Audiology'' ',
' WHEN ''998be6de-bd13-4136-ba0d-3f772139895f'' THEN ''Cardiology'' ',
' WHEN ''efa2f992-44af-487e-aaa7-c92813a34612'' THEN ''Dermatology'' ',
' WHEN ''f97f2bf3-c26b-4adf-aacd-e09d720a14cd'' THEN ''Neurology'' ',
' WHEN ''9f6543e4-0821-4f9c-9264-94e45dc35e17'' THEN ''Diabetic'' ',
' WHEN ''d95e44dd-e389-42ae-a9b6-1160d8eeebc4'' THEN ''Pediatrics'' ',
' WHEN ''da1f7e74-5371-4997-8a02-b7b9303ddb61'' THEN ''Surgical'' ',
' WHEN ''b40d369c-31d0-4c1d-a80a-7e4b7f73bea0'' THEN ''Maxillofacial'' ',
' WHEN ''32e43fc9-6de3-48e3-aafe-3b92f167753d'' THEN ''Fertility'' ',
' WHEN ''a3c01460-c346-4f3d-a627-5c7de9494ba0'' THEN ''Dental'' ',
' WHEN ''6b4fa553-f2b3-47d0-a4c5-fc11f38b0b24'' THEN ''Gastroenterology'' ',
' WHEN ''4b5f79f5-f6bf-4dc2-b5c3-f5d77506775c'' THEN ''Hearing'' ',
' END AS special_clinic, ',
'f.uuid AS special_clinic_form_uuid, ',
'e.date_created, e.date_changed AS date_last_modified ',
'FROM encounter e ',
' INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
' INNER JOIN form f ON f.form_id = e.form_id AND f.uuid IN (',
'  ''c5055956-c3bb-45f2-956f-82e114c57aa7'', ',
'  ''1fbd26f1-0478-437c-be1e-b8468bd03ffa'', ',
'  ''235900ff-4d4a-4575-9759-96f325f5e291'', ',
'  ''beec83df-6606-4019-8223-05a54a52f2b0'', ',
'  ''35ab0825-33af-49e7-ac01-bb0b05753732'', ',
'  ''062a24b5-728b-4639-8176-197e8f458490'', ',
'  ''18c209ac-0787-4b51-b9aa-aa8b1581239c'', ',
'  ''b8357314-0f6a-4fc9-a5b7-339f47095d62'', ',
'  ''31a371c6-3cfe-431f-94db-4acadad8d209'', ',
'  ''d9f74419-e179-426e-9aff-ec97f334a075'', ',
'  ''998be6de-bd13-4136-ba0d-3f772139895f'', ',
'  ''efa2f992-44af-487e-aaa7-c92813a34612'', ',
'  ''f97f2bf3-c26b-4adf-aacd-e09d720a14cd'', ',
'  ''9f6543e4-0821-4f9c-9264-94e45dc35e17'', ',
'  ''d95e44dd-e389-42ae-a9b6-1160d8eeebc4'', ',
'  ''00aa7662-e3fd-44a5-8f3a-f73eb7afa437'', ',
'  ''da1f7e74-5371-4997-8a02-b7b9303ddb61'', ',
'  ''6b4fa553-f2b3-47d0-a4c5-fc11f38b0b24'', ',
'  ''32e43fc9-6de3-48e3-aafe-3b92f167753d'', ',
'  ''a3c01460-c346-4f3d-a627-5c7de9494ba0'', ',
'  ''b40d369c-31d0-4c1d-a80a-7e4b7f73bea0'', ',
'  ''54462245-2cb6-4ca9-a15a-ba35adfa0e8f'' ',
') ',
' LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (164181,161643,164448,163145,165302,164204,160336,162558,163894,160205,5272,1169,162696,168734,1149,156625,163304,161005,161648,159854,5484,1788,167381,162477,5619,167273,165911,165241,1000494,164209,165430,162747,1000088,162737,166663,5219,159402,985,1151,1069,165531,168146,159893,163300,5272,160348,2031533,160433) AND o.voided = 0 ',
'WHERE e.voided = 0 AND (',
' e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
' e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
' e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
' o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
' o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
') ',
'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
'ON DUPLICATE KEY UPDATE ',
' provider=VALUES(provider), visit_date=VALUES(visit_date), visit_type=VALUES(visit_type), pregnantOrLactating=VALUES(pregnantOrLactating), referred_from=VALUES(referred_from), eye_assessed=VALUES(eye_assessed), acuity_finding=VALUES(acuity_finding), referred_to=VALUES(referred_to), ot_intervention=VALUES(ot_intervention), assistive_technology=VALUES(assistive_technology), enrolled_in_school=VALUES(enrolled_in_school), patient_with_disability=VALUES(patient_with_disability), patient_has_edema=VALUES(patient_has_edema), nutritional_status=VALUES(nutritional_status), patient_pregnant=VALUES(patient_pregnant), sero_status=VALUES(sero_status), nutritional_intervention=VALUES(nutritional_intervention), postnatal=VALUES(postnatal), patient_on_arv=VALUES(patient_on_arv), anaemia_level=VALUES(anaemia_level), metabolic_disorders=VALUES(metabolic_disorders), critical_nutrition_practices=VALUES(critical_nutrition_practices), maternal_nutrition=VALUES(maternal_nutrition), therapeutic_food=VALUES(therapeutic_food), supplemental_food=VALUES(supplemental_food), micronutrients=VALUES(micronutrients), referral_status=VALUES(referral_status), criteria_for_admission=VALUES(criteria_for_admission), type_of_admission=VALUES(type_of_admission), cadre=VALUES(cadre), neuron_developmental_findings=VALUES(neuron_developmental_findings), neurodiversity_conditions=VALUES(neurodiversity_conditions), learning_findings=VALUES(learning_findings), screening_site=VALUES(screening_site), communication_mode=VALUES(communication_mode), neonatal_risk_factor=VALUES(neonatal_risk_factor), presence_of_comobidities=VALUES(presence_of_comobidities), first_screening_date=VALUES(first_screening_date), first_screening_outcome=VALUES(first_screening_outcome), second_screening_outcome=VALUES(second_screening_outcome), symptoms_for_otc=VALUES(symptoms_for_otc), nutritional_details=VALUES(nutritional_details), first_0_6_months=VALUES(first_0_6_months), second_6_12_months=VALUES(second_6_12_months), disability_classification=VALUES(disability_classification), treatment_intervention=VALUES(treatment_intervention), area_of_service=VALUES(area_of_service), diagnosis_category=VALUES(diagnosis_category), orthopaedic_patient_no=VALUES(orthopaedic_patient_no), patient_outcome=VALUES(patient_outcome);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing ', @target_table, ' Time: ', NOW()) AS status;
END $$



-- --------------------------------------
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- PROCEDURE: sp_update_etl_high_iit_intervention
-- Purpose: update tenant-aware etl_high_iit_intervention
-- Tenant-aware: calls sp_set_tenant_session_vars() and inserts into dynamic target
-- --------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_high_iit_intervention $$
CREATE PROCEDURE sp_update_etl_high_iit_intervention(IN last_update_time DATETIME)
BEGIN
  -- resolve tenant schema variable
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT('', etl_schema, '.etl_high_iit_intervention');

  SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
      'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, ',
      'interventions_offered, appointment_mgt_interventions, reminder_methods, enrolled_in_ushauri, ',
      'appointment_mngt_intervention_date, date_assigned_case_manager, eacs_recommended, ',
      'enrolled_in_psychosocial_support_group, robust_literacy_interventions_date, ',
      'expanding_differentiated_service_delivery_interventions, enrolled_in_nishauri, ',
      'expanded_differentiated_service_delivery_interventions_date, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.uuid, e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'CONCAT_WS('','', ',
        'MAX(IF(o.concept_id = 166937 AND o.value_coded = 160947, ''''Appointment management'''', NULL)), ',
        'MAX(IF(o.concept_id = 166937 AND o.value_coded = 164836, ''''Assigning Case managers'''', NULL)), ',
        'MAX(IF(o.concept_id = 166937 AND o.value_coded = 167809, ''''Robust client literacy'''', NULL)), ',
        'MAX(IF(o.concept_id = 166937 AND o.value_coded = 164947, ''''Expanding Differentiated Service Delivery'''', NULL))',
      ') AS interventions_offered, ',
      'CONCAT_WS('','', ',
        'MAX(IF(o.concept_id = 165353 AND o.value_coded = 162135, ''''Individualized discussion with the recipient of care to understand their preferences for appointments'''', NULL)), ',
        'MAX(IF(o.concept_id = 165353 AND o.value_coded = 166065, ''''Agree on a plan if the recipient of care cannot honor their given appointments'''', NULL)), ',
        'MAX(IF(o.concept_id = 165353 AND o.value_coded = 163164, ''''Willngness to receive reminders'''', NULL)), ',
        'MAX(IF(o.concept_id = 165353 AND o.value_coded = 167733, ''''Immediate follow up via phone calls on the day of missed appointment, next day and intensely up to 7 days'''', NULL)), ',
        'MAX(IF(o.concept_id = 165353 AND o.value_coded = 164965, ''''Physical tracing by the CHW/Volunteers if not returned by day 7'''', NULL))',
      ') AS appointment_mgt_interventions, ',
      'CONCAT_WS('','', ',
        'MAX(IF(o.concept_id = 166607 AND o.value_coded = 162135, ''''SMS'''', NULL)), ',
        'MAX(IF(o.concept_id = 166607 AND o.value_coded = 166065, ''''Phone call'''', NULL))',
      ') AS reminder_methods, ',
      'MAX(IF(o.concept_id = 163777, o.value_coded, NULL)) AS enrolled_in_ushauri, ',
      'MAX(IF(o.concept_id = 5096, o.value_datetime, NULL)) AS appointment_mngt_intervention_date, ',
      'MAX(IF(o.concept_id = 160753, o.value_datetime, NULL)) AS date_assigned_case_manager, ',
      'MAX(IF(o.concept_id = 168804, o.value_coded, NULL)) AS eacs_recommended, ',
      'MAX(IF(o.concept_id = 165163, o.value_coded, NULL)) AS enrolled_in_psychosocial_support_group, ',
      'MAX(IF(o.concept_id = 162869, o.value_datetime, NULL)) AS robust_literacy_interventions_date, ',
      'MAX(IF(o.concept_id = 164947, o.value_coded, NULL)) AS expanding_differentiated_service_delivery_interventions, ',
      'MAX(IF(o.concept_id = 163766, o.value_coded, NULL)) AS enrolled_in_nishauri, ',
      'MAX(IF(o.concept_id = 166865, o.value_datetime, NULL)) AS expanded_differentiated_service_delivery_interventions_date, ',
      'e.date_created, e.date_changed ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''6817d322-f938-4f38-8ccf-caa6fa7a499f'' ',
      'LEFT OUTER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (166937,166607,163777,5096,160753,168804,165163,162869,164947,163766,166865,165353) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider), visit_date=VALUES(visit_date), interventions_offered=VALUES(interventions_offered), ',
      'appointment_mgt_interventions=VALUES(appointment_mgt_interventions), reminder_methods=VALUES(reminder_methods), ',
      'enrolled_in_ushauri=VALUES(enrolled_in_ushauri), appointment_mngt_intervention_date=VALUES(appointment_mngt_intervention_date), ',
      'date_assigned_case_manager=VALUES(date_assigned_case_manager), eacs_recommended=VALUES(eacs_recommended), ',
      'enrolled_in_psychosocial_support_group=VALUES(enrolled_in_psychosocial_support_group), ',
      'robust_literacy_interventions_date=VALUES(robust_literacy_interventions_date), ',
      'expanding_differentiated_service_delivery_interventions=VALUES(expanding_differentiated_service_delivery_interventions), ',
      'enrolled_in_nishauri=VALUES(enrolled_in_nishauri), ',
      'expanded_differentiated_service_delivery_interventions_date=VALUES(expanded_differentiated_service_delivery_interventions_date), ',
      'date_created=VALUES(date_created), date_last_modified=VALUES(date_last_modified);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing ', @target_table, ' Time: ', NOW()) AS status;
END $$



-- sql

-- --------------------------------------
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- PROCEDURE: sp_update_etl_home_visit_checklist
-- Purpose: update tenant-aware etl_home_visit_checklist
-- Tenant-aware: calls sp_set_tenant_session_vars() and builds dynamic INSERT target
-- --------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_home_visit_checklist $$
CREATE PROCEDURE sp_update_etl_home_visit_checklist(IN last_update_time DATETIME)
BEGIN
  -- ensure tenant session variables are set (etl_schema etc.)
CALL sp_set_tenant_session_vars();

-- fully qualified target table for the current tenant
SET @target_table = CONCAT('', etl_schema, '.etl_home_visit_checklist');

  SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
    'uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, independence_in_daily_activities, other_independence_activities, meeting_basic_needs, other_basic_needs, disclosure_to_sexual_partner, disclosure_to_household_members, disclosure_to, mode_of_storing_arv_drugs, arv_drugs_taking_regime, receives_household_social_support, household_social_support_given, receives_community_social_support, community_social_support_given, linked_to_non_clinical_services, linked_to_other_services, has_mental_health_issues, suffering_stressful_situation, uses_drugs_alcohol, has_side_medications_effects, medication_side_effects, assessment_notes, date_created, date_last_modified',
    ') ',
    'SELECT ',
      'e.uuid, e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
      'CONCAT_WS('','', ',
        'NULLIF(MAX(IF(o.concept_id = 162063 AND o.value_coded = 161650, ''Feeding'', NULL)), ''''), ',
        'NULLIF(MAX(IF(o.concept_id = 162063 AND o.value_coded = 159438, ''Grooming'', NULL)), ''''), ',
        'NULLIF(MAX(IF(o.concept_id = 162063 AND o.value_coded = 1000360, ''Toileting'', NULL)), ''''), ',
        'NULLIF(MAX(IF(o.concept_id = 162063 AND o.value_coded = 5622, ''Other'', NULL)), '''')',
      ') AS independence_in_daily_activities, ',
      'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS other_independence_activities, ',
      'CONCAT_WS('','', ',
        'NULLIF(MAX(IF(o.concept_id = 168076 AND o.value_coded = 165474, ''Clothing'', NULL)), ''''), ',
        'NULLIF(MAX(IF(o.concept_id = 168076 AND o.value_coded = 159597, ''Food'', NULL)), ''''), ',
        'NULLIF(MAX(IF(o.concept_id = 168076 AND o.value_coded = 157519, ''Shelter'', NULL)), ''''), ',
        'NULLIF(MAX(IF(o.concept_id = 168076 AND o.value_coded = 5622, ''Other'', NULL)), '''')',
      ') AS meeting_basic_needs, ',
      'MAX(IF(o.concept_id = 162725, o.value_text, NULL)) AS other_basic_needs, ',
      'MAX(IF(o.concept_id = 167144, o.value_coded, NULL)) AS disclosure_to_sexual_partner, ',
      'MAX(IF(o.concept_id = 159425, o.value_coded, NULL)) AS disclosure_to_household_members, ',
      'MAX(IF(o.concept_id = 163108, o.value_text, NULL)) AS disclosure_to, ',
      'MAX(IF(o.concept_id = 165250, o.value_text, NULL)) AS mode_of_storing_arv_drugs, ',
      'MAX(IF(o.concept_id = 163104, o.value_text, NULL)) AS arv_drugs_taking_regime, ',
      'MAX(IF(o.concept_id = 165302, o.value_coded, NULL)) AS receives_household_social_support, ',
      'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS household_social_support_given, ',
      'MAX(IF(o.concept_id = 165052, o.value_coded, NULL)) AS receives_community_social_support, ',
      'MAX(IF(o.concept_id = 165225, o.value_text, NULL)) AS community_social_support_given, ',
      'CONCAT_WS('','', ',
        'NULLIF(MAX(IF(o.concept_id = 159550 AND o.value_coded = 167814, ''Legal'', NULL)), ''''), ',
        'NULLIF(MAX(IF(o.concept_id = 159550 AND o.value_coded = 115125, ''Nutritional'', NULL)), ''''), ',
        'NULLIF(MAX(IF(o.concept_id = 159550 AND o.value_coded = 167180, ''Spiritual'', NULL)), ''''), ',
        'NULLIF(MAX(IF(o.concept_id = 159550 AND o.value_coded = 5622, ''Other'', NULL)), '''')',
      ') AS linked_to_non_clinical_services, ',
      'MAX(IF(o.concept_id = 164879, o.value_text, NULL)) AS linked_to_other_services, ',
      'MAX(IF(o.concept_id = 165034, o.value_coded, NULL)) AS has_mental_health_issues, ',
      'MAX(IF(o.concept_id = 165241, o.value_coded, NULL)) AS suffering_stressful_situation, ',
      'MAX(IF(o.concept_id = 1288, o.value_coded, NULL)) AS uses_drugs_alcohol, ',
      'MAX(IF(o.concept_id = 159935, o.value_coded, NULL)) AS has_side_medications_effects, ',
      'MAX(IF(o.concept_id = 163076, o.value_text, NULL)) AS medication_side_effects, ',
      'MAX(IF(o.concept_id = 162169, o.value_text, NULL)) AS assessment_notes, ',
      'e.date_created, e.date_changed ',
    'FROM encounter e ',
      'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
      'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''ac3152de-1728-4786-828a-7fb4db0fc384'' ',
      'LEFT JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (162063,160632,162725,163108,163104,161011,168076,167144,159425,165302,165225,164879,165250,165052,159550,165034,165241,1288,159935,163076,162169) AND o.voided = 0 ',
    'WHERE e.voided = 0 AND (',
      'e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
      'o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
    ') ',
    'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
    'ON DUPLICATE KEY UPDATE ',
      'provider=VALUES(provider), visit_date=VALUES(visit_date), independence_in_daily_activities=VALUES(independence_in_daily_activities), other_independence_activities=VALUES(other_independence_activities), meeting_basic_needs=VALUES(meeting_basic_needs), other_basic_needs=VALUES(other_basic_needs), disclosure_to_sexual_partner=VALUES(disclosure_to_sexual_partner), disclosure_to_household_members=VALUES(disclosure_to_household_members), disclosure_to=VALUES(disclosure_to), mode_of_storing_arv_drugs=VALUES(mode_of_storing_arv_drugs), arv_drugs_taking_regime=VALUES(arv_drugs_taking_regime), receives_household_social_support=VALUES(receives_household_social_support), household_social_support_given=VALUES(household_social_support_given), receives_community_social_support=VALUES(receives_community_social_support), community_social_support_given=VALUES(community_social_support_given), linked_to_non_clinical_services=VALUES(linked_to_non_clinical_services), linked_to_other_services=VALUES(linked_to_other_services), has_mental_health_issues=VALUES(has_mental_health_issues), suffering_stressful_situation=VALUES(suffering_stressful_situation), uses_drugs_alcohol=VALUES(uses_drugs_alcohol), has_side_medications_effects=VALUES(has_side_medications_effects), medication_side_effects=VALUES(medication_side_effects), assessment_notes=VALUES(assessment_notes), date_created=VALUES(date_created), date_last_modified=VALUES(date_last_modified);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing ', @target_table, ' Time: ', NOW()) AS status;
END $$



-- sql
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- --------------------------------------
-- PROCEDURE: sp_update_etl_ncd_enrollment
-- Purpose: update tenant-aware etl_ncd_enrollment
-- Tenant-aware: calls sp_set_tenant_session_vars() and inserts into dynamic target
-- --------------------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_ncd_enrollment $$
CREATE PROCEDURE sp_update_etl_ncd_enrollment(IN last_update_time DATETIME)
BEGIN
  -- ensure tenant variables (sets etl_schema etc.)
CALL sp_set_tenant_session_vars();

-- build target table name for tenant
SET @target_table = CONCAT('', etl_schema, '.etl_ncd_enrollment');

SELECT CONCAT('Processing NCD Enrollment data ', 'Target: ', @target_table, ' Time: ', NOW()) AS status;

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
'patient_id, uuid, provider, visit_id, visit_date, encounter_id, location_id, visit_type, ',
'referred_from, referred_from_department, referred_from_department_other, patient_complaint, specific_complaint, ',
'disease_type, diabetes_condition, diabetes_type, hypertension_condition, hypertension_stage, hypertension_type, ',
'comorbid_condition, diagnosis_date, hiv_status, hiv_positive_on_art, tb_screening, smoke_check, date_stopped_smoke, ',
'drink_alcohol, date_stopped_alcohol, cessation_counseling, physical_activity, diet_routine, existing_complications, ',
'other_existing_complications, new_complications, other_new_complications, examination_findings, cardiovascular, respiratory, ',
'abdominal_pelvic, neurological, oral_exam, foot_risk, foot_low_risk, foot_high_risk, diabetic_foot, describe_diabetic_foot_type, ',
'treatment_given, other_treatment_given, lifestyle_advice, nutrition_assessment, footcare_outcome, referred_to, reasons_for_referral, ',
'clinical_notes, date_created, date_last_modified, voided) ',
'SELECT ',
'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.encounter_id, e.location_id, ',
'MAX(IF(o.concept_id = 164181, o.value_coded, NULL)) AS visit_type, ',
'MAX(IF(o.concept_id = 161550, o.value_text, NULL)) AS referred_from, ',
'MAX(IF(o.concept_id = 159371, o.value_coded, NULL)) AS referred_from_department, ',
'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS referred_from_department_other, ',
'MAX(IF(o.concept_id = 1628, o.value_coded, NULL)) AS patient_complaint, ',
'CONCAT_WS('','', ',
'NULLIF(MAX(IF(o.concept_id=5219 AND o.value_coded =147104, ''Blurring of vision'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=5219 AND o.value_coded =135592, ''Loss of consciousness'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=5219 AND o.value_coded =156046, ''Recurrent dizziness'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=5219 AND o.value_coded =116860, ''Foot complaints'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=5219 AND o.value_coded =141600, ''Shortness of breath on activity'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=5219 AND o.value_coded =130987, ''Palpitations (Heart racing)'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=5219 AND o.value_coded =6005, ''Focal weakness'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=5219 AND o.value_coded =112961, ''Fainting'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=5219 AND o.value_coded =5622, ''Other'', '''')), '''') ) AS specific_complaint, ',
'MAX(IF(o.concept_id = 1000485, o.value_coded, NULL)) AS disease_type, ',
'MAX(IF(o.concept_id = 119481, o.value_coded, NULL)) AS diabetes_condition, ',
'MAX(IF(o.concept_id = 152909, o.value_coded, NULL)) AS diabetes_type, ',
'MAX(IF(o.concept_id = 160223, o.value_coded, NULL)) AS hypertension_condition, ',
'MAX(IF(o.concept_id = 162725, o.value_text, NULL)) AS hypertension_stage, ',
'MAX(IF(o.concept_id = 162725, o.value_text, NULL)) AS hypertension_type, ',
'MAX(IF(o.concept_id = 162747, o.value_coded, NULL)) AS comorbid_condition, ',
'MAX(IF(o.concept_id = 162869, o.value_datetime, NULL)) AS diagnosis_date, ',
'MAX(IF(o.concept_id = 1169, o.value_coded, NULL)) AS hiv_status, ',
'MAX(IF(o.concept_id = 163783, o.value_coded, NULL)) AS hiv_positive_on_art, ',
'MAX(IF(o.concept_id = 165198, o.value_coded, NULL)) AS tb_screening, ',
'MAX(IF(o.concept_id = 152722, o.value_coded, NULL)) AS smoke_check, ',
'MAX(IF(o.concept_id = 1191, o.value_datetime, NULL)) AS date_stopped_smoke, ',
'MAX(IF(o.concept_id = 159449, o.value_coded, NULL)) AS drink_alcohol, ',
'MAX(IF(o.concept_id = 1191, o.value_datetime, NULL)) AS date_stopped_alcohol, ',
'MAX(IF(o.concept_id = 1455, o.value_coded, NULL)) AS cessation_counseling, ',
'MAX(IF(o.concept_id = 1000519, o.value_coded, NULL)) AS physical_activity, ',
'MAX(IF(o.concept_id = 1000520, o.value_coded, NULL)) AS diet_routine, ',
'CONCAT_WS('','', ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =111103, ''Stroke'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =159298, ''Retinopathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =139069, ''Heart failure'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =163411, ''Diabetic Foot'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =142451, ''Diabetic Foot Ulcer'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =6033, ''Kidney Failure(CKD)'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =116123, ''Erectile dysfunction'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =118983, ''Peripheral Neuropathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =116506, ''Nephropathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =116608, ''Ischaemic Heart Disease'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =114212, ''Peripheral Vascular Disease'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =118189, ''Gastropathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =120860, ''Cataracts'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =119270, ''Cardiovascular Disease (CVD)'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =142587, ''Dental complications'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =141623, ''Dyslipidemia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =115115, ''Obesity'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =138406, ''HIV'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =115753, ''TB'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =5622, ''Other'', '''')), '''') ) AS existing_complications, ',
'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS other_existing_complications, ',
'CONCAT_WS('','', ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =111103, ''Stroke'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =159298, ''Retinopathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =139069, ''Heart failure'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =163411, ''Diabetic Foot'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =142451, ''Diabetic Foot Ulcer'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =6033, ''Kidney Failure(CKD)'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =116123, ''Erectile dysfunction'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =118983, ''Peripheral Neuropathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =116506, ''Nephropathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =116608, ''Ischaemic Heart Disease'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =114212, ''Peripheral Vascular Disease'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =118189, ''Gastropathy'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =120860, ''Cataracts'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =119270, ''Cardiovascular Disease (CVD)'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =142587, ''Dental complications'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =141623, ''Dyslipidemia'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =115115, ''Obesity'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =138406, ''HIV'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =115753, ''TB'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=120240 AND o.value_coded =5622, ''Other'', '''')), '''') ) AS new_complications, ',
'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS other_new_complications, ',
'CONCAT_WS('','', ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =1107, ''None'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =143050, ''Cyanosis'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =142630, ''Dehydration'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =140125, ''Finger Clubbing'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =136443, ''Jaundice'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =116334, ''Lethargic'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =126952, ''Lymph Node Axillary'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =126939, ''Lymph Nodes Inguinal'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =1861, ''Nasal Flaring'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =460, ''Oedema'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =5334, ''Oral thrush'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =5245, ''Pallor'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =206, ''Convulsions'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =823, ''Wasting'', '''')), '''') ) AS examination_findings, ',
'MAX(IF(o.concept_id = 1124, o.value_coded, NULL)) AS cardiovascular, ',
'MAX(IF(o.concept_id = 1124, o.value_coded, NULL)) AS respiratory, ',
'MAX(IF(o.concept_id = 1124, o.value_coded, NULL)) AS abdominal_pelvic, ',
'MAX(IF(o.concept_id = 1124, o.value_coded, NULL)) AS neurological, ',
'MAX(IF(o.concept_id = 163308, o.value_coded, NULL)) AS oral_exam, ',
'MAX(IF(o.concept_id = 166879, o.value_coded, NULL)) AS foot_risk, ',
'CONCAT_WS('','', ',
'NULLIF(MAX(IF(o.concept_id=166676 AND o.value_coded =164188, ''Intact protective sensation'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=166676 AND o.value_coded =158955, ''Pedal Pulses Present'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=166676 AND o.value_coded =155871, ''No deformity'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=166676 AND o.value_coded =123919, ''No prior foot ulcer'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=166676 AND o.value_coded =164009, ''No amputation'', '''')), '''') ) AS foot_low_risk, ',
'CONCAT_WS('','', ',
'NULLIF(MAX(IF(o.concept_id=1284 AND o.value_coded =166844, ''Loss of protective sensation'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=1284 AND o.value_coded =150518, ''Absent pedal pulses'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=1284 AND o.value_coded =142677, ''Foot deformity'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=1284 AND o.value_coded =123919, ''History of foot ulcer'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=1284 AND o.value_coded =164009, ''Prior amputation'', '''')), '''') ) AS foot_high_risk, ',
'MAX(IF(o.concept_id = 1284, o.value_coded, NULL)) AS diabetic_foot, ',
'MAX(IF(o.concept_id = 165250, o.value_text, NULL)) AS describe_diabetic_foot_type, ',
'CONCAT_WS('','', ',
'NULLIF(MAX(IF(o.concept_id=166665 AND o.value_coded =168812, ''Diet & physical activity'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=166665 AND o.value_coded =167915, ''Oral glucose-lowering agents (OGLAs)'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=166665 AND o.value_coded =167962, ''Insulin and OGLAs'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=166665 AND o.value_coded =78056, ''Insulin'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=166665 AND o.value_coded =2024964, ''Anti hypertensives'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=166665 AND o.value_coded =2028777, ''Herbal'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=166665 AND o.value_coded =5622, ''Others'', '''')), '''') ) AS treatment_given, ',
'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS other_treatment_given, ',
'CONCAT_WS('','', ',
'NULLIF(MAX(IF(o.concept_id=165070 AND o.value_coded =168812, ''Physical Activities'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=165070 AND o.value_coded =168807, ''Support Group'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=165070 AND o.value_coded =900009, ''Nutrition'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=165070 AND o.value_coded =2022484, ''Mental wellbeing'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=165070 AND o.value_coded =121712, ''Alcohol'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=165070 AND o.value_coded =156830, ''Alcohol Cessation'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=165070 AND o.value_coded =137093, ''Tobacco Cessation'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=165070 AND o.value_coded =1000023, ''Other substances'', '''')), ''''), ',
'NULLIF(MAX(IF(o.concept_id=165070 AND o.value_coded =2028777, ''Herbal use or alternative therapies advise remarks'', '''')), '''') ) AS lifestyle_advice, ',
'MAX(IF(o.concept_id = 165250, o.value_text, NULL)) AS nutrition_assessment, ',
'MAX(IF(o.concept_id = 162737, o.value_coded, NULL)) AS footcare_outcome, ',
'MAX(IF(o.concept_id = 162724, o.value_text, NULL)) AS referred_to, ',
'MAX(IF(o.concept_id = 159623, o.value_text, NULL)) AS reasons_for_referral, ',
'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS clinical_notes, ',
'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, e.voided ',
'FROM encounter e ',
' INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
' INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''c4994dd7-f2b6-4c28-bdc7-8b1d9d2a6a97'' ',
' INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (164181,161550,159371,161011,1628,5219,1000485,119481,152909,160223,162725,162747,162869,1169,163783,165198,152722,1191,1455,1000519,1000520,6042,120240,162737,1124,163308,166879,166676,1284,165250,166665,165070,162724,159623,160632) AND o.voided = 0 ',
' WHERE e.voided = 0 ',
' GROUP BY e.encounter_id ',
'ON DUPLICATE KEY UPDATE ',
'provider=VALUES(provider), visit_date=VALUES(visit_date), patient_complaint=VALUES(patient_complaint), specific_complaint=VALUES(specific_complaint), ',
'disease_type=VALUES(disease_type), diabetes_condition=VALUES(diabetes_condition), diabetes_type=VALUES(diabetes_type), ',
'hypertension_condition=VALUES(hypertension_condition), hypertension_stage=VALUES(hypertension_stage), hypertension_type=VALUES(hypertension_type), ',
'comorbid_condition=VALUES(comorbid_condition), diagnosis_date=VALUES(diagnosis_date), hiv_status=VALUES(hiv_status), ',
'hiv_positive_on_art=VALUES(hiv_positive_on_art), tb_screening=VALUES(tb_screening), smoke_check=VALUES(smoke_check), ',
'date_stopped_smoke=VALUES(date_stopped_smoke), drink_alcohol=VALUES(drink_alcohol), date_stopped_alcohol=VALUES(date_stopped_alcohol), ',
'cessation_counseling=VALUES(cessation_counseling), physical_activity=VALUES(physical_activity), diet_routine=VALUES(diet_routine), ',
'examination_findings=VALUES(examination_findings), cardiovascular=VALUES(cardiovascular), respiratory=VALUES(respiratory), ',
'abdominal_pelvic=VALUES(abdominal_pelvic), neurological=VALUES(neurological), oral_exam=VALUES(oral_exam), foot_risk=VALUES(foot_risk), ',
'foot_low_risk=VALUES(foot_low_risk), foot_high_risk=VALUES(foot_high_risk), diabetic_foot=VALUES(diabetic_foot), treatment_given=VALUES(treatment_given), ',
'lifestyle_advice=VALUES(lifestyle_advice), nutrition_assessment=VALUES(nutrition_assessment), footcare_outcome=VALUES(footcare_outcome), ',
'clinical_notes=VALUES(clinical_notes), date_created=VALUES(date_created), date_last_modified=VALUES(date_last_modified), voided=VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing ', @target_table, ' Time: ', NOW()) AS status;
END $$


-- sql

-- --------------------------------------
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- PROCEDURE: sp_update_etl_adr_assessment_tool
-- Purpose: update tenant-aware etl_adr_assessment_tool
-- Tenant-aware: calls sp_set_tenant_session_vars() and inserts into dynamic target
-- --------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_adr_assessment_tool $$
CREATE PROCEDURE sp_update_etl_adr_assessment_tool(IN last_update_time DATETIME)
BEGIN
  -- set tenant session variables (must be implemented elsewhere)
CALL sp_set_tenant_session_vars();

SET @target_table = CONCAT('', etl_schema, '.etl_adr_assessment_tool');

  SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (',
' uuid, provider, patient_id, visit_id, visit_date, location_id, encounter_id, ',
' weight_taken, weight_not_taken_specify, taking_arvs_everyday, not_taking_arvs_everyday, ',
' correct_dosage_per_weight, dosage_not_correct_specify, arv_dosage_frequency, other_medication_dosage_frequency, ',
' arv_medication_time, arv_timing_working, arv_timing_not_working_specify, other_medication_time, ',
' other_medication_timing_working, other_medication_time_not_working_specify, arv_frequency_difficult_to_follow, ',
' difficult_arv_to_follow_specify, difficulty_with_arv_tablets_or_liquids, difficulty_with_arv_tablets_or_liquids_specify, ',
' othe_drugs_frequency_difficult_to_follow, difficult_other_drugs_to_follow_specify, difficulty_other_drugs_tablets_or_liquids, ',
' difficulty_other_drugs_tablets_or_liquids_specify, arv_difficulty_due_to_taste_or_size, arv_difficulty_due_to_taste_or_size_specify, ',
' arv_symptoms_on_intake, laboratory_abnormalities, laboratory_abnormalities_specify, summary_findings, ',
' severity_of_reaction, reaction_seriousness, reason_for_seriousness, action_taken_on_reaction, ',
' reaction_resolved_on_dose_change, reaction_reappeared_after_drug_introduced, laboratory_investigations_done, outcome, ',
' reported_adr_to_pharmacy_board, name_of_adr, adr_report_number, date_created, date_last_modified',
') ',
'SELECT ',
' e.uuid, e.creator, e.patient_id, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.location_id, e.encounter_id, ',
' MAX(IF(o.concept_id = 163515, o.value_coded, NULL)) AS weight_taken, ',
' MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS weight_not_taken_specify, ',
' MAX(IF(o.concept_id = 162736, o.value_coded, NULL)) AS taking_arvs_everyday, ',
' MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS not_taking_arvs_everyday, ',
' MAX(IF(o.concept_id = 160582, o.value_coded, NULL)) AS correct_dosage_per_weight, ',
' MAX(IF(o.concept_id = 164378, o.value_text, NULL)) AS dosage_not_correct_specify, ',
' MAX(IF(o.concept_id = 160855, o.value_coded, NULL)) AS arv_dosage_frequency, ',
' MAX(IF(o.concept_id = 159367, o.value_coded, NULL)) AS other_medication_dosage_frequency, ',
' MAX(IF(o.concept_id = 161076, o.value_coded, NULL)) AS arv_medication_time, ',
' MAX(IF(o.concept_id = 160119, o.value_coded, NULL)) AS arv_timing_working, ',
' MAX(IF(o.concept_id = 164879, o.value_text, NULL)) AS arv_timing_not_working_specify, ',
' MAX(IF(o.concept_id = 1724, o.value_coded, NULL)) AS other_medication_time, ',
' MAX(IF(o.concept_id = 1417, o.value_coded, NULL)) AS other_medication_timing_working, ',
' MAX(IF(o.concept_id = 160618, o.value_text, NULL)) AS other_medication_time_not_working_specify, ',
' MAX(IF(o.concept_id = 163331, o.value_coded, NULL)) AS arv_frequency_difficult_to_follow, ',
' MAX(IF(o.concept_id = 163322, o.value_text, NULL)) AS difficult_arv_to_follow_specify, ',
' MAX(IF(o.concept_id = 161911, o.value_coded, NULL)) AS difficulty_with_arv_tablets_or_liquids, ',
' MAX(IF(o.concept_id = 159395, o.value_text, NULL)) AS difficulty_with_arv_tablets_or_liquids_specify, ',
' MAX(IF(o.concept_id = 1803, o.value_coded, NULL)) AS othe_drugs_frequency_difficult_to_follow, ',
' MAX(IF(o.concept_id = 165399, o.value_text, NULL)) AS difficult_other_drugs_to_follow_specify, ',
' MAX(IF(o.concept_id = 1198, o.value_coded, NULL)) AS difficulty_other_drugs_tablets_or_liquids, ',
' MAX(IF(o.concept_id = 162169, o.value_text, NULL)) AS difficulty_other_drugs_tablets_or_liquids_specify, ',
' MAX(IF(o.concept_id = 166365, o.value_coded, NULL)) AS arv_difficulty_due_to_taste_or_size, ',
' MAX(IF(o.concept_id = 162749, o.value_text, NULL)) AS arv_difficulty_due_to_taste_or_size_specify, ',
' CONCAT_WS('','', ',
'  MAX(IF(o.concept_id = 1729 AND o.value_coded = 122983, ''Vomiting'', NULL)), ',
'  MAX(IF(o.concept_id = 1729 AND o.value_coded = 832, ''Rapid or excessive weight loss'', NULL)), ',
'  MAX(IF(o.concept_id = 1729 AND o.value_coded = 140937, ''Rapid or excessive weight gain'', NULL)), ',
'  MAX(IF(o.concept_id = 1729 AND o.value_coded = 879, ''Itching of the skin'', NULL)), ',
'  MAX(IF(o.concept_id = 1729 AND o.value_coded = 139084, ''Headache'', NULL)), ',
'  MAX(IF(o.concept_id = 1729 AND o.value_coded = 142412, ''Diarrhea'', NULL)), ',
'  MAX(IF(o.concept_id = 1729 AND o.value_coded = 5192, ''Yellowness of eyes'', NULL)), ',
'  MAX(IF(o.concept_id = 1729 AND o.value_coded = 512, ''Rash on the skin'', NULL)), ',
'  MAX(IF(o.concept_id = 1729 AND o.value_coded = 141830, ''Dizziness'', NULL)), ',
'  MAX(IF(o.concept_id = 1729 AND o.value_coded = 141597, ''Sleep Disturbance'', NULL)), ',
'  MAX(IF(o.concept_id = 1729 AND o.value_coded = 137601, ''Increased appetite'', NULL)), ',
'  MAX(IF(o.concept_id = 1729 AND o.value_coded = 6031, ''Decreased appetite'', NULL)), ',
'  MAX(IF(o.concept_id = 1729 AND o.value_coded = 5622, ''Other changes or concerns'', NULL)) ',
' ) AS arv_symptoms_on_intake, ',
' MAX(IF(o.concept_id = 164217, o.value_coded, NULL)) AS laboratory_abnormalities, ',
' MAX(IF(o.concept_id = 1356, o.value_coded, NULL)) AS laboratory_abnormalities_specify, ',
' MAX(IF(o.concept_id = 162165, o.value_text, NULL)) AS summary_findings, ',
' MAX(IF(o.concept_id = 162760, o.value_coded, NULL)) AS severity_of_reaction, ',
' MAX(IF(o.concept_id = 162867, o.value_coded, NULL)) AS reaction_seriousness, ',
' MAX(IF(o.concept_id = 168296, o.value_coded, NULL)) AS reason_for_seriousness, ',
' MAX(IF(o.concept_id = 1255, o.value_coded, NULL)) AS action_taken_on_reaction, ',
' MAX(IF(o.concept_id = 6097, o.value_coded, NULL)) AS reaction_resolved_on_dose_change, ',
' MAX(IF(o.concept_id = 159924, o.value_coded, NULL)) AS reaction_reappeared_after_drug_introduced, ',
' MAX(IF(o.concept_id = 164422, o.value_text, NULL)) AS laboratory_investigations_done, ',
' MAX(IF(o.concept_id = 163105, o.value_coded, NULL)) AS outcome, ',
' MAX(IF(o.concept_id = 162871, o.value_coded, NULL)) AS reported_adr_to_pharmacy_board, ',
' MAX(IF(o.concept_id = 162872, o.value_text, NULL)) AS name_of_adr, ',
' MAX(IF(o.concept_id = 162054, o.value_text, NULL)) AS adr_report_number, ',
' e.date_created, e.date_changed ',
'FROM encounter e ',
' INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
' INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''cc27af13-69ee-49e2-8a43-e1b1926403c1'' ',
' LEFT JOIN obs o ON o.encounter_id = e.encounter_id ',
'   AND o.concept_id IN (162054,162872,162871,163105,164422,160119,1724,162736,159367,159924,6097,1255,168296,162867,160618,163515,160632,160855,162760,162165,1356,164217,1729,162749,163322,163331,160582,1417,166365,162169,1198,165399,1803,159395,161011,164378,161076,161911,164879) ',
'   AND o.voided = 0 ',
'WHERE e.voided = 0 AND (',
' e.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
' e.date_changed >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
' e.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
' o.date_created >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'), ' OR ',
' o.date_voided >= ', IFNULL(CONCAT('''', last_update_time, ''''), 'NULL'),
') ',
'GROUP BY e.patient_id, DATE(e.encounter_datetime) ',
'ON DUPLICATE KEY UPDATE ',
' provider=VALUES(provider), visit_date=VALUES(visit_date), ',
' weight_taken=VALUES(weight_taken), weight_not_taken_specify=VALUES(weight_not_taken_specify), ',
' taking_arvs_everyday=VALUES(taking_arvs_everyday), not_taking_arvs_everyday=VALUES(not_taking_arvs_everyday), ',
' correct_dosage_per_weight=VALUES(correct_dosage_per_weight), dosage_not_correct_specify=VALUES(dosage_not_correct_specify), ',
' arv_dosage_frequency=VALUES(arv_dosage_frequency), other_medication_dosage_frequency=VALUES(other_medication_dosage_frequency), ',
' arv_medication_time=VALUES(arv_medication_time), arv_timing_working=VALUES(arv_timing_working), ',
' arv_timing_not_working_specify=VALUES(arv_timing_not_working_specify), other_medication_time=VALUES(other_medication_time), ',
' other_medication_timing_working=VALUES(other_medication_timing_working), other_medication_time_not_working_specify=VALUES(other_medication_time_not_working_specify), ',
' arv_frequency_difficult_to_follow=VALUES(arv_frequency_difficult_to_follow), difficult_arv_to_follow_specify=VALUES(difficult_arv_to_follow_specify), ',
' difficulty_with_arv_tablets_or_liquids=VALUES(difficulty_with_arv_tablets_or_liquids), difficulty_with_arv_tablets_or_liquids_specify=VALUES(difficulty_with_arv_tablets_or_liquids_specify), ',
' othe_drugs_frequency_difficult_to_follow=VALUES(othe_drugs_frequency_difficult_to_follow), difficult_other_drugs_to_follow_specify=VALUES(difficult_other_drugs_to_follow_specify), ',
' difficulty_other_drugs_tablets_or_liquids=VALUES(difficulty_other_drugs_tablets_or_liquids), difficulty_other_drugs_tablets_or_liquids_specify=VALUES(difficulty_other_drugs_tablets_or_liquids_specify), ',
' arv_difficulty_due_to_taste_or_size=VALUES(arv_difficulty_due_to_taste_or_size), arv_difficulty_due_to_taste_or_size_specify=VALUES(arv_difficulty_due_to_taste_or_size_specify), ',
' arv_symptoms_on_intake=VALUES(arv_symptoms_on_intake), laboratory_abnormalities=VALUES(laboratory_abnormalities), ',
' laboratory_abnormalities_specify=VALUES(laboratory_abnormalities_specify), summary_findings=VALUES(summary_findings), ',
' severity_of_reaction=VALUES(severity_of_reaction), reaction_seriousness=VALUES(reaction_seriousness), ',
' reason_for_seriousness=VALUES(reason_for_seriousness), action_taken_on_reaction=VALUES(action_taken_on_reaction), ',
' reaction_resolved_on_dose_change=VALUES(reaction_resolved_on_dose_change), reaction_reappeared_after_drug_introduced=VALUES(reaction_reappeared_after_drug_introduced), ',
' laboratory_investigations_done=VALUES(laboratory_investigations_done), outcome=VALUES(outcome), ',
' reported_adr_to_pharmacy_board=VALUES(reported_adr_to_pharmacy_board), name_of_adr=VALUES(name_of_adr), adr_report_number=VALUES(adr_report_number), ',
' date_created=VALUES(date_created), date_last_modified=VALUES(date_last_modified);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing ', @target_table, ' Time: ', NOW()) AS status;
END $$


-- sql

-- --------------------------------------
-- File: src/main/resources/sql/hiv/Scheduled_Updates.sql
-- PROCEDURE: sp_update_etl_ncd_followup
-- Purpose: update tenant-aware etl_ncd_followup
-- Tenant-aware: calls sp_set_tenant_session_vars() and inserts into dynamic target
-- --------------------------------------
DROP PROCEDURE IF EXISTS sp_update_etl_ncd_followup $$
CREATE PROCEDURE sp_update_etl_ncd_followup(IN last_update_time DATETIME)
BEGIN
CALL sp_set_tenant_session_vars();

SET @target_table = CONCAT(@etl_schema, '.etl_ncd_followup');

  SET @sql = CONCAT(
'INSERT INTO ', @target_table, ' (',
'patient_id, uuid, provider, visit_id, visit_date, encounter_id, location_id, visit_type, ',
'tobacco_use, drink_alcohol, physical_activity, healthy_diet, patient_complaint, specific_complaint, ',
'other_specific_complaint, examination_findings, cardiovascular, respiratory, abdominal_pelvic, ',
'neurological, oral_exam, foot_exam, diabetic_foot, foot_risk_assessment, diabetic_foot_risk, ',
'adhering_medication, referred_to, reasons_for_referral, clinical_notes, date_created, date_last_modified, voided',
') ',
'SELECT ',
'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.encounter_id, e.location_id, ',
'MAX(IF(o.concept_id = 164181, o.value_coded, NULL)) AS visit_type, ',
'MAX(IF(o.concept_id = 152722, o.value_coded, NULL)) AS tobacco_use, ',
'MAX(IF(o.concept_id = 159449, o.value_coded, NULL)) AS drink_alcohol, ',
'MAX(IF(o.concept_id = 1000519, o.value_coded, NULL)) AS physical_activity, ',
'MAX(IF(o.concept_id = 1000520, o.value_coded, NULL)) AS healthy_diet, ',
'MAX(IF(o.concept_id = 6042, o.value_coded, NULL)) AS patient_complaint, ',
'CONCAT_WS('','', ',
' NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =111103, ''Stroke'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =159298, ''Visual impairment'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =139069, ''Heart failure'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =163411, ''Foot ulcers/deformity'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =6033, ''Renal disease'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =116123, ''Erectile dysfunction'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =118983, ''Peripheral Neuropathy'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=6042 AND o.value_coded =5622, ''Other'', '''')), '''') ',
') AS specific_complaint, ',
'MAX(IF(o.concept_id = 161011, o.value_text, NULL)) AS other_specific_complaint, ',
'CONCAT_WS('','', ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =1107, ''None'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =143050, ''Cyanosis'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =142630, ''Dehydration'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =140125, ''Finger Clubbing'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =136443, ''Jaundice'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =116334, ''Lethargic'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =126952, ''Lymph Node Axillary'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =126939, ''Lymph Nodes Inguinal'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =1861, ''Nasal Flaring'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =460, ''Oedema'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =5334, ''Oral thrush'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =5245, ''Pallor'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =206, ''Convulsions'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=162737 AND o.value_coded =823, ''Wasting'', '''')), '''') ',
') AS examination_findings, ',
'MAX(IF(o.concept_id = 1124, o.value_coded, NULL)) AS cardiovascular, ',
'MAX(IF(o.concept_id = 1124, o.value_coded, NULL)) AS respiratory, ',
'MAX(IF(o.concept_id = 1124, o.value_coded, NULL)) AS abdominal_pelvic, ',
'MAX(IF(o.concept_id = 1124, o.value_coded, NULL)) AS neurological, ',
'MAX(IF(o.concept_id = 163308, o.value_coded, NULL)) AS oral_exam, ',
'CONCAT_WS('','', ',
' NULLIF(MAX(IF(o.concept_id=1127 AND o.value_coded =163411, ''Calluses'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=1127 AND o.value_coded =1116, ''Ulcers'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=1127 AND o.value_coded =165471, ''Deformity'', '''')), '''') ',
') AS foot_exam, ',
'MAX(IF(o.concept_id = 1284, o.value_coded, NULL)) AS diabetic_foot, ',
'CONCAT_WS('','', ',
' NULLIF(MAX(IF(o.concept_id=1284 AND o.value_coded =166844, ''Loss of sensation'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=1284 AND o.value_coded =150518, ''Absent pulses'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=1284 AND o.value_coded =142677, ''Foot deformity'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=1284 AND o.value_coded =123919, ''History of ulcer'', '''')), ''''), ',
' NULLIF(MAX(IF(o.concept_id=1284 AND o.value_coded =164009, ''Prior amputation'', '''')), '''') ',
') AS foot_risk_assessment, ',
'MAX(IF(o.concept_id = 166879, o.value_coded, NULL)) AS diabetic_foot_risk, ',
'MAX(IF(o.concept_id = 164075, o.value_coded, NULL)) AS adhering_medication, ',
'MAX(IF(o.concept_id = 162724, o.value_text, NULL)) AS referred_to, ',
'MAX(IF(o.concept_id = 159623, o.value_text, NULL)) AS reasons_for_referral, ',
'MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS clinical_notes, ',
'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, e.voided ',
'FROM encounter e ',
' INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
' INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''3e1057da-f130-44d9-b2bb-53e039b953c6'' ',
' INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (164181,152722,159449,1000519,1000520,6042,161011,162737,1124,163308,1127,1284,166879,164075,162724,159623,160632) AND o.voided = 0 ',
' WHERE e.voided = 0 ',
' GROUP BY e.encounter_id ',
' ON DUPLICATE KEY UPDATE ',
' provider=VALUES(provider), visit_date=VALUES(visit_date), visit_type=VALUES(visit_type), ',
' tobacco_use=VALUES(tobacco_use), drink_alcohol=VALUES(drink_alcohol), physical_activity=VALUES(physical_activity), ',
' healthy_diet=VALUES(healthy_diet), patient_complaint=VALUES(patient_complaint), specific_complaint=VALUES(specific_complaint), ',
' other_specific_complaint=VALUES(other_specific_complaint), examination_findings=VALUES(examination_findings), ',
' cardiovascular=VALUES(cardiovascular), respiratory=VALUES(respiratory), abdominal_pelvic=VALUES(abdominal_pelvic), ',
' neurological=VALUES(neurological), oral_exam=VALUES(oral_exam), foot_exam=VALUES(foot_exam), ',
' diabetic_foot=VALUES(diabetic_foot), foot_risk_assessment=VALUES(foot_risk_assessment), diabetic_foot_risk=VALUES(diabetic_foot_risk), ',
' adhering_medication=VALUES(adhering_medication), referred_to=VALUES(referred_to), reasons_for_referral=VALUES(reasons_for_referral), ',
' clinical_notes=VALUES(clinical_notes), date_created=VALUES(date_created), date_last_modified=VALUES(date_last_modified), voided=VALUES(voided);'
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing ', @target_table, ' Time: ', NOW()) AS status;
END $$


-- sql
-- --------------------------------------
-- PROCEDURE: sp_update_etl_inpatient_admission
-- Purpose: update tenant-aware etl_inpatient_admission
-- Tenant-aware: calls sp_set_tenant_session_vars() and uses dynamic INSERT target
-- File: src/main/resources/sql/hiv/DML.sql
-- --------------------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_inpatient_admission $$
CREATE PROCEDURE sp_update_etl_inpatient_admission(IN last_update_time DATETIME)
BEGIN
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(etl_schema, '.etl_inpatient_admission');

  SET @sql = CONCAT(
'INSERT INTO ', @target_table, ' (patient_id, uuid, provider, visit_id, visit_date, encounter_id, location_id, admission_date, payment_mode, admission_location_id, admission_location_name, date_created, date_last_modified, voided) ',
'SELECT ',
'e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.encounter_id, e.location_id, ',
'MAX(IF(o.concept_id = 1640, o.value_datetime, NULL)) AS admission_date, ',
'MAX(IF(o.concept_id = 168882, o.value_coded, NULL)) AS payment_mode, ',
'MAX(IF(o.concept_id = 169403, o.value_text, NULL)) AS admission_location_id, ',
'MAX(CASE WHEN o.concept_id = 169403 THEN (SELECT l.name FROM location l WHERE l.location_id = CAST(o.value_text AS UNSIGNED)) ELSE NULL END) AS admission_location_name, ',
'e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, e.voided ',
'FROM encounter e ',
'INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
'INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''6a90499a-7d82-4fac-9692-b8bd879f0348'' ',
'INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (1640,168882,169403) AND o.voided = 0 ',
'WHERE e.voided = 0 ',
'GROUP BY e.encounter_id ',
'ON DUPLICATE KEY UPDATE ',
' provider = VALUES(provider), ',
' visit_date = VALUES(visit_date), ',
' admission_date = VALUES(admission_date), ',
' payment_mode = VALUES(payment_mode), ',
' admission_location_id = VALUES(admission_location_id), ',
' admission_location_name = VALUES(admission_location_name), ',
' date_created = VALUES(date_created), ',
' date_last_modified = VALUES(date_last_modified), ',
' voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Successfully updated ', @target_table) AS message;
END $$



DROP PROCEDURE IF EXISTS sp_update_etl_inpatient_discharge $$
CREATE PROCEDURE sp_update_etl_inpatient_discharge(IN last_update_time DATETIME)
BEGIN
CALL sp_set_tenant_session_vars();
SET @target_table = CONCAT(etl_schema, '.etl_inpatient_discharge');

SELECT CONCAT('Processing inpatient discharge data into ', @target_table) AS message;

SET @sql = CONCAT(
    'INSERT INTO ', @target_table, ' (patient_id, uuid, provider, visit_id, visit_date, encounter_id, location_id, discharge_instructions, discharge_status, follow_up_date, followup_specialist, date_created, date_last_modified, voided) ',
    'SELECT ',
    ' e.patient_id, e.uuid, e.creator, e.visit_id, DATE(e.encounter_datetime) AS visit_date, e.encounter_id, e.location_id, ',
    ' MAX(IF(o.concept_id = 160632, o.value_text, NULL)) AS discharge_instructions, ',
    ' MAX(IF(o.concept_id = 1695, o.value_coded, NULL)) AS discharge_status, ',
    ' MAX(IF(o.concept_id = 5096, o.value_datetime, NULL)) AS follow_up_date, ',
    ' MAX(IF(o.concept_id = 167079, o.value_coded, NULL)) AS followup_specialist, ',
    ' e.date_created AS date_created, IF(MAX(o.date_created) > MIN(e.date_created), MAX(o.date_created), NULL) AS date_last_modified, e.voided ',
    'FROM encounter e ',
    ' INNER JOIN person p ON p.person_id = e.patient_id AND p.voided = 0 ',
    ' INNER JOIN form f ON f.form_id = e.form_id AND f.uuid = ''98a781d2-b777-4756-b4c9-c9b0deb3483c'' ',
    ' INNER JOIN obs o ON o.encounter_id = e.encounter_id AND o.concept_id IN (160632,1695,5096,167079) AND o.voided = 0 ',
    ' WHERE e.voided = 0 ',
    ' GROUP BY e.encounter_id ',
    ' ON DUPLICATE KEY UPDATE ',
    ' provider = VALUES(provider), ',
    ' visit_date = VALUES(visit_date), ',
    ' discharge_instructions = VALUES(discharge_instructions), ',
    ' discharge_status = VALUES(discharge_status), ',
    ' follow_up_date = VALUES(follow_up_date), ',
    ' followup_specialist = VALUES(followup_specialist), ',
    ' date_created = VALUES(date_created), ',
    ' date_last_modified = VALUES(date_last_modified), ',
    ' voided = VALUES(voided);'
  );

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONCAT('Completed processing inpatient discharge data into ', @target_table) AS message;
END $$




/* =========================================================
   First Time Setup (Master)
   ========================================================= */
DROP PROCEDURE IF EXISTS sp_scheduled_updates $$
CREATE PROCEDURE sp_scheduled_updates()
BEGIN
    DECLARE update_script_id INT(11);
    DECLARE last_update_time DATETIME;

    CALL sp_set_tenant_session_vars();

    SELECT 'Beginning scheduled update', NOW();

    -- Log start in status table
    SET @sql_log := CONCAT('INSERT INTO ', @script_status_table_quoted, ' (script_name, start_time) VALUES (''initial_population'', NOW())');
    PREPARE stmt FROM @sql_log; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    SET update_script_id = LAST_INSERT_ID();

    CALL sp_update_etl_patient_demographics(last_update_time);
CALL sp_update_etl_hiv_enrollment(last_update_time);
CALL sp_update_etl_hiv_followup(last_update_time);
CALL sp_update_etl_program_discontinuation(last_update_time);
CALL sp_update_etl_mch_enrollment(last_update_time);
CALL sp_update_etl_mch_antenatal_visit(last_update_time);
CALL sp_update_etl_mch_postnatal_visit(last_update_time);
CALL sp_update_etl_generalized_anxiety_disorder(last_update_time);
CALL sp_update_etl_tb_enrollment(last_update_time);
CALL sp_update_etl_tb_follow_up_visit(last_update_time);
CALL sp_update_etl_tb_screening(last_update_time);
CALL sp_update_etl_hei_enrolment(last_update_time);
CALL sp_update_etl_hei_immunization(last_update_time);
CALL sp_update_etl_hei_follow_up(last_update_time);
CALL sp_update_etl_mch_discharge(last_update_time);
CALL sp_update_etl_mch_delivery(last_update_time);
CALL sp_update_drug_event(last_update_time);
CALL sp_update_etl_pharmacy_extract(last_update_time);
CALL sp_update_etl_laboratory_extract(last_update_time);
CALL sp_update_hts_test(last_update_time);
CALL sp_update_hts_linkage_and_referral(last_update_time);
CALL sp_update_hts_referral(last_update_time);
CALL sp_update_etl_ipt_initiation(last_update_time);
CALL sp_update_etl_ipt_outcome(last_update_time);
CALL sp_update_etl_ipt_follow_up(last_update_time);
CALL sp_update_etl_ccc_defaulter_tracing(last_update_time);
CALL sp_update_etl_ART_preparation(last_update_time);
CALL sp_update_etl_enhanced_adherence(last_update_time);
CALL sp_update_etl_patient_triage(last_update_time);
CALL sp_update_etl_prep_enrolment(last_update_time);
CALL sp_update_etl_prep_behaviour_risk_assessment(last_update_time);
CALL sp_update_etl_prep_monthly_refill(last_update_time);
CALL sp_update_etl_prep_followup(last_update_time);
CALL sp_update_etl_progress_note(last_update_time);
CALL sp_update_etl_prep_discontinuation(last_update_time);
CALL sp_update_etl_hts_linkage_tracing(last_update_time);
CALL sp_update_etl_patient_program(last_update_time);
CALL sp_update_etl_person_address(last_update_time);
CALL sp_update_etl_otz_enrollment(last_update_time);
CALL sp_update_etl_otz_activity(last_update_time);
CALL sp_update_etl_ovc_enrolment(last_update_time);
CALL sp_update_etl_cervical_cancer_screening(last_update_time);
CALL sp_update_etl_patient_contact(last_update_time);
CALL sp_update_etl_kp_contact(last_update_time);
CALL sp_update_etl_kp_client_enrollment(last_update_time);
CALL sp_update_etl_kp_clinical_visit(last_update_time);
CALL sp_update_etl_kp_sti_treatment(last_update_time);
CALL sp_update_etl_kp_peer_calendar(last_update_time);
CALL sp_update_etl_kp_peer_tracking(last_update_time);
CALL sp_update_etl_kp_treatment_verification(last_update_time);
CALL sp_update_etl_PrEP_verification(last_update_time);
CALL sp_update_etl_alcohol_drug_abuse_screening(last_update_time);
CALL sp_update_etl_gbv_screening(last_update_time);
CALL sp_update_etl_violence_reporting(last_update_time);
CALL sp_update_etl_link_facility_tracking(last_update_time);
CALL sp_update_etl_depression_screening(last_update_time);
CALL sp_update_etl_adverse_events(last_update_time);
CALL sp_update_etl_allergy_chronic_illness(last_update_time);
CALL sp_update_etl_ipt_screening(last_update_time);
CALL sp_update_etl_pre_hiv_enrollment_art(last_update_time);
CALL sp_update_etl_covid_19_assessment(last_update_time);
CALL sp_update_etl_vmmc_enrolment(last_update_time);
CALL sp_update_etl_vmmc_circumcision_procedure(last_update_time);
CALL sp_update_etl_vmmc_client_followup(last_update_time);
CALL sp_update_etl_vmmc_post_operation_assessment(last_update_time);
CALL sp_update_etl_hts_eligibility_screening(last_update_time);
CALL sp_update_etl_drug_order(last_update_time);
CALL sp_update_etl_preventive_services(last_update_time);
CALL sp_update_etl_overdose_reporting(last_update_time);
CALL sp_update_etl_art_fast_track(last_update_time);
CALL sp_update_etl_clinical_encounter(last_update_time);
CALL sp_update_etl_pep_management_survivor(last_update_time);
CALL sp_update_etl_sgbv_pep_followup(last_update_time);
CALL sp_update_etl_sgbv_post_rape_care(last_update_time);
CALL sp_update_etl_gbv_physical_emotional_abuse(last_update_time);
CALL sp_update_etl_family_planning(last_update_time);
CALL sp_update_etl_physiotherapy(last_update_time);
CALL sp_update_etl_psychiatry(last_update_time);
CALL sp_update_etl_kvp_clinical_enrollment(last_update_time);
CALL sp_update_etl_high_iit_intervention(last_update_time);
CALL sp_update_etl_home_visit_checklist(last_update_time);
CALL sp_update_etl_patient_appointments(last_update_time);
CALL sp_update_next_appointment_dates(last_update_time);
CALL sp_update_etl_adr_assessment_tool(last_update_time);
CALL sp_update_etl_ncd_enrollment(last_update_time);
CALL sp_update_etl_ncd_followup(last_update_time);
CALL sp_update_etl_inpatient_admission(last_update_time);
CALL sp_update_etl_inpatient_discharge(last_update_time);
CALL sp_update_dashboard_table();

    SET @sql_upd := CONCAT('UPDATE ', @script_status_table_quoted, ' SET stop_time=NOW() WHERE id=', update_script_id);
    PREPARE stmt FROM @sql_upd; EXECUTE stmt; DEALLOCATE PREPARE stmt;

    SELECT update_script_id, NOW();
END $$