
SET @OLD_SQL_MODE=@@SQL_MODE $$
SET SQL_MODE='' $$
-- Procedure sp_populate_etl_daily_revenue_summary--
DROP PROCEDURE IF EXISTS sp_populate_etl_daily_revenue_summary $$
CREATE PROCEDURE sp_populate_etl_daily_revenue_summary()
BEGIN
	SELECT "Processing daily revenue summary";
    INSERT INTO kenyaemr_etl.etl_daily_revenue_summary (
        transaction_date,
        total_sales,
        ipd_cash,
        maternity,
        xray,
        lab,
        theatre,
        mortuary,
        op_treatment,
        pharmacy,
        medical_exam,
        dental,
        physio_therapy,
        occupational_therapy,
        medical_records_cards_and_files,
        ambulance,
        ent_and_other_clinics,
        other,
        cash_receipts_cash_from_daily_services,
        cash_receipt_nhif_receipt,
        cash_receipt_other_debtors_receipt,
        revenue_not_collected_patient_not_yet_paid_other_debtors,
        revenue_not_collected_patient_not_yet_paid_waivers
    )
    WITH BillLineItems AS (
        SELECT
            cbl.bill_id,
            DATE(cbl.date_created) AS date_created,
            cbl.price,
            cbl.service_id,
            cbl.payment_status
        FROM
            cashier_bill_line_item cbl
        WHERE
            cbl.payment_status IN ('PAID', 'PENDING')
    ),
         BillableServices AS (
             SELECT
                 cbs.service_id,
                 cbs.service_type
             FROM
                 cashier_billable_service cbs
         ),
         AggregatedBillItems AS (
             SELECT
                 bli.date_created,
                 SUM(CASE WHEN bs.service_type = 160542 OR bs.service_type = 167410 THEN bli.price ELSE 0 END) AS op_treatment,
                 SUM(CASE WHEN bs.service_type = 160463 THEN bli.price ELSE 0 END) AS xray,
                 SUM(CASE WHEN bs.service_type = 900007 THEN bli.price ELSE 0 END) AS lab,
                 SUM(CASE WHEN bs.service_type = 900008 THEN bli.price ELSE 0 END) AS pharmacy,
                 SUM(CASE WHEN bs.service_type = 161252 THEN bli.price ELSE 0 END) AS dental,
                 SUM(CASE WHEN bs.service_type = 160455 THEN bli.price ELSE 0 END) AS ent_and_other_clinics,
                 SUM(CASE WHEN bs.service_type = 1000032 THEN bli.price ELSE 0 END) AS maternity,
                 SUM(CASE WHEN bs.service_type = 167050 THEN bli.price ELSE 0 END) AS ipd_cash,
                 SUM(CASE WHEN bs.service_type = 168812 THEN bli.price ELSE 0 END) AS physio_therapy,
                 SUM(CASE WHEN bs.service_type = 1000472 THEN bli.price ELSE 0 END) AS mortuary,
                 SUM(CASE WHEN bs.service_type = 1377 THEN bli.price ELSE 0 END) AS ambulance,
                 SUM(CASE WHEN bs.service_type = 164834 THEN bli.price ELSE 0 END) AS theatre,
                 SUM(CASE WHEN bs.service_type = 1000209 THEN bli.price ELSE 0 END) AS occupational_therapy,
                 SUM(CASE WHEN bs.service_type = 432 THEN bli.price ELSE 0 END) AS medical_exam,
                 SUM(CASE WHEN bs.service_type = 1000234 THEN bli.price ELSE 0 END) AS medical_records_cards_and_files,
                 SUM(CASE WHEN bs.service_type NOT IN (1000234, 432, 1000209, 164834, 1377, 1000472, 168812, 160542, 167410, 167050, 1000032, 160455, 161252, 900008, 900007, 160463)
                              THEN bli.price ELSE 0 END) AS other
             FROM
                 BillLineItems bli
                     JOIN
                 BillableServices bs ON bli.service_id = bs.service_id
             GROUP BY
                 bli.date_created
         ),
         CashReceipts AS (
             SELECT
                 DATE(cbp.date_created) AS date_created,
                 SUM(CASE WHEN cpm.payment_mode_id = 1 THEN cbp.amount_tendered ELSE 0 END) AS cash_receipts_cash_from_daily_services,
                 SUM(CASE WHEN cpm.payment_mode_id = 2 THEN cbp.amount_tendered ELSE 0 END) AS cash_receipt_nhif_receipt,
                 SUM(CASE WHEN cpm.payment_mode_id NOT IN (1, 2, 3) THEN cbp.amount_tendered ELSE 0 END) AS cash_receipt_other_debtors_receipt,
                 SUM(CASE WHEN cpm.payment_mode_id = 3 THEN cbp.amount_tendered ELSE 0 END) AS revenue_not_collected_patient_not_yet_paid_waivers
             FROM
                 cashier_bill_payment cbp
                     JOIN
                 cashier_payment_mode cpm ON cpm.payment_mode_id = cbp.payment_mode_id
             GROUP BY
                 DATE(cbp.date_created)
         ),
         PendingRevenue AS (
             SELECT
                 DATE(cbl.date_created) AS date_created,
                 SUM(cbl.price) AS revenue_not_collected_patient_not_yet_paid_other_debtors
             FROM
                 cashier_bill_line_item cbl
             WHERE
                 cbl.payment_status = 'PENDING'
             GROUP BY
                 DATE(cbl.date_created)
         ),
         TotalSales AS (
             SELECT
                 bli.date_created,
                 SUM(bli.price) AS total_sales
             FROM
                 BillLineItems bli
             GROUP BY
                 bli.date_created
         )
    SELECT
        DATE(cbp.date_created) AS transaction_date,
        ts.total_sales,
        abi.ipd_cash, abi.maternity, abi.xray, abi.lab, abi.theatre, abi.mortuary,
        abi.op_treatment, abi.pharmacy, abi.medical_exam, abi.dental, abi.physio_therapy,
        abi.occupational_therapy, abi.medical_records_cards_and_files, abi.ambulance,
        abi.ent_and_other_clinics, abi.other,
        cr.cash_receipts_cash_from_daily_services, cr.cash_receipt_nhif_receipt,
        cr.cash_receipt_other_debtors_receipt,
        pr.revenue_not_collected_patient_not_yet_paid_other_debtors,
        cr.revenue_not_collected_patient_not_yet_paid_waivers
    FROM
        cashier_bill_payment cbp
            LEFT JOIN
        TotalSales ts ON ts.date_created = DATE(cbp.date_created)
            LEFT JOIN
        AggregatedBillItems abi ON abi.date_created = DATE(cbp.date_created)
            LEFT JOIN
        CashReceipts cr ON cr.date_created = DATE(cbp.date_created)
            LEFT JOIN
        PendingRevenue pr ON pr.date_created = DATE(cbp.date_created)
    GROUP BY
        DATE(cbp.date_created);

	SELECT "Completed processing Daily Revenue Summary";
END $$

-- Procedure sp_populate_etl_special_clinics
DROP PROCEDURE IF EXISTS sp_populate_etl_special_clinics $$
CREATE PROCEDURE sp_populate_etl_special_clinics()
BEGIN
SELECT "Processing special clinics";
INSERT INTO kenyaemr_etl.etl_special_clinics (
    patient_id,
    visit_id,
    encounter_id,
    uuid,
    location_id,
    provider,
    visit_date,
    visit_type,
    special_clinic,
    special_clinic_form_uuid)
select e.patient_id,
       e.visit_id,
       e.encounter_id,
       e.uuid,
       e.location_id,
       e.creator,
    date(e.encounter_datetime)                                                  as visit_date,
    max(if(o.concept_id = 164181, o.value_coded, null))                         as visit_type,
    case f.uuid
    when 'c5055956-c3bb-45f2-956f-82e114c57aa7' then 'ENT'
    when '22c68f86-bbf0-49ba-b2d1-23fa7ccf0259' then 'HIV'
    when '1fbd26f1-0478-437c-be1e-b8468bd03ffa' then 'Psychiatry'
    when '235900ff-4d4a-4575-9759-96f325f5e291' then 'Ophthamology'
    when 'beec83df-6606-4019-8223-05a54a52f2b0' then 'Orthopaedic'
    when '062a24b5-728b-4639-8176-197e8f458490' then 'Occupational Therapy'
    when '18c209ac-0787-4b51-b9aa-aa8b1581239c' then 'Physiotherapy'
    when 'b8357314-0f6a-4fc9-a5b7-339f47095d62' then 'Nutrition'
    when '31a371c6-3cfe-431f-94db-4acadad8d209' then 'Oncology'
    when 'd9f74419-e179-426e-9aff-ec97f334a075' then 'Audiology'
    when '998be6de-bd13-4136-ba0d-3f772139895f' then 'Cardiology'
    when 'efa2f992-44af-487e-aaa7-c92813a34612' then 'Dermatology'
    when 'f97f2bf3-c26b-4adf-aacd-e09d720a14cd' then 'Neurology'
    when '35ab0825-33af-49e7-ac01-bb0b05753732' then 'Obstetric'
    when '9f6543e4-0821-4f9c-9264-94e45dc35e17' then 'Diabetic'
    when 'd95e44dd-e389-42ae-a9b6-1160d8eeebc4' then 'Pediatrics'
    when '00aa7662-e3fd-44a5-8f3a-f73eb7afa437' then 'Medical'
    when 'da1f7e74-5371-4997-8a02-b7b9303ddb61' then 'Surgical'
    when 'b40d369c-31d0-4c1d-a80a-7e4b7f73bea0' then 'Maxillofacial'
    when '998be6de-bd13-4136-ba0d-3f772139895f' then 'Cardiology'
    when '32e43fc9-6de3-48e3-aafe-3b92f167753d' then 'Fertility'
    when 'a3c01460-c346-4f3d-a627-5c7de9494ba0' then 'Dental'
    when '6d0be8bd-5320-45a0-9463-60c9ee2b1338' then 'Renal'
    when '57df8a60-7585-4fc0-b51b-e10e568cf53c' then 'Urology'
    when '6b4fa553-f2b3-47d0-a4c5-fc11f38b0b24' then 'Gastroenterology' end as special_clinic,
           f.uuid                                                                      as special_clinic_form_uuid
    from encounter e
             inner join person p on p.person_id = e.patient_id and p.voided = 0
             inner join form f on f.form_id = e.form_id and f.uuid in ('c5055956-c3bb-45f2-956f-82e114c57aa7', -- ENT
                                                                       '22c68f86-bbf0-49ba-b2d1-23fa7ccf0259', -- HIV
                                                                       '1fbd26f1-0478-437c-be1e-b8468bd03ffa', -- Psychiatry
                                                                       '235900ff-4d4a-4575-9759-96f325f5e291', -- Ophthamology
                                                                       'beec83df-6606-4019-8223-05a54a52f2b0', -- Orthopaedic
                                                                       '35ab0825-33af-49e7-ac01-bb0b05753732', -- Obstetric/Gynaecology
                                                                       '062a24b5-728b-4639-8176-197e8f458490', -- Occupational Therapy Clinic
                                                                       '18c209ac-0787-4b51-b9aa-aa8b1581239c', -- Physiotherapy
                                                                       'b8357314-0f6a-4fc9-a5b7-339f47095d62', -- Nutrition
                                                                       '31a371c6-3cfe-431f-94db-4acadad8d209', -- Oncology
                                                                       'd9f74419-e179-426e-9aff-ec97f334a075', -- Audiology
                                                                       '998be6de-bd13-4136-ba0d-3f772139895f', -- Cardiology
                                                                       'efa2f992-44af-487e-aaa7-c92813a34612', -- Dermatology
                                                                       'f97f2bf3-c26b-4adf-aacd-e09d720a14cd', -- Neurology
                                                                       '9f6543e4-0821-4f9c-9264-94e45dc35e17', -- Diabetic
                                                                       '6b4fa553-f2b3-47d0-a4c5-fc11f38b0b24', -- Gastroenterology
                                                                       '00aa7662-e3fd-44a5-8f3a-f73eb7afa437', -- Medical
                                                                       'da1f7e74-5371-4997-8a02-b7b9303ddb61', -- Surgical
                                                                       'b40d369c-31d0-4c1d-a80a-7e4b7f73bea0', -- Maxillofacial
                                                                       '32e43fc9-6de3-48e3-aafe-3b92f167753d', -- Fertility
                                                                       'a3c01460-c346-4f3d-a627-5c7de9494ba0', -- Dental
                                                                       '6d0be8bd-5320-45a0-9463-60c9ee2b1338', -- Renal
                                                                       '57df8a60-7585-4fc0-b51b-e10e568cf53c', -- Urology
                                                                       'd95e44dd-e389-42ae-a9b6-1160d8eeebc4' -- Pediatrics
        )
             left outer join obs o on o.encounter_id = e.encounter_id
        and o.voided = 0
    where e.voided = 0
    group by e.patient_id,e.encounter_id;
SELECT "Completed processing special clinics";
END $$
    -- end of dml procedures

SET sql_mode=@OLD_SQL_MODE $$

-- ------------------------------------------- running all procedures -----------------------------

DROP PROCEDURE IF EXISTS sp_facility_wide_refresh $$
CREATE PROCEDURE sp_facility_wide_refresh()
BEGIN
DECLARE populate_script_id INT(11);
SELECT "Beginning first time setup", CONCAT("Time: ", NOW());
INSERT INTO kenyaemr_etl.etl_script_status(script_name, start_time) VALUES('initial_population_of_facilitywide_tables', NOW());
SET populate_script_id = LAST_INSERT_ID();

CALL sp_populate_etl_daily_revenue_summary();
CALL sp_populate_etl_special_clinics();

UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= populate_script_id;

SELECT "Completed refreshing facility-wide tables", CONCAT("Time: ", NOW());
END $$



