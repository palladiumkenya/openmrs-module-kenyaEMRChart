
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
             WHERE
                 cbp.voided = 0
             GROUP BY
                 DATE(cbp.date_created)
         ),
         BillAmounts AS (
             -- total amount per bill and the bill's date (earliest line-item date)
             SELECT
                 bill_id,
                 SUM(price) AS bill_amount,
                 DATE(MIN(date_created)) AS bill_date
             FROM cashier_bill_line_item
             GROUP BY bill_id
         ),
         PaymentsPerBill AS (
             -- total paid per bill (exclude voided payments)
             SELECT
                 bill_id,
                 SUM(amount_tendered) AS total_paid
             FROM cashier_bill_payment
             WHERE voided = 0
             GROUP BY bill_id
         ),
         PendingBills AS (
             -- bills that have at least one PENDING line item
             SELECT DISTINCT bill_id
             FROM cashier_bill_line_item
             WHERE payment_status = 'PENDING'
         ),
         PendingRevenue AS (
             -- for each pending bill compute bill_amount - total_paid (could be zero or positive)
             SELECT
                 ba.bill_date AS date_created,
                 SUM( (ba.bill_amount - COALESCE(pp.total_paid, 0)) ) AS revenue_not_collected_patient_not_yet_paid_other_debtors
             FROM BillAmounts ba
                      JOIN PendingBills pb ON ba.bill_id = pb.bill_id
                      LEFT JOIN PaymentsPerBill pp ON ba.bill_id = pp.bill_id
             GROUP BY ba.bill_date
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
        DATE(cbp.date_created),
        ts.total_sales,
        abi.ipd_cash, abi.maternity, abi.xray, abi.lab, abi.theatre, abi.mortuary,
        abi.op_treatment, abi.pharmacy, abi.medical_exam, abi.dental, abi.physio_therapy,
        abi.occupational_therapy, abi.medical_records_cards_and_files, abi.ambulance,
        abi.ent_and_other_clinics, abi.other,
        cr.cash_receipts_cash_from_daily_services, cr.cash_receipt_nhif_receipt,
        cr.cash_receipt_other_debtors_receipt,
        pr.revenue_not_collected_patient_not_yet_paid_other_debtors,
        cr.revenue_not_collected_patient_not_yet_paid_waivers
    ORDER BY
        DATE(cbp.date_created);

    SELECT "Completed processing Daily Revenue Summary";
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

UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= populate_script_id;

SELECT "Completed refreshing facility-wide tables", CONCAT("Time: ", NOW());
END $$



