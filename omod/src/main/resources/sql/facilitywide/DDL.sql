DROP PROCEDURE IF EXISTS create_facility_wide_etl_tables $$
CREATE PROCEDURE create_facility_wide_etl_tables()
BEGIN
DECLARE script_id INT(11);

-- create/recreate database kenyaemr_etl

CREATE DATABASE IF NOT EXISTS kenyaemr_etl DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;
-- Log start time
INSERT INTO kenyaemr_etl.etl_script_status (script_name, start_time) VALUES('initial_creation_of_facility_wide_tables', NOW());
SET script_id = LAST_INSERT_ID();
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS kenyaemr_etl.etl_daily_revenue_summary;
DROP TABLE IF EXISTS kenyaemr_etl.etl_special_clinics;
SET FOREIGN_KEY_CHECKS = 1;

-- Create etl_daily_revenue_summary table
CREATE TABLE kenyaemr_etl.etl_daily_revenue_summary(
	daily_summary_id                                         INT AUTO_INCREMENT    	PRIMARY KEY,
	transaction_date                                         DATE NULL,
	total_sales                                              INT      NULL,
	ipd_cash                                                 INT      NULL,
	maternity                                                INT      NULL,
	xray                                                     INT      NULL,
	lab                                                      INT      NULL,
	theatre                                                  INT      NULL,
	mortuary                                                 INT      NULL,
	op_treatment                                             INT      NULL,
	pharmacy                                                 INT      NULL,
	medical_exam                                             INT      NULL,
	medical_reports_including_P3                             INT      NULL,
	dental                                                   INT      NULL,
	physio_therapy                                           INT      NULL,
	occupational_therapy                                     INT      NULL,
	medical_records_cards_and_files                          INT      NULL,
	booking_fees                                             INT      NULL,
	rental_services                                          INT      NULL,
	ambulance                                                INT      NULL,
	public_health_services                                   INT      NULL,
	ent_and_other_clinics                                    INT      NULL,
	other                                                    INT      NULL,
	cash_receipts_cash_from_daily_services                   INT      NULL,
	cash_receipt_nhif_receipt                                INT      NULL,
	cash_receipt_other_debtors_receipt                       INT      NULL,
	revenue_not_collected_patient_not_yet_paid_nhif_patients INT      NULL,
	revenue_not_collected_patient_not_yet_paid_other_debtors INT      NULL,
	revenue_not_collected_patient_not_yet_paid_waivers       INT      NULL,
	revenue_not_collected_write_offs_exemptions              INT      NULL,
	revenue_not_collected_write_offs_absconders              INT      NULL,
	INDEX (daily_summary_id),
	INDEX (transaction_date)
);

SELECT "Successfully created etl_daily_revenue_summary table";

-- Create etl_special_clinics table
CREATE TABLE kenyaemr_etl.etl_special_clinics
(
    patient_id                    INT(11)  NOT NULL,
    visit_id                      INT(11) DEFAULT NULL,
    encounter_id                  INT(11)  NOT NULL PRIMARY KEY,
    uuid                          CHAR(38) NOT NULL,
    location_id                   INT(11)  NOT NULL,
    provider                      INT(11)  NOT NULL,
    visit_date                    DATE,
    visit_type                    INT(11),
    referred_from                 INT(11),
    acuity_finding                INT(11),
    referred_to                   INT(11),
    ot_intervention               VARCHAR(255),
    assistive_technology          VARCHAR(255),
    enrolled_in_school            INT(11),
    patient_with_disability       INT(11),
    patient_has_edema             INT(11),
    nutritional_status            INT(11),
    patient_pregnant              INT(11),
    sero_status                   INT(11),
    medication_condition          INT(11),
    nutritional_intervention      INT(11),
    postnatal                     INT(11),
    patient_on_arv                INT(11),
    anaemia_level                 INT(11),
    metabolic_disorders           VARCHAR(255),
    critical_nutrition_practices  VARCHAR(255),
    therapeutic_food              VARCHAR(255),
    supplemental_food             VARCHAR(255),
    micronutrients                VARCHAR(255),
    referral_status               INT(11),
    criteria_for_admission        INT(11),
    type_of_admission             INT(11),
    cadre                         INT(11),
    neuron_developmental_findings VARCHAR(255),
    neurodiversity_conditions     VARCHAR(255),
    learning_findings             VARCHAR(255),
    disability_classification     VARCHAR(255),
    special_clinic                VARCHAR(255),
    special_clinic_form_uuid      CHAR(38),
    CONSTRAINT FOREIGN KEY (patient_id)
        REFERENCES kenyaemr_etl.etl_patient_demographics (patient_id),
    CONSTRAINT unique_uuid UNIQUE (uuid),
    INDEX (patient_id),
    INDEX (visit_type),
    INDEX (visit_date)
);
SELECT "Successfully created etl_special_clinics table";

UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= script_id;

END $$
