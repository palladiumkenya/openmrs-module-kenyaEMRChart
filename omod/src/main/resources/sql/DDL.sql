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


DROP TABLE IF EXISTS kenyaemr_etl.etl_laboratory_extract;
DROP TABLE IF EXISTS kenyaemr_etl.etl_patient_treatment_event;
DROP TABLE IF EXISTS kenyaemr_etl.etl_patient_program_discontinuation;


DROP TABLE if exists kenyaemr_etl.etl_patient_demographics;
DROP TABLE IF EXISTS kenyaemr_etl.etl_person_address;


DROP TABLE IF EXISTS kenyaemr_etl.etl_patient_triage;

DROP TABLE IF EXISTS kenyaemr_etl.etl_patient_program;
DROP TABLE IF EXISTS kenyaemr_etl.etl_default_facility_info;

DROP TABLE IF EXISTS kenyaemr_etl.etl_covid_19_enrolment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_contact_tracing_followup;
DROP TABLE IF EXISTS kenyaemr_etl.etl_covid_quarantine_enrolment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_covid_quarantine_followup;
DROP TABLE IF EXISTS kenyaemr_etl.etl_covid_quarantine_outcome;
DROP TABLE IF EXISTS kenyaemr_etl.etl_covid_travel_history;


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


-- ------------ create table etl_patient_treatment_discontinuation-----------------------

CREATE TABLE kenyaemr_etl.etl_patient_program_discontinuation(
uuid char(38),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
encounter_date DATETIME,
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
INDEX(encounter_date),
INDEX(encounter_date, program_name, patient_id),
INDEX(encounter_date, patient_id),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(discontinuation_reason),
INDEX(date_died),
INDEX(transfer_date)
);
SELECT "Successfully created etl_patient_program_discontinuation table";


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



    -------------- create table etl_covid_19_enrolment-----------------------
    CREATE TABLE kenyaemr_etl.etl_covid_19_enrolment (
      uuid CHAR(38),
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      visit_id INT(11) DEFAULT NULL,
      patient_id INT(11) NOT NULL ,
      location_id INT(11) DEFAULT NULL,
      visit_date DATE,
      encounter_provider INT(11),
      date_created DATE,
      sub_county VARCHAR(255),
      county VARCHAR(255),
      detection_point VARCHAR(255),
      date_detected DATE,
      onset_symptoms_date DATE,
      symptomatic VARCHAR(10),
      fever VARCHAR(50),
      cough VARCHAR(10),
      runny_nose VARCHAR(10),
      diarrhoea VARCHAR(10),
      headache VARCHAR(10),
      muscular_pain VARCHAR(10),
      abdominal_pain VARCHAR(10),
      general_weakness VARCHAR(10),
      sore_throat VARCHAR(10),
      shortness_breath VARCHAR(10),
      vomiting VARCHAR(10),
      confusion VARCHAR(10),
      chest_pain VARCHAR(10),
      joint_pain VARCHAR(10),
      other_symptom VARCHAR(10),
      specify_symptoms VARCHAR(255),
      temperature VARCHAR(10),
      pharyngeal_exudate VARCHAR(10),
      tachypnea VARCHAR(10),
      abnormal_xray VARCHAR(10),
      coma VARCHAR(10),
      conjuctival_injection VARCHAR(10),
      abnormal_lung_auscultation VARCHAR(10),
      seizures VARCHAR(10),
      pregnancy_status VARCHAR(10),
      trimester VARCHAR(10),
      underlying_condition VARCHAR(10),
      cardiovascular_dse_hypertension VARCHAR(10),
      diabetes VARCHAR(10),
      liver_disease VARCHAR(10),
      chronic_neurological_neuromascular_dse VARCHAR(10),
      post_partum VARCHAR(10),
      immunodeficiency VARCHAR(10),
      renal_disease VARCHAR(10),
      chronic_lung_disease VARCHAR(10),
      malignancy VARCHAR(10),
      occupation VARCHAR(10),
      other_signs VARCHAR(10),
      specify_signs VARCHAR(255),
      admitted_to_hospital VARCHAR(10),
      date_of_first_admission DATE,
      hospital_name VARCHAR(255),
      date_of_isolation DATE,
      patient_ventilated  VARCHAR(10),
      health_status_at_reporting VARCHAR(255),
      date_of_death DATE,
      recently_travelled VARCHAR(10),
      country_recently_travelled VARCHAR(100),
      city_recently_travelled VARCHAR(100),
      recently_visited_health_facility VARCHAR(10),
      recent_contact_with_infected_person VARCHAR(10),
      recent_contact_with_confirmed_person VARCHAR(10),
      recent_contact_setting VARCHAR(200),
      recent_visit_to_animal_market VARCHAR(10),
      animal_market_name varchar(200),
      voided INT(11),
      CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      INDEX(visit_date),
      INDEX(visit_id),
      INDEX(encounter_id),
      INDEX(patient_id),
      INDEX(patient_id, visit_date)
    );

  SELECT "Successfully created etl_covid_19_enrolment table";

   -------------- create table etl_contact_tracing_followup-----------------------
    CREATE TABLE kenyaemr_etl.etl_contact_tracing_followup (
      uuid CHAR(38),
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      visit_id INT(11) DEFAULT NULL,
      patient_id INT(11) NOT NULL ,
      location_id INT(11) DEFAULT NULL,
      visit_date DATE,
      encounter_provider INT(11),
      date_created DATE,
	    fever VARCHAR(10),
	    cough VARCHAR(10),
	    difficulty_breathing VARCHAR(10),
	    sore_throat VARCHAR(10),
	    referred_to_hosp VARCHAR(10),
	    voided INT(11),
      CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      INDEX(visit_date),
      INDEX(visit_id),
      INDEX(encounter_id),
      INDEX(patient_id),
      INDEX(patient_id, visit_date)
    );

SELECT "Successfully created etl_contact_tracing_followup table";

	 -------------- create table etl_covid_quarantine_enrolment-----------------------
    CREATE TABLE kenyaemr_etl.etl_covid_quarantine_enrolment (
      uuid CHAR(38),
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      visit_id INT(11) DEFAULT NULL,
      patient_id INT(11) NOT NULL ,
      location_id INT(11) DEFAULT NULL,
      visit_date DATE,
      encounter_provider INT(11),
      date_created DATE,
      quarantine_center VARCHAR(100),
      type_of_admission VARCHAR(100),
      quarantine_center_trf_from VARCHAR(100),
      voided INT(11),
      CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      INDEX(visit_date),
      INDEX(visit_id),
      INDEX(encounter_id),
      INDEX(patient_id),
      INDEX(patient_id, visit_date)
    );
	SELECT "Successfully created etl_covid_quarantine_enrolment table";

			 -------------- create table etl_covid_quarantine_followup-----------------------
    CREATE TABLE kenyaemr_etl.etl_covid_quarantine_followup (
      uuid CHAR(38),
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      visit_id INT(11) DEFAULT NULL,
      patient_id INT(11) NOT NULL ,
      location_id INT(11) DEFAULT NULL,
      visit_date DATE,
      encounter_provider INT(11),
      date_created DATE,
      sub_county VARCHAR(255),
      county VARCHAR(255),
      fever VARCHAR(10),
      cough VARCHAR(10),
      difficulty_breathing VARCHAR(10),
      sore_throat VARCHAR(10),
      referred_to_hosp VARCHAR(10),
      voided INT(11),
      CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      INDEX(visit_date),
      INDEX(visit_id),
      INDEX(encounter_id),
      INDEX(patient_id),
      INDEX(patient_id, visit_date)
    );
	SELECT "Successfully created etl_covid_quarantine_followup table";

				 -------------- create table etl_covid_quarantine_outcome-----------------------
    CREATE TABLE kenyaemr_etl.etl_covid_quarantine_outcome (
      uuid CHAR(38),
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      visit_id INT(11) DEFAULT NULL,
      patient_id INT(11) NOT NULL ,
      location_id INT(11) DEFAULT NULL,
      visit_date DATE,
      encounter_provider INT(11),
      date_created DATE,
      discontinuation_reason VARCHAR(100),
      transfer_to_facility VARCHAR(100),
      referral_reason VARCHAR(100),
      facility_referred_to VARCHAR(100),
      discharge_reason VARCHAR(100),
      comment VARCHAR(200),
      voided INT(11),
      CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      INDEX(visit_date),
      INDEX(visit_id),
      INDEX(encounter_id),
      INDEX(patient_id),
      INDEX(patient_id, visit_date)
    );
	SELECT "Successfully created etl_covid_quarantine_outcome table";

					 -------------- create table etl_covid_travel_history-----------------------
    CREATE TABLE kenyaemr_etl.etl_covid_travel_history (
      uuid CHAR(38),
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      visit_id INT(11) DEFAULT NULL,
      patient_id INT(11) NOT NULL ,
      location_id INT(11) DEFAULT NULL,
      visit_date DATE,
      encounter_provider INT(11),
      date_created DATE,
      date_arrived_in_kenya DATE,
      mode_of_transport VARCHAR(100),
      flight_bus_number VARCHAR(100),
      seat_number VARCHAR(100),
      country_visited VARCHAR(100),
      destination_in_kenya VARCHAR(100),
      name_of_contact_person VARCHAR(200),
      phone_of_contact_person VARCHAR(200),
      county VARCHAR(200),
      sublocation_estate VARCHAR(200),
      village_house_no_hotel VARCHAR(200),
      address VARCHAR(200),
      local_phone_number VARCHAR(200),
      email VARCHAR(200),
      fever VARCHAR(10),
      cough VARCHAR(10),
      difficulty_breathing VARCHAR(10),
      voided INT(11),
      CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      INDEX(visit_date),
      INDEX(visit_id),
      INDEX(encounter_id),
      INDEX(patient_id),
      INDEX(patient_id, visit_date)
    );
	SELECT "Successfully created etl_covid_travel_history table";

  UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= script_id;

END$$






