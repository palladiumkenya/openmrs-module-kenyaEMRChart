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
    DROP TABLE if exists kenyaemr_etl.etl_patient_demographics;
    DROP TABLE IF EXISTS kenyaemr_etl.etl_patient_program_discontinuation;
    DROP TABLE if exists kenyaemr_etl.etl_covid19_contact_tracing;
    DROP TABLE IF EXISTS kenyaemr_etl.etl_covid19_clinical;
    DROP TABLE IF EXISTS kenyaemr_etl.etl_covid19_Travell_History;

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


-- ------------ create table etl_patient_treatment_discontinuation-----------------------

    CREATE TABLE kenyaemr_etl.etl_patient_program_discontinuation(
                                                                     uuid char(38),
                                                                     patient_id INT(11) NOT NULL ,
                                                                     visit_id INT(11),
                                                                     visit_date DATETIME,
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
                                                                     INDEX(visit_date),
                                                                     INDEX(visit_date, program_name, patient_id),
                                                                     INDEX(visit_date, patient_id),
                                                                     INDEX(encounter_id),
                                                                     INDEX(patient_id),
                                                                     INDEX(discontinuation_reason),
                                                                     INDEX(date_died),
                                                                     INDEX(transfer_date)
    );
    SELECT "Successfully created etl_patient_program_discontinuation table";



-- --------------------- creating Covid19 contact tracing and followup table-------------------------------
    CREATE TABLE kenyaemr_etl.etl_covid19_contact_tracing (
                                                              uuid CHAR(38),
                                                              encounter_id INT(11) NOT NULL PRIMARY KEY,
                                                              patient_id INT(11) NOT NULL ,
                                                              location_id INT(11) DEFAULT NULL,
                                                              visit_date DATE,
                                                              Fever INT(11),
                                                              Cough INT(11),
                                                              Difficulty_breathing INT(11),
                                                              Sore_throat INT(11),
                                                              Client_Referred_to_hospital INT(11),
                                                              date_created DATE,
                                                              voided INT(11),
                                                              CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
                                                              CONSTRAINT unique_uuid UNIQUE(uuid),
                                                              INDEX(visit_date),
                                                              INDEX(encounter_id),
                                                              INDEX(patient_id),
                                                              INDEX(patient_id, visit_date)
    );
    SELECT "Successfully created etl_covid19_contact_tracing table";
-- --------------------- creating Covid19 Clinical Table -------------------------------
    CREATE TABLE kenyaemr_etl.etl_covid19_clinical (
                                                       uuid CHAR(38),
                                                       encounter_id INT(11) NOT NULL PRIMARY KEY,
                                                       patient_id INT(11) NOT NULL ,
                                                       location_id INT(11) DEFAULT NULL,
                                                       visit_date DATE,

                                                       Detection_point INT(11),
                                                       Date_detected DATE,
                                                       Asymptomatic INT(11),
                                                       Date_of_onset_of_symptoms DATE,
                                                       Health_status_at_the_time_of_reporting INT(11),
                                                       Date_of_death DATE,
                                                       Admission_to_hospital INT(11),
                                                       First_date_of_admission_to_hospital DATE,
                                                       Name_of_hospital INT(11),
                                                       Date_of_isolation DATE,
                                                       Was_the_patient_ventilated INT(11),
                                                       History_of_fever INT(11),
                                                       General_Weakness INT(11),
                                                       Sore_throat INT(11),
                                                       Runny_nose INT(11),
                                                       Shortness_of_breath INT(11),
                                                       Diarrhoea INT(11),
                                                       Nausea INT(11),
                                                       Headache INT(11),
                                                       Irritability_or_confusion INT(11),
                                                       Mascular_pain INT(11),
                                                       Chest_pain INT(11),
                                                       Abdominal_pain INT(11),
                                                       Joint_pain INT(11),
                                                       Other INT(11),
                                                       Temperature INT(11),
                                                       Pharyngeal_exudate INT(11),
                                                       Conjunctival_injection INT(11),
                                                       Dyspnea_tachypnea INT(11),
                                                       Abnormal_lung_auscultation INT(11),
                                                       Abnormal_lung_x_ray_findings INT(11),
                                                       Seizures INT(11),
                                                       Other_signs VARCHAR(255),
                                                       Pregnant INT(11),
                                                       Trimester INT(11),
                                                       Anderlying_conditions INT(11),
                                                       Cardiovascular_disease_including_hypertension INT(11),
                                                       Diabetes INT(11),
                                                       Liver_disease INT(11),
                                                       Chronic_neurological_or_neuromascular_disease INT(11),
                                                       Post_partum_less_than_6_weeks INT(11),
                                                       Immunodeficiency INT(11),
                                                       Renal_disease INT(11),
                                                       Chronic_lung_disease INT(11),
                                                       Malignancy INT(11),
                                                       Occupation INT(11),
                                                       Travell_history INT(11),
                                                       Country VARCHAR(255),
                                                       City VARCHAR(255),
                                                       Pateient_visit_health_care_in_14_days INT(11),
                                                       Pateient_had_close_contact_in_14_days INT(11),
                                                       Contact_setting INT(11),
                                                       Pateient_had_contact_with_probable_case_in_14_days INT(11), Pateient_visited_animal_market_in_14_days INT(11),
                                                       Location_of_exposure INT(11),
                                                       date_created DATE,
                                                       voided INT(11),
                                                       CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
                                                       CONSTRAINT unique_uuid UNIQUE(uuid),
                                                       INDEX(visit_date),
                                                       INDEX(encounter_id),
                                                       INDEX(patient_id),
                                                       INDEX(patient_id, visit_date)
    );
    SELECT "Successfully created etl_covid19_clinical table";


-- --------------------- creating Covid19 Travel History-------------------------------
    CREATE TABLE kenyaemr_etl.etl_covid19_Travell_History (
                                                              uuid CHAR(38),
                                                              encounter_id INT(11) NOT NULL PRIMARY KEY,
                                                              patient_id INT(11) NOT NULL ,
                                                              location_id INT(11) DEFAULT NULL,
                                                              visit_date DATE,
                                                              Date_of_arrival_in_Kenya DATE,
                                                              Airline_or_Bus INT(11),
                                                              Flight_or_Bus_Number VARCHAR(255),
                                                              Seat_Number VARCHAR(255),
                                                              Destination_in_Kenya VARCHAR(255),
                                                              Name_of_your_contact_person  VARCHAR(255),
                                                              Telephone_No_of_your_contact_person VARCHAR(255),
                                                              Village_House_number_Hotel VARCHAR(255),
                                                              Sublocation_Estate VARCHAR(255),
                                                              County VARCHAR(255),
                                                              Address VARCHAR(255),
                                                              Local_Telephone_Number INT(11),
                                                              Email VARCHAR(255),
                                                              Cough INT(11),
                                                              Difficulty_breathing INT(11),
                                                              Fever INT(11),
                                                              date_created DATE,
                                                              voided INT(11),
                                                              CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kenyaemr_etl.etl_patient_demographics(patient_id),
                                                              CONSTRAINT unique_uuid UNIQUE(uuid),
                                                              INDEX(visit_date),
                                                              INDEX(encounter_id),
                                                              INDEX(patient_id),
                                                              INDEX(patient_id, visit_date)
    );
    SELECT "Successfully created etl_covid19_Travell_History table";



    UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= script_id;

    END$$






