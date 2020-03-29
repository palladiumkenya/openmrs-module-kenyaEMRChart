SET @OLD_SQL_MODE=@@SQL_MODE$$
SET SQL_MODE=''$$
DROP PROCEDURE IF EXISTS sp_update_etl_patient_demographics$$
CREATE PROCEDURE sp_update_etl_patient_demographics(IN last_update_time DATETIME)
BEGIN
-- update etl_patient_demographics table
insert into kenyaemr_etl.etl_patient_demographics(
patient_id,
given_name,
middle_name,
family_name,
Gender,
DOB,
dead,
voided,
death_date
)
select 
p.person_id,
p.given_name,
p.middle_name,
p.family_name,
p.gender,
p.birthdate,
p.dead,
p.voided,
p.death_date
FROM (
select 
p.person_id,
pn.given_name,
pn.middle_name,
pn.family_name,
p.gender,
p.birthdate,
p.dead,
p.voided,
p.death_date
from person p 
left join patient pa on pa.patient_id=p.person_id and pa.voided=0
inner join person_name pn on pn.person_id = p.person_id and pn.voided=0
where pn.date_created >= last_update_time
or pn.date_changed >= last_update_time
or pn.date_voided >= last_update_time
or p.date_created >= last_update_time
or p.date_changed >= last_update_time
or p.date_voided >= last_update_time
GROUP BY p.person_id
) p
ON DUPLICATE KEY UPDATE 
given_name = p.given_name, 
middle_name=p.middle_name, 
family_name=p.family_name, 
DOB=p.birthdate, 
dead=p.dead, voided=p.voided, death_date=p.death_date;


-- update etl_patient_demographics with patient attributes: birthplace, citizenship, mother_name, phone number and kin's details
update kenyaemr_etl.etl_patient_demographics d 
inner join 
(
select 
pa.person_id,  
max(if(pat.uuid='8d8718c2-c2cc-11de-8d13-0010c6dffd0f', pa.value, null)) as birthplace,
max(if(pat.uuid='8d871afc-c2cc-11de-8d13-0010c6dffd0f', pa.value, null)) as citizenship,
max(if(pat.uuid='8d871d18-c2cc-11de-8d13-0010c6dffd0f', pa.value, null)) as Mother_name,
max(if(pat.uuid='b2c38640-2603-4629-aebd-3b54f33f1e3a', pa.value, null)) as phone_number,
max(if(pat.uuid='342a1d39-c541-4b29-8818-930916f4c2dc', pa.value, null)) as next_of_kin_contact,
max(if(pat.uuid='d0aa9fd1-2ac5-45d8-9c5e-4317c622c8f5', pa.value, null)) as next_of_kin_relationship,
max(if(pat.uuid='7cf22bec-d90a-46ad-9f48-035952261294', pa.value, null)) as next_of_kin_address,
max(if(pat.uuid='830bef6d-b01f-449d-9f8d-ac0fede8dbd3', pa.value, null)) as next_of_kin_name,
max(if(pat.uuid='b8d0b331-1d2d-4a9a-b741-1816f498bdb6', pa.value, null)) as email_address
from person_attribute pa
inner join
(
select 
pat.person_attribute_type_id,
pat.name,
pat.uuid
from person_attribute_type pat
where pat.retired=0
) pat on pat.person_attribute_type_id = pa.person_attribute_type_id 
and pat.uuid in (
	'8d8718c2-c2cc-11de-8d13-0010c6dffd0f', -- birthplace
	'8d871afc-c2cc-11de-8d13-0010c6dffd0f', -- citizenship
	'8d871d18-c2cc-11de-8d13-0010c6dffd0f', -- mother's name
	'b2c38640-2603-4629-aebd-3b54f33f1e3a', -- telephone contact
	'342a1d39-c541-4b29-8818-930916f4c2dc', -- next of kin's contact
	'd0aa9fd1-2ac5-45d8-9c5e-4317c622c8f5', -- next of kin's relationship
	'7cf22bec-d90a-46ad-9f48-035952261294', -- next of kin's address
	'830bef6d-b01f-449d-9f8d-ac0fede8dbd3', -- next of kin's name
	'b8d0b331-1d2d-4a9a-b741-1816f498bdb6' -- email address

	)
where pa.date_created >= last_update_time
or pa.date_changed >= last_update_time
or pa.date_voided >= last_update_time
group by pa.person_id
) att on att.person_id = d.patient_id
set d.phone_number=att.phone_number, 
	d.next_of_kin=att.next_of_kin_name,
	d.next_of_kin_relationship=att.next_of_kin_relationship,
	d.next_of_kin_phone=att.next_of_kin_contact,
	d.phone_number=att.phone_number,
	d.birth_place = att.birthplace,
	d.citizenship = att.citizenship,
	d.email_address=att.email_address;


update kenyaemr_etl.etl_patient_demographics d
inner join (select pi.patient_id,
max(if(pit.uuid='05ee9cf4-7242-4a17-b4d4-00f707265c8a',pi.identifier,null)) as upn,
max(if(pit.uuid='d8ee3b8c-a8fc-4d6b-af6a-9423be5f8906',pi.identifier,null)) district_reg_number,
max(if(pit.uuid='c4e3caca-2dcc-4dc4-a8d9-513b6e63af91',pi.identifier,null)) Tb_treatment_number,
max(if(pit.uuid='b4d66522-11fc-45c7-83e3-39a1af21ae0d',pi.identifier,null)) Patient_clinic_number,
max(if(pit.uuid='49af6cdc-7968-4abb-bf46-de10d7f4859f',pi.identifier,null)) National_id,
max(if(pit.uuid='0691f522-dd67-4eeb-92c8-af5083baf338',pi.identifier,null)) Hei_id
from patient_identifier pi
join patient_identifier_type pit on pi.identifier_type=pit.patient_identifier_type_id
where voided=0 and
pi.date_created >= last_update_time
or pi.date_changed >= last_update_time
or pi.date_voided >= last_update_time
group by pi.patient_id) pid on pid.patient_id=d.patient_id
set d.unique_patient_no=pid.UPN, 
	d.national_id_no=pid.National_id,
	d.patient_clinic_number=pid.Patient_clinic_number,
    d.hei_no=pid.Hei_id,
    d.Tb_no=pid.Tb_treatment_number,
    d.district_reg_no=pid.district_reg_number
;

update kenyaemr_etl.etl_patient_demographics d
inner join (select o.person_id as patient_id,
max(if(o.concept_id in(1054),cn.name,null))  as marital_status,
max(if(o.concept_id in(1712),cn.name,null))  as education_level
from obs o
join concept_name cn on cn.concept_id=o.value_coded and cn.concept_name_type='FULLY_SPECIFIED'
and cn.locale='en'
where o.concept_id in (1054,1712) and o.voided=0 and 
o.date_created >= last_update_time
or o.date_voided >= last_update_time
group by person_id) pstatus on pstatus.patient_id=d.patient_id
set d.marital_status=pstatus.marital_status,
d.education_level=pstatus.education_level;

END$$


-- ------------ create table etl_patient_treatment_event----------------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_program_discontinuation$$
CREATE PROCEDURE sp_update_etl_program_discontinuation(IN last_update_time DATETIME)
BEGIN
insert into kenyaemr_etl.etl_patient_program_discontinuation(
patient_id,
uuid,
visit_id,
visit_date,
program_uuid,
program_name,
encounter_id,
discontinuation_reason,
date_died,
transfer_facility,
transfer_date
)
select 
e.patient_id,
e.uuid,
e.visit_id,
e.encounter_datetime,
e.uuid,
"COVID-19" as program_name,
e.encounter_id,
max(if(o.concept_id=161555, o.value_coded, null)) as reason_discontinued,
max(if(o.concept_id=1543, o.value_datetime, null)) as date_died,
max(if(o.concept_id=159495, left(trim(o.value_text),100), null)) as to_facility,
max(if(o.concept_id=160649, o.value_datetime, null)) as to_date
from encounter e
inner join person p on p.person_id=e.patient_id and p.voided=0
inner join obs o on o.encounter_id=e.encounter_id and o.voided=0 and o.concept_id in (161555,1543,159495,160649)
where e.date_created >= last_update_time
or e.date_changed >= last_update_time
or e.date_voided >= last_update_time
or o.date_created >= last_update_time
or o.date_voided >= last_update_time
group by e.encounter_id
ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date),discontinuation_reason=VALUES(discontinuation_reason),
date_died=VALUES(date_died),transfer_facility=VALUES(transfer_facility),transfer_date=VALUES(transfer_date)
;

END$$
-- DELIMITER ;

-- ------------------------------------- laboratory updates ---------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_laboratory_extract$$
CREATE PROCEDURE sp_update_etl_laboratory_extract(IN last_update_time DATETIME)
BEGIN

insert into kenyaemr_etl.etl_laboratory_extract(
uuid,
encounter_id,
patient_id,
location_id,
visit_date,
visit_id,
lab_test,
urgency,
test_result,
date_created,
created_by 
)
select 
o.uuid,
e.encounter_id,
e.patient_id,
e.location_id,
e.encounter_datetime as visit_date,
e.visit_id,
o.concept_id,
od.urgency,
(CASE when o.concept_id in(5497,730,654,790,856) then o.value_numeric
	when o.concept_id in(1030,1305) then o.value_coded
	END) AS test_result,
e.date_created,
e.creator
from encounter e
	inner join person p on p.person_id=e.patient_id and p.voided=0
	inner join
(
	select encounter_type_id, uuid, name from encounter_type where uuid in('17a381d1-7e29-406a-b782-aa903b963c28', 'a0034eee-1940-4e35-847f-97537a35d05e','e1406e88-e9a9-11e8-9f32-f2801f1b9fd1','de78a6be-bfc5-4634-adc3-5f1a280455cc')
) et on et.encounter_type_id=e.encounter_type
inner join obs o on e.encounter_id=o.encounter_id and o.voided=0 and o.concept_id in (5497,730,654,790,856,1030,1305)
left join orders od on od.order_id = o.order_id and od.voided=0
where e.date_created >= last_update_time
or e.date_changed >= last_update_time
or e.date_voided >= last_update_time
or o.date_created >= last_update_time
or o.date_voided >= last_update_time
ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date), lab_test=VALUES(lab_test), test_result=VALUES(test_result)
; 

END$$
-- DELIMITER ;

-- ------------- update etl_patient_triage-------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_patient_triage$$
CREATE PROCEDURE sp_update_etl_patient_triage(IN last_update_time DATETIME)
	BEGIN
		SELECT "Processing Patient Triage ", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_patient_triage(
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
			nutritional_status,
			last_menstrual_period,
			voided
		)
			select
				e.uuid,
				e.patient_id,
				e.visit_id,
				date(e.encounter_datetime) as visit_date,
				e.location_id,
				e.encounter_id as encounter_id,
				e.creator,
				e.date_created as date_created,
				max(if(o.concept_id=160430,trim(o.value_text),null)) as visit_reason,
				max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
				max(if(o.concept_id=5090,o.value_numeric,null)) as height,
				max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_pressure,
				max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_pressure,
				max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
				max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
				max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
				max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
				max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
				max(if(o.concept_id=163300,o.value_coded,null)) as nutritional_status,
				max(if(o.concept_id=1427,date(o.value_datetime),null)) as last_menstrual_period,
				e.voided as voided
			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join
				(
					select encounter_type_id, uuid, name from encounter_type where uuid in('d1059fb9-a079-4feb-a749-eedd709ae542')
				) et on et.encounter_type_id=e.encounter_type
				left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
																 and o.concept_id in (160430,5089,5090,5085,5086,5088,5087,5242,5092,1343,163300,1427)
			where e.voided=0 and e.date_created >= last_update_time
						or e.date_changed >= last_update_time
						or e.date_voided >= last_update_time
						or o.date_created >= last_update_time
						or o.date_voided >= last_update_time
			group by e.patient_id, visit_date
		ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date),encounter_provider=VALUES(encounter_provider),weight=VALUES(weight),height=VALUES(height),systolic_pressure=VALUES(systolic_pressure),diastolic_pressure=VALUES(diastolic_pressure),
			temperature=VALUES(temperature),pulse_rate=VALUES(pulse_rate),respiratory_rate=VALUES(respiratory_rate),
			oxygen_saturation=VALUES(oxygen_saturation),muac=VALUES(muac),nutritional_status=VALUES(nutritional_status),last_menstrual_period=VALUES(last_menstrual_period),voided=VALUES(voided);

		END$$


-- ------------- populate etl_progress_note-------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_progress_note$$
CREATE PROCEDURE sp_update_etl_progress_note(IN last_update_time DATETIME)
  BEGIN
    SELECT "Processing progress", CONCAT("Time: ", NOW());
    insert into kenyaemr_etl.etl_progress_note(
        uuid,
        provider ,
        patient_id,
        visit_id,
        visit_date,
        location_id,
        encounter_id,
        date_created,
        notes,
        voided
        )
    select
           e.uuid, e.creator as provider,e.patient_id, e.visit_id, e.encounter_datetime as visit_date, e.location_id, e.encounter_id,e.date_created,
           max(if(o.concept_id = 159395, o.value_text, null )) as notes,
           e.voided
    from encounter e
			inner join person p on p.person_id=e.patient_id and p.voided=0
			inner join form f on f.form_id=e.form_id and f.uuid in ("c48ed2a2-0a0f-4f4e-9fed-a79ca3e1a9b9")
           inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (159395) and o.voided=0
    where e.voided=0 and e.date_created >= last_update_time
						or e.date_changed >= last_update_time
						or e.date_voided >= last_update_time
						or o.date_created >= last_update_time
						or o.date_voided >= last_update_time
    group by e.encounter_id
    ON DUPLICATE KEY UPDATE visit_date=values(visit_date),
                        provider=values(provider),
                        notes=values(notes),
                        voided=values(voided);
    END$$




-- ------------------------- process patient program ------------------------

DROP PROCEDURE IF EXISTS sp_update_etl_patient_program$$
CREATE PROCEDURE sp_update_etl_patient_program(IN last_update_time DATETIME)
	BEGIN
		SELECT "Updating patient program ", CONCAT("Time: ", NOW());
		INSERT INTO kenyaemr_etl.etl_patient_program(
			uuid,
			patient_id,
			location_id,
			program,
			date_enrolled,
			date_completed,
			outcome,
			date_created,
			voided
		)
			select
				pp.uuid uuid,
				pp.patient_id patient_id,
				pp.location_id location_id,
				"COVID-19" as program,
				pp.date_enrolled date_enrolled,
				pp.date_completed date_completed,
				pp.outcome_concept_id outcome,
				pp.date_created,
				pp.voided voided
			from patient_program pp
				inner join program p on p.program_id=pp.program_id and p.retired=0
				inner join patient pt on pt.patient_id=pp.patient_id and pt.voided=0
			where pp.date_created >= last_update_time
						or pp.date_changed >= last_update_time
						or pp.date_voided >= last_update_time
		GROUP BY pp.uuid
		ON DUPLICATE KEY UPDATE date_enrolled=VALUES(date_enrolled),date_completed=VALUES(date_completed),
			program=VALUES(program),outcome=VALUES(outcome),voided=VALUES(outcome),voided=VALUES(voided)
		;
		SELECT "Completed updating patient program data ", CONCAT("Time: ", NOW());
		END$$

-- ------------------- update person address table -------------

DROP PROCEDURE IF EXISTS sp_update_etl_person_address$$
CREATE PROCEDURE sp_update_etl_person_address(IN last_update_time DATETIME)
	BEGIN
		SELECT "Processing person addresses ", CONCAT("Time: ", NOW());
		INSERT INTO kenyaemr_etl.etl_person_address(
			uuid,
			patient_id,
			county,
			sub_county,
			location,
			ward,
			sub_location,
			village,
			postal_address,
			land_mark,
			voided
		)
			select
				pa.uuid,
				pa.person_id,
				coalesce(pa.country,pa.county_district) county,
				pa.state_province sub_county,
				pa.address6 location,
				pa.address4 ward,
				pa.address5 sub_location,
				pa.city_village village,
				pa.address1 postal_address,
				pa.address2 land_mark,
				pa.voided voided
			from person_address pa
				inner join patient pt on pt.patient_id=pa.person_id and pt.voided=0
			where pa.date_created >= last_update_time
						or pa.date_changed >= last_update_time
						or pa.date_voided >= last_update_time
		ON DUPLICATE KEY UPDATE county=VALUES(county),sub_county=values(sub_county),location=values(location),
			ward=values(ward),sub_location=values(sub_location),village=VALUES(village),postal_address=values(postal_address),
			land_mark=values(land_mark),voided=values(voided)
		;
		SELECT "Completed processing person_address data ", CONCAT("Time: ", NOW());
		END$$



		-- end of scheduled updates procedures

		SET sql_mode=@OLD_SQL_MODE$$
-- ----------------------------  scheduled updates ---------------------


DROP PROCEDURE IF EXISTS sp_scheduled_updates$$
CREATE PROCEDURE sp_scheduled_updates()
BEGIN
DECLARE update_script_id INT(11);
DECLARE last_update_time DATETIME;
SELECT max(start_time) into last_update_time from kenyaemr_etl.etl_script_status where stop_time is not null or stop_time !="";

INSERT INTO kenyaemr_etl.etl_script_status(script_name, start_time) VALUES('scheduled_updates', NOW());
SET update_script_id = LAST_INSERT_ID();
CALL sp_update_etl_patient_demographics(last_update_time);
CALL sp_update_etl_program_discontinuation(last_update_time);
CALL sp_update_etl_patient_triage(last_update_time);
CALL sp_update_etl_progress_note(last_update_time);
CALL sp_update_etl_patient_program(last_update_time);
CALL sp_update_etl_person_address(last_update_time);

CALL sp_update_dashboard_table();

UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where  id= update_script_id;
DELETE FROM kenyaemr_etl.etl_script_status where script_name in ("KenyaEMR_Data_Tool", "scheduled_updates") and start_time < DATE_SUB(NOW(), INTERVAL 12 HOUR);
SELECT update_script_id;

END$$
-- DELIMITER ;










