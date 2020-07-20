
SET @OLD_SQL_MODE=@@SQL_MODE$$
SET SQL_MODE=''$$
DROP PROCEDURE IF EXISTS sp_populate_etl_patient_demographics$$
CREATE PROCEDURE sp_populate_etl_patient_demographics()
BEGIN
-- initial set up of etl_patient_demographics table
SELECT "Processing patient demographics data ", CONCAT("Time: ", NOW());
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
left join patient pa on pa.patient_id=p.person_id
left join person_name pn on pn.person_id = p.person_id and pn.voided=0
where p.voided=0
GROUP BY p.person_id
) p
ON DUPLICATE KEY UPDATE given_name = p.given_name, middle_name=p.middle_name, family_name=p.family_name;


-- update etl_patient_demographics with patient attributes: birthplace, citizenship, mother_name, phone number and kin's details
update kenyaemr_etl.etl_patient_demographics d
left outer join
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
where pa.voided=0
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
join (select pi.patient_id,
max(if(pit.uuid='05ee9cf4-7242-4a17-b4d4-00f707265c8a',pi.identifier,null)) as upn,
max(if(pit.uuid='d8ee3b8c-a8fc-4d6b-af6a-9423be5f8906',pi.identifier,null)) district_reg_number,
max(if(pit.uuid='c4e3caca-2dcc-4dc4-a8d9-513b6e63af91',pi.identifier,null)) Tb_treatment_number,
max(if(pit.uuid='b4d66522-11fc-45c7-83e3-39a1af21ae0d',pi.identifier,null)) Patient_clinic_number,
max(if(pit.uuid='49af6cdc-7968-4abb-bf46-de10d7f4859f',pi.identifier,null)) National_id,
max(if(pit.uuid='0691f522-dd67-4eeb-92c8-af5083baf338',pi.identifier,null)) Hei_id
from patient_identifier pi
join patient_identifier_type pit on pi.identifier_type=pit.patient_identifier_type_id
where voided=0
group by pi.patient_id) pid on pid.patient_id=d.patient_id
set d.unique_patient_no=pid.UPN,
	d.national_id_no=pid.National_id,
	d.patient_clinic_number=pid.Patient_clinic_number,
    d.hei_no=pid.Hei_id,
    d.Tb_no=pid.Tb_treatment_number,
    d.district_reg_no=pid.district_reg_number
;

update kenyaemr_etl.etl_patient_demographics d
join (select o.person_id as patient_id,
max(if(o.concept_id in(1054),cn.name,null))  as marital_status,
max(if(o.concept_id in(1712),cn.name,null))  as education_level
from obs o
join concept_name cn on cn.concept_id=o.value_coded and cn.concept_name_type='FULLY_SPECIFIED'
and cn.locale='en'
where o.concept_id in (1054,1712) and o.voided=0
group by person_id) pstatus on pstatus.patient_id=d.patient_id
set d.marital_status=pstatus.marital_status,
d.education_level=pstatus.education_level;

END$$


DROP PROCEDURE IF EXISTS sp_populate_etl_hiv_enrollment$$
CREATE PROCEDURE sp_populate_etl_hiv_enrollment()
BEGIN
-- populate patient_hiv_enrollment table
-- uuid: de78a6be-bfc5-4634-adc3-5f1a280455cc
SELECT "Processing HIV Enrollment data ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_hiv_enrollment (
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
entry_point,
transfer_in_date,
facility_transferred_from,
district_transferred_from,
date_started_art_at_transferring_facility,
date_confirmed_hiv_positive,
facility_confirmed_hiv_positive,
arv_status,
name_of_treatment_supporter,
relationship_of_treatment_supporter,
treatment_supporter_telephone,
treatment_supporter_address,
voided
)
select
e.patient_id,
e.uuid,
e.visit_id,
e.encounter_datetime as visit_date,
e.location_id,
e.encounter_id,
e.creator,
e.date_created,
max(if(o.concept_id in (164932), o.value_coded, if(o.concept_id=160563 and o.value_coded=1065, 160563, null))) as patient_type ,
max(if(o.concept_id=160555,o.value_datetime,null)) as date_first_enrolled_in_care ,
max(if(o.concept_id=160540,o.value_coded,null)) as entry_point,
max(if(o.concept_id=160534,o.value_datetime,null)) as transfer_in_date,
max(if(o.concept_id=160535,left(trim(o.value_text),100),null)) as facility_transferred_from,
max(if(o.concept_id=161551,left(trim(o.value_text),100),null)) as district_transferred_from,
max(if(o.concept_id=159599,o.value_datetime,null)) as date_started_art_at_transferring_facility,
max(if(o.concept_id=160554,o.value_datetime,null)) as date_confirmed_hiv_positive,
max(if(o.concept_id=160632,left(trim(o.value_text),100),null)) as facility_confirmed_hiv_positive,
max(if(o.concept_id=160533,o.value_boolean,null)) as arv_status,
max(if(o.concept_id=160638,left(trim(o.value_text),100),null)) as name_of_treatment_supporter,
max(if(o.concept_id=160640,o.value_coded,null)) as relationship_of_treatment_supporter,
max(if(o.concept_id=160642,left(trim(o.value_text),100),null)) as treatment_supporter_telephone ,
max(if(o.concept_id=160641,left(trim(o.value_text),100),null)) as treatment_supporter_address,
e.voided
from encounter e
inner join
(
	select encounter_type_id, uuid, name from encounter_type where uuid='de78a6be-bfc5-4634-adc3-5f1a280455cc'
) et on et.encounter_type_id=e.encounter_type
join patient p on p.patient_id=e.patient_id and p.voided=0
left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
	and o.concept_id in (160555,160540,160534,160535,161551,159599,160554,160632,160533,160638,160640,160642,160641,164932,160563)
where e.voided=0
group by e.patient_id, e.encounter_id;
SELECT "Completed processing HIV Enrollment data ", CONCAT("Time: ", NOW());
END$$



-- ------------- populate etl_hiv_followup--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_hiv_followup$$
CREATE PROCEDURE sp_populate_etl_hiv_followup()
BEGIN
SELECT "Processing HIV Followup data ", CONCAT("Time: ", NOW());
INSERT INTO kenyaemr_etl.etl_patient_hiv_followup(
patient_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
visit_scheduled,
person_present,
weight,
systolic_pressure,
diastolic_pressure,
height,
temperature,
pulse_rate,
respiratory_rate,
oxygen_saturation,
muac,
nutritional_status,
population_type,
key_population_type,
who_stage,
presenting_complaints,
clinical_notes,
on_anti_tb_drugs,
on_ipt,
ever_on_ipt,
spatum_smear_ordered,
chest_xray_ordered,
genexpert_ordered,
spatum_smear_result,
chest_xray_result,
genexpert_result,
referral,
clinical_tb_diagnosis,
contact_invitation,
evaluated_for_ipt,
has_known_allergies,
has_chronic_illnesses_cormobidities,
has_adverse_drug_reaction,
pregnancy_status,
wants_pregnancy,
pregnancy_outcome,
anc_number,
expected_delivery_date,
last_menstrual_period,
gravida,
parity,
full_term_pregnancies,
abortion_miscarriages,
family_planning_status,
family_planning_method,
reason_not_using_family_planning,
tb_status,
tb_treatment_no,
ctx_adherence,
ctx_dispensed,
dapsone_adherence,
dapsone_dispensed,
inh_dispensed,
arv_adherence,
poor_arv_adherence_reason,
poor_arv_adherence_reason_other,
pwp_disclosure,
pwp_partner_tested,
condom_provided,
screened_for_sti,
cacx_screening,
sti_partner_notification,
at_risk_population,
system_review_finding,
next_appointment_date,
next_appointment_reason,
stability,
differentiated_care,
voided
)
select
e.patient_id,
e.visit_id,
date(e.encounter_datetime) as visit_date,
e.location_id,
e.encounter_id as encounter_id,
e.creator,
e.date_created as date_created,
max(if(o.concept_id=1246,o.value_coded,null)) as visit_scheduled ,
max(if(o.concept_id=161643,o.value_coded,null)) as person_present,
max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_pressure,
max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_pressure,
max(if(o.concept_id=5090,o.value_numeric,null)) as height,
max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
max(if(o.concept_id=163300,o.value_coded,null)) as nutritional_status,
max(if(o.concept_id=164930,o.value_coded,null)) as population_type,
max(if(o.concept_id=160581,o.value_coded,null)) as key_population_type,
max(if(o.concept_id=5356,o.value_coded,null)) as who_stage ,
max(if(o.concept_id=1154,o.value_coded,null)) as presenting_complaints ,
null as clinical_notes, -- max(if(o.concept_id=160430,left(trim(o.value_text),600),null)) as clinical_notes ,
max(if(o.concept_id=164948,o.value_coded,null)) as on_anti_tb_drugs ,
max(if(o.concept_id=164949,o.value_coded,null)) as on_ipt ,
max(if(o.concept_id=164950,o.value_coded,null)) as ever_on_ipt ,
max(if(o.concept_id=1271 and o.value_coded = 307,1065,1066)) as spatum_smear_ordered ,
max(if(o.concept_id=1271 and o.value_coded = 12 ,1065,1066)) as chest_xray_ordered ,
max(if(o.concept_id=1271 and o.value_coded = 162202,1065,1066)) as genexpert_ordered ,
max(if(o.concept_id=307,o.value_coded,null)) as spatum_smear_result ,
max(if(o.concept_id=12,o.value_coded,null)) as chest_xray_result ,
max(if(o.concept_id=162202,o.value_coded,null)) as genexpert_result ,
max(if(o.concept_id=1272,o.value_coded,null)) as referral ,
max(if(o.concept_id=163752,o.value_coded,null)) as clinical_tb_diagnosis ,
max(if(o.concept_id=163414,o.value_coded,null)) as contact_invitation ,
max(if(o.concept_id=162275,o.value_coded,null)) as evaluated_for_ipt ,
max(if(o.concept_id=160557,o.value_coded,null)) as has_known_allergies ,
max(if(o.concept_id=162747,o.value_coded,null)) as has_chronic_illnesses_cormobidities ,
max(if(o.concept_id=121764,o.value_coded,null)) as has_adverse_drug_reaction ,
max(if(o.concept_id=5272,o.value_coded,null)) as pregnancy_status,
max(if(o.concept_id=164933,o.value_coded,null)) as wants_pregnancy,
max(if(o.concept_id=161033,o.value_coded,null)) as pregnancy_outcome,
max(if(o.concept_id=163530,o.value_text,null)) as anc_number,
max(if(o.concept_id=5596,date(o.value_datetime),null)) as expected_delivery_date,
max(if(o.concept_id=1427,date(o.value_datetime),null)) as last_menstrual_period,
max(if(o.concept_id=5624,o.value_numeric,null)) as gravida,
max(if(o.concept_id=1053,o.value_numeric,null)) as parity ,
max(if(o.concept_id=160080,o.value_numeric,null)) as full_term_pregnancies,
max(if(o.concept_id=1823,o.value_numeric,null)) as abortion_miscarriages ,
max(if(o.concept_id=160653,o.value_coded,null)) as family_planning_status,
max(if(o.concept_id=374,o.value_coded,null)) as family_planning_method,
max(if(o.concept_id=160575,o.value_coded,null)) as reason_not_using_family_planning ,
max(if(o.concept_id=1659,o.value_coded,null)) as tb_status,
max(if(o.concept_id=161654,trim(o.value_text),null)) as tb_treatment_no,
max(if(o.concept_id=161652,o.value_coded,null)) as ctx_adherence,
max(if(o.concept_id=162229 or (o.concept_id=1282 and o.value_coded = 105281),o.value_coded,null)) as ctx_dispensed,
max(if(o.concept_id=164941,o.value_coded,null)) as dapsone_adherence,
max(if(o.concept_id=164940 or (o.concept_id=1282 and o.value_coded = 74250),o.value_coded,null)) as dapsone_dispensed,
max(if(o.concept_id=162230,o.value_coded,null)) as inh_dispensed,
max(if(o.concept_id=1658,o.value_coded,null)) as arv_adherence,
max(if(o.concept_id=160582,o.value_coded,null)) as poor_arv_adherence_reason,
null as poor_arv_adherence_reason_other, -- max(if(o.concept_id=160632,trim(o.value_text),null)) as poor_arv_adherence_reason_other,
max(if(o.concept_id=159423,o.value_coded,null)) as pwp_disclosure,
max(if(o.concept_id=161557,o.value_coded,null)) as pwp_partner_tested,
max(if(o.concept_id=159777,o.value_coded,null)) as condom_provided ,
max(if(o.concept_id=161558,o.value_coded,null)) as screened_for_sti,
max(if(o.concept_id=164934,o.value_coded,null)) as cacx_screening,
max(if(o.concept_id=164935,o.value_coded,null)) as sti_partner_notification,
max(if(o.concept_id=160581,o.value_coded,null)) as at_risk_population,
max(if(o.concept_id=159615,o.value_coded,null)) as system_review_finding,
max(if(o.concept_id=5096,o.value_datetime,null)) as next_appointment_date,
max(if(o.concept_id=160288,o.value_coded,null)) as next_appointment_reason,
max(if(o.concept_id=1855,o.value_coded,null)) as stability,
max(if(o.concept_id=164947,o.value_coded,null)) as differentiated_care,
e.voided as voided
from encounter e
	inner join person p on p.person_id=e.patient_id and p.voided=0
inner join
(
	select encounter_type_id, uuid, name from encounter_type where uuid in('a0034eee-1940-4e35-847f-97537a35d05e','d1059fb9-a079-4feb-a749-eedd709ae542', '465a92f2-baf8-42e9-9612-53064be868e8')
) et on et.encounter_type_id=e.encounter_type
left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
	and o.concept_id in (1282,1246,161643,5089,5085,5086,5090,5088,5087,5242,5092,1343,5356,5272,161033,163530,5596,1427,5624,1053,160653,374,160575,1659,161654,161652,162229,162230,1658,160582,160632,159423,161557,159777,161558,160581,5096,163300, 164930, 160581, 1154, 160430, 164948, 164949, 164950, 1271, 307, 12, 162202, 1272, 163752, 163414, 162275, 160557, 162747,
121764, 164933, 160080, 1823, 164940, 164934, 164935, 159615, 160288, 1855, 164947)
where e.voided=0
group by e.patient_id, e.encounter_id, visit_date
;
SELECT "Completed processing HIV Followup data ", CONCAT("Time: ", NOW());
END$$


-- ------------- populate etl_laboratory_extract  uuid:  --------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_laboratory_extract$$
CREATE PROCEDURE sp_populate_etl_laboratory_extract()
BEGIN
SELECT "Processing Laboratory data ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_laboratory_extract(
uuid,
encounter_id,
patient_id,
location_id,
visit_date,
visit_id,
order_id,
lab_test,
urgency,
test_result,
-- date_test_requested,
-- date_test_result_received,
-- test_requested_by,
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
o.order_id,
o.concept_id,
od.urgency,
(CASE when o.concept_id in(5497,730,654,790,856) then o.value_numeric
	when o.concept_id in(1030,1305) then o.value_coded
	END) AS test_result,
-- date requested,
-- date result received
-- test requested by
e.date_created,
e.creator
from encounter e
	inner join person p on p.person_id=e.patient_id and p.voided=0
	inner join
(
	select encounter_type_id, uuid, name from encounter_type where uuid in('17a381d1-7e29-406a-b782-aa903b963c28', 'a0034eee-1940-4e35-847f-97537a35d05e','e1406e88-e9a9-11e8-9f32-f2801f1b9fd1', 'de78a6be-bfc5-4634-adc3-5f1a280455cc')
) et on et.encounter_type_id=e.encounter_type
inner join obs o on e.encounter_id=o.encounter_id and o.voided=0 and o.concept_id in (5497,730,654,790,856,1030,1305)
left join orders od on od.order_id = o.order_id and od.voided=0
where e.voided=0
;

/*-- >>>>>>>>>>>>>>> -----------------------------------  Wagners input ------------------------------------------------------------
insert into kenyaemr_etl.etl_laboratory_extract(
encounter_id,
patient_id,
visit_date,
visit_id,
lab_test,
test_result,
-- date_test_requested,
-- date_test_result_received,
-- test_requested_by,
date_created,
created_by
)
select
e.encounter_id,
e.patient_id,
e.encounter_datetime as visit_date,
e.visit_id,
o.concept_id,
(CASE when o.concept_id in(5497,730,654,790,856,21) then o.value_numeric
when o.concept_id in(299,1030,302,32) then o.value_coded
END) AS test_result,
-- date requested,
-- date result received
-- test requested by
e.date_created,
e.creator
from encounter e, obs o, encounter_type et
where e.encounter_id=o.encounter_id and o.voided=0
and o.concept_id in (5497,730,299,654,790,856,1030,21,302,32) and et.encounter_type_id=e.encounter_type
group by e.encounter_id;

-- --------<<<<<<<<<<<<<<<<<<<< ------------------------------------------------------------------------------------------------------
*/
SELECT "Completed processing Laboratory data ", CONCAT("Time: ", NOW());
END$$


-- ------------- populate etl_pharmacy_extract table--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_pharmacy_extract$$
CREATE PROCEDURE sp_populate_etl_pharmacy_extract()
BEGIN
SELECT "Processing Pharmacy data ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_pharmacy_extract(
obs_group_id,
patient_id,
uuid,
visit_date,
visit_id,
encounter_id,
date_created,
encounter_name,
location_id,
drug,
drug_name,
is_arv,
is_ctx,
is_dapsone,
frequency,
duration,
duration_units,
voided,
date_voided,
dispensing_provider
)
select
	o.obs_group_id obs_group_id,
	o.person_id,
	max(if(o.concept_id=1282, o.uuid, null)),
	date(o.obs_datetime) as enc_date,
	e.visit_id,
	o.encounter_id,
	e.date_created,
	et.name as enc_name,
	e.location_id,
	max(if(o.concept_id = 1282 and o.value_coded is not null,o.value_coded, null)) as drug_dispensed,
	max(if(o.concept_id = 1282, left(cn.name,255), 0)) as drug_name, -- arv:1085
	max(if(o.concept_id = 1282 and cs.concept_set=1085, 1, 0)) as arv_drug, -- arv:1085
	max(if(o.concept_id = 1282 and o.value_coded = 105281,1, 0)) as is_ctx,
	max(if(o.concept_id = 1282 and o.value_coded = 74250,1, 0)) as is_dapsone,
	max(if(o.concept_id = 1443, o.value_numeric, null)) as dose,
	max(if(o.concept_id = 159368, if(o.value_numeric > 10000, 10000, o.value_numeric), null)) as duration, -- catching typos in duration field
	max(if(o.concept_id = 1732 and o.value_coded=1072,'Days',if(o.concept_id=1732 and o.value_coded=1073,'Weeks',if(o.concept_id=1732 and o.value_coded=1074,'Months',null)))) as duration_units,
	o.voided,
	o.date_voided,
	e.creator
from obs o
	inner join person p on p.person_id=o.person_id and p.voided=0
	left outer join encounter e on e.encounter_id = o.encounter_id and e.voided=0
left outer join encounter_type et on et.encounter_type_id = e.encounter_type
left outer join concept_name cn on o.value_coded = cn.concept_id and cn.locale='en' and cn.concept_name_type='FULLY_SPECIFIED' -- SHORT'
left outer join concept_set cs on o.value_coded = cs.concept_id
where o.voided=0 and o.concept_id in(1282,1732,159368,1443,1444)  and e.voided=0
group by o.obs_group_id, o.person_id, encounter_id
having drug_dispensed is not null and obs_group_id is not null;

update kenyaemr_etl.etl_pharmacy_extract
	set duration_in_days = if(duration_units= 'Days', duration,if(duration_units='Weeks',duration * 7,if(duration_units='Months',duration * 31,null)))
	where (duration is not null or duration <> "") and (duration_units is not null or duration_units <> "");

SELECT "Completed processing Pharmacy data ", CONCAT("Time: ", NOW());
END$$


-- ------------ create table etl_patient_treatment_event----------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_program_discontinuation$$
CREATE PROCEDURE sp_populate_etl_program_discontinuation()
BEGIN
SELECT "Processing Program (HIV, TB, MCH,IPT,OTZ,OVC ...) discontinuations ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_patient_program_discontinuation(
patient_id,
uuid,
visit_id,
visit_date,
program_uuid,
program_name,
encounter_id,
discontinuation_reason,
effective_discontinuation_date,
date_died,
transfer_facility,
transfer_date
)
select
e.patient_id,
e.uuid,
e.visit_id,
e.encounter_datetime, -- trying to make us of index
et.uuid,
(case et.uuid
	when '2bdada65-4c72-4a48-8730-859890e25cee' then 'HIV'
	when 'd3e3d723-7458-4b4e-8998-408e8a551a84' then 'TB'
	when '01894f88-dc73-42d4-97a3-0929118403fb' then 'MCH Child HEI'
	when '5feee3f1-aa16-4513-8bd0-5d9b27ef1208' then 'MCH Child'
	when '7c426cfc-3b47-4481-b55f-89860c21c7de' then 'MCH Mother'
	when 'bb77c683-2144-48a5-a011-66d904d776c9' then 'IPT'
	when '162382b8-0464-11ea-9a9f-362b9e155667' then 'OTZ'
	when '5cf00d9e-09da-11ea-8d71-362b9e155667' then 'OVC'
end) as program_name,
e.encounter_id,
max(if(o.concept_id=161555, o.value_coded, null)) as reason_discontinued,
max(if(o.concept_id=164384, o.value_datetime, null)) as effective_discontinuation_date,
max(if(o.concept_id=1543, o.value_datetime, null)) as date_died,
max(if(o.concept_id=159495, left(trim(o.value_text),100), null)) as to_facility,
max(if(o.concept_id=160649, o.value_datetime, null)) as to_date
from encounter e
	inner join person p on p.person_id=e.patient_id and p.voided=0
	inner join obs o on o.encounter_id=e.encounter_id and o.voided=0 and o.concept_id in (161555,164384,1543,159495,160649)
inner join
(
	select encounter_type_id, uuid, name from encounter_type where
	uuid in('2bdada65-4c72-4a48-8730-859890e25cee','d3e3d723-7458-4b4e-8998-408e8a551a84','5feee3f1-aa16-4513-8bd0-5d9b27ef1208',
	'7c426cfc-3b47-4481-b55f-89860c21c7de','01894f88-dc73-42d4-97a3-0929118403fb','bb77c683-2144-48a5-a011-66d904d776c9','162382b8-0464-11ea-9a9f-362b9e155667','5cf00d9e-09da-11ea-8d71-362b9e155667')
) et on et.encounter_type_id=e.encounter_type
where e.voided=0
group by e.encounter_id;
SELECT "Completed processing discontinuation data ", CONCAT("Time: ", NOW());
END$$

-- ------------- populate etl_mch_enrollment-------------------------
DROP PROCEDURE IF EXISTS sp_populate_etl_mch_enrollment$$
CREATE PROCEDURE sp_populate_etl_mch_enrollment()
	BEGIN
		SELECT "Processing MCH Enrollments ", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_mch_enrollment(
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
			blood_group,
			serology,
			tb_screening,
			bs_for_mps,
			hiv_status,
			hiv_test_date,
			partner_hiv_status,
			partner_hiv_test_date,
			urine_microscopy,
			urinary_albumin,
			glucose_measurement,
			urine_ph,
			urine_gravity,
			urine_nitrite_test,
			urine_leukocyte_esterace_test,
			urinary_ketone,
			urine_bile_salt_test,
			urine_bile_pigment_test,
			urine_colour,
			urine_turbidity,
			urine_dipstick_for_blood,
			-- date_of_discontinuation,
			discontinuation_reason
		)
			select
				e.patient_id,
				e.uuid,
				e.visit_id,
				e.encounter_datetime,
				e.location_id,
				e.encounter_id,
				max(if(o.concept_id=163530,o.value_text,null)) as anc_number,
				max(if(o.concept_id=163547,o.value_datetime,null)) as first_anc_visit_date,
				max(if(o.concept_id=5624,o.value_numeric,null)) as gravida,
				max(if(o.concept_id=160080,o.value_numeric,null)) as parity,
				max(if(o.concept_id=1823,o.value_numeric,null)) as parity_abortion,
				max(if(o.concept_id=160598,o.value_numeric,null)) as age_at_menarche,
				max(if(o.concept_id=1427,o.value_datetime,null)) as lmp,
				max(if(o.concept_id=162095,o.value_datetime,null)) as lmp_estimated,
				max(if(o.concept_id=5596,o.value_datetime,null)) as edd_ultrasound,
				max(if(o.concept_id=300,o.value_coded,null)) as blood_group,
				max(if(o.concept_id=299,o.value_coded,null)) as serology,
				max(if(o.concept_id=160108,o.value_coded,null)) as tb_screening,
				max(if(o.concept_id=32,o.value_coded,null)) as bs_for_mps,
				max(if(o.concept_id=159427,o.value_coded,null)) as hiv_status,
				max(if(o.concept_id=160554,o.value_datetime,null)) as hiv_test_date,
				max(if(o.concept_id=1436,o.value_coded,null)) as partner_hiv_status,
				max(if(o.concept_id=160082,o.value_datetime,null)) as partner_hiv_test_date,
				max(if(o.concept_id=56,o.value_text,null)) as urine_microscopy,
				max(if(o.concept_id=1875,o.value_coded,null)) as urinary_albumin,
				max(if(o.concept_id=159734,o.value_coded,null)) as glucose_measurement,
				max(if(o.concept_id=161438,o.value_numeric,null)) as urine_ph,
				max(if(o.concept_id=161439,o.value_numeric,null)) as urine_gravity,
				max(if(o.concept_id=161440,o.value_coded,null)) as urine_nitrite_test,
				max(if(o.concept_id=161441,o.value_coded,null)) as urine_leukocyte_esterace_test,
				max(if(o.concept_id=161442,o.value_coded,null)) as urinary_ketone,
				max(if(o.concept_id=161444,o.value_coded,null)) as urine_bile_salt_test,
				max(if(o.concept_id=161443,o.value_coded,null)) as urine_bile_pigment_test,
				max(if(o.concept_id=162106,o.value_coded,null)) as urine_colour,
				max(if(o.concept_id=162101,o.value_coded,null)) as urine_turbidity,
				max(if(o.concept_id=162096,o.value_coded,null)) as urine_dipstick_for_blood,
				-- max(if(o.concept_id=161655,o.value_text,null)) as date_of_discontinuation,
				max(if(o.concept_id=161555,o.value_coded,null)) as discontinuation_reason
			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(163530,163547,5624,160080,1823,160598,1427,162095,5596,300,299,160108,32,159427,160554,1436,160082,56,1875,159734,161438,161439,161440,161441,161442,161444,161443,162106,162101,162096,161555)
				inner join
				(
					select encounter_type_id, uuid, name from encounter_type where
						uuid in('3ee036d8-7c13-4393-b5d6-036f2fe45126')
				) et on et.encounter_type_id=e.encounter_type
				where e.voided=0
			group by e.encounter_id;
		SELECT "Completed processing MCH Enrollments ", CONCAT("Time: ", NOW());
		END$$
-- ------------- populate etl_mch_antenatal_visit-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_mch_antenatal_visit$$
CREATE PROCEDURE sp_populate_etl_mch_antenatal_visit()
	BEGIN
		SELECT "Processing MCH antenatal visits ", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_mch_antenatal_visit(
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
			breast_exam_done,
			pallor,
			maturity,
			fundal_height,
			fetal_presentation,
			lie,
			fetal_heart_rate,
			fetal_movement,
			who_stage,
			cd4,
			viral_load,
			ldl,
			arv_status,
			test_1_kit_name,
			test_1_kit_lot_no,
			test_1_kit_expiry,
			test_1_result,
			test_2_kit_name,
			test_2_kit_lot_no,
			test_2_kit_expiry,
			test_2_result,
			final_test_result,
			patient_given_result,
			partner_hiv_tested,
			partner_hiv_status,
			prophylaxis_given,
			baby_azt_dispensed,
			baby_nvp_dispensed,
			TTT,
			IPT_malaria,
			iron_supplement,
			deworming,
			bed_nets,
			urine_microscopy,
			urinary_albumin,
			glucose_measurement,
			urine_ph,
			urine_gravity,
			urine_nitrite_test,
			urine_leukocyte_esterace_test,
			urinary_ketone,
			urine_bile_salt_test,
			urine_bile_pigment_test,
			urine_colour,
			urine_turbidity,
			urine_dipstick_for_blood,
			syphilis_test_status,
			syphilis_treated_status,
			bs_mps,
			anc_exercises,
			tb_screening,
			cacx_screening,
			cacx_screening_method,
			has_other_illnes,
			counselled,
			referred_from,
			referred_to,
			next_appointment_date,
			clinical_notes
		)
			select
				e.patient_id,
				e.uuid,
				e.visit_id,
				e.encounter_datetime,
				e.location_id,
				e.encounter_id,
				e.creator,
				max(if(o.concept_id=1425,o.value_numeric,null)) as anc_visit_number,
				max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
				max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
				max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_bp,
				max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_bp,
				max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
				max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
				max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
				max(if(o.concept_id=5090,o.value_numeric,null)) as height,
				max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
				max(if(o.concept_id=21,o.value_numeric,null)) as hemoglobin,
				max(if(o.concept_id=163590,o.value_coded,null)) as breast_exam_done,
				max(if(o.concept_id=5245,o.value_coded,null)) as pallor,
				max(if(o.concept_id=1438,o.value_numeric,null)) as maturity,
				max(if(o.concept_id=1439,o.value_numeric,null)) as fundal_height,
				max(if(o.concept_id=160090,o.value_coded,null)) as fetal_presentation,
				max(if(o.concept_id=162089,o.value_coded,null)) as lie,
				max(if(o.concept_id=1440,o.value_numeric,null)) as fetal_heart_rate,
				max(if(o.concept_id=162107,o.value_coded,null)) as fetal_movement,
				max(if(o.concept_id=5356,o.value_coded,null)) as who_stage,
				max(if(o.concept_id=5497,o.value_numeric,null)) as cd4,
				max(if(o.concept_id=856,o.value_numeric,null)) as viral_load,
				max(if(o.concept_id=1305,o.value_coded,null)) as ldl,
				max(if(o.concept_id=1147,o.value_coded,null)) as arv_status,
				max(if(t.test_1_result is not null, t.kit_name, null)) as test_1_kit_name,
				max(if(t.test_1_result is not null, t.lot_no, null)) as test_1_kit_lot_no,
				max(if(t.test_1_result is not null, t.expiry_date, null)) as test_1_kit_expiry,
				max(if(t.test_1_result is not null, t.test_1_result, null)) as test_1_result,
				max(if(t.test_2_result is not null, t.kit_name, null)) as test_2_kit_name,
				max(if(t.test_2_result is not null, t.lot_no, null)) as test_2_kit_lot_no,
				max(if(t.test_2_result is not null, t.expiry_date, null)) as test_2_kit_expiry,
				max(if(t.test_2_result is not null, t.test_2_result, null)) as test_2_result,
				max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" else "" end),null)) as final_test_result,
				max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_given_result,
				max(if(o.concept_id=161557,o.value_coded,null)) as partner_hiv_tested,
				max(if(o.concept_id=1436,o.value_coded,null)) as partner_hiv_status,
				max(if(o.concept_id=1109,o.value_coded,null)) as prophylaxis_given,
				max(if(o.concept_id=1282,o.value_coded,null)) as baby_azt_dispensed,
				max(if(o.concept_id=1282,o.value_coded,null)) as baby_nvp_dispensed,
				max(if(o.concept_id=984,(case o.value_coded when 84879 then "Yes" else "" end),null)) as TTT,
				max(if(o.concept_id=984,(case o.value_coded when 159610 then "Yes" else "" end),null)) as IPT_malaria,
				max(if(o.concept_id=984,(case o.value_coded when 104677 then "Yes" else "" end),null)) as iron_supplement,
				max(if(o.concept_id=984,(case o.value_coded when 79413 then "Yes"  else "" end),null)) as deworming,
				max(if(o.concept_id=984,(case o.value_coded when 160428 then "Yes" else "" end),null)) as bed_nets,
				max(if(o.concept_id=56,o.value_text,null)) as urine_microscopy,
				max(if(o.concept_id=1875,o.value_coded,null)) as urinary_albumin,
				max(if(o.concept_id=159734,o.value_coded,null)) as glucose_measurement,
				max(if(o.concept_id=161438,o.value_numeric,null)) as urine_ph,
				max(if(o.concept_id=161439,o.value_numeric,null)) as urine_gravity,
				max(if(o.concept_id=161440,o.value_coded,null)) as urine_nitrite_test,
				max(if(o.concept_id=161441,o.value_coded,null)) as urine_leukocyte_esterace_test,
				max(if(o.concept_id=161442,o.value_coded,null)) as urinary_ketone,
				max(if(o.concept_id=161444,o.value_coded,null)) as urine_bile_salt_test,
				max(if(o.concept_id=161443,o.value_coded,null)) as urine_bile_pigment_test,
				max(if(o.concept_id=162106,o.value_coded,null)) as urine_colour,
				max(if(o.concept_id=162101,o.value_coded,null)) as urine_turbidity,
				max(if(o.concept_id=162096,o.value_coded,null)) as urine_dipstick_for_blood,
				max(if(o.concept_id=299,o.value_coded,null)) as syphilis_test_status,
				max(if(o.concept_id=159918,o.value_coded,null)) as syphilis_treated_status,
				max(if(o.concept_id=32,o.value_coded,null)) as bs_mps,
				max(if(o.concept_id=161074,o.value_coded,null)) as anc_exercises,
				max(if(o.concept_id=1659,o.value_coded,null)) as tb_screening,
				max(if(o.concept_id=164934,o.value_coded,null)) as cacx_screening,
				max(if(o.concept_id=163589,o.value_coded,null)) as cacx_screening_method,
				max(if(o.concept_id=162747,o.value_coded,null)) as has_other_illnes,
				max(if(o.concept_id=1912,o.value_coded,null)) as counselled,
				max(if(o.concept_id=160481,o.value_coded,null)) as referred_from,
				max(if(o.concept_id=163145,o.value_coded,null)) as referred_to,
				max(if(o.concept_id=5096,o.value_datetime,null)) as next_appointment_date,
				max(if(o.concept_id=159395,o.value_text,null)) as clinical_notes

			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(1282,984,1425,5088,5087,5085,5086,5242,5092,5089,5090,1343,21,163590,5245,1438,1439,160090,162089,1440,162107,5356,5497,856,1305,1147,159427,164848,161557,1436,1109,128256,1875,159734,161438,161439,161440,161441,161442,161444,161443,162106,162101,162096,299,159918,32,161074,1659,164934,163589,162747,1912,160481,163145,5096,159395)
				inner join
				(
					select form_id, uuid,name from form where
						uuid in('e8f98494-af35-4bb8-9fc7-c409c8fed843','d3ea25c7-a3e8-4f57-a6a9-e802c3565a30')
				) f on f.form_id=e.form_id
				left join (
										 select
											 o.person_id,
											 o.encounter_id,
											 o.obs_group_id,
											 max(if(o.concept_id=1040, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 163611 then "Invalid"  else "" end),null)) as test_1_result ,
											 max(if(o.concept_id=1326, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_2_result ,
											 max(if(o.concept_id=164962, (case o.value_coded when 164960 then "Determine" when 164961 then "First Response" else "" end),null)) as kit_name ,
											 max(if(o.concept_id=164964,trim(o.value_text),null)) as lot_no,
											 max(if(o.concept_id=162502,date(o.value_datetime),null)) as expiry_date
										 from obs o
											 inner join encounter e on e.encounter_id = o.encounter_id
											 inner join form f on f.form_id=e.form_id and f.uuid in ('e8f98494-af35-4bb8-9fc7-c409c8fed843')
										 where o.concept_id in (1040, 1326, 164962, 164964, 162502) and o.voided=0
										 group by e.encounter_id, o.obs_group_id
									 ) t on e.encounter_id = t.encounter_id
    where e.voided=0
			group by e.encounter_id;
		SELECT "Completed processing MCH antenatal visits ", CONCAT("Time: ", NOW());
		END$$


-- ------------- populate etl_mchs_delivery-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_mch_delivery$$
CREATE PROCEDURE sp_populate_etl_mch_delivery()
	BEGIN
		SELECT "Processing MCH Delivery visits", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_mchs_delivery(
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
			mode_of_delivery,
			date_of_delivery,
			blood_loss,
			condition_of_mother,
			apgar_score_1min,
			apgar_score_5min,
			apgar_score_10min,
			resuscitation_done,
			place_of_delivery,
			delivery_assistant,
			counseling_on_infant_feeding ,
			counseling_on_exclusive_breastfeeding,
			counseling_on_infant_feeding_for_hiv_infected,
			mother_decision,
			placenta_complete,
			maternal_death_audited,
			cadre,
			delivery_complications,
			coded_delivery_complications,
			other_delivery_complications,
			duration_of_labor,
			baby_sex,
			baby_condition,
			teo_given,
			birth_weight,
			bf_within_one_hour,
			birth_with_deformity,
			test_1_kit_name,
			test_1_kit_lot_no,
			test_1_kit_expiry,
			test_1_result,
			test_2_kit_name,
			test_2_kit_lot_no,
			test_2_kit_expiry,
			test_2_result,
			final_test_result,
			patient_given_result,
			partner_hiv_tested,
			partner_hiv_status,
			prophylaxis_given,
			baby_azt_dispensed,
			baby_nvp_dispensed,
			clinical_notes
		)
			select
				e.patient_id,
				e.uuid,
				e.creator,
				e.visit_id,
				e.encounter_datetime,
				e.location_id,
				e.encounter_id,
				e.date_created,
				max(if(o.concept_id=162054,o.value_text,null)) as admission_number,
				max(if(o.concept_id=1789,o.value_numeric,null)) as duration_of_pregnancy,
				max(if(o.concept_id=5630,o.value_coded,null)) as mode_of_delivery,
				max(if(o.concept_id=5599,o.value_datetime,null)) as date_of_delivery,
				max(if(o.concept_id=162092,o.value_coded,null)) as blood_loss,
				max(if(o.concept_id=1856,o.value_coded,null)) as condition_of_mother,
				max(if(o.concept_id=159603,o.value_numeric,null)) as apgar_score_1min,
				max(if(o.concept_id=159604,o.value_numeric,null)) as apgar_score_5min,
				max(if(o.concept_id=159605,o.value_numeric,null)) as apgar_score_10min,
				max(if(o.concept_id=162131,o.value_coded,null)) as resuscitation_done,
				max(if(o.concept_id=1572,o.value_coded,null)) as place_of_delivery,
				max(if(o.concept_id=1473,o.value_text,null)) as delivery_assistant,
				max(if(o.concept_id=1379 and o.value_coded=161651,o.value_coded,null)) as counseling_on_infant_feeding,
				max(if(o.concept_id=1379 and o.value_coded=161096,o.value_coded,null)) as counseling_on_exclusive_breastfeeding,
				max(if(o.concept_id=1379 and o.value_coded=162091,o.value_coded,null)) as counseling_on_infant_feeding_for_hiv_infected,
				max(if(o.concept_id=1151,o.value_coded,null)) as mother_decision,
				max(if(o.concept_id=163454,o.value_coded,null)) as placenta_complete,
				max(if(o.concept_id=1602,o.value_coded,null)) as maternal_death_audited,
				max(if(o.concept_id=1573,o.value_coded,null)) as cadre,
				max(if(o.concept_id=120216,o.value_coded,null)) as delivery_complications,
				max(if(o.concept_id=1576,o.value_coded,null)) as coded_delivery_complications,
				max(if(o.concept_id=162093,o.value_text,null)) as other_delivery_complications,
				max(if(o.concept_id=159616,o.value_numeric,null)) as duration_of_labor,
				max(if(o.concept_id=1587,o.value_coded,null)) as baby_sex,
				max(if(o.concept_id=159917,o.value_coded,null)) as baby_condition,
				max(if(o.concept_id=1282 and o.value_coded = 84893,1,0)) as teo_given,
				max(if(o.concept_id=5916,o.value_numeric,null)) as birth_weight,
				max(if(o.concept_id=161543,o.value_coded,null)) as bf_within_one_hour,
				max(if(o.concept_id=164122,o.value_coded,null)) as birth_with_deformity,
				max(if(t.test_1_result is not null, t.kit_name, null)) as test_1_kit_name,
				max(if(t.test_1_result is not null, t.lot_no, null)) as test_1_kit_lot_no,
				max(if(t.test_1_result is not null, t.expiry_date, null)) as test_1_kit_expiry,
				max(if(t.test_1_result is not null, t.test_1_result, null)) as test_1_result,
				max(if(t.test_2_result is not null, t.kit_name, null)) as test_2_kit_name,
				max(if(t.test_2_result is not null, t.lot_no, null)) as test_2_kit_lot_no,
				max(if(t.test_2_result is not null, t.expiry_date, null)) as test_2_kit_expiry,
				max(if(t.test_2_result is not null, t.test_2_result, null)) as test_2_result,
				max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" else "" end),null)) as final_test_result,
				max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_given_result,
				max(if(o.concept_id=161557,o.value_coded,null)) as partner_hiv_tested,
				max(if(o.concept_id=1436,o.value_coded,null)) as partner_hiv_status,
				max(if(o.concept_id=1109,o.value_coded,null)) as prophylaxis_given,
				max(if(o.concept_id = 1282 and o.value_coded = 160123,1,0)) as baby_azt_dispensed,
				max(if(o.concept_id = 1282 and o.value_coded = 80586,1,0)) as baby_nvp_dispensed,
				max(if(o.concept_id=159395,o.value_text,null)) as clinical_notes

			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(162054,1789,5630,5599,162092,1856,162093,159603,159604,159605,162131,1572,1473,1379,1151,163454,1602,1573,162093,1576,120216,159616,1587,159917,1282,5916,161543,164122,159427,164848,161557,1436,1109,5576,159595,163784,159395)
				inner join
				(
					select form_id, uuid,name from form where
						uuid in('496c7cc3-0eea-4e84-a04c-2292949e2f7f')
				) f on f.form_id=e.form_id
				left join (
										select
											o.person_id,
											o.encounter_id,
											o.obs_group_id,
											max(if(o.concept_id=1040, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 163611 then "Invalid"  else "" end),null)) as test_1_result ,
											max(if(o.concept_id=1326, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_2_result ,
											max(if(o.concept_id=164962, (case o.value_coded when 164960 then "Determine" when 164961 then "First Response" else "" end),null)) as kit_name ,
											max(if(o.concept_id=164964,trim(o.value_text),null)) as lot_no,
											max(if(o.concept_id=162502,date(o.value_datetime),null)) as expiry_date
										from obs o
											 inner join encounter e on e.encounter_id = o.encounter_id
											 inner join form f on f.form_id=e.form_id and f.uuid in ('496c7cc3-0eea-4e84-a04c-2292949e2f7f')
										 where o.concept_id in (1040, 1326, 164962, 164964, 162502) and o.voided=0
										 group by e.encounter_id, o.obs_group_id
									 ) t on e.encounter_id = t.encounter_id
			where e.voided=0
			group by e.encounter_id ;
		SELECT "Completed processing MCH Delivery visits", CONCAT("Time: ", NOW());
		END$$

-- ------------- populate etl_mchs_discharge-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_mch_discharge$$
CREATE PROCEDURE sp_populate_etl_mch_discharge()
	BEGIN
		SELECT "Processing MCH Discharge ", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_mchs_discharge(
			patient_id,
			uuid,
			provider,
			visit_id,
			visit_date,
			location_id,
			encounter_id,
			date_created,
			counselled_on_feeding,
			baby_status,
			vitamin_A_dispensed,
			birth_notification_number,
			condition_of_mother,
			discharge_date,
			referred_from,
			referred_to,
			clinical_notes
		)
			select
				e.patient_id,
				e.uuid,
				e.creator,
				e.visit_id,
				e.encounter_datetime,
				e.location_id,
				e.encounter_id,
				e.date_created,
				max(if(o.concept_id=161651,o.value_coded,null)) as counselled_on_feeding,
				max(if(o.concept_id=159926,o.value_coded,null)) as baby_status,
				max(if(o.concept_id=161534,o.value_coded,null)) as vitamin_A_dispensed,
				max(if(o.concept_id=162051,o.value_text,null)) as birth_notification_number,
				max(if(o.concept_id=162093,o.value_text,null)) as condition_of_mother,
				max(if(o.concept_id=1641,o.value_datetime,null)) as discharge_date,
				max(if(o.concept_id=160481,o.value_coded,null)) as referred_from,
				max(if(o.concept_id=163145,o.value_coded,null)) as referred_to,
				max(if(o.concept_id=159395,o.value_text,null)) as clinical_notes
			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(161651,159926,161534,162051,162093,1641,160481,163145,159395)
				inner join
				(
					select form_id, uuid,name from form where
						uuid in('af273344-a5f9-11e8-98d0-529269fb1459')
				) f on f.form_id=e.form_id
				where e.voided=0
			group by e.encounter_id ;
		SELECT "Completed processing MCH Discharge visits", CONCAT("Time: ", NOW());
		END$$

-- ------------- populate etl_mch_postnatal_visit-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_mch_postnatal_visit$$
CREATE PROCEDURE sp_populate_etl_mch_postnatal_visit()
	BEGIN
		SELECT "Processing MCH postnatal visits ", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_mch_postnatal_visit(
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
			mode_of_delivery,
			place_of_delivery,
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
			arv_status,
			general_condition,
			breast,
			cs_scar,
			gravid_uterus,
			episiotomy,
			lochia,
			pallor,
			pph,
			mother_hiv_status,
			condition_of_baby,
			baby_feeding_method,
			umblical_cord,
			baby_immunization_started,
			family_planning_counseling,
			uterus_examination,
			uterus_cervix_examination,
			vaginal_examination,
			parametrial_examination,
			external_genitalia_examination,
			ovarian_examination,
			pelvic_lymph_node_exam,
			test_1_kit_name,
			test_1_kit_lot_no,
			test_1_kit_expiry,
			test_1_result,
			test_2_kit_name,
			test_2_kit_lot_no,
			test_2_kit_expiry,
			test_2_result,
			final_test_result,
			patient_given_result,
			partner_hiv_tested,
			partner_hiv_status,
			prophylaxis_given,
			baby_azt_dispensed,
			baby_nvp_dispensed,
			pnc_exercises,
			maternal_condition,
			iron_supplementation,
			fistula_screening,
			cacx_screening,
			cacx_screening_method,
			family_planning_status,
			family_planning_method,
			referred_from,
			referred_to,
			clinical_notes
		)
			select
				e.patient_id,
				e.uuid,
				e.visit_id,
				e.encounter_datetime,
				e.location_id,
				e.encounter_id,
				e.creator,
				max(if(o.concept_id=1646,o.value_text,null)) as pnc_register_no,
				max(if(o.concept_id=159893,o.value_numeric,null)) as pnc_visit_no,
				max(if(o.concept_id=5599,o.value_datetime,null)) as delivery_date,
				max(if(o.concept_id=5630,o.value_coded,null)) as mode_of_delivery,
				max(if(o.concept_id=1572,o.value_coded,null)) as place_of_delivery,
				max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
				max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
				max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_bp,
				max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_bp,
				max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
				max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
				max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
				max(if(o.concept_id=5090,o.value_numeric,null)) as height,
				max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
				max(if(o.concept_id=21,o.value_numeric,null)) as hemoglobin,
				max(if(o.concept_id=1147,o.value_coded,null)) as arv_status,
				max(if(o.concept_id=1856,o.value_coded,null)) as general_condition,
				max(if(o.concept_id=159780,o.value_coded,null)) as breast,
				max(if(o.concept_id=162128,o.value_coded,null)) as cs_scar,
				max(if(o.concept_id=162110,o.value_coded,null)) as gravid_uterus,
				max(if(o.concept_id=159840,o.value_coded,null)) as episiotomy,
				max(if(o.concept_id=159844,o.value_coded,null)) as lochia,
				max(if(o.concept_id=5245,o.value_coded,null)) as pallor,
				max(if(o.concept_id=230,o.value_coded,null)) as pph,
				max(if(o.concept_id=1396,o.value_coded,null)) as mother_hiv_status,
				max(if(o.concept_id=162134,o.value_coded,null)) as condition_of_baby,
				max(if(o.concept_id=1151,o.value_coded,null)) as baby_feeding_method,
				max(if(o.concept_id=162121,o.value_coded,null)) as umblical_cord,
				max(if(o.concept_id=162127,o.value_coded,null)) as baby_immunization_started,
				max(if(o.concept_id=1382,o.value_coded,null)) as family_planning_counseling,
				max(if(o.concept_id=160967,o.value_text,null)) as uterus_examination,
				max(if(o.concept_id=160968,o.value_text,null)) as uterus_cervix_examination,
				max(if(o.concept_id=160969,o.value_text,null)) as vaginal_examination,
				max(if(o.concept_id=160970,o.value_text,null)) as parametrial_examination,
				max(if(o.concept_id=160971,o.value_text,null)) as external_genitalia_examination,
				max(if(o.concept_id=160975,o.value_text,null)) as ovarian_examination,
				max(if(o.concept_id=160972,o.value_text,null)) as pelvic_lymph_node_exam,
				max(if(t.test_1_result is not null, t.kit_name, null)) as test_1_kit_name,
				max(if(t.test_1_result is not null, t.lot_no, null)) as test_1_kit_lot_no,
				max(if(t.test_1_result is not null, t.expiry_date, null)) as test_1_kit_expiry,
				max(if(t.test_1_result is not null, t.test_1_result, null)) as test_1_result,
				max(if(t.test_2_result is not null, t.kit_name, null)) as test_2_kit_name,
				max(if(t.test_2_result is not null, t.lot_no, null)) as test_2_kit_lot_no,
				max(if(t.test_2_result is not null, t.expiry_date, null)) as test_2_kit_expiry,
				max(if(t.test_2_result is not null, t.test_2_result, null)) as test_2_result,
				max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" else "" end),null)) as final_test_result,
				max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_given_result,
				max(if(o.concept_id=161557,o.value_coded,null)) as partner_hiv_tested,
				max(if(o.concept_id=1436,o.value_coded,null)) as partner_hiv_status,
				max(if(o.concept_id=1109,o.value_coded,null)) as prophylaxis_given,
				max(if(o.concept_id=1282,o.value_coded,null)) as baby_azt_dispensed,
				max(if(o.concept_id=1282,o.value_coded,null)) as baby_nvp_dispensed,
				max(if(o.concept_id=161074,o.value_coded,null)) as pnc_exercises,
				max(if(o.concept_id=160085,o.value_coded,null)) as maternal_condition,
				max(if(o.concept_id=161004,o.value_coded,null)) as iron_supplementation,
				max(if(o.concept_id=159921,o.value_coded,null)) as fistula_screening,
				max(if(o.concept_id=164934,o.value_coded,null)) as cacx_screening,
				max(if(o.concept_id=163589,o.value_coded,null)) as cacx_screening_method,
				max(if(o.concept_id=160653,o.value_coded,null)) as family_planning_status,
				max(if(o.concept_id=374,o.value_coded,null)) as family_planning_method,
				max(if(o.concept_id=160481,o.value_coded,null)) as referred_from,
				max(if(o.concept_id=163145,o.value_coded,null)) as referred_to,
				max(if(o.concept_id=159395,o.value_text,null)) as clinical_notes


			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(1646,159893,5599,5630,1572,5088,5087,5085,5086,5242,5092,5089,5090,1343,21,1147,1856,159780,162128,162110,159840,159844,5245,230,1396,162134,1151,162121,162127,1382,160967,160968,160969,160970,160971,160975,160972,159427,164848,161557,1436,1109,5576,159595,163784,1282,161074,160085,161004,159921,164934,163589,160653,374,160481,163145,159395)
				inner join
				(
					select form_id, uuid,name from form where
						uuid in('72aa78e0-ee4b-47c3-9073-26f3b9ecc4a7')
				) f on f.form_id= e.form_id
				left join (
										 select
											 o.person_id,
											 o.encounter_id,
											 o.obs_group_id,
											 max(if(o.concept_id=1040, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 163611 then "Invalid"  else "" end),null)) as test_1_result ,
											 max(if(o.concept_id=1326, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_2_result ,
											 max(if(o.concept_id=164962, (case o.value_coded when 164960 then "Determine" when 164961 then "First Response" else "" end),null)) as kit_name ,
											 max(if(o.concept_id=164964,trim(o.value_text),null)) as lot_no,
											 max(if(o.concept_id=162502,date(o.value_datetime),null)) as expiry_date
										 from obs o
											 inner join encounter e on e.encounter_id = o.encounter_id
											 inner join form f on f.form_id=e.form_id and f.uuid in ('72aa78e0-ee4b-47c3-9073-26f3b9ecc4a7')
										 where o.concept_id in (1040, 1326, 164962, 164964, 162502) and o.voided=0
										 group by e.encounter_id, o.obs_group_id
									 ) t on e.encounter_id = t.encounter_id
			where e.voided=0
			group by e.encounter_id;
		SELECT "Completed processing MCH postnatal visits ", CONCAT("Time: ", NOW());
		END$$

-- ------------- populate etl_hei_enrollment-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_hei_enrolment$$
CREATE PROCEDURE sp_populate_etl_hei_enrolment()
	BEGIN
		SELECT "Processing HEI Enrollments", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_hei_enrollment(
			patient_id,
			uuid,
			provider,
			visit_id,
			visit_date,
			location_id,
			encounter_id,
			child_exposed,
			-- hei_id_number,
			spd_number,
			birth_weight,
			gestation_at_birth,
			date_first_seen,
			birth_notification_number,
			birth_certificate_number,
			need_for_special_care,
			reason_for_special_care,
			referral_source ,
			transfer_in,
			transfer_in_date,
			facility_transferred_from,
			district_transferred_from,
			date_first_enrolled_in_hei_care,
			-- arv_prophylaxis,
			mother_breastfeeding,
			-- mother_on_NVP_during_breastfeeding,
			TB_contact_history_in_household,
			-- infant_mother_link,
			mother_alive,
			mother_on_pmtct_drugs,
			mother_on_drug,
			mother_on_art_at_infant_enrollment,
			mother_drug_regimen,
			infant_prophylaxis,
			parent_ccc_number,
			mode_of_delivery,
			place_of_delivery,
			birth_length,
			birth_order,
			health_facility_name,
			date_of_birth_notification,
			date_of_birth_registration,
			birth_registration_place,
			permanent_registration_serial,
			mother_facility_registered,
			exit_date,
      exit_reason,
      hiv_status_at_exit
		)
			select
				e.patient_id,
				e.uuid,
				e.creator,
				e.visit_id,
				e.encounter_datetime,
				e.location_id,
				e.encounter_id,
				max(if(o.concept_id=5303,o.value_coded,null)) as child_exposed,
				-- max(if(o.concept_id=5087,o.value_numeric,null)) as hei_id_number,
				max(if(o.concept_id=162054,o.value_text,null)) as spd_number,
				max(if(o.concept_id=5916,o.value_numeric,null)) as birth_weight,
				max(if(o.concept_id=1409,o.value_numeric,null)) as gestation_at_birth,
				max(if(o.concept_id=162140,o.value_datetime,null)) as date_first_seen,
				max(if(o.concept_id=162051,o.value_text,null)) as birth_notification_number,
				max(if(o.concept_id=162052,o.value_text,null)) as birth_certificate_number,
				max(if(o.concept_id=161630,o.value_coded,null)) as need_for_special_care,
				max(if(o.concept_id=161601,o.value_coded,null)) as reason_for_special_care,
				max(if(o.concept_id=160540,o.value_coded,null)) as referral_source,
				max(if(o.concept_id=160563,o.value_coded,null)) as transfer_in,
				max(if(o.concept_id=160534,o.value_datetime,null)) as transfer_in_date,
				max(if(o.concept_id=160535,o.value_text,null)) as facility_transferred_from,
				max(if(o.concept_id=161551,o.value_text,null)) as district_transferred_from,
				max(if(o.concept_id=160555,o.value_datetime,null)) as date_first_enrolled_in_hei_care,
				-- max(if(o.concept_id=1282,o.value_coded,null)) as arv_prophylaxis,
				max(if(o.concept_id=159941,o.value_coded,null)) as mother_breastfeeding,
				-- max(if(o.concept_id=1282,o.value_coded,null)) as mother_on_NVP_during_breastfeeding,
				max(if(o.concept_id=152460,o.value_coded,null)) as TB_contact_history_in_household,
				-- max(if(o.concept_id=162121,o.value_coded,null)) as infant_mother_link,
				max(if(o.concept_id=160429,o.value_coded,null)) as mother_alive,
				max(if(o.concept_id=1148,o.value_coded,null)) as mother_on_pmtct_drugs,
				max(if(o.concept_id=1086,o.value_coded,null)) as mother_on_drug,
				max(if(o.concept_id=162055,o.value_coded,null)) as mother_on_art_at_infant_enrollment,
				max(if(o.concept_id=1088,o.value_coded,null)) as mother_drug_regimen,
				max(if(o.concept_id=1282,o.value_coded,null)) as infant_prophylaxis,
				max(if(o.concept_id=162053,o.value_numeric,null)) as parent_ccc_number,
				max(if(o.concept_id=5630,o.value_coded,null)) as mode_of_delivery,
				max(if(o.concept_id=1572,o.value_coded,null)) as place_of_delivery,
				max(if(o.concept_id=1503,o.value_numeric,null)) as birth_length,
				max(if(o.concept_id=163460,o.value_numeric,null)) as birth_order,
				max(if(o.concept_id=162724,o.value_text,null)) as health_facility_name,
				max(if(o.concept_id=164130,o.value_datetime,null)) as date_of_birth_notification,
				max(if(o.concept_id=164129,o.value_datetime,null)) as date_of_birth_registration,
				max(if(o.concept_id=164140,o.value_text,null)) as birth_registration_place,
				max(if(o.concept_id=1646,o.value_text,null)) as permanent_registration_serial,
				max(if(o.concept_id=162724,o.value_text,null)) as mother_facility_registered,
			  max(if(o.concept_id=160753,o.value_datetime,null)) as exit_date,
			  max(if(o.concept_id=161555,o.value_coded,null)) as exit_reason,
			  max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" else "" end),null)) as hiv_status_at_exit
			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(5303,162054,5916,1409,162140,162051,162052,161630,161601,160540,160563,160534,160535,161551,160555,1282,159941,1282,152460,160429,1148,1086,162055,1088,1282,162053,5630,1572,161555,159427,1503,163460,162724,164130,164129,164140,1646,160753,161555,159427)

				inner join
				(
					select encounter_type_id, uuid, name from encounter_type where
						uuid in('415f5136-ca4a-49a8-8db3-f994187c3af6','01894f88-dc73-42d4-97a3-0929118403fb')
				) et on et.encounter_type_id=e.encounter_type
				where e.voided=0
			group by e.encounter_id ;
		SELECT "Completed processing HEI Enrollments", CONCAT("Time: ", NOW());
		END$$


-- ------------- populate etl_hei_follow_up_visit-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_hei_follow_up$$
CREATE PROCEDURE sp_populate_etl_hei_follow_up()
	BEGIN
		SELECT "Processing HEI Followup visits", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_hei_follow_up_visit(
			patient_id,
			uuid,
			provider,
			visit_id,
			visit_date,
			location_id,
			encounter_id,
			weight,
			height,
			primary_caregiver,
			infant_feeding,
			tb_assessment_outcome,
			social_smile_milestone,
			head_control_milestone,
			response_to_sound_milestone,
			hand_extension_milestone,
			sitting_milestone,
			walking_milestone,
			standing_milestone,
			talking_milestone,
			review_of_systems_developmental,
			dna_pcr_sample_date,
			dna_pcr_contextual_status,
			dna_pcr_result,
			azt_given,
			nvp_given,
			ctx_given,
			-- dna_pcr_dbs_sample_code,
			-- dna_pcr_results_date,
			-- first_antibody_sample_date,
			first_antibody_result,
			-- first_antibody_dbs_sample_code,
			-- first_antibody_result_date,
			-- final_antibody_sample_date,
			final_antibody_result,
			-- final_antibody_dbs_sample_code,
			-- final_antibody_result_date,
			tetracycline_ointment_given,
			pupil_examination,
			sight_examination,
			squint,
			deworming_drug,
			dosage,
			unit,
			comments,
			next_appointment_date
		)
			select
				e.patient_id,
				e.uuid,
				e.creator,
				e.visit_id,
				e.encounter_datetime,
				e.location_id,
				e.encounter_id,
				max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
				max(if(o.concept_id=5090,o.value_numeric,null)) as height,
				max(if(o.concept_id=160640,o.value_coded,null)) as primary_caregiver,
				max(if(o.concept_id=1151,o.value_coded,null)) as infant_feeding,
				max(if(o.concept_id=1659,o.value_coded,null)) as tb_assessment_outcome,
				max(if(o.concept_id=162069 and o.value_coded=162056,o.value_coded,null)) as social_smile_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162057,o.value_coded,null)) as head_control_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162058,o.value_coded,null)) as response_to_sound_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162059,o.value_coded,null)) as hand_extension_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162061,o.value_coded,null)) as sitting_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162063,o.value_coded,null)) as walking_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162062,o.value_coded,null)) as standing_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162060,o.value_coded,null)) as talking_milestone,
				max(if(o.concept_id=1189,o.value_coded,null)) as review_of_systems_developmental,
				max(if(o.concept_id=159951,o.value_datetime,null)) as dna_pcr_sample_date,
				max(if(o.concept_id=162084,o.value_coded,null)) as dna_pcr_contextual_status,
				max(if(o.concept_id=1030,o.value_coded,null)) as dna_pcr_result,
				max(if(o.concept_id=966 and o.value_coded=86663,o.value_coded,null)) as azt_given,
				max(if(o.concept_id=966 and o.value_coded=80586,o.value_coded,null)) as nvp_given,
				max(if(o.concept_id=1109,o.value_coded,null)) as ctx_given,
				-- max(if(o.concept_id=162086,o.value_text,null)) as dna_pcr_dbs_sample_code,
				-- max(if(o.concept_id=160082,o.value_datetime,null)) as dna_pcr_results_date,
				-- max(if(o.concept_id=159951,o.value_datetime,null)) as first_antibody_sample_date,
				max(if(o.concept_id=1040,o.value_coded,null)) as first_antibody_result,
				-- max(if(o.concept_id=162086,o.value_text,null)) as first_antibody_dbs_sample_code,
				-- max(if(o.concept_id=160082,o.value_datetime,null)) as first_antibody_result_date,
				-- max(if(o.concept_id=159951,o.value_datetime,null)) as final_antibody_sample_date,
				max(if(o.concept_id=1326,o.value_coded,null)) as final_antibody_result,
				-- max(if(o.concept_id=162086,o.value_text,null)) as final_antibody_dbs_sample_code,
				-- max(if(o.concept_id=160082,o.value_datetime,null)) as final_antibody_result_date,
				max(if(o.concept_id=162077,o.value_coded,null)) as tetracycline_ointment_given,
				max(if(o.concept_id=162064,o.value_coded,null)) as pupil_examination,
				max(if(o.concept_id=162067,o.value_coded,null)) as sight_examination,
				max(if(o.concept_id=162066,o.value_coded,null)) as squint,
				max(if(o.concept_id=1282,o.value_coded,null)) as deworming_drug,
				max(if(o.concept_id=1443,o.value_numeric,null)) as dosage,
				max(if(o.concept_id=1621,o.value_text,null)) as unit,
				max(if(o.concept_id=159395,o.value_text,null)) as comments,
				max(if(o.concept_id=5096,o.value_datetime,null)) as next_appointment_date
			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(844,5089,5090,160640,1151,1659,5096,162069,162069,162069,162069,162069,162069,162069,162069,1189,159951,966,1109,162084,1030,162086,160082,159951,1040,162086,160082,159951,1326,162086,160082,162077,162064,162067,162066,1282,1443,1621,159395,5096)
				inner join
				(
					select encounter_type_id, uuid, name from encounter_type where
						uuid in('bcc6da85-72f2-4291-b206-789b8186a021','c6d09e05-1f25-4164-8860-9f32c5a02df0')
				) et on et.encounter_type_id=e.encounter_type
			where e.voided=0
			group by e.encounter_id ;

		SELECT "Completed processing HEI Followup visits", CONCAT("Time: ", NOW());
		END$$

-- ------------- populate etl_immunization   --------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_hei_immunization$$
CREATE PROCEDURE sp_populate_etl_hei_immunization()
	BEGIN
		SELECT "Processing hei_immunization data ", CONCAT("Time: ", NOW());
    insert into kenyaemr_etl.etl_hei_immunization(
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
    )
      select
        patient_id,
        visit_date,
        y.creator,
        y.date_created,
        y.encounter_id,
        max(if(vaccine="BCG", date_given, "")) as BCG,
        max(if(vaccine="OPV" and sequence=0, date_given, "")) as OPV_birth,
        max(if(vaccine="OPV" and sequence=1, date_given, "")) as OPV_1,
        max(if(vaccine="OPV" and sequence=2, date_given, "")) as OPV_2,
        max(if(vaccine="OPV" and sequence=3, date_given, "")) as OPV_3,
        max(if(vaccine="IPV", date_given, ""))  as IPV,
        max(if(vaccine="DPT" and sequence=1, date_given, "")) as DPT_Hep_B_Hib_1,
        max(if(vaccine="DPT" and sequence=2, date_given, "")) as DPT_Hep_B_Hib_2,
        max(if(vaccine="DPT" and sequence=3, date_given, "")) as DPT_Hep_B_Hib_3,
        max(if(vaccine="PCV" and sequence=1, date_given, "")) as PCV_10_1,
        max(if(vaccine="PCV" and sequence=2, date_given, "")) as PCV_10_2,
        max(if(vaccine="PCV" and sequence=3, date_given, "")) as PCV_10_3,
        max(if(vaccine="ROTA" and sequence=1, date_given, "")) as ROTA_1,
        max(if(vaccine="ROTA" and sequence=2, date_given, "")) as ROTA_2,
        max(if(vaccine="measles_rubella" and sequence=1, date_given, "")) as Measles_rubella_1,
        max(if(vaccine="measles_rubella" and sequence=2, date_given, "")) as Measles_rubella_2,
        max(if(vaccine="yellow_fever", date_given, "")) as Yellow_fever,
        max(if(vaccine="measles", date_given, "")) as Measles_6_months,
        max(if(vaccine="Vitamin A" and sequence=1, date_given, "")) as VitaminA_6_months,
        max(if(vaccine="Vitamin A" and sequence=2, date_given, "")) as VitaminA_1_yr,
        max(if(vaccine="Vitamin A" and sequence=3, date_given, "")) as VitaminA_1_and_half_yr,
        max(if(vaccine="Vitamin A" and sequence=4, date_given, "")) as VitaminA_2_yr,
        max(if(vaccine="Vitamin A" and sequence=5, date_given, "")) as VitaminA_2_to_5_yr,
				max(date(o.value_datetime)) as fully_immunized
      from (
						 (select
								person_id as patient_id,
								date(encounter_datetime) as visit_date,
								creator,
								date(date_created) as date_created,
								encounter_id,
								name as encounter_type,
								max(if(concept_id=1282 , "Vitamin A", "")) as vaccine,
								max(if(concept_id=1418, value_numeric, "")) as sequence,
								max(if(concept_id=1282 , date(obs_datetime), "")) as date_given,
								obs_group_id
							from (
										 select o.person_id, e.encounter_datetime, e.creator, e.date_created, o.concept_id, o.value_coded, o.value_numeric, date(o.value_datetime) date_given, o.obs_group_id, o.encounter_id, et.uuid, et.name, o.obs_datetime
										 from obs o
											 inner join encounter e on e.encounter_id=o.encounter_id
											 inner join person p on p.person_id=o.person_id and p.voided=0
											 inner join
											 (
												 select encounter_type_id, uuid, name from encounter_type where
													 uuid = '82169b8d-c945-4c41-be62-433dfd9d6c86'
											 ) et on et.encounter_type_id=e.encounter_type
										 where concept_id in(1282,1418) and o.voided=0
									 ) t
							group by obs_group_id
							having vaccine != ""
						 )
						 union
						 (
							 select
								 person_id as patient_id,
								 date(encounter_datetime) as visit_date,
								 creator,
								 date(date_created) as date_created,
								 encounter_id,
								 name as encounter_type,
								 max(if(concept_id=984 , (case when value_coded=886 then "BCG" when value_coded=783 then "OPV" when value_coded=1422 then "IPV"
																					when value_coded=781 then "DPT" when value_coded=162342 then "PCV" when value_coded=83531 then "ROTA"
																					when value_coded=162586 then "measles_rubella"  when value_coded=5864 then "yellow_fever" when value_coded=36 then "measles" when value_coded=84879 then "TETANUS TOXOID"  end), "")) as vaccine,
								 max(if(concept_id=1418, value_numeric, "")) as sequence,
								 max(if(concept_id=1410, date_given, "")) as date_given,
								 obs_group_id
							 from (
											select o.person_id, e.encounter_datetime, e.creator, e.date_created, o.concept_id, o.value_coded, o.value_numeric, date(o.value_datetime) date_given, o.obs_group_id, o.encounter_id, et.uuid, et.name
											from obs o
												inner join encounter e on e.encounter_id=o.encounter_id
												inner join person p on p.person_id=o.person_id and p.voided=0
												inner join
												(
													select encounter_type_id, uuid, name from encounter_type where
														uuid = '82169b8d-c945-4c41-be62-433dfd9d6c86'
												) et on et.encounter_type_id=e.encounter_type
											where concept_id in(984,1418,1410) and o.voided=0
										) t
							 group by obs_group_id
							 having vaccine != ""
						 )
           ) y
				left join obs o on y.encounter_id = o.encounter_id and o.concept_id=162585 and o.voided=0

      group by patient_id;

	SELECT "Completed processing hei_immunization data ", CONCAT("Time: ", NOW());
	END$$

		-- ------------- populate etl_tb_enrollment-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_tb_enrollment$$
CREATE PROCEDURE sp_populate_etl_tb_enrollment()
BEGIN
SELECT "Processing TB Enrollments ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_tb_enrollment(
patient_id,
uuid,
provider,
visit_id,
visit_date,
location_id,
encounter_id,
date_treatment_started,
district,
-- district_registration_number,
referred_by,
referral_date,
date_transferred_in,
facility_transferred_from,
district_transferred_from,
date_first_enrolled_in_tb_care,
weight,
height,
treatment_supporter,
relation_to_patient,
treatment_supporter_address,
treatment_supporter_phone_contact,
disease_classification,
patient_classification,
pulmonary_smear_result,
has_extra_pulmonary_pleurial_effusion,
has_extra_pulmonary_milliary,
has_extra_pulmonary_lymph_node,
has_extra_pulmonary_menengitis,
has_extra_pulmonary_skeleton,
has_extra_pulmonary_abdominal
-- has_extra_pulmonary_other,
-- treatment_outcome,
-- treatment_outcome_date
)
select
e.patient_id,
e.uuid,
e.creator,
e.visit_id,
e.encounter_datetime,
e.location_id,
e.encounter_id,
max(if(o.concept_id=1113,o.value_datetime,null)) as date_treatment_started,
max(if(o.concept_id=161564,trim(o.value_text),null)) as district,
-- max(if(o.concept_id=5085,o.value_numeric,null)) as district_registration_number,
max(if(o.concept_id=160540,o.value_coded,null)) as referred_by,
max(if(o.concept_id=161561,o.value_datetime,null)) as referral_date,
max(if(o.concept_id=160534,o.value_datetime,null)) as date_transferred_in,
max(if(o.concept_id=160535,left(trim(o.value_text),100),null)) as facility_transferred_from,
max(if(o.concept_id=161551,left(trim(o.value_text),100),null)) as district_transferred_from,
max(if(o.concept_id=161552,o.value_datetime,null)) as date_first_enrolled_in_tb_care,
max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
max(if(o.concept_id=5090,o.value_numeric,null)) as height,
max(if(o.concept_id=160638,left(trim(o.value_text),100),null)) as treatment_supporter,
max(if(o.concept_id=160640,o.value_coded,null)) as relation_to_patient,
max(if(o.concept_id=160641,left(trim(o.value_text),100),null)) as treatment_supporter_address,
max(if(o.concept_id=160642,left(trim(o.value_text),100),null)) as treatment_supporter_phone_contact,
max(if(o.concept_id=160040,o.value_coded,null)) as disease_classification,
max(if(o.concept_id=159871,o.value_coded,null)) as patient_classification,
max(if(o.concept_id=159982,o.value_coded,null)) as pulmonary_smear_result,
max(if(o.concept_id=161356 and o.value_coded=130059,o.value_coded,null)) as has_extra_pulmonary_pleurial_effusion,
max(if(o.concept_id=161356 and o.value_coded=115753,o.value_coded,null)) as has_extra_pulmonary_milliary,
max(if(o.concept_id=161356 and o.value_coded=111953,o.value_coded,null)) as has_extra_pulmonary_lymph_node,
max(if(o.concept_id=161356 and o.value_coded=111967,o.value_coded,null)) as has_extra_pulmonary_menengitis,
max(if(o.concept_id=161356 and o.value_coded=112116,o.value_coded,null)) as has_extra_pulmonary_skeleton,
max(if(o.concept_id=161356 and o.value_coded=1350,o.value_coded,null)) as has_extra_pulmonary_abdominal
-- max(if(o.concept_id=161356,o.value_coded,null)) as has_extra_pulmonary_other
-- max(if(o.concept_id=159786,o.value_coded,null)) as treatment_outcome,
-- max(if(o.concept_id=159787,o.value_coded,null)) as treatment_outcome_date

from encounter e
	inner join person p on p.person_id=e.patient_id and p.voided=0
	inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
and o.concept_id in(160540,161561,160534,160535,161551,161552,5089,5090,160638,160640,160641,160642,160040,159871,159982,161356)
inner join
(
	select encounter_type_id, uuid, name from encounter_type where
	uuid in('9d8498a4-372d-4dc4-a809-513a2434621e')
) et on et.encounter_type_id=e.encounter_type
where e.voided=0
group by e.encounter_id;
SELECT "Completed processing TB Enrollments ", CONCAT("Time: ", NOW());
END$$


-- ------------- populate etl_tb_follow_up_visit-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_tb_follow_up_visit$$
CREATE PROCEDURE sp_populate_etl_tb_follow_up_visit()
BEGIN
SELECT "Processing TB Followup visits ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_tb_follow_up_visit(
patient_id,
uuid,
provider,
visit_id ,
visit_date ,
location_id,
encounter_id,
spatum_test,
spatum_result,
result_serial_number,
quantity ,
date_test_done,
bacterial_colonie_growth,
number_of_colonies,
resistant_s,
resistant_r,
resistant_inh,
resistant_e,
sensitive_s,
sensitive_r,
sensitive_inh,
sensitive_e,
test_date,
hiv_status,
next_appointment_date
)
select
e.patient_id,
e.uuid,
e.creator,
e.visit_id,
e.encounter_datetime,
e.location_id,
e.encounter_id,
max(if(o.concept_id=159961,o.value_coded,null)) as spatum_test,
max(if(o.concept_id=307,o.value_coded,null)) as spatum_result,
max(if(o.concept_id=159968,o.value_numeric,null)) as result_serial_number,
max(if(o.concept_id=160023,o.value_numeric,null)) as quantity,
max(if(o.concept_id=159964,o.value_datetime,null)) as date_test_done,
max(if(o.concept_id=159982,o.value_coded,null)) as bacterial_colonie_growth,
max(if(o.concept_id=159952,o.value_numeric,null)) as number_of_colonies,
max(if(o.concept_id=159956 and o.value_coded=84360,o.value_coded,null)) as resistant_s,
max(if(o.concept_id=159956 and o.value_coded=767,o.value_coded,null)) as resistant_r,
max(if(o.concept_id=159956 and o.value_coded=78280,o.value_coded,null)) as resistant_inh,
max(if(o.concept_id=159956 and o.value_coded=75948,o.value_coded,null)) as resistant_e,
max(if(o.concept_id=159958 and o.value_coded=84360,o.value_coded,null)) as sensitive_s,
max(if(o.concept_id=159958 and o.value_coded=767,o.value_coded,null)) as sensitive_r,
max(if(o.concept_id=159958 and o.value_coded=78280,o.value_coded,null)) as sensitive_inh,
max(if(o.concept_id=159958 and o.value_coded=75948,o.value_coded,null)) as sensitive_e,
max(if(o.concept_id=159964,o.value_datetime,null)) as test_date,
max(if(o.concept_id=1169,o.value_coded,null)) as hiv_status,
max(if(o.concept_id=5096,o.value_datetime,null)) as next_appointment_date
from encounter e
	inner join person p on p.person_id=e.patient_id and p.voided=0
	inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
and o.concept_id in(159961,307,159968,160023,159964,159982,159952,159956,159958,159964,1169,5096)
inner join
(
	select encounter_type_id, uuid, name from encounter_type where
	uuid in('fbf0bfce-e9f4-45bb-935a-59195d8a0e35')
) et on et.encounter_type_id=e.encounter_type
where e.voided=0
group by e.encounter_id;
SELECT "Completed processing TB Followup visits ", CONCAT("Time: ", NOW());
END$$


-- ------------- populate etl_tb_screening-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_tb_screening$$
CREATE PROCEDURE sp_populate_etl_tb_screening()
BEGIN
SELECT "Processing TB Screening data ", CONCAT("Time: ", NOW());

insert into kenyaemr_etl.etl_tb_screening(
patient_id,
uuid,
provider,
visit_id,
visit_date,
encounter_id,
location_id,
resulting_tb_status ,
tb_treatment_start_date,
notes
)
select
e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_datetime, e.encounter_id, e.location_id,
max(case o.concept_id when 1659 then o.value_coded else null end) as resulting_tb_status,
max(case o.concept_id when 1113 then date(o.value_datetime)  else NULL end) as tb_treatment_start_date,
"" as notes -- max(case o.concept_id when 160632 then value_text else "" end) as notes
from encounter e
	inner join person p on p.person_id=e.patient_id and p.voided=0
	inner join form f on f.form_id=e.form_id and f.uuid in ("22c68f86-bbf0-49ba-b2d1-23fa7ccf0259", "59ed8e62-7f1f-40ae-a2e3-eabe350277ce")
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (1659, 1113, 160632) and o.voided=0
where e.voided=0
group by e.encounter_id;

SELECT "Completed processing TB Screening data ", CONCAT("Time: ", NOW());
END$$

-- ------------------------------------------- drug event ---------------------------

DROP PROCEDURE IF EXISTS sp_drug_event$$
CREATE PROCEDURE sp_drug_event()
BEGIN
SELECT "Processing Drug Event Data", CONCAT("Time: ", NOW());
	INSERT INTO kenyaemr_etl.etl_drug_event(
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
		reason_discontinued,
		reason_discontinued_other
	)
		select
			e.uuid,
			e.patient_id,
			e.encounter_datetime,
			e.encounter_datetime,
			e.creator,
			e.encounter_id,
			max(if(o.concept_id=1255,'HIV',if(o.concept_id=1268, 'TB', null))) as program,
			max(if(o.concept_id=1193,(
				case o.value_coded
					-- HIV
				when 162565 then "3TC/NVP/TDF"
				when 164505 then "TDF/3TC/EFV"
				when 1652 then "AZT/3TC/NVP"
				when 160124 then "AZT/3TC/EFV"
				when 792 then "D4T/3TC/NVP"
				when 160104 then "D4T/3TC/EFV"
				when 164971 then "TDF/3TC/AZT"
				when 164968 then "AZT/3TC/DTG"
				when 164969 then "TDF/3TC/DTG"
				when 164970 then "ABC/3TC/DTG"
				when 162561 then "AZT/3TC/LPV/r"
				when 164511 then "AZT/3TC/ATV/r"
				when 162201 then "TDF/3TC/LPV/r"
				when 164512 then "TDF/3TC/ATV/r"
				when 162560 then "D4T/3TC/LPV/r"
				when 164972 then "AZT/TDF/3TC/LPV/r"
				when 164973 then "ETR/RAL/DRV/RTV"
				when 164974 then "ETR/TDF/3TC/LPV/r"
				when 165357 then "ABC+3TC+ATV/r"
				when 162200 then "ABC/3TC/LPV/r"
				when 162199 then "ABC/3TC/NVP"
				when 162563 then "ABC/3TC/EFV"
				when 817 then "AZT/3TC/ABC"
				when 164975 then "D4T/3TC/ABC"
				when 162562 then "TDF/ABC/LPV/r"
				when 162559 then "ABC/DDI/LPV/r"
				when 164976 then "ABC/TDF/3TC/LPV/r"
				when 165375 then "RAL/3TC/DRV/RTV"
				when 165376 then "RAL/3TC/DRV/RTV/AZT"
				when 165377 then "RAL/3TC/DRV/RTV/ABC"
				when 165378 then "ETV/3TC/DRV/RTV"
				when 165379 then "RAL/3TC/DRV/RTV/TDF"
				when 165369 then "TDF/3TC/DTG/DRV/r"
				when 165370 then "TDF/3TC/RAL/DRV/r"
				when 165371 then "TDF/3TC/DTG/EFV/DRV/r"
				when 165372 then "ABC/3TC/RAL"
				when 165373 then "AZT/3TC/RAL/DRV/r"
				when 165374 then "ABC/3TC/RAL/DRV/r"
         -- TB
				when 1675 then "RHZE"
				when 768 then "RHZ"
				when 1674 then "SRHZE"
				when 164978 then "RfbHZE"
				when 164979 then "RfbHZ"
				when 164980 then "SRfbHZE"
				when 84360 then "S (1 gm vial)"
				when 75948 then "E"
				when 1194 then "RH"
				when 159851 then "RHE"
				when 1108 then "EH"
				else ""
				end ),null)) as regimen,
			max(if(o.concept_id=1193,(
				case o.value_coded
					-- HIV
				when 162565 then "3TC+NVP+TDF"
				when 164505 then "TDF+3TC+EFV"
				when 1652 then "AZT+3TC+NVP"
				when 160124 then "AZT+3TC+EFV"
				when 792 then "D4T+3TC+NVP"
				when 160104 then "D4T+3TC+EFV"
				when 164971 then "TDF+3TC+AZT"
				when 164968 then "AZT+3TC+DTG"
				when 164969 then "TDF+3TC+DTG"
				when 164970 then "ABC+3TC+DTG"
				when 162561 then "AZT+3TC+LPV/r"
				when 164511 then "AZT+3TC+ATV/r"
				when 162201 then "TDF+3TC+LPV/r"
				when 164512 then "TDF+3TC+ATV/r"
				when 162560 then "D4T+3TC+LPV/r"
				when 164972 then "AZT+TDF+3TC+LPV/r"
				when 164973 then "ETR+RAL+DRV+RTV"
				when 164974 then "ETR+TDF+3TC+LPV/r"
				when 165357 then "ABC+3TC+ATV/r"
				when 162200 then "ABC+3TC+LPV/r"
				when 162199 then "ABC+3TC+NVP"
				when 162563 then "ABC+3TC+EFV"
				when 817 then "AZT+3TC+ABC"
				when 164975 then "D4T+3TC+ABC"
				when 162562 then "TDF+ABC+LPV/r"
				when 162559 then "ABC+DDI+LPV/r"
				when 164976 then "ABC+TDF+3TC+LPV/r"
				when 165375 then "RAL+3TC+DRV+RTV"
				when 165376 then "RAL+3TC+DRV+RTV+AZT"
				when 165377 then "RAL+3TC+DRV+RTV+ABC"
				when 165378 then "ETV+3TC+DRV+RTV"
				when 165379 then "RAL+3TC+DRV+RTV+TDF"
				when 165369 then "TDF+3TC+DTG+DRV/r"
				when 165370 then "TDF+3TC+RAL+DRV/r"
				when 165371 then "TDF+3TC+DTG+EFV+DRV/r"
				when 165372 then "ABC+3TC+RAL"
				when 165373 then "AZT+3TC+RAL+DRV/r"
				when 165374 then "ABC+3TC+RAL+DRV/r"
					-- TB
				when 1675 then "RHZE"
				when 768 then "RHZ"
				when 1674 then "SRHZE"
				when 164978 then "RfbHZE"
				when 164979 then "RfbHZ"
				when 164980 then "SRfbHZE"
				when 84360 then "S (1 gm vial)"
				when 75948 then "E"
				when 1194 then "RH"
				when 159851 then "RHE"
				when 1108 then "EH"
				else ""
				end ),null)) as regimen_name,
			max(if(o.concept_id=1193,(
				case o.value_coded
				-- adult first line
				when 162565 then "First line"
				when 164505 then "First line"
				when 1652 then "First line"
				when 160124 then "First line"
				when 792 then "First line"
				when 160104 then "First line"
				when 164971 then "First line"
				when 164968 then "First line"
				when 164969 then "First line"
				when 164970 then "First line"
				when 162561 then "First line"
				when 164511 then "First line"
				when 164512 then "First line"
				when 162201 then "First line"
				-- adult second line
				when 162561 then "Second line"
				when 164511 then "Second line"
				when 162201 then "Second line"
				when 164512 then "Second line"
				when 162560 then "Second line"
				when 164972 then "Second line"
				when 164973 then "Second line"
				when 164974 then "Second line"
				when 165357 then "Second line"
				when 164968 then "Second line"
				when 164969 then "Second line"
				when 164970 then "Second line"
				-- adult third line
				when 165375 then "Third line"
				when 165376 then "Third line"
				when 165379 then "Third line"
				when 165378 then "Third line"
				when 165369 then "Third line"
				when 165370 then "Third line"
				when 165371 then "Third line"
				-- child 1st line
				when 162200 then "First line"
				when 162199 then "First line"
				when 162563 then "First line"
				when 817 then "First line"
				when 164975 then "First line"
				when 162562 then "First line"
				when 162559 then "First line"
				when 164976 then "First line"
				when 165372 then "First line"
				-- child second line
				when 162561 then "Second line"
				when 164511 then "Second line"
				when 162200 then "Second line"
				when 165357 then "Second line"
				when 165373 then "Second line"
				when 165374 then "Second line"
				-- child third line
				when 165375 then "Third line"
				when 165376 then "Third line"
				when 165377 then "Third line"
				when 165378 then "Third line"
				when 165373 then "Third line"
				when 165374 then "Third line"
				-- tb
				when 1675 then "Adult intensive"
				when 768 then "Adult intensive"
				when 1674 then "Adult intensive"
				when 164978 then "Adult intensive"
				when 164979 then "Adult intensive"
				when 164980 then "Adult intensive"
				when 84360 then "Adult intensive"
				-- child intensive
				when 75948 then "Child intensive"
				when 1194 then "Child intensive"
				-- adult continuation
				when 159851 then "Adult continuation"
				when 1108 then "Adult continuation"
				else ""
				end ),null)) as regimen_line,
			max(if(o.concept_id=1191,(case o.value_datetime when NULL then 0 else 1 end),null)) as discontinued,
			null as regimen_discontinued,
			max(if(o.concept_id=1191,o.value_datetime,null)) as date_discontinued,
			max(if(o.concept_id=1252,o.value_coded,null)) as reason_discontinued,
			max(if(o.concept_id=5622,o.value_text,null)) as reason_discontinued_other

		from encounter e
			inner join person p on p.person_id=e.patient_id and p.voided=0
			inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
													and o.concept_id in(1193,1252,5622,1191,1255,1268)
			inner join
			(
				select encounter_type, uuid,name from form where
					uuid in('da687480-e197-11e8-9f32-f2801f1b9fd1') -- regimen editor form
			) f on f.encounter_type=e.encounter_type
			where e.voided=0
		group by e.encounter_id;

SELECT "Completed processing Drug Event Data", CONCAT("Time: ", NOW());
END$$



-- ------------------------------------ populate hts test table ----------------------------------------


DROP PROCEDURE IF EXISTS sp_populate_hts_test$$
CREATE PROCEDURE sp_populate_hts_test()
BEGIN
SELECT "Processing hts tests";
INSERT INTO kenyaemr_etl.etl_hts_test (
patient_id,
visit_id,
encounter_id,
encounter_uuid,
encounter_location,
creator,
date_created,
visit_date,
test_type,
population_type,
key_population_type,
ever_tested_for_hiv,
months_since_last_test,
patient_disabled,
disability_type,
patient_consented,
client_tested_as,
test_strategy,
hts_entry_point,
test_1_kit_name,
test_1_kit_lot_no,
test_1_kit_expiry,
test_1_result,
test_2_kit_name,
test_2_kit_lot_no,
test_2_kit_expiry,
test_2_result,
final_test_result,
patient_given_result,
couple_discordant,
tb_screening,
patient_had_hiv_self_test ,
remarks,
voided
)
select
e.patient_id,
e.visit_id,
e.encounter_id,
e.uuid,
e.location_id,
e.creator,
e.date_created,
e.encounter_datetime as visit_date,
max(if((o.concept_id=162084 and o.value_coded=162082 and f.uuid = "402dc5d7-46da-42d4-b2be-f43ea4ad87b0") or (f.uuid = "b08471f6-0892-4bf7-ab2b-bf79797b8ea4"), 2, 1)) as test_type , -- 2 for confirmation, 1 for initial
max(if(o.concept_id=164930,(case o.value_coded when 164928 then "General Population" when 164929 then "Key Population" else "" end),null)) as population_type,
max(if(o.concept_id=160581,(case o.value_coded when 105 then "People who inject drugs" when 160578 then "Men who have sex with men" when 160579 then "Female sex worker" else "" end),null)) as key_population_type,
max(if(o.concept_id=164401,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as ever_tested_for_hiv,
max(if(o.concept_id=159813,o.value_numeric,null)) as months_since_last_test,
max(if(o.concept_id=164951,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_disabled,
max(if(o.concept_id=162558,(case o.value_coded when 120291 then "Deaf" when 147215 then "Blind" when 151342 then "Mentally Challenged" when 164538 then "Physically Challenged" when 5622 then "Other" else "" end),null)) as disability_type,
max(if(o.concept_id=1710,(case o.value_coded when 1 then "Yes" when 0 then "No" else "" end),null)) as patient_consented,
max(if(o.concept_id=164959,(case o.value_coded when 164957 then "Individual" when 164958 then "Couple" else "" end),null)) as client_tested_as,
max(if(o.concept_id=164956,(
  case o.value_coded
  when 164163 then "Provider Initiated Testing(PITC)"
  when 164953 then "Non Provider Initiated Testing"
  when 164954 then "Integrated VCT Center"
  when 164955 then "Stand Alone VCT Center"
  when 159938 then "Home Based Testing"
  when 159939 then "Mobile Outreach HTS"
  when 5622 then "Other"
  else ""
  end ),null)) as test_strategy,
   max(if(o.concept_id=160540,(
             case o.value_coded
             when 5485 then "In Patient Department(IPD)"
             when 160542 then "Out Patient Department(OPD)"
             when 162181 then "Peadiatric Clinic"
             when 160552 then "Nutrition Clinic"
             when 160538 then "PMTCT"
             when 160541 then "TB"
             when 162050 then "CCC"
             when 159940 then "VCT"
             when 159938 then "Home Based Testing"
             when 159939 then "Mobile Outreach"
             when 5622 then "Other"
             else ""
             end ),null)) as hts_entry_point,
max(if(t.test_1_result is not null, t.kit_name, null)) as test_1_kit_name,
max(if(t.test_1_result is not null, t.lot_no, null)) as test_1_kit_lot_no,
max(if(t.test_1_result is not null, t.expiry_date, null)) as test_1_kit_expiry,
max(if(t.test_1_result is not null, t.test_1_result, null)) as test_1_result,
max(if(t.test_2_result is not null, t.kit_name, null)) as test_2_kit_name,
max(if(t.test_2_result is not null, t.lot_no, null)) as test_2_kit_lot_no,
max(if(t.test_2_result is not null, t.expiry_date, null)) as test_2_kit_expiry,
max(if(t.test_2_result is not null, t.test_2_result, null)) as test_2_result,
max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" else "" end),null)) as final_test_result,
max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_given_result,
max(if(o.concept_id=6096,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as couple_discordant,
max(if(o.concept_id=1659,(case o.value_coded when 1660 then "No TB signs" when 142177 then "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "Not done" when 1111 then "On TB Treatment"  else "" end),null)) as tb_screening,
max(if(o.concept_id=164952,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_had_hiv_self_test,
max(if(o.concept_id=163042,trim(o.value_text),null)) as remarks,
e.voided
from encounter e
	inner join person p on p.person_id=e.patient_id and p.voided=0
	inner join form f on f.form_id=e.form_id and f.uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (162084, 164930, 160581, 164401, 164951, 162558, 1710, 164959, 164956,
                                                                                 160540,159427, 164848, 6096, 1659, 164952, 163042, 159813)
inner join (
             select
               o.person_id,
               o.encounter_id,
               o.obs_group_id,
               max(if(o.concept_id=1040, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 163611 then "Invalid"  else "" end),null)) as test_1_result ,
               max(if(o.concept_id=1326, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_2_result ,
               max(if(o.concept_id=164962, (case o.value_coded when 164960 then "Determine" when 164961 then "First Response" else "" end),null)) as kit_name ,
               max(if(o.concept_id=164964,trim(o.value_text),null)) as lot_no,
               max(if(o.concept_id=162502,date(o.value_datetime),null)) as expiry_date
             from obs o
             inner join encounter e on e.encounter_id = o.encounter_id
             inner join form f on f.form_id=e.form_id and f.uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
             where o.concept_id in (1040, 1326, 164962, 164964, 162502) and o.voided=0
             group by e.encounter_id, o.obs_group_id
           ) t on e.encounter_id = t.encounter_id
where e.voided=0
group by e.encounter_id;
SELECT "Completed processing hts tests";
END$$


-- ------------------------------------ POPULATE HTS LINKAGES AND REFERRALS -------------------------------

DROP PROCEDURE IF EXISTS sp_populate_hts_linkage_and_referral$$
CREATE PROCEDURE sp_populate_hts_linkage_and_referral()
BEGIN
SELECT "Processing hts linkages, referrals and tracing";
INSERT INTO kenyaemr_etl.etl_hts_referral_and_linkage (
  patient_id,
  visit_id,
  encounter_id,
  encounter_uuid,
  encounter_location,
  creator,
  date_created,
  visit_date,
  tracing_type,
  tracing_status,
  facility_linked_to,
	enrollment_date,
	art_start_date,
  ccc_number,
  provider_handed_to,
  voided
)
  select
    e.patient_id,
    e.visit_id,
    e.encounter_id,
    e.uuid,
    e.location_id,
    e.creator,
    e.date_created,
    e.encounter_datetime as visit_date,
    max(if(o.concept_id=164966,(case o.value_coded when 1650 then "Phone" when 164965 then "Physical" else "" end),null)) as tracing_type ,
    max(if(o.concept_id=159811,(case o.value_coded when 1065 then "Contacted and linked" when 1066 then "Contacted but not linked" else "" end),null)) as tracing_status,
    max(if(o.concept_id=162724,trim(o.value_text),null)) as facility_linked_to,
		max(if(o.concept_id=160555,o.value_datetime,null)) as enrollment_date,
		max(if(o.concept_id=159599,o.value_datetime,null)) as art_start_date,
    max(if(o.concept_id=162053,o.value_numeric,null)) as ccc_number,
    max(if(o.concept_id=1473,trim(o.value_text),null)) as provider_handed_to,
    e.voided
  from encounter e
		inner join person p on p.person_id=e.patient_id and p.voided=0
		inner join form f on f.form_id = e.form_id and f.uuid = "050a7f12-5c52-4cad-8834-863695af335d"
  left outer join obs o on o.encounter_id = e.encounter_id and o.concept_id in (164966, 159811, 162724, 160555, 159599, 162053, 1473) and o.voided=0
  where e.voided=0
  group by e.encounter_id;
  SELECT "Completed processing hts linkages";

END$$


-- ------------------------------------ update hts referral table ---------------------------------

DROP PROCEDURE IF EXISTS sp_populate_hts_referral$$
CREATE PROCEDURE sp_populate_hts_referral()
  BEGIN
    SELECT "Processing hts referrals";
    INSERT INTO kenyaemr_etl.etl_hts_referral (
      patient_id,
      visit_id,
      encounter_id,
      encounter_uuid,
      encounter_location,
      creator,
      date_created,
      visit_date,
      facility_referred_to,
      date_to_enrol,
      remarks,
      voided
    )
      select
        e.patient_id,
        e.visit_id,
        e.encounter_id,
        e.uuid,
        e.location_id,
        e.creator,
        e.date_created,
        e.encounter_datetime as visit_date,
        max(if(o.concept_id=161550,o.value_text,null)) as facility_referred_to ,
        max(if(o.concept_id=161561,o.value_datetime,null)) as date_to_be_enrolled,
        max(if(o.concept_id=163042,o.value_text,null)) as remarks,
        e.voided
      from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join form f on f.form_id = e.form_id and f.uuid = "9284828e-ce55-11e9-a32f-2a2ae2dbcce4"
        left outer join obs o on o.encounter_id = e.encounter_id and o.concept_id in (161550, 161561, 163042) and o.voided=0
        where e.voided=0
      group by e.encounter_id;
    SELECT "Completed processing hts referrals";

    END$$

-- ----------------------------------- UPDATE DASHBOARD TABLE ---------------------


DROP PROCEDURE IF EXISTS sp_update_dashboard_table$$
CREATE PROCEDURE sp_update_dashboard_table()
BEGIN

DECLARE startDate DATE;
DECLARE endDate DATE;
DECLARE reportingPeriod VARCHAR(20);

SET startDate = DATE_FORMAT(NOW() - INTERVAL 1 MONTH, '%Y-%m-01');
SET endDate = DATE_FORMAT(LAST_DAY(NOW() - INTERVAL 1 MONTH), '%Y-%m-%d');
SET reportingPeriod = DATE_FORMAT(NOW() - INTERVAL 1 MONTH, '%Y-%M');

-- CURRENT IN CARE
DROP TABLE IF EXISTS kenyaemr_etl.etl_current_in_care;

CREATE TABLE kenyaemr_etl.etl_current_in_care AS
select fup.visit_date,fup.patient_id,p.dob,p.Gender, min(e.visit_date) as enroll_date,
	greatest(max(fup.visit_date), ifnull(max(d.visit_date),'0000-00-00')) as latest_vis_date,
	greatest(mid(max(concat(fup.visit_date,fup.next_appointment_date)),11), ifnull(max(d.visit_date),'0000-00-00')) as latest_tca,
p.unique_patient_no,
max(d.visit_date) as date_discontinued,
d.patient_id as disc_patient,
de.patient_id as started_on_drugs
from kenyaemr_etl.etl_patient_hiv_followup fup
join kenyaemr_etl.etl_patient_demographics p on p.patient_id=fup.patient_id
join kenyaemr_etl.etl_hiv_enrollment e on fup.patient_id=e.patient_id
left outer join kenyaemr_etl.etl_drug_event de on e.patient_id = de.patient_id and date(date_started) <= endDate
left outer JOIN
(select patient_id, coalesce(date(effective_discontinuation_date),visit_date) visit_date from kenyaemr_etl.etl_patient_program_discontinuation
where date(visit_date) <= endDate and program_name='HIV'
group by patient_id
) d on d.patient_id = fup.patient_id
where fup.visit_date <= endDate
group by patient_id
having (
(date(latest_tca) > endDate and (date(latest_tca) >= date(date_discontinued) or disc_patient is null ) and (date(latest_vis_date) >= date(date_discontinued) or disc_patient is null)) or
(((date(latest_tca) between startDate and endDate) and ((date(latest_vis_date) >= date(latest_tca)) or date(latest_tca) > curdate())) and (date(latest_tca) >= date(date_discontinued) or disc_patient is null )) )
;

-- ADD INDICES
ALTER TABLE kenyaemr_etl.etl_current_in_care ADD INDEX(enroll_date);
ALTER TABLE kenyaemr_etl.etl_current_in_care ADD INDEX(latest_vis_date);
ALTER TABLE kenyaemr_etl.etl_current_in_care ADD INDEX(latest_tca);
ALTER TABLE kenyaemr_etl.etl_current_in_care ADD INDEX(started_on_drugs);


DROP TABLE IF EXISTS kenyaemr_etl.etl_last_month_newly_enrolled_in_care;
CREATE TABLE kenyaemr_etl.etl_last_month_newly_enrolled_in_care (
patient_id INT(11) not null
);

INSERT INTO kenyaemr_etl.etl_last_month_newly_enrolled_in_care
select distinct e.patient_id
from kenyaemr_etl.etl_hiv_enrollment e
join kenyaemr_etl.etl_patient_demographics p on p.patient_id=e.patient_id
where  e.entry_point <> 160563  and transfer_in_date is null
and date(e.visit_date) between startDate and endDate and (e.patient_type not in (160563, 164931, 159833) or e.patient_type is null or e.patient_type='');


DROP TABLE IF EXISTS kenyaemr_etl.etl_last_month_newly_on_art;
CREATE TABLE kenyaemr_etl.etl_last_month_newly_on_art (
patient_id INT(11) not null
);

INSERT INTO kenyaemr_etl.etl_last_month_newly_on_art
select distinct net.patient_id
from (
select e.patient_id,e.date_started,
e.gender,
e.dob,
d.visit_date as dis_date,
if(d.visit_date is not null, 1, 0) as TOut,
e.regimen, e.regimen_line, e.alternative_regimen,
mid(max(concat(fup.visit_date,fup.next_appointment_date)),11) as latest_tca,
max(if(enr.date_started_art_at_transferring_facility is not null and enr.facility_transferred_from is not null, 1, 0)) as TI_on_art,
max(if(enr.transfer_in_date is not null, 1, 0)) as TIn,
max(fup.visit_date) as latest_vis_date
from (select e.patient_id,p.dob,p.Gender,min(e.date_started) as date_started,
mid(min(concat(e.date_started,e.regimen_name)),11) as regimen,
mid(min(concat(e.date_started,e.regimen_line)),11) as regimen_line,
max(if(discontinued,1,0))as alternative_regimen
from kenyaemr_etl.etl_drug_event e
join kenyaemr_etl.etl_patient_demographics p on p.patient_id=e.patient_id
group by e.patient_id) e
left outer join kenyaemr_etl.etl_patient_program_discontinuation d on d.patient_id=e.patient_id
left outer join kenyaemr_etl.etl_hiv_enrollment enr on enr.patient_id=e.patient_id
left outer join kenyaemr_etl.etl_patient_hiv_followup fup on fup.patient_id=e.patient_id
where  date(e.date_started) between startDate and endDate
group by e.patient_id
having TI_on_art=0
)net;

-- populate people booked today
TRUNCATE TABLE kenyaemr_etl.etl_patients_booked_today;
ALTER TABLE kenyaemr_etl.etl_patients_booked_today AUTO_INCREMENT = 1;

INSERT INTO kenyaemr_etl.etl_patients_booked_today(patient_id, last_visit_date)
SELECT patient_id, max(visit_date)
FROM kenyaemr_etl.etl_patient_hiv_followup
WHERE date(next_appointment_date) = CURDATE()
GROUP BY patient_id;

SELECT "Completed processing dashboard indicators", CONCAT("Time: ", NOW());

END$$


-- ------------- populate etl_ipt_screening-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_ipt_screening$$
CREATE PROCEDURE sp_populate_etl_ipt_screening()
BEGIN
SELECT "Processing IPT screening forms", CONCAT("Time: ", NOW());

insert into kenyaemr_etl.etl_ipt_screening(
patient_id,
uuid,
provider,
visit_id,
visit_date,
encounter_id,
location_id,
ipt_started
)
select
e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_datetime, e.encounter_id, e.location_id,
max(o.value_coded) as ipt_started
from encounter e
	inner join person p on p.person_id=e.patient_id and p.voided=0
	inner join form f on f.form_id=e.form_id and f.uuid in ("22c68f86-bbf0-49ba-b2d1-23fa7ccf0259", "59ed8e62-7f1f-40ae-a2e3-eabe350277ce")
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id=1265 and o.voided=0
where e.voided=0
group by e.encounter_id;

SELECT "Completed processing IPT screening forms", CONCAT("Time: ", NOW());
END$$



-- ------------- populate etl_ipt_followup-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_ipt_follow_up$$
CREATE PROCEDURE sp_populate_etl_ipt_follow_up()
BEGIN
SELECT "Processing IPT followup forms", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_ipt_follow_up(
patient_id,
uuid,
provider,
visit_id,
visit_date,
encounter_id,
location_id,
ipt_due_date,
date_collected_ipt,
hepatotoxity,
peripheral_neuropathy,
rash,
adherence,
action_taken,
voided
)
select
e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_datetime, e.encounter_id, e.location_id,
max(if(o.concept_id = 164073, o.value_datetime, null )) as ipt_due_date,
max(if(o.concept_id = 164074, o.value_datetime, null )) as date_collected_ipt,
max(if(o.concept_id = 159098, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as hepatotoxity,
max(if(o.concept_id = 118983, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as peripheral_neuropathy,
max(if(o.concept_id = 512, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as rash,
max(if(o.concept_id = 164075, (case o.value_coded when 159407 then "Poor" when 159405 then "Good" when 159406 then "Fair" when 164077 then "Very Good" when 164076 then "Excellent" when 1067 then "Unknown" else "" end), "" )) as adherence,
max(if(o.concept_id = 160632, trim(o.value_text), "" )) as action_taken,
e.voided
from encounter e
	inner join person p on p.person_id=e.patient_id and p.voided=0
	inner join
(
select encounter_type_id, uuid, name from encounter_type where uuid in('aadeafbe-a3b1-4c57-bc76-8461b778ebd6')
) et on et.encounter_type_id=e.encounter_type
left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
and o.concept_id in (164073,164074,159098,118983,512,164075,160632)
where e.voided=0
group by e.encounter_id;

SELECT "Completed processing IPT followup forms", CONCAT("Time: ", NOW());
END$$

-- ------------- populate defaulter tracing-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_ccc_defaulter_tracing$$
CREATE PROCEDURE sp_populate_etl_ccc_defaulter_tracing()
BEGIN
SELECT "Processing ccc defaulter tracing form", CONCAT("Time: ", NOW());

insert into kenyaemr_etl.etl_ccc_defaulter_tracing(
uuid,
provider,
patient_id,
visit_id,
visit_date,
location_id,
encounter_id,
tracing_type,
tracing_outcome,
attempt_number,
is_final_trace,
true_status,
cause_of_death,
comments
)
select
e.uuid, e.creator, e.patient_id, e.visit_id, e.encounter_datetime, e.location_id, e.encounter_id,
max(if(o.concept_id = 164966, o.value_coded, null )) as tracing_type,
max(if(o.concept_id = 160721, o.value_coded, null )) as tracing_outcome,
max(if(o.concept_id = 1639, value_numeric, "" )) as attempt_number,
max(if(o.concept_id = 163725, o.value_coded, "" )) as is_final_trace,
max(if(o.concept_id = 160433, o.value_coded, "" )) as true_status,
max(if(o.concept_id = 1599, o.value_coded, "" )) as cause_of_death,
max(if(o.concept_id = 160716, o.value_text, "" )) as comments
from encounter e
	inner join person p on p.person_id=e.patient_id and p.voided=0
	inner join form f on f.form_id=e.form_id and f.uuid in ("a1a62d1e-2def-11e9-b210-d663bd873d93")
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (164966, 160721, 1639, 163725, 160433, 1599, 160716) and o.voided=0
where e.voided=0
group by e.encounter_id;
SELECT "Completed processing CCC defaulter tracing forms", CONCAT("Time: ", NOW());
END$$

-- ------------- populate etl_ART_preparation-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_ART_preparation $$
CREATE PROCEDURE sp_populate_etl_ART_preparation()
  BEGIN
    SELECT "Processing ART Preparation ", CONCAT("Time: ", NOW());
    insert into kenyaemr_etl.etl_ART_preparation(

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
informed_drug_side_effects,
caregiver_committed,
adherance_barriers_identified,
caregiver_location_contacts_known,
ready_to_start_art,
identified_drug_time,
treatment_supporter_engaged,
support_grp_meeting_awareness,
enrolled_in_reminder_system,
other_support_systems

)
    select
   e.uuid,
   e.patient_id,
   e.visit_id,
   e.encounter_datetime,
   e.location_id,
   e.encounter_id,
   e.creator,
   max(if(o.concept_id=1729,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as understands_hiv_art_benefits,
   max(if(o.concept_id=160246,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as screened_negative_substance_abuse,
   max(if(o.concept_id=159891,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as screened_negative_psychiatric_illness,
   max(if(o.concept_id=1048,(case o.value_coded when 1 then "Yes" when 0 then "No" else "" end), "" )) as HIV_status_disclosure,
   max(if(o.concept_id=164425,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as trained_drug_admin,
   max(if(o.concept_id=121764,(case o.value_coded when 1 then "Yes" when 0 then "No" else "" end), "" )) as informed_drug_side_effects,
   max(if(o.concept_id=5619,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as caregiver_committed,
   max(if(o.concept_id=159707,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as adherance_barriers_identified,
   max(if(o.concept_id=163089,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as caregiver_location_contacts_given,
   max(if(o.concept_id=162695,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as ready_to_start_art,
   max(if(o.concept_id=160119,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as identified_drug_time,
   max(if(o.concept_id=164886,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as treatment_supporter_engaged,
   max(if(o.concept_id=163766,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as support_grp_meeting_awareness,
   max(if(o.concept_id=163164,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as enrolled_in_reminder_system,
   max(if(o.concept_id=164360,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as other_support_systems
    from encounter e
			inner join person p on p.person_id=e.patient_id and p.voided=0
			inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
 and o.concept_id in(1729,160246,159891,1048,164425,121764,5619,159707,163089,162695,160119,164886,163766,163164,164360)
   inner join
     (
     select form_id, uuid,name from form where
 uuid in('782a4263-3ac9-4ce8-b316-534571233f12')
     ) f on f.form_id= e.form_id
   left join (
     select
    o.person_id,
    o.encounter_id,
    o.obs_group_id
     from obs o
    inner join encounter e on e.encounter_id = o.encounter_id
    inner join form f on f.form_id=e.form_id and f.uuid in ('782a4263-3ac9-4ce8-b316-534571233f12')
     where o.voided=0
     group by e.encounter_id, o.obs_group_id
     ) t on e.encounter_id = t.encounter_id
     where e.voided=0
    group by e.encounter_id;
    SELECT "Completed processing ART Preparation ", CONCAT("Time: ", NOW());
    END$$

-- ------------- populate etl_enhanced_adherence-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_enhanced_adherence $$
CREATE PROCEDURE sp_populate_etl_enhanced_adherence()
	BEGIN
		SELECT "Processing Enhanced Adherence ", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_enhanced_adherence(
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
		)
			select
				e.uuid,
				e.patient_id,
				e.visit_id,
				e.encounter_datetime,
				e.location_id,
				e.encounter_id,
				e.creator,
				max(if(o.concept_id=1639,o.value_numeric,null)) as session_number,
				max(if(o.concept_id=164891,o.value_datetime,null)) as first_session_date,
				max(if(o.concept_id=162846,o.value_numeric,null)) as pill_count,
				max(if(o.concept_id=1658,(case o.value_coded when 159405 then "Good" when 163794 then "Inadequate" when 159407 then "Poor" else "" end), "" )) as arv_adherence,
				max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as has_vl_results,
				max(if(o.concept_id=163310,(case o.value_coded when 1302 then "Suppressed" when 1066 then "Unsuppresed" else "" end), "" )) as vl_results_suppressed,
				max(if(o.concept_id=164981,trim(o.value_text),null)) as vl_results_feeling,
				max(if(o.concept_id=164982,trim(o.value_text),null)) as cause_of_high_vl,
				max(if(o.concept_id=160632,trim(o.value_text),null)) as way_forward,
				max(if(o.concept_id=164983,trim(o.value_text),null)) as patient_hiv_knowledge,
				max(if(o.concept_id=164984,trim(o.value_text),null)) as patient_drugs_uptake,
				max(if(o.concept_id=164985,trim(o.value_text),null)) as patient_drugs_reminder_tools,
				max(if(o.concept_id=164986,trim(o.value_text),null)) as patient_drugs_uptake_during_travels,
				max(if(o.concept_id=164987,trim(o.value_text),null)) as patient_drugs_side_effects_response,
				max(if(o.concept_id=164988,trim(o.value_text),null)) as patient_drugs_uptake_most_difficult_times,
				max(if(o.concept_id=164989,trim(o.value_text),null)) as patient_drugs_daily_uptake_feeling,
				max(if(o.concept_id=164990,trim(o.value_text),null)) as patient_ambitions,
				max(if(o.concept_id=164991,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as patient_has_people_to_talk,
				max(if(o.concept_id=164992,trim(o.value_text),null)) as patient_enlisting_social_support,
				max(if(o.concept_id=164993,trim(o.value_text),null)) as patient_income_sources,
				max(if(o.concept_id=164994,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as patient_challenges_reaching_clinic,
				max(if(o.concept_id=164995,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as patient_worried_of_accidental_disclosure,
				max(if(o.concept_id=164996,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as patient_treated_differently,
				max(if(o.concept_id=164997,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as stigma_hinders_adherence,
				max(if(o.concept_id=164998,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as patient_tried_faith_healing,
				max(if(o.concept_id=1898,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as patient_adherence_improved,
				max(if(o.concept_id=160110,(case o.value_coded when 1 then "Yes" when 0 then "No" else "" end), "" )) as patient_doses_missed,
				max(if(o.concept_id=163108,trim(o.value_text),null)) as review_and_barriers_to_adherence,
				max(if(o.concept_id=1272,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as other_referrals,
				max(if(o.concept_id=164999,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as appointments_honoured,
				max(if(o.concept_id=165000,trim(o.value_text),null)) as referral_experience,
				max(if(o.concept_id=165001,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as home_visit_benefit,
				max(if(o.concept_id=165002,trim(o.value_text),null)) as adherence_plan,
				max(if(o.concept_id=5096,o.value_datetime,null)) as next_appointment_date

			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
																		and o.concept_id in(1639,164891,162846,1658,164848,163310,164981,164982,160632,164983,164984,164985,164986,164987,164988,164989,164990,164991,164992,164993,164994,164995,164996,164997,164998,1898,160110,163108,1272,164999,165000,165001,165002,5096)
				inner join
				(
					select form_id, uuid,name from form where
						uuid in('c483f10f-d9ee-4b0d-9b8c-c24c1ec24701')
				) f on f.form_id= e.form_id
				left join (
										select
											o.person_id,
											o.encounter_id,
											o.obs_group_id
										from obs o
											inner join encounter e on e.encounter_id = o.encounter_id
											inner join form f on f.form_id=e.form_id and f.uuid in ('c483f10f-d9ee-4b0d-9b8c-c24c1ec24701')
										where o.voided=0
										group by e.encounter_id, o.obs_group_id
									) t on e.encounter_id = t.encounter_id
			where e.voided=0
			group by e.encounter_id;
		SELECT "Completed processing Enhanced Adherence ", CONCAT("Time: ", NOW());
		END$$

-- ------------- populate etl_patient_triage--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_patient_triage$$
CREATE PROCEDURE sp_populate_etl_patient_triage()
	BEGIN
		SELECT "Processing Patient Triage ", CONCAT("Time: ", NOW());
		INSERT INTO kenyaemr_etl.etl_patient_triage(
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
			where e.voided=0
			group by e.patient_id, e.encounter_id, visit_date
		;
		SELECT "Completed processing Patient Triage data ", CONCAT("Time: ", NOW());
		END$$


-- ------------- populate etl_prep_behaviour_risk_assessment-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_prep_behaviour_risk_assessment$$
CREATE PROCEDURE sp_populate_etl_prep_behaviour_risk_assessment()
  BEGIN
    SELECT "Processing Behaviour risk assessment form", CONCAT("Time: ", NOW());
    insert into kenyaemr_etl.etl_prep_behaviour_risk_assessment(
        uuid,
        provider,
        patient_id,
        visit_id,
        visit_date,
        location_id,
        encounter_id,
        date_created,
        sexual_partner_hiv_status,
        sexual_partner_on_art,
        risk,
        high_risk_partner,
        sex_with_multiple_partners,
        ipv_gbv,
        transactional_sex,
        recent_sti_infected,
        recurrent_pep_use,
        recurrent_sex_under_influence,
        inconsistent_no_condom_use,
        sharing_drug_needles,
        risk_education_offered,
        risk_reduction,
        assessment_outcome,
        willing_to_take_prep,
        reason_not_willing,
        risk_edu_offered,
        risk_education,
        referral_for_prevention_services,
        referral_facility,
        time_partner_hiv_positive_known,
        partner_enrolled_ccc,
        partner_ccc_number,
        partner_art_start_date,
        serodiscordant_confirmation_date,
        recent_unprotected_sex_with_positive_partner,
        children_with_hiv_positive_partner,
        voided
        )
    select
           e.uuid, e.creator as provider,e.patient_id, e.visit_id, e.encounter_datetime as visit_date, e.location_id, e.encounter_id,e.date_created,
           max(if(o.concept_id = 1436, (case o.value_coded when 703 then "HIV Positive" when 664 then "HIV Negative" when 1067 then "Unknown" else "" end), "" )) as sexual_partner_hiv_status,
           max(if(o.concept_id = 160119, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as sexual_partner_on_art,
           max(if(o.concept_id = 163310, (case o.value_coded when 162185 then "Detectable viral load" when 160119 then "On ART for less than 6 months"
                                                             when 160571 then "Couple is trying to concieve" when 159598 then "Suspected poor adherence" else "" end), "" )) as risk,
           max(if(o.concept_id = 160581, (case o.value_coded when 1065 then "High risk partner" else "" end), "" )) as high_risk_partner,
           max(if(o.concept_id = 159385, (case o.value_coded when 1065 then "Yes" else "" end), "" )) as sex_with_multiple_partners,
           max(if(o.concept_id = 141814, (case o.value_coded when 1065 then "Yes" else "" end), "" )) as ipv_gbv,
           max(if(o.concept_id = 160579, (case o.value_coded when 1065 then "Yes" else "" end), "" )) as transactional_sex,
           max(if(o.concept_id = 156660, (case o.value_coded when 1065 then "Yes" else "" end), "" )) as recent_sti_infected,
           max(if(o.concept_id = 164845, (case o.value_coded when 1065 then "Yes" else "" end), "" )) as recurrent_pep_use,
           max(if(o.concept_id = 165088, (case o.value_coded when 1065 then "Yes" else "" end), "" )) as recurrent_sex_under_influence,
           max(if(o.concept_id = 165089, (case o.value_coded when 1065 then "Yes" else "" end), "" )) as inconsistent_no_condom_use,
           max(if(o.concept_id = 165090, (case o.value_coded when 1065 then "Yes" else "" end), "" )) as sharing_drug_needles,
           max(if(o.concept_id = 165091, (case o.value_coded when 138643 then "Risk" when 1066 then "No risk" else "" end), "" )) as risk_assessment_outcome,
           max(if(o.concept_id = 165053, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as risk_education_offered,
           max(if(o.concept_id = 165092, o.value_text, null )) as risk_reduction,
           max(if(o.concept_id = 165094, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as willing_to_take_prep,
           max(if(o.concept_id = 1743, (case o.value_coded when 1107 then "None" when 159935 then "Side effects(ADR)" when 159935 then "Side effects(ADR)" when 164997 then "Stigma" when 160588 then "Pill burden" when 164401 then "Too many HIV tests" when 161888 then "Taking pills for a long time" else "" end), "" )) as reason_not_willing,
           max(if(o.concept_id = 161595, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as risk_edu_offered,
           max(if(o.concept_id = 161011, o.value_text, null )) as risk_education,
           max(if(o.concept_id = 165093, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as referral_for_prevention_services,
           max(if(o.concept_id = 161550, o.value_text, null )) as referral_facility,
           max(if(o.concept_id = 160082, o.value_datetime, null )) as time_partner_hiv_positive_known,
           max(if(o.concept_id = 165095, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as partner_enrolled_ccc,
           max(if(o.concept_id = 162053, o.value_numeric, null )) as partner_ccc_number,
           max(if(o.concept_id = 159599, o.value_datetime, null )) as partner_art_start_date,
           max(if(o.concept_id = 165096, o.value_datetime, null )) as serodiscordant_confirmation_date,
           max(if(o.concept_id = 165097, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as recent_unprotected_sex_with_positive_partner,
           max(if(o.concept_id = 1825, o.value_numeric, null )) as children_with_hiv_positive_partner,
           e.voided as voided

    from encounter e
			inner join person p on p.person_id=e.patient_id and p.voided=0
			inner join form f on f.form_id=e.form_id and f.uuid in ("40374909-05fc-4af8-b789-ed9c394ac785")
           inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (1436,160119,163310,160581,159385,160579,156660,164845,141814,165088,165089,165090,165091,165053,165092,165094,1743,161595,161011,165093,161550,160082,165095,162053,159599,165096,165097,1825) and o.voided=0
    where e.voided=0
    group by e.encounter_id;
    SELECT "Completed processing Behaviour risk assessment forms", CONCAT("Time: ", NOW());
  END$$

-- ------------- populate etl_prep_monthly_refill-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_prep_monthly_refill$$
CREATE PROCEDURE sp_populate_etl_prep_monthly_refill()
  BEGIN
    SELECT "Processing monthly refill form", CONCAT("Time: ", NOW());
    insert into kenyaemr_etl.etl_prep_monthly_refill(
        uuid,
        provider,
        patient_id,
        visit_id,
        visit_date,
        location_id,
        encounter_id,
        date_created,
        risk_for_hiv_positive_partner,
        client_assessment,
        adherence_assessment,
        poor_adherence_reasons,
        other_poor_adherence_reasons,
        adherence_counselling_done,
        prep_status,
        prescribed_prep_today,
        prescribed_regimen,
        prescribed_regimen_months,
        prep_discontinue_reasons,
        prep_discontinue_other_reasons,
        appointment_given,
        next_appointment,
        remarks,
        voided
        )
    select
           e.uuid, e.creator as provider,e.patient_id, e.visit_id, e.encounter_datetime as visit_date, e.location_id, e.encounter_id,e.date_created,
           max(if(o.concept_id = 1169, (case o.value_coded when 160571 then "Couple is trying to conceive" when 159598 then "Suspected poor adherence"
                                                           when 160119 then "On ART for less than 6 months" when 162854 then "Not on ART" else "" end), "" )) as risk_for_hiv_positive_partner,
           max(if(o.concept_id = 162189, (case o.value_coded when 159385 then "Has Sex with more than one partner" when 1402 then "Sex partner(s)at high risk for HIV and HIV status unknown"
                                                             when 160579 then "Transactional sex" when 165088 then "Recurrent sex under influence of alcohol/recreational drugs" when 165089 then "Inconsistent or no condom use" when 165090 then "Injecting drug use with shared needles and/or syringes"
                                                             when 164845 then "Recurrent use of Post Exposure Prophylaxis (PEP)" when 112992 then "Recent STI" when 141814 then "Ongoing IPV/GBV"  else "" end), "" )) as client_assessment,
           max(if(o.concept_id = 164075, (case o.value_coded when 159405 then "Good" when 159406 then "Fair"
                                                             when 159407 then "Poor" when 1067 then "Good,Fair,Poor,N/A(Did not pick PrEP at last"  else "" end), "" )) as adherence_assessment,
           max(if(o.concept_id = 160582, (case o.value_coded when 163293 then "Sick" when 1107 then "None"
                                                             when 164997 then "Stigma" when 160583 then "Shared with others" when 1064 then "No perceived risk"
                                                             when 160588 then "Pill burden" when 160584 then "Lost/out of pills" when 1056 then "Separated from HIV+"
                                                             when 159935 then "Side effects" when 160587 then "Forgot" when 5622 then "Other-specify" else "" end), "" )) as poor_adherence_reasons,
           max(if(o.concept_id = 160632, o.value_text, null )) as other_poor_adherence_reasons,
           max(if(o.concept_id = 164425, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as adherence_counselling_done,
           max(if(o.concept_id = 161641, (case o.value_coded when 159836 then "Discontinue" when 159835 then "Continue" else "" end), "" )) as prep_status,
           max(if(o.concept_id = 1417, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as prescribed_prep_today,
           max(if(o.concept_id = 164515, (case o.value_coded when 161364 then "TDF/3TC" when 84795 then "TDF"  when 104567 then "FTC/TDF" else "" end), "" )) as prescribed_regimen,
           max(if(o.concept_id = 164433, o.value_text, null )) as prescribed_regimen_months,
           max(if(o.concept_id = 161555, (case o.value_coded when 138571 then "HIV test is positive" when 113338 then "Renal dysfunction"
                                                             when 1302 then "Viral suppression of HIV+" when 159598 then "Not adherent to PrEP" when 164401 then "Too many HIV tests"
                                                             when 162696 then "Client request" when 5622 then "other"  else "" end), "" )) as prep_discontinue_reasons,
           max(if(o.concept_id = 160632, o.value_text, null )) as other_poor_adherence_reasons,
           max(if(o.concept_id = 164999, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as appointment_given,
           max(if(o.concept_id = 160632, o.value_datetime, null )) as next_appointment,
           max(if(o.concept_id = 161011, o.value_text, null )) as remarks,
           e.voided as voided
    from encounter e
           inner join form f on f.form_id=e.form_id and f.uuid in ("291c0828-a216-11e9-a2a3-2a2ae2dbcce4")
           inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (1169,162189,164075,160582,160632,164425,161641,1417,164515,164433,161555,160632,164999,161011) and o.voided=0
    where e.voided=0
    group by e.encounter_id;
    SELECT "Completed processing monthly refill", CONCAT("Time: ", NOW());
  END$$

-- ------------- populate etl_prep_discontinuation-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_prep_discontinuation$$
CREATE PROCEDURE sp_populate_etl_prep_discontinuation()
  BEGIN
    SELECT "Processing PrEP discontinuation form", CONCAT("Time: ", NOW());
    insert into kenyaemr_etl.etl_prep_discontinuation(
        uuid,
        provider,
        patient_id,
        visit_id,
        visit_date,
        location_id,
        encounter_id,
        date_created,
        discontinue_reason,
        care_end_date,
        voided
        )
    select
           e.uuid, e.creator as provider,e.patient_id, e.visit_id, e.encounter_datetime as visit_date, e.location_id, e.encounter_id,e.date_created,
           max(if(o.concept_id = 161555, (case o.value_coded when 138571 then "HIV test is positive" when 113338 then "Renal dysfunction" when 1302 then "Viral suppression of HIV+" when 159598 then "Not adherent to PrEP" when 164401 then "Too many HIV tests" when 162696 then "Client request"
                                                             when 150506 then "Intimate partner violence"  when 978 then "Self Discontinuation"  when 160581 then "Low risk of HIV" when 5622 then "Other" else "" end), "" )) as discontinue_reason,
           max(if(o.concept_id = 164073, o.value_datetime, null )) as care_end_date,
           e.voided
    from encounter e
			inner join person p on p.person_id=e.patient_id and p.voided=0
			inner join form f on f.form_id=e.form_id and f.uuid in ("467c4cc3-25eb-4330-9cf6-e41b9b14cc10")
      inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (161555,164073) and o.voided=0
    where e.voided=0
    group by e.encounter_id;
    SELECT "Completed processing PrEP discontinuation", CONCAT("Time: ", NOW());
  END$$

-- ------------- populate etl_prep_enrollment-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_prep_enrolment$$
CREATE PROCEDURE sp_populate_etl_prep_enrolment()
  BEGIN
    SELECT "Processing PrEP enrolment form", CONCAT("Time: ", NOW());
    insert into kenyaemr_etl.etl_prep_enrolment(
        uuid,
        provider,
        patient_id,
        visit_id,
        visit_date,
        location_id,
        encounter_id,
        date_created,
        patient_type,
        transfer_in_entry_point,
        referred_from,
        transit_from,
        transfer_in_date,
        transfer_from,
        initial_enrolment_date,
        date_started_prep_trf_facility,
        previously_on_prep,
        regimen,
        prep_last_date,
        in_school,
        buddy_name,
        buddy_alias,
        buddy_relationship,
        buddy_phone,
        buddy_alt_phone,
        voided
        )
    select
           e.uuid, e.creator as provider,e.patient_id, e.visit_id, e.encounter_datetime as visit_date, e.location_id, e.encounter_id,e.date_created,
           max(if(o.concept_id = 164932, (case o.value_coded when 164144 then "New Patient" when 160563 then "Transfer in" when 164931 then "Transit" when 159833 then "Re-enrollment(Re-activation)" else "" end), "" )) as patient_type,
           max(if(o.concept_id = 160540, (case o.value_coded when 159938 then "HBTC" when 160539 then "VCT Site" when 159937 then "MCH" when 160536 then "IPD-Adult" when 160541 then "TB Clinic" when 160542 then "OPD" when 162050 then "CCC" when 160551 then "Self Test" when 5622 then "Other" else "" end), "" )) as transfer_in_entry_point,
           max(if(o.concept_id = 162724, o.value_text, null )) as referred_from,
           max(if(o.concept_id = 161550, o.value_text, null )) as transit_from,
           max(if(o.concept_id = 160534, o.value_datetime, null )) as transfer_in_date,
           max(if(o.concept_id = 160535, o.value_text, null )) as transfer_from,
           max(if(o.concept_id = 160555, o.value_datetime, null )) as initial_enrolment_date,
           max(if(o.concept_id = 159599, o.value_datetime, null )) as date_started_prep_trf_facility,
           max(if(o.concept_id = 160533, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as previously_on_prep,
           max(if(o.concept_id = 1088, (case o.value_coded when 104567 then "TDF/FTC" when 84795 then "TDF" when 161364 then "TDF/3TC" else "" end), "" )) as regimen,
           max(if(o.concept_id = 162881, o.value_datetime, null )) as prep_last_date,
           max(if(o.concept_id = 5629, o.value_coded, null )) as in_school,
           max(if(o.concept_id = 160638, o.value_text, null )) as buddy_name,
           max(if(o.concept_id = 165038, o.value_text, null )) as buddy_alias,
           max(if(o.concept_id = 160640,(case o.value_coded when 973 then "Grandparent" when 972 then "Sibling" when 160639 then "Guardian" when 1527 then "Parent" when 5617 then "Spouse" when 163565 then "Partner" when 5622 then "Other" else "" end), "" )) as buddy_relationship,
           max(if(o.concept_id = 160642, o.value_text, null )) as buddy_phone,
           max(if(o.concept_id = 160641, o.value_text, null )) as buddy_alt_phone,
           e.voided as voided

    from encounter e
			inner join person p on p.person_id=e.patient_id and p.voided=0
			inner join form f on f.form_id=e.form_id and f.uuid in ("d5ca78be-654e-4d23-836e-a934739be555")
      inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (164932,160540,162724,161550,160534,160535,160555,159599,160533,1088162881,5629,160638,165038,160640,160642,160641) and o.voided=0
    where e.voided=0
    group by e.encounter_id;
    SELECT "Completed processing PrEP enrolment", CONCAT("Time: ", NOW());
  END$$

-- ------------- populate etl_prep_followup-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_prep_followup$$
CREATE PROCEDURE sp_populate_etl_prep_followup()
  BEGIN
    SELECT "Processing PrEP follow-up form", CONCAT("Time: ", NOW());
    insert into kenyaemr_etl.etl_prep_followup(
        uuid,
        provider,
        patient_id,
        visit_id,
        visit_date,
        location_id,
        encounter_id,
        date_created,
        sti_screened,
        genital_ulcer_desease,
        vaginal_discharge,
        cervical_discharge,
        pid,
        urethral_discharge,
        anal_discharge,
        other_sti_symptoms,
        sti_treated,
        vmmc_screened,
        vmmc_status,
        vmmc_referred,
        lmp,
        pregnant,
        edd,
        planned_pregnancy,
        wanted_pregnancy,
        breastfeeding,
        fp_status,
        fp_method,
        ended_pregnancy,
        pregnancy_outcome,
        outcome_date,
        defects,
        has_chronic_illness,
        chronic_illness,
        chronic_illness_onset_date,
        chronic_illness_drug,
        chronic_illness_dose,
        chronic_illness_units,
        chronic_illness_frequency,
        chronic_illness_duration,
        chronic_illness_duration_units,
        adverse_reactions,
        medicine_reactions,
        reaction,
        severity,
        action_taken,
        known_allergies,
        allergen,
        allergy_reaction,
        allergy_severity,
        allergy_date,
        hiv_signs,
        adherence_counselled,
        prep_contraindicatios,
        treatment_plan,
        condoms_issued,
        number_of_condoms,
        appointment_given,
        appointment_date,
        reason_no_appointment,
        clinical_notes,
        voided
        )
    select
           e.uuid, e.creator as provider,e.patient_id, e.visit_id, e.encounter_datetime as visit_date, e.location_id, e.encounter_id,e.date_created,
           max(if(o.concept_id = 161558,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as sti_screened,
           max(if(o.concept_id = 165098 and o.value_coded = 145762,"GUD",null)) as genital_ulcer_desease,
           max(if(o.concept_id = 165098 and o.value_coded = 121809,"VG",null)) as vaginal_discharge,
           max(if(o.concept_id = 165098 and o.value_coded = 116995,"CD",null)) as cervical_discharge,
           max(if(o.concept_id = 165098 and o.value_coded = 130644,"PID",null)) as pid,
           max(if(o.concept_id = 165098 and o.value_coded = 123529,"UD",null)) as urethral_discharge,
           max(if(o.concept_id = 165098 and o.value_coded = 148895,"AD",null)) as anal_discharge,
           max(if(o.concept_id = 165098 and o.value_coded = 5622,"Other",null)) as other_sti_symptoms,
           max(if(o.concept_id = 165200,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as sti_treated,
           max(if(o.concept_id = 165308,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as vmmc_screened,
           max(if(o.concept_id = 165099,(case o.value_coded when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end), "" )) as vmmc_status,
           max(if(o.concept_id = 1272,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as vmmc_referred,
           max(if(o.concept_id = 1472, o.value_datetime, null )) as lmp,
           max(if(o.concept_id = 5272,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as pregnant,
           max(if(o.concept_id = 5596, o.value_datetime, null )) as edd,
           max(if(o.concept_id = 1426, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as planned_pregnancy,
           max(if(o.concept_id = 164933, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as wanted_pregnancy,
           max(if(o.concept_id = 5632, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as breastfeeding,
           max(if(o.concept_id = 160653, (case o.value_coded when 965 then "On Family Planning" when 160652 then "Not using Family Planning" when 1360 then "Wants Family Planning" else "" end), "" )) as fp_status,
           max(if(o.concept_id = 374, (case o.value_coded when 160570 then "Emergency contraceptive pills" when 780 then "Oral Contraceptives Pills" when 5279 then "Injectable" when 1359 then "Implant" when 136163 then "Lactational Amenorhea Method"
                                                          when 5275 then "Intrauterine Device" when 5278 then "Diaphram/Cervical Cap" when 5277 then "Fertility Awareness" when 1472 then "Tubal Ligation/Female sterilization" when 190 then "Condoms" when 1489 then "Vasectomy(Partner)" when 162332 then "Undecided" else "" end), "" )) as fp_method,
           max(if(o.concept_id = 165103, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as ended_pregnancy,
           max(if(o.concept_id = 161033, (case o.value_coded when 1395 then "Term live" when 129218 then "Preterm Delivery" when 125872 then "Still birth" when 159896 then "Induced abortion" else "" end), "" )) as pregnancy_outcome,
           max(if(o.concept_id = 1596, o.value_datetime, null )) as outcome_date,
           max(if(o.concept_id = 164122, (case o.value_coded when 155871 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end), "" )) as defects,
           max(if(o.concept_id = 162747, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as has_chronic_illness,
           max(if(o.concept_id = 1284, (case o.value_coded when 149019 then "Alzheimer''s Disease and other Dementias" when 148432 then "Arthritis" when 153754 then "Asthma" when 159351 then "Cancer" when 119270 then "Cardiovascular diseases" when 120637 then "Chronic Hepatitis"
                                                           when 145438 then "Chronic Kidney Disease" when 1295 then "Chronic Obstructive Pulmonary Disease(COPD)" when 120576 then "Chronic Renal Failure" when 119692 then "Cystic Fibrosis" when 120291 then "Deafness and Hearing impairment" when 119481 then "Diabetes" when 118631 then "Endometriosis" when 117855 then "Epilepsy" when 117789 then "Glaucoma" when 139071 then "Heart Disease" when 115728 then "Hyperlipidaemia" when 117399 then "Hypertension"  when 117321 then "Hypothyroidism" when 151342 then "Mental illness"
                                                           when 133687 then "Multiple Sclerosis" when 115115 then "Obesity" when 114662 then "Osteoporosis" when 117703 then "Sickle Cell Anaemia" when 118976 then "Thyroid disease" else "" end), "" )) as chronic_illness,
           max(if(o.concept_id = 159948, o.value_datetime, null )) as chronic_illness_onset_date,
           max(if(o.concept_id = 1282, o.value_coded, null )) as chronic_illness_drug,
           max(if(o.concept_id = 1443, o.value_numeric, null )) as chronic_illness_dose,
           max(if(o.concept_id = 1444, o.value_text, null )) as chronic_illness_units,
           max(if(o.concept_id = 160855, (case o.value_coded when 160862 then "Once daily" when 160863 then "Once daily at bedtime" when 160864 then "Once daily in the evening" when 160865 then "Once daily in the morning" when 160858 then "Twice daily" when 160866 then "Thrice daily" when 160870 then "Four times daily" else "" end), "" )) as chronic_illness_frequency,
           max(if(o.concept_id = 159368, o.value_numeric, null )) as chronic_illness_duration,
           max(if(o.concept_id = 1732, (case o.value_coded when 1822 then "Hours" when 1072 then "Days" when 1073 then "Weeks" when 1074 then "Months" else "" end), "" )) as chronic_illness_duration_units,
           max(if(o.concept_id = 121764, o.value_boolean, null )) as adverse_reactions,
           max(if(o.concept_id = 1193, (case o.value_coded when 70056 then "Abicavir" when 162298 then "ACE inhibitors" when 70878 then "Allopurinol" when 155060 then "Aminoglycosides"
                                                           when 162299 then "ARBs (angiotensin II receptor blockers)" when  103727 then "Aspirin" when 71647 then "Atazanavir" when 72822 then "Carbamazepine"  when 162301 then "Cephalosporins" when 73300 then "Chloroquine" when 73667 then "Codeine"
                                                           when 74807 then "Didanosine" when 75523 then "Efavirenz" when 162302 then "Erythromycins" when 75948 then "Ethambutol" when 77164 then "Griseofulvin" when 162305 then "Heparins" when 77675 then "Hydralazine" when 78280 then "Isoniazid"
                                                           when 794 then "Lopinavir/ritonavir" when 80106 then "Morphine" when 80586 then "Nevirapine" when 80696 then "Nitrofurans"  when 162306 then "Non-steroidal anti-inflammatory drugs" when 81723 then "Penicillamine" when 81724 then "Penicillin"
                                                           when 81959 then "Phenolphthaleins" when 82023 then "Phenytoin" when 82559 then "Procainamide" when 82900 then "Pyrazinamide" when 83018 then "Quinidine" when 767 then "Rifampin" when 162307 then "Statins" when 84309 then "Stavudine"
                                                           when 162170 then "Sulfonamides" when 84795 then "Tenofovir" when 84893 then "Tetracycline" when 86663 then "Zidovudine" when 5622 then "Other"
                                                           else "" end), "" )) as medicine_reactions,
           max(if(o.concept_id = 159935, (case o.value_coded when 1067 then "Unknown" when 121629 then "Anaemia" when 148888 then "Anaphylaxis" when 148787 then "Angioedema" when 120148 then "Arrhythmia" when 108 then "Bronchospasm" when 143264 then "Cough"
                                                             when 142412 then "Diarrhea" when 118773 then "Dystonia" when 140238 then "Fever" when 140039 then "Flushing" when 139581 then "GI upset" when 139084 then "Headache" when 159098 then "Hepatotoxicity" when 111061 then "Hives" when 117399 then "Hypertension"
                                                             when 879 then "Itching" when 121677 then "Mental status change" when 159347 then "Musculoskeletal pain" when 121 then "Myalgia" when 512 then "Rash" when 5622 then "Other" else "" end), "" )) as reaction,
           max(if(o.concept_id = 162760, (case o.value_coded when 1498 then "Mild" when 1499 then "Moderate" when 1500 then "Severe" when 162819 then "Fatal" when 1067 then  "Unknown" else "" end), "" )) as severity,
           max(if(o.concept_id = 1255, (case o.value_coded when 1257 then "Continue Regimen" when 1259 then "Switched Regimen" when 981 then "Changed Dose" when 1258 then "Substituted Drug" when 1107 then "None" when 1260 then "Stop" when 5622 then "Other" else "" end), "" )) as action_taken,
           max(if(o.concept_id = 160557, o.value_boolean, null )) as known_allergies,
           max(if(o.concept_id = 160643, (case o.value_coded when 162543 then "Beef" when 72609 then "Caffeine" when 162544 then "Chocolate" when 162545 then "Dairy Food" when 162171 then "Eggs" when 162546 then "Fish" when 162547  then "Milk Protein" when 162172 then "Peanuts" when 162175  then "Shellfish"
                                                             when 162176 then "Soy" when 162548 then "Strawberries" when 162177 then "Wheat" when 162542 then "Adhesive Tape" when 162536 then "Bee Stings" when 162537 then "Dust" when 162538 then "Latex" when 162539 then "Mold" when 162540 then "Pollen"
                                                             when 162541 then "Ragweed" when 5622 then "Other" else "" end), "" )) as allergen,
           max(if(o.concept_id = 159935, (case o.value_coded when 1067 then "Unknown" when 121629 then "Anaemia" when 148888 then "Anaphylaxis" when 148787 then "Angioedema" when 120148 then "Arrhythmia" when 108 then "Bronchospasm" when  143264  then "Cough" when 142412  then "Diarrhea" when 118773 then "Dystonia"
                                                             when  140238 then "Fever" when  140039 then "Flushing" when  139581  then "GI upset" when 139084 then "Headache" when 159098 then "Hepatotoxicity" when 111061 then "Hives" when  117399 then "Hypertension" when 879  then "Itching" when 121677 then "Mental status change" when 159347 then "Musculoskeletal pain"
                                                             when 121 then "Myalgia" when 512 then "Rash" when 5622 then "Other"  else "" end), "" )) as allergy_reaction,
           max(if(o.concept_id = 162760, (case o.value_coded when 1498 then "Mild" when 1499 then "Moderate" when 1500 then "Severe" when 162819 then "Fatal" when 1067 then "Unknown" else "" end), "" )) as allergy_severity,
           max(if(o.concept_id = 160753, o.value_datetime, null )) as allergy_date,
           max(if(o.concept_id = 165101, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as hiv_signs,
           max(if(o.concept_id = 165104, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as adherence_counselled,
           max(if(o.concept_id = 165106, (case o.value_coded when 1107 then "None" when 138571 then "Confirmed HIV+" when 155589 then "Renal impairment" when 127750 then "Not willing" when 165105 then "Less than 35ks and under 15 yrs" else "" end), "" )) as prep_contraindicatios,
           max(if(o.concept_id = 165109, (case o.value_coded when 1256 then "Start" when 1257 then "Continue" when 162904 then "Restart" when 1258 then "Substitute" when 1260 then "Defer" else "" end), "" )) as treatment_plan,
           max(if(o.concept_id = 159777, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as condoms_issued,
           max(if(o.concept_id = 165055, o.value_numeric, null )) as number_of_condoms,
           max(if(o.concept_id = 165309, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as appointment_given,
           max(if(o.concept_id = 5096, o.value_datetime, null )) as appointment_date,
           max(if(o.concept_id = 165310, (case o.value_coded when 165053 then "Risk will no longer exist" when 159492 then "Intention to transfer out" else "" end), "" )) as reason_no_appointment,
           max(if(o.concept_id = 163042, o.value_text, null )) as clinical_notes,
           e.voided
    from encounter e
			inner join person p on p.person_id=e.patient_id and p.voided=0
			inner join form f on f.form_id=e.form_id and f.uuid in ("ee3e2017-52c0-4a54-99ab-ebb542fb8984","1bfb09fc-56d7-4108-bd59-b2765fd312b8")
      inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (161558,165098,165200,165308,165099,1272,1472,5272,5596,1426,164933,5632,160653,374,
            165103,161033,1596,164122,162747,1284,159948,1282,1443,1444,160855,159368,1732,121764,1193,159935,162760,1255,160557,160643,159935,162760,160753,165101,165104,165106,
            165109,159777,165055,165309,5096,165310,163042) and o.voided=0
    where e.voided=0
    group by e.encounter_id;
    SELECT "Completed processing PrEP follow-up form", CONCAT("Time: ", NOW());
  END$$

-- ------------- populate etl_progress_note-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_progress_note$$
CREATE PROCEDURE sp_populate_etl_progress_note()
  BEGIN
    SELECT "Processing progress form", CONCAT("Time: ", NOW());
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
    where e.voided=0
    group by e.encounter_id;
    SELECT "Completed processing progress note", CONCAT("Time: ", NOW());

END$$
		---------------------------------------- populate ipt initiation -----------------------------
DROP PROCEDURE IF EXISTS sp_populate_etl_ipt_initiation$$
CREATE PROCEDURE sp_populate_etl_ipt_initiation()
	BEGIN
		SELECT "Processing IPT initiations ", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_ipt_initiation(
			patient_id,
			uuid,
			encounter_provider,
			visit_date ,
			location_id,
			encounter_id,
			date_created,
			ipt_indication,
      sub_county_reg_date,
			voided
		)
			select
				e.patient_id,
				e.uuid,
				e.creator,
				e.encounter_datetime,
				e.location_id,
				e.encounter_id,
				e.date_created,
				max(if(o.concept_id=162276,o.value_coded,null)) as ipt_indication,
				max(if(o.concept_id=161552,o.value_datetime,null)) as sub_county_reg_date,
				e.voided
			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0 and o.concept_id in(162276,161552)
				inner join
				(
					select encounter_type_id, uuid, name from encounter_type where
						uuid in('de5cacd4-7d15-4ad0-a1be-d81c77b6c37d')
				) et on et.encounter_type_id=e.encounter_type
				where e.voided=0
			group by e.encounter_id;
		SELECT "Completed processing IPT Initiation ", CONCAT("Time: ", NOW());

update kenyaemr_etl.etl_ipt_initiation i
join (select pi.patient_id,
max(if(pit.uuid='d8ee3b8c-a8fc-4d6b-af6a-9423be5f8906',pi.identifier,null)) sub_county_reg_number
from patient_identifier pi
join patient_identifier_type pit on pi.identifier_type=pit.patient_identifier_type_id
where voided=0
group by pi.patient_id) pid on pid.patient_id=i.patient_id
set i.sub_county_reg_number=pid.sub_county_reg_number;
END$$

	-- ------------------------------------- process ipt followup -------------------------
DROP PROCEDURE IF EXISTS sp_populate_etl_ipt_followup$$
CREATE PROCEDURE sp_populate_etl_ipt_followup()
	BEGIN
		SELECT "Processing IPT followup ", CONCAT("Time: ", NOW());
		INSERT INTO kenyaemr_etl.etl_ipt_follow_up(
			uuid,
			patient_id,
			visit_id,
			visit_date,
			location_id,
			encounter_id,
			provider,
			date_created,
			ipt_due_date,
			weight,
			date_collected_ipt,
			hepatotoxity,
			peripheral_neuropathy,
			rash,
			adherence,
			action_taken,
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
				max(if(o.concept_id=164073,date(o.value_datetime),null)) as ipt_due_date,
				max(if(o.concept_id=164074,date(o.value_datetime),null)) as date_collected_ipt,
				max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
				max(if(o.concept_id=159098,o.value_coded,null)) as hepatotoxity,
				max(if(o.concept_id=118983,o.value_coded,null)) as peripheral_neuropathy,
				max(if(o.concept_id=512,o.value_coded,null)) as rash,
				max(if(o.concept_id=164075,o.value_coded,null)) as adherence,
				max(if(o.concept_id=160632,o.value_text,null)) as action_taken,
				e.voided as voided
			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join
				(
					select encounter_type_id, uuid, name from encounter_type where uuid in('aadeafbe-a3b1-4c57-bc76-8461b778ebd6')
				) et on et.encounter_type_id=e.encounter_type
				left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
																 and o.concept_id in (164073,164074,159098,5089,118983,512,164075,160632)
			where e.voided=0
			group by e.patient_id, e.encounter_id, visit_date
		;
		SELECT "Completed processing IPT followup data ", CONCAT("Time: ", NOW());
		END$$
		-- ----------------------------------- process ipt outcome ---------------------------
DROP PROCEDURE IF EXISTS sp_populate_etl_ipt_outcome$$
CREATE PROCEDURE sp_populate_etl_ipt_outcome()
	BEGIN
		SELECT "Processing IPT outcome ", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_ipt_outcome(
			patient_id,
			uuid,
			encounter_provider,
			visit_date ,
			location_id,
			encounter_id,
			date_created,
			outcome,
			voided
		)
			select
				e.patient_id,
				e.uuid,
				e.creator,
				e.encounter_datetime,
				e.location_id,
				e.encounter_id,
				e.date_created,
				max(if(o.concept_id=161555,o.value_coded,null)) as ipt_outcome,
				e.voided
			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0 and o.concept_id=161555
				inner join
				(
					select encounter_type_id, uuid, name from encounter_type where
						uuid in('bb77c683-2144-48a5-a011-66d904d776c9')
				) et on et.encounter_type_id=e.encounter_type
				where e.voided=0
			group by e.encounter_id;
		SELECT "Completed processing IPT outcome ", CONCAT("Time: ", NOW());
		END$$

		-- --------------------------------------- process HTS linkage tracing ------------------------
DROP PROCEDURE IF EXISTS sp_populate_etl_hts_linkage_tracing$$
CREATE PROCEDURE sp_populate_etl_hts_linkage_tracing()
	BEGIN
		SELECT "Processing HTS Linkage tracing ", CONCAT("Time: ", NOW());
		INSERT INTO kenyaemr_etl.etl_hts_linkage_tracing(
			uuid,
			patient_id,
			visit_date,
			location_id,
			encounter_id,
			encounter_provider,
			date_created,
			tracing_type,
			tracing_outcome,
			reason_not_contacted,
			voided
		)
			select
				e.uuid,
				e.patient_id,
				date(e.encounter_datetime) as visit_date,
				e.location_id,
				e.encounter_id as encounter_id,
				e.creator,
				e.date_created as date_created,
				max(if(o.concept_id=164966,o.value_coded,null)) as tracing_type,
				max(if(o.concept_id=159811,o.value_coded,null)) as tracing_outcome,
				max(if(o.concept_id=1779,o.value_coded,null)) as reason_not_contacted,
				e.voided as voided
			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join
				(
					select form_id, uuid,name from form where
						uuid in('15ed03d2-c972-11e9-a32f-2a2ae2dbcce4')
				) f on f.form_id=e.form_id
				left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
																 and o.concept_id in (164966,159811,1779)
			where e.voided=0
			group by e.patient_id, e.encounter_id, visit_date
		;
		SELECT "Completed processing HTS linkage tracing data ", CONCAT("Time: ", NOW());
		END$$

		-- ------------------------- process patient program ------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_patient_program$$
CREATE PROCEDURE sp_populate_etl_patient_program()
	BEGIN
		SELECT "Processing patient program ", CONCAT("Time: ", NOW());
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
				pp.uuid,
				pp.patient_id,
				pp.location_id,
				(case p.uuid
				when "9f144a34-3a4a-44a9-8486-6b7af6cc64f6" then "TB"
				when "dfdc6d40-2f2f-463d-ba90-cc97350441a8" then "HIV"
				when "c2ecdf11-97cd-432a-a971-cfd9bd296b83" then "MCH-Child Services"
				when "b5d9e05f-f5ab-4612-98dd-adb75438ed34" then "MCH-Mother Services"
				when "335517a1-04bc-438b-9843-1ba49fb7fcd9" then "IPT"
				when "24d05d30-0488-11ea-8d71-362b9e155667" then "OTZ"
				end) as program,
				pp.date_enrolled,
				pp.date_completed,
				pp.outcome_concept_id,
				pp.date_created,
				pp.voided
			from patient_program pp
				inner join patient pt on pt.patient_id=pp.patient_id and pt.voided=0
				inner join program p on p.program_id=pp.program_id and p.retired=0
        where pp.voided=0
		;
		SELECT "Completed processing patient program data ", CONCAT("Time: ", NOW());
		END$$

  -- ------------------- populate person address table -------------

DROP PROCEDURE IF EXISTS sp_populate_etl_person_address$$
CREATE PROCEDURE sp_populate_etl_person_address()
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
      where pa.voided=0
    ;
    SELECT "Completed processing person_address data ", CONCAT("Time: ", NOW());
    END$$

    	 -- --------------------------------------- process OTZ enrollment ------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_otz_enrollment$$
CREATE PROCEDURE sp_populate_etl_otz_enrollment()
	BEGIN
		SELECT "Processing OTZ Enrollment ", CONCAT("Time: ", NOW());
		INSERT INTO kenyaemr_etl.etl_otz_enrollment(
			uuid,
			patient_id,
			visit_date,
			location_id,
			encounter_id,
			encounter_provider,
			date_created,
			orientation,
			leadership,
			participation,
			treatment_literacy,
			transition_to_adult_care,
			making_decision_future,
			srh,
			beyond_third_ninety,
			transfer_in,
			voided
		)
			select
				e.uuid,
				e.patient_id,
				date(e.encounter_datetime) as visit_date,
				e.location_id,
				e.encounter_id as encounter_id,
				e.creator,
				e.date_created as date_created,
				max(if(o.concept_id=165359,(case o.value_coded when 1065 then "Yes" else "" end),null)) as orientation,
				max(if(o.concept_id=165361,(case o.value_coded when 1065 then "Yes" else "" end),null)) as leadership,
				max(if(o.concept_id=165360,(case o.value_coded when 1065 then "Yes" else "" end),null)) as participation,
				max(if(o.concept_id=165364,(case o.value_coded when 1065 then "Yes" else "" end),null)) as treatment_literacy,
				max(if(o.concept_id=165363,(case o.value_coded when 1065 then "Yes" else "" end),null)) as transition_to_adult_care,
				max(if(o.concept_id=165362,(case o.value_coded when 1065 then "Yes" else "" end),null)) as making_decision_future,
				max(if(o.concept_id=165365,(case o.value_coded when 1065 then "Yes" else "" end),null)) as srh,
				max(if(o.concept_id=165366,(case o.value_coded when 1065 then "Yes" else "" end),null)) as beyond_third_ninety,
				max(if(o.concept_id=160563,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as transfer_in,
				e.voided as voided
			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join
				(
					select form_id, uuid,name from form where
						uuid in('3ae95898-0464-11ea-8d71-362b9e155667')
				) f on f.form_id=e.form_id
				left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
																 and o.concept_id in (165359,165361,165360,165364,165363,165362,165365,165366,160563)
			where e.voided=0
			group by e.patient_id, e.encounter_id, visit_date
		;
		SELECT "Completed processing OTZ enrollment data ", CONCAT("Time: ", NOW());
		END$$


    -- --------------------------------------- process OTZ activity ------------------------
DROP PROCEDURE IF EXISTS sp_populate_etl_otz_activity$$
CREATE PROCEDURE sp_populate_etl_otz_activity()
	BEGIN
		SELECT "Processing OTZ Activity ", CONCAT("Time: ", NOW());
		INSERT INTO kenyaemr_etl.etl_otz_activity(
			uuid,
			patient_id,
			visit_date,
			location_id,
			encounter_id,
			encounter_provider,
			date_created,
			orientation,
			leadership,
			participation,
			treatment_literacy,
			transition_to_adult_care,
			making_decision_future,
			srh,
			beyond_third_ninety,
			attended_support_group,
			remarks,
			voided
		)
			select
				e.uuid,
				e.patient_id,
				date(e.encounter_datetime) as visit_date,
				e.location_id,
				e.encounter_id as encounter_id,
				e.creator,
				e.date_created as date_created,
				max(if(o.concept_id=165359,(case o.value_coded when 1065 then "Yes" else "" end),null)) as orientation,
				max(if(o.concept_id=165361,(case o.value_coded when 1065 then "Yes" else "" end),null)) as leadership,
				max(if(o.concept_id=165360,(case o.value_coded when 1065 then "Yes" else "" end),null)) as participation,
				max(if(o.concept_id=165364,(case o.value_coded when 1065 then "Yes" else "" end),null)) as treatment_literacy,
				max(if(o.concept_id=165363,(case o.value_coded when 1065 then "Yes" else "" end),null)) as transition_to_adult_care,
				max(if(o.concept_id=165362,(case o.value_coded when 1065 then "Yes" else "" end),null)) as making_decision_future,
				max(if(o.concept_id=165365,(case o.value_coded when 1065 then "Yes" else "" end),null)) as srh,
				max(if(o.concept_id=165366,(case o.value_coded when 1065 then "Yes" else "" end),null)) as beyond_third_ninety,
				max(if(o.concept_id=165302,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as attended_support_group,
				max(if(o.concept_id=161011,trim(o.value_text),null)) as remarks,

				e.voided as voided
			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join
				(
					select form_id, uuid,name from form where
						uuid in('3ae95d48-0464-11ea-8d71-362b9e155667')
				) f on f.form_id=e.form_id
				left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
																 and o.concept_id in (165359,165361,165360,165364,165363,165362,165365,165366,165302)
			where e.voided=0
			group by e.patient_id, e.encounter_id, visit_date
		;
		SELECT "Completed processing OTZ activity data ", CONCAT("Time: ", NOW());
		END$$



-- ------------------------- create table for default facility ------------------------

DROP PROCEDURE IF EXISTS sp_create_default_facility_table$$
CREATE PROCEDURE sp_create_default_facility_table()
	BEGIN
		SELECT "Processing default facility info ", CONCAT("Time: ", NOW());
		CREATE TABLE kenyaemr_etl.etl_default_facility_info
			as select (select value_reference from location_attribute
			where location_id in (select property_value
														from global_property
														where property='kenyaemr.defaultLocation') and attribute_type_id=1) as siteCode,
								(select name from location
								where location_id in (select property_value
																			from global_property
																			where property='kenyaemr.defaultLocation')) as FacilityName;

		SELECT "Completed processing information about default facility ", CONCAT("Time: ", NOW());
		END$$

		    	 -- --------------------------------------- process OVC enrollment ------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_ovc_enrolment$$
CREATE PROCEDURE sp_populate_etl_ovc_enrolment()
	BEGIN
		SELECT "Processing OVC Enrolment ", CONCAT("Time: ", NOW());
		INSERT INTO kenyaemr_etl.etl_ovc_enrolment(
			uuid,
			patient_id,
			visit_date,
			location_id,
			encounter_id,
			encounter_provider,
			date_created,
		  caregiver_enrolled_here,
		  caregiver_name,
		  caregiver_gender,
		  relationship_to_client,
		  caregiver_phone_number,
		  client_enrolled_cpims,
		  partner_offering_ovc,
			voided
		)
			select
				e.uuid,
				e.patient_id,
				date(e.encounter_datetime) as visit_date,
				e.location_id,
				e.encounter_id as encounter_id,
				e.creator,
				e.date_created as date_created,
				max(if(o.concept_id=163777,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as caregiver_enrolled_here,
				max(if(o.concept_id=163258,o.value_text,null)) as caregiver_name,
				max(if(o.concept_id=1533,(case o.value_coded when 1534 then "Male" when 1535 then "Female" else "" end),null)) as caregiver_gender,
				max(if(o.concept_id=164352,(case o.value_coded when 1527 then "Parent" when 974 then "Uncle" when 972 then "Sibling" when 162722 then "Childrens home" when 975 then "Aunt"  else "" end),null)) as relationship_to_client,
				max(if(o.concept_id=160642,o.value_text,null)) as caregiver_phone_number,
				max(if(o.concept_id=163766,(case o.value_coded when 1065 then "Yes" else "" end),null)) as client_enrolled_cpims,
				max(if(o.concept_id=165347,o.value_text,null)) as partner_offering_ovc,
				e.voided as voided
			from encounter e
				inner join person p on p.person_id=e.patient_id and p.voided=0
				inner join
				(
					select form_id, uuid,name from form where
						uuid in('5cf01528-09da-11ea-8d71-362b9e155667')
				) f on f.form_id=e.form_id
				left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
																 and o.concept_id in (163777,163258,1533,164352,160642,163766,165347)
			where e.voided=0
			group by e.patient_id, e.encounter_id, visit_date
		;
		SELECT "Completed processing OVC enrolment data ", CONCAT("Time: ", NOW());
		END$$


-- -------------populate etl_cervical_cancer_screening-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_cervical_cancer_screening$$
CREATE PROCEDURE sp_populate_etl_cervical_cancer_screening()
BEGIN
SELECT "Processing HIV Follow-up, MCH ANC and PNC forms for CAXC screening", CONCAT("Time: ", NOW());

insert into kenyaemr_etl.etl_cervical_cancer_screening(
    uuid,
    encounter_id,
    encounter_provider,
    patient_id,
    visit_id,
    visit_date,
    location_id,
    date_created,
    screening_method,
    screening_result,
    encounter_type,
    voided
    )
select
       e.uuid,  e.encounter_id,e.creator as provider,e.patient_id, e.visit_id, e.encounter_datetime as visit_date, e.location_id,e.date_created,
       max(if(o.concept_id = 163589, (case o.value_coded when 885 then 'Pap Smear' when 162816 then 'VIA' when 164977 then 'VILI' when 5622 then 'Other' else "" end), "" )) as screening_method,
       max(if(o.concept_id = 164934, (case o.value_coded when 703 then 'Positive' when 159393 then 'Presumed' when 664  then 'Negative' else NULL end), '' )) as screening_result,
      f.name as encounter_type,
       e.voided as voided
from encounter e
	inner join person p on p.person_id=e.patient_id and p.voided=0
	inner join form f on f.form_id=e.form_id and f.uuid in ('e8f98494-af35-4bb8-9fc7-c409c8fed843','72aa78e0-ee4b-47c3-9073-26f3b9ecc4a7','22c68f86-bbf0-49ba-b2d1-23fa7ccf0259')
  inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (164934,163589) and o.voided=0
where e.voided=0
group by e.encounter_id
having screening_result is not null;

update kenyaemr_etl.etl_cervical_cancer_screening scr,
     (
     SELECT
            ThisRow.uuid,
            ThisRow.patient_id,
            ThisRow.visit_date,
            ThisRow.visit_id,
            ThisRow.screening_result currentResult,
            PrevRow.visit_date as prevVisitDate,
            PrevRow.screening_result previousResult,
            @x:=IF(@same_value=ThisRow.patient_id,@x+1,1) as rowNum,
            @same_value:=ThisRow.patient_id as dummy
     FROM
          kenyaemr_etl.etl_cervical_cancer_screening    AS ThisRow
            LEFT JOIN
              kenyaemr_etl.etl_cervical_cancer_screening    AS PrevRow
              ON  PrevRow.patient_id   = ThisRow.patient_id
                    AND PrevRow.visit_date = (SELECT MAX(s.visit_date)
                                              FROM kenyaemr_etl.etl_cervical_cancer_screening s
                                              WHERE s.patient_id  = ThisRow.patient_id
                                                AND s.visit_date < ThisRow.visit_date) order by ThisRow.patient_id, ThisRow.visit_date
     ) u,
     (SELECT  @x:=0, @same_value:='') t
set scr.previous_screening_date = u.prevVisitDate,scr.previous_screening_result = u.previousResult, scr.screening_number = u.rowNum
where scr.patient_id = u.patient_id and scr.visit_date = u.visit_date;

update kenyaemr_etl.etl_cervical_cancer_screening scr,
     (
     SELECT
            ThisRow.uuid,
            ThisRow.patient_id,
            ThisRow.visit_date,
            ThisRow.visit_id,
            ThisRow.screening_result currentResult,
            PrevRow.visit_date as prevVisitDate,
            PrevRow.screening_result previousResult,
            @x:=IF(@same_value=ThisRow.patient_id,@x+1,1) as rowNum,
            @same_value:=ThisRow.patient_id as dummy
     FROM
          kenyaemr_etl.etl_cervical_cancer_screening    AS ThisRow
            LEFT JOIN
              kenyaemr_etl.etl_cervical_cancer_screening    AS PrevRow
              ON  PrevRow.patient_id   = ThisRow.patient_id
                    AND PrevRow.visit_date = (SELECT MAX(s.visit_date)
                                              FROM kenyaemr_etl.etl_cervical_cancer_screening s
                                              WHERE s.patient_id  = ThisRow.patient_id
                                                AND s.visit_date < ThisRow.visit_date) order by ThisRow.patient_id, ThisRow.visit_date
     ) u,
     (SELECT  @x:=0, @same_value:='') t
set scr.previous_screening_date = u.prevVisitDate,scr.previous_screening_result = u.previousResult, scr.screening_number = u.rowNum
where scr.patient_id = u.patient_id and scr.visit_date = u.visit_date;
SELECT "Completed processing Cervical Cancer Screening", CONCAT("Time: ", NOW());

END$$
		-- ------------------------- process patient contact ------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_patient_contact$$
CREATE PROCEDURE sp_populate_etl_patient_contact()
	BEGIN
		SELECT "Processing patient contact ", CONCAT("Time: ", NOW());
		INSERT INTO kenyaemr_etl.etl_patient_contact(
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
      relationship_type,
      appointment_date,
      baseline_hiv_status,
      ipv_outcome,
      marital_status,
      living_with_patient,
      pns_approach,
      contact_listing_decline_reason,
      consented_contact_listing,
      voided
		)
			select
			  pc.id,
			  pc.uuid,
        pc.date_created,
        pc.first_name,
        pc.middle_name,
        pc.last_name,
        pc.sex,
        pc.birth_date,
        pc.physical_address,
        pc.phone_contact,
        pc.patient_related_to,
        pc.patient_id,
        pc.relationship_type,
        pc.appointment_date,
        pc.baseline_hiv_status,
        pc.ipv_outcome,
        pc.marital_status,
        pc.living_with_patient,
        pc.pns_approach,
        pc.contact_listing_decline_reason,
        pc.consented_contact_listing,
        pc.voided
			from kenyaemr_hiv_testing_patient_contact pc
				inner join kenyaemr_etl.etl_patient_demographics dm on dm.patient_id=pc.patient_related_to and dm.voided=0
        where pc.voided=0
		;
		SELECT "Completed processing patient contact data ", CONCAT("Time: ", NOW());
		END$$

				-- ------------------------- process contact trace ------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_client_trace$$
CREATE PROCEDURE sp_populate_etl_client_trace()
	BEGIN
		SELECT "Processing client trace ", CONCAT("Time: ", NOW());
		INSERT INTO kenyaemr_etl.etl_client_trace(
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
		)
			select
			  ct.id,
        ct.uuid,
        ct.date_created,
        ct.encounter_date,
        ct.client_id,
        ct.contact_type,
        ct.status,
        ct.unique_patient_no,
        ct.facility_linked_to,
        ct.health_worker_handed_to,
        ct.remarks,
        ct.appointment_date,
        ct.voided
			from kenyaemr_hiv_testing_client_trace ct
				inner join kenyaemr_etl.etl_patient_contact pc on pc.id=ct.client_id and ct.voided=0
        where pc.voided=0
		;
		SELECT "Completed processing client trace data ", CONCAT("Time: ", NOW());
		END$$
		-- end of dml procedures

		SET sql_mode=@OLD_SQL_MODE$$

-- ------------------------------------------- running all procedures -----------------------------

DROP PROCEDURE IF EXISTS sp_first_time_setup$$
CREATE PROCEDURE sp_first_time_setup()
BEGIN
DECLARE populate_script_id INT(11);
SELECT "Beginning first time setup", CONCAT("Time: ", NOW());
INSERT INTO kenyaemr_etl.etl_script_status(script_name, start_time) VALUES('initial_population_of_tables', NOW());
SET populate_script_id = LAST_INSERT_ID();

CALL sp_populate_etl_patient_demographics();
CALL sp_populate_etl_hiv_enrollment();
CALL sp_populate_etl_hiv_followup();
CALL sp_populate_etl_laboratory_extract();
CALL sp_populate_etl_pharmacy_extract();
CALL sp_populate_etl_program_discontinuation();
CALL sp_populate_etl_mch_enrollment();
CALL sp_populate_etl_mch_antenatal_visit();
CALL sp_populate_etl_mch_postnatal_visit();
CALL sp_populate_etl_tb_enrollment();
CALL sp_populate_etl_tb_follow_up_visit();
CALL sp_populate_etl_tb_screening();
CALL sp_populate_etl_hei_enrolment();
CALL sp_populate_etl_hei_immunization();
CALL sp_populate_etl_hei_follow_up();
CALL sp_populate_etl_mch_delivery();
CALL sp_populate_etl_mch_discharge();
CALL sp_drug_event();
CALL sp_populate_hts_test();
CALL sp_populate_hts_linkage_and_referral();
CALL sp_populate_hts_referral();
-- CALL sp_populate_etl_ipt_screening();
CALL sp_populate_etl_ccc_defaulter_tracing();
CALL sp_populate_etl_ART_preparation();
CALL sp_populate_etl_enhanced_adherence();
CALL sp_populate_etl_patient_triage();
CALL sp_populate_etl_ipt_initiation();
CALL sp_populate_etl_ipt_follow_up();
CALL sp_populate_etl_ipt_outcome();
CALL sp_populate_etl_prep_enrolment();
CALL sp_populate_etl_prep_followup();
CALL sp_populate_etl_prep_behaviour_risk_assessment();
CALL sp_populate_etl_prep_monthly_refill();
CALL sp_populate_etl_progress_note();
CALL sp_populate_etl_prep_discontinuation();
CALL sp_populate_etl_hts_linkage_tracing();
CALL sp_populate_etl_patient_program();
CALL sp_update_dashboard_table();
CALL sp_create_default_facility_table();
CALL sp_populate_etl_person_address();
CALL sp_populate_etl_otz_enrollment();
CALL sp_populate_etl_otz_activity();
CALL sp_populate_etl_ovc_enrolment();
CALL sp_populate_etl_cervical_cancer_screening();
CALL sp_populate_etl_patient_contact();
CALL sp_populate_etl_client_trace();


UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= populate_script_id;

SELECT "Completed first time setup", CONCAT("Time: ", NOW());
END$$



