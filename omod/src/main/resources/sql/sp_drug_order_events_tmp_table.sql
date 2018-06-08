DROP PROCEDURE IF EXISTS sp_process_regimen_switch_list$$
CREATE PROCEDURE sp_process_regimen_switch_list()
BEGIN

DECLARE no_more_rows BOOLEAN;
DECLARE eRowNum double;
DECLARE v_row_count INT(11);

DECLARE existing_drug_orders CURSOR FOR
SELECT distinct rowNum FROM tmp_regimen_events_ordered order by rowNum;

DECLARE CONTINUE HANDLER FOR NOT FOUND
SET no_more_rows = TRUE;

OPEN existing_drug_orders;
SET v_row_count = FOUND_ROWS();

IF v_row_count > 0 THEN
getUniqueNumRows: LOOP
FETCH existing_drug_orders INTO eRowNum;

IF no_more_rows THEN
CLOSE existing_drug_orders;
LEAVE getUniqueNumRows;
END IF;

IF eRowNum > 1 THEN
CALL sp_process_regimen_switch_item(eRowNum);
END IF;


END LOOP getUniqueNumRows;
ELSE
SELECT "NO ROWS WERE FOUND";
END IF;

END
$$

-- ---------------------------------------------------- process by rowNum -------------------------------


DROP PROCEDURE IF EXISTS sp_process_regimen_switch_item$$
CREATE PROCEDURE sp_process_regimen_switch_item(IN rowNum double)
BEGIN
DECLARE exec_status INT(11) DEFAULT 1;


DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
BEGIN
SET exec_status = -1;
ROLLBACK;
END;
-- perform all procedure calls within a transaction
START TRANSACTION;

UPDATE tmp_regimen_events_ordered t
inner join (
select ThisRow.rowNum ThisRowNum, ThisRow.uuid, ThisRow.patient_id,ThisRow.originalRegimen,  ThisRow.startedRegimen, ThisRow.DiscontinuedRegimen, PrevRow.resultingRegimen prevRowResultingRegimen
from tmp_regimen_events_ordered ThisRow inner join tmp_regimen_events_ordered PrevRow on ThisRow.patient_id=PrevRow.patient_id and ThisRow.rowNum=(PrevRow.rowNum+1)
where ThisRow.rowNum=rowNum order by ThisRow.patient_id, ThisRow.rowNum
) u on u.uuid = t.uuid
SET t.originalRegimen=u.prevRowResultingRegimen,
t.resultingRegimen=IF(CONVERT(REPLACE(TRIM(BOTH ',' FROM openmrs.process_regimen_switch(u.prevRowResultingRegimen, CONCAT("(", t.DiscontinuedRegimen, ")"), '', TRUE, 0, 0)),",,", "," ) USING utf8) <> ''
AND u.startedRegimen <> '',
concat_ws(",", u.startedRegimen, CONVERT(REPLACE(TRIM(BOTH ',' FROM openmrs.process_regimen_switch(u.prevRowResultingRegimen, CONCAT("(", t.DiscontinuedRegimen, ")"), '', TRUE, 0, 0)),",,", "," ) USING utf8)),
u.prevRowResultingRegimen
);


COMMIT;

END;
$$

-- creating tmp table to hold regimen events. rows are numbered to track which event came first for procedural processing

DROP PROCEDURE IF EXISTS sp_create_regimen_switch_tmp_table$$
CREATE PROCEDURE sp_create_drug_order_events_tmp_table()
BEGIN
-- creating numbered rows for regimen change
drop table if exists tmp_regimen_events_ordered;
create table tmp_regimen_events_ordered as
SELECT
uuid,
patient_id,
date_started,
originalRegimen,
DiscontinuedRegimen,
startedRegimen,
resultingRegimen,
@x:=IF(@same_value=patient_id,@x+1,1) as rowNum,
@same_value:=patient_id as dummy
FROM
(
SELECT
ThisRow.uuid,
ThisRow.date_started,
ThisRow.patient_id,
PrevRow.regimen originalRegimen,
REPLACE(PrevRow.regimen_discontinued, ",","|") DiscontinuedRegimen,
ThisRow.regimen startedRegimen,
concat_ws(",", PrevRow.regimen, ThisRow.regimen) as resultingRegimen
FROM
etl_drug_event    AS ThisRow
LEFT JOIN
etl_drug_event    AS PrevRow
ON  PrevRow.patient_id   = ThisRow.patient_id
AND PrevRow.date_started = (SELECT MAX(date_started)
FROM kenyaemr_etl.etl_drug_event
WHERE patient_id  = ThisRow.patient_id
AND date_started < ThisRow.date_started) order by patient_id, date_started
) u,
(SELECT  @x:=0, @same_value:='') t
ORDER BY patient_id, date_started;

END;
$$
