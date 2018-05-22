SELECT
originalRegimen, DiscontinuedRegimen, startedRegimen, resultingRegimen,
TRIM(CASE WHEN resultingRegimen LIKE CONCAT("%,", DiscontinuedRegimen,",%")   -- In the middle
                           THEN REPLACE(resultingRegimen, CONCAT(',', DiscontinuedRegimen,','), ',')
                      WHEN resultingRegimen LIKE CONCAT(DiscontinuedRegimen, ",%" )    -- At the beginning
                           THEN REPLACE(resultingRegimen, CONCAT(DiscontinuedRegimen,','), '')
                      WHEN resultingRegimen LIKE CONCAT("%,", DiscontinuedRegimen)    -- At the end
                           THEN REPLACE(resultingRegimen, CONCAT(',', DiscontinuedRegimen), '')
                      WHEN resultingRegimen = DiscontinuedRegimen         -- At whole
                           THEN ''
                      ELSE resultingRegimen
                 END) refinedRegimen
from (
SELECT
  ThisRow.patient_id,
  PrevRow.regimen originalRegimen,
  PrevRow.regimen_discontinued DiscontinuedRegimen,
  ThisRow.regimen startedRegimen,
  concat_ws(",", PrevRow.regimen, ThisRow.regimen) as resultingRegimen
FROM
  etl_drug_event    AS ThisRow
LEFT JOIN
  etl_drug_event    AS PrevRow
    ON  PrevRow.patient_id   = ThisRow.patient_id
    AND PrevRow.date_started = (SELECT MAX(date_started)
                          FROM etl_drug_event
                         WHERE patient_id  = ThisRow.patient_id
                           AND date_started < ThisRow.date_started) order by patient_id) a
                           where DiscontinuedRegimen