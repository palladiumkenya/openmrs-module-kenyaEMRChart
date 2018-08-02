SET @OLD_SQL_MODE=@@SQL_MODE$$
SET SQL_MODE=''$$

DROP FUNCTION IF EXISTS process_regimen_switch$$
CREATE FUNCTION process_regimen_switch(subject VARCHAR(21845), pattern VARCHAR(21845),
                       replacement VARCHAR(21845), greedy BOOLEAN, minMatchLen INT, maxMatchLen INT)
RETURNS VARCHAR(21845) DETERMINISTIC BEGIN
DECLARE result, subStr, usePattern VARCHAR(21845);
DECLARE startPos, prevStartPos, startInc, len, lenInc INT;
-- origin: http://stevettt.blogspot.co.ke/2018/02/a-mysql-regular-expression-replace.html
IF subject REGEXP pattern THEN
SET result = '';
-- Sanitize input parameter values
SET minMatchLen = IF(minMatchLen < 1, 1, minMatchLen);
SET maxMatchLen = IF(maxMatchLen < 1 OR maxMatchLen > CHAR_LENGTH(subject),
         CHAR_LENGTH(subject), maxMatchLen);
-- Set the pattern to use to match an entire string rather than part of a string
SET usePattern = IF (LEFT(pattern, 1) = '^', pattern, CONCAT('^', pattern));
SET usePattern = IF (RIGHT(pattern, 1) = '$', usePattern, CONCAT(usePattern, '$'));
-- Set start position to 1 if pattern starts with ^ or doesn't end with $.
IF LEFT(pattern, 1) = '^' OR RIGHT(pattern, 1) <> '$' THEN
SET startPos = 1, startInc = 1;
-- Otherwise (i.e. pattern ends with $ but doesn't start with ^): Set start pos
-- to the min or max match length from the end (depending on "greedy" flag).
ELSEIF greedy THEN
SET startPos = CHAR_LENGTH(subject) - maxMatchLen + 1, startInc = 1;
ELSE
SET startPos = CHAR_LENGTH(subject) - minMatchLen + 1, startInc = -1;
END IF;
WHILE startPos >= 1 AND startPos <= CHAR_LENGTH(subject)
AND startPos + minMatchLen - 1 <= CHAR_LENGTH(subject)
AND !(LEFT(pattern, 1) = '^' AND startPos <> 1)
AND !(RIGHT(pattern, 1) = '$'
AND startPos + maxMatchLen - 1 < CHAR_LENGTH(subject)) DO
-- Set start length to maximum if matching greedily or pattern ends with $.
-- Otherwise set starting length to the minimum match length.
IF greedy OR RIGHT(pattern, 1) = '$' THEN
SET len = LEAST(CHAR_LENGTH(subject) - startPos + 1, maxMatchLen), lenInc = -1;
ELSE
SET len = minMatchLen, lenInc = 1;
END IF;
SET prevStartPos = startPos;
lenLoop: WHILE len >= 1 AND len <= maxMatchLen
     AND startPos + len - 1 <= CHAR_LENGTH(subject)
     AND !(RIGHT(pattern, 1) = '$'
           AND startPos + len - 1 <> CHAR_LENGTH(subject)) DO
SET subStr = SUBSTRING(subject, startPos, len);
IF subStr REGEXP usePattern THEN
SET result = IF(startInc = 1,
          CONCAT(result, replacement), CONCAT(replacement, result));
SET startPos = startPos + startInc * len;
LEAVE lenLoop;
END IF;
SET len = len + lenInc;
END WHILE;
IF (startPos = prevStartPos) THEN
SET result = IF(startInc = 1, CONCAT(result, SUBSTRING(subject, startPos, 1)),
        CONCAT(SUBSTRING(subject, startPos, 1), result));
SET startPos = startPos + startInc;
END IF;
END WHILE;
IF startInc = 1 AND startPos <= CHAR_LENGTH(subject) THEN
SET result = CONCAT(result, RIGHT(subject, CHAR_LENGTH(subject) + 1 - startPos));
ELSEIF startInc = -1 AND startPos >= 1 THEN
SET result = CONCAT(LEFT(subject, startPos), result);
END IF;
ELSE
SET result = subject;
END IF;
RETURN result;
END$$

SET sql_mode=@OLD_SQL_MODE$$




