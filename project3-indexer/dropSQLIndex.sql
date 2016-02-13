USE CS144;
DELIMITER //
DROP PROCEDURE IF EXISTS removespatialtables;
CREATE PROCEDURE removespatialtables()
BEGIN
	DECLARE v_finished INTEGER DEFAULT 0;
	DECLARE v_name varchar(200) DEFAULT "";
	DECLARE name_cursor CURSOR FOR
		SELECT TABLE_NAME FROM INFORMATION_SCHEMA.statistics WHERE INDEX_TYPE='SPATIAL';
	DECLARE CONTINUE HANDLER
	FOR NOT FOUND SET v_finished = 1;
	OPEN name_cursor;
		remove_tables: LOOP FETCH name_cursor INTO v_name;
			IF v_finished = 1 THEN
				LEAVE remove_tables;
			END IF;
			SET @statement = CONCAT("DROP TABLE IF EXISTS ", v_name);
			PREPARE stmt FROM @statement;
			EXECUTE stmt;
		END LOOP remove_tables;
	CLOSE name_cursor;
END//
DELIMITER ;
CALL removespatialtables();
