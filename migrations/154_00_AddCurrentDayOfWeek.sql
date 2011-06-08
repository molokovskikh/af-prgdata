DROP FUNCTION IF EXISTS UserSettings.`CurrentDayOfWeek`;

CREATE 
  DEFINER = `RootDBMS`@`127.0.0.1`
FUNCTION UserSettings.CurrentDayOfWeek()
  RETURNS varchar(10) CHARSET cp1251
  READS SQL DATA
BEGIN
  SET @dayWeek = dayofweek(curdate());
  SET @result =
              CASE @dayWeek
              WHEN 1 THEN
                'Sunday'
              WHEN 2 THEN
                'Monday'
              WHEN 3 THEN
                'Tuesday'
              WHEN 4 THEN
                'Wednesday'
              WHEN 5 THEN
                'Thursday'
              WHEN 6 THEN
                'Friday'
              ELSE
                'Saturday'
              END;
  RETURN @result;
END;
