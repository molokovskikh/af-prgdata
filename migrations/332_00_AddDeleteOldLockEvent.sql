CREATE 
	DEFINER = 'RootDBMS'@'127.0.0.1'
EVENT logs.DeleteOldLocks
	ON SCHEDULE EVERY '15' MINUTE
	STARTS '2011-11-28 15:30:00'
	ON COMPLETION PRESERVE
	DO 
BEGIN
  delete from Logs.PrgDataLogs where StartTime < now() - interval 20 minute;
END;