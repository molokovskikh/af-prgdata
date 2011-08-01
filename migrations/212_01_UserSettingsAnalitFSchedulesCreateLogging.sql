
CREATE TABLE  `logs`.`AnalitFScheduleLogs` (
  `Id` int unsigned NOT NULL AUTO_INCREMENT,
  `LogTime` datetime NOT NULL,
  `OperatorName` varchar(50) NOT NULL,
  `OperatorHost` varchar(50) NOT NULL,
  `Operation` tinyint(3) unsigned NOT NULL,
  `ScheduleId` int(10) unsigned not null,
  `ClientId` int(10) unsigned,
  `Enable` tinyint(1) unsigned,
  `Hour` int(10) unsigned,
  `Minute` int(10) unsigned,

  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=cp1251;

DROP TRIGGER IF EXISTS UserSettings.AnalitFScheduleLogDelete; 
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER UserSettings.AnalitFScheduleLogDelete AFTER DELETE ON UserSettings.AnalitFSchedules
FOR EACH ROW BEGIN
	INSERT 
	INTO `logs`.AnalitFScheduleLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 2,
		ScheduleId = OLD.Id,
		ClientId = OLD.ClientId,
		Enable = OLD.Enable,
		Hour = OLD.Hour,
		Minute = OLD.Minute;
END;

DROP TRIGGER IF EXISTS UserSettings.AnalitFScheduleLogUpdate; 
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER UserSettings.AnalitFScheduleLogUpdate AFTER UPDATE ON UserSettings.AnalitFSchedules
FOR EACH ROW BEGIN
	INSERT 
	INTO `logs`.AnalitFScheduleLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 1,
		ScheduleId = OLD.Id,
		ClientId = NULLIF(NEW.ClientId, OLD.ClientId),
		Enable = NULLIF(NEW.Enable, OLD.Enable),
		Hour = NULLIF(NEW.Hour, OLD.Hour),
		Minute = NULLIF(NEW.Minute, OLD.Minute);
END;

DROP TRIGGER IF EXISTS UserSettings.AnalitFScheduleLogInsert; 
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER UserSettings.AnalitFScheduleLogInsert AFTER INSERT ON UserSettings.AnalitFSchedules
FOR EACH ROW BEGIN
	INSERT 
	INTO `logs`.AnalitFScheduleLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 0,
		ScheduleId = NEW.Id,
		ClientId = NEW.ClientId,
		Enable = NEW.Enable,
		Hour = NEW.Hour,
		Minute = NEW.Minute;
END;

