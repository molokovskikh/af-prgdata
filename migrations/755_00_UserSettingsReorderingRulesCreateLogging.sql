
CREATE TABLE  `logs`.`ReorderingRuleLogs` (
  `Id` int unsigned NOT NULL AUTO_INCREMENT,
  `LogTime` datetime NOT NULL,
  `OperatorName` varchar(50) NOT NULL,
  `OperatorHost` varchar(50) NOT NULL,
  `Operation` tinyint(3) unsigned NOT NULL,
  `RuleId` int(10) unsigned not null,
  `RegionalDataId` int(10) unsigned,
  `DayOfWeek` enum('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'),
  `TimeOfStopsOrders` bigint(20),

  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=cp1251;

DROP TRIGGER IF EXISTS UserSettings.ReorderingRuleLogDelete;
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER UserSettings.ReorderingRuleLogDelete AFTER DELETE ON UserSettings.ReorderingRules
FOR EACH ROW BEGIN
	INSERT
	INTO `logs`.ReorderingRuleLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 2,
		RuleId = OLD.Id,
		RegionalDataId = OLD.RegionalDataId,
		DayOfWeek = OLD.DayOfWeek,
		TimeOfStopsOrders = OLD.TimeOfStopsOrders;
END;

DROP TRIGGER IF EXISTS UserSettings.ReorderingRuleLogUpdate;
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER UserSettings.ReorderingRuleLogUpdate AFTER UPDATE ON UserSettings.ReorderingRules
FOR EACH ROW BEGIN
	INSERT
	INTO `logs`.ReorderingRuleLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 1,
		RuleId = OLD.Id,
		RegionalDataId = NULLIF(NEW.RegionalDataId, OLD.RegionalDataId),
		DayOfWeek = NULLIF(NEW.DayOfWeek, OLD.DayOfWeek),
		TimeOfStopsOrders = NULLIF(NEW.TimeOfStopsOrders, OLD.TimeOfStopsOrders);
END;

DROP TRIGGER IF EXISTS UserSettings.ReorderingRuleLogInsert;
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER UserSettings.ReorderingRuleLogInsert AFTER INSERT ON UserSettings.ReorderingRules
FOR EACH ROW BEGIN
	INSERT
	INTO `logs`.ReorderingRuleLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 0,
		RuleId = NEW.Id,
		RegionalDataId = NEW.RegionalDataId,
		DayOfWeek = NEW.DayOfWeek,
		TimeOfStopsOrders = NEW.TimeOfStopsOrders;
END;