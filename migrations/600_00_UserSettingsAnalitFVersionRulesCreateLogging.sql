CREATE TABLE  `logs`.`AnalitFVersionRuleLogs` (
  `Id` int unsigned NOT NULL AUTO_INCREMENT,
  `LogTime` datetime NOT NULL,
  `OperatorName` varchar(50) NOT NULL,
  `OperatorHost` varchar(50) NOT NULL,
  `Operation` tinyint(3) unsigned NOT NULL,
  `RuleId` int(11) not null,
  `SourceVersion` mediumint(8) unsigned,
  `DestinationVersion` mediumint(8) unsigned,

  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=cp1251;

DROP TRIGGER IF EXISTS UserSettings.AnalitFVersionRuleLogDelete;
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER UserSettings.AnalitFVersionRuleLogDelete AFTER DELETE ON UserSettings.AnalitFVersionRules
FOR EACH ROW BEGIN
	INSERT
	INTO `logs`.AnalitFVersionRuleLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 2,
		RuleId = OLD.Id,
		SourceVersion = OLD.SourceVersion,
		DestinationVersion = OLD.DestinationVersion;
END;

DROP TRIGGER IF EXISTS UserSettings.AnalitFVersionRuleLogUpdate;
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER UserSettings.AnalitFVersionRuleLogUpdate AFTER UPDATE ON UserSettings.AnalitFVersionRules
FOR EACH ROW BEGIN
	INSERT
	INTO `logs`.AnalitFVersionRuleLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 1,
		RuleId = OLD.Id,
		SourceVersion = NULLIF(NEW.SourceVersion, OLD.SourceVersion),
		DestinationVersion = NULLIF(NEW.DestinationVersion, OLD.DestinationVersion);
END;

DROP TRIGGER IF EXISTS UserSettings.AnalitFVersionRuleLogInsert;
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER UserSettings.AnalitFVersionRuleLogInsert AFTER INSERT ON UserSettings.AnalitFVersionRules
FOR EACH ROW BEGIN
	INSERT
	INTO `logs`.AnalitFVersionRuleLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 0,
		RuleId = NEW.Id,
		SourceVersion = NEW.SourceVersion,
		DestinationVersion = NEW.DestinationVersion;
END;