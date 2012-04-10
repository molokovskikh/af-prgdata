
CREATE TABLE  `logs`.`RetailMarginLogs` (
  `Id` int unsigned NOT NULL AUTO_INCREMENT,
  `LogTime` datetime NOT NULL,
  `OperatorName` varchar(50) NOT NULL,
  `OperatorHost` varchar(50) NOT NULL,
  `Operation` tinyint(3) unsigned NOT NULL,
  `MarginId` int(10) unsigned not null,
  `ClientId` int(10) unsigned,
  `CatalogId` int(10) unsigned,
  `Markup` decimal(5,3),
  `MaxMarkup` decimal(5,3),
  `MaxSupplierMarkup` decimal(5,3),
  `UpdateTime` timestamp,

  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=cp1251;

DROP TRIGGER IF EXISTS UserSettings.RetailMarginLogDelete; 
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER UserSettings.RetailMarginLogDelete AFTER DELETE ON UserSettings.RetailMargins
FOR EACH ROW BEGIN
	INSERT 
	INTO `logs`.RetailMarginLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 2,
		MarginId = OLD.Id,
		ClientId = OLD.ClientId,
		CatalogId = OLD.CatalogId,
		Markup = OLD.Markup,
		MaxMarkup = OLD.MaxMarkup,
		MaxSupplierMarkup = OLD.MaxSupplierMarkup,
		UpdateTime = OLD.UpdateTime;
END;

DROP TRIGGER IF EXISTS UserSettings.RetailMarginLogUpdate; 
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER UserSettings.RetailMarginLogUpdate AFTER UPDATE ON UserSettings.RetailMargins
FOR EACH ROW BEGIN
	INSERT 
	INTO `logs`.RetailMarginLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 1,
		MarginId = OLD.Id,
		ClientId = NULLIF(NEW.ClientId, OLD.ClientId),
		CatalogId = NULLIF(NEW.CatalogId, OLD.CatalogId),
		Markup = NULLIF(NEW.Markup, OLD.Markup),
		MaxMarkup = NULLIF(NEW.MaxMarkup, OLD.MaxMarkup),
		MaxSupplierMarkup = NULLIF(NEW.MaxSupplierMarkup, OLD.MaxSupplierMarkup),
		UpdateTime = NULLIF(NEW.UpdateTime, OLD.UpdateTime);
END;

DROP TRIGGER IF EXISTS UserSettings.RetailMarginLogInsert; 
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER UserSettings.RetailMarginLogInsert AFTER INSERT ON UserSettings.RetailMargins
FOR EACH ROW BEGIN
	INSERT 
	INTO `logs`.RetailMarginLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 0,
		MarginId = NEW.Id,
		ClientId = NEW.ClientId,
		CatalogId = NEW.CatalogId,
		Markup = NEW.Markup,
		MaxMarkup = NEW.MaxMarkup,
		MaxSupplierMarkup = NEW.MaxSupplierMarkup,
		UpdateTime = NEW.UpdateTime;
END;

