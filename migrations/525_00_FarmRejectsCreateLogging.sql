
CREATE TABLE  `logs`.`RejectLogs` (
  `Id` int unsigned NOT NULL AUTO_INCREMENT,
  `LogTime` datetime NOT NULL,
  `OperatorName` varchar(50) NOT NULL,
  `OperatorHost` varchar(50) NOT NULL,
  `Operation` tinyint(3) unsigned NOT NULL,
  `RejectId` int(10) unsigned not null,
  `Product` varchar(255),
  `ProductId` int(10) unsigned,
  `Producer` varchar(255),
  `ProducerId` int(10) unsigned,
  `Series` varchar(255),
  `LetterNo` varchar(255),
  `LetterDate` datetime,
  `CauseRejects` varchar(255),
  `CancelDate` datetime,
  `UpdateTime` timestamp,

  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=cp1251;

DROP TRIGGER IF EXISTS Farm.RejectLogDelete; 
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER Farm.RejectLogDelete AFTER DELETE ON Farm.Rejects
FOR EACH ROW BEGIN
	INSERT 
	INTO `logs`.RejectLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 2,
		RejectId = OLD.Id,
		Product = OLD.Product,
		ProductId = OLD.ProductId,
		Producer = OLD.Producer,
		ProducerId = OLD.ProducerId,
		Series = OLD.Series,
		LetterNo = OLD.LetterNo,
		LetterDate = OLD.LetterDate,
		CauseRejects = OLD.CauseRejects,
		CancelDate = OLD.CancelDate,
		UpdateTime = OLD.UpdateTime;
END;

DROP TRIGGER IF EXISTS Farm.RejectLogUpdate; 
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER Farm.RejectLogUpdate AFTER UPDATE ON Farm.Rejects
FOR EACH ROW BEGIN
	INSERT 
	INTO `logs`.RejectLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 1,
		RejectId = OLD.Id,
		Product = NULLIF(NEW.Product, OLD.Product),
		ProductId = NULLIF(NEW.ProductId, OLD.ProductId),
		Producer = NULLIF(NEW.Producer, OLD.Producer),
		ProducerId = NULLIF(NEW.ProducerId, OLD.ProducerId),
		Series = NULLIF(NEW.Series, OLD.Series),
		LetterNo = NULLIF(NEW.LetterNo, OLD.LetterNo),
		LetterDate = NULLIF(NEW.LetterDate, OLD.LetterDate),
		CauseRejects = NULLIF(NEW.CauseRejects, OLD.CauseRejects),
		CancelDate = NULLIF(NEW.CancelDate, OLD.CancelDate),
		UpdateTime = NULLIF(NEW.UpdateTime, OLD.UpdateTime);
END;

DROP TRIGGER IF EXISTS Farm.RejectLogInsert; 
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER Farm.RejectLogInsert AFTER INSERT ON Farm.Rejects
FOR EACH ROW BEGIN
	INSERT 
	INTO `logs`.RejectLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 0,
		RejectId = NEW.Id,
		Product = NEW.Product,
		ProductId = NEW.ProductId,
		Producer = NEW.Producer,
		ProducerId = NEW.ProducerId,
		Series = NEW.Series,
		LetterNo = NEW.LetterNo,
		LetterDate = NEW.LetterDate,
		CauseRejects = NEW.CauseRejects,
		CancelDate = NEW.CancelDate,
		UpdateTime = NEW.UpdateTime;
END;

