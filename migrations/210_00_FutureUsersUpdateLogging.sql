
alter table Logs.UserLogs
add column PromoFileLimit int(10) unsigned,
add column RootService int(10) unsigned,
add column AllowDownloadUnconfirmedOrders tinyint(1) unsigned,
add column ShowSupplierCost tinyint(1) unsigned
;

DROP TRIGGER IF EXISTS Future.UserLogDelete; 
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER Future.UserLogDelete AFTER DELETE ON Future.Users
FOR EACH ROW BEGIN
	INSERT 
	INTO `logs`.UserLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 2,
		UserId = OLD.Id,
		ClientId = OLD.ClientId,
		Enabled = OLD.Enabled,
		Login = OLD.Login,
		Name = OLD.Name,
		SendRejects = OLD.SendRejects,
		SendWaybills = OLD.SendWaybills,
		SubmitOrders = OLD.SubmitOrders,
		EnableUpdate = OLD.EnableUpdate,
		Auditor = OLD.Auditor,
		InheritPricesFrom = OLD.InheritPricesFrom,
		Registrant = OLD.Registrant,
		ContactGroupId = OLD.ContactGroupId,
		RegistrationDate = OLD.RegistrationDate,
		WorkRegionMask = OLD.WorkRegionMask,
		OrderRegionMask = OLD.OrderRegionMask,
		Free = OLD.Free,
		AccountingId = OLD.AccountingId,
		PayerId = OLD.PayerId,
		SaveAFDataFiles = OLD.SaveAFDataFiles,
		TargetVersion = OLD.TargetVersion,
		UseAdjustmentOrders = OLD.UseAdjustmentOrders,
		PromoFileLimit = OLD.PromoFileLimit,
		RootService = OLD.RootService,
		AllowDownloadUnconfirmedOrders = OLD.AllowDownloadUnconfirmedOrders,
		ShowSupplierCost = OLD.ShowSupplierCost;
END;

DROP TRIGGER IF EXISTS Future.UserLogUpdate; 
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER Future.UserLogUpdate AFTER UPDATE ON Future.Users
FOR EACH ROW BEGIN
	INSERT 
	INTO `logs`.UserLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 1,
		UserId = OLD.Id,
		ClientId = NULLIF(NEW.ClientId, OLD.ClientId),
		Enabled = NULLIF(NEW.Enabled, OLD.Enabled),
		Login = NULLIF(NEW.Login, OLD.Login),
		Name = NULLIF(NEW.Name, OLD.Name),
		SendRejects = NULLIF(NEW.SendRejects, OLD.SendRejects),
		SendWaybills = NULLIF(NEW.SendWaybills, OLD.SendWaybills),
		SubmitOrders = NULLIF(NEW.SubmitOrders, OLD.SubmitOrders),
		EnableUpdate = NULLIF(NEW.EnableUpdate, OLD.EnableUpdate),
		Auditor = NULLIF(NEW.Auditor, OLD.Auditor),
		InheritPricesFrom = NULLIF(NEW.InheritPricesFrom, OLD.InheritPricesFrom),
		Registrant = NULLIF(NEW.Registrant, OLD.Registrant),
		ContactGroupId = NULLIF(NEW.ContactGroupId, OLD.ContactGroupId),
		RegistrationDate = NULLIF(NEW.RegistrationDate, OLD.RegistrationDate),
		WorkRegionMask = NULLIF(NEW.WorkRegionMask, OLD.WorkRegionMask),
		OrderRegionMask = NULLIF(NEW.OrderRegionMask, OLD.OrderRegionMask),
		Free = NULLIF(NEW.Free, OLD.Free),
		AccountingId = NULLIF(NEW.AccountingId, OLD.AccountingId),
		PayerId = NULLIF(NEW.PayerId, OLD.PayerId),
		SaveAFDataFiles = NULLIF(NEW.SaveAFDataFiles, OLD.SaveAFDataFiles),
		TargetVersion = NULLIF(NEW.TargetVersion, OLD.TargetVersion),
		UseAdjustmentOrders = NULLIF(NEW.UseAdjustmentOrders, OLD.UseAdjustmentOrders),
		PromoFileLimit = NULLIF(NEW.PromoFileLimit, OLD.PromoFileLimit),
		RootService = NULLIF(NEW.RootService, OLD.RootService),
		AllowDownloadUnconfirmedOrders = NULLIF(NEW.AllowDownloadUnconfirmedOrders, OLD.AllowDownloadUnconfirmedOrders),
		ShowSupplierCost = NULLIF(NEW.ShowSupplierCost, OLD.ShowSupplierCost);
END;

DROP TRIGGER IF EXISTS Future.UserLogInsert; 
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER Future.UserLogInsert AFTER INSERT ON Future.Users
FOR EACH ROW BEGIN
	INSERT 
	INTO `logs`.UserLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 0,
		UserId = NEW.Id,
		ClientId = NEW.ClientId,
		Enabled = NEW.Enabled,
		Login = NEW.Login,
		Name = NEW.Name,
		SendRejects = NEW.SendRejects,
		SendWaybills = NEW.SendWaybills,
		SubmitOrders = NEW.SubmitOrders,
		EnableUpdate = NEW.EnableUpdate,
		Auditor = NEW.Auditor,
		InheritPricesFrom = NEW.InheritPricesFrom,
		Registrant = NEW.Registrant,
		ContactGroupId = NEW.ContactGroupId,
		RegistrationDate = NEW.RegistrationDate,
		WorkRegionMask = NEW.WorkRegionMask,
		OrderRegionMask = NEW.OrderRegionMask,
		Free = NEW.Free,
		AccountingId = NEW.AccountingId,
		PayerId = NEW.PayerId,
		SaveAFDataFiles = NEW.SaveAFDataFiles,
		TargetVersion = NEW.TargetVersion,
		UseAdjustmentOrders = NEW.UseAdjustmentOrders,
		PromoFileLimit = NEW.PromoFileLimit,
		RootService = NEW.RootService,
		AllowDownloadUnconfirmedOrders = NEW.AllowDownloadUnconfirmedOrders,
		ShowSupplierCost = NEW.ShowSupplierCost;
END;

