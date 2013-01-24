DROP TRIGGER IF EXISTS Customers.AddressIntersectionLogDelete;
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER Customers.AddressIntersectionLogDelete AFTER DELETE ON Customers.AddressIntersection
FOR EACH ROW BEGIN
	INSERT
	INTO `logs`.AddressIntersectionLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 2,
		AddressIntersectionId = OLD.Id,
		AddressId = OLD.AddressId,
		IntersectionId = OLD.IntersectionId,
		SupplierDeliveryId = OLD.SupplierDeliveryId,
		ControlMinReq = OLD.ControlMinReq,
		MinReq = OLD.MinReq;
END;

DROP TRIGGER IF EXISTS Customers.AddressIntersectionLogUpdate;
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER Customers.AddressIntersectionLogUpdate AFTER UPDATE ON Customers.AddressIntersection
FOR EACH ROW BEGIN
	INSERT
	INTO `logs`.AddressIntersectionLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 1,
		AddressIntersectionId = OLD.Id,
		AddressId = NULLIF(NEW.AddressId, OLD.AddressId),
		IntersectionId = NULLIF(NEW.IntersectionId, OLD.IntersectionId),
		SupplierDeliveryId = NULLIF(NEW.SupplierDeliveryId, OLD.SupplierDeliveryId),
		ControlMinReq = NULLIF(NEW.ControlMinReq, OLD.ControlMinReq),
		MinReq = NULLIF(NEW.MinReq, OLD.MinReq);
END;

DROP TRIGGER IF EXISTS Customers.AddressIntersectionLogInsert;
CREATE DEFINER = RootDBMS@127.0.0.1 TRIGGER Customers.AddressIntersectionLogInsert AFTER INSERT ON Customers.AddressIntersection
FOR EACH ROW BEGIN
	INSERT
	INTO `logs`.AddressIntersectionLogs
	SET LogTime = now(),
		OperatorName = IFNULL(@INUser, SUBSTRING_INDEX(USER(),'@',1)),
		OperatorHost = IFNULL(@INHost, SUBSTRING_INDEX(USER(),'@',-1)),
		Operation = 0,
		AddressIntersectionId = NEW.Id,
		AddressId = NEW.AddressId,
		IntersectionId = NEW.IntersectionId,
		SupplierDeliveryId = NEW.SupplierDeliveryId,
		ControlMinReq = NEW.ControlMinReq,
		MinReq = NEW.MinReq;
END;