DROP TRIGGER IF EXISTS Customers.IntersectionLogUpdate;
CREATE
	DEFINER = 'RootDBMS'@'127.0.0.1'
TRIGGER Customers.IntersectionLogUpdate
	AFTER UPDATE
	ON customers.intersection
	FOR EACH ROW
BEGIN
  IF @INUser != 'ProcessingSVC' THEN
  INSERT INTO `logs`.IntersectionLogs
  SET
    LogTime = now(), OperatorName = ifnull(@INUser, substring_index(user(), '@', 1)), OperatorHost = ifnull(@INHost, substring_index(user(), '@', -1)), Operation = 1, IntersectionId = OLD.Id, ClientId = nullif(NEW.ClientId, OLD.ClientId), RegionId = nullif(NEW.RegionId, OLD.RegionId), PriceId = nullif(NEW.PriceId, OLD.PriceId), LegalEntityId = nullif(NEW.LegalEntityId, OLD.LegalEntityId), CostId = nullif(NEW.CostId, OLD.CostId), AvailableForClient = nullif(NEW.AvailableForClient, OLD.AvailableForClient), AgencyEnabled = nullif(NEW.AgencyEnabled, OLD.AgencyEnabled), PriceMarkup = nullif(NEW.PriceMarkup, OLD.PriceMarkup), SupplierClientId = nullif(NEW.SupplierClientId, OLD.SupplierClientId), SupplierPaymentId = nullif(NEW.SupplierPaymentId, OLD.SupplierPaymentId);
END IF;
END;