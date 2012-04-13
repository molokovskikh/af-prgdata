DROP TRIGGER IF EXISTS Customers.UserPricesBeforeInsert;

CREATE
DEFINER=`RootDBMS`@`127.0.0.1`
TRIGGER `Customers`.`UserPricesBeforeInsert`
BEFORE INSERT ON `Customers`.`userprices`
FOR EACH ROW
BEGIN

	update 
		Usersettings.AnalitFReplicationInfo ar
		join Usersettings.PricesData pd on pd.FirmCode = ar.FirmCode
	set ForceReplication = 1
	where ar.UserId = NEW.UserId and pd.PriceCode = NEW.PriceId;

	update 
		Customers.Users u
		join Usersettings.AnalitFReplicationInfo ar on ar.UserId = u.Id
		join Usersettings.PricesData pd on pd.FirmCode = ar.FirmCode and pd.PriceCode = NEW.PriceId
	set ForceReplication = 1
	where 
		u.InheritPricesFrom = NEW.UserId;
END;
