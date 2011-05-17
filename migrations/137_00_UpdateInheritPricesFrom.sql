DROP TRIGGER IF EXISTS future.UserPricesBeforeInsert;
DROP TRIGGER IF EXISTS future.UserPricesBeforeDelete;


CREATE
DEFINER=`RootDBMS`@`127.0.0.1`
TRIGGER `future`.`UserPricesBeforeInsert`
BEFORE INSERT ON `future`.`userprices`
FOR EACH ROW
BEGIN

	update 
		Usersettings.AnalitFReplicationInfo ar
		join Usersettings.PricesData pd on pd.FirmCode = ar.FirmCode
	set ForceReplication = 1
	where ar.UserId = NEW.UserId and pd.PriceCode = NEW.PriceId;

	update 
		future.Users u
		join Usersettings.AnalitFReplicationInfo ar on ar.UserId = u.Id
		join Usersettings.PricesData pd on pd.FirmCode = ar.FirmCode and pd.PriceCode = NEW.PriceId
	set ForceReplication = 1
	where 
		u.InheritPricesFrom = NEW.UserId;
END;

CREATE
DEFINER=`RootDBMS`@`127.0.0.1`
TRIGGER `future`.`UserPricesBeforeDelete`
BEFORE DELETE ON `future`.`userprices`
FOR EACH ROW
BEGIN

	update 
		Usersettings.AnalitFReplicationInfo ar
		join Usersettings.PricesData pd on pd.FirmCode = ar.FirmCode
	set ForceReplication = 1
	where ar.UserId = OLD.UserId and pd.PriceCode = OLD.PriceId;

	update 
		future.Users u
		join Usersettings.AnalitFReplicationInfo ar on ar.UserId = u.Id
		join Usersettings.PricesData pd on pd.FirmCode = ar.FirmCode and pd.PriceCode = OLD.PriceId
	set ForceReplication = 1
	where 
		u.InheritPricesFrom = OLD.UserId;
END;