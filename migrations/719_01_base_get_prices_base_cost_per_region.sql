DROP PROCEDURE Customers.BaseGetPrices;
CREATE DEFINER=`RootDBMS`@`127.0.0.1` PROCEDURE Customers.`BaseGetPrices`(IN UserIdParam INT UNSIGNED, IN AddressIdParam INT UNSIGNED)
BEGIN

set @currentDay = usersettings.CurrentDayOfWeek();
drop temporary table IF EXISTS Customers.BasePrices;
create temporary table
Customers.BasePrices
(
 FirmCode int Unsigned,
 PriceCode int Unsigned,
 CostCode int Unsigned,
 PriceSynonymCode int Unsigned,
 RegionCode BigInt Unsigned,
 DelayOfPayment decimal(5,3),
 DisabledByClient bool,
 Upcost decimal(7,5),
 Actual bool,
 CostType bool,
 PriceDate DateTime,
 ShowPriceName bool,
 PriceName VarChar(50),
 PositionCount int Unsigned,
 MinReq mediumint,
 ControlMinReq int Unsigned,
 AllowOrder bool,
 ShortName varchar(50),
 FirmCategory tinyint unsigned,
 MainFirm bool,
 Storage bool,
 VitallyImportantDelay decimal(5,3),
 OtherDelay decimal(5,3),
 index (PriceCode),
 index (RegionCode)
)engine = MEMORY;

INSERT
INTO    Customers.BasePrices
select  Pd.firmcode,
        i.PriceId,
        if(r.InvisibleOnFirm = 0, i.CostId, ifnull(prd.BaseCost, pc.CostCode)),
        ifnull(pd.ParentSynonym, pd.pricecode) PriceSynonymCode,
        i.RegionId,
        0 as DelayOfPayment,
        if(up.PriceId is null, 1, 0),
        round((1 + pd.UpCost / 100) * (1 + prd.UpCost / 100) * (1 + i.PriceMarkup / 100), 5),
        (to_seconds(now()) - to_seconds(pi.PriceDate)) < (f.maxold * 86400),
        pd.CostType,
        pi.PriceDate,
        r.ShowPriceName,
        pd.PriceName,
        pi.RowCount,
        if(ai.Id is not null, if (ai.MinReq > 0, ai.MinReq, prd.MinReq), prd.MinReq),
        if(ai.Id is not null, if (ai.ControlMinReq, 1, 0), 0),
        (r.OrderRegionMask & i.RegionId & u.OrderRegionMask) > 0,
        supplier.Name as ShortName,
        si.SupplierCategory,
        si.SupplierCategory >= r.BaseFirmCategory,
        Storage,
        dop.VitallyImportantDelay,
        dop.OtherDelay
from customers.Users u
  join Customers.Addresses adr on adr.Id = AddressIdParam
  join Customers.Intersection i on i.ClientId = u.ClientId and i.LegalEntityId = Adr.LegalEntityId
  join Customers.AddressIntersection ai ON ai.IntersectionId = i.Id AND ai.addressid = adr.Id
  join Customers.Clients drugstore ON drugstore.Id = i.ClientId
  join usersettings.RetClientsSet r ON r.clientcode = drugstore.Id
  join usersettings.PricesData pd ON pd.pricecode = i.PriceId
    join usersettings.SupplierIntersection si on si.SupplierId = pd.FirmCode and i.ClientId = si.ClientId
    join usersettings.PriceIntersections pinter on pinter.SupplierIntersectionId = si.Id and pinter.PriceId = pd.PriceCode
    join usersettings.DelayOfPayments dop on dop.PriceIntersectionId = pinter.Id and dop.DayOfWeek = @currentDay
  JOIN usersettings.PricesCosts pc on pc.PriceCode = i.PriceId and pc.BaseCost = 1
    join usersettings.PriceItems pi on pi.Id = pc.PriceItemId
    join farm.FormRules f on f.Id = pi.FormRuleId
    join Customers.Suppliers supplier ON supplier.Id = pd.firmcode
    join usersettings.PricesRegionalData prd ON prd.regioncode = i.RegionId AND prd.pricecode = pd.pricecode
    join usersettings.RegionalData rd ON rd.RegionCode = i.RegionId AND rd.FirmCode = pd.firmcode
  left join Customers.UserPrices up on up.PriceId = i.PriceId and up.UserId = ifnull(u.InheritPricesFrom, u.Id) and up.RegionId = i.RegionId
where   supplier.Disabled = 0
    AND (supplier.RegionMask & i.RegionId) > 0
    and (drugstore.maskregion & i.RegionId & u.WorkRegionMask) > 0
    and (r.WorkRegionMask & i.RegionId) > 0
    and pd.agencyenabled = 1
    and pd.enabled = 1
    and pd.pricetype <> 1
    and prd.enabled = 1
    and if(not r.ServiceClient, supplier.Id != 234, 1)
    AND i.AvailableForClient = 1
	AND i.AgencyEnabled = 1
    and u.Id = UserIdParam
group by PriceId, RegionId;

END;
