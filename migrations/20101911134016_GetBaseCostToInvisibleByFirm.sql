DROP PROCEDURE IF EXISTS Future.`BaseGetPrices`; 

CREATE DEFINER=`RootDBMS`@`127.0.0.1` PROCEDURE Future.`BaseGetPrices`(IN UserIdParam INT UNSIGNED, IN AddressIdParam INT UNSIGNED)
BEGIN

drop temporary table IF EXISTS Future.BasePrices;
create temporary table
Future.BasePrices
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
 index (PriceCode),
 index (RegionCode)
)engine = MEMORY;

INSERT
INTO    Future.BasePrices
SELECT  pd.firmcode,
        i.PriceId,
        pc.CostCode,
        ifnull(pd.ParentSynonym, pd.pricecode) PriceSynonymCode,
        i.RegionId,
        si.DelayOfPayment,
        if(up.PriceId is null, 1, 0),
        round((1 + pd.UpCost / 100) * (1 + prd.UpCost / 100) * (1 + i.PriceMarkup / 100), 5),
        to_days(now()) - to_days(pi.PriceDate) < f.maxold,
        pd.CostType,
        pi.PriceDate,
        r.ShowPriceName,
        pd.PriceName,
        pi.RowCount,
        if(ai.Id is not null, if (ai.MinReq > 0, ai.MinReq, prd.MinReq), prd.MinReq),
        if(ai.Id is not null, if (ai.ControlMinReq, 1, 0), 0),
        (r.OrderRegionMask & i.RegionId & u.OrderRegionMask) > 0,
        supplier.ShortName,
        si.SupplierCategory,
        si.SupplierCategory >= r.BaseFirmCategory,
        Storage
FROM Future.Users u
  join Future.Intersection i on i.ClientId = u.ClientId and i.AgencyEnabled = 1
  join Future.Addresses adr on adr.Id = AddressIdParam
  join Future.Intersection IByAdr 
    on 
             IByAdr.ClientId = adr.ClientId 
       and IByAdr.RegionId = i.RegionId 
       and IByAdr.PriceId = I.PriceId 
       and IByAdr.LegalEntityId = Adr.LegalEntityId 
       and IByAdr.AgencyEnabled = 1 
       and IByAdr.AvailableForClient = 1
  JOIN Future.AddressIntersection ai ON ai.IntersectionId = IByAdr.Id AND ai.addressid = adr.Id
  JOIN Future.Clients drugstore ON drugstore.Id = i.ClientId
  JOIN usersettings.RetClientsSet r ON r.clientcode = drugstore.Id
  JOIN usersettings.PricesData pd ON pd.pricecode = i.PriceId
    join usersettings.SupplierIntersection si on si.SupplierId = pd.FirmCode and i.ClientId = si.ClientId
  JOIN usersettings.PricesCosts pc on pc.PriceCode = i.PriceId and ((r.InvisibleOnFirm > 0 and pc.BaseCost = 1) or (r.InvisibleOnFirm = 0 and pc.CostCode = i.CostId))
    JOIN usersettings.PriceItems pi on pi.Id = pc.PriceItemId
    JOIN farm.FormRules f on f.Id = pi.FormRuleId
    JOIN usersettings.ClientsData supplier ON supplier.firmcode = pd.firmcode
    JOIN usersettings.PricesRegionalData prd ON prd.regioncode = i.RegionId AND prd.pricecode = pd.pricecode
    JOIN usersettings.RegionalData rd ON rd.RegionCode = i.RegionId AND rd.FirmCode = pd.firmcode
  left join future.UserPrices up on up.PriceId = i.PriceId and up.UserId = ifnull(u.InheritPricesFrom, u.Id) and up.RegionId = i.RegionId
WHERE   supplier.firmstatus = 1
    AND supplier.firmtype = 0
    and (supplier.maskregion & i.RegionId) > 0
    AND (drugstore.maskregion & i.RegionId & u.WorkRegionMask) > 0
    AND (r.WorkRegionMask & i.RegionId) > 0
    AND pd.agencyenabled = 1
    AND pd.enabled = 1
    AND pd.pricetype <> 1
    AND prd.enabled = 1
    AND if(not r.ServiceClient, supplier.FirmCode != 234, 1)
    and i.AvailableForClient = 1
    AND u.Id = UserIdParam
group by PriceId, RegionId;

END;



DROP PROCEDURE IF EXISTS Future.`GetPrices`; 

CREATE DEFINER=`RootDBMS`@`127.0.0.1` PROCEDURE Future.`GetPrices`(IN UserIdParam INT UNSIGNED)
BEGIN

drop temporary table IF EXISTS Usersettings.Prices;
create temporary table
Usersettings.Prices
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
 MinReq mediumint Unsigned,
 ControlMinReq bool,
 AllowOrder bool,
 ShortName varchar(50),
 FirmCategory tinyint unsigned,
 MainFirm bool,
 Storage bool,
 index (PriceCode),
 index (RegionCode)
)engine = MEMORY;

INSERT
INTO    Usersettings.Prices
SELECT  pd.firmcode,
        i.PriceId,
        pc.CostCode,
        ifnull(pd.ParentSynonym, pd.pricecode) PriceSynonymCode,
        i.RegionId,
        si.DelayOfPayment,
        if(up.PriceId is null, 1, 0),
        round((1 + pd.UpCost / 100) * (1 + prd.UpCost / 100) * (1 + i.PriceMarkup / 100), 5),
        to_days(now()) - to_days(pi.PriceDate) < f.maxold,
        pd.CostType,
        pi.PriceDate,
        r.ShowPriceName,
        pd.PriceName,
        pi.RowCount,
        prd.MinReq,
        0,
        (r.OrderRegionMask & i.RegionId & u.OrderRegionMask) > 0,
        supplier.ShortName,
        si.SupplierCategory,
        si.SupplierCategory >= r.BaseFirmCategory,
        Storage
FROM Future.Users u
  join Future.Intersection i on i.ClientId = u.ClientId and i.AgencyEnabled = 1
  JOIN Future.Clients drugstore ON drugstore.Id = i.ClientId
  JOIN usersettings.RetClientsSet r ON r.clientcode = drugstore.Id
  JOIN usersettings.PricesData pd ON pd.pricecode = i.PriceId
    join usersettings.SupplierIntersection si on si.SupplierId = pd.FirmCode and i.ClientId = si.ClientId
  JOIN usersettings.PricesCosts pc on pc.PriceCode = i.PriceId and ((r.InvisibleOnFirm > 0 and pc.BaseCost = 1) or (r.InvisibleOnFirm = 0 and pc.CostCode = i.CostId))
    JOIN usersettings.PriceItems pi on pi.Id = pc.PriceItemId
    JOIN farm.FormRules f on f.Id = pi.FormRuleId
    JOIN usersettings.ClientsData supplier ON supplier.firmcode = pd.firmcode
    JOIN usersettings.PricesRegionalData prd ON prd.regioncode = i.RegionId AND prd.pricecode = pd.pricecode
    JOIN usersettings.RegionalData rd ON rd.RegionCode = i.RegionId AND rd.FirmCode = pd.firmcode
  left join future.UserPrices up on up.PriceId = i.PriceId and up.UserId = ifnull(u.InheritPricesFrom, u.Id) and up.RegionId = i.RegionId
WHERE   supplier.firmstatus = 1
    AND supplier.firmtype = 0
    and (supplier.maskregion & i.RegionId) > 0
    AND (drugstore.maskregion & i.RegionId & u.WorkRegionMask) > 0
    AND (r.WorkRegionMask & i.RegionId) > 0
    AND pd.agencyenabled = 1
    AND pd.enabled = 1
    AND pd.pricetype <> 1
    AND prd.enabled = 1
    AND if(not r.ServiceClient, supplier.FirmCode != 234, 1)
    and i.AvailableForClient = 1
    AND u.Id = UserIdParam
group by PriceId, RegionId;

END;
