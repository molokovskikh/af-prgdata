DROP PROCEDURE IF EXISTS Future.`AFGetActivePrices`; 
CREATE DEFINER=`RootDBMS`@`127.0.0.1` PROCEDURE Future.`AFGetActivePrices`(IN UserIdParam INT UNSIGNED)
BEGIN

Declare TabelExsists Bool DEFAULT false;
DECLARE CONTINUE HANDLER FOR 1146
begin
  Call Future.GetPrices(UserIdParam);
end;

if not TabelExsists then
DROP TEMPORARY TABLE IF EXISTS Usersettings.ActivePrices;
create temporary table
Usersettings.ActivePrices
(
 FirmCode int Unsigned,
 PriceCode int Unsigned,
 CostCode int Unsigned,
 PriceSynonymCode int Unsigned,
 RegionCode BigInt Unsigned,
 Fresh bool,
 Upcost decimal(7,5),
 MaxSynonymCode Int Unsigned,
 MaxSynonymFirmCrCode Int Unsigned,
 CostType bool,
 PriceDate DateTime,
 ShowPriceName bool,
 PriceName VarChar(50),
 PositionCount int Unsigned,
 MinReq mediumint Unsigned,
 FirmCategory tinyint unsigned,
 MainFirm bool,
 unique (PriceCode, RegionCode, CostCode),
 index  (CostCode, PriceCode),
 index  (PriceSynonymCode),
 index  (MaxSynonymCode),
 index  (PriceCode),
 index  (MaxSynonymFirmCrCode)
 )engine=MEMORY
 ;
set TabelExsists=true;
end if;
select null from Usersettings.Prices limit 0;
INSERT
INTO Usersettings.ActivePrices(
 FirmCode,
 PriceCode,
 CostCode,
 PriceSynonymCode,
 RegionCode,
 Fresh,
 Upcost,
 MaxSynonymCode,
 MaxSynonymFirmCrCode,
 CostType,
 PriceDate,
 ShowPriceName,
 PriceName,
 PositionCount,
 MinReq,
 FirmCategory,
 MainFirm)
SELECT P.FirmCode,
       P.PriceCode,
       P.CostCode,
       P.PriceSynonymCode,
       P.RegionCode,
       A.ForceReplication !=0,
       P.Upcost,
       A.MaxSynonymCode,
       A.MaxSynonymFirmCrCode,
       P.CostType,
       P.PriceDate,
       P.ShowPriceName,
       P.PriceName,
       P.PositionCount,
       P.MinReq,
       P.FirmCategory,
       P.MainFirm
FROM Usersettings.Prices P
  join Usersettings.AnalitFReplicationInfo A on A.FirmCode = P.FirmCode
WHERE  Actual = 1
  and p.DisabledByClient = 0
  and A.UserId = UserIdParam;

drop temporary table IF EXISTS Usersettings.Prices;

END;



DROP PROCEDURE IF EXISTS UserSettings.`AFGetActivePricesByUserId`;

CREATE DEFINER=`RootDBMS`@`127.0.0.1` PROCEDURE UserSettings.`AFGetActivePricesByUserId`(IN UserIdParam INT UNSIGNED)
BEGIN

Declare TabelExsists Bool DEFAULT false;
Declare ClientCodeParam INT UNSIGNED;
DECLARE CONTINUE HANDLER FOR 1146
begin
SELECT ClientCode
INTO   ClientCodeParam
FROM   OsUserAccessRight
WHERE  RowId=UserIdParam;
Call GetPrices2(ClientCodeParam);
end;
if not TabelExsists then
DROP TEMPORARY TABLE IF EXISTS  ActivePrices;
create temporary table
ActivePrices
(

 FirmCode int Unsigned,
 PriceCode int Unsigned,
 CostCode int Unsigned,
 PriceSynonymCode int Unsigned,
 RegionCode BigInt Unsigned,
 Fresh bool,
 Upcost decimal(7,5),
 PublicUpCost decimal(7,5),
 MaxSynonymCode Int Unsigned,
 MaxSynonymFirmCrCode Int Unsigned,
 CostType bool,
 PriceDate DateTime,
 ShowPriceName bool,
 PriceName VarChar(50),
 PositionCount int Unsigned,
 MinReq mediumint Unsigned,
 CostCorrByClient bool,
 FirmCategory tinyint unsigned,
 MainFirm bool,
 unique (PriceCode, RegionCode, CostCode),
 index  (CostCode, PriceCode),
 index  (PriceSynonymCode),
 index  (MaxSynonymCode),
 index  (PriceCode),
 index  (MaxSynonymFirmCrCode)
 )engine=MEMORY
 ;
set TabelExsists=true;
end if;
select null from Prices limit 0;
INSERT
INTO    ActivePrices
        (
 FirmCode,
 PriceCode,
 CostCode,
 PriceSynonymCode,
 RegionCode,
 Fresh,
 Upcost,
 PublicUpCost,
 MaxSynonymCode,
 MaxSynonymFirmCrCode,
 CostType,
 PriceDate,
 ShowPriceName,
 PriceName,
 PositionCount,
 MinReq,
 CostCorrByClient,
 FirmCategory,
 MainFirm
        ) 
SELECT P.FirmCode            ,
       P.PriceCode           ,
       P.CostCode            ,
       P.PriceSynonymCode    ,
       P.RegionCode          ,
       A.ForceReplication !=0,
       P.Upcost              ,
       P.PublicUpCost        ,
       A.MaxSynonymCode      ,
       A.MaxSynonymFirmCrCode,
       P.CostType            ,
       P.PriceDate           ,
       P.ShowPriceName       ,
       P.PriceName           ,
       P.PositionCount       ,
       P.MinReq              ,
       P.CostCorrByClient    ,
       P.FirmCategory        ,
       P.MainFirm
FROM   Prices P,
       AnalitFReplicationInfo A
WHERE  DisabledByClient=0
   AND Actual          =1
   AND A.UserId        =UserIdParam
   AND A.FirmCode      =P.FirmCode;
drop temporary table IF EXISTS Prices;

END;
