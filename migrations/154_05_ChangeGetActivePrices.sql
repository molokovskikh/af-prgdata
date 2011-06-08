DROP PROCEDURE IF EXISTS Future.`GetActivePrices`;

CREATE DEFINER=`RootDBMS`@`127.0.0.1` PROCEDURE Future.`GetActivePrices`(IN UserIdParam INT UNSIGNED)
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
 DelayOfPayment decimal(5,3),
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
 VitallyImportantDelay decimal(5,3),
 OtherDelay decimal(5,3),
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
 DelayOfPayment,
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
 MainFirm,
 VitallyImportantDelay,
 OtherDelay
)
SELECT P.FirmCode,
       P.PriceCode,
       P.CostCode,
       P.PriceSynonymCode,
       P.RegionCode,
       1,
       p.DelayOfPayment,
       P.Upcost,
       0,
       0,
       P.CostType,
       P.PriceDate,
       P.ShowPriceName,
       P.PriceName,
       P.PositionCount,
       P.MinReq,
       P.FirmCategory,
       P.MainFirm,
       P.VitallyImportantDelay,
       P.OtherDelay
FROM Usersettings.Prices P
WHERE p.DisabledByClient = 0
  and p.Actual = 1;

drop temporary table IF EXISTS Usersettings.Prices;

END;
