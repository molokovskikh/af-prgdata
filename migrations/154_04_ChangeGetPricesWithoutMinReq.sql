DROP PROCEDURE IF EXISTS Future.`GetPricesWithoutMinReq`;

CREATE DEFINER=`RootDBMS`@`127.0.0.1` PROCEDURE Future.`GetPricesWithoutMinReq`(IN UserIdParam INT UNSIGNED)
BEGIN

CALL Future.BaseGetPrices(UserIdParam, 0);

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

INSERT INTO Usersettings.Prices (
  FirmCode,
  PriceCode,
  CostCode,
  PriceSynonymCode,
  RegionCode,
  DelayOfPayment,
  DisabledByClient,
  Upcost,
  Actual,
  CostType,
  PriceDate,
  ShowPriceName,
  PriceName,
  PositionCount,
  AllowOrder,
  ShortName,
  FirmCategory,
  MainFirm,
  Storage,
  VitallyImportantDelay,
  OtherDelay
)
SELECT
  FirmCode,
  PriceCode,
  CostCode,
  PriceSynonymCode,
  RegionCode,
  DelayOfPayment,
  DisabledByClient,
  Upcost,
  Actual,
  CostType,
  PriceDate,
  ShowPriceName,
  PriceName,
  PositionCount,
  AllowOrder,
  ShortName,
  FirmCategory,
  MainFirm,
  Storage,
  VitallyImportantDelay,
  OtherDelay
FROM
  Future.BasePrices;

DROP TEMPORARY TABLE IF EXISTS Future.BasePrices;

END;
