alter table usersettings.RetClientsSet
  add column `NetworkPriceId` int(10) unsigned default null,
  add CONSTRAINT `FK_RetClientsSet_NetworkPriceId` FOREIGN KEY (`NetworkPriceId`) REFERENCES `pricesdata` (`PriceCode`) ON DELETE SET NULL;

update usersettings.RetClientsSet
set
  NetworkPriceId = 5161
where
  NetworkSupplierId = 7482;