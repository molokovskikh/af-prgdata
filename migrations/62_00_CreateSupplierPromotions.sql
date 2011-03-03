create table usersettings.SupplierPromotions
(
  Id int unsigned not null auto_increment,
  UpdateTime timestamp not null default current_timestamp on update current_timestamp,
  Enabled tinyint(1) unsigned not null default '0',
  CatalogId int unsigned not null,
  SupplierId int unsigned not null,
  Annotation varchar(255) not null,
  PromoFile varchar(255), 
  primary key (Id),
  CONSTRAINT `FK_SupplierPromotions_CatalogId` FOREIGN KEY (`CatalogId`) REFERENCES catalogs.`Catalog` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_SupplierPromotions_SupplierId` FOREIGN KEY (`SupplierId`) REFERENCES usersettings.`ClientsData` (`FirmCode`) ON DELETE CASCADE ON UPDATE CASCADE
);