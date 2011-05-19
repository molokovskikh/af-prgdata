create table UserSettings.OfferMatrixSuppliers
(
  Id int unsigned not null auto_increment,
  ClientId int unsigned not null,
  SupplierId int unsigned not null,
  primary key (Id),
  CONSTRAINT `FK_OfferMatrixSuppliers_ClientId` FOREIGN KEY (ClientId) REFERENCES future.Clients (Id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_OfferMatrixSuppliers_SupplierId` FOREIGN KEY (SupplierId) REFERENCES future.Suppliers (Id) ON DELETE CASCADE ON UPDATE CASCADE
);
