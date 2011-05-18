alter table farm.BuyingMatrix
  add column `ProductId` int unsigned not null AFTER `PriceId`;

alter table farm.BuyingMatrix
  drop column CatalogId;

alter table farm.BuyingMatrix
  add constraint FK_BuyingMatrix_PriceId foreign key (PriceId) references usersettings.PricesData(PriceCode) on delete cascade,
  add constraint FK_BuyingMatrix_ProductId foreign key (ProductId) references catalogs.Products(Id) on delete cascade,
  add constraint FK_BuyingMatrix_ProducerId foreign key (ProducerId) references catalogs.Producers(Id) on delete set null;

alter table farm.BuyingMatrix
  add UNIQUE KEY `FK_BuyingMatrix_Comb` (`PriceId`,`ProductId`,`ProducerId`) USING BTREE;
