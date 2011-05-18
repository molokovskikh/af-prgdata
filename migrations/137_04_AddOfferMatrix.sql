alter table usersettings.RetClientsSet
  add column `OfferMatrixPriceId` int unsigned default null comment 'ссылка на ассортиментный прайс-лист, из которого будет формироваться матрица предложений, если не установлен, то механизм не активирован',
  add column `OfferMatrixType` int unsigned not null default 0 comment 'тип матрицы предложений: 0 - белый список, 1 - черный список',
  add constraint FK_RetClientsSet_OfferMatrixPriceId foreign key (OfferMatrixPriceId) references usersettings.PricesData(PriceCode) on delete set null;