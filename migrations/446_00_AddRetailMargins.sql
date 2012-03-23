create table UserSettings.RetailMargins
(
	Id int unsigned not null auto_increment,
	ClientId int unsigned not null,
	CatalogId int unsigned not null,
	Markup decimal(5,3) not null comment 'наценка на препарат (%)',
	MaxMarkup decimal(5,3) not null comment 'максимальная наценка (%)',
	MaxSupplierMarkup decimal(5,3) default null comment 'максимальная наценка оптового звена(%)',
	UpdateTime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	primary key(Id),
	constraint `FK_RetailMargins_ClientId` foreign key (ClientId) references Future.Clients(Id) on delete cascade,
	constraint `FK_RetailMargins_CatalogId` foreign key (CatalogId) references Catalogs.Catalog(Id) on delete cascade 
);
