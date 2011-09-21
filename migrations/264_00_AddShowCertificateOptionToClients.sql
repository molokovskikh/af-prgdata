alter table Future.Clients
  add column ShowCertificatesWithoutRefSupplier tinyint(1) unsigned not null default '0' comment 'Отображать сертификаты без привязки к поставщику';
