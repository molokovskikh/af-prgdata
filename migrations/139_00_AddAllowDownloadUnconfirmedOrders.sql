alter table future.Users
  add column AllowDownloadUnconfirmedOrders tinyint(1) unsigned not null default '0' comment 'Разрешено загружать неподтвержденные заказы с сервера';