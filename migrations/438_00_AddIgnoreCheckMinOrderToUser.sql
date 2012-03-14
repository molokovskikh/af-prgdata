alter table Future.Users
  add column IgnoreCheckMinOrder tinyint(1) unsigned not null default '0' comment 'Игнорировать проверку минимальной суммы заказа у Поставщика';