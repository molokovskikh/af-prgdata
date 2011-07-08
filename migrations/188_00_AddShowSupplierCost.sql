alter table Future.Users
  add column ShowSupplierCost tinyint(1) unsigned not null default '1' comment 'Показывать столбец Цена поставщика в AnalitF при включенном механизме отсрочки платежа';