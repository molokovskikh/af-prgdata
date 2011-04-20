alter table `usersettings`.`SupplierIntersection`
  add column UseWeeklyDelays tinyint(1) unsigned not null default '0' comment 'Использовать отсрочки платежа, установленные для каждого дня недели';