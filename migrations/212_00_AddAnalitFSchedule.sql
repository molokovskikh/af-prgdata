alter table UserSettings.RetClientsSet
  add column AllowAnalitFSchedule tinyint(1) unsigned not null default '0' comment 'Разрешить функциональность расписания обновлений в AnalitF';

create table UserSettings.AnalitFSchedules
(
  Id int unsigned NOT NULL AUTO_INCREMENT,
  ClientId int unsigned NOT NULL,
  Enable tinyint(1) unsigned not null default '0' comment 'Включено ли данное расписание',
  Hour int unsigned not null default 0 comment 'назначенный час расписания',
  Minute int unsigned not null default 0 comment 'назначенная минута расписания',
  PRIMARY KEY (`Id`),
  CONSTRAINT `FK_AnalitFSchedules_ClientId` FOREIGN KEY (ClientId) REFERENCES future.Clients (Id) ON DELETE CASCADE ON UPDATE CASCADE
);