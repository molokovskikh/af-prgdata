create table UserSettings.UserActions
(
  Id int unsigned NOT NULL AUTO_INCREMENT,
  Name varchar(255) not null comment 'Наименование действия',
  primary key (Id)
);

create table Logs.AnalitFUserActionLogs
(
  `Id` int unsigned NOT NULL AUTO_INCREMENT,
  `LogTime` datetime NOT NULL,
  `UserId` int unsigned NOT NULL,
  `UserActionId` int unsigned NOT NULL,
  `Context` varchar(255) default null comment 'Контекст действия',
  PRIMARY KEY (`Id`),
  KEY `IDX_AnalitFUserActionLogs_LogTime` (`LogTime`) USING BTREE,
  KEY `IDX_AnalitFUserActionLogs_UserId` (`UserId`),
  KEY `IDX_AnalitFUserActionLogs_UserActionId` (`UserActionId`)
);

insert into UserSettings.UserActions (Id, Name) values (1, 'Запуск программы');
insert into UserSettings.UserActions (Id, Name) values (2, 'Завершение программы');