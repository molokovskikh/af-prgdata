create table UserSettings.ReorderingRules
(
  Id int unsigned not null auto_increment,
  RegionalDataId int unsigned not null,
  `DayOfWeek` enum ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday') not null,
  TimeOfStopsOrders BIGINT default null comment 'Время окончания приема заявок',
  primary key (Id),
  UNIQUE KEY `ReorderingRuleId` (`RegionalDataId`, `DayOfWeek`),
  CONSTRAINT `FK_ReorderingRules_RegionalDataId` FOREIGN KEY (`RegionalDataId`) REFERENCES usersettings.`RegionalData` (`RowId`) ON DELETE CASCADE ON UPDATE CASCADE
);