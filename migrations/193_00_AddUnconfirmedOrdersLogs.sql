create table Logs.UnconfirmedOrdersSendLogs
(
  `Id` int unsigned NOT NULL AUTO_INCREMENT,
  `UserId` int unsigned NOT NULL,
  `OrderId` int unsigned NOT NULL,
  `UpdateId` int unsigned DEFAULT NULL,
  `Committed` tinyint(1) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`Id`),
  KEY IDX_UnconfirmedOrdersSendLogs_OrdreId (OrderId),
  UNIQUE KEY `UnconfirmedOrdersSendId` (`UserId`, `OrderId`),
  CONSTRAINT `FK_UnconfirmedOrdersSendLogs_UserId` FOREIGN KEY (`UserId`) REFERENCES `future`.`users` (`Id`) ON DELETE CASCADE,
  CONSTRAINT `FK_UnconfirmedOrdersSendLogs_UpdateId` FOREIGN KEY (`UpdateId`) REFERENCES `logs`.`analitfupdates` (`UpdateId`) ON DELETE CASCADE
);