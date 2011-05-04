create table usersettings.PriceIntersections
(
  `Id` int unsigned not null auto_increment,
  `SupplierIntersectionId` int unsigned not null,
  `PriceId` int unsigned NOT NULL,
  `UseWeeklyDelays` tinyint(1) unsigned not null default '0' comment 'Использовать отсрочки платежа, установленные для каждого дня недели',
  primary key (Id),
  CONSTRAINT `FK_PriceIntersections_SupplierIntersectionId` FOREIGN KEY (`SupplierIntersectionId`) REFERENCES usersettings.`SupplierIntersection` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_PriceIntersections_PriceId` FOREIGN KEY (`PriceId`) REFERENCES `usersettings`.`pricesdata` (`PriceCode`) ON DELETE CASCADE ON UPDATE CASCADE
);


create table usersettings.DelayOfPayments
(
  `Id` int unsigned not null auto_increment,
  `PriceIntersectionId` int unsigned not null,
  `VitallyImportantDelay` decimal(5,3) NOT NULL DEFAULT '0.000' comment 'Значение отсрочки платежа для ЖНВЛС',
  `OtherDelay` decimal(5,3) NOT NULL DEFAULT '0.000' comment 'Значение отсрочки платежа для прочего ассортимента',
  `DayOfWeek` enum ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday') not null,   
  primary key (Id),
  UNIQUE KEY `DelayOfPaymentId` (`PriceIntersectionId`,`DayOfWeek`),
  CONSTRAINT `FK_DelayOfPayments_PriceIntersectionId` FOREIGN KEY (`PriceIntersectionId`) REFERENCES `PriceIntersections` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE
);