create table usersettings.DelayOfPayments
(
  Id int unsigned not null auto_increment,
  ClientId int unsigned not null,
  SupplierId int unsigned not null,
  `VitallyImportantDelay` decimal(5,3) NOT NULL DEFAULT '0.000' comment 'Значение отсрочки платежа для ЖНВЛС',
  `OtherDelay` decimal(5,3) NOT NULL DEFAULT '0.000' comment 'Значение отсрочки платежа для прочего ассортимента',
  `DayOfWeek` enum ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday') not null, 
  primary key (Id),
  CONSTRAINT `FK_DelayOfPayments_SupplierId` FOREIGN KEY (`SupplierId`) REFERENCES usersettings.`ClientsData` (`FirmCode`) ON DELETE CASCADE ON UPDATE CASCADE
);