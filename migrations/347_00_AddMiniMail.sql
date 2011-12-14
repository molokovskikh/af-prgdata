﻿create table Documents.Mails(
  Id int unsigned not null auto_increment,
  LogTime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  SupplierId int unsigned not null,
  RegionId bigint(20) unsigned DEFAULT NULL,
  IsVIPMail tinyint(1) unsigned NOT NULL DEFAULT '0',
  Subject varchar(255) default null,
  Body mediumtext,
  primary key (Id),
  key `IDX_Mails_LogTime` (LogTime),
  key `IDX_Mails_Subject` (Subject),
  CONSTRAINT `FK_Mails_SupplierId` FOREIGN KEY (SupplierId) REFERENCES future.Suppliers (Id) ON DELETE CASCADE,
  CONSTRAINT `FK_Mails_RegionId` FOREIGN KEY (RegionId) REFERENCES farm.Regions (RegionCode) ON DELETE set null
);


create table Documents.Attachements(
  Id int unsigned not null auto_increment,
  MailId int unsigned not null,
  FileName varchar(255) not null,
  Extension varchar(255) not null,
  Size int unsigned not null,
  primary key (Id),
  CONSTRAINT `FK_Attachements_MailId` FOREIGN KEY (MailId) REFERENCES Documents.Mails (Id) ON DELETE CASCADE
);

create table logs.AttachementSendLogs(
  Id int unsigned not null auto_increment,
  UserId int unsigned not null,
  AttachementId int unsigned not null,
  UpdateId int unsigned default null,
  Committed tinyint(1) unsigned NOT NULL DEFAULT '0',
  primary key (Id),
  CONSTRAINT `FK_AttachementSendLogs_UserId` FOREIGN KEY (`UserId`) REFERENCES `future`.`users` (`Id`) ON DELETE CASCADE,
  CONSTRAINT `FK_AttachementSendLogs_AttachementId` FOREIGN KEY (`AttachementId`) REFERENCES `Documents`.`Attachements` (Id) ON DELETE CASCADE,
  CONSTRAINT `FK_AttachementSendLogs_UpdateId` FOREIGN KEY (`UpdateId`) REFERENCES `logs`.`analitfupdates` (`UpdateId`) ON DELETE CASCADE
);

