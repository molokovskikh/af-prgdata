create table logs.MailSendLogs(
  Id int unsigned not null auto_increment,
  UserId int unsigned not null,
  MailId int unsigned not null,
  UpdateId int unsigned default null,
  Committed tinyint(1) unsigned NOT NULL DEFAULT '0',
  primary key (Id),
  CONSTRAINT `FK_MailSendLogs_UserId` FOREIGN KEY (`UserId`) REFERENCES `future`.`users` (`Id`) ON DELETE CASCADE,
  CONSTRAINT `FK_MailSendLogs_MailId` FOREIGN KEY (`MailId`) REFERENCES `Documents`.`Mails` (Id) ON DELETE CASCADE,
  CONSTRAINT `FK_MailSendLogs_UpdateId` FOREIGN KEY (`UpdateId`) REFERENCES `logs`.`analitfupdates` (`UpdateId`) ON DELETE CASCADE
);
