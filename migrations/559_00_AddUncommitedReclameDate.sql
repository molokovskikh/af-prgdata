alter table UserSettings.UserUpdateInfo
  add column `UncommitedReclameDate` datetime DEFAULT NULL after `UncommitedUpdateDate`;