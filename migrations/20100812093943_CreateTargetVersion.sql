alter table usersettings.UserUpdateInfo
  add column `TargetVersion` mediumint(8) unsigned DEFAULT NULL comment 'Номер версии, до которой программа может автообновляться; null - до максимально возможной.';

update usersettings.UserUpdateInfo
set
  TargetVersion = AFAppVersion
where
  AFAppVersion > 0;