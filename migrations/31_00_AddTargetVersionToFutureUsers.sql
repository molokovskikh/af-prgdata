alter table future.Users
  add column `SaveAFDataFiles` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT 'Сохранять подготовленный данные в архив?', 
  add column `TargetVersion` mediumint(8) unsigned DEFAULT NULL comment 'Номер версии, до которой программа может автообновляться; null - до максимально возможной.';