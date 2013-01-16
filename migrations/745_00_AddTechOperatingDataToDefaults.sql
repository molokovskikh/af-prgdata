ALTER TABLE `usersettings`.`defaults` MODIFY COLUMN `AnalitFVersion` MEDIUMINT(8) UNSIGNED NOT NULL DEFAULT 0,
 ADD COLUMN `TechOperatingModeTemplate` TEXT AFTER `AddressesHelpText`,
 ADD COLUMN `TechOperatingModeBegin` VARCHAR(5) NOT NULL DEFAULT '' AFTER `TechOperatingModeTemplate`,
 ADD COLUMN `TechOperatingModeEnd` VARCHAR(5) NOT NULL DEFAULT '' AFTER `TechOperatingModeBegin`;

 update `usersettings`.`defaults` set TechOperatingModeTemplate = '<p>будни: с {0} до {1}</p>',
TechOperatingModeBegin = '7.00',
TechOperatingModeEnd = '19.00';