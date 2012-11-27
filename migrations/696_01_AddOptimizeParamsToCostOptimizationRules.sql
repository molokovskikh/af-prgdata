ALTER TABLE `usersettings`.`costoptimizationrules` ADD COLUMN `MinAmount` DECIMAL(11,2) AFTER `SupplierId`,
 ADD COLUMN `MinPercent` DECIMAL(11,2) AFTER `MinAmount`,
 ADD COLUMN `MaxPercent` DECIMAL(11,2) AFTER `MinPercent`,
 ADD COLUMN `MinDelta` DECIMAL(11,2) AFTER `MaxPercent`,
 ADD COLUMN `MaxDelta` DECIMAL(11,2) AFTER `MinDelta`;
 
 UPDATE usersettings.costoptimizationrules c SET MinAmount=30, MinPercent=0.8, MaxPercent=23, MinDelta=0.2, MaxDelta=0.7 where SupplierId=12423;