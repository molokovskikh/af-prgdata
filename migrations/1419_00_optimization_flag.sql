ALTER TABLE `farm`.`FormRules`	ADD COLUMN `FOptimizationSkip` VARCHAR(20) NULL DEFAULT NULL;
ALTER TABLE `farm`.`FormRules`	ADD COLUMN `TxtOptimizationSkipBegin` INT(11) NULL DEFAULT NULL;
ALTER TABLE `farm`.`FormRules`	ADD COLUMN `TxtOptimizationSkipEnd` INT(11) NULL DEFAULT NULL;