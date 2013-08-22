ALTER TABLE `farm`.`buyingmatrix` ADD COLUMN `SupplierId` INT(10) UNSIGNED AFTER `CodeOKP`,
 ADD CONSTRAINT `FK_Suppliers` FOREIGN KEY `FK_Suppliers` (`SupplierId`)
    REFERENCES `customers`.`Suppliers` (`Id`)
    ON DELETE CASCADE
    ON UPDATE SET NULL;
