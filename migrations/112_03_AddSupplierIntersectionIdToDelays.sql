alter table usersettings.DelayOfPayments
  add CONSTRAINT `FK_DelayOfPayments_SupplierIntersectionId` FOREIGN KEY (`SupplierIntersectionId`) REFERENCES usersettings.`SupplierIntersection` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE;
