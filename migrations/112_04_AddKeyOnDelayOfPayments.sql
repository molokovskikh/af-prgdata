alter table usersettings.DelayOfPayments
  add UNIQUE KEY `DelayOfPaymentId` (`SupplierIntersectionId`, `DayOfWeek`);