alter table usersettings.DelayOfPayments
  drop key DelayOfPaymentId,
  drop FOREIGN KEY FK_DelayOfPayments_SupplierId;

alter table usersettings.DelayOfPayments
  add column SupplierIntersectionId int(10) default null,
  drop column ClientId,
  drop column SupplierId;

alter table usersettings.DelayOfPayments
  modify column SupplierIntersectionId int(10) default null after `Id`;