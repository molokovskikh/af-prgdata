alter table orders.OrdersHead
  add column `VitallyImportantDelayOfPayment` decimal(5,3) DEFAULT NULL;

alter table orders.OrdersList
  add column `CostWithDelayOfPayment` decimal(9,2) unsigned DEFAULT NULL;