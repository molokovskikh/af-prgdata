alter table orders.OrdersList
  add column `RetailCost` decimal(9,2) unsigned DEFAULT NULL comment 'Розничная цена, сформированная клиентом';