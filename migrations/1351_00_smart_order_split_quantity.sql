alter table OrderSendRules.smart_order_rules add column DoNotSplitQuantity TINYINT(1) unsigned not null default 1;
alter table OrderSendRules.smart_order_rules add column MaxSplitQuantityPercent INTEGER unsigned not null default 100;
