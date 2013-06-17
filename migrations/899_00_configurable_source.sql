alter table OrderSendRules.smart_order_rules
add column Loader int unsigned not null default "0",
add column ColumnSeparator varchar(10),
add column CodeColumn varchar(20),
add column CodeCrColumn varchar(20),
add column ProductColumn varchar(20),
add column ProducerColumn varchar(20),
add column QuantityColumn varchar(20),
add column StartLine int,
add column CodePage int;
