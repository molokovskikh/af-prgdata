drop table Future.AddressGroups;

drop table Future.OrderRules;

alter table Future.Addresses
drop column Group,
drop constraint `FK_Addresses_Group`;
