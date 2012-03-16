create table Future.RuleGroups
(
	Id int unsigned not null auto_increment,
	primary key(Id)
);

create table Future.OrderRules
(
	Id int unsigned not null auto_increment,
	Supplier int unsigned not null,
	RuleGroup int unsigned not null,
	MaxSum decimal not null,
	primary key(Id),
	constraint `FK_OrderRules_Supplier` foreign key (Supplier) references Future.Suppliers(Id) on delete cascade,
	constraint `FK_OrderRules_RuleGroup` foreign key (RuleGroup) references Future.RuleGroups(Id) on delete cascade 
);

alter table Future.Addresses
add column RuleGroup int unsigned,
add constraint `FK_Addresses_RuleGroup` foreign key (RuleGroup) references Future.RuleGroups(Id) on delete set null;
