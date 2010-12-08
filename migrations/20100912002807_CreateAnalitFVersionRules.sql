create table usersettings.`AnalitFVersionRules`
(
  Id int not null auto_increment,
  SourceVersion mediumint(8) unsigned not null,
  DestinationVersion mediumint(8) unsigned not null,
  primary key (Id)
);