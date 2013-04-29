drop temporary table if exists usersettings.migrationFor12419;

create temporary table usersettings.migrationFor12419(
	migrationName varchar(255)
) engine=memory;

insert into usersettings.migrationFor12419 (migrationName) values ('742_00_AddMinReorderingToAddressIntersection');

insert into usersettings.migrationFor12419 (migrationName) values ('742_01_AddMinReorderingToAddressIntersectionLogs');

insert into usersettings.migrationFor12419 (migrationName) values ('742_02_CustomersAddressIntersectionUpdateLogging');

insert into usersettings.migrationFor12419 (migrationName) values ('742_06_CreateReorderingRules');

insert into usersettings.migrationFor12419 (migrationName) values ('755_00_UserSettingsReorderingRulesCreateLogging');

insert into usersettings.migrationFor12419 (migrationName) values ('755_01_FillReorderingRules');

insert into logs.schemamigrations (Version, MigratedOn)
select migrationFor12419.migrationName, '2013-01-30 09:17'
from
  usersettings.migrationFor12419
  left join logs.schemamigrations mig on mig.Version = migrationFor12419.migrationName
where
mig.Version is null;

drop temporary table if exists usersettings.migrationFor12419;
