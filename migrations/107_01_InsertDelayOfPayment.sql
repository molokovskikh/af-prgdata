delete from usersettings.DelayOfPayments where ClientId = 10005;

insert into usersettings.DelayOfPayments 
(ClientId, SupplierId, VitallyImportantDelay, OtherDelay, DayOfWeek)
SELECT 
ClientId, SupplierId, DelayOfPayment, DelayOfPayment, d.DayOfWeek
FROM 
`usersettings`.`SupplierIntersection`,
(
select 'Monday' as DayOfWeek
union
select 'Tuesday' 
union
select 'Wednesday' 
union
select 'Thursday' 
union
select 'Friday' 
union
select 'Saturday' 
union
select 'Sunday' 
) d
where
ClientId = 10005
and DelayOfPayment > 0;