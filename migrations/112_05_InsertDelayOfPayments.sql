insert 
into usersettings.DelayOfPayments (SupplierIntersectionId, VitallyImportantDelay, OtherDelay, DayOfWeek)
SELECT 
si.Id, si.DelayOfPayment, si.DelayOfPayment, d.DayOfWeek
FROM 
(
`usersettings`.`SupplierIntersection` si,
 usersettings.ClientsData cd,
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
)
left join usersettings.DelayOfPayments dop on dop.SupplierIntersectionId = si.Id and dop.DayOfWeek = convert(d.DayOfWeek using cp1251)
where
cd.FirmCode = si.SupplierId
and dop.Id is null;
