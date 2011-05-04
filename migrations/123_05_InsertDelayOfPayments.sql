insert into 
  usersettings.PriceIntersections (SupplierIntersectionId, PriceId, UseWeeklyDelays)
SELECT 
  si.Id, pd.PriceCode, si.UseWeeklyDelays
FROM 
  usersettings.SupplierIntersection si
  inner join usersettings.PricesData pd on pd.FirmCode = si.SupplierId
  left join usersettings.PriceIntersections pi on pi.SupplierIntersectionId = si.Id and pi.PriceId = pd.PriceCode
where
  pi.Id is null;

insert into 
  usersettings.DelayOfPayments (PriceIntersectionId, VitallyImportantDelay, OtherDelay, DayOfWeek)
SELECT 
  pi.Id, dopOld.VitallyImportantDelay, dopOld.OtherDelay, dopOld.DayOfWeek
FROM 
  usersettings.SupplierIntersection si
  inner join usersettings.DelayOfPaymentsOld dopOld on dopOld.SupplierIntersectionId = si.Id
  inner join usersettings.PriceIntersections pi on pi.SupplierIntersectionId = si.Id
  left join usersettings.DelayOfPayments dop on dop.PriceIntersectionId = pi.Id and dop.DayOfWeek = dopOld.DayOfWeek
where
  dop.Id is null;

insert into 
  usersettings.DelayOfPayments (PriceIntersectionId, VitallyImportantDelay, OtherDelay, DayOfWeek)
SELECT 
  pi.Id, si.DelayOfPayment, si.DelayOfPayment, d.DayOfWeek
FROM 
  (
  usersettings.SupplierIntersection si,
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
  inner join usersettings.PriceIntersections pi on pi.SupplierIntersectionId = si.Id  
  left join usersettings.DelayOfPayments dop on dop.PriceIntersectionId = pi.Id and dop.DayOfWeek = convert(d.DayOfWeek using cp1251)
where
  dop.Id is null;
