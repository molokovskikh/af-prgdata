insert
into UserSettings.ReorderingRules (RegionalDataId, DayOfWeek)
SELECT
rd.RowId, d.DayOfWeek
FROM
(
`usersettings`.`RegionalData` rd,
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
left join usersettings.ReorderingRules rr on rr.RegionalDataId = rd.RowId and rr.DayOfWeek = convert(d.DayOfWeek using cp1251)
where
rr.Id is null;
