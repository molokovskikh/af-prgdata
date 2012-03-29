update
  catalogs.Catalognames c,
  catalogs.Descriptions d 
set
  c.UpdateTime = current_timestamp()
where
    d.Id = c.DescriptionId
and d.NeedCorrect = 1;