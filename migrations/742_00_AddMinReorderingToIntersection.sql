alter table Customers.Intersection
  add column MinReordering INT UNSIGNED DEFAULT NULL comment 'Минимальное значение дозаявки, если неопределенно, то дозаявка невозможена';