alter table farm.regions
  add column TechContact mediumtext comment 'телефоны Централизованной службы поддержки пользователей',
  add column OperatingMode mediumtext comment 'режим работы Централизованной службы поддержки пользователей';