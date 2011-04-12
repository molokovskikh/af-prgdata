alter table `future`.`Users`
  add column `PromoFileLimit` int unsigned DEFAULT '512000' comment 'размер файлов с промо-акциями, передаваемый за одно обновление';