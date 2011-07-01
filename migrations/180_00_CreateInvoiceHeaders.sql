create table Documents.InvoiceHeaders
(
  Id int unsigned not null default '0',
  InvoiceNumber varchar(20) default null comment 'Номер счет-фактуры',
  InvoiceDate datetime default null comment 'Дата счет-фактуры',
  SellerName varchar(255) default null comment 'Наименование продавца',
  SellerAddress varchar(255) default null comment 'Адрес продавца',
  SellerINN varchar(20) default null comment 'ИНН продавца',
  SellerKPP varchar(20) default null comment 'КПП продавца',
  ShipperInfo varchar(255) default null comment 'Грузоотправитель и его адрес',
  ConsigneeInfo varchar(255) default null comment 'Грузополучатель и его адрес',
  PaymentDocumentInfo varchar(255) default null comment 'Поле К платежно-расчетному документу N',
  BuyerName varchar(255) default null comment 'Наименование покупателя',
  BuyerAddress varchar(255) default null comment 'Адрес покупателя',
  BuyerINN varchar(20) default null comment 'ИНН покупателя',
  BuyerKPP varchar(20) default null comment 'КПП покупателя',

  AmountWithoutNDS0 decimal(12,6) default null comment 'Стоимость товаров без налога для группы товаров, облагаемых ставкой 0% НДС',

  AmountWithoutNDS10 decimal(12,6) default null comment 'Стоимость товаров без налога для группы товаров, облагаемых ставкой 10% НДС',
  NDSAmount10 decimal(12,6) default null comment 'Сумма налога для группы товаров , облагаемых ставкой 10% НДС',
  Amount10 decimal(12,6) default null comment 'Стоимость товаров для группы товаров , облагаемых ставкой 10% НДС всего с учётом налога',

  AmountWithoutNDS18 decimal(12,6) default null comment 'Стоимость товаров без налога для группы товаров, облагаемых ставкой 18% НДС',
  NDSAmount18 decimal(12,6) default null comment 'Сумма налога для группы товаров , облагаемых ставкой 18% НДС',
  Amount18 decimal(12,6) default null comment 'Стоимость товаров для группы товаров , облагаемых ставкой 18% НДС всего с учётом налога',

  AmountWithoutNDS decimal(12,6) default null comment 'Общая стоимость товаров без налога (указывается в конце таблицы счёт-фактуры по строке «ИТОГО»)',
  NDSAmount decimal(12,6) default null comment 'Общая сумма налога (указывается в конце таблицы счёт-фактуры по строке «ИТОГО»)',
  Amount decimal(12,6) default null comment 'Общая стоимость товаров с налогом (указывается в конце таблицы счёт-фактуры по строке «ИТОГО»)',

  primary key (Id)
);


alter table Documents.DocumentBodies
  add column Unit varchar(20) default null comment 'Единица измерения',
  add column ExciseTax decimal(12,6) default null comment 'В том числе акциз',
  add column BillOfEntryNumber varchar(30) default null comment '№ Таможенной декларации',
  add column EAN13 varchar(13) default null comment 'Код EAN-13';


