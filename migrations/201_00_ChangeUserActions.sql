alter table Logs.AnalitFUserActionLogs
  add column `UpdateId` int unsigned not null after `UserId`,
  add key `IDX_AnalitFUserActionLogs_UpdateId` (`UpdateId`);

alter table UserSettings.UserActions
  add column `Identifier` varchar(255) not null comment 'Идентификатор действия для AnalitF.dpr';

update UserSettings.UserActions
set
  Identifier = 'Start'
where
  Id = 1;

update UserSettings.UserActions
set
  Identifier = 'Stop'
where
  Id = 2;

insert into UserSettings.UserActions (Id, Name, Identifier) values (3, 'Запрос накопительного обновления', 'GetData');
insert into UserSettings.UserActions (Id, Name, Identifier) values (4, 'Запрос кумулятивного обновления', 'GetCumulative');
insert into UserSettings.UserActions (Id, Name, Identifier) values (5, 'Отправка заказов', 'SendOrders');
insert into UserSettings.UserActions (Id, Name, Identifier) values (6, 'Загрузка и получение накладных', 'SendWaybills');
insert into UserSettings.UserActions (Id, Name, Identifier) values (7, 'Список препаратов', 'CatalogSearch');
insert into UserSettings.UserActions (Id, Name, Identifier) values (8, 'Поиск в прайс-листах', 'SynonymSearch');
insert into UserSettings.UserActions (Id, Name, Identifier) values (9, 'Поиск по МНН', 'MnnSearch');
insert into UserSettings.UserActions (Id, Name, Identifier) values (10, 'Прайс-листы', 'ShowPrices');
insert into UserSettings.UserActions (Id, Name, Identifier) values (11, 'Минимальные цены', 'ShowMinPrices');
insert into UserSettings.UserActions (Id, Name, Identifier) values (12, 'Сводный заказ', 'ShowSummaryOrder');
insert into UserSettings.UserActions (Id, Name, Identifier) values (13, 'Заказы', 'ShowOrders');
insert into UserSettings.UserActions (Id, Name, Identifier) values (14, 'АвтоЗаказ', 'ShowOrderBatch');
insert into UserSettings.UserActions (Id, Name, Identifier) values (15, 'Уцененные препараты', 'ShowExpireds');
insert into UserSettings.UserActions (Id, Name, Identifier) values (16, 'Забракованные препараты', 'ShowDefectives');
insert into UserSettings.UserActions (Id, Name, Identifier) values (17, 'Накладные', 'ShowDocuments');
insert into UserSettings.UserActions (Id, Name, Identifier) values (18, 'На главную страницу', 'Home');
insert into UserSettings.UserActions (Id, Name, Identifier) values (19, 'Конфигурация', 'ShowConfig');



