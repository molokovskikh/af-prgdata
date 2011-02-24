alter table future.Users
  add column `UseAdjustmentOrders` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT 'Производить корректировку заказов по цене и количеству перед отправкой?';