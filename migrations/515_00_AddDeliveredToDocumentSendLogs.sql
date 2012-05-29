alter table logs.DocumentSendLogs
  add column FileDelivered tinyint(1) unsigned NOT NULL DEFAULT '0' comment 'Признак того, что файл был доставлен клиенту',
  add column DocumentDelivered tinyint(1) unsigned NOT NULL DEFAULT '0'  comment 'Признак того, что документ был доставлен клиенту';