alter table logs.document_logs
  add column `IsFake` tinyint(1) unsigned not null DEFAULT 0 comment 'Признак того, что документ ненастоящий и файла для него нет.';