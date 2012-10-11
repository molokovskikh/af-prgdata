alter table logs.unconfirmedorderssendlogs
  add column `ExportedClientOrderId` int unsigned default NULL;