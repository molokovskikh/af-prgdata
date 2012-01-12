alter table Logs.CertificateRequestLogs
add constraint `FK_CertificateRequestLogs_UpdateId` foreign key (UpdateId)
references Logs.AnalitFUpdates(UpdateId) on delete cascade;
