create table Logs.CertificateRequestLogs
(
	Id int unsigned not null auto_increment,
	UpdateId int unsigned not null,
	DocumentBodyId int unsigned not null,
	CertificateId int unsigned,
	Filename varchar(20),
	primary key(Id)
)
