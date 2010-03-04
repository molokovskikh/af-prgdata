﻿using System;
using System.Data;
using System.Data.Common;
using MySql.Data.MySqlClient;
using System.Threading;
using Common.MySql;

namespace PrgData.Common
{
	public class UpdateData
	{
		public string ShortName;
		public uint ClientId;
		public uint UserId;
		public bool CheckCopyId;
		public string Message;
		public DateTime OldUpdateTime;
		public DateTime UncommitedUpdateTime;

		public uint? OffersClientCode;
		public ulong? OffersRegionCode;
		public bool ShowAvgCosts;
		public bool ShowJunkOffers;

		public bool IsFutureClient;

		public bool Spy;
		public bool SpyAccount;

		public bool EnableUpdate;

		public UpdateData(DataSet data)
		{
			var row = data.Tables[0].Rows[0];
			ClientId = Convert.ToUInt32(row["ClientId"]);
			UserId = Convert.ToUInt32(row["UserId"]);
			Message = Convert.ToString(row["Message"]);
			CheckCopyId = Convert.ToBoolean(row["CheckCopyId"]);
			if (!(row["UpdateDate"] is DBNull))
				OldUpdateTime = Convert.ToDateTime(row["UpdateDate"]);
			if (!(row["UncommitedUpdateDate"] is DBNull))
				UncommitedUpdateTime = Convert.ToDateTime(row["UncommitedUpdateDate"]);
			if (data.Tables[0].Columns.Contains("Future"))
				IsFutureClient = true;
			ShortName = Convert.ToString(row["ShortName"]);
			Spy = Convert.ToBoolean(row["Spy"]);
			SpyAccount = Convert.ToBoolean(row["SpyAccount"]);
			EnableUpdate = Convert.ToBoolean(row["EnableUpdate"]);
		}
	}

	public class Reclame
	{
		public string Region { get; set; }
		public DateTime ReclameDate { get; set;}

		public Reclame()
		{
			ReclameDate = new DateTime(2003, 1, 1);
		}
	}

	public class UpdateHelper
	{
		private UpdateData _updateData;
		private MySqlConnection _readWriteConnection;
		private MySqlConnection _readOnlyConnection;

		public UpdateHelper(UpdateData updateData, MySqlConnection readOnlyConnection, MySqlConnection readWriteConnection)
		{
			_updateData = updateData;
			_readWriteConnection = readWriteConnection;
			_readOnlyConnection = readOnlyConnection;
		}

		public void MaintainReplicationInfo()
		{
			var command = new MySqlCommand();

			command.Connection = _readWriteConnection;
			command.CommandText = @"
INSERT
INTO   Usersettings.AnalitFReplicationInfo 
       (
              UserId,
              FirmCode,
              ForceReplication
       )
SELECT ouar.RowId,
       supplier.FirmCode,
       1
FROM usersettings.clientsdata AS drugstore
	JOIN usersettings.OsUserAccessRight ouar  ON ouar.ClientCode = drugstore.FirmCode
	JOIN clientsdata supplier ON supplier.firmsegment = drugstore.firmsegment
	LEFT JOIN Usersettings.AnalitFReplicationInfo ari ON ari.UserId   = ouar.RowId AND ari.FirmCode = supplier.FirmCode
WHERE ari.UserId IS NULL 
	AND supplier.firmtype = 0
	AND drugstore.FirmCode = ?ClientCode
	AND drugstore.firmtype = 1
	AND supplier.maskregion & drugstore.maskregion > 0
GROUP BY ouar.RowId, supplier.FirmCode;

INSERT
INTO   Usersettings.AnalitFReplicationInfo 
       (
              UserId,
              FirmCode,
              ForceReplication
       )
SELECT u.Id,
       supplier.FirmCode,
       1
FROM Future.Clients drugstore
	JOIN Future.Users u ON u.ClientId = drugstore.Id
	JOIN clientsdata supplier ON supplier.maskregion & drugstore.maskregion > 0
	LEFT JOIN Usersettings.AnalitFReplicationInfo ari ON ari.UserId   = u.Id AND ari.FirmCode = supplier.FirmCode
WHERE ari.UserId IS NULL 
	AND supplier.firmtype = 0
	AND drugstore.Id = ?ClientCode
	AND supplier.firmsegment = 0
GROUP BY u.Id, supplier.FirmCode;
";
			command.Parameters.AddWithValue("?ClientCode", _updateData.ClientId);

			command.ExecuteNonQuery();
		}


		public string GetRegionsCommand()
		{
			if (_updateData.IsFutureClient)
			{
				return @"
SELECT r.regioncode,
r.region
FROM future.Clients c
	join farm.regions r on r.RegionCode & c.maskregion > 0
where c.Id = ?ClientCode
";
			}
			var command = @"
SELECT regions.regioncode,
region
FROM farm.regions, clientsdata
WHERE firmcode = ifnull(?OffersClientCode, ?ClientCode)
AND (regions.regioncode & maskregion > 0)";

			if (!_updateData.ShowJunkOffers)
			{
				command += @"
UNION 
SELECT regions.regioncode,
       region
FROM   farm.regions,
       clientsdata
WHERE firmcode = ?ClientCode
AND regions.regioncode= clientsdata.regioncode 

UNION

SELECT DISTINCT regions.regioncode,
                region
FROM            farm.regions,
                includeregulation,
                clientsdata 
WHERE includeclientcode = ?ClientCode
            AND firmcode          = primaryclientcode
            AND includetype      IN (1, 2)
            AND regions.regioncode & clientsdata.maskregion > 0

UNION

SELECT regions.regioncode,
       region
FROM   farm.regions,
       clientsdata ,
       includeregulation 
WHERE  primaryclientcode = ?ClientCode 
   AND firmcode          = includeclientcode 
   AND firmstatus        = 1 
   AND includetype       = 0 
   AND regions.regioncode= clientsdata.regioncode";
			}
			return command;
		}

		public static UpdateData GetUpdateData(MySqlConnection connection, string userName)
		{
			UpdateData updateData = null;
			if (userName.ToLower().StartsWith(@"analit\"))
				userName = userName.ToLower().Replace(@"analit\", "");

			var dataAdapter = new MySqlDataAdapter(@"
SELECT  c.Id ClientId,
	u.Id UserId,
	rui.UpdateDate,
	rui.UncommitedUpdateDate,
	IF(rui.MessageShowCount < 1, '', rui.MESSAGE) Message,
	CheckCopyId,
	'' Future,
    c.Name as ShortName,
    retclientsset.Spy, 
    retclientsset.SpyAccount,
    retclientsset.EnableUpdate 
FROM (future.Clients c,
        retclientsset,
        UserUpdateInfo rui,
        UserPermissions up,
        AssignedPermissions ap)
  join future.users u on c.Id = u.ClientId
WHERE u.Id = ap.UserId
    AND up.Id = ap.PermissionId 
    AND up.Shortcut = 'AF' 
    AND retclientsset.clientcode = c.Id 
    AND rui.UserId = u.Id 
    AND c.Status = 1 
    AND u.Login = ?user", connection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?user", userName);

			var data = new DataSet();
			dataAdapter.Fill(data);

			if (data.Tables[0].Rows.Count > 0)
				updateData = new UpdateData(data);

			if (updateData == null)
			{
				dataAdapter.SelectCommand.CommandText = @"
SELECT  ouar.clientcode as ClientId,
        ouar.RowId UserId,
        rui.UpdateDate,
        rui.UncommitedUpdateDate,
        IF(rui.MessageShowCount<1, '', rui.MESSAGE) Message,
        CheckCopyID,
        clientsdata.ShortName,
        retclientsset.Spy, 
        retclientsset.SpyAccount,
        retclientsset.EnableUpdate
FROM    clientsdata,
        retclientsset,
        UserUpdateInfo rui,
        UserPermissions up,
        AssignedPermissions ap,
        osuseraccessright ouar
	LEFT JOIN IncludeRegulation ir ON IncludeClientCode=ouar.ClientCode
WHERE   ouar.clientcode          =clientsdata.firmcode 
    AND ouar.rowid               = ap.userid 
    AND up.id                    = ap.permissionid 
    AND up.Shortcut              = 'AF' 
    AND IF(ir.id                IS NULL, 1, ir.IncludeType IN (1,2,3)) 
    AND retclientsset.clientcode =ouar.clientcode 
    AND rui.UserId               =ouar.RowId 
    AND firmstatus               =1 
    AND OSUserName = ?user";
				data = new DataSet();
				dataAdapter.Fill(data);
				if (data.Tables[0].Rows.Count > 0)
					updateData = new UpdateData(data);
			}

			if (updateData == null)
				return null;

			dataAdapter = new MySqlDataAdapter(@"
SELECT s.OffersClientCode,
    s.ShowAvgCosts,
    s.ShowJunkOffers,
	ifnull(cd.RegionCode, c.RegionCode) as OfferRegionCode
FROM retclientsset r
	join OrderSendRules.smart_order_rules s
		left join Usersettings.ClientsData cd on cd.FirmCode = s.OffersClientCode
		left join Future.Clients c on c.Id = s.OffersClientCode
WHERE r.clientcode = ?ClientCode
	and s.id = r.smartorderruleid
	and s.offersclientcode != r.clientcode;", connection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", updateData.ClientId);
			var smartOrderData = new DataSet();
			dataAdapter.Fill(smartOrderData);

			if (smartOrderData.Tables[0].Rows.Count > 0)
			{
				var row = smartOrderData.Tables[0].Rows[0];
				updateData.OffersClientCode = Convert.ToUInt32(row["OffersClientCode"]);
				updateData.ShowAvgCosts = Convert.ToBoolean(row["ShowAvgCosts"]);
				updateData.ShowJunkOffers = Convert.ToBoolean(row["ShowJunkOffers"]);
				if (!(row["OfferRegionCode"] is DBNull))
					updateData.OffersRegionCode = Convert.ToUInt64(row["OfferRegionCode"]);
			}

			return updateData;
		}

		public void SelectActivePrices()
		{
			if (_updateData.IsFutureClient)
			{
				var command = new MySqlCommand("call future.AFGetActivePrices(?UserId);", _readOnlyConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.ExecuteNonQuery();
			}
			else
			{
				var command = new MySqlCommand("call AFGetActivePricesByUserId(?UserId);", _readOnlyConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.ExecuteNonQuery();
			}
		}

		public void SelectActivePricesInMaster()
		{
			if (_updateData.IsFutureClient)
			{
				var command = new MySqlCommand("call future.AFGetActivePrices(?UserId);", _readWriteConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.ExecuteNonQuery();
			}
			else
			{
				var command = new MySqlCommand("call AFGetActivePricesByUserId(?UserId);", _readWriteConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.ExecuteNonQuery();
			}
		}

		public void SelectPrices()
		{
			if (_updateData.IsFutureClient)
			{
				var command = new MySqlCommand("CALL future.GetPrices(?UserId)", _readOnlyConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.ExecuteNonQuery();
			}
			else
			{
				var command = new MySqlCommand("CALL GetPrices2(?ClientCode)", _readOnlyConnection);
				command.Parameters.AddWithValue("?clientCode", _updateData.ClientId);
				command.ExecuteNonQuery();
			}
		}

		public void SelectOffers()
		{
			if (_updateData.IsFutureClient)
			{
				var command = new MySqlCommand("call future.GetOffers(?UserId)", _readOnlyConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.ExecuteNonQuery();
			}
			else
			{
				var command = new MySqlCommand("call GetOffers(?ClientCode, 0)", _readOnlyConnection);
				command.Parameters.AddWithValue("?clientCode", _updateData.ClientId);
				command.ExecuteNonQuery();
			}
		}

		public Reclame GetReclame()
		{
			MySqlCommand command;
			if (_updateData.IsFutureClient)
			{
				command = new MySqlCommand(@"
SELECT r.Region,
       uui.ReclameDate
FROM Future.Clients c
	join Future.Users u on c.Id = u.Clientid
	join farm.regions r on r.RegionCode = c.RegionCode
	join UserUpdateInfo uui on u.Id = uui.UserId
WHERE u.Id = ?UserId", _readOnlyConnection);
			}
			else
			{
				command = new MySqlCommand(@"
SELECT Region,
       ReclameDate
FROM   clientsdata cd,
       farm.regions r,
       UserUpdateInfo UUI,
       OsUserAccessRight OUAR
WHERE  r.regioncode = cd.regioncode
   AND OUAR.RowId = ?UserId
   AND OUAR.Rowid =UUI.UserId
   AND OUAR.ClientCode = cd.firmcode", _readOnlyConnection);
			}
			command.Parameters.AddWithValue("?UserId", _updateData.UserId);
			using (var reader = command.ExecuteReader())
			{
				reader.Read();
				var reclame = new Reclame {
					Region = reader.GetString("region")
				};
				if (!reader.IsDBNull(reader.GetOrdinal("ReclameDate")))
					reclame.ReclameDate = reader.GetDateTime("ReclameDate");
				return reclame;
			}
		}

		public void Cleanup()
		{
			var command = new MySqlCommand("drop temporary table IF EXISTS MaxCodesSynFirmCr, MinCosts, ActivePrices, Prices, Core, tmpprd, MaxCodesSyn, ParentCodes;", _readWriteConnection);
			command.ExecuteNonQuery();
		}

		private void InsistHelper(Action action)
		{
			var iteration = 0;
			var done = false;
			while(!done)
			{
				iteration++;
				var transaction = _readWriteConnection.BeginTransaction();
				try
				{
					action();
					transaction.Commit();
					done = true;
				}
				catch(Exception ex)
				{
					transaction.Rollback();

					if (!ExceptionHelper.IsDeadLockOrSimilarExceptionInChain(ex) || iteration > 3)
						throw;

					Thread.Sleep(500);
				}
			}
		}

		public void UpdateReplicationInfo()
		{
			InsistHelper(() => {
				Cleanup();

				SelectActivePricesInMaster();

				var commandText = @"
CREATE TEMPORARY TABLE MaxCodesSyn engine=MEMORY
SELECT   Prices.FirmCode, 
       MAX(synonym.synonymcode) SynonymCode 
FROM     ActivePrices Prices       , 
       farm.synonym                
WHERE    synonym.pricecode  = PriceSynonymCode 
   AND synonym.synonymcode>MaxSynonymCode 
GROUP BY 1;

CREATE TEMPORARY TABLE MaxCodesSynFirmCr engine=MEMORY 
SELECT   Prices.FirmCode, 
         MAX(synonymfirmcr.synonymfirmcrcode) SynonymCode 
FROM     ActivePrices Prices       , 
         farm.synonymfirmcr          
WHERE    synonymfirmcr.pricecode        =PriceSynonymCode 
     AND synonymfirmcr.synonymfirmcrcode>MaxSynonymfirmcrCode 
GROUP BY 1;

UPDATE AnalitFReplicationInfo AFRI 
SET    UncMaxSynonymFirmCrCode    = 0, 
     UncMaxSynonymCode          = 0 
WHERE  AFRI.UserId                =  ?UserId;

UPDATE AnalitFReplicationInfo AFRI, 
       MaxCodesSynFirmCr            
SET    UncMaxSynonymFirmCrCode    = MaxCodesSynFirmCr.synonymcode 
WHERE  MaxCodesSynFirmCr.FirmCode = AFRI.FirmCode 
   AND AFRI.UserId = ?UserId;

UPDATE AnalitFReplicationInfo AFRI 
SET    ForceReplication    = 2 
WHERE  ForceReplication    = 1 
 AND AFRI.UserId = ?UserId;

UPDATE AnalitFReplicationInfo AFRI, 
       maxcodessyn                  
SET    UncMaxSynonymCode     = maxcodessyn.synonymcode 
WHERE  maxcodessyn.FirmCode  = AFRI.FirmCode 
   AND AFRI.UserId = ?UserId;";
				var command = new MySqlCommand(commandText, _readWriteConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.ExecuteNonQuery();

				Cleanup();
			});
		}

		public DataTable GetProcessedDocuments(uint updateId)
		{
			string command;
			if (_updateData.IsFutureClient)
			{
				command = @"
SELECT  DocumentId,
        DocumentType,
        dl.AddressId as ClientCode
FROM AnalitFDocumentsProcessing AFDP
	join logs.document_logs DL on DL.RowId=AFDP.DocumentId
WHERE AFDP.UpdateId = ?updateId";
			}
			else
			{
				command = @"
SELECT  DocumentId,
        DocumentType,
        ClientCode 
FROM    AnalitFDocumentsProcessing AFDP,
        `logs`.document_logs DL
WHERE   DL.RowId = AFDP.DocumentId
AND     AFDP.UpdateId = ?updateId";
			}
			var dataAdapter = new MySqlDataAdapter(command, _readOnlyConnection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?updateId", updateId);
			var documents = new DataTable();
			dataAdapter.Fill(documents);
			return documents;
		}

		public string GetDocumentsCommand()
		{
			if (_updateData.IsFutureClient)
			{
				return @"
select d.AddressId as ClientCode,
	d.RowId,
	d.DocumentType
from future.Users u
	join future.UserAddresses ua on u.Id = ua.UserId
		join logs.document_logs d on ua.AddressId = d.AddressId
where u.Id = ?UserId
	and d.UpdateId is null
	and d.FirmCode is not null
	and d.Addition IS NULL
	and (d.DocumentType = if(u.SendRejects, 2, 0) or
		d.DocumentType = if(u.SendWaybills, 1, 0) or
		d.DocumentType = 3)
";
			}
			else
			{
				return @"
SELECT  RCS.ClientCode,
        RowId,
        DocumentType
FROM    logs.document_logs d,
        retclientsset RCS
WHERE   RCS.ClientCode = ?ClientCode
    AND RCS.ClientCode=d.ClientCode
    AND UpdateId IS NULL
    AND FirmCode IS NOT NULL
    AND AllowDocuments = 1
    AND d.Addition IS NULL

UNION

SELECT  ir.IncludeClientCode,
		RowId,
        DocumentType
FROM    logs.document_logs d,
        retclientsset RCS,
        includeregulation ir
WHERE   ir.PrimaryClientCode = ?ClientCode
    AND RCS.ClientCode      =ir.IncludeClientCode 
    AND RCS.ClientCode      =d.ClientCode 
    AND UpdateId           IS NULL 
    AND FirmCode           IS NOT NULL 
    AND AllowDocuments      =1 
    AND d.Addition IS NULL 
    AND IncludeType        IN (0,3)
Order by 3";
			}
		}

		public string GetDocumentHeadersCommand(string downloadIds)
		{
			return String.Format(@"
select
  DocumentHeaders.Id,
  DocumentHeaders.DownloadId,
  date_sub(DocumentHeaders.WriteTime, interval time_to_sec(date_sub(now(), interval unix_timestamp() second)) second) as WriteTime,
  DocumentHeaders.FirmCode,
  DocumentHeaders.ClientCode,
  DocumentHeaders.DocumentType,
  DocumentHeaders.ProviderDocumentId,
  DocumentHeaders.OrderId
from
  documents.DocumentHeaders
where
  DocumentHeaders.DownloadId in ({0})
"
				,
				downloadIds);
 		}

		public string GetDocumentBodiesCommand(string downloadIds)
		{
			return String.Format(@"
select
  DocumentBodies.*
from
  documents.DocumentHeaders,
  documents.DocumentBodies
where
    DocumentHeaders.DownloadId in ({0})
and DocumentBodies.DocumentId = DocumentHeaders.Id
"
				,
				downloadIds);
		}

		public string GetUserCommand()
		{
			if (_updateData.IsFutureClient)
			{
				return @"
SELECT a.Id as ClientCode,
	u.Id as RowId,
	'',
    (u.InheritPricesFrom is not null) as InheritPrices,
    1 as IsFutureClient
FROM Future.Users u
  join future.Clients c on u.ClientId = c.Id
  join Future.UserAddresses ua on ua.UserId = u.Id
  join future.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
WHERE u.Id = " + _updateData.UserId +
@"
limit 1";
			}
			else
			{
				return @"
SELECT ClientCode,
	RowId,
	'',
    0 as InheritPrices,
    0 as IsFutureClient
FROM OsUserAccessRight O
WHERE RowId =" + _updateData.UserId;

			}
		}

		public string GetClientsCommand(bool isFirebird)
		{
			if (_updateData.IsFutureClient)
			{
				return String.Format(@"
SELECT a.Id as FirmCode,
     a.Address as ShortName,
     ifnull(?OffersRegionCode, c.RegionCode) as RegionCode,
     rcs.OverCostPercent,
     rcs.DifferenceCalculation,
     rcs.MultiUserLevel,
     rcs.OrderRegionMask,
     {0}
     rcs.CalculateLeader
     {1}
FROM Future.Users u
  join future.Clients c on u.ClientId = c.Id
  join usersettings.RetClientsSet rcs on c.Id = rcs.ClientCode
  join Future.UserAddresses ua on ua.UserId = u.Id
  join future.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
WHERE u.Id = ?UserId", 
					 isFirebird ? "'', " : "",
					 isFirebird ? "" : ", rcs.AllowDelayOfPayment");
			}
			else
			{
				return String.Format(@"
SELECT clientsdata.firmcode,
     ShortName                                         , 
     ifnull(?OffersRegionCode, RegionCode)             , 
     retclientsset.OverCostPercent                     , 
     retclientsset.DifferenceCalculation               , 
     retclientsset.MultiUserLevel                      , 
     retclientsset.OrderRegionMask                     , 
     {0}
     retclientsset.CalculateLeader 
     {1}
FROM   retclientsset, 
     clientsdata 
WHERE  clientsdata.firmcode    = ?ClientCode 
 AND retclientsset.clientcode= clientsdata.firmcode 

UNION 

SELECT clientsdata.firmcode,
     ShortName,
     ifnull(?OffersRegionCode, RegionCode)                                    , 
     retclientsset.OverCostPercent                                           , 
     retclientsset.DifferenceCalculation                                     , 
     retclientsset.MultiUserLevel                                            , 
     IF(IncludeType=3, parent.OrderRegionMask, retclientsset.OrderRegionMask), 
     {0}
     retclientsset.CalculateLeader 
     {1}
FROM   retclientsset       , 
     clientsdata         , 
     retclientsset parent, 
     IncludeRegulation 
WHERE  clientsdata.firmcode    = IncludeClientCode 
 AND retclientsset.clientcode= clientsdata.firmcode 
 AND parent.clientcode       = Primaryclientcode 
 AND firmstatus              = 1 
 AND IncludeType            IN (0,3) 
 AND Primaryclientcode       = ?ClientCode"
					,
					isFirebird ? "'', " : "",
					isFirebird ? "" : ", retclientsset.AllowDelayOfPayment");
			}
		}

		public string GetClientCommand()
		{
			if (_updateData.IsFutureClient)
			{
				return @"
SELECT 
     c.Id as ClientId,
     c.Name
FROM Future.Users u
  join future.Clients c on u.ClientId = c.Id
WHERE u.Id = ?UserId";
			}
			else
			{
				return @"
SELECT 
     clientsdata.firmcode   as ClientId,
     clientsdata.ShortName  as Name 
FROM   
     clientsdata 
WHERE  clientsdata.firmcode    = ?ClientCode";
			}
		}

		public string GetDelayOfPaymentsCommand()
		{
			if (_updateData.IsFutureClient)
			{
				return @"
select
       si.SupplierId   ,
       si.DelayOfPayment
from
       Future.Users u
       join
              future.Clients c
       on
              u.ClientId = c.Id
       join
              Usersettings.SupplierIntersection si
       on
              si.ClientId = c.Id
where
       u.Id = ?UserId";
			}
			else
			{
				return @"
select
       si.SupplierId,
       si.DelayOfPayment
from
       Usersettings.SupplierIntersection si
where
       si.ClientId = ?ClientCode";
			}
		}

		public string GetMNNCommand()
		{
			return @"
select
  Mnn.Id,
  Mnn.Mnn
from
  catalogs.Mnn
where
  if(not ?Cumulative, Mnn.UpdateTime > ?UpdateTime, 1)";
		}

		public string GetDescriptionCommand()
		{
			return @"
select
  Descriptions.Id,
  Descriptions.Name,
  Descriptions.EnglishName,
  Descriptions.Description,
  Descriptions.Interaction, 
  Descriptions.SideEffect, 
  Descriptions.IndicationsForUse, 
  Descriptions.Dosing, 
  Descriptions.Warnings, 
  Descriptions.ProductForm, 
  Descriptions.PharmacologicalAction, 
  Descriptions.Storage, 
  Descriptions.Expiration, 
  Descriptions.Composition
from
  catalogs.Descriptions
where
  if(not ?Cumulative, Descriptions.UpdateTime > ?UpdateTime, 1)";
		}

		public string GetCoreCommand(bool exportInforoomPrice, bool exportSupplierPriceMarkup)
		{
			if (exportInforoomPrice)
				return @"
SELECT 2647                             ,
       ?OffersRegionCode                ,
       A.ProductId                      ,
       A.CodeFirmCr                     ,
       S.SynonymCode                    ,
       SF.SynonymFirmCrCode             ,
       ''                               ,
       ''                               ,
       ''                               ,
       ''                               ,
       0                                ,
       0                                ,
       ''                               ,
       ''                               ,
       ''                               ,
       ''                               ,
       ''                               ,
       0                                ,
       ''                               ,
       IF(?ShowAvgCosts, a.Cost, '') ,
       @RowId := @RowId + 1             ,
       ''                               ,
       ''
FROM   farm.Synonym S        ,
       farm.SynonymFirmCr SF ,
       CoreT A
WHERE  S.PriceCode            =2647
AND    SF.PriceCode           =2647
AND    S.ProductId            =A.ProductId
AND    SF.CodeFirmCr          =A.CodeFirmCr
AND    A.CodeFirmCr IS NOT NULL

UNION

SELECT 2647                              ,
       ?OffersRegionCode                 ,
       A.ProductId                       ,
       1                                 ,
       S.SynonymCode                     ,
       0                                 ,
       ''                                ,
       ''                                ,
       ''                                ,
       ''                                ,
       0                                 ,
       0                                 ,
       ''                                ,
       ''                                ,
       ''                                ,
       ''                                ,
       ''                                ,
       0                                 ,
       ''                                ,
       IF(?ShowAvgCosts, A.Cost, ''),
       @RowId := @RowId + 1              ,
       ''                                ,
       ''
FROM   farm.Synonym S ,
       CoreTP A
WHERE  S.PriceCode =2647
AND    S.ProductId =A.ProductId";
			else
				return 
				String.Format(@"
SELECT CT.PriceCode               ,
       CT.regioncode              ,
       CT.ProductId               ,
       ifnull(Core.codefirmcr, 0) ,
       Core.synonymcode           ,
       Core.SynonymFirmCrCode     ,
       Core.Code                  ,
       Core.CodeCr                ,
       Core.unit                  ,
       Core.volume                ,
       Core.Junk                  ,
       Core.Await                 ,
       Core.quantity              ,
       Core.note                  ,
       Core.period                ,
       Core.doc                   ,
       Core.RegistryCost          ,
       Core.VitallyImportant      ,
       Core.RequestRatio          ,
       CT.Cost                    ,
       RIGHT(CT.ID, 9)            ,
       OrderCost                  ,
       MinOrderCount
       {0}
FROM   Core CT        ,
       ActivePrices AT,
       farm.core0 Core
WHERE  ct.pricecode =at.pricecode
AND    ct.regioncode=at.regioncode
AND    Core.id      =CT.id
AND    IF(?Cumulative, 1, fresh)"
				,
				exportSupplierPriceMarkup ? ", if((Core.ProducerCost is null) or (Core.ProducerCost = 0), null, (CT.Cost/Core.ProducerCost-1)*100) " : "");
		}

		public void UpdatePriceSettings(int[] priceIds, long[] regionIds, bool[] injobs)
		{
			var deleteCommand = new MySqlCommand("delete from Future.UserPrices where PriceId = ?PriceId and UserId = ?UserId and RegionId = ?RegionId", _readWriteConnection);
			deleteCommand.Parameters.AddWithValue("?UserId", _updateData.UserId);
			deleteCommand.Parameters.Add("?PriceId", MySqlDbType.UInt32);
			deleteCommand.Parameters.Add("?RegionId", MySqlDbType.UInt64);
			var insertCommand = new MySqlCommand(@"
insert into Future.UserPrices(UserId, PriceId, RegionId)
select ?UserId, ?PriceId, ?RegionId
from (select 1) as c
where not exists (
	select *
	from Future.UserPrices up
	where up.UserId = ?UserId and up.PriceId = ?PriceId and up.RegionId = ?RegionId
);", _readWriteConnection);
			insertCommand.Parameters.AddWithValue("?UserId", _updateData.UserId);
			insertCommand.Parameters.Add("?PriceId", MySqlDbType.UInt32);
			insertCommand.Parameters.Add("?RegionId", MySqlDbType.UInt64);
			for(var i = 0; i < injobs.Length; i++)
			{
				MySqlCommand command;
				if (injobs[i])
					command = insertCommand;
				else
					command = deleteCommand;
				command.Parameters["?PriceId"].Value = priceIds[i];
				command.Parameters["?RegionId"].Value = regionIds[i];
				command.ExecuteNonQuery();
			}
		}
	}
}