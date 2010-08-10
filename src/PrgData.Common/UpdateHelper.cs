using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Web;
using MySql.Data.MySqlClient;
using System.Threading;
using Common.MySql;
using log4net;

namespace PrgData.Common
{
	public class UpdateData
	{
		public string ShortName;
		public uint ClientId;
		public uint UserId;
		public string UserName;
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
		private readonly bool _updateToTestBuild;

		public bool ClientEnabled;
		public bool UserEnabled;

		public uint? BuyingMatrixPriceId;
		public int BuyingMatrixType;
		public bool WarningOnBuyingMatrix;

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
			_updateToTestBuild = Convert.ToBoolean(row["UpdateToTestBuild"]);
			ClientEnabled = Convert.ToBoolean(row["ClientEnabled"]);
			UserEnabled = Convert.ToBoolean(row["UserEnabled"]);
			BuyingMatrixPriceId = Convert.IsDBNull(row["BuyingMatrixPriceId"])
			                      	? null
			                      	: (uint?) Convert.ToUInt32(row["BuyingMatrixPriceId"]);
			BuyingMatrixType = Convert.ToInt32(row["BuyingMatrixType"]);
			WarningOnBuyingMatrix = Convert.ToBoolean(row["WarningOnBuyingMatrix"]);
		}

		public bool Disabled()
		{
			return !ClientEnabled || !UserEnabled;
		}

		public string[] GetUpdateFiles(string result, int build)
		{
			return GetUpdateFiles(GetUpdateFilesPath(result, build), "exe");
		}

		public string[] GetFrfUpdateFiles(string result, int build)
		{
			return GetUpdateFiles(GetUpdateFilesPath(result, build), "frf");
		}

		private string GetUpdateFilesPath(string result, int build)
		{
			if (EnableUpdate && _updateToTestBuild)
			{
				var testPath = result + @"Updates\test_" + build + @"\";
				if (Directory.Exists(testPath))
					return testPath;
			}
			return result + @"Updates\Future_" + build + @"\";
		}

		private string[] GetUpdateFiles(string path, string sufix)
		{
			path += sufix;
			if (Directory.Exists(path))
				return Directory.GetFiles(path);
			return new string[0];
		}
	}

	public class Reclame
	{
		public string Region { get; set; }
		public DateTime ReclameDate { get; set;}
		public bool ShowAdvertising { get; set; }

		public Reclame()
		{
			ReclameDate = new DateTime(2003, 1, 1);
		}
	}

	public class UpdateHelper
	{
		private UpdateData _updateData;
		private MySqlConnection _readWriteConnection;

		//Код поставщика 7664
		public uint MaxProducerCostsPriceId { get; private set; }
		private uint maxProducerCostsCostId;

		public UpdateHelper(UpdateData updateData, MySqlConnection readWriteConnection)
		{
			MaxProducerCostsPriceId = 4863;
			maxProducerCostsCostId = 8317;
			_updateData = updateData;
			_readWriteConnection = readWriteConnection;
		}

		public static Func<string> GetDownloadUrl =
			() => HttpContext.Current.Request.Url.Scheme
				+ Uri.SchemeDelimiter
				+ HttpContext.Current.Request.Url.Authority
				+ HttpContext.Current.Request.ApplicationPath;

		public string GetConfirmDocumentsCommnad(uint? updateId)
		{
			if (!_updateData.IsFutureClient)
			{
				return String.Format(@"
UPDATE AnalitFDocumentsProcessing A, `logs`.document_logs d 
SET d.UpdateId = A.UpdateId 
WHERE d.RowId = A.DocumentId 
	AND A.UpdateId = {0};

DELETE 
FROM AnalitFDocumentsProcessing 
WHERE UpdateId = {0};", updateId);
			}
			else
			{
				return @"
update Logs.DocumentSendLogs ds
set ds.Committed = 1
where ds.updateid = " + updateId;
			}
		}

		public bool DefineMaxProducerCostsCostId()
		{
			var costId = MySql.Data.MySqlClient.MySqlHelper.ExecuteScalar(_readWriteConnection, @"
select CostCode 
from 
  usersettings.PricesCosts,
  usersettings.PriceItems
where
    (PricesCosts.PriceCode = ?PriceCode)
and (PriceItems.Id = PricesCosts.PriceItemId)
and (PricesCosts.CostCode = ?CostCode)
"
				,
				new MySqlParameter("?PriceCode", MaxProducerCostsPriceId),
				new MySqlParameter("?CostCode", maxProducerCostsCostId));
			return (costId != null);
		}

		public bool MaxProducerCostIsFresh()
		{
			var fresh = MySql.Data.MySqlClient.MySqlHelper.ExecuteScalar(_readWriteConnection, @"
select Id 
from 
  usersettings.PricesCosts,
  usersettings.PriceItems
where
    (PricesCosts.CostCode = ?CostCode)
and (PriceItems.Id = PricesCosts.PriceItemId)
and (PriceItems.LastFormalization > ?UpdateTime)
"
				,
				new MySqlParameter("?CostCode", maxProducerCostsCostId),
				new MySqlParameter("?UpdateTime", _updateData.OldUpdateTime));
			return (fresh != null);
		}

		public string GetMaxProducerCostsCommand()
		{
			return String.Format(@"
select
  c.Id,
  p.CatalogId,
  c.ProductId,
  s.Synonym,
  sfc.Synonym,
  c.Note,
  ifnull(c.CodeFirmCr, 0) as ProducerId,
  cc.Cost as RealCost
from
  (
  farm.Core0 c,
  farm.CoreCosts cc,
  catalogs.products p,
  farm.synonym s
  )
  left join farm.SynonymFirmCr sfc on sfc.SynonymFirmCrCode = c.SynonymFirmCrCode
where
    (c.PriceCode = {0})
and (cc.Core_Id = c.Id)
and (cc.PC_CostCode = {1})
and (p.Id = c.ProductId)
and (s.SynonymCode = c.SynonymCode)
"
				,
				MaxProducerCostsPriceId,
				maxProducerCostsCostId);
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
SELECT 
  r.regioncode,
  left(r.region, 25) as region
FROM future.Clients c
	join farm.regions r on r.RegionCode & c.maskregion > 0
where c.Id = ?ClientCode
";
			}
			var command = @"
SELECT 
  regions.regioncode,
  left(regions.region, 25) as region
FROM farm.regions, clientsdata
WHERE firmcode = ifnull(?OffersClientCode, ?ClientCode)
AND (regions.regioncode & maskregion > 0)";

			if (!_updateData.ShowJunkOffers)
			{
				command += @"
UNION 
SELECT regions.regioncode,
       left(regions.region, 25) as region
FROM   farm.regions,
       clientsdata
WHERE firmcode = ?ClientCode
AND regions.regioncode= clientsdata.regioncode 

UNION

SELECT DISTINCT regions.regioncode,
                left(regions.region, 25) as region
FROM            farm.regions,
                includeregulation,
                clientsdata 
WHERE includeclientcode = ?ClientCode
            AND firmcode          = primaryclientcode
            AND includetype      IN (1, 2)
            AND regions.regioncode & clientsdata.maskregion > 0

UNION

SELECT regions.regioncode,
       left(regions.region, 25) as region
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
SELECT  
    c.Id ClientId,
	u.Id UserId,
	rui.UpdateDate,
	rui.UncommitedUpdateDate,
	IF(rui.MessageShowCount < 1, '', rui.MESSAGE) Message,
	retclientsset.CheckCopyId,
	'' Future,
    c.Name as ShortName,
    retclientsset.Spy, 
    retclientsset.SpyAccount,
    retclientsset.BuyingMatrixPriceId,
    retclientsset.BuyingMatrixType,
    retclientsset.WarningOnBuyingMatrix,
    u.EnableUpdate,
    c.Status as ClientEnabled,
    (u.Enabled and ap.UserId is not null) as UserEnabled,
	0 as UpdateToTestBuild
FROM  
  future.users u
  join future.Clients c                         on c.Id = u.ClientId
  join usersettings.retclientsset               on retclientsset.clientcode = c.Id
  join usersettings.UserUpdateInfo rui          on rui.UserId = u.Id 
  join usersettings.UserPermissions up          on up.Shortcut = 'AF'
  left join usersettings.AssignedPermissions ap on ap.UserId = u.Id and ap.PermissionId = up.Id
WHERE 
   u.Login = ?user"
				, 
				connection);
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
        retclientsset.CheckCopyID,
        clientsdata.ShortName,
        retclientsset.Spy, 
        retclientsset.SpyAccount,
        retclientsset.EnableUpdate,
		retclientsset.UpdateToTestBuild,
		retclientsset.BuyingMatrixPriceId,
		retclientsset.BuyingMatrixType,
		retclientsset.WarningOnBuyingMatrix,
        clientsdata.firmstatus as ClientEnabled,
        (ap.UserId is not null and IF(ir.id IS NULL, 1, ir.IncludeType IN (1,2,3))) as UserEnabled
FROM    
  usersettings.osuseraccessright ouar
  join usersettings.clientsdata                 on clientsdata.firmcode = ouar.clientcode
  join usersettings.retclientsset               on retclientsset.clientcode = ouar.clientcode 
  join usersettings.UserUpdateInfo rui          on rui.UserId = ouar.RowId
  join usersettings.UserPermissions up          on up.Shortcut = 'AF'
  left join usersettings.AssignedPermissions ap on ap.UserId = ouar.rowid and ap.PermissionId = up.Id
  left join usersettings.IncludeRegulation ir   on ir.IncludeClientCode = ouar.ClientCode
WHERE   
    ouar.OSUserName = ?user
";
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
			updateData.UserName = userName;
			return updateData;
		}

		public void SelectActivePrices()
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
				var command = new MySqlCommand("CALL future.GetPrices(?UserId)", _readWriteConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.ExecuteNonQuery();
			}
			else
			{
				var command = new MySqlCommand("CALL GetPrices2(?ClientCode)", _readWriteConnection);
				command.Parameters.AddWithValue("?clientCode", _updateData.ClientId);
				command.ExecuteNonQuery();
			}
		}

		public void SelectOffers()
		{
			if (_updateData.IsFutureClient)
			{
				var command = new MySqlCommand("call future.GetOffers(?UserId)", _readWriteConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.ExecuteNonQuery();
			}
			else
			{
				var command = new MySqlCommand("call GetOffers(?ClientCode, 0)", _readWriteConnection);
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
       uui.ReclameDate,
       rcs.ShowAdvertising
FROM Future.Clients c
	join Future.Users u on c.Id = u.Clientid
    join usersettings.RetClientsSet rcs on rcs.ClientCode = u.Clientid
	join farm.regions r on r.RegionCode = c.RegionCode
	join UserUpdateInfo uui on u.Id = uui.UserId
WHERE u.Id = ?UserId", _readWriteConnection);
			}
			else
			{
				command = new MySqlCommand(@"
SELECT r.Region,
       UUI.ReclameDate,
       rcs.ShowAdvertising
FROM   clientsdata cd,
       usersettings.RetClientsSet rcs,
       farm.regions r,
       UserUpdateInfo UUI,
       OsUserAccessRight OUAR
WHERE  r.regioncode = cd.regioncode
   and rcs.ClientCode = cd.FirmCode
   AND OUAR.RowId = ?UserId
   AND OUAR.Rowid =UUI.UserId
   AND OUAR.ClientCode = cd.firmcode", _readWriteConnection);
			}
			command.Parameters.AddWithValue("?UserId", _updateData.UserId);
			using (var reader = command.ExecuteReader())
			{
				reader.Read();
				var reclame = new Reclame {
					Region = reader.GetString("region"),
					ShowAdvertising = reader.GetBoolean("ShowAdvertising")
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

		public void UpdateReplicationInfo()
		{
			With.DeadlockWraper(() => {
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

		public void PrepareLimitedCumulative(DateTime oldUpdateTime)
		{
			With.DeadlockWraper(() => {
				Cleanup();

				SelectActivePricesInMaster();

				var commandText = @"

UPDATE AnalitFReplicationInfo AFRI 
SET    ForceReplication    = 1 
WHERE  
  AFRI.UserId = ?UserId;

CREATE TEMPORARY TABLE MaxCodesSyn engine=MEMORY
SELECT   
  Prices.FirmCode, 
  max(SynonymLogs.synonymcode) SynonymCode 
FROM     
  ActivePrices Prices, 
  logs.SynonymLogs
WHERE    
      SynonymLogs.pricecode = PriceSynonymCode
  and SynonymLogs.LogTime >= (?oldUpdateTime - interval ?Depth day)
  and SynonymLogs.LogTime < ?oldUpdateTime 
GROUP BY 1;

CREATE TEMPORARY TABLE MaxCodesSynFirmCr engine=MEMORY 
SELECT   
  Prices.FirmCode, 
  max(SynonymFirmCrLogs.synonymfirmcrcode) SynonymCode 
FROM     
  ActivePrices Prices, 
  logs.SynonymFirmCrLogs
WHERE    
      SynonymFirmCrLogs.pricecode = PriceSynonymCode 
  and SynonymFirmCrLogs.LogTime >= (?oldUpdateTime - interval ?Depth day)
  AND SynonymFirmCrLogs.LogTime < ?oldUpdateTime 
GROUP BY 1;

UPDATE AnalitFReplicationInfo AFRI 
SET    UncMaxSynonymFirmCrCode    = 0, 
     UncMaxSynonymCode          = 0 
WHERE  AFRI.UserId                =  ?UserId;

UPDATE AnalitFReplicationInfo AFRI, 
       MaxCodesSynFirmCr            
SET    MaxSynonymFirmCrCode    = MaxCodesSynFirmCr.synonymcode 
WHERE  MaxCodesSynFirmCr.FirmCode = AFRI.FirmCode 
   AND AFRI.UserId = ?UserId
  and AFRI.MaxSynonymFirmCrCode > MaxCodesSynFirmCr.synonymcode;

UPDATE AnalitFReplicationInfo AFRI, 
       maxcodessyn                  
SET    MaxSynonymCode     = maxcodessyn.synonymcode 
WHERE  maxcodessyn.FirmCode  = AFRI.FirmCode 
  AND AFRI.UserId = ?UserId
  and AFRI.MaxSynonymCode > maxcodessyn.synonymcode;";
				var command = new MySqlCommand(commandText, _readWriteConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.Parameters.AddWithValue("?oldUpdateTime", oldUpdateTime);
				command.Parameters.AddWithValue("?Depth", System.Configuration.ConfigurationManager.AppSettings["AccessTimeHistoryDepth"]);
				command.ExecuteNonQuery();

				Cleanup();
			});
		}

		public DataTable GetProcessedDocuments(uint updateId)
		{
				var command = @"
SELECT  DocumentId,
        DocumentType,
        ClientCode 
FROM    AnalitFDocumentsProcessing AFDP,
        `logs`.document_logs DL
WHERE   DL.RowId = AFDP.DocumentId
AND     AFDP.UpdateId = ?updateId";
			var dataAdapter = new MySqlDataAdapter(command, _readWriteConnection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?updateId", updateId);
			var documents = new DataTable();
			dataAdapter.Fill(documents);
			return documents;
		}

		public string GetDocumentsCommand()
		{
			if (_updateData.IsFutureClient)
			{
				//начинаем отдавать документы с самых новых что бы 
				//отдать наиболее актуальные
				return @"
select d.AddressId as ClientCode,
	d.RowId,
	d.DocumentType
from Logs.DocumentSendLogs ds
	join Logs.Document_logs d on d.RowId = ds.DocumentId
where ds.UserId = ?UserId 
	and ds.Committed = 0
	and d.LogTime > curdate() - interval 30 day
order by d.LogTime desc
limit 100;
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
			if (_updateData.IsFutureClient)
			{
				return String.Format(@"
select
  DocumentHeaders.Id,
  DocumentHeaders.DownloadId,
  DocumentHeaders.DocumentDate as WriteTime,
  DocumentHeaders.FirmCode,
  DocumentHeaders.AddressId as ClientCode,
  DocumentHeaders.DocumentType,
  DocumentHeaders.ProviderDocumentId,
  DocumentHeaders.OrderId
from
  documents.DocumentHeaders,
  future.Clients,
  farm.regions
where
    DocumentHeaders.DownloadId in ({0})
and (Clients.Id = DocumentHeaders.ClientCode)
and (regions.RegionCode = Clients.RegionCode)
"
					,
					downloadIds);
			}
			else
			{
				return String.Format(@"
select
  DocumentHeaders.Id,
  DocumentHeaders.DownloadId,
  date_sub(date_sub(DocumentHeaders.DocumentDate, interval time_to_sec(date_sub(now(), interval unix_timestamp() second)) second), interval regions.MoscowBias hour) as WriteTime,
  DocumentHeaders.FirmCode,
  DocumentHeaders.ClientCode,
  DocumentHeaders.DocumentType,
  DocumentHeaders.ProviderDocumentId,
  DocumentHeaders.OrderId
from
  documents.DocumentHeaders,
  usersettings.clientsdata,
  farm.regions
where
  DocumentHeaders.DownloadId in ({0})
and (clientsdata.FirmCode = DocumentHeaders.ClientCode)
and (regions.RegionCode = clientsdata.RegionCode)
"
					,
					downloadIds);
			}
 		}

		public string GetDocumentBodiesCommand(string downloadIds)
		{
			return String.Format(@"
select
  DocumentBodies.Id, 
  DocumentBodies.DocumentId, 
  DocumentBodies.Product, 
  DocumentBodies.Code, 
  DocumentBodies.Certificates, 
  DocumentBodies.Period, 
  DocumentBodies.Producer, 
  DocumentBodies.Country, 
  DocumentBodies.ProducerCost, 
  DocumentBodies.RegistryCost, 
  DocumentBodies.SupplierPriceMarkup, 
  DocumentBodies.SupplierCostWithoutNDS, 
  DocumentBodies.SupplierCost, 
  DocumentBodies.Quantity, 
  DocumentBodies.VitallyImportant, 
  DocumentBodies.NDS,
  DocumentBodies.SerialNumber
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
SELECT 
    a.Id as ClientCode,
	u.Id as RowId,
	'',
    (u.InheritPricesFrom is not null) as InheritPrices,
    1 as IsFutureClient
FROM 
  Future.Users u
  join future.Clients c on u.ClientId = c.Id
  left join Future.UserAddresses ua on ua.UserId = u.Id
  left join future.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
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
     right(a.Address, 50) as ShortName,
     ifnull(?OffersRegionCode, c.RegionCode) as RegionCode,
     rcs.OverCostPercent,
     rcs.DifferenceCalculation,
     rcs.MultiUserLevel,
     (rcs.OrderRegionMask & u.OrderRegionMask) OrderRegionMask,
     {0}
     rcs.CalculateLeader
     {1}
FROM Future.Users u
  join future.Clients c on u.ClientId = c.Id
  join usersettings.RetClientsSet rcs on c.Id = rcs.ClientCode
  join Future.UserAddresses ua on ua.UserId = u.Id
  join future.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
WHERE 
    u.Id = ?UserId
and a.Enabled = 1", 
					 isFirebird ? "'', " : "",
					 isFirebird ? "" : ", rcs.AllowDelayOfPayment, c.FullName ");
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
					isFirebird ? "" : ", retclientsset.AllowDelayOfPayment, clientsdata.FullName ");
			}
		}

		public string GetClientCommand()
		{
			if (_updateData.IsFutureClient)
			{
				return @"
SELECT 
     c.Id as ClientId,
     left(c.Name, 50) as Name,
     regions.CalculateOnProducerCost,
     rcs.ParseWaybills,
     rcs.SendRetailMarkup,
     rcs.ShowAdvertising,
     rcs.SendWaybillsFromClient,
     rcs.EnableSmartOrder
FROM Future.Users u
  join future.Clients c on u.ClientId = c.Id
  join farm.regions on regions.RegionCode = c.RegionCode
  join usersettings.RetClientsSet rcs on rcs.ClientCode = c.Id
WHERE u.Id = ?UserId";
			}
			else
			{
				return @"
SELECT 
     clientsdata.firmcode   as ClientId,
     clientsdata.ShortName  as Name, 
     regions.CalculateOnProducerCost,
     rcs.ParseWaybills,
     rcs.SendRetailMarkup,
     rcs.ShowAdvertising,
     rcs.SendWaybillsFromClient,
     rcs.EnableSmartOrder
FROM   
     clientsdata 
     join farm.regions on regions.RegionCode = clientsdata.RegionCode
     join usersettings.RetClientsSet rcs on rcs.ClientCode = clientsdata.FirmCode
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

		public string GetMNNCommand(bool before1150, bool Cumulative)
		{
			if (before1150)
			{
				return @"
select
  Mnn.Id,
  Mnn.Mnn,
  Mnn.RussianMnn
from
  catalogs.Mnn
where
  if(not ?Cumulative, Mnn.UpdateTime > ?UpdateTime, 1)";
			}
			else
				if (Cumulative)
				return @"
select
  Mnn.Id,
  Mnn.Mnn,
  Mnn.RussianMnn,
  0 as Hidden
from
  catalogs.Mnn";
			else
				return @"
select
  Mnn.Id,
  Mnn.Mnn,
  Mnn.RussianMnn,
  0 as Hidden
from
  catalogs.Mnn
where
  Mnn.UpdateTime > ?UpdateTime
union
select
  MnnLogs.MnnId,
  MnnLogs.Mnn,
  MnnLogs.RussianMnn,
  1 as Hidden
from
  logs.MnnLogs
where
    (MnnLogs.LogTime >= ?UpdateTime) 
and (MnnLogs.Operation = 2)
";
		}

		public string GetDescriptionCommand(bool before1150, bool Cumulative)
		{
			if (before1150)
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
			else
				if (Cumulative)
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
  Descriptions.Composition,
  0 as Hidden
from
  catalogs.Descriptions";
			else
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
  Descriptions.Composition,
  0 as Hidden
from
  catalogs.Descriptions
where
  Descriptions.UpdateTime > ?UpdateTime
union
select
  DescriptionLogs.DescriptionId,
  DescriptionLogs.Name,
  DescriptionLogs.EnglishName,
  DescriptionLogs.Description,
  DescriptionLogs.Interaction, 
  DescriptionLogs.SideEffect, 
  DescriptionLogs.IndicationsForUse, 
  DescriptionLogs.Dosing, 
  DescriptionLogs.Warnings, 
  DescriptionLogs.ProductForm, 
  DescriptionLogs.PharmacologicalAction, 
  DescriptionLogs.Storage, 
  DescriptionLogs.Expiration, 
  DescriptionLogs.Composition,
  1 as Hidden
from
  logs.DescriptionLogs
where
    (DescriptionLogs.LogTime >= ?UpdateTime) 
and (DescriptionLogs.Operation = 2)
";
		}

		public string GetProducerCommand(bool Cumulative)
		{
			if (Cumulative)
				return @"
	select
	  Producers.Id,
	  Producers.Name,
      0 as Hidden
	from
	  catalogs.Producers
	where
		(Producers.Id > 1)";
			else
				return @"
	select
	  Producers.Id,
	  Producers.Name,
      0 as Hidden
	from
	  catalogs.Producers
	where
		(Producers.Id > 1)
	and Producers.UpdateTime > ?UpdateTime
    union
    select
	  ProducerLogs.ProducerId,
	  ProducerLogs.Name,
      1 as Hidden
    from
      logs.ProducerLogs
    where
        (ProducerLogs.LogTime >= ?UpdateTime) 
    and (ProducerLogs.Operation = 2)        
      ";
		}

		public string GetCatalogCommand(bool before1150, bool Cumulative)
		{
			if (before1150)
			{
				return @"
SELECT C.Id               ,
       CN.Id              ,
       LEFT(CN.name, 250) ,
       LEFT(CF.form, 250) ,
       C.vitallyimportant ,
       C.needcold         ,
       C.fragile          ,
       C.MandatoryList    ,
       CN.MnnId           ,
       CN.DescriptionId
FROM   Catalogs.Catalog C       ,
       Catalogs.CatalogForms CF ,
       Catalogs.CatalogNames CN
WHERE  C.NameId =CN.Id
AND    C.FormId =CF.Id
AND
       (
              IF(NOT ?Cumulative, C.UpdateTime  > ?UpdateTime, 1)
       OR     IF(NOT ?Cumulative, CN.UpdateTime > ?UpdateTime, 1)
       )
AND    C.hidden =0";
			}
			else
				if (Cumulative)
				return @"
SELECT C.Id               ,
       CN.Id              ,
       LEFT(CN.name, 250) ,
       LEFT(CF.form, 250) ,
       C.vitallyimportant ,
       C.needcold         ,
       C.fragile          ,
       C.MandatoryList    ,
       CN.MnnId           ,
       CN.DescriptionId   ,
       C.Hidden
FROM   Catalogs.Catalog C       ,
       Catalogs.CatalogForms CF ,
       Catalogs.CatalogNames CN
WHERE  C.NameId =CN.Id
AND    C.FormId =CF.Id
AND    C.hidden =0
";
			else
				return @"
SELECT C.Id               ,
       CN.Id              ,
       LEFT(CN.name, 250) ,
       LEFT(CF.form, 250) ,
       C.vitallyimportant ,
       C.needcold         ,
       C.fragile          ,
       C.MandatoryList    ,
       CN.MnnId           ,
       CN.DescriptionId   ,
       C.Hidden
FROM   Catalogs.Catalog C       ,
       Catalogs.CatalogForms CF ,
       Catalogs.CatalogNames CN
WHERE  C.NameId =CN.Id
AND    C.FormId =CF.Id
AND
       (
              IF(NOT ?Cumulative, C.UpdateTime  > ?UpdateTime, 1)
       OR     IF(NOT ?Cumulative, CN.UpdateTime > ?UpdateTime, 1)
       )
";
		}

		public string GetCoreCommand(bool exportInforoomPrice, bool exportSupplierPriceMarkup, bool exportBuyingMatrix)
		{
			string buyingMatrixCondition;
			if (_updateData.BuyingMatrixPriceId.HasValue)
			{
				if (_updateData.BuyingMatrixType == 0)
					//белый список
					buyingMatrixCondition = ", if(list.Id is not null, 0, " + (_updateData.WarningOnBuyingMatrix ? "2" : "1") + ") as BuyingMatrixType";
				else
					//черный список
					buyingMatrixCondition = ", if(list.Id is null, 0, " + (_updateData.WarningOnBuyingMatrix ? "2" : "1") + ") as BuyingMatrixType";
			}
			else
				//разрешено все
				buyingMatrixCondition = ", 0 as BuyingMatrixType";

			if (exportInforoomPrice)
				if (!exportSupplierPriceMarkup)
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
						String.Format(
@"
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
       null as RegistryCost              ,
       0 as VitallyImportant             ,
       null as RequestRatio              ,
       IF(?ShowAvgCosts, a.Cost, '') ,
       @RowId := @RowId + 1             ,
       null as OrderCost                 ,
       null as MinOrderCount             ,
       null as SupplierPriceMarkup       ,
       null as ProducerCost              ,
       null as NDS
      {0}
FROM   
       (
       farm.Synonym S        ,
       farm.SynonymFirmCr SF ,
       CoreT A
       )
      {1}
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
       1                                 ,
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
       null as RegistryCost              ,
       0 as VitallyImportant             ,
       null as RequestRatio              ,
       IF(?ShowAvgCosts, A.Cost, ''),
       @RowId := @RowId + 1              ,
       null as OrderCost                 ,
       null as MinOrderCount             ,
       null as SupplierPriceMarkup       ,
       null as ProducerCost              ,
       null as NDS
       {0}
FROM   
       (
       farm.Synonym S ,
       CoreTP A
       )
       {2}
WHERE  S.PriceCode =2647
AND    S.ProductId =A.ProductId
"
	,
		exportBuyingMatrix ? buyingMatrixCondition : "",
		exportBuyingMatrix && _updateData.BuyingMatrixPriceId.HasValue ? @" 
  left join catalogs.Products on Products.Id = A.ProductId
  left join farm.BuyingMatrix list on list.CatalogId = Products.CatalogId and if(list.ProducerId is null, 1, if(a.CodeFirmCr is null, 0, list.ProducerId = a.CodeFirmCr)) and list.PriceId = " + _updateData.BuyingMatrixPriceId : "",
		exportBuyingMatrix && _updateData.BuyingMatrixPriceId.HasValue ? @" 
  left join catalogs.Products on Products.Id = A.ProductId
  left join farm.BuyingMatrix list on list.CatalogId = Products.CatalogId and if(list.ProducerId is null, 1, 0) and list.PriceId = " + _updateData.BuyingMatrixPriceId : ""
	 );
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
       {1}
FROM   
       (
       Core CT        ,
       ActivePrices AT,
       farm.core0 Core
       )
       {2}
WHERE  ct.pricecode =at.pricecode
AND    ct.regioncode=at.regioncode
AND    Core.id      =CT.id
AND    IF(?Cumulative, 1, fresh)"
				,
				exportSupplierPriceMarkup ? @"
, 
if((Core.ProducerCost is null) or (Core.ProducerCost = 0), 
   null, 
   if((Core.NDS is null) or (Core.NDS <= 0), 
     (CT.Cost/(Core.ProducerCost*1.1)-1)*100,
     (CT.Cost/(Core.ProducerCost*(1 + Core.NDS/100))-1)*100
   )
),
Core.ProducerCost,
Core.NDS " : "",
				exportSupplierPriceMarkup && exportBuyingMatrix ? buyingMatrixCondition : "",
				exportSupplierPriceMarkup && exportBuyingMatrix && _updateData.BuyingMatrixPriceId.HasValue ? @" 
  left join catalogs.Products on Products.Id = CT.ProductId
  left join farm.BuyingMatrix list on list.CatalogId = Products.CatalogId and if(list.ProducerId is null, 1, if(Core.CodeFirmCr is null, 0, list.ProducerId = Core.CodeFirmCr)) and list.PriceId = " + _updateData.BuyingMatrixPriceId : ""
);
		}

		public string GetSynonymFirmCrCommand(bool Cumulative)
		{ 
			var sql = @"
SELECT synonymfirmcr.synonymfirmcrcode,
       LEFT(SYNONYM, 250)
FROM   farm.synonymfirmcr,
       ParentCodes
WHERE  synonymfirmcr.pricecode = ParentCodes.PriceCode";
			if (!Cumulative)
				sql += " AND synonymfirmcr.synonymfirmcrcode > MaxSynonymFirmCrCode ";
			sql += @"
UNION

SELECT 1,
       '-'
";

			return sql;
		}

		public string GetSynonymCommand(bool Cumulative)
		{
			var sql = @"
SELECT 
  synonym.synonymcode, 
  LEFT(synonym.synonym, 250) 
FROM   
  farm.synonym,
  ParentCodes 
WHERE  
  synonym.pricecode = ParentCodes.PriceCode ";

			if (!Cumulative)
				sql += " AND synonym.synonymcode > MaxSynonymCode ";

			return sql;
		}

		public void UpdatePriceSettings(int[] priceIds, long[] regionIds, bool[] injobs)
		{
			With.DeadlockWraper(() =>
			{
				var transaction = _readWriteConnection.BeginTransaction();
				try
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
					for (var i = 0; i < injobs.Length; i++)
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

					InsertAnalitFUpdatesLog(transaction.Connection, _updateData, RequestType.PostPriceDataSettings);

					transaction.Commit();
				}
				catch
				{
					ConnectionHelper.SafeRollback(transaction);
					throw;
				}
			});
			
		}

		public bool NeedClientToAddressMigration()
		{
			var userId = MySql.Data.MySqlClient.MySqlHelper.ExecuteScalar(_readWriteConnection, @"
select UserId 
from 
  future.ClientToAddressMigrations
where
    (UserId = ?UserId)
limit 1
"
				,
				new MySqlParameter("?UserId", _updateData.UserId));
			return (userId != null);
		}

		public string GetClientToAddressMigrationCommand()
		{
			return @"
select 
  ClientCode, AddressId
from 
  future.ClientToAddressMigrations
where
    UserId = " + _updateData.UserId;
		}

		public string GetMinReqRuleCommand()
		{
			if (_updateData.IsFutureClient)
				return @"
select
  a.Id as ClientId,
  i.PriceId as PriceCode,
  i.RegionId as RegionCode,
  ai.ControlMinReq,
  if(ai.MinReq > 0, ai.MinReq, Prices.MinReq) as MinReq 
from
  Future.Users u
  join future.Clients c on u.ClientId = c.Id
  join Future.UserAddresses ua on ua.UserId = u.Id
  join future.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
  join future.Intersection i on i.ClientId = c.Id
  join future.AddressIntersection ai on (ai.IntersectionId = i.Id) and (ai.AddressId = a.Id)
  join Prices on (Prices.PriceCode = i.PriceId) and (Prices.RegionCode = i.RegionId)
where
  (u.Id = ?UserId)";
			else
				return @"
select
  clients.FirmCode as ClientId,
  Prices.PriceCode,
  Prices.RegionCode,
  Prices.ControlMinReq,
  Prices.MinReq
from
  (
SELECT
  clientsdata.firmcode
FROM
  clientsdata
WHERE
  clientsdata.firmcode    = ?ClientCode
UNION
SELECT
  clientsdata.firmcode
FROM
     clientsdata         ,
     IncludeRegulation
WHERE
     clientsdata.firmcode                 = IncludeRegulation.IncludeClientCode
 AND clientsdata.firmstatus               = 1
 AND IncludeRegulation.IncludeType        IN (0,3)
 AND IncludeRegulation.Primaryclientcode  = ?ClientCode
 ) clients,
 Prices
  ";
		}

		public void OldCommit(string absentPriceCodes)
		{
			var commitCommand =
				String.Format(
@"
UPDATE AnalitFReplicationInfo
SET    ForceReplication =0
WHERE  UserId           = {0}
AND    ForceReplication =2;

UPDATE UserUpdateInfo
SET    UpdateDate      =UncommitedUpdateDate,
       MessageShowCount=IF(MessageShowCount > 0, MessageShowCount - 1, 0)
WHERE  UserId          = {0};
"
					,
					_updateData.UserId);

			if (String.IsNullOrEmpty(absentPriceCodes))
				commitCommand += 
					String.Format(@"
UPDATE AnalitFReplicationInfo
SET    MaxSynonymFirmCrCode    =UncMaxSynonymFirmCrCode
WHERE  UncMaxSynonymFirmCrCode!=0
AND    UserId                  = {0};

UPDATE AnalitFReplicationInfo
SET    MaxSynonymCode    =UncMaxSynonymCode
WHERE  UncMaxSynonymCode!=0
AND    UserId            = {0};
"
						,
						_updateData.UserId);
			else
			{
				commitCommand +=
				String.Format(@"
UPDATE AnalitFReplicationInfo ARI,
       PricesData Pd
SET    MaxSynonymFirmCrCode   =0,
       MaxSynonymCode         =0,
       UncMaxSynonymCode      =0,
       UncMaxSynonymFirmCrCode=0
WHERE  UserId                 = {0}
AND    Pd.FirmCode            =ARI.FirmCode
AND    Pd.PriceCode IN ( {1} );

UPDATE AnalitFReplicationInfo ARI,
       PricesData Pd
SET    ARI.MaxSynonymFirmCrCode    =ARI.UncMaxSynonymFirmCrCode
WHERE  ARI.UncMaxSynonymFirmCrCode!=0
AND    ARI.UserId                  = {0}
AND    Pd.FirmCode            =ARI.FirmCode
AND    not (Pd.PriceCode IN ( {1} ));

UPDATE AnalitFReplicationInfo ARI,
       PricesData Pd
SET    ARI.MaxSynonymCode    =ARI.UncMaxSynonymCode
WHERE  ARI.UncMaxSynonymCode!=0
AND    ARI.UserId            = {0}
AND    Pd.FirmCode            =ARI.FirmCode
AND    not (Pd.PriceCode IN ( {1} ));
"
					,
					_updateData.UserId,
					absentPriceCodes);
			}

			ProcessCommitCommand(commitCommand);
		}

		public void ResetAbsentPriceCodes(string absentPriceCodes)
		{
			var commitCommand = 
				String.Format(@"
UPDATE AnalitFReplicationInfo ARI,
       PricesData Pd
SET    MaxSynonymFirmCrCode   =0,
       MaxSynonymCode         =0,
       UncMaxSynonymCode      =0,
       UncMaxSynonymFirmCrCode=0
WHERE  UserId                 = {0}
AND    Pd.FirmCode            =ARI.FirmCode
AND    Pd.PriceCode IN ( {1} );"
					,
					_updateData.UserId,
					absentPriceCodes);

			ProcessCommitCommand(commitCommand);
		}

		public void CommitExchange()
		{
			var commitCommand = 
				String.Format(
@"
UPDATE AnalitFReplicationInfo
SET    ForceReplication =0
WHERE  UserId           = {0}
AND    ForceReplication =2;

UPDATE UserUpdateInfo
SET    UpdateDate      =UncommitedUpdateDate,
       MessageShowCount=IF(MessageShowCount > 0, MessageShowCount - 1, 0)
WHERE  UserId          = {0};

UPDATE AnalitFReplicationInfo
SET    MaxSynonymFirmCrCode    =UncMaxSynonymFirmCrCode
WHERE  UncMaxSynonymFirmCrCode!=0
AND    UserId                  = {0};

UPDATE AnalitFReplicationInfo
SET    MaxSynonymCode    =UncMaxSynonymCode
WHERE  UncMaxSynonymCode!=0
AND    UserId            = {0};
"
					,
					_updateData.UserId);

			ProcessCommitCommand(commitCommand);
		}

		public DateTime GetCurrentUpdateDate(RequestType updateType)
		{
			return With.DeadlockWraper(() => {
				var transaction = _readWriteConnection.BeginTransaction();
				try
				{
					var command = new MySqlCommand("", _readWriteConnection);
					if (updateType != RequestType.ResumeData)
						command.CommandText = @"update UserUpdateInfo set UncommitedUpdateDate=now() where UserId = ?userId; ";

					command.CommandText += "select UncommitedUpdateDate from UserUpdateInfo where UserId = ?userId;";

					command.Parameters.AddWithValue("?UserId", _updateData.UserId);

					DateTime updateTime;
					using (var reader = command.ExecuteReader())
					{
						reader.Read();
						updateTime = reader.GetDateTime(0);
					}

					transaction.Commit();
					return updateTime;
				}
				catch
				{
					ConnectionHelper.SafeRollback(transaction);
					throw;
				}
			});
		}

		private void ProcessCommitCommand(string commitCommand)
		{
			With.DeadlockWraper(() => {
				var transaction = _readWriteConnection.BeginTransaction();
				try
				{
					MySql.Data.MySqlClient.MySqlHelper.ExecuteNonQuery(_readWriteConnection, commitCommand);
					transaction.Commit();
				}
				catch
				{
					ConnectionHelper.SafeRollback(transaction);
					throw;
				}
			});
		}

		public static void InsertAnalitFUpdatesLog(MySqlConnection connection, UpdateData updateData, RequestType request)
		{
			MySql.Data.MySqlClient.MySqlHelper.ExecuteScalar(
				connection,
				@"
insert into logs.AnalitFUpdates 
  (RequestTime, UpdateType, UserId, Commit) 
values 
  (now(), ?UpdateType, ?UserId, 1);
"
				,
				new MySqlParameter("?UpdateType", (int) request),
				new MySqlParameter("?UserId", updateData.UserId));
		}

		public void SetForceReplication()
		{
			With.DeadlockWraper(() =>
			{
				var commandText = @"

UPDATE AnalitFReplicationInfo AFRI 
SET    ForceReplication    = 1 
WHERE  
  AFRI.UserId = ?UserId;
";
				var command = new MySqlCommand(commandText, _readWriteConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.ExecuteNonQuery();
			});
		}

		public bool NeedUpdateToBuyingMatrix(string resultPath, int build)
		{
			try
			{
				if (_updateData.EnableUpdate && build >= 1183 && build <= 1229)
				{
					var exeName = Array.Find(_updateData.GetUpdateFiles(resultPath, build), item => item.EndsWith("AnalitF.exe", StringComparison.OrdinalIgnoreCase));
					var info = FileVersionInfo.GetVersionInfo(exeName);
					return info.FilePrivatePart >= 1249;
				}
			}
			catch 
			{
			}
			return false;
		}

	}
}