using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Web;
using Common.Models;
using Common.Models.Helpers;
using Common.MySql;
using MySql.Data.MySqlClient;
using System.Threading;
using NHibernate;
using NHibernate.Linq;
using NHibernate.Type;
using PrgData.Common.Models;
using log4net;
using PrgData.Common.Orders;
using PrgData.Common.SevenZip;
using MySqlHelper = MySql.Data.MySqlClient.MySqlHelper;
using System.Collections.Generic;
using Common.Tools;
using With = Common.MySql.With;

namespace PrgData.Common
{
	public class Reclame
	{
		public string Region { get; set; }
		public ulong RegionCode { get; set; }
		public DateTime ReclameDate { get; set; }
		public bool ShowAdvertising { get; set; }
		public UpdateData UpdateData { get; set; }

		public string DefaultReclameFolder
		{
			get { return Region + "_" + RegionCode; }
		}

		public Reclame()
		{
			ReclameDate = new DateTime(2003, 1, 1);
		}

		public string[] ExcludeFileNames(string[] fileNames)
		{
			if (UpdateData == null)
				throw new Exception("Не установлено свойство UpdateData");

			var excludeList = UpdateData.AllowMatchWaybillsToOrders() ? new string[] { "01.htm", "02.htm", "2b.gif", "Inforrom-logo.gif" } : new string[] { "index.htm", "2block.gif" };

			return (from file in fileNames
				let extractFileName = Path.GetFileName(file)
				let fileInfo = new FileInfo(file)
				where (fileInfo.Attributes & FileAttributes.Hidden) == 0 && !excludeList.Any(f => f.Equals(extractFileName, StringComparison.OrdinalIgnoreCase))
				select file).ToArray();
		}

		public string[] GetReclameFiles(string reclamePath)
		{
			if (Directory.Exists(reclamePath))
				return ExcludeFileNames(Directory.GetFiles(reclamePath));

			return new string[] { };
		}

		public void SetUncommitedReclameDate(MySqlConnection connection, DateTime uncommitedReclameDate)
		{
			MySqlHelper.ExecuteNonQuery(connection,
				"update UserSettings.UserUpdateInfo set UncommitedReclameDate = ?UncommitedReclameDate where UserId = ?UserId",
				new MySqlParameter("?UserId", UpdateData.UserId),
				new MySqlParameter("?UncommitedReclameDate", uncommitedReclameDate));
		}
	}

	public class UpdateHelper
	{
		private UpdateData _updateData;
		private MySqlConnection _readWriteConnection;
		private ILog log;
		private uint maxProducerCostsCostId;

		public static Func<string> GetDownloadUrl =
			() => HttpContext.Current.Request.Url.Scheme
				+ Uri.SchemeDelimiter
				+ HttpContext.Current.Request.Url.Authority
				+ HttpContext.Current.Request.ApplicationPath;

		public UpdateHelper(UpdateData updateData, MySqlConnection readWriteConnection)
		{
			log = LogManager.GetLogger(typeof(UpdateHelper));
			MaxProducerCostsPriceId = 4863;
			maxProducerCostsCostId = 8148;
			_updateData = updateData;
			_readWriteConnection = readWriteConnection;

			if (_updateData.MissingProductIds.Count > 0) {
				var cmd = new MySqlCommand(String.Format(@"
select
	cast(min(least(p.UpdateTime, c.UpdateTime, CN.UpdateTime)) as datetime)
from
	catalogs.Products p
	inner join catalogs.Catalog c on c.Id = p.CatalogId
	inner join Catalogs.CatalogNames CN on CN.Id = c.NameId
where
	p.Id in ({0})
and p.Hidden = 0", _updateData.MissingProductIds.Implode()), _readWriteConnection);
				var catalogUpdateTime = cmd.ExecuteScalar();
				if (!(catalogUpdateTime is DBNull))
					_updateData.CatalogUpdateTime = Convert.ToDateTime(catalogUpdateTime).AddDays(-7);
			}
		}

		public static ISessionFactory SessionFactory
		{
			get
			{
				return IoC.Resolve<ISessionFactoryHolder>().SessionFactory;
			}
		}

		public MySqlConnection ReadWriteConnection
		{
			get { return _readWriteConnection; }
		}

		//Код поставщика 7664
		public uint MaxProducerCostsPriceId { get; private set; }

		public static string GetFullUrl(string handlerName)
		{
			var downloadUrl = GetDownloadUrl();
			if (downloadUrl.EndsWith("/"))
				downloadUrl = downloadUrl.Slice(downloadUrl.Length - 1);
			return downloadUrl + "/" + handlerName;
		}

		public static string GetConfirmDocumentsCommnad(uint? updateId)
		{
			return @"
update Logs.DocumentSendLogs ds
set ds.Committed = 1
where ds.updateid = "
				+ updateId;
		}

		public bool DefineMaxProducerCostsCostId()
		{
			var costId = MySqlHelper.ExecuteScalar(_readWriteConnection, @"
select CostCode 
from 
  usersettings.PricesCosts,
  usersettings.PriceItems
where
	(PricesCosts.PriceCode = ?PriceCode)
and (PriceItems.Id = PricesCosts.PriceItemId)
and (PricesCosts.CostCode = ?CostCode)
",
				new MySqlParameter("?PriceCode", MaxProducerCostsPriceId),
				new MySqlParameter("?CostCode", maxProducerCostsCostId));
			return (costId != null);
		}

		public bool MaxProducerCostIsFresh()
		{
			var fresh = MySqlHelper.ExecuteScalar(_readWriteConnection, @"
select Id 
from 
  usersettings.PricesCosts,
  usersettings.PriceItems
where
	(PricesCosts.CostCode = ?CostCode)
and (PriceItems.Id = PricesCosts.PriceItemId)
and (PriceItems.LastFormalization > ?UpdateTime)
",
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
",
				MaxProducerCostsPriceId,
				maxProducerCostsCostId);
		}

		public void MaintainReplicationInfo()
		{
			var command = new MySqlCommand();

			command.Connection = _readWriteConnection;

			if (_updateData.EnableImpersonalPrice) {
				command.CommandText = @"
INSERT
INTO   Usersettings.AnalitFReplicationInfo 
	   (
			  UserId,
			  FirmCode,
			  ForceReplication
	   )
SELECT u.Id,
	   supplier.Id,
	   1
FROM Customers.Clients drugstore
	JOIN Customers.Users u ON u.ClientId = drugstore.Id
	JOIN Customers.Suppliers supplier ON supplier.regionmask & ?OffersRegionCode > 0
	LEFT JOIN Usersettings.AnalitFReplicationInfo ari ON ari.UserId   = u.Id AND ari.FirmCode = supplier.Id
WHERE ari.UserId IS NULL 
	AND drugstore.Id = ?ClientCode
GROUP BY u.Id, supplier.Id;";

				command.Parameters.AddWithValue("?OffersRegionCode", _updateData.OffersRegionCode);
			}
			else
				command.CommandText = @"
INSERT
INTO   Usersettings.AnalitFReplicationInfo 
	   (
			  UserId,
			  FirmCode,
			  ForceReplication
	   )
SELECT u.Id,
	   supplier.Id,
	   1
FROM Customers.Clients drugstore
	JOIN Customers.Users u ON u.ClientId = drugstore.Id
	JOIN Customers.Suppliers supplier ON supplier.regionmask & drugstore.maskregion > 0
	LEFT JOIN Usersettings.AnalitFReplicationInfo ari ON ari.UserId   = u.Id AND ari.FirmCode = supplier.Id
WHERE ari.UserId IS NULL 
	AND drugstore.Id = ?ClientCode
GROUP BY u.Id, supplier.Id;
";
			command.Parameters.AddWithValue("?ClientCode", _updateData.ClientId);

			command.ExecuteNonQuery();
		}


		public string GetRegionsCommand()
		{
			if (_updateData.EnableImpersonalPrice)
				return @"
SELECT 
  r.regioncode,
  left(r.region, 25) as region
FROM 
   farm.regions r
where r.RegionCode = ?OffersRegionCode
";
			return @"
SELECT 
  r.regioncode,
  left(r.region, 25) as region
FROM Customers.Clients c
	join farm.regions r on r.RegionCode & c.maskregion > 0
where c.Id = ?ClientCode
";
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
	rui.AFAppVersion as KnownBuildNumber,
	rui.AFCopyId as KnownUniqueID,
	if(rui.MessageShowCount < 1, '', rui.MESSAGE) Message,
	u.TargetVersion,
	u.SaveAFDataFiles,
	retclientsset.CheckCopyId,
	c.Name as ShortName,
	retclientsset.Spy, 
	retclientsset.SpyAccount,
	retclientsset.BuyingMatrixPriceId,
	retclientsset.BuyingMatrixType,
	retclientsset.WarningOnBuyingMatrix,
	retclientsset.EnableImpersonalPrice,
	retclientsset.NetworkPriceId,
	retclientsset.ShowAdvertising,
	retclientsset.OfferMatrixPriceId,
	retclientsset.OfferMatrixType,
	retclientsset.AllowAnalitFSchedule,
	c.Status as ClientEnabled,
	u.Enabled as UserEnabled,
	u.AllowDownloadUnconfirmedOrders,
	u.SendWaybills,
	u.SendRejects,
	u.WorkRegionMask as RegionMask,
	ap.UserId is not null as AFPermissionExists,
	buyingMat.MatrixUpdateTime as BuyingMatrixUpdateTime,
	offerMat.MatrixUpdateTime as OfferMatrixUpdateTime
FROM  
  Customers.users u
  join Customers.Clients c                         on c.Id = u.ClientId
  join usersettings.retclientsset               on retclientsset.clientcode = c.Id
  left join farm.matrices buyingMat on buyingMat.Id = retclientsset.BuyingMatrix
  left join farm.matrices offerMat on offerMat.Id = retclientsset.OfferMatrix
  join usersettings.UserUpdateInfo rui          on rui.UserId = u.Id 
  join usersettings.UserPermissions up          on up.Shortcut = 'AF'
  left join usersettings.AssignedPermissions ap on ap.UserId = u.Id and ap.PermissionId = up.Id
WHERE 
   u.Login = ?user;
select
	AnalitFUpdates.UpdateId,
	AnalitFUpdates.RequestTime,
	AnalitFUpdates.UpdateType,
	AnalitFUpdates.Commit,
	AnalitFUpdates.Addition
from
	logs.AnalitFUpdates,
	Customers.users u
where
	u.Login = ?user
and AnalitFUpdates.UserId = u.Id
and AnalitFUpdates.RequestTime > curdate() - interval 1 day
and AnalitFUpdates.UpdateType IN (1, 2, 10, 16, 17, 18, 19) 
order by AnalitFUpdates.UpdateId desc
limit 1;",
				connection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?user", userName);

			var data = new DataSet();
			dataAdapter.Fill(data);

			if (data.Tables[0].Rows.Count > 0) {
				var id = Convert.ToUInt32(data.Tables[0].Rows[0]["ClientId"]);
				OrderRules settings;
				using(var session = SessionFactory.OpenSession(connection)) {
					settings = session.Load<OrderRules>(id);
					NHibernateUtil.Initialize(settings);
				}
				updateData = new UpdateData(data, settings);
			}

			if (updateData == null)
				return null;

			var offersSql = @"
SELECT 
  s.OffersClientCode,
  c.RegionCode as OfferRegionCode
FROM retclientsset r
	join OrderSendRules.smart_order_rules s
	left join Customers.Users u on u.Id = s.OffersClientCode
	left join Customers.Clients c on c.Id = u.ClientId
WHERE r.clientcode = ?ClientCode
	and s.id = r.smartorderruleid
	and s.offersclientcode != r.clientcode;";

			dataAdapter = new MySqlDataAdapter(offersSql, connection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", updateData.ClientId);

			var smartOrderData = new DataSet();
			dataAdapter.Fill(smartOrderData);

			if (smartOrderData.Tables[0].Rows.Count > 0) {
				var row = smartOrderData.Tables[0].Rows[0];
				updateData.OffersClientCode = Convert.ToUInt32(row["OffersClientCode"]);
				if (!(row["OfferRegionCode"] is DBNull))
					updateData.OffersRegionCode = Convert.ToUInt64(row["OfferRegionCode"]);
			}
			updateData.UserName = userName;
			return updateData;
		}

		public void SelectActivePrices()
		{
			if (_updateData.EnableImpersonalPrice) {
				var command = new MySqlCommand(@"
DROP TEMPORARY TABLE IF EXISTS ActivePrices;
create temporary table ActivePrices ENGINE = MEMORY as select * from Prices;
", _readWriteConnection);
				command.Parameters.AddWithValue("?OffersClientCode", _updateData.OffersClientCode);
				command.ExecuteNonQuery();
			}
			else {
				var command = new MySqlCommand("call Customers.AFGetActivePrices(?UserId);", _readWriteConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.ExecuteNonQuery();
			}
		}

		public void SelectPrices()
		{
			if (_updateData.EnableImpersonalPrice) {
				var command = new MySqlCommand("call Customers.GetPrices(?OffersClientCode)", _readWriteConnection);
				command.Parameters.AddWithValue("?OffersClientCode", _updateData.OffersClientCode);
				command.ExecuteNonQuery();
			}
			else {
				var command = new MySqlCommand("CALL Customers.GetPrices(?UserId)", _readWriteConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.ExecuteNonQuery();
			}
		}

		public void SelectOffers()
		{
			var command = new MySqlCommand("call Customers.GetOffers(?UserId)", _readWriteConnection);
			command.Parameters.AddWithValue("?UserId", _updateData.UserId);
			command.ExecuteNonQuery();
		}

		public Reclame GetReclame()
		{
			var command = new MySqlCommand(@"
SELECT r.Region,
	   uui.ReclameDate,
	   rcs.ShowAdvertising,
		r.RegionCode
FROM Customers.Clients c
	join Customers.Users u on c.Id = u.Clientid
	join usersettings.RetClientsSet rcs on rcs.ClientCode = u.Clientid
	join farm.regions r on r.RegionCode = c.RegionCode
	join UserUpdateInfo uui on u.Id = uui.UserId
WHERE u.Id = ?UserId", _readWriteConnection);

			command.Parameters.AddWithValue("?UserId", _updateData.UserId);
			using (var reader = command.ExecuteReader()) {
				reader.Read();
				var reclame = new Reclame {
					Region = reader.GetString("region"),
					RegionCode = reader.GetUInt64("RegionCode"),
					ShowAdvertising = reader.GetBoolean("ShowAdvertising"),
					UpdateData = _updateData
				};
				if (_updateData.UpdateExeVersionInfo == null && !reader.IsDBNull(reader.GetOrdinal("ReclameDate")))
					reclame.ReclameDate = reader.GetDateTime("ReclameDate");
				return reclame;
			}
		}

		public void Cleanup()
		{
			var command = new MySqlCommand("drop temporary table IF EXISTS MaxCodesSynFirmCr, MinCosts, ActivePrices, Prices, Core, PriceCounts, MaxCodesSyn, ParentCodes, CurrentReplicationInfo;", _readWriteConnection);
			command.ExecuteNonQuery();
		}

		public void SelectReplicationInfo()
		{
			var commandText = @"
drop temporary table IF EXISTS CurrentReplicationInfo;

CREATE TEMPORARY TABLE CurrentReplicationInfo engine=MEMORY
SELECT   
  Prices.FirmCode, 
  AFRI.UserId,
  MAX(AFRI.ForceReplicationUpdate) CurrentForceReplicationUpdate,
  AFRI.ForceReplication,
  AFRI.MaxSynonymCode,
  AFRI.MaxSynonymFirmCrCode
FROM     
  Prices       , 
  AnalitFReplicationInfo AFRI
WHERE    
	AFRI.UserId                =  ?UserId
and Prices.FirmCode = AFRI.FirmCode
GROUP BY 1;";

			var command = new MySqlCommand(commandText, _readWriteConnection);
			command.Parameters.AddWithValue("?UserId", _updateData.UserId);
			command.ExecuteNonQuery();
		}

		public void FillParentCodes()
		{
			if (!_updateData.EnableImpersonalPrice) {
				var commandText = @"
CREATE TEMPORARY TABLE ParentCodes ENGINE=memory
		SELECT   PriceSynonymCode PriceCode,
				 MaxSynonymCode            ,
				 MaxSynonymFirmCrCode
		FROM     ActivePrices Prices
		GROUP BY 1;";

				var command = new MySqlCommand(commandText, _readWriteConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.ExecuteNonQuery();
			}
		}

		public void UpdateReplicationInfo()
		{
			//Cleanup();
			var commandclear = new MySqlCommand("drop temporary table IF EXISTS MaxCodesSynFirmCr, MaxCodesSyn;",
				_readWriteConnection);
			commandclear.ExecuteNonQuery();

			var commandText = String.Empty;

			if (!_updateData.EnableImpersonalPrice)
				commandText +=
					@"
CREATE TEMPORARY TABLE MaxCodesSyn engine=MEMORY
SELECT   Prices.FirmCode, 
	   MAX(synonym.synonymcode) SynonymCode 
FROM     ActivePrices Prices       , 
	   farm.synonym                
WHERE    synonym.pricecode  = Prices.PriceSynonymCode 
   AND synonym.synonymcode > Prices.MaxSynonymCode 
GROUP BY 1;

CREATE TEMPORARY TABLE MaxCodesSynFirmCr engine=MEMORY 
SELECT   Prices.FirmCode, 
		 MAX(synonymfirmcr.synonymfirmcrcode) SynonymCode 
FROM     ActivePrices Prices       , 
		 farm.synonymfirmcr          
WHERE    synonymfirmcr.pricecode        = Prices.PriceSynonymCode 
	 AND synonymfirmcr.synonymfirmcrcode > Prices.MaxSynonymfirmcrCode 
GROUP BY 1;";

			commandText +=
				@"
UPDATE 
  AnalitFReplicationInfo AFRI,
  CurrentReplicationInfo 
SET    AFRI.ForceReplication    = 2 
WHERE  AFRI.ForceReplication    = 1 
 AND AFRI.UserId = ?UserId
and CurrentReplicationInfo.FirmCode = AFRI.FirmCode
and CurrentReplicationInfo.ForceReplication = 1
and CurrentReplicationInfo.CurrentForceReplicationUpdate = AFRI.ForceReplicationUpdate;

UPDATE AnalitFReplicationInfo AFRI 
SET    UncMaxSynonymFirmCrCode    = 0, 
	 UncMaxSynonymCode          = 0 
WHERE  AFRI.UserId                =  ?UserId;";

			if (!_updateData.EnableImpersonalPrice)
				commandText +=
					@"
UPDATE AnalitFReplicationInfo AFRI, 
	   MaxCodesSynFirmCr            
SET    UncMaxSynonymFirmCrCode    = MaxCodesSynFirmCr.synonymcode 
WHERE  MaxCodesSynFirmCr.FirmCode = AFRI.FirmCode 
   AND AFRI.UserId = ?UserId;

UPDATE AnalitFReplicationInfo AFRI, 
	   maxcodessyn                  
SET    UncMaxSynonymCode     = maxcodessyn.synonymcode 
WHERE  maxcodessyn.FirmCode  = AFRI.FirmCode 
   AND AFRI.UserId = ?UserId;";

			commandText +=
				@"
drop temporary table IF EXISTS CurrentReplicationInfo;";

			var command = new MySqlCommand(commandText, _readWriteConnection);
			command.Parameters.AddWithValue("?UserId", _updateData.UserId);
			command.ExecuteNonQuery();

			Cleanup();
		}

		public void PrepareLimitedCumulative(DateTime oldUpdateTime)
		{
			With.DeadlockWraper(() => {
				Cleanup();

				SelectActivePricesFull();

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
  and AFRI.MaxSynonymCode > maxcodessyn.synonymcode;

update
  logs.AnalitFUpdates afu,
  Logs.DocumentSendLogs ds
set
  ds.Committed = 0,
  ds.FileDelivered = 0, 
  ds.DocumentDelivered = 0
where
	afu.RequestTime > ?oldUpdateTime
and afu.UserId = ?UserId
and ds.UpdateId = afu.UpdateId
and ds.UserId = afu.UserId
and ds.Committed = 1;
update
  logs.AnalitFUpdates afu,
  Logs.MailSendLogs ms
set
  ms.Committed = 0
where
	afu.RequestTime > ?oldUpdateTime
and afu.UserId = ?UserId
and ms.UpdateId = afu.UpdateId
and ms.UserId = afu.UserId
and ms.Committed = 1;

update
  logs.AnalitFUpdates afu,
  Logs.UnconfirmedOrdersSendLogs sendlogs,
  orders.OrdersHead
set
  sendlogs.Committed = 0,
  sendlogs.UpdateId = null,
  OrdersHead.Deleted = 0
where
	afu.RequestTime > ?oldUpdateTime
and afu.UserId = ?UserId
and sendlogs.UpdateId = afu.UpdateId
and sendlogs.UserId = afu.UserId
and sendlogs.Committed = 1
and OrdersHead.RowId = sendlogs.OrderId;

";

				var command = new MySqlCommand(commandText, _readWriteConnection);
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.Parameters.AddWithValue("?oldUpdateTime", oldUpdateTime);
				command.Parameters.AddWithValue("?Depth", System.Configuration.ConfigurationManager.AppSettings["AccessTimeHistoryDepth"]);
				command.ExecuteNonQuery();

				Cleanup();
			});
		}

		public void SelectActivePricesFull()
		{
			SelectPrices();
			SelectReplicationInfo();
			SelectActivePrices();
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
			//начинаем отдавать документы с самых новых что бы 
			//отдать наиболее актуальные
			return @"
select d.AddressId as ClientCode,
	d.RowId,
	d.DocumentType,
	d.IsFake,
	d.SendUpdateId,
	d.LogTime
from Logs.DocumentSendLogs ds
	join Logs.Document_logs d on d.RowId = ds.DocumentId
where ds.UserId = ?UserId 
	and ds.Committed = 0
order by d.LogTime desc
limit 800;
";
		}

		public string GetDocumentHeadersCommand(string downloadIds)
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
  Customers.Clients,
  farm.regions
where
	DocumentHeaders.DownloadId in ({0})
and (Clients.Id = DocumentHeaders.ClientCode)
and (regions.RegionCode = Clients.RegionCode)
",
				downloadIds);
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
  {1}
  {2}
  {3}
from
  documents.DocumentHeaders,
  documents.DocumentBodies
where
	DocumentHeaders.DownloadId in ({0})
and DocumentBodies.DocumentId = DocumentHeaders.Id
",
				downloadIds,
				!_updateData.AllowDelayWithVitallyImportant()
					? String.Empty
					: @"
  ,
  DocumentBodies.Amount,
  DocumentBodies.NdsAmount",
				!_updateData.AllowInvoiceHeaders
					? String.Empty
					: @"
  ,
  DocumentBodies.Unit,
  DocumentBodies.ExciseTax,
  DocumentBodies.BillOfEntryNumber,
  DocumentBodies.EAN13",
				!_updateData.AllowCertificates
					? String.Empty
					: @"
  ,
  DocumentBodies.ProductId,
  DocumentBodies.ProducerId");
		}

		public string GetWaybillOrdersCommand(string downloadIds)
		{
			return String.Format(@"
select
  WaybillOrders.DocumentLineId as ServerDocumentLineId,
  WaybillOrders.OrderLineId as ServerOrderListId
from
  documents.DocumentHeaders,
  documents.DocumentBodies,
  documents.WaybillOrders
where
	DocumentHeaders.DownloadId in ({0})
and DocumentBodies.DocumentId = DocumentHeaders.Id
and WaybillOrders.DocumentLineId = DocumentBodies.Id",
				downloadIds);
		}

		public string GetInvoiceHeadersCommand(string downloadIds)
		{
			return String.Format(@"
select
	InvoiceHeaders.Id,
	InvoiceHeaders.InvoiceNumber,
	InvoiceHeaders.InvoiceDate,
	InvoiceHeaders.SellerName,
	InvoiceHeaders.SellerAddress,
	InvoiceHeaders.SellerINN,
	InvoiceHeaders.SellerKPP,
	InvoiceHeaders.ShipperInfo,
	InvoiceHeaders.ConsigneeInfo,
	InvoiceHeaders.PaymentDocumentInfo,
	InvoiceHeaders.BuyerName,
	InvoiceHeaders.BuyerAddress,
	InvoiceHeaders.BuyerINN,
	InvoiceHeaders.BuyerKPP,
	InvoiceHeaders.AmountWithoutNDS0,
	InvoiceHeaders.AmountWithoutNDS10,
	InvoiceHeaders.NDSAmount10,
	InvoiceHeaders.Amount10,
	InvoiceHeaders.AmountWithoutNDS18,
	InvoiceHeaders.NDSAmount18,
	InvoiceHeaders.Amount18,
	InvoiceHeaders.AmountWithoutNDS,
	InvoiceHeaders.NDSAmount,
	InvoiceHeaders.Amount
from
	documents.DocumentHeaders
	inner join documents.InvoiceHeaders on InvoiceHeaders.Id = DocumentHeaders.Id
where
	DocumentHeaders.DownloadId in ({0})
",
				downloadIds);
		}

		public string GetUserCommand()
		{
			return @"
SELECT 
	a.Id as ClientCode,
	u.Id as RowId,
	'',
	(u.InheritPricesFrom is not null) as InheritPrices,
	1 as IsFutureClient,
	u.UseAdjustmentOrders,
	u.ShowSupplierCost
FROM 
  Customers.Users u
  join Customers.Clients c on u.ClientId = c.Id
  left join Customers.UserAddresses ua on ua.UserId = u.Id
  left join Customers.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
WHERE u.Id = " + _updateData.UserId +
				@"
limit 1";
		}

		public string GetClientsCommand()
		{
			var networkSelfAddressIdColumn = " , null as SelfAddressId ";
			var networkSelfAddressIdJoin = String.Empty;
			if (_updateData.NetworkPriceId.HasValue) {
				networkSelfAddressIdColumn = _updateData.NetworkPriceId.HasValue ? ", ai.SupplierDeliveryId as SelfAddressId " : ", a.Id as SelfAddressId";
				networkSelfAddressIdJoin =
					_updateData.NetworkPriceId.HasValue
						? " left join Customers.Intersection i on i.ClientId = a.ClientId and i.RegionId = c.RegionCode and i.LegalEntityId = a.LegalEntityId and i.PriceId = " +
							_updateData.NetworkPriceId +
							" left join Customers.AddressIntersection ai on ai.IntersectionId = i.Id and ai.AddressId = a.Id  "
						: "";
			}

			if (_updateData.BuildNumber > 1271 || _updateData.NeedUpdateToNewClientsWithLegalEntity) {
				var clientShortNameField = "right(a.Address, 255)";
				var orgCount = MySqlHelper.ExecuteScalar(_readWriteConnection, @"
	SELECT 
		count(distinct le.Id)
	FROM 
	Customers.Users u
	  join Customers.Clients c on u.ClientId = c.Id
	  join Customers.Addresses a on c.Id = a.ClientId
	  join billing.LegalEntities le on le.Id = a.LegalEntityId
	WHERE 
		u.Id = ?UserId
	and a.Enabled = 1
", new MySqlParameter("?UserId", _updateData.UserId));
				if (Convert.ToInt32(orgCount) > 1)
					clientShortNameField = "concat(left(le.Name, 100), ', ', right(a.Address, 153))";

				return String.Format(
					@"
	SELECT a.Id as FirmCode,
		 {0} as ShortName,
		 ifnull(?OffersRegionCode, c.RegionCode) as RegionCode,
		 rcs.OverCostPercent,
		 rcs.DifferenceCalculation,
		 rcs.MultiUserLevel,
		 (rcs.OrderRegionMask & u.OrderRegionMask) OrderRegionMask,
		 rcs.CalculateLeader, 
		 rcs.AllowDelayOfPayment, 
		 c.FullName
		{1}
		{3}
	FROM Customers.Users u
	  join Customers.Clients c on u.ClientId = c.Id
	  join usersettings.RetClientsSet rcs on c.Id = rcs.ClientCode
	  join Customers.UserAddresses ua on ua.UserId = u.Id
	  join Customers.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
	  join billing.LegalEntities le on le.Id = a.LegalEntityId
	  {2}
	WHERE 
		u.Id = ?UserId
	and a.Enabled = 1
",
					clientShortNameField,
					networkSelfAddressIdColumn,
					networkSelfAddressIdJoin,
					_updateData.AllowExcessAvgOrderTimes ? " , rcs.ExcessAvgOrderTimes " : "");
			}
			return String.Format(@"
	SELECT a.Id as FirmCode,
		 right(a.Address, 50) as ShortName,
		 ifnull(?OffersRegionCode, c.RegionCode) as RegionCode,
		 rcs.OverCostPercent,
		 rcs.DifferenceCalculation,
		 rcs.MultiUserLevel,
		 (rcs.OrderRegionMask & u.OrderRegionMask) OrderRegionMask,
		 rcs.CalculateLeader,
		 rcs.AllowDelayOfPayment,
		 c.FullName
		 {0}
	FROM Customers.Users u
	  join Customers.Clients c on u.ClientId = c.Id
	  join usersettings.RetClientsSet rcs on c.Id = rcs.ClientCode
	  join Customers.UserAddresses ua on ua.UserId = u.Id
	  join Customers.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
	  {1}
	WHERE 
		u.Id = ?UserId
	and a.Enabled = 1",
				networkSelfAddressIdColumn,
				networkSelfAddressIdJoin);
		}

		public string TechOperatingDateSubstring(string dateColumn, string biasColumn, string separator)
		{
			if (String.IsNullOrEmpty(dateColumn)
				|| String.IsNullOrEmpty(biasColumn)
				|| String.IsNullOrEmpty(separator))
				return "";
			var hours = String.Format(@"substring({0}, 1, instr({0}, '{2}')-1)+{1}", dateColumn, biasColumn, separator);
			var resultHours = String.Format("if({0}>24, {0}-24, if({0}<0, {0}+24, {0}))", hours);
			var time = String.Format(@"concat({2},
substring({0}, instr({0}, '{1}'), length({0})))", dateColumn, separator, resultHours);
			return time;
		}

		public string GetClientCommand()
		{
			var techInfo = String.Empty;
			if (_updateData.AllowMatchWaybillsToOrders())
				techInfo =
					_updateData.AllowCorrectTechContact() ?
						String.Format(@", c.RegionCode as HomeRegion, regions.TechContact,
(select {0} FROM usersettings.defaults a, farm.regions r where r.RegionCode = regions.RegionCode) as TechOperatingMode ",
							String.Format("replace(replace(a.TechOperatingModeTemplate, '{{0}}', {0}), '{{1}}', {1})",
								TechOperatingDateSubstring("a.TechOperatingModeBegin", "r.MoscowBias", "."),
								TechOperatingDateSubstring("a.TechOperatingModeEnd", "r.MoscowBias", ".")))
						: String.Format(", c.RegionCode as HomeRegion, concat('<tr> <td class=\"contactText\">', regions.TechContact, '</td> </tr>') as TechContact," +
							"concat('<tr> <td class=\"contactText\">'," +
							"(select {0} FROM usersettings.defaults a, farm.regions r where r.RegionCode = regions.RegionCode), '</td> </tr>') as TechOperatingMode ",
							String.Format("replace(replace(a.TechOperatingModeTemplate, '{{0}}', {0}), '{{1}}', {1})",
								TechOperatingDateSubstring("a.TechOperatingModeBegin", "r.MoscowBias", "."),
								TechOperatingDateSubstring("a.TechOperatingModeEnd", "r.MoscowBias", ".")));

			return String.Format(@"
SELECT 
	c.Id as ClientId,
	left(c.Name, 50) as Name,
	regions.CalculateOnProducerCost,
	1,
	rcs.SendRetailMarkup,
	rcs.ShowAdvertising,
	rcs.SendWaybillsFromClient,
	rcs.EnableSmartOrder,
	rcs.EnableImpersonalPrice
{0}
{1}
{2}
FROM Customers.Users u
join Customers.Clients c on u.ClientId = c.Id
join farm.regions on regions.RegionCode = c.RegionCode
join usersettings.RetClientsSet rcs on rcs.ClientCode = c.Id
WHERE u.Id = ?UserId
",
				_updateData.AllowShowSupplierCost ? ", rcs.AllowDelayOfPayment " : String.Empty,
				_updateData.AllowCertificates ? ", rcs.ShowCertificatesWithoutRefSupplier " : String.Empty,
				techInfo);
		}

		public string GetDelayOfPaymentsCommand()
		{
			if (_updateData.EnableImpersonalPrice)
				return "select null from Customers.Clients limit 0";
			else if (_updateData.AllowDelayByPrice()) {
				return @"
select
	pi.PriceId,
	d.DayOfWeek,
	d.VitallyImportantDelay,
	d.OtherDelay
from
	Customers.Users u
	join Customers.Clients c on u.ClientId = c.Id
	join UserSettings.SupplierIntersection si on si.ClientId = c.Id
	join UserSettings.PriceIntersections pi on pi.SupplierIntersectionId = si.Id
	join Usersettings.DelayOfPayments d on d.PriceIntersectionId = pi.Id
where
	   u.Id = ?UserId";
			}
			else if (_updateData.AllowDelayWithVitallyImportant()) {
				return @"
select
	si.SupplierId,
	d.DayOfWeek,
	min(d.VitallyImportantDelay) as VitallyImportantDelay,
	min(d.OtherDelay) as OtherDelay
from
	Customers.Users u
	join Customers.Clients c on u.ClientId = c.Id
	join UserSettings.SupplierIntersection si on si.ClientId = c.Id
	join UserSettings.PriceIntersections pi on pi.SupplierIntersectionId = si.Id
	join Usersettings.DelayOfPayments d on d.PriceIntersectionId = pi.Id
where
	   u.Id = ?UserId
group by si.SupplierId, d.DayOfWeek";
			}
			else {
				return @"
select
	   si.SupplierId   ,
	   si.DelayOfPayment
from
	   Customers.Users u
	   join Customers.Clients c on u.ClientId = c.Id
	   join Usersettings.SupplierIntersection si on si.ClientId = c.Id
where
	   u.Id = ?UserId";
			}
		}

		public string GetMNNCommand(bool before1150, bool after1263)
		{
			if (before1150) {
				return @"
select
  Mnn.Id,
  Mnn.Mnn,
  Mnn.RussianMnn
from
  catalogs.Mnn
where
  if(not ?Cumulative, Mnn.UpdateTime > ?CatalogUpdateTime, 1)";
			}
			else if (after1263) {
				if (_updateData.Cumulative)
					return @"
select
  Mnn.Id,
  Mnn.Mnn,
  0 as Hidden
from
  catalogs.Mnn";
				else
					return @"
select
  Mnn.Id,
  Mnn.Mnn,
  0 as Hidden
from
  catalogs.Mnn
where
  Mnn.UpdateTime > ?CatalogUpdateTime
union
select
  MnnLogs.MnnId,
  MnnLogs.Mnn,
  1 as Hidden
from
  logs.MnnLogs
where
	(MnnLogs.LogTime >= ?CatalogUpdateTime)
and (MnnLogs.Operation = 2)
";
			}
			else if (_updateData.Cumulative)
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
  Mnn.UpdateTime > ?CatalogUpdateTime
union
select
  MnnLogs.MnnId,
  MnnLogs.Mnn,
  MnnLogs.RussianMnn,
  1 as Hidden
from
  logs.MnnLogs
where
	(MnnLogs.LogTime >= ?CatalogUpdateTime)
and (MnnLogs.Operation = 2)
";
		}

		public string GetDescriptionCommand(bool before1150)
		{
			if (before1150) {
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
  if(not ?Cumulative, Descriptions.UpdateTime > ?CatalogUpdateTime, 1)
and Descriptions.NeedCorrect = 0";
			}
			else if (_updateData.Cumulative)
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
 Descriptions.NeedCorrect = 0";
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
  Descriptions.UpdateTime > ?CatalogUpdateTime
and Descriptions.NeedCorrect = 0
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
	(DescriptionLogs.LogTime >= ?CatalogUpdateTime)
and (DescriptionLogs.Operation = 2)
";
		}

		public void ArchiveCertificates(MySqlConnection connection,
			string archiveFileName,
			ref string addition,
			ref string updateLog,
			uint updateId)
		{
			try {
				log.Debug("Будем выгружать сертификаты");

				ExportCertificates(archiveFileName, connection, _updateData.FilesForArchive);
				updateLog = ArchiveCertificatesFiles(archiveFileName, connection);
			}
			catch (Exception exception) {
				log.Error("Ошибка при архивировании сертификатов", exception);
				addition += "Архивирование сертификатов: " + exception.Message + "; ";

				ShareFileHelper.MySQLFileDelete(archiveFileName);
			}
		}

		private void ExportCertificates(string archiveFileName, MySqlConnection connection, ConcurrentQueue<string> filesForArchive)
		{
			var certificateRequestsFile = DeleteFileByPrefix("CertificateRequests");
			var certificatesFile = DeleteFileByPrefix("Certificates");
			var certificateSourcesFile = DeleteFileByPrefix("CertificateSources");
			var sourceSuppliersFile = DeleteFileByPrefix("SourceSuppliers");
			var certificateFilesFile = DeleteFileByPrefix("CertificateFiles");
			var fileCertificatesFile = DeleteFileByPrefix("FileCertificates");

			ProcessCertificates(connection);

			File.WriteAllText(ServiceContext.GetFileByLocal(certificateRequestsFile), _updateData.GetCertificatesResult());

			ProcessArchiveFile(certificateRequestsFile, archiveFileName);


			GetMySQLFileWithDefaultEx("Certificates", connection, GetCertificatesCommand(), false, false);

			ProcessArchiveFile(certificatesFile, archiveFileName);


			GetMySQLFileWithDefaultEx("CertificateSources", connection, GetCertificateSourcesCommand(), false, false);

			ProcessArchiveFile(certificateSourcesFile, archiveFileName);


			GetMySQLFileWithDefaultEx("SourceSuppliers", connection, GetSourceSuppliersCommand(), false, false);

			ProcessArchiveFile(sourceSuppliersFile, archiveFileName);


			GetMySQLFileWithDefaultEx("CertificateFiles", connection, GetCertificateFilesCommand(), false, false);

			ProcessArchiveFile(certificateFilesFile, archiveFileName);


			GetMySQLFileWithDefaultEx("FileCertificates", connection, GetFileCertificatesCommand(), false, false);

			ProcessArchiveFile(fileCertificatesFile, archiveFileName);
		}

		private string GetFileCertificatesCommand()
		{
			var ids =
				_updateData.CertificateRequests.Where(c => c.CertificateId.HasValue && c.CertificateFiles.Count > 0).SelectMany(
					c => c.CertificateFiles).Implode();

			if (String.IsNullOrEmpty(ids))
				ids = "-1";

			return @"
	select
		fc.CertificateId,
		fc.CertificateFileId
	from
		documents.certificatefiles cf
		inner join documents.FileCertificates fc on fc.CertificateFileId = cf.Id
	where
		cf.Id in ("
				+ ids + ")";
		}

		private string GetSourceSuppliersCommand()
		{
			var ids =
				_updateData.CertificateRequests.Where(c => c.CertificateId.HasValue && c.CertificateFiles.Count > 0).SelectMany(
					c => c.CertificateFiles).Implode();

			if (String.IsNullOrEmpty(ids))
				ids = "-1";

			return @"
	select
		ss.CertificateSourceId, ss.SupplierId
	from
		documents.certificatefiles cf
		left join documents.SourceSuppliers ss on ss.CertificateSourceId = cf.CertificateSourceId
	where
		cf.Id in (" + ids + @")
	group by ss.CertificateSourceId, ss.SupplierId
	order by ss.CertificateSourceId, ss.SupplierId	
";
		}

		private string GetCertificateSourcesCommand()
		{
			var ids =
				_updateData.CertificateRequests.Where(c => c.CertificateId.HasValue && c.CertificateFiles.Count > 0).SelectMany(
					c => c.CertificateFiles).Implode();

			if (String.IsNullOrEmpty(ids))
				ids = "-1";

			return @"
	select
		distinct
		cf.CertificateSourceId
	from
		documents.certificatefiles cf
	where
		cf.Id in ("
				+ ids + ")";
		}

		private string GetCertificateFilesCommand()
		{
			var ids =
				_updateData.CertificateRequests.Where(c => c.CertificateId.HasValue && c.CertificateFiles.Count > 0).SelectMany(
					c => c.CertificateFiles).Implode();

			if (String.IsNullOrEmpty(ids))
				ids = "-1";

			return @"
	select
		distinct
		cf.Id,
		cf.OriginFilename,
		cf.ExternalFileId,
		cf.CertificateSourceId,
		cf.Extension
	from
		documents.certificatefiles cf
	where
		cf.Id in ("
				+ ids + ")";
		}

		private string GetCertificatesCommand()
		{
			var ids =
				_updateData.CertificateRequests.Where(c => c.CertificateId.HasValue && c.CertificateFiles.Count > 0).Select(
					c => c.CertificateId.Value).Implode();

			if (String.IsNullOrEmpty(ids))
				ids = "-1";

			return @"
	select
		c.Id,
		c.CatalogId,
		c.SerialNumber
	from
		documents.certificates c
	where
		c.Id in ("
				+ ids + ")";
		}

		private void ProcessCertificates(MySqlConnection connection)
		{
			var showWithoutSuppliers = Convert.ToBoolean(
				MySqlHelper.ExecuteScalar(
					connection,
					"select ShowCertificatesWithoutRefSupplier from UserSettings.RetClientsSet where ClientCode = ?clientId",
					new MySqlParameter("?clientId", _updateData.ClientId)));

			string sql;
			if (showWithoutSuppliers)
				sql = @"
	select
		c.Id as CertificateId,
		cf.Id as CertificateFileId
	from
		documents.DocumentBodies db
		inner join documents.DocumentHeaders dh on dh.Id = db.DocumentId
		inner join documents.Certificates c on c.Id = db.CertificateId
		inner join documents.FileCertificates fs on fs.CertificateId = c.Id
		inner join documents.CertificateFiles cf on cf.Id = fs.CertificateFileId
	where
		db.Id = ?bodyId
";
			else
				sql = @"
	select
		c.Id as CertificateId,
		cf.Id as CertificateFileId
	from
		documents.DocumentBodies db
		inner join documents.DocumentHeaders dh on dh.Id = db.DocumentId
		inner join documents.SourceSuppliers ss on ss.SupplierId = dh.FirmCode
		inner join documents.Certificates c on c.Id = db.CertificateId
		inner join documents.FileCertificates fs on fs.CertificateId = c.Id
		inner join documents.CertificateFiles cf on cf.Id = fs.CertificateFileId and cf.CertificateSourceId = ss.CertificateSourceId
	where
		db.Id = ?bodyId
";

			var dataAdapter = new MySqlDataAdapter(sql, connection);
			dataAdapter.SelectCommand.Parameters.Add("?bodyId", MySqlDbType.UInt32);

			foreach (var certificateRequest in _updateData.CertificateRequests) {
				dataAdapter.SelectCommand.Parameters["?bodyId"].Value = certificateRequest.DocumentBodyId;

				var table = new DataTable();
				dataAdapter.Fill(table);

				if (table.Rows.Count > 0) {
					certificateRequest.CertificateId = Convert.ToUInt32(table.Rows[0]["CertificateId"]);
					certificateRequest.CertificateFiles.Clear();
					foreach (DataRow row in table.Rows) {
						certificateRequest.CertificateFiles.Add(Convert.ToUInt32(row["CertificateFileId"]));
					}
				}
			}
		}

		private void ProcessArchiveFile(string processedFile, string archiveFileName)
		{
			var fullPathFile = ServiceContext.GetFileByLocal(processedFile);
#if DEBUG
			ShareFileHelper.WaitFile(fullPathFile);
#endif

			try {
				SevenZipHelper.ArchiveFiles(archiveFileName, fullPathFile);
				log.DebugFormat("файл для архивации: {0}", fullPathFile);
			}
			catch {
				ShareFileHelper.MySQLFileDelete(archiveFileName);
				throw;
			}

			ShareFileHelper.MySQLFileDelete(fullPathFile);

			ShareFileHelper.WaitDeleteFile(fullPathFile);
		}

		private string ArchiveCertificatesFiles(string archiveFileName, MySqlConnection connection)
		{
			var certificatesFolder = "Certificates";
			var certificatesPath = Path.Combine(_updateData.ResultPath, certificatesFolder);

			foreach (var request in _updateData.CertificateRequests) {
				foreach (var fileId in request.CertificateFiles) {
					var files = Directory.GetFiles(certificatesPath, fileId + ".*");
					if (files.Length > 0) {
						SevenZipHelper.ArchiveFilesWithNames(
							archiveFileName,
							Path.Combine(certificatesFolder, fileId + ".*"),
							_updateData.ResultPath);

						request.SendedFiles.AddRange(files);
					}
				}
			}

			return BuildLog(connection);
		}

		private string BuildLog(MySqlConnection connection)
		{
			var sended = _updateData.CertificateRequests.Where(r => r.SendedFiles.Count > 0).ToArray();
			if (sended.Length == 0)
				return "";

			var sql = String.Format(@"select db.Id, dh.DownloadId, c.Name
from Documents.DocumentBodies db
join Documents.DocumentHeaders dh on dh.Id = db.DocumentId
join Catalogs.Products p on p.Id = db.ProductId
join Catalogs.Catalog c on c.Id = p.CatalogId
where db.Id in ({0})
", sended.Implode(r => r.DocumentBodyId));
			var adapter = new MySqlDataAdapter(sql, connection);
			var table = new DataTable();
			adapter.Fill(table);

			if (table.Rows.Count == 0)
				return "";

			var writer = new StringWriter();
			writer.WriteLine("Отправлены сертификаты:");

			foreach (var row in table.AsEnumerable()) {
				var id = Convert.ToUInt32(row["id"]);
				var files = sended.Where(s => s.DocumentBodyId == id).SelectMany(s => s.SendedFiles);
				foreach (var file in files) {
					writer.WriteLine("Номер документа = {0}, Сопоставленный продукт = {1}, Файл = {2}",
						row["DownloadId"],
						row["Name"],
						Path.GetFileName(file));
				}
			}

			return writer.ToString();
		}

		public int GetMySQLFileWithDefault(string fileName, MySqlConnection connection, string sql)
		{
			return GetMySQLFileWithDefaultEx(fileName, connection, sql, false, true);
		}

		public int GetMySQLFileWithDefaultEx(string fileName, MySqlConnection connection, string SQLText, bool SetCumulative, bool AddToQueue)
		{
			var cmd = new MySqlCommand(SQLText, connection);
			fileName = ServiceContext.GetFileByShared(fileName + _updateData.UserId + ".txt");

			SetUpdateParameters(cmd);
			if (SetCumulative)
				cmd.Parameters["?Cumulative"].Value = true;

			cmd.CommandText += " INTO OUTFILE '" + MySqlHelper.EscapeString(fileName) + "' ";
			log.DebugFormat("SQL команда: {0}", cmd.CommandText);
			var result = cmd.ExecuteNonQuery();

			if (AddToQueue) {
				_updateData.FilesForArchive.Enqueue(fileName);
			}
			return result;
		}

		private string DeleteFileByPrefix(string prefix)
		{
			var deletedFile = prefix + _updateData.UserId + ".txt";
			ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal(deletedFile));

			ShareFileHelper.WaitDeleteFile(ServiceContext.GetFileByLocal(deletedFile));

			return deletedFile;
		}

		public string GetCatalogCommand(bool before1150)
		{
			if (before1150) {
				return @"
SELECT 
	C.Id               ,
	CN.Id              ,
	LEFT(CN.name, 250) ,
	LEFT(CF.form, 250) ,
	C.vitallyimportant ,
	C.needcold         ,
	C.fragile          ,
	C.MandatoryList    ,
	CN.MnnId           ,
	if(d.Id is not null and d.NeedCorrect = 0, CN.DescriptionId, null) DescriptionId
FROM
	(   
	Catalogs.Catalog C       ,
	Catalogs.CatalogForms CF ,
	Catalogs.CatalogNames CN
	)
	left join catalogs.Descriptions d on d.Id = CN.DescriptionId
WHERE  
	C.NameId =CN.Id
AND C.FormId =CF.Id
AND
	(
			IF(NOT ?Cumulative, C.UpdateTime  > ?CatalogUpdateTime, 1)
		OR	IF(NOT ?Cumulative, CN.UpdateTime > ?CatalogUpdateTime, 1)
		OR	IF(NOT ?Cumulative and d.Id is not null, d.UpdateTime > ?CatalogUpdateTime, ?Cumulative)
	)
AND C.hidden = 0";
			}
			else if (_updateData.AllowRetailMargins) {
				if (_updateData.Cumulative) {
					return @"
SELECT 
	C.Id               ,
	CN.Id              ,
	LEFT(CN.name, 250) ,
	LEFT(CF.form, 250) ,
	C.vitallyimportant ,
	C.needcold         ,
	C.fragile          ,
	C.MandatoryList    ,
	CN.MnnId           ,
	if(d.Id is not null and d.NeedCorrect = 0, CN.DescriptionId, null) DescriptionId,
	C.Hidden,
	rm.Markup,
	rm.MaxMarkup,
	rm.MaxSupplierMarkup
FROM   
	(
	Catalogs.Catalog C       ,
	Catalogs.CatalogForms CF ,
	Catalogs.CatalogNames CN
	)
	left join usersettings.RetailMargins rm on rm.CatalogId = c.Id and rm.ClientId = ?ClientCode
	left join catalogs.Descriptions d on d.Id = CN.DescriptionId
WHERE  
	C.NameId = CN.Id
AND C.FormId = CF.Id
AND C.hidden = 0";
				}
				else {
					return String.Format(@"
SELECT 
	C.Id               ,
	CN.Id              ,
	LEFT(CN.name, 250) ,
	LEFT(CF.form, 250) ,
	C.vitallyimportant ,
	C.needcold         ,
	C.fragile          ,
	C.MandatoryList    ,
	CN.MnnId           ,
	if(d.Id is not null and d.NeedCorrect = 0, CN.DescriptionId, null) DescriptionId,
	C.Hidden,
	rm.Markup,
	rm.MaxMarkup,
	rm.MaxSupplierMarkup
FROM   
	(
	Catalogs.Catalog C       ,
	Catalogs.CatalogForms CF ,
	Catalogs.CatalogNames CN
	)
	left join usersettings.RetailMargins rm on rm.CatalogId = c.Id and rm.ClientId = ?ClientCode
	left join catalogs.Descriptions d on d.Id = CN.DescriptionId
WHERE  
	C.NameId = CN.Id
AND C.FormId = CF.Id
AND
	(
			IF(NOT ?Cumulative, C.UpdateTime  > ?CatalogUpdateTime, 1)
		OR	IF(NOT ?Cumulative, CN.UpdateTime > ?CatalogUpdateTime, 1)
		OR	IF(NOT ?Cumulative and d.Id is not null, d.UpdateTime > ?CatalogUpdateTime, ?Cumulative)
		OR	IF(NOT ?Cumulative and rm.Id is not null, {0}, ?Cumulative)
	)",
						_updateData.NeedUpdateForRetailMargins() ? "1" : "rm.UpdateTime > ?UpdateTime");
				}
			}
			else if (_updateData.Cumulative)
				return @"
SELECT 
	C.Id               ,
	CN.Id              ,
	LEFT(CN.name, 250) ,
	LEFT(CF.form, 250) ,
	C.vitallyimportant ,
	C.needcold         ,
	C.fragile          ,
	C.MandatoryList    ,
	CN.MnnId           ,
	if(d.Id is not null and d.NeedCorrect = 0, CN.DescriptionId, null) DescriptionId,
	C.Hidden
FROM   
	(
	Catalogs.Catalog C       ,
	Catalogs.CatalogForms CF ,
	Catalogs.CatalogNames CN
	)
	left join catalogs.Descriptions d on d.Id = CN.DescriptionId
WHERE
	C.NameId =CN.Id
AND C.FormId =CF.Id
AND C.hidden =0
";
			else
				return @"
SELECT 
	C.Id               ,
	CN.Id              ,
	LEFT(CN.name, 250) ,
	LEFT(CF.form, 250) ,
	C.vitallyimportant ,
	C.needcold         ,
	C.fragile          ,
	C.MandatoryList    ,
	CN.MnnId           ,
	if(d.Id is not null and d.NeedCorrect = 0, CN.DescriptionId, null) DescriptionId,
	C.Hidden
FROM   
	(
	Catalogs.Catalog C       ,
	Catalogs.CatalogForms CF ,
	Catalogs.CatalogNames CN
	)
	left join catalogs.Descriptions d on d.Id = CN.DescriptionId
WHERE  
	C.NameId =CN.Id
AND C.FormId =CF.Id
AND
	(
			IF(NOT ?Cumulative, C.UpdateTime  > ?CatalogUpdateTime, 1)
		OR	IF(NOT ?Cumulative, CN.UpdateTime > ?CatalogUpdateTime, 1)
		OR	IF(NOT ?Cumulative and d.Id is not null, d.UpdateTime > ?CatalogUpdateTime, ?Cumulative)
	)
";
		}

		public string GetProductsCommand()
		{
			return @"
SELECT
	P.Id,
	P.CatalogId
FROM
	Catalogs.Products P
WHERE
	(If(Not ?Cumulative, (P.UpdateTime > ?CatalogUpdateTime), 1))
AND hidden = 0";
		}

		public string GetCoreCommand(bool exportInforoomPrice, bool exportSupplierPriceMarkup, bool exportBuyingMatrix)
		{
			var matrixParts = new SqlParts();
			if (exportBuyingMatrix && exportSupplierPriceMarkup)
				matrixParts = new MatrixHelper(_updateData.Settings).BuyingMatrixCondition(exportInforoomPrice);

			var sql = "";
			if (exportInforoomPrice)
				if (!exportSupplierPriceMarkup)
					sql = @"
SELECT 
	   ?ImpersonalPriceId               ,
	   ?OffersRegionCode                ,
	   A.ProductId                      ,
	   A.CodeFirmCr as ProducerId       ,
	   A.ProductId as SynonymCode       ,
	   A.CodeFirmCr as SynonymFirmCrCode,
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
	   '' as Cost                       ,
	   @RowId := @RowId + 1             ,
	   ''                               ,
	   ''
FROM   
	   CoreAssortment A
WHERE  
   A.CodeFirmCr IS NOT NULL

UNION

SELECT 
	   ?ImpersonalPriceId                ,
	   ?OffersRegionCode                 ,
	   A.ProductId                       ,
	   1 as ProducerId                   ,
	   A.ProductId as SynonymCode        ,
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
	   '' as Cost                        ,
	   @RowId := @RowId + 1              ,
	   ''                                ,
	   ''
FROM   
	   CoreProducts A
";
				else
					sql = String.Format(
						@"
SELECT 
	   ?ImpersonalPriceId               ,
	   ?OffersRegionCode                ,
	   A.ProductId                      ,
	   A.CodeFirmCr as ProducerId       ,
	   A.ProductId as SynonymCode       ,
	   A.CodeFirmCr as SynonymFirmCrCode,
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
	   '' as Cost                       ,
	   @RowId := @RowId + 1             ,
	   null as OrderCost                 ,
	   null as MinOrderCount             ,
	   null as SupplierPriceMarkup       ,
	   null as ProducerCost              ,
	   null as NDS
	  {0}
FROM   
	   CoreAssortment A
	  {1}
WHERE
	A.CodeFirmCr IS NOT NULL
group by  @RowId
{3}

UNION

SELECT 
	   ?ImpersonalPriceId                ,
	   ?OffersRegionCode                 ,
	   A.ProductId                       ,
	   1 as ProducerId                   ,
	   A.ProductId as SynonymCode        ,
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
	   '' as Cost                        ,
	   @RowId := @RowId + 1              ,
	   null as OrderCost                 ,
	   null as MinOrderCount             ,
	   null as SupplierPriceMarkup       ,
	   null as ProducerCost              ,
	   null as NDS
	   {0}
FROM   
	   CoreProducts A
	   {2}
group by  @RowId
{3}
",
						matrixParts.Select,
						matrixParts.Join,
						matrixParts.JoinWithoutProducer,
						matrixParts.Having);
			else {
				return GetOfferQuery(exportSupplierPriceMarkup, exportBuyingMatrix).ToSql();
			}

			return sql;
		}

		private Query GetOfferQuery(bool exportSupplierPriceMarkup, bool exportBuyingMatrix,
			bool ignoreVersionRules = false,
			bool costOptimization = false)
		{
			var matrixParts = new SqlParts();
			if (ignoreVersionRules || (exportBuyingMatrix && exportSupplierPriceMarkup))
				matrixParts = new MatrixHelper(_updateData.Settings).BuyingMatrixCondition(false);

			var costPart = "CT.Cost as Cost";
			if (costOptimization)
				costPart = "if(k.Id is null or k.Date < at.PriceDate, ct.Cost, ifnull(ca.Cost, ct.Cost)) as Cost";
			var query = SqlQueryBuilderHelper.GetFromPartForCoreTable(matrixParts, true);
			var select = String.Format(@"
	CT.PriceCode               ,
	CT.regioncode              ,
	CT.ProductId               ,
	ifnull(Core.codefirmcr, 0) as ProducerId,
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
	ifnull(cc.RequestRatio, Core.RequestRatio) as RequestRatio,
	{5},
	RIGHT(CT.ID, 9) as CoreID,
	ifnull(cc.MinOrderSum, core.OrderCost) as OrderCost,
	ifnull(cc.MinOrderCount, core.MinOrderCount) as MinOrderCount
	{0}
	{2}
	{1}
	{3}
	{4}
",
					ignoreVersionRules || exportSupplierPriceMarkup ? @"
, 
if((Core.ProducerCost is null) or (Core.ProducerCost = 0),
   null,
   if((Core.NDS is null) or (Core.NDS < 0),
	 (CT.Cost/(Core.ProducerCost*1.1)-1)*100,
	 if(Core.NDS = 0,
	   (CT.Cost/Core.ProducerCost-1)*100,
	   (CT.Cost/(Core.ProducerCost*(1 + Core.NDS/100))-1)*100
	 )
   )
) as SupplierPriceMarkup,
Core.ProducerCost,
Core.NDS " : "",
					matrixParts.Select,
					ignoreVersionRules || (exportSupplierPriceMarkup && _updateData.AllowDelayByPrice()) ? ", (Core.VitallyImportant or ifnull(catalog.VitallyImportant,0)) as RetailVitallyImportant " : "",
					ignoreVersionRules || _updateData.AllowEAN13() ? ", Core.EAN13, Core.CodeOKP, Core.Series " : "",
					ignoreVersionRules || _updateData.SupportExportExp() ? ", Core.Exp " : "",
					costPart);
			query.Select(select);
			return query;
		}

		public string GetSynonymFirmCrCommand(bool cumulative)
		{
			var sql = String.Empty;

			if (_updateData.EnableImpersonalPrice) {
				sql = @"
select
	Producers.Id as synonymfirmcrcode,
	LEFT(Producers.Name, 250) as Synonym
from
	catalogs.Producers
where
	(Producers.Id > 1)";
				if (!cumulative)
					sql += " and Producers.UpdateTime > ?UpdateTime ";
			}
			else {
				sql = @"
SELECT synonymfirmcr.synonymfirmcrcode,
	   LEFT(SYNONYM, 250)
FROM   farm.synonymfirmcr,
	   ParentCodes
WHERE  synonymfirmcr.pricecode = ParentCodes.PriceCode";
				if (!cumulative)
					sql += " AND synonymfirmcr.synonymfirmcrcode > MaxSynonymFirmCrCode ";
			}

			sql += @"
UNION

SELECT 1,
	   '-'
";

			return sql;
		}

		public string GetSynonymCommand(bool Cumulative)
		{
			var sql = String.Empty;

			if (_updateData.EnableImpersonalPrice) {
				sql = @"

select 
  p.Id As SynonymCode,
  LEFT( concat(c.Name, ' ', cast(GROUP_CONCAT(ifnull(PropertyValues.Value, '') order by Properties.PropertyName, PropertyValues.Value SEPARATOR ', ') as char)) , 250) as Synonym
from 
  catalogs.products p
  join catalogs.catalog c on c.Id = p.CatalogId
  left join catalogs.ProductProperties on ProductProperties.ProductId = p.Id
  left join catalogs.PropertyValues on PropertyValues.Id = ProductProperties.PropertyValueId
  left join catalogs.Properties on Properties.Id = PropertyValues.PropertyId
where 
	   C.hidden = 0
   and p.hidden = 0
";

				if (!Cumulative)
					sql += @" and ( IF(NOT ?Cumulative, C.UpdateTime  > ?UpdateTime, 1)  OR  IF(NOT ?Cumulative, p.UpdateTime > ?UpdateTime, 1) ) ";

				sql += "group by p.id";
			}
			else {
				sql = @"
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
			}

			return sql;
		}

		private void InternalUpdatePriceSettings(int[] priceIds, long[] regionIds, bool[] injobs)
		{
			With.DeadlockWraper(() => {
				var transaction = _readWriteConnection.BeginTransaction(IsolationLevel.RepeatableRead);
				try {
					MySqlHelper.ExecuteNonQuery(
						_readWriteConnection,
						"set @INHost = ?host;set @INUser = ?username;",
						new MySqlParameter("?host", ServiceContext.GetUserHost()),
						new MySqlParameter("?username", _updateData.UserName));
					MySqlHelper.ExecuteNonQuery(_readWriteConnection, "DROP TEMPORARY TABLE IF EXISTS Prices, ActivePrices;");
					SelectPrices();

					var pricesSet = MySqlHelper.ExecuteDataset(_readWriteConnection, @"
select 
  Prices.*,
  concat(cd.Name, ' (', Prices.PriceName, ') ', r.Region) as FirmName 
from 
  Prices
  inner join Customers.Suppliers cd on cd.Id = Prices.FirmCode
  inner join farm.regions r on r.RegionCode = Prices.RegionCode
  ");
					var prices = pricesSet.Tables[0];

					var addition = new List<string>();

					var deleteCommand = new MySqlCommand("delete from Customers.UserPrices where PriceId = ?PriceId and UserId = ?UserId and RegionId = ?RegionId", _readWriteConnection);
					deleteCommand.Parameters.AddWithValue("?UserId", _updateData.UserId);
					deleteCommand.Parameters.Add("?PriceId", MySqlDbType.UInt32);
					deleteCommand.Parameters.Add("?RegionId", MySqlDbType.UInt64);
					var insertCommand = new MySqlCommand(@"
insert into Customers.UserPrices(UserId, PriceId, RegionId)
select ?UserId, ?PriceId, ?RegionId
from (select 1) as c
where not exists (
	select *
	from Customers.UserPrices up
	where up.UserId = ?UserId and up.PriceId = ?PriceId and up.RegionId = ?RegionId
);", _readWriteConnection);
					insertCommand.Parameters.AddWithValue("?UserId", _updateData.UserId);
					insertCommand.Parameters.Add("?PriceId", MySqlDbType.UInt32);
					insertCommand.Parameters.Add("?RegionId", MySqlDbType.UInt64);
					for (var i = 0; i < injobs.Length; i++) {
						var row = prices.Select("PriceCode = " + priceIds[i] + " and RegionCode = " + regionIds[i]);
						if (row.Length > 0)
							addition.Add(String.Format("{0} - {1}", row[0]["FirmName"], injobs[i] ? "вкл" : "выкл"));

						MySqlCommand command;
						if (injobs[i])
							command = insertCommand;
						else
							command = deleteCommand;

						command.Parameters["?PriceId"].Value = priceIds[i];
						command.Parameters["?RegionId"].Value = regionIds[i];
						command.ExecuteNonQuery();
					}

					AnalitFUpdate.InsertAnalitFUpdatesLog(transaction.Connection, _updateData, RequestType.PostPriceDataSettings, String.Join("; ", addition.ToArray()));

					transaction.Commit();
				}
				catch {
					With.SafeRollback(transaction);
					throw;
				}
			});
		}

		public void UpdatePriceSettings(int[] priceIds, long[] regionIds, bool[] injobs)
		{
			if (priceIds.Length > 0 && priceIds.Length == regionIds.Length && regionIds.Length == injobs.Length) {
				InternalUpdatePriceSettings(priceIds, regionIds, injobs);
			}
			else
				throw new Exception("Не совпадают длины полученных массивов");
		}

		public bool NeedClientToAddressMigration()
		{
			var userId = MySql.Data.MySqlClient.MySqlHelper.ExecuteScalar(_readWriteConnection, @"
select UserId 
from 
  Customers.ClientToAddressMigrations
where
	(UserId = ?UserId)
limit 1
",
				new MySqlParameter("?UserId", _updateData.UserId));
			return (userId != null);
		}

		public string GetClientToAddressMigrationCommand()
		{
			return @"
select 
  ClientCode, AddressId
from 
  Customers.ClientToAddressMigrations
where
	UserId = "
				+ _updateData.UserId;
		}

		public string GetUpdateValuesCommand()
		{
			return @"
select
	BaseFirmCategory,
	OverCostPercent,
	ExcessAvgOrderTimes,
	DifferenceCalculation,
	ShowPriceName	
from
	UserSettings.RetClientsSet
where
	ClientCode = "
				+ _updateData.ClientId;
		}

		public string GetMinReqRuleCommand()
		{
			if (_updateData.EnableImpersonalPrice)
				return @"
select
  a.Id as ClientId,
  ?ImpersonalPriceId as PriceCode,
  ?OffersRegionCode as RegionCode,
  0 as ControlMinReq,
  null as MinReq 
from
  Customers.Users u
  join Customers.Clients c on u.ClientId = c.Id
  join Customers.UserAddresses ua on ua.UserId = u.Id
  join Customers.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
where
  (u.Id = ?UserId)";

			return @"
select
  a.Id as ClientId,
  i.PriceId as PriceCode,
  i.RegionId as RegionCode,
  ai.ControlMinReq,
  if(ai.MinReq > 0, ai.MinReq, Prices.MinReq) as MinReq 
from
  Customers.Users u
  join Customers.Clients c on u.ClientId = c.Id
  join Customers.UserAddresses ua on ua.UserId = u.Id
  join Customers.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
  join Customers.Intersection i on i.ClientId = c.Id
  join Customers.AddressIntersection ai on (ai.IntersectionId = i.Id) and (ai.AddressId = a.Id)
  join Prices on (Prices.PriceCode = i.PriceId) and (Prices.RegionCode = i.RegionId)
where
  (u.Id = ?UserId)";
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
#CostSessionKey = null,
	   MessageShowCount = if(MessageShowCount > 0, MessageShowCount - 1, 0)
WHERE  UserId          = {0};
",
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
",
						_updateData.UserId);
			else {
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
",
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
AND    Pd.PriceCode IN ( {1} );",
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
SET    UpdateDate      =UncommitedUpdateDate
#CostSessionKey = null,
	   {1}
WHERE  UserId          = {0};

UPDATE AnalitFReplicationInfo
SET    MaxSynonymFirmCrCode    =UncMaxSynonymFirmCrCode
WHERE  UncMaxSynonymFirmCrCode!=0
AND    UserId                  = {0};

UPDATE AnalitFReplicationInfo
SET    MaxSynonymCode    =UncMaxSynonymCode
WHERE  UncMaxSynonymCode!=0
AND    UserId            = {0};
",
					_updateData.UserId,
					_updateData.IsConfirmUserMessage() ? "" : ", MessageShowCount = if(MessageShowCount > 0, MessageShowCount - 1, 0) ");

			ProcessCommitCommand(commitCommand);
		}

		public DateTime GetCurrentUpdateDate(RequestType updateType)
		{
			return With.DeadlockWraper(() => {
				var transaction = _readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
				try {
					var command = new MySqlCommand("", _readWriteConnection);

					if (updateType != RequestType.ResumeData)
						command.CommandText += @"update UserUpdateInfo set UncommitedUpdateDate=now() where UserId = ?userId; ";
					command.CommandText += "select UncommitedUpdateDate from UserUpdateInfo where UserId = ?userId;";
					command.Parameters.AddWithValue("?UserId", _updateData.UserId);
					var updateTime = Convert.ToDateTime(command.ExecuteScalar());

					transaction.Commit();
					return updateTime;
				}
				catch {
					With.SafeRollback(transaction);
					throw;
				}
			});
		}

		public static void GenerateSessionKey(MySqlConnection readWriteConnection, UpdateData updateData)
		{
			With.DeadlockWraper(() => {
				var transaction = readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
				try {
					updateData.CostSessionKey = Convert.ToString(MySqlHelper.ExecuteScalar(
						readWriteConnection,
						@"
update UserUpdateInfo set CostSessionKey = usersettings.GeneratePassword() where UserId = ?userId;
select CostSessionKey from UserUpdateInfo where UserId = ?userId;
",
						new MySqlParameter("?UserId", updateData.UserId)));

					transaction.Commit();
				}
				catch {
					With.SafeRollback(transaction);
					throw;
				}
			});
		}


		private void ProcessCommitCommand(string commitCommand)
		{
			With.DeadlockWraper(() => {
				var transaction = _readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
				try {
					MySqlHelper.ExecuteNonQuery(_readWriteConnection, commitCommand);
					transaction.Commit();
				}
				catch {
					With.SafeRollback(transaction);
					throw;
				}
			});
		}

		public void SetForceReplication()
		{
			With.DeadlockWraper(() => {
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

		public void SetUpdateParameters(MySqlCommand selectComand)
		{
			selectComand.Parameters.AddWithValue("?ClientCode", _updateData.ClientId);
			selectComand.Parameters.AddWithValue("?UserId", _updateData.UserId);
			selectComand.Parameters.AddWithValue("?Cumulative", _updateData.Cumulative);
			selectComand.Parameters.AddWithValue("?UpdateTime", _updateData.OldUpdateTime);
			selectComand.Parameters.AddWithValue("?LastUpdateTime", _updateData.CurrentUpdateTime);
			selectComand.Parameters.AddWithValue("?OffersClientCode", _updateData.OffersClientCode);
			selectComand.Parameters.AddWithValue("?OffersRegionCode", _updateData.OffersRegionCode);
			selectComand.Parameters.AddWithValue("?ImpersonalPriceId", _updateData.ImpersonalPriceId);
			selectComand.Parameters.AddWithValue("?ImpersonalPriceDate", DateTime.Now);
			selectComand.Parameters.AddWithValue("?ImpersonalPriceFresh", _updateData.ImpersonalPriceFresh);
			selectComand.Parameters.AddWithValue("?CatalogUpdateTime", _updateData.CatalogUpdateTime ?? _updateData.OldUpdateTime);
		}

		public void PrepareImpersonalOffres()
		{
			var cmd = new MySqlCommand(@"
DROP TEMPORARY TABLE IF EXISTS Prices, ActivePrices;
CALL Customers.GetActivePrices(?OffersClientCode);
CALL Customers.GetOffers(?OffersClientCode);", _readWriteConnection);
			SetUpdateParameters(cmd);
			cmd.ExecuteNonQuery();

			cmd = new MySqlCommand(@"
DROP TEMPORARY TABLE IF EXISTS CoreAssortment, CoreProducts;
CREATE TEMPORARY TABLE CoreAssortment (ProductId       INT unsigned, CodeFirmCr INT unsigned, UNIQUE MultiK(ProductId, CodeFirmCr)) engine=MEMORY;
CREATE TEMPORARY TABLE CoreProducts (ProductId INT unsigned, UNIQUE MultiK(ProductId)) engine=MEMORY;

INSERT
INTO   CoreAssortment
		(
				ProductId ,
				CodeFirmCr
		)
SELECT   core0.ProductId ,
			core0.codefirmcr
FROM     farm.core0,
			Core
WHERE    core0.id=Core.id
GROUP BY ProductId,
			CodeFirmCr;
						
INSERT
INTO   CoreProducts
		(
				ProductId
		)
SELECT   ProductId
FROM     CoreAssortment
GROUP BY ProductId;
						
SET @RowId :=1;", _readWriteConnection);
			SetUpdateParameters(cmd);
			cmd.ExecuteNonQuery();
		}

		public string GetPricesRegionalDataCommand()
		{
			if (_updateData.EnableImpersonalPrice)
				return @"
SELECT 
	   ?ImpersonalPriceId as PriceCode  ,
	   ?OffersRegionCode as RegionCode,
	   0 as STORAGE                   ,
	   null as MinReq                 ,
	   1 as MainFirm                  ,
	   1 as InJob                     ,
	   0 as ControlMinReq
FROM   
   UserSettings.PricesData
where
  PricesData.PriceCode = ?ImpersonalPriceId
limit 1";
			else
				return @"
SELECT 
	   PriceCode           ,
	   RegionCode          ,
	   STORAGE             ,
	   MinReq              ,
	   MainFirm            ,
	   NOT disabledbyclient,
	   ControlMinReq
FROM   Prices";
		}

		public string GetRegionalDataCommand()
		{
			if (_updateData.EnableImpersonalPrice)
				return String.Format(@"
SELECT 
	PricesData.FirmCode            ,
	?OffersRegionCode as RegionCode,
	'4732-606000' as supportphone  ,
	null as ContactInfo            ,
	null as OperativeInfo
	{0}
FROM   
   UserSettings.PricesData
where
  PricesData.PriceCode = ?ImpersonalPriceId
limit 1",
					_updateData.AllowAfter1883() ? ", null as Address " : string.Empty);
			else
				return String.Format(@"
SELECT DISTINCT 
	regionaldata.FirmCode  ,
	regionaldata.RegionCode,
	supportphone           ,
	{0},
	OperativeInfo
	{1}
FROM            
				regionaldata,
				Prices,
				customers.Suppliers
WHERE           regionaldata.firmcode  = Prices.firmcode
AND             regionaldata.regioncode= Prices.regioncode
and				Suppliers.Id = regionaldata.firmcode",
					_updateData.AllowAfter1883() ? " ContactInfo " : "concat(if(Suppliers.Address is not null and Length(Suppliers.Address) > 0, concat(Suppliers.Address, '\r\n'), ''), ContactInfo) as ContactInfo",
					_updateData.AllowAfter1883() ? ", Suppliers.Address " : string.Empty);
		}

		public void PrepareProviderContacts(MySqlConnection connection)
		{
			if (!_updateData.EnableImpersonalPrice) {
				connection.Execute(@"
DROP TEMPORARY TABLE IF EXISTS ProviderContacts;

CREATE TEMPORARY TABLE ProviderContacts engine=MEMORY
AS
SELECT DISTINCT c.contactText,
				cd.Id as FirmCode
FROM            Customers.Suppliers cd
				JOIN contacts.contact_groups cg
				ON              cd.ContactGroupOwnerId = cg.ContactGroupOwnerId
				JOIN contacts.contacts c
				ON              cg.Id = c.ContactOwnerId
WHERE           cd.Id IN
								(SELECT DISTINCT FirmCode
								FROM             Prices
								)
AND             cg.Type = 1
AND             c.Type  = 0;
				
INSERT
INTO   ProviderContacts
SELECT DISTINCT c.contactText,
				cd.Id as FirmCode
FROM            Customers.Suppliers cd
				JOIN contacts.contact_groups cg
				ON              cd.ContactGroupOwnerId = cg.ContactGroupOwnerId
				JOIN contacts.persons p
				ON              cg.id = p.ContactGroupId
				JOIN contacts.contacts c
				ON              p.Id = c.ContactOwnerId
WHERE           cd.Id IN
								(SELECT DISTINCT FirmCode
								FROM             Prices
								)
AND             cg.Type = 1
AND             c.Type  = 0;");
			}
		}

		public void ClearProviderContacts()
		{
			_readWriteConnection.Execute("drop TEMPORARY TABLE IF EXISTS ProviderContacts");
		}

		public string GetProvidersCommand()
		{
			if (_updateData.EnableImpersonalPrice)
				return @"
SELECT   
		 firm.Id as FirmCode                                                       ,
		 firm.FullName                                                             ,
		 '' as Fax                                                                 ,
		 null as ContactText                                                       ,
		 firm.Name,
		0 as CertificateSourceExists,
		0 as SupplierCategory
FROM     
		 usersettings.PricesData pd
		 inner join Customers.Suppliers AS firm on firm.Id = pd.FirmCode
WHERE    
		 pd.PriceCode = ?ImpersonalPriceId";
			else
				return @"
SELECT   
		 firm.Id as FirmCode                                                       ,
		 firm.FullName                                                             ,
		 '' as Fax                                                                 ,
		 LEFT(ifnull(group_concat(DISTINCT ProviderContacts.ContactText), ''), 255),
		 firm.Name,
		if(ss.CertificateSourceId is not null, 1, 0) as CertificateSourceExists,
		si.SupplierCategory
FROM     Customers.Suppliers AS firm
		inner join usersettings.SupplierIntersection si on si.SupplierId = firm.Id and si.ClientId = ?ClientCode
		 LEFT JOIN ProviderContacts
		 ON       ProviderContacts.FirmCode = firm.Id
		left join Documents.SourceSuppliers ss on ss.SupplierId = firm.Id		
WHERE    firm.Id IN
						   (SELECT DISTINCT FirmCode
						   FROM             Prices
						   )
GROUP BY firm.Id";
		}

		public string GetPricesDataCommand()
		{
			if (_updateData.EnableImpersonalPrice)
				return @"
SELECT   
		 pd.FirmCode ,
		 pd.pricecode,
		 firm.name                                                                                                      as PriceName,
		 ''                                                                                                             as PRICEINFO,
		 date_sub(?ImpersonalPriceDate, interval time_to_sec(date_sub(now(), interval unix_timestamp() second)) second) as DATEPRICE,
		 ?ImpersonalPriceFresh                                                                                          as Fresh
FROM     
		 usersettings.pricesdata pd
		 join Customers.Suppliers AS firm on firm.Id = pd.FirmCode
WHERE    
   pd.PriceCode = ?ImpersonalPriceId
";
			else
				return String.Format(@"
SELECT   
		 Prices.FirmCode ,
		 Prices.pricecode,
		 {0}    as PriceName,
		 ''                                                                                                  as PRICEINFO,
		 date_sub(Prices.PriceDate, interval time_to_sec(date_sub(now(), interval unix_timestamp() second)) second) as DATEPRICE,
		 max(ifnull(ActivePrices.Fresh, ARI.ForceReplication > 0) OR (Prices.actual = 0) OR ?Cumulative)                                            as Fresh
FROM     
		 (
		 Customers.Suppliers AS firm,
		 PriceCounts             ,
		 Prices             ,
		 CurrentReplicationInfo ARI
		 )
		 left join ActivePrices on ActivePrices.PriceCode = Prices.pricecode and ActivePrices.RegionCode = Prices.RegionCode
WHERE    PriceCounts.firmcode = firm.Id
AND      firm.Id   = Prices.FirmCode
AND      ARI.FirmCode    = Prices.FirmCode
AND      ARI.UserId      = ?UserId
GROUP BY Prices.FirmCode,
		 Prices.pricecode",
					_updateData.AllowHistoryDocs ? " Prices.pricename " : " concat(firm.name, IF(PriceCounts.PriceCount> 1 OR Prices.ShowPriceName = 1, concat(' (', Prices.pricename, ')'), '')) ");
		}

		public void PreparePricesData()
		{
			if (_updateData.EnableImpersonalPrice) {
				var cmd = new MySqlCommand("select max(PriceDate) from Prices", _readWriteConnection);
				SetUpdateParameters(cmd);
				var priceDate = Convert.ToDateTime(cmd.ExecuteScalar());
				cmd.Parameters["?ImpersonalPriceDate"].Value = priceDate;

				cmd.CommandText = @"
select 
  ifnull(sum((ARI.ForceReplication != 0) OR ((Prices.actual = 0) and (?UpdateTime < Prices.PriceDate + interval f.maxold day)) OR ?Cumulative), 0) as Fresh
from 
  Prices
  inner join AnalitFReplicationInfo ARI on ARI.FirmCode = Prices.FirmCode
  inner JOIN usersettings.PricesCosts pc on pc.CostCode = Prices.CostCode
  inner JOIN usersettings.PriceItems pi on pi.Id = pc.PriceItemId
  inner JOIN farm.formrules f on f.Id = pi.FormRuleId
where
  ARI.UserId = ?UserId";
				var priceFresh = Convert.ToInt32(cmd.ExecuteScalar());
				_updateData.ImpersonalPriceFresh = priceFresh > 0;
			}
			else {
				_readWriteConnection.Execute(@"
CREATE TEMPORARY TABLE PriceCounts ( FirmCode INT unsigned, PriceCount MediumINT unsigned )engine=MEMORY;
		INSERT
		INTO   PriceCounts
		SELECT   firmcode,
				 COUNT(pricecode)
		FROM     Prices
		GROUP BY FirmCode,
				 RegionCode;");
			}
		}

		public void ResetReclameDate()
		{
			var transaction = _readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
			try {
				var resetCommand = new MySqlCommand("update usersettings.UserUpdateInfo set ReclameDate = NULL where UserId= ?UserId;", _readWriteConnection, transaction);
				resetCommand.Parameters.AddWithValue("?UserId", _updateData.UserId);
				resetCommand.ExecuteNonQuery();

				transaction.Commit();
			}
			catch {
				With.SafeRollback(transaction);
				throw;
			}
		}

		public void ResetDocumentCommited(DateTime oldAccessTime)
		{
			var oneMonthOld = DateTime.Now.AddMonths(-1);
			var resetDate = oldAccessTime;
			if (oldAccessTime.CompareTo(oneMonthOld) < 0)
				resetDate = oneMonthOld;

			var transaction = _readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
			try {
				var resetCommand = new MySqlCommand(@"
update
  logs.AnalitFUpdates afu,
  Logs.DocumentSendLogs ds
set
  ds.Committed = 0,
  ds.FileDelivered = 0, 
  ds.DocumentDelivered = 0
where
	afu.RequestTime > ?resetDate
and afu.UserId = ?UserId
and ds.UpdateId = afu.UpdateId
and ds.UserId = afu.UserId
and ds.Committed = 1;

update
  logs.AnalitFUpdates afu,
  Logs.UnconfirmedOrdersSendLogs sendlogs,
  orders.OrdersHead
set
  sendlogs.Committed = 0,
  sendlogs.UpdateId = null,
  OrdersHead.Deleted = 0
where
	afu.RequestTime > ?resetDate
and afu.UserId = ?UserId
and sendlogs.UpdateId = afu.UpdateId
and sendlogs.UserId = afu.UserId
and sendlogs.Committed = 1
and OrdersHead.RowId = sendlogs.OrderId;

", _readWriteConnection, transaction);
				resetCommand.Parameters.AddWithValue("?UserId", _updateData.UserId);
				resetCommand.Parameters.AddWithValue("?resetDate", resetDate);
				resetCommand.ExecuteNonQuery();

				transaction.Commit();
			}
			catch {
				With.SafeRollback(transaction);
				throw;
			}
		}

		public static void UpdateBuildNumber(MySqlConnection readWriteConnection, UpdateData updateData)
		{
			if (!updateData.KnownBuildNumber.HasValue || updateData.KnownBuildNumber < updateData.BuildNumber)
				With.DeadlockWraper(() => {
					var command = new MySqlCommand("update usersettings.UserUpdateInfo set AFAppVersion = ?BuildNumber where UserId = ?UserId", readWriteConnection);
					command.Parameters.AddWithValue("?BuildNumber", updateData.BuildNumber);
					command.Parameters.AddWithValue("?UserId", updateData.UserId);
					var transaction = readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
					try {
						command.Transaction = transaction;
						command.ExecuteNonQuery();

						transaction.Commit();
					}
					catch {
						With.SafeRollback(transaction);
						throw;
					}
				});
		}

		public static void CheckUniqueId(MySqlConnection readWriteConnection, UpdateData updateData, string uniqueId)
		{
			CheckUniqueId(readWriteConnection, updateData, uniqueId, RequestType.GetData);
		}

		public static void CheckUniqueId(MySqlConnection readWriteConnection, UpdateData updateData, string uniqueId, RequestType request)
		{
			With.DeadlockWraper(() => {
				updateData.UniqueID = uniqueId;

				var command = new MySqlCommand("update UserUpdateInfo set AFCopyId= ?UniqueId where UserId = ?UserId", readWriteConnection);
				command.Parameters.AddWithValue("?UniqueId", updateData.UniqueID);
				command.Parameters.AddWithValue("?UserId", updateData.UserId);

				var transaction = readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
				try {
					command.Transaction = transaction;
					if (String.IsNullOrEmpty(updateData.KnownUniqueID))
						command.ExecuteNonQuery();
					else if (updateData.KnownUniqueID != uniqueId) {
						string description;
						switch (request) {
							case RequestType.SendWaybills:
								description = "Отправка накладных на данном компьютере запрещена.";
								break;
							case RequestType.PostOrderBatch:
								description = "Отправка дефектуры на данном компьютере запрещена.";
								break;
							case RequestType.SendOrder:
							case RequestType.SendOrders:
								description = "Отправка заказов на данном компьютере запрещена.";
								break;
							case RequestType.PostPriceDataSettings:
								description = "Изменение настроек прайс-листов на данном компьютере запрещено.";
								break;
							case RequestType.GetHistoryOrders:
								description = "Запрос истории заказов на данном компьютере запрещен.";
								break;
							default:
								description = "Обновление программы на данном компьютере запрещено.";
								break;
						}

						throw new UpdateException(description,
							"Пожалуйста, обратитесь в АК \"Инфорум\".[2]",
							"Несоответствие UIN.",
							RequestType.Forbidden);
					}

					transaction.Commit();
				}
				catch {
					With.SafeRollback(transaction);
					throw;
				}
			});
		}

		public void ConfirmUserMessage(string confirmedMessage)
		{
			With.DeadlockWraper(() => {
				var transaction = _readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
				try {
					if (_updateData.Message.Slice(255).Equals(confirmedMessage.Trim().Slice(255), StringComparison.OrdinalIgnoreCase))
						MySqlHelper.ExecuteNonQuery(
							_readWriteConnection,
							@"
	update 
	  usersettings.UserUpdateInfo 
	set 
	  MessageShowCount = if(MessageShowCount > 0, MessageShowCount - 1, 0) 
	where
		UserId = ?UserId",
							new MySqlParameter("?UserId", _updateData.UserId));

					AnalitFUpdate.InsertAnalitFUpdatesLog(transaction.Connection, _updateData, RequestType.ConfirmUserMessage, confirmedMessage);

					transaction.Commit();
				}
				catch {
					With.SafeRollback(transaction);
					throw;
				}
			});
		}

		public void UnconfirmedOrdersExport(string exportFolder)
		{
			if (_updateData.NeedDownloadUnconfirmedOrders) {
				var exporter = new UnconfirmedOrdersExporter(_updateData, this, exportFolder, _updateData.FilesForArchive);
				exporter.Export();
			}
		}

		public string GetSchedulesCommand()
		{
			if (_updateData.AllowAnalitFSchedule)
				return @"
SELECT 
	s.Id,
	s.Hour,
	s.Minute
FROM 
	UserSettings.AnalitFSchedules s
WHERE 
	s.ClientId = ?ClientCode
and s.Enable = 1
order by s.Hour, s.Minute";

			return "select null from Customers.Clients limit 0";
		}

		public static bool UserExists(MySqlConnection connection, string userName)
		{
			var exists = false;

			var userId = MySqlHelper.ExecuteScalar(
				connection,
				"select Id from Customers.Users where Login = ?userName",
				new MySqlParameter("?userName", userName));
			if (userId != null)
				exists = true;

			return exists;
		}

		public static void UpdateRequestType(MySqlConnection readWriteConnection, UpdateData updateData, ulong updateId, string addition, uint resultSize)
		{
			if (updateId <= 0)
				throw new Exception("Значение updateId при обновлении типа запроса меньше или равно 0: {0}".Format(updateId));

			With.DeadlockWraper(() => {
				var realUpdateType = MySqlHelper.ExecuteScalar(
					readWriteConnection,
					"select UpdateType from logs.AnalitFUpdates where UpdateId = ?UpdateId",
					new MySqlParameter("?UpdateId", updateId));

				var allowedTypes = new int[] { Convert.ToInt32(RequestType.GetCumulativeAsync), Convert.ToInt32(RequestType.GetLimitedCumulativeAsync), Convert.ToInt32(RequestType.GetDataAsync) };

				if (realUpdateType != null && Convert.ToInt32(realUpdateType) > 0 && allowedTypes.Any(i => i == Convert.ToInt32(realUpdateType))) {
					var transaction = readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
					try {
						var newUpdateType = RequestType.GetData;
						if (Convert.ToInt32(realUpdateType) == Convert.ToInt32(RequestType.GetCumulativeAsync))
							newUpdateType = RequestType.GetCumulative;
						if (Convert.ToInt32(realUpdateType) == Convert.ToInt32(RequestType.GetLimitedCumulativeAsync))
							newUpdateType = RequestType.GetLimitedCumulative;

						MySqlHelper.ExecuteNonQuery(
							readWriteConnection,
							"update logs.AnalitFUpdates set UpdateType = ?UpdateType, Addition = ?Addition, ResultSize = ?ResultSize where UpdateId = ?UpdateId",
							new MySqlParameter("?UpdateId", updateId),
							new MySqlParameter("?UpdateType", Convert.ToInt32(newUpdateType)),
							new MySqlParameter("?Addition", addition),
							new MySqlParameter("?ResultSize", resultSize));

						transaction.Commit();
					}
					catch {
						With.SafeRollback(transaction);
						throw;
					}
				}
				else
					throw new Exception("Неожидаемый тип {0} у запроса c updateId {1}".Format(realUpdateType, updateId));
			});
		}

		public string GetConfirmMailsCommnad(uint? updateId)
		{
			return @"
update Logs.MailSendLogs ms
set ms.Committed = 1
where ms.updateid = {0};
update Logs.AttachmentSendLogs ms
set ms.Committed = 1
where ms.updateid = {0};"
				.Format(updateId);
		}

		public void WaitParsedDocs()
		{
			var startTime = DateTime.Now;
			int? waitCount = null;
			do {
				var realWaitCount = MySqlHelper.ExecuteScalar(_readWriteConnection, @"
select
	count(dl.RowId)-count(dh.Id) as waitCount
from
	logs.AnalitFUpdates afu
	inner join logs.document_logs dl on dl.SendUpdateId = afu.UpdateId
	left join documents.DocumentHeaders dh on dh.DownloadId = dl.RowId
where
	afu.RequestTime > curdate() - interval 1 day
and afu.UserId = ?UserId
and afu.UpdateType = ?UpdateType
group by afu.UserId",
					new MySqlParameter("?UserId", _updateData.UserId),
					new MySqlParameter("?UpdateType", (int)RequestType.SendWaybills));
				waitCount = realWaitCount != null ? (int?)Convert.ToInt32(realWaitCount) : null;
			} while (waitCount > 0 && DateTime.Now.Subtract(startTime).TotalSeconds < 60);
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
	and (ProducerLogs.Operation = 2)";
		}

		public int ExportOffers()
		{
			var exportSupplierPriceMarkup = (_updateData.BuildNumber > 1027) || (_updateData.EnableUpdate() && ((_updateData.BuildNumber >= 945) || ((_updateData.BuildNumber >= 705) && (_updateData.BuildNumber <= 716)) || ((_updateData.BuildNumber >= 829) && (_updateData.BuildNumber <= 837))));
			var exportBuyingMatrix = _updateData.BuildNumber >= 1249 || _updateData.NeedUpdateToBuyingMatrix;
			var watch = new Stopwatch();
			watch.Start();
			SelectOffers();
			CostOptimizer.OptimizeCostIfNeeded(_readWriteConnection, _updateData.ClientId, _updateData.UserId);

			int count;
			if (!TryExportAndOptimizeCosts(exportSupplierPriceMarkup, exportBuyingMatrix, out count)) {
				var sql = GetCoreCommand(false, exportSupplierPriceMarkup, exportBuyingMatrix);
				count = GetMySQLFileWithDefaultEx("Core", _readWriteConnection, sql,
					(_updateData.BuildNumber <= 1027) && _updateData.EnableUpdate(),
					true);
			}
			watch.Stop();
			log.DebugFormat("Экспорт предложений завершен за {0} экспортировано {1} позиций", watch.Elapsed, count);
			return count;
		}

		private bool TryExportAndOptimizeCosts(bool exportSupplierPriceMarkup, bool exportBuyingMatrix, out int count)
		{
			count = 0;

			if (!_updateData.SupportExportExp())
				return false;

			using (var session = SessionFactory.OpenSession(_readWriteConnection)) {
				var user = session.Load<User>(_updateData.UserId);
				var activePrices = session.Query<AFActivePrice>().ToList();
				var supplierIds = activePrices.Select(p => p.Id.Price.Supplier.Id).Distinct().ToArray();
				var optimizer = MonopolisticsOptimizer.Load(session, user, supplierIds);
				var rules = optimizer.Rules;
				//отметить те прайс-листы которые мы будем передавать из-за того что обновился кто-то из конкурентов
				var fresh = activePrices.Where(p => p.Fresh).Select(p => p.Id.Price.Supplier).ToArray();
				var topatch = rules
					.Where(r => !fresh.Contains(r.Supplier) && r.Concurrents.Intersect(fresh).Any())
					.Select(r => r.Supplier);
				activePrices
					.Where(p => topatch.Contains(p.Id.Price.Supplier))
					.Each(p => p.Fresh = true);
				var exportable = activePrices.Where(p => p.Fresh).Select(p => p.Id.Price.PriceCode).OrderBy(i => i).ToArray();

				var query = GetOfferQuery(true, true, true, true);
				query.Select("ct.Id", "at.FirmCode", "core.MaxBoundCost",
					"if(k.Id is null or k.Date < at.PriceDate, core.OptimizationSkip, 1) as OptimizationSkip");
				query.Join("left join farm.CachedCostKeys k on k.PriceId = ct.PriceCode " +
					"and k.RegionId = ct.RegionCode and k.ClientId = ?clientCode");
				query.Join("left join farm.CachedCosts ca on ca.CoreId = core.Id and ca.KeyId = k.Id");
				if (log.IsDebugEnabled)
					log.Debug(query.ToSql());
				var cmd = new MySqlCommand(query.ToSql(), _readWriteConnection);
				//для вычисления монопольных предложений нам нужно выбрать все предложения
				SetUpdateParameters(cmd);
				cmd.Parameters["?Cumulative"].Value = true;

				var offers = new List<Offer2>();
				using (var reader = cmd.ExecuteReader()) {
					log.Debug("Данные выбраны, начинаю загрузку");
					while (reader.Read()) {
						offers.Add(new Offer2 {
							//поля для экспорта
							PriceId = reader.GetUInt32(0),
							RegionId = reader.GetUInt64(1),
							ProductId = reader.GetUInt32(2),
							ProducerId = reader.GetNullableUInt32(3),
							SynonymCode = reader.GetUInt32(4),
							SynonymFirmCrCode = reader.GetNullableUInt32(5),
							Code = reader.SafeGetString(6),
							CodeCr = reader.SafeGetString(7),
							Unit = reader.SafeGetString(8),
							Volume = reader.SafeGetString(9),
							Junk = reader.GetBoolean(10),
							Await = reader.GetBoolean(11),
							RawQuantity = reader.SafeGetString(12),
							Note = reader.SafeGetString(13),
							Period = reader.SafeGetString(14),
							Doc = reader.SafeGetString(15),
							RegistryCost = reader.GetNullableFloat(16),
							VitallyImportant = reader.GetBoolean(17),
							RequestRatio = reader.GetNullableUInt32(18),
							Cost = reader.GetDecimal(19),
							ClientOfferId = reader.SafeGetString(20),
							OrderCost = reader.GetNullableFloat(21),
							MinOrderCount = reader.GetNullableUInt32(22),
							SupplierPriceMarkup = reader.GetNullableDecimal(23),
							ProducerCost = reader.GetNullableFloat(24),
							Nds = reader.GetNullableUInt32(25),
							RetailVitallyImportan = reader.GetBoolean(26),
							BuyingMatrixType = reader.GetUInt32(27),
							EAN13 = reader.SafeGetString(28),
							CodeOKP = reader.SafeGetString(29),
							Series = reader.SafeGetString(30),
							Exp = reader.GetNullableDateTime(31),
							//поля для оптимизации цен
							OfferId = reader.GetUInt64(32),
							SupplierId = reader.GetUInt32(33),
							MaxBoundCost = reader.GetNullableFloat(34),
							OptimizationSkip = reader.GetBoolean(35)
						});
					}
				}
				log.Debug("Загрузка завершена, начинаю оптимизацию");
				var logs = optimizer.Optimize(offers);
				optimizer.UpdateCostCache(session, activePrices.Select(x => new ActivePrice { Id = x.Id }), logs);
				CostOptimizer.SaveLogs(_readWriteConnection, logs, _updateData.UserId, _updateData.ClientId);
				log.Debug("Оптимизация завершена, начинаю выгрузку");

				var columnCount = 32;
				if (!exportSupplierPriceMarkup) {
					//добавили Nds, ProducerCost, SupplierPriceMarkup
					columnCount -= 3 + 1 + 1 + 3 + 1;
				}
				else if (!exportBuyingMatrix) {
					//добавили BuyingMatrixType
					columnCount -= 1 + 1 + 3 + 1;
				}
				else if (!_updateData.AllowDelayByPrice()) {
					//добавили RetailVitallyImportan
					columnCount -= 1 + 3 + 1;
				}
				else if (!_updateData.AllowEAN13()) {
					//добавили CodeOKP, EAN13, Series
					columnCount -= 3 + 1;
				}
				else if (!_updateData.SupportExportExp()) {
					//добавили Exp
					columnCount -= 1;
				}
				var filename = ServiceContext.GetFileByShared("Core" + _updateData.UserId + ".txt");
				using (var file = new StreamWriter(filename, false, Encoding.GetEncoding(1251))) {
					var toexport = offers
						.Where(o => _updateData.Cumulative || Array.BinarySearch(exportable, o.PriceId) >= 0)
						.Select(o => new object[] {
							o.PriceId,
							o.RegionId,
							o.ProductId,
							o.ProducerId.GetValueOrDefault(),
							o.SynonymCode,
							o.SynonymFirmCrCode,
							o.Code,
							o.CodeCr,
							o.Unit,
							o.Volume,
							o.Junk,
							o.Await,
							o.RawQuantity,
							o.Note,
							o.Period,
							o.Doc,
							o.RegistryCost,
							o.VitallyImportant,
							o.RequestRatio,
							o.Cost,
							o.ClientOfferId,
							o.OrderCost,
							o.MinOrderCount,
							o.SupplierPriceMarkup,
							o.ProducerCost,
							o.Nds,
							o.RetailVitallyImportan,
							o.BuyingMatrixType,
							o.EAN13,
							o.CodeOKP,
							o.Series,
							o.Exp
						});
					count = global::Common.MySql.MySqlHelper.Export(toexport, file, 0, columnCount);
				}
				_updateData.FilesForArchive.Enqueue(filename);
				session.Flush();
			}
			return true;
		}
	}
}