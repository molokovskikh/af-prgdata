using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Web;
using MySql.Data.MySqlClient;
using System.Threading;
using Common.MySql;
using PrgData.Common.Model;
using log4net;
using PrgData.Common.Orders;
using PrgData.Common.SevenZip;
using MySqlHelper = MySql.Data.MySqlClient.MySqlHelper;
using System.Collections.Generic;
using Common.Tools;

namespace PrgData.Common
{


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

		public MySqlConnection ReadWriteConnection
		{
			get { return _readWriteConnection; }
		}

		//Код поставщика 7664
		public uint MaxProducerCostsPriceId { get; private set; }
		private uint maxProducerCostsCostId;

		public UpdateHelper(UpdateData updateData, MySqlConnection readWriteConnection)
		{
			MaxProducerCostsPriceId = 4863;
			maxProducerCostsCostId = 8148;
			_updateData = updateData;
			_readWriteConnection = readWriteConnection;
		}

		public static Func<string> GetDownloadUrl =
			() => HttpContext.Current.Request.Url.Scheme
				+ Uri.SchemeDelimiter
				+ HttpContext.Current.Request.Url.Authority
				+ HttpContext.Current.Request.ApplicationPath;

		public static string GetFullUrl(string handlerName)
		{
			var downloadUrl = GetDownloadUrl();
			if (downloadUrl.EndsWith("/"))
				downloadUrl = downloadUrl.Slice(downloadUrl.Length-1);
			return downloadUrl + "/" + handlerName;
		}

		public static string GetConfirmDocumentsCommnad(uint? updateId)
		{
			return @"
update Logs.DocumentSendLogs ds
set ds.Committed = 1
where ds.updateid = " + updateId;
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
"
				,
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

			if (_updateData.EnableImpersonalPrice)
			{
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
	ap.UserId is not null as AFPermissionExists
FROM  
  Customers.users u
  join Customers.Clients c                         on c.Id = u.ClientId
  join usersettings.retclientsset               on retclientsset.clientcode = c.Id
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
limit 1;"
				, 
				connection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?user", userName);

			var data = new DataSet();
			dataAdapter.Fill(data);

			if (data.Tables[0].Rows.Count > 0)
				updateData = new UpdateData(data);

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

			if (smartOrderData.Tables[0].Rows.Count > 0)
			{
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
	   rcs.ShowAdvertising
FROM Customers.Clients c
	join Customers.Users u on c.Id = u.Clientid
	join usersettings.RetClientsSet rcs on rcs.ClientCode = u.Clientid
	join farm.regions r on r.RegionCode = c.RegionCode
	join UserUpdateInfo uui on u.Id = uui.UserId
WHERE u.Id = ?UserId", _readWriteConnection);

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
			if (!_updateData.EnableImpersonalPrice)
			{
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

				SelectPrices();
				SelectReplicationInfo();
				SelectActivePrices();

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
	and d.LogTime > curdate() - interval 45 day
order by d.LogTime desc
limit 400;
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
", downloadIds);
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
"
				,
				downloadIds,
				!_updateData.AllowDelayWithVitallyImportant() 
					? String.Empty 
					: @"
  ,
  DocumentBodies.Amount,
  DocumentBodies.NdsAmount",
				!_updateData.AllowInvoiceHeaders()
					? String.Empty
					: @"
  ,
  DocumentBodies.Unit,
  DocumentBodies.ExciseTax,
  DocumentBodies.BillOfEntryNumber,
  DocumentBodies.EAN13",
				!_updateData.AllowCertificates()
					? String.Empty
					: @"
  ,
  DocumentBodies.ProductId,
  DocumentBodies.ProducerId");
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
"
				,
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

		public string GetClientsCommand(bool isFirebird)
		{
			uint? networkPriceId = null;
			var networkSelfAddressIdColumn = " , null as SelfAddressId ";
			var networkSelfAddressIdJoin = String.Empty; 
			if (_updateData.NetworkPriceId.HasValue)
			{
				networkSelfAddressIdColumn = _updateData.NetworkPriceId.HasValue ? ", ai.SupplierDeliveryId as SelfAddressId " : ", a.Id as SelfAddressId";
				networkSelfAddressIdJoin =
					_updateData.NetworkPriceId.HasValue
						? " left join Customers.Intersection i on i.ClientId = a.ClientId and i.RegionId = c.RegionCode and i.LegalEntityId = a.LegalEntityId and i.PriceId = " +
							_updateData.NetworkPriceId +
							" left join Customers.AddressIntersection ai on ai.IntersectionId = i.Id and ai.AddressId = a.Id  "
						: "";
			}

			if (_updateData.BuildNumber > 1271 || _updateData.NeedUpdateToNewClientsWithLegalEntity)
			{
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
				_updateData.AllowExcessAvgOrderTimes() ? " , rcs.ExcessAvgOrderTimes " : "");
			}
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
		 {2}
	FROM Customers.Users u
	  join Customers.Clients c on u.ClientId = c.Id
	  join usersettings.RetClientsSet rcs on c.Id = rcs.ClientCode
	  join Customers.UserAddresses ua on ua.UserId = u.Id
	  join Customers.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
	  {3}
	WHERE 
		u.Id = ?UserId
	and a.Enabled = 1",
				isFirebird ? "'', " : "",
				isFirebird ? "" : ", rcs.AllowDelayOfPayment, c.FullName ",
				isFirebird ? "" : networkSelfAddressIdColumn,
				isFirebird ? "" : networkSelfAddressIdJoin);
		}

		public string GetClientCommand()
		{
			return String.Format(@"
SELECT 
	c.Id as ClientId,
	left(c.Name, 50) as Name,
	regions.CalculateOnProducerCost,
	rcs.ParseWaybills,
	rcs.SendRetailMarkup,
	rcs.ShowAdvertising,
	rcs.SendWaybillsFromClient,
	rcs.EnableSmartOrder,
	rcs.EnableImpersonalPrice
{0}
{1}
FROM Customers.Users u
join Customers.Clients c on u.ClientId = c.Id
join farm.regions on regions.RegionCode = c.RegionCode
join usersettings.RetClientsSet rcs on rcs.ClientCode = c.Id
WHERE u.Id = ?UserId
",
				_updateData.AllowShowSupplierCost() ? ", rcs.AllowDelayOfPayment " : String.Empty,
				_updateData.AllowCertificates() ? ", rcs.ShowCertificatesWithoutRefSupplier " : String.Empty);
		}

		public string GetDelayOfPaymentsCommand()
		{
			if (_updateData.EnableImpersonalPrice)
				return "select null from Customers.Clients limit 0";
			else if (_updateData.AllowDelayByPrice())
			{
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
			else if (_updateData.AllowDelayWithVitallyImportant())
			{
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
			else
			{
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

		public string GetMNNCommand(bool before1150, bool Cumulative, bool after1263)
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
				if (after1263)
				{
					if (Cumulative)
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
  Mnn.UpdateTime > ?UpdateTime
union
select
  MnnLogs.MnnId,
  MnnLogs.Mnn,
  1 as Hidden
from
  logs.MnnLogs
where
	(MnnLogs.LogTime >= ?UpdateTime) 
and (MnnLogs.Operation = 2)
";
				}
				else
				{
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
  if(not ?Cumulative, Descriptions.UpdateTime > ?UpdateTime, 1)
and Descriptions.NeedCorrect = 0";
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
  Descriptions.UpdateTime > ?UpdateTime
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

		private string GetAbstractPromotionsCommand()
		{
			return
				@"
select
	log.PromotionId as Id,
	0 as Status,
	log.SupplierId,
	log.Name,
	log.Annotation,
	log.PromoFile,
	log.Begin,
	log.End
from
	logs.SupplierPromotionLogs log
where
	log.LogTime > '2011-03-01'
and log.Operation = 2
and not ?Cumulative
union
select
	SupplierPromotions.Id,
	SupplierPromotions.Status,
	SupplierPromotions.SupplierId,
	SupplierPromotions.Name,
	SupplierPromotions.Annotation,
	SupplierPromotions.PromoFile,
	SupplierPromotions.Begin,
	SupplierPromotions.End
from
	usersettings.SupplierPromotions	
where";
		}

		public string GetPromotionsCommand()
		{
			return GetAbstractPromotionsCommand() +
@"
	if(not ?Cumulative, 1, SupplierPromotions.Status)
	";
		}

		public string GetPromotionsCommandById(List<uint> promotionIds)
		{
			return
				GetAbstractPromotionsCommand() +
				string.Format("  SupplierPromotions.Id in ({0})", promotionIds.Implode());
		}

		public string GetPromotionCatalogsCommandById(List<uint> promotionIds)
		{
			return
				String.Format(
				@"
select 
  CatalogId,
  PromotionId,
  1 as Hidden
from
  logs.PromotionCatalogLogs
where
	LogTime > ?UpdateTime
and Operation = 2
and not ?Cumulative
union
select 
  CatalogId,
  PromotionId,
  0 as Hidden
from
  usersettings.PromotionCatalogs
where
  PromotionId in ({0})
", promotionIds.Implode());
		}

		public List<SupplierPromotion> GetPromotionsList(MySqlCommand sqlCommand)
		{
			var list = new List<SupplierPromotion>();

			if (!_updateData.ShowAdvertising)
				return list;

			var dataAdapter = new MySqlDataAdapter(sqlCommand);
			var dataTable = new DataTable();
			bool oldCumulative = false;

			try
			{
				if (sqlCommand.Parameters.Contains("?Cumulative"))
				{
					oldCumulative = Convert.ToBoolean(sqlCommand.Parameters["?Cumulative"].Value);
					if (_updateData.NeedUpdateToSupplierPromotions)
						sqlCommand.Parameters["?Cumulative"].Value = true;
				}
				sqlCommand.CommandText = GetPromotionsCommand();
				dataAdapter.Fill(dataTable);
			}
			finally
			{
				if (sqlCommand.Parameters.Contains("?Cumulative"))
					sqlCommand.Parameters["?Cumulative"].Value = oldCumulative;
			}

			foreach (DataRow row in dataTable.Rows)
			{
				list.Add(
					new SupplierPromotion 
					{ 
						Id = Convert.ToUInt32(row["Id"]),
						Status = Convert.ToBoolean(row["Status"])
					});
			}
			return list;
		}

		public void ArchivePromotions(MySqlConnection connection, string archiveFileName, bool cumulative, DateTime oldUpdateTime, DateTime currentUpdateTime, ref string addition, Queue<FileForArchive> filesForArchive)
		{
			var log = LogManager.GetLogger(typeof(UpdateHelper));

			try
			{
				log.Debug("Будем выгружать акции");

				var command = new MySqlCommand();
				command.Connection = connection;
				SetUpdateParameters(command, cumulative, oldUpdateTime, currentUpdateTime);

				ExportSupplierPromotions(archiveFileName, command, filesForArchive);

				ArchivePromoFiles(archiveFileName, command);
			}
			catch (Exception exception)
			{
				log.Error("Ошибка при архивировании акций поставщиков", exception);
				addition += "Архивирование акций поставщиков: " + exception.Message + "; ";

				ShareFileHelper.MySQLFileDelete(archiveFileName);
			}
		}

		private void ArchivePromoFiles(string archiveFileName, MySqlCommand command)
		{
			var promotionsFolder = "Promotions";
			var promotionsPath = Path.Combine(_updateData.ResultPath, promotionsFolder);
			if (!Directory.Exists(promotionsPath))
				Directory.CreateDirectory(promotionsPath);

			foreach (var supplierPromotion in _updateData.SupplierPromotions)
			{
				if (supplierPromotion.Status)
				{
					var files = Directory.GetFiles(promotionsPath, supplierPromotion.Id + "*");
					if (files.Length > 0)
					{
						SevenZipHelper.ArchiveFilesWithNames(
							archiveFileName,
							Path.Combine(promotionsFolder, supplierPromotion.Id + "*"),
							_updateData.ResultPath);
					}
				}
			}
		}


		public void ArchiveCertificates(MySqlConnection connection,
			string archiveFileName,
			bool cumulative,
			DateTime oldUpdateTime,
			DateTime currentUpdateTime,
			ref string addition,
			ref string updateLog,
			uint updateId,
			Queue<FileForArchive> filesForArchive)
		{
			var log = LogManager.GetLogger(typeof(UpdateHelper));

			try
			{
				log.Debug("Будем выгружать сертификаты");

				var command = new MySqlCommand();
				command.Connection = connection;
				SetUpdateParameters(command, cumulative, oldUpdateTime, currentUpdateTime);

				ExportCertificates(archiveFileName, command, filesForArchive);

				updateLog = ArchiveCertificatesFiles(archiveFileName, command);
			}
			catch (Exception exception)
			{
				log.Error("Ошибка при архивировании сертификатов", exception);
				addition += "Архивирование сертификатов: " + exception.Message + "; ";

				ShareFileHelper.MySQLFileDelete(archiveFileName);
			}
		}

		private void ExportCertificates(string archiveFileName, MySqlCommand command, Queue<FileForArchive> filesForArchive)
		{
			var certificateRequestsFile = DeleteFileByPrefix("CertificateRequests");
			var certificatesFile = DeleteFileByPrefix("Certificates");
			var certificateSourcesFile = DeleteFileByPrefix("CertificateSources");
			var sourceSuppliersFile = DeleteFileByPrefix("SourceSuppliers");
			var certificateFilesFile = DeleteFileByPrefix("CertificateFiles");
			var fileCertificatesFile = DeleteFileByPrefix("FileCertificates");

			ProcessCertificates(command);

			File.WriteAllText(ServiceContext.MySqlLocalImportPath() + certificateRequestsFile, _updateData.GetCertificatesResult());

			ProcessArchiveFile(certificateRequestsFile, archiveFileName);


			GetMySQLFileWithDefaultEx("Certificates", command, GetCertificatesCommand(), false, false, filesForArchive);

			ProcessArchiveFile(certificatesFile, archiveFileName);


			GetMySQLFileWithDefaultEx("CertificateSources", command, GetCertificateSourcesCommand(), false, false, filesForArchive);

			ProcessArchiveFile(certificateSourcesFile, archiveFileName);


			GetMySQLFileWithDefaultEx("SourceSuppliers", command, GetSourceSuppliersCommand(), false, false, filesForArchive);

			ProcessArchiveFile(sourceSuppliersFile, archiveFileName);


			GetMySQLFileWithDefaultEx("CertificateFiles", command, GetCertificateFilesCommand(), false, false, filesForArchive);

			ProcessArchiveFile(certificateFilesFile, archiveFileName);


			GetMySQLFileWithDefaultEx("FileCertificates", command, GetFileCertificatesCommand(), false, false, filesForArchive);

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
		cf.Id in (" + ids + ")";
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
		cf.Id in (" + ids + ")";
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
		cf.Id in (" + ids + ")";
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
		c.Id in (" + ids + ")";
		}

		private void ProcessCertificates(MySqlCommand command)
		{
			var showWithoutSuppliers = Convert.ToBoolean(
				MySqlHelper.ExecuteScalar(
				command.Connection, 
				"select ShowCertificatesWithoutRefSupplier from UserSettings.RetClientsSet where ClientCode = ?clientId", 
				new MySqlParameter("?clientId", _updateData.ClientId)));

			if (showWithoutSuppliers)
				command.CommandText = @"
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
				command.CommandText = @"
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

			command.Parameters.Add("?bodyId", MySqlDbType.UInt32);

			var dataAdapter = new MySqlDataAdapter(command);

			foreach (var certificateRequest in _updateData.CertificateRequests) {
				command.Parameters["?bodyId"].Value = certificateRequest.DocumentBodyId;

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
#if DEBUG
			ShareFileHelper.WaitFile(ServiceContext.MySqlLocalImportPath() + processedFile);
#endif

			try
			{
				SevenZipHelper.ArchiveFiles(archiveFileName, ServiceContext.MySqlLocalImportPath() + processedFile);
				var log = LogManager.GetLogger(typeof(UpdateHelper));
				log.DebugFormat("файл для архивации: {0}", ServiceContext.MySqlLocalImportPath() + processedFile);
			}
			catch
			{
				ShareFileHelper.MySQLFileDelete(archiveFileName);
				throw;
			}

			ShareFileHelper.MySQLFileDelete(ServiceContext.MySqlLocalImportPath() + processedFile);

			ShareFileHelper.WaitDeleteFile(ServiceContext.MySqlLocalImportPath() + processedFile);
		}

		private string ArchiveCertificatesFiles(string archiveFileName, MySqlCommand command)
		{
			var certificatesFolder = "Certificates";
			var certificatesPath = Path.Combine(_updateData.ResultPath, certificatesFolder);

			foreach (var request in _updateData.CertificateRequests) {
				foreach (var fileId in request.CertificateFiles) {
					var files = Directory.GetFiles(certificatesPath, fileId + ".*");
					if (files.Length > 0)
					{
						SevenZipHelper.ArchiveFilesWithNames(
							archiveFileName,
							Path.Combine(certificatesFolder, fileId + ".*"),
							_updateData.ResultPath);

						request.SendedFiles.AddRange(files);
					}
				}
			}

			return BuildLog(command);
		}

		private string BuildLog(MySqlCommand command)
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
			var adapter = new MySqlDataAdapter(sql, command.Connection);
			var table = new DataTable();
			adapter.Fill(table);

			if (table.Rows.Count == 0)
				return "";

			var writer = new StringWriter();
			writer.WriteLine("Отправлены сертификаты:");

			foreach (var row in table.AsEnumerable())
			{
				var id = Convert.ToUInt32(row["id"]);
				var files = sended.Where(s => s.DocumentBodyId == id).SelectMany(s => s.SendedFiles);
				foreach (var file in files)
				{
					writer.WriteLine("Номер документа = {0}, Сопоставленный продукт = {1}, Файл = {2}",
						row["DownloadId"],
						row["Name"],
						Path.GetFileName(file));
				}
			}

			return writer.ToString();
		}

		private void GetMySQLFileWithDefaultEx(string FileName, MySqlCommand MyCommand, string SQLText, bool SetCumulative, bool AddToQueue, Queue<FileForArchive> filesForArchive)
		{
			var log = LogManager.GetLogger(typeof(UpdateHelper));
			var SQL = SQLText;
			bool oldCumulative = false;

			try
			{
				if(SetCumulative && MyCommand.Parameters.Contains("?Cumulative"))
				{
					oldCumulative = Convert.ToBoolean(MyCommand.Parameters["?Cumulative"].Value);
					MyCommand.Parameters["?Cumulative"].Value = true;
				}

				var fullName = Path.Combine(ServiceContext.MySqlSharedExportPath(), FileName + _updateData.UserId + ".txt");
				fullName = MySqlHelper.EscapeString(fullName);

				SQL += " INTO OUTFILE '" + fullName + "' ";

				log.DebugFormat("SQL команда для выгрузки акций: {0}", SQL);

				MyCommand.CommandText = SQL;
				MyCommand.ExecuteNonQuery();
			}
			finally
			{
				if (SetCumulative && MyCommand.Parameters.Contains("?Cumulative"))
					MyCommand.Parameters["?Cumulative"].Value = oldCumulative;
			
			}

			if (AddToQueue)
			{
				lock (filesForArchive)
				{
					filesForArchive.Enqueue(new FileForArchive(FileName, false));
				}
			}
		}
		
		private string DeleteFileByPrefix(string prefix)
		{
			var deletedFile = prefix + _updateData.UserId + ".txt";
			ShareFileHelper.MySQLFileDelete(ServiceContext.MySqlLocalImportPath() + deletedFile);

			ShareFileHelper.WaitDeleteFile(ServiceContext.MySqlLocalImportPath() + deletedFile);

			return deletedFile;
		}

		private void ExportSupplierPromotions(string archiveFileName, MySqlCommand command, Queue<FileForArchive> filesForArchive)
		{
			var supplierFile = DeleteFileByPrefix("SupplierPromotions");
			var catalogFile = DeleteFileByPrefix("PromotionCatalogs");

			var ids = _updateData.SupplierPromotions.Select(promotion => promotion.Id).ToList();

			GetMySQLFileWithDefaultEx("SupplierPromotions", command, GetPromotionsCommandById(ids), false, false, filesForArchive);

#if DEBUG
			ShareFileHelper.WaitFile(ServiceContext.MySqlLocalImportPath() + supplierFile);
#endif

			try
			{
				SevenZipHelper.ArchiveFiles(archiveFileName, ServiceContext.MySqlLocalImportPath() + supplierFile);
				var log = LogManager.GetLogger(typeof(UpdateHelper));
				log.DebugFormat("файл для архивации: {0}", ServiceContext.MySqlLocalImportPath() + supplierFile);
			}
			catch
			{
				ShareFileHelper.MySQLFileDelete(archiveFileName);
				throw;
			}

			ShareFileHelper.MySQLFileDelete(ServiceContext.MySqlLocalImportPath() + supplierFile);

			ShareFileHelper.WaitDeleteFile(ServiceContext.MySqlLocalImportPath() + supplierFile);



			GetMySQLFileWithDefaultEx("PromotionCatalogs", command, GetPromotionCatalogsCommandById(ids), false, false, filesForArchive);

#if DEBUG
			ShareFileHelper.WaitFile(ServiceContext.MySqlLocalImportPath() + catalogFile);
#endif
			try
			{
				SevenZipHelper.ArchiveFiles(archiveFileName, ServiceContext.MySqlLocalImportPath() + catalogFile);
				var log = LogManager.GetLogger(typeof(UpdateHelper));
				log.DebugFormat("файл для архивации: {0}", ServiceContext.MySqlLocalImportPath() + catalogFile);
			}
			catch
			{
				ShareFileHelper.MySQLFileDelete(archiveFileName);
				throw;
			}

			ShareFileHelper.MySQLFileDelete(ServiceContext.MySqlLocalImportPath() + catalogFile);

			ShareFileHelper.WaitDeleteFile(ServiceContext.MySqlLocalImportPath() + catalogFile);
		}

		public string GetCatalogCommand(bool before1150, bool Cumulative)
		{
			if (before1150)
			{
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
			IF(NOT ?Cumulative, C.UpdateTime  > ?UpdateTime, 1)
		OR	IF(NOT ?Cumulative, CN.UpdateTime > ?UpdateTime, 1)
		OR	IF(NOT ?Cumulative and d.Id is not null, d.UpdateTime > ?UpdateTime, ?Cumulative)
	)
AND C.hidden = 0";
			}
			else if (_updateData.AllowRetailMargins()) {
				if (Cumulative) {
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
			IF(NOT ?Cumulative, C.UpdateTime  > ?UpdateTime, 1)
		OR	IF(NOT ?Cumulative, CN.UpdateTime > ?UpdateTime, 1)
		OR	IF(NOT ?Cumulative and d.Id is not null, d.UpdateTime > ?UpdateTime, ?Cumulative)
		OR	IF(NOT ?Cumulative and rm.Id is not null, {0}, ?Cumulative)
	)"
						,
						_updateData.NeedUpdateForRetailMargins() ? "1" : "rm.UpdateTime > ?UpdateTime");
				}
			}
			else if (Cumulative)
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
			IF(NOT ?Cumulative, C.UpdateTime  > ?UpdateTime, 1)
		OR	IF(NOT ?Cumulative, CN.UpdateTime > ?UpdateTime, 1)
		OR	IF(NOT ?Cumulative and d.Id is not null, d.UpdateTime > ?UpdateTime, ?Cumulative)
	)
";
		}

		public string GetCoreCommand(bool exportInforoomPrice, bool exportSupplierPriceMarkup, bool exportBuyingMatrix, bool cryptCost)
		{
			string buyingMatrixCondition;
			string buyingMatrixProducerNullCondition = " 0 ";
			string offerMatrixProducerNullCondition = " 0 ";

			if (exportInforoomPrice) {
				if (_updateData.BuyingMatrixPriceId.HasValue)
				{
					if (_updateData.BuyingMatrixType == 0)
					{
						//белый список
						buyingMatrixCondition = ", if(list.Id is not null, 0, " + (_updateData.WarningOnBuyingMatrix ? "2" : "1") + ") as BuyingMatrixType";
						buyingMatrixProducerNullCondition = " 0 ";
					}
					else
					{
						//черный список
						buyingMatrixCondition = ", if(list.Id is null, 0, " + (_updateData.WarningOnBuyingMatrix ? "2" : "1") + ") as BuyingMatrixType";
						buyingMatrixProducerNullCondition = " 1 ";
					}
				}
				else
					//разрешено все
					buyingMatrixCondition = ", 0 as BuyingMatrixType";
			}
			else {
				//Включена матрица предложений
				if (_updateData.OfferMatrixPriceId.HasValue) {

					if (_updateData.BuyingMatrixPriceId.HasValue) {
						if (_updateData.BuyingMatrixType == 0)
						{
							//белый список
							buyingMatrixCondition = " if(list.Id is not null, 0, " + (_updateData.WarningOnBuyingMatrix ? "2" : "1") + ") ";
							buyingMatrixProducerNullCondition = " 0 ";
						}
						else
						{
							//черный список
							buyingMatrixCondition = " if(list.Id is null, 0, " + (_updateData.WarningOnBuyingMatrix ? "2" : "1") + ") ";
							buyingMatrixProducerNullCondition = " 1 ";
						}
					}
					else {
						//разрешено все
						buyingMatrixCondition = " 0 ";
					}

					if (_updateData.OfferMatrixType == 0)
					{
						//белый список - попал в список => попал в предложения
						buyingMatrixCondition = ", if(oms.Id is not null or offerList.Id is not null, " + buyingMatrixCondition + ", 1) as BuyingMatrixType ";
						offerMatrixProducerNullCondition = " 0 ";
					}
					else
					{
						//черный список - не попал в список => попал в предложения
						buyingMatrixCondition = ", if(oms.Id is not null or offerList.Id is null, " + buyingMatrixCondition + ", 1) as BuyingMatrixType ";
						offerMatrixProducerNullCondition = " 1 ";
					}

				}
				else 
					//включена матрица закупок
					if (_updateData.BuyingMatrixPriceId.HasValue) {
						if (_updateData.BuyingMatrixType == 0)
						{
							//белый список
							buyingMatrixCondition = ", if(list.Id is not null, 0, " + (_updateData.WarningOnBuyingMatrix ? "2" : "1") + ") as BuyingMatrixType";
							buyingMatrixProducerNullCondition = " 0 ";
						}
						else
						{
							//черный список
							buyingMatrixCondition = ", if(list.Id is null, 0, " + (_updateData.WarningOnBuyingMatrix ? "2" : "1") + ") as BuyingMatrixType";
							buyingMatrixProducerNullCondition = " 1 ";
						}
					}
					//ничего не включено
					else {
						//разрешено все
						buyingMatrixCondition = ", 0 as BuyingMatrixType";
					}
			}

			if (exportInforoomPrice)
				if (!exportSupplierPriceMarkup)
					return @"
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
					return 
						String.Format(
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

"
	,
		exportBuyingMatrix ? buyingMatrixCondition : "",
		exportBuyingMatrix && _updateData.BuyingMatrixPriceId.HasValue ? @" 
  left join catalogs.Products on Products.Id = A.ProductId
  left join farm.BuyingMatrix list on list.ProductId = Products.Id and if(list.ProducerId is null, 1, if(a.CodeFirmCr is null, " + buyingMatrixProducerNullCondition + ", list.ProducerId = a.CodeFirmCr)) and list.PriceId = " + _updateData.BuyingMatrixPriceId : "",
		exportBuyingMatrix && _updateData.BuyingMatrixPriceId.HasValue ? @" 
  left join catalogs.Products on Products.Id = A.ProductId
  left join farm.BuyingMatrix list on list.ProductId = Products.Id and if(list.ProducerId is null, 1,  " + buyingMatrixProducerNullCondition + ") and list.PriceId = " + _updateData.BuyingMatrixPriceId : ""
	 );
			else
				return 
				String.Format(@"
SELECT CT.PriceCode               ,
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
	   {3} as Cost,
	   RIGHT(CT.ID, 9) as CoreID,
	   ifnull(cc.MinOrderSum, core.OrderCost) as OrderCost,
	   ifnull(cc.MinOrderCount, core.MinOrderCount) as MinOrderCount
	   {0}
	   {4}
	   {1}
FROM (Core CT,
		ActivePrices AT,
		farm.core0 Core)
	join Farm.CoreCosts cc on cc.PC_CostCode = at.CostCode and cc.Core_Id = core.Id
		left join catalogs.Products on Products.Id = CT.ProductId
		left join catalogs.catalog on catalog.Id = Products.CatalogId
	   {2}
	   {5}
WHERE  ct.pricecode =at.pricecode
AND    ct.regioncode=at.regioncode
AND    Core.id      =CT.id
AND    IF(?Cumulative, 1, fresh)
group by CT.id, CT.regioncode "
				,
				exportSupplierPriceMarkup ? @"
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
				exportSupplierPriceMarkup && exportBuyingMatrix ? buyingMatrixCondition : "",
				exportSupplierPriceMarkup && exportBuyingMatrix && _updateData.BuyingMatrixPriceId.HasValue ? @" 
  left join farm.BuyingMatrix list on list.ProductId = Products.Id and if(list.ProducerId is null, 1, if(Core.CodeFirmCr is null, " + buyingMatrixProducerNullCondition +", list.ProducerId = Core.CodeFirmCr)) and list.PriceId = " + _updateData.BuyingMatrixPriceId : "",
				cryptCost ? "CT.CryptCost" : "CT.Cost",
				exportSupplierPriceMarkup && _updateData.AllowDelayByPrice() ? ", (Core.VitallyImportant or ifnull(catalog.VitallyImportant,0)) as RetailVitallyImportant " : "",
				_updateData.OfferMatrixPriceId.HasValue ? @" 
  left join farm.BuyingMatrix offerlist on offerList.ProductId = Products.Id and if(offerList.ProducerId is null, 1, if(Core.CodeFirmCr is null, " + offerMatrixProducerNullCondition + ", offerList.ProducerId = Core.CodeFirmCr)) and offerList.PriceId = " + _updateData.OfferMatrixPriceId + @"
  left join UserSettings.OfferMatrixSuppliers oms on oms.SupplierId = at.FirmCode and oms.ClientId = ?ClientCode " : ""
				);
		}

		public string GetSynonymFirmCrCommand(bool Cumulative)
		{ 
			var sql = String.Empty;

			if (_updateData.EnableImpersonalPrice)
			{
				sql = @"
select
	Producers.Id as synonymfirmcrcode,
	LEFT(Producers.Name, 250) as Synonym
from
	catalogs.Producers
where
	(Producers.Id > 1)";
				if (!Cumulative)
					sql += " and Producers.UpdateTime > ?UpdateTime ";
			}
			else
			{
				sql = @"
SELECT synonymfirmcr.synonymfirmcrcode,
	   LEFT(SYNONYM, 250)
FROM   farm.synonymfirmcr,
	   ParentCodes
WHERE  synonymfirmcr.pricecode = ParentCodes.PriceCode";
				if (!Cumulative)
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

			if (_updateData.EnableImpersonalPrice)
			{
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
			else
			{
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
			With.DeadlockWraper(() =>
			{
				var transaction = _readWriteConnection.BeginTransaction(IsolationLevel.RepeatableRead);
				try
				{
					MySqlHelper.ExecuteNonQuery(
						_readWriteConnection, 
						"set @INHost = ?INHost;set @INUser = ?INUser;",
						new MySqlParameter("?INHost", ServiceContext.GetUserHost()),
						new MySqlParameter("?INUser", _updateData.UserName));
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
					for (var i = 0; i < injobs.Length; i++)
					{
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
				catch
				{
					ConnectionHelper.SafeRollback(transaction);
					throw;
				}
			});

		}

		public void UpdatePriceSettings(int[] priceIds, long[] regionIds, bool[] injobs)
		{
			if (priceIds.Length > 0 && priceIds.Length == regionIds.Length && regionIds.Length == injobs.Length)
			{
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
  Customers.ClientToAddressMigrations
where
	UserId = " + _updateData.UserId;
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
	ClientCode = "+ _updateData.ClientId;
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
"
					,
					_updateData.UserId,
					_updateData.IsConfirmUserMessage() ? "" : ", MessageShowCount = if(MessageShowCount > 0, MessageShowCount - 1, 0) ");

			ProcessCommitCommand(commitCommand);
		}

		public DateTime GetCurrentUpdateDate(RequestType updateType)
		{
			return With.DeadlockWraper(() => {
				var transaction = _readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
				try
				{
					var command = new MySqlCommand("", _readWriteConnection);

					if (updateType != RequestType.ResumeData)
						command.CommandText += @"update UserUpdateInfo set UncommitedUpdateDate=now() where UserId = ?userId; ";
					command.CommandText += "select UncommitedUpdateDate from UserUpdateInfo where UserId = ?userId;";
					command.Parameters.AddWithValue("?UserId", _updateData.UserId);
					var updateTime = Convert.ToDateTime(command.ExecuteScalar());

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

		public static void GenerateSessionKey(MySqlConnection readWriteConnection, UpdateData updateData)
		{
			With.DeadlockWraper(() => {
				var transaction = readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
				try
				{
					updateData.CostSessionKey = Convert.ToString(MySqlHelper.ExecuteScalar(
						readWriteConnection,
						@"
update UserUpdateInfo set CostSessionKey = usersettings.GeneratePassword() where UserId = ?userId;
select CostSessionKey from UserUpdateInfo where UserId = ?userId;
",
						new MySqlParameter("?UserId", updateData.UserId)));

					transaction.Commit();
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
				var transaction = _readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
				try
				{
					MySqlHelper.ExecuteNonQuery(_readWriteConnection, commitCommand);
					transaction.Commit();
				}
				catch
				{
					ConnectionHelper.SafeRollback(transaction);
					throw;
				}
			});
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

		public void SetUpdateParameters(MySqlCommand selectComand, bool cumulative, DateTime oldUpdateTime, DateTime currentUpdateTime)
		{
			selectComand.Parameters.AddWithValue("?ClientCode", _updateData.ClientId);
			selectComand.Parameters.AddWithValue("?UserId", _updateData.UserId);
			selectComand.Parameters.AddWithValue("?Cumulative", cumulative);
			selectComand.Parameters.AddWithValue("?UpdateTime", oldUpdateTime);
			selectComand.Parameters.AddWithValue("?LastUpdateTime", currentUpdateTime);
			selectComand.Parameters.AddWithValue("?OffersClientCode", _updateData.OffersClientCode);
			selectComand.Parameters.AddWithValue("?OffersRegionCode", _updateData.OffersRegionCode);
			selectComand.Parameters.AddWithValue("?ImpersonalPriceId", _updateData.ImpersonalPriceId);
			selectComand.Parameters.AddWithValue("?ImpersonalPriceDate", DateTime.Now);
			selectComand.Parameters.AddWithValue("?ImpersonalPriceFresh", 0);
		}

		public void PrepareImpersonalOffres(MySqlCommand selectCommand)
		{
			selectCommand.CommandText = @"
DROP TEMPORARY TABLE IF EXISTS Prices, ActivePrices;
CALL Customers.GetActivePrices(?OffersClientCode);
CALL Customers.GetOffers(?OffersClientCode);";
			selectCommand.ExecuteNonQuery();

			selectCommand.CommandText = @"
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
						
SET @RowId :=1;";
			selectCommand.ExecuteNonQuery();
		}

		public string GetRejectsCommand(bool Cumulative)
		{
			var sql = @"
SELECT 
	   rejects.RowId         ,
	   rejects.FullName      ,
	   rejects.FirmCr        ,
	   rejects.CountryCr     ,
	   rejects.Series        ,
	   rejects.LetterNo      ,
	   rejects.LetterDate    ,
	   rejects.LaboratoryName,
	   rejects.CauseRejects
FROM   addition.rejects,
	   retclientsset rcs
WHERE  rcs.clientcode = ?ClientCode
AND    alowrejection  = 1 ";

			if (!Cumulative)
				sql += "   AND accessTime > ?UpdateTime";

			return sql;
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
				return @"
SELECT 
	   PricesData.FirmCode            ,
	   ?OffersRegionCode as RegionCode,
	   '4732-606000' as supportphone  ,
	   null as ContactInfo            ,
	   null as OperativeInfo          
FROM   
   UserSettings.PricesData
where
  PricesData.PriceCode = ?ImpersonalPriceId
limit 1";
			else
				return @"
SELECT DISTINCT 
				regionaldata.FirmCode  ,
				regionaldata.RegionCode,
				supportphone           ,
				ContactInfo            ,
				OperativeInfo
FROM            
				regionaldata,
				Prices
WHERE           regionaldata.firmcode  = Prices.firmcode
AND             regionaldata.regioncode= Prices.regioncode";
		}

		public void PrepareProviderContacts(MySqlCommand selectCommand)
		{
			if (!_updateData.EnableImpersonalPrice)
			{
				selectCommand.CommandText = @"
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
AND             c.Type  = 0;
";
				selectCommand.ExecuteNonQuery();
			}
		}

		public void ClearProviderContacts(MySqlCommand selectCommand)
		{
			selectCommand.CommandText = "drop TEMPORARY TABLE IF EXISTS ProviderContacts";
			selectCommand.ExecuteNonQuery();
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
		 firm.Name
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
		if(ss.CertificateSourceId is not null, 1, 0) as CertificateSourceExists
FROM     Customers.Suppliers AS firm
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
				return @"
SELECT   
		 Prices.FirmCode ,
		 Prices.pricecode,
		 concat(firm.name, IF(PriceCounts.PriceCount> 1 OR Prices.ShowPriceName = 1, concat(' (', Prices.pricename, ')'), ''))    as PriceName,
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
		 Prices.pricecode";
		}

		public void PreparePricesData(MySqlCommand selectCommand)
		{
			if (_updateData.EnableImpersonalPrice)
			{
				selectCommand.CommandText = "select max(PriceDate) from Prices";
				var priceDate = Convert.ToDateTime(selectCommand.ExecuteScalar());
				selectCommand.Parameters["?ImpersonalPriceDate"].Value = priceDate;

				selectCommand.CommandText = @"
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
				var priceFresh = Convert.ToInt32(selectCommand.ExecuteScalar());
				selectCommand.Parameters["?ImpersonalPriceFresh"].Value = priceFresh > 0 ? 1 : 0;
			}
			else
			{
				selectCommand.CommandText = @"
CREATE TEMPORARY TABLE PriceCounts ( FirmCode INT unsigned, PriceCount MediumINT unsigned )engine=MEMORY;
		INSERT
		INTO   PriceCounts
		SELECT   firmcode,
				 COUNT(pricecode)
		FROM     Prices
		GROUP BY FirmCode,
				 RegionCode;";
				selectCommand.ExecuteNonQuery();
			}
		}

		public void ResetReclameDate()
		{
			var transaction = _readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
			try
			{
				var resetCommand = new MySqlCommand("update usersettings.UserUpdateInfo set ReclameDate = NULL where UserId= ?UserId;", _readWriteConnection, transaction);
				resetCommand.Parameters.AddWithValue("?UserId", _updateData.UserId);
				resetCommand.ExecuteNonQuery();

				transaction.Commit();
			}
			catch
			{
				ConnectionHelper.SafeRollback(transaction);
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
			try
			{
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
			catch
			{
				ConnectionHelper.SafeRollback(transaction);
				throw;
			}
		}

		public static void UpdateBuildNumber(MySqlConnection readWriteConnection, UpdateData updateData)
		{
			if (!updateData.KnownBuildNumber.HasValue || updateData.KnownBuildNumber < updateData.BuildNumber)
				With.DeadlockWraper(() =>
				{
					var command = new MySqlCommand("update usersettings.UserUpdateInfo set AFAppVersion = ?BuildNumber where UserId = ?UserId", readWriteConnection);
					command.Parameters.AddWithValue("?BuildNumber", updateData.BuildNumber);
					command.Parameters.AddWithValue("?UserId", updateData.UserId);
					var transaction = readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
					try
					{
						command.Transaction = transaction;
						command.ExecuteNonQuery();

						transaction.Commit();
					}
					catch
					{
						ConnectionHelper.SafeRollback(transaction);
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
			With.DeadlockWraper(() =>
			{
				updateData.UniqueID = uniqueId;

				var command = new MySqlCommand("update UserUpdateInfo set AFCopyId= ?UniqueId where UserId = ?UserId", readWriteConnection);
				command.Parameters.AddWithValue("?UniqueId", updateData.UniqueID);
				command.Parameters.AddWithValue("?UserId", updateData.UserId);

				var transaction = readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
				try
				{
					command.Transaction = transaction;
					if (String.IsNullOrEmpty(updateData.KnownUniqueID))
						command.ExecuteNonQuery();
					else
						if (updateData.KnownUniqueID != uniqueId)
						{
							string description;
							switch (request)
							{
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
				catch
				{
					ConnectionHelper.SafeRollback(transaction);
					throw;
				}
			});
		}

		public void ConfirmUserMessage(string confirmedMessage)
		{
			With.DeadlockWraper(() =>
			{
				var transaction = _readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
				try
				{
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
				catch
				{
					ConnectionHelper.SafeRollback(transaction);
					throw;
				}
			});

		}

		public void UnconfirmedOrdersExport(string exportFolder, Queue<FileForArchive> filesForArchive)
		{
			if (_updateData.NeedDownloadUnconfirmedOrders)
			{
				var exporter = new UnconfirmedOrdersExporter(_updateData, this, exportFolder, filesForArchive);
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

				var allowedTypes = new int[] {Convert.ToInt32(RequestType.GetCumulativeAsync), Convert.ToInt32(RequestType.GetLimitedCumulativeAsync), Convert.ToInt32(RequestType.GetDataAsync)};

				if (realUpdateType != null && Convert.ToInt32(realUpdateType) > 0 && allowedTypes.Any(i => i == Convert.ToInt32(realUpdateType)))
				{
					var transaction = readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
					try
					{
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
					catch
					{
						ConnectionHelper.SafeRollback(transaction);
						throw;
					}
				}
				else 
					throw new Exception("Неожидаемый тип {0} у запроса c updateId {1}".Format(realUpdateType, updateId));
			});
		}

		public void ArchiveAttachments(MySqlConnection connection, string archiveFileName, ref string addition, Queue<FileForArchive> filesForArchive)
		{
			var log = LogManager.GetLogger(typeof(UpdateHelper));

			try
			{
				log.Debug("Будем выгружать вложения");

				var command = new MySqlCommand();
				command.Connection = connection;
				command.Parameters.AddWithValue("?UserId", _updateData.UserId);
				command.Parameters.Add("?AttachmentId", MySqlDbType.UInt32);

				var attachmentRequestsFile = DeleteFileByPrefix("AttachmentRequests");

				ArchiveAttachmentFiles(archiveFileName, command);

				if (_updateData.SuccesAttachmentsExists()) {
					File.WriteAllText(ServiceContext.MySqlLocalImportPath() + attachmentRequestsFile, _updateData.GetAttachmentsResult());
					ProcessArchiveFile(attachmentRequestsFile, archiveFileName);
				}
			}
			catch (Exception exception)
			{
				log.Error("Ошибка при архивировании почтовых вложений", exception);
				addition += "Архивирование почтовых вложений: " + exception.Message + "; ";

				ShareFileHelper.MySQLFileDelete(archiveFileName);
			}
		}

		private void ArchiveAttachmentFiles(string archiveFileName, MySqlCommand command)
		{
			var attachmentsFolder = "Attachments";
			var attachmentsPath = Path.Combine(_updateData.ResultPath, attachmentsFolder);

			var tempPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
			if (!Directory.Exists(tempPath))
				Directory.CreateDirectory(tempPath);
			if (!Directory.Exists(Path.Combine(tempPath, "Docs")))
				Directory.CreateDirectory(Path.Combine(tempPath, "Docs"));

			command.CommandText = @"
select 
	Attachments.Extension 
from 
	logs.AttachmentSendLogs 
	inner join documents.Attachments on Attachments.Id = AttachmentSendLogs.AttachmentId
where 
	AttachmentSendLogs.UserId = ?UserId
and AttachmentSendLogs.AttachmentId = ?AttachmentId";


			try {
				foreach (var request in _updateData.AttachmentRequests) {
					command.Parameters["?AttachmentId"].Value = request.AttachmentId;
					var extension = command.ExecuteScalar();
					if (extension != null && !String.IsNullOrEmpty((string)extension)) {
						File.Copy(
							Path.Combine(attachmentsPath, request.AttachmentId + (string)extension), 
							Path.Combine(tempPath, "Docs", request.AttachmentId + (string)extension));
						request.Success = true;
					}
				}

				if (_updateData.SuccesAttachmentsExists())
					SevenZipHelper.ArchiveFilesWithNames(
						archiveFileName,
						Path.Combine("Docs", "*.*"),
						tempPath);
			}
			finally {
				if (Directory.Exists(tempPath))
					try {
						Directory.Delete(tempPath, true);
					}
					catch (Exception exception) {
						var log = LogManager.GetLogger(typeof(UpdateHelper));
						log.WarnFormat("Ошибка при удалении временной папки {0}: {1}", tempPath, exception);
					}
			}
		}

		public void FillExportMails(MySqlCommand selectCommand)
		{
			_updateData.ExportMails.Clear();
				//начинаем отдавать документы с самых новых что бы 
				//отдать наиболее актуальные
			var sql = @"
select 
	Mails.Id
from 
	documents.Mails 
	inner join Logs.MailSendLogs ms on ms.MailId = Mails.Id
where 
	Mails.LogTime > curdate() - interval 30 day
and ms.UserId = ?UserId 
and ms.Committed = 0
order by Mails.LogTime desc
limit 200;
";
			selectCommand.CommandText = sql;
			using (var reader = selectCommand.ExecuteReader())
			{
				while (reader.Read())
					_updateData.ExportMails.Add(reader.GetUInt32(0));
			}
		}

		public string GetMailsCommand()
		{
			return @"
select
	Mails.Id,
	Mails.LogTime,
	Mails.SupplierId,
	Suppliers.Name as SupplierName,
	Mails.IsVIPMail,
	Mails.Subject,
	Mails.Body
from
	Documents.Mails
	inner join Customers.Suppliers on Suppliers.Id = Mails.SupplierId
where
  Mails.Id in (" + _updateData.ExportMails.Implode() + ")";
		}

		public string GetAttachmentsCommand()
		{
			return @"
select
	Attachments.Id,
	Attachments.MailId,
	Attachments.FileName,
	Attachments.Extension,
	Attachments.Size
from
	Documents.Mails
	inner join Documents.Attachments on Attachments.MailId = Mails.Id
where
  Mails.Id in (" + _updateData.ExportMails.Implode() + ")";
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
group by afu.UserId"
					,
					new MySqlParameter("?UserId", _updateData.UserId),
					new MySqlParameter("?UpdateType", (int)RequestType.SendWaybills));
				waitCount = realWaitCount != null ? (int?) Convert.ToInt32(realWaitCount) : null;
			} while (waitCount > 0 && DateTime.Now.Subtract(startTime).TotalSeconds < 60);
		}
	}
}