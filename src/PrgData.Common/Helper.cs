using System;
using System.Data;
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

		public UpdateData(DataRow row)
		{
			ClientId = Convert.ToUInt32(row["ClientId"]);
			UserId = Convert.ToUInt32(row["UserId"]);
			Message = Convert.ToString(row["Message"]);
			CheckCopyId = Convert.ToBoolean(row["CheckCopyId"]);
			if (!(row["UpdateDate"] is DBNull))
				OldUpdateTime = Convert.ToDateTime(row["UpdateDate"]);
			if (!(row["UncommitedUpdateDate"] is DBNull))
				UncommitedUpdateTime = Convert.ToDateTime(row["UncommitedUpdateDate"]);
		}
	}

	public class Helper
	{
		private UpdateData _updateData;
		private MySqlConnection _readWriteConnection;
		private MySqlConnection _readOnlyConnection;

		public Helper(UpdateData updateData, MySqlConnection readOnlyConnection, MySqlConnection readWriteConnection)
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
              FirmCode
       )
SELECT ouar.RowId,
       supplier.FirmCode
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
              FirmCode
       )
SELECT u.Id,
       supplier.FirmCode
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
			if (userName.ToLower().StartsWith(@"analit\"))
				userName = userName.Replace(@"analit\", "");

			var dataAdapter = new MySqlDataAdapter(@"
SELECT  c.Id ClientId,
        u.Id UserId,
        rui.UpdateDate,
        rui.UncommitedUpdateDate,
        IF(rui.MessageShowCount < 1, '', rui.MESSAGE) Message,
        CheckCopyID
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

			if (data.Tables[0].Rows.Count == 0)
			{
				dataAdapter.SelectCommand.CommandText = @"
SELECT  ouar.clientcode as ClientId, 
        ouar.RowId UserId,
        rui.UpdateDate, 
        rui.UncommitedUpdateDate,
        IF(rui.MessageShowCount<1, '', rui.MESSAGE) Message, 
        CheckCopyID 
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
				dataAdapter.Fill(data);
			}

			if (data.Tables[0].Rows.Count == 0)
				return null;

			var updateData = new UpdateData(data.Tables[0].Rows[0]);

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
				var command = new MySqlCommand("call future.GetActivePrices(?UserId);", _readOnlyConnection);
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
				var command = new MySqlCommand("call future.GetActivePrices(?UserId);", _readWriteConnection);
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
				var command = new MySqlCommand("CALL future.GetPrices(?ClientCode)", _readOnlyConnection);
				command.Parameters.AddWithValue("?clientCode", _updateData.ClientId);
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

		public string GetClientsCommand(bool isFirebird)
		{
			if (_updateData.IsFutureClient)
			{
				return String.Format(@"
SELECT a.LegacyId as FirmCode,
     c.Name as ShortName,
     ifnull(?OffersRegionCode, c.RegionCode) as RegionCode,
     rsc.OverCostPercent,
     rsc.DifferenceCalculation,
     rsc.MultiUserLevel,
     rsc.OrderRegionMask,
     {0}
     rsc.CalculateLeader
FROM future.Clients c
  join usersettings.RetClientsSet rcs on c.Id = rsc.ClientCode
  join future.Address a on c.id = a.ClientId
WHERE c.Id = ?ClientCode", isFirebird ? "'', " : "");
			}
			else
			{
				return String.Format(@"
SELECT clientsdata.firmcode,
     ShortName                           , 
     ifnull(?OffersRegionCode, RegionCode), 
     OverCostPercent                     , 
     DifferenceCalculation               , 
     MultiUserLevel                      , 
     OrderRegionMask                     , 
     {0}
     CalculateLeader 
FROM   retclientsset, 
     clientsdata 
WHERE  clientsdata.firmcode    = ?ClientCode 
 AND retclientsset.clientcode= clientsdata.firmcode 

UNION 

SELECT clientsdata.firmcode                                                    , 
     ShortName                                                               , 
     ifnull(?OffersRegionCode, RegionCode)                                    , 
     retclientsset.OverCostPercent                                           , 
     retclientsset.DifferenceCalculation                                     , 
     retclientsset.MultiUserLevel                                            , 
     IF(IncludeType=3, parent.OrderRegionMask, retclientsset.OrderRegionMask), 
     {0}
     retclientsset.CalculateLeader 
FROM   retclientsset       , 
     clientsdata         , 
     retclientsset parent, 
     IncludeRegulation 
WHERE  clientsdata.firmcode    = IncludeClientCode 
 AND retclientsset.clientcode= clientsdata.firmcode 
 AND parent.clientcode       = Primaryclientcode 
 AND firmstatus              = 1 
 AND IncludeType            IN (0,3) 
 AND Primaryclientcode       = ?ClientCode", isFirebird ? "'', " : "");
			}
		}
	}
}