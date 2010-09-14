using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using Castle.ActiveRecord;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Tests.Repositories;
using Inforoom.Common;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData;
using System.Data;
using PrgData.Common;
using SmartOrderFactory.Domain;
using SmartOrderFactory.Repositories;
using Test.Support;


namespace Integration
{
	[TestFixture]
	public class PostSomeOrdersFixture
	{
		private TestOldClient oldClient;
		private TestOldUser oldUser;


		private bool getOffers;
		private DataRow activePrice;
		private DataRow firstOffer;
		private DataRow secondOffer;
		private DataRow thirdOffer;
		private DataRow fourOffer;

		[SetUp]
		public void Setup()
		{

			Test.Support.Setup.Initialize();
			ContainerInitializer.InitializerContainerForTests(typeof(SmartOrderRule).Assembly);
			IoC.Container.Register(
				Component.For<ISmartOfferRepository>().ImplementedBy<SmartOfferRepository>()
				);

			ServiceContext.GetUserHost = () => "127.0.0.1";
			ConfigurationManager.AppSettings["WaybillPath"] = "FtpRoot\\";
			if (Directory.Exists("FtpRoot"))
				FileHelper.DeleteDir("FtpRoot");
			Directory.CreateDirectory("FtpRoot");

			using (var transaction = new TransactionScope())
			{
				var permission = TestUserPermission.ByShortcut("AF");

				oldClient = TestOldClient.CreateTestClient();
				oldUser = oldClient.Users[0];

				ServiceContext.GetUserName = () => oldUser.OSUserName;

				var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof(ActiveRecordBase));
				try
				{
					session.CreateSQLQuery(@"
				insert into usersettings.AssignedPermissions (PermissionId, UserId) values (:permissionid, :userid)")
						.SetParameter("permissionid", permission.Id)
						.SetParameter("userid", oldUser.Id)
						.ExecuteUpdate();
				}
				finally
				{
					ActiveRecordMediator.GetSessionFactoryHolder().ReleaseSession(session);
				}
			}

			CreateFolders(oldClient.Id.ToString());

			MySqlHelper.ExecuteNonQuery(Settings.ConnectionString(), @"
delete 
from orders.OrdersHead 
where 
    ClientCode = ?ClientCode 
and WriteTime > now() - interval 2 week"
				,
				new MySqlParameter("?ClientCode", oldClient.Id));

			GetOffers();
		}

		private void GetOffers()
		{
			if (!getOffers)
				using (var connection = new MySqlConnection(Settings.ConnectionString()))
				{
					connection.Open();

					MySqlHelper.ExecuteNonQuery(
						connection,
						@"
drop temporary table if exists Usersettings.Prices, Usersettings.ActivePrices, Usersettings.Core;
call usersettings.GetOffers(?ClientCode, 0)",
						new MySqlParameter("?ClientCode", oldClient.Id));

					activePrice = ExecuteDataRow(
						connection, @"
select 
* 
from 
  ActivePrices 
where 
  (select Count(*) from Core where Core.PriceCode = ActivePrices.PriceCode and Core.RegionCode = ActivePrices.RegionCode) > 3000 limit 1");

					var firstProductId = MySqlHelper.ExecuteScalar(
						connection,
						@"
select 
  c.ProductId 
from 
  Core 
  inner join farm.Core0 c on c.Id = Core.Id
where
    Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
group by c.ProductId
having count(distinct c.SynonymCode) > 2
limit 1
"
						,
						new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
						new MySqlParameter("?RegionCode", activePrice["RegionCode"]));

					var secondProductId = MySqlHelper.ExecuteScalar(
						connection,
						@"
select 
  c.ProductId 
from 
  Core 
  inner join farm.Core0 c on c.Id = Core.Id
where
    Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
and C.ProductId <> ?FirstProductId
limit 1
"
						,
						new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
						new MySqlParameter("?RegionCode", activePrice["RegionCode"]),
						new MySqlParameter("?FirstProductId", firstProductId));

					var thirdProductId = MySqlHelper.ExecuteScalar(
						connection,
						@"
select 
  c.ProductId 
from 
  Core 
  inner join farm.Core0 c on c.Id = Core.Id
where
    Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
and C.ProductId <> ?FirstProductId
and C.ProductId <> ?SecondProductId
limit 1
"
						,
						new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
						new MySqlParameter("?RegionCode", activePrice["RegionCode"]),
						new MySqlParameter("?FirstProductId", firstProductId),
						new MySqlParameter("?SecondProductId", secondProductId));

					firstOffer = ExecuteDataRow(
						connection,
						@"
select
  Core.Cost,
  Core.RegionCode,
  c.*
from
  Core 
  inner join farm.Core0 c on c.Id = Core.Id
where
    Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
and C.ProductId = ?ProductId
limit 1
"
						,
						new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
						new MySqlParameter("?RegionCode", activePrice["RegionCode"]),
						new MySqlParameter("?ProductId", firstProductId));
					Assert.IsNotNull(firstOffer, "Не найдено предложение");

					secondOffer = ExecuteDataRow(
						connection,
						@"
select
  Core.Cost,
  Core.RegionCode,
  c.*
from
  Core 
  inner join farm.Core0 c on c.Id = Core.Id
where
    Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
and C.ProductId = ?ProductId
and C.SynonymCode <> ?SynonymCode
limit 1
"
						,
						new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
						new MySqlParameter("?RegionCode", activePrice["RegionCode"]),
						new MySqlParameter("?ProductId", firstProductId),
						new MySqlParameter("?SynonymCode", firstOffer["SynonymCode"]));
					Assert.IsNotNull(secondOffer, "Не найдено предложение");

					thirdOffer = ExecuteDataRow(
						connection,
						@"
select
  Core.Cost,
  Core.RegionCode,
  c.*
from
  Core 
  inner join farm.Core0 c on c.Id = Core.Id
where
    Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
and C.ProductId = ?ProductId
limit 1
"
						,
						new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
						new MySqlParameter("?RegionCode", activePrice["RegionCode"]),
						new MySqlParameter("?ProductId", secondProductId));
					Assert.IsNotNull(thirdOffer, "Не найдено предложение");

					fourOffer = ExecuteDataRow(
						connection,
						@"
select
  Core.Cost,
  Core.RegionCode,
  c.*
from
  Core 
  inner join farm.Core0 c on c.Id = Core.Id
where
    Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
and C.ProductId = ?ProductId
limit 1
"
						,
						new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
						new MySqlParameter("?RegionCode", activePrice["RegionCode"]),
						new MySqlParameter("?ProductId", thirdProductId));
					Assert.IsNotNull(fourOffer, "Не найдено предложение");
					getOffers = true;
				}
		}

		private static DataRow ExecuteDataRow(MySqlConnection connection, string commandText, params MySqlParameter[] parms)
		{
			DataSet set = MySqlHelper.ExecuteDataset(connection, commandText, parms);
			if (set == null)
			{
				return null;
			}
			if (set.Tables.Count == 0)
			{
				return null;
			}
			if (set.Tables[0].Rows.Count == 0)
			{
				return null;
			}
			return set.Tables[0].Rows[0];
		}

		[Test]
		public void Send_orders_without_SupplierPriceMarkup()		
		{
			using(var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var UniqueId = MySqlHelper.ExecuteScalar(connection, "select AFCopyId from usersettings.UserUpdateInfo where UserId = " + oldUser.Id).ToString();

				string serverResponse;
				using (var prgData = new PrgDataEx())
				{
					serverResponse = prgData.PostSomeOrders(
						UniqueId,
						true,         //ForceSend
						false,        //UseCorrectOrders
						oldClient.Id, //ClientId
						1,            //OrderCount
						new ulong[] { 1L },  //ClientOrderId
						new ulong[] { Convert.ToUInt64(activePrice["PriceCode"]) },
						new ulong[] { Convert.ToUInt64(activePrice["RegionCode"]) },
						new DateTime[] { DateTime.Now }, //PriceDate
						new string[] { "" },             //ClientAddition
						new ushort[] { 1 },              //RowCount
						new ulong[] { 1 },               //ClientPositionId
						new ulong[] { Convert.ToUInt64(firstOffer["Id"].ToString().Substring(firstOffer["Id"].ToString().Length - 9, 9)) },  //ClientServerCoreId
						new ulong[] { Convert.ToUInt64(firstOffer["ProductId"]) },
						new string[] { firstOffer["CodeFirmCr"].ToString() },
						new ulong[] { Convert.ToUInt64(firstOffer["SynonymCode"]) },
						new string[] { firstOffer["SynonymFirmCrCode"].ToString() },
						new string[] { firstOffer["Code"].ToString() },
						new string[] { firstOffer["Code"].ToString() },
						new bool[] { Convert.ToBoolean(firstOffer["Junk"]) },
						new bool[] { Convert.ToBoolean(firstOffer["Await"]) },
						new string[] { firstOffer["RequestRatio"].ToString() },
						new string[] { firstOffer["OrderCost"].ToString() },
						new string[] { firstOffer["MinOrderCount"].ToString() },
						new ushort[] { 1 }, //Quantity
						new decimal[] { Convert.ToDecimal(firstOffer["Cost"]) },
						new string[] { firstOffer["Cost"].ToString() },        //minCost
						new string[] { activePrice["PriceCode"].ToString() },  //MinPriceCode
						new string[] { firstOffer["Cost"].ToString() },        //leaderMinCost
						new string[] { activePrice["PriceCode"].ToString() }); //leaderMinPriceCode
				}

				Assert.That(serverResponse, Is.Not.Null);
				Assert.That(serverResponse, Is.Not.Empty);
				var serverParams = serverResponse.Split(';');
				Assert.That(serverParams[0], Is.StringStarting("ClientOrderId=").IgnoreCase);
				Assert.That(serverParams[1], Is.StringStarting("PostResult=").IgnoreCase);
				Assert.That(serverParams[2], Is.StringStarting("ServerOrderId=").IgnoreCase);
				Assert.That(serverParams[3], Is.StringStarting("ErrorReason=").IgnoreCase);
				Assert.That(serverParams[4], Is.StringStarting("ServerMinReq=").IgnoreCase);

				var postResult = serverParams[1].Substring(serverParams[1].IndexOf('=') + 1);
				Assert.That(postResult, Is.EqualTo("0"));

				var serverOrderId = serverParams[2].Substring(serverParams[2].IndexOf('=') + 1);
				Assert.That(serverOrderId, Is.Not.Null);
				Assert.That(serverOrderId, Is.Not.Empty);

				var realOrderId = MySqlHelper.ExecuteScalar(connection, "select RowId from orders.ordershead where RowId = " + serverOrderId);
				Assert.That(realOrderId, Is.Not.Null);
				Assert.That(realOrderId.ToString(), Is.Not.Empty);

				var supplierPriceMarkup = MySqlHelper.ExecuteScalar(connection, "select SupplierPriceMarkup from orders.ordersList where OrderId = " + serverOrderId);
				Assert.That(supplierPriceMarkup, Is.EqualTo(DBNull.Value));

				var orderedOffersId = MySqlHelper.ExecuteScalar(connection, @"
select 
OrderedOffers.Id 
from 
orders.ordersList 
left join orders.OrderedOffers on OrderedOffers.Id = ordersList.RowId
where ordersList.OrderId = " + serverOrderId);
				Assert.That(orderedOffersId, Is.Not.Null);
				Assert.That(orderedOffersId, Is.Not.EqualTo(DBNull.Value));

			}
		}

		private void CreateFolders(string folderName)
		{
			var fullName = Path.Combine("FtpRoot", folderName, "Waybills");
			Directory.CreateDirectory(fullName);
			fullName = Path.Combine("FtpRoot", folderName, "Rejects");
			Directory.CreateDirectory(fullName);
			fullName = Path.Combine("FtpRoot", folderName, "Docs");
			Directory.CreateDirectory(fullName);
		}

	}
}
