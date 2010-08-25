using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using MySql.Data.MySqlClient;
using PrgData.Common;
using System.Data;
using PrgData.Common.Orders;
using System.Configuration;
using System.IO;
using Inforoom.Common;
using Common.Models.Tests.Repositories;
using Common.Models;
using Castle.MicroKernel.Registration;
using SmartOrderFactory.Repositories;
using SmartOrderFactory.Domain;

namespace Integration
{
	[TestFixture]
	public class ReorderHelperFixture
	{
		//Пользователь "sergei" - это клиент с кодом 1349, он должен быть старым
		private uint oldClientId = 1349;
		private string oldUserName = "sergei";
		//Пользователь "10081" - это клиент с кодом 10005, он должен быть новым
		private uint futureClientId = 10005;
		private uint futureAddressId = 10068;
		private string futureUserName = "10081";

		private DataRow activePrice;
		private DataRow firstOffer;
		private DataRow secondOffer;
		private DataRow thirdOffer;
		private DataRow fourOffer;

		[SetUp]
		public void Setup()
		{
			Test.Support.Setup.Initialize();
			ServiceContext.GetUserHost = () => "127.0.0.1";
			ContainerInitializer.InitializerContainerForTests(typeof(SmartOrderRule).Assembly);
			IoC.Container.Register(
				Component.For<ISmartOfferRepository>().ImplementedBy<SmartOfferRepository>()
				);

			ConfigurationManager.AppSettings["WaybillPath"] = "FtpRoot\\";
			if (Directory.Exists("FtpRoot"))
				FileHelper.DeleteDir("FtpRoot");

			Directory.CreateDirectory("FtpRoot");
			CreateFolders(oldClientId.ToString());
			CreateFolders(futureAddressId.ToString());

			MySqlHelper.ExecuteNonQuery(Settings.ConnectionString(), @"
delete 
from orders.OrdersHead 
where 
    ClientCode = ?ClientCode 
and WriteTime > now() - interval 2 week"
				,
				new MySqlParameter("?ClientCode", oldClientId));
			MySqlHelper.ExecuteNonQuery(Settings.ConnectionString(), @"
delete 
from orders.OrdersHead 
where 
    ClientCode = ?ClientCode 
and WriteTime > now() - interval 2 week"
				,
				new MySqlParameter("?ClientCode", futureClientId));

			GetOffers();
		}

		private void GetOffers()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				MySqlHelper.ExecuteNonQuery(
					connection,
					"call usersettings.GetOffers(?ClientCode, 0)",
					new MySqlParameter("?ClientCode", oldClientId));

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

		public void CreateFolders(string folderName)
		{
			var fullName = Path.Combine("FtpRoot", folderName, "Waybills");
			Directory.CreateDirectory(fullName);
			fullName = Path.Combine("FtpRoot", folderName, "Rejects");
			Directory.CreateDirectory(fullName);
			fullName = Path.Combine("FtpRoot", folderName, "Docs");
			Directory.CreateDirectory(fullName);
		}

		public void ParseSimpleOrder(ReorderHelper orderHelper)
		{
			orderHelper.ParseOrders(
					1,
					new ulong[] { 1L },
					new ulong[] { Convert.ToUInt64(activePrice["PriceCode"]) },
					new ulong[] { Convert.ToUInt64(activePrice["RegionCode"]) },
					new DateTime[] { DateTime.Now }, //pricedate
					new string[] { "" },             //clientaddition
					new ushort[] { 1 },              //rowCount
					new ulong[] { 1L },              //clientPositionId
					new ulong[] { Convert.ToUInt64(firstOffer["Id"].ToString().Substring(firstOffer["Id"].ToString().Length - 9, 9)) }, //ClientServerCoreID
					new ulong[] { Convert.ToUInt64(firstOffer["ProductId"]) },     //ProductId
					new string[] { firstOffer["CodeFirmCr"].ToString() },
					new ulong[] { Convert.ToUInt64(firstOffer["SynonymCode"]) }, //SynonymCode
					new string[] { firstOffer["SynonymFirmCrCode"].ToString() },
					new string[] { firstOffer["Code"].ToString() },
					new string[] { firstOffer["CodeCr"].ToString() },
					new bool[] { Convert.ToBoolean(firstOffer["Junk"]) },
					new bool[] { Convert.ToBoolean(firstOffer["Await"]) },
					new string[] { firstOffer["RequestRatio"].ToString() },
					new string[] { firstOffer["OrderCost"].ToString() },
					new string[] { firstOffer["MinOrderCount"].ToString() },
					new ushort[] { 1 }, //Quantity
					new decimal[] { Convert.ToDecimal(firstOffer["Cost"]) },
					new string[] { "" },  //minCost
					new string[] { "" },  //MinPriceCode
					new string[] { "" },  //leaderMinCost
					new string[] { "" },  //leaderMinPriceCode
					new string[] { "" },  //supplierPriceMarkup
					new string[] { "" }, //delayOfPayment,
					new string[] { "" }, //coreQuantity,
					new string[] { "" }, //unit,
					new string[] { "" }, //volume,
					new string[] { "" }, //note,
					new string[] { "" }, //period,
					new string[] { "" }, //doc,
					new string[] { "" }, //registryCost,
					new bool[] { false }, //vitallyImportant,
					new string[] { "" }, //retailMarkup,
					new string[] { "" }, //producerCost,
					new string[] { "" } //nds
					);
		}

		public void ParseFirstOrder(ReorderHelper orderHelper)
		{
			orderHelper.ParseOrders(
					1,
					new ulong[] { 1L },
					new ulong[] { 1L },
					new ulong[] { 1L },
					new DateTime[] { DateTime.Now },
					new string[] { "" },
					new ushort[] { 2 },
					new ulong[] { 1L, 2L }, 
					new ulong[] { 1L, 2L },
					new ulong[] { Convert.ToUInt64(secondOffer["ProductId"]), Convert.ToUInt64(thirdOffer["ProductId"]) },  //ProductId
					new string[] { "", "" },
					new ulong[] { Convert.ToUInt64(secondOffer["SynonymCode"]), Convert.ToUInt64(thirdOffer["SynonymCode"]) }, //SynonymCode
					new string[] { "", "" },
					new string[] { "", "" },
					new string[] { "", "" },
					new bool[] { false, false },
					new bool[] { false, false },
					new string[] { "", "" },
					new string[] { "", "" },
					new string[] { "", "" },
					new ushort[] { 1, 2 },
					new decimal[] { 100m, 200m },
					new string[] { "", "" },
					new string[] { "", "" },
					new string[] { "", "" },
					new string[] { "", "" },
					new string[] { "", "" },
					new string[] { "" }, //delayOfPayment,
					new string[] { "", "" }, //coreQuantity,
					new string[] { "", "" }, //unit,
					new string[] { "", "" }, //volume,
					new string[] { "", "" }, //note,
					new string[] { "", "" }, //period,
					new string[] { "", "" }, //doc,
					new string[] { "", "" }, //registryCost,
					new bool[] { false, false }, //vitallyImportant,
					new string[] { "", "" }, //retailMarkup,
					new string[] { "", "" }, //producerCost,
					new string[] { "", "" } //nds
					);
		}

		public void ParseSecondOrder(ReorderHelper orderHelper)
		{
			orderHelper.ParseOrders(
					1,
					new ulong[] { 1L },
					new ulong[] { 1L },
					new ulong[] { 1L },
					new DateTime[] { DateTime.Now },
					new string[] { "" },
					new ushort[] { 2 },
					new ulong[] { 1L, 2L },
					new ulong[] { 1L, 2L },
					new ulong[] { Convert.ToUInt64(secondOffer["ProductId"]), Convert.ToUInt64(fourOffer["ProductId"]) },
					new string[] { "", "" },
					new ulong[] { Convert.ToUInt64(secondOffer["SynonymCode"]), Convert.ToUInt64(fourOffer["SynonymCode"]) },
					new string[] { "", "" },
					new string[] { "", "" },
					new string[] { "", "" },
					new bool[] { false, false },
					new bool[] { false, false },
					new string[] { "", "" },
					new string[] { "", "" },
					new string[] { "", "" },
					new ushort[] { 1, 2 },
					new decimal[] { 100m, 200m },
					new string[] { "", "" },
					new string[] { "", "" },
					new string[] { "", "" },
					new string[] { "", "" },
					new string[] { "", "" },
					new string[] { "" }, //delayOfPayment,
					new string[] { "", "" }, //coreQuantity,
					new string[] { "", "" }, //unit,
					new string[] { "", "" }, //volume,
					new string[] { "", "" }, //note,
					new string[] { "", "" }, //period,
					new string[] { "", "" }, //doc,
					new string[] { "", "" }, //registryCost,
					new bool[] { false, false }, //vitallyImportant,
					new string[] { "", "" }, //retailMarkup,
					new string[] { "", "" }, //producerCost,
					new string[] { "", "" } //nds
					);
		}

		public int GetOrderCount(MySqlConnection connection, string orderId)
		{
			return Convert.ToInt32(MySqlHelper
				.ExecuteScalar(
					connection,
					"select count(*) from orders.orderslist where OrderId = ?OrderId",
					new MySqlParameter("?OrderId", orderId)));

		}

		public void Check_simple_double_order(string userName, uint orderedClientId)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, userName);
				var orderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false, 1183);

				ParseSimpleOrder(orderHelper);

				var result = orderHelper.PostSomeOrders();

				var serverParams = result.Split(';');
				Assert.That(serverParams[0], Is.StringStarting("ClientOrderId=").IgnoreCase);
				Assert.That(serverParams[1], Is.StringStarting("PostResult=").IgnoreCase);
				Assert.That(serverParams[2], Is.StringStarting("ServerOrderId=").IgnoreCase);
				Assert.That(serverParams[3], Is.StringStarting("ErrorReason=").IgnoreCase);
				Assert.That(serverParams[4], Is.StringStarting("ServerMinReq=").IgnoreCase);

				var firstServerOrderId = serverParams[2].Substring(serverParams[2].IndexOf('=') + 1);
				Assert.That(firstServerOrderId, Is.Not.Null);
				Assert.That(firstServerOrderId, Is.Not.Empty);

				Assert.That(GetOrderCount(connection, firstServerOrderId), Is.EqualTo(1), "Не совпадает кол-во позиций в заказа");

				orderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false, 1183);

				ParseSimpleOrder(orderHelper);

				result = orderHelper.PostSomeOrders();

				serverParams = result.Split(';');
				Assert.That(serverParams[0], Is.StringStarting("ClientOrderId=").IgnoreCase);
				Assert.That(serverParams[1], Is.StringStarting("PostResult=").IgnoreCase);
				Assert.That(serverParams[2], Is.StringStarting("ServerOrderId=").IgnoreCase);
				Assert.That(serverParams[3], Is.StringStarting("ErrorReason=").IgnoreCase);
				Assert.That(serverParams[4], Is.StringStarting("ServerMinReq=").IgnoreCase);

				var secondServerOrderId = serverParams[2].Substring(serverParams[2].IndexOf('=') + 1);
				Assert.That(secondServerOrderId, Is.Not.Null);
				Assert.That(secondServerOrderId, Is.Not.Empty);

				Assert.That(firstServerOrderId, Is.EqualTo(secondServerOrderId), "Заказ не помечен как дублирующийся");
			}
		}

		[Test]
		public void Check_double_order_for_old_client()
		{
			Check_simple_double_order(oldUserName, oldClientId);
		}

		[Test]
		public void Check_double_order_for_future_client()
		{
			Check_simple_double_order(futureUserName, futureAddressId);
		}

		public void Check_double_order_without_FullDuplicated(string userName, uint orderedClientId)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, userName);
				var orderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false, 1183);

				ParseFirstOrder(orderHelper);

				var result = orderHelper.PostSomeOrders();

				var serverParams = result.Split(';');
				Assert.That(serverParams[0], Is.StringStarting("ClientOrderId=").IgnoreCase);
				Assert.That(serverParams[1], Is.StringStarting("PostResult=").IgnoreCase);
				Assert.That(serverParams[2], Is.StringStarting("ServerOrderId=").IgnoreCase);
				Assert.That(serverParams[3], Is.StringStarting("ErrorReason=").IgnoreCase);
				Assert.That(serverParams[4], Is.StringStarting("ServerMinReq=").IgnoreCase);

				var firstServerOrderId = serverParams[2].Substring(serverParams[2].IndexOf('=') + 1);
				Assert.That(firstServerOrderId, Is.Not.Null);
				Assert.That(firstServerOrderId, Is.Not.Empty);

				Assert.That(GetOrderCount(connection, firstServerOrderId), Is.EqualTo(2), "Не совпадает кол-во позиций в заказа");

				orderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false, 1183);

				ParseSecondOrder(orderHelper);

				result = orderHelper.PostSomeOrders();

				serverParams = result.Split(';');
				Assert.That(serverParams[0], Is.StringStarting("ClientOrderId=").IgnoreCase);
				Assert.That(serverParams[1], Is.StringStarting("PostResult=").IgnoreCase);
				Assert.That(serverParams[2], Is.StringStarting("ServerOrderId=").IgnoreCase);
				Assert.That(serverParams[3], Is.StringStarting("ErrorReason=").IgnoreCase);
				Assert.That(serverParams[4], Is.StringStarting("ServerMinReq=").IgnoreCase);

				var secondServerOrderId = serverParams[2].Substring(serverParams[2].IndexOf('=') + 1);
				Assert.That(secondServerOrderId, Is.Not.Null);
				Assert.That(secondServerOrderId, Is.Not.Empty);

				Assert.That(firstServerOrderId, Is.Not.EqualTo(secondServerOrderId), "Заказ помечен как дублирующийся");

				Assert.That(GetOrderCount(connection, secondServerOrderId), Is.EqualTo(1), "Не совпадает кол-во позиций в заказа");
			}
		}

		[Test]
		public void Check_double_order_without_FullDuplicated_for_old_client()
		{
			Check_double_order_without_FullDuplicated(oldUserName, oldClientId);
		}

		[Test]
		public void Check_double_order_without_FullDuplicated_for_future_client()
		{
			Check_double_order_without_FullDuplicated(futureUserName, futureAddressId);
		}

		public void Check_simple_double_order_with_correctorders(string userName, uint orderedClientId)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, userName);
				var orderHelper = new ReorderHelper(updateData, connection, false, orderedClientId, true, 1183);

				ParseSimpleOrder(orderHelper);

				var result = orderHelper.PostSomeOrders();

				var serverParams = result.Split(';');
				Assert.That(serverParams[0], Is.StringStarting("ClientOrderId=").IgnoreCase);
				Assert.That(serverParams[1], Is.StringStarting("PostResult=").IgnoreCase);
				Assert.That(serverParams[2], Is.StringStarting("ServerOrderId=").IgnoreCase);
				Assert.That(serverParams[3], Is.StringStarting("ErrorReason=").IgnoreCase);
				Assert.That(serverParams[4], Is.StringStarting("ServerMinReq=").IgnoreCase);

				var firstServerOrderId = serverParams[2].Substring(serverParams[2].IndexOf('=') + 1);
				Assert.That(firstServerOrderId, Is.Not.Null);
				Assert.That(firstServerOrderId, Is.Not.Empty);

				Assert.That(GetOrderCount(connection, firstServerOrderId), Is.EqualTo(1), "Не совпадает кол-во позиций в заказа");

				orderHelper = new ReorderHelper(updateData, connection, false, orderedClientId, true, 1183);

				ParseSimpleOrder(orderHelper);

				result = orderHelper.PostSomeOrders();

				serverParams = result.Split(';');
				Assert.That(serverParams[0], Is.StringStarting("ClientOrderId=").IgnoreCase);
				Assert.That(serverParams[1], Is.StringStarting("PostResult=").IgnoreCase);
				Assert.That(serverParams[2], Is.StringStarting("ServerOrderId=").IgnoreCase);
				Assert.That(serverParams[3], Is.StringStarting("ErrorReason=").IgnoreCase);
				Assert.That(serverParams[4], Is.StringStarting("ServerMinReq=").IgnoreCase);

				var secondServerOrderId = serverParams[2].Substring(serverParams[2].IndexOf('=') + 1);
				Assert.That(secondServerOrderId, Is.Not.Null);
				Assert.That(secondServerOrderId, Is.Not.Empty);

				Assert.That(firstServerOrderId, Is.EqualTo(secondServerOrderId), "Заказ не помечен как дублирующийся");
			}
		}

		[Test]
		public void Check_duplicate_order_and_useCorrectOrders()
		{
			Check_simple_double_order_with_correctorders(oldUserName, oldClientId);
		}
	}
}
