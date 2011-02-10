using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Filter;
using NHibernate;
using NHibernate.Criterion;
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
using Test.Support;
using Common.Tools;
using Castle.ActiveRecord;
using Test.Support.Logs;

namespace Integration
{

	public class ParseHelper
	{
		public static string GetValue(string paramValue, string parametrName)
		{
			Assert.That(paramValue, Is.StringStarting(parametrName + "=").IgnoreCase);
			return paramValue.Substring(paramValue.IndexOf('=') + 1);
		}
	}

	public class OrderPositionResult
	{
		public ulong ClientPositionId { get; set; }

		public PositionSendResult SendResult { get; set; }

		public float ServerCost { get; set; }

		public uint ServerQuantity { get; set; }

		public static OrderPositionResult Parse(string clientPositionId, string sendResult, string serverCost, string serverQuantity)
		{
			var position = new OrderPositionResult
							{
								ClientPositionId = Convert.ToUInt64(ParseHelper.GetValue(clientPositionId, "ClientPositionId")),
								SendResult = (PositionSendResult)Convert.ToInt32(ParseHelper.GetValue(sendResult, "SendResult")),
								ServerCost = Convert.ToSingle(ParseHelper.GetValue(serverCost, "ServerCost"), CultureInfo.InvariantCulture.NumberFormat),
								ServerQuantity = Convert.ToUInt32(ParseHelper.GetValue(serverQuantity, "ServerQuantity"))
							};

			return position;
		}
	}

	public class OrderServiceResponce
	{
		public ulong ClientOrderId { get; set; }

		public OrderSendResult PostResult { get; set; }

		public ulong ServerOrderId { get; set; }

		public string ErrorReason { get; set; }

		public uint? ServerMinReq { get; set; }

		public DateTime SendDate { get; set; }

		public List<OrderPositionResult> Positions;

		public OrderServiceResponce()
		{
			Positions = new List<OrderPositionResult>();
		}

		public static OrderServiceResponce Parse(string clientOrderId, string postResult, string serverOrderId, string errorReason, string serverMinReq, string sendDate, List<string> positionResults)
		{
			var responce = new OrderServiceResponce
							{
								ClientOrderId = Convert.ToUInt64(ParseHelper.GetValue(clientOrderId, "ClientOrderId")),
								PostResult = (OrderSendResult)Convert.ToInt32(ParseHelper.GetValue(postResult, "PostResult")),
								ServerOrderId = Convert.ToUInt64(ParseHelper.GetValue(serverOrderId, "ServerOrderId")),
								ErrorReason = ParseHelper.GetValue(errorReason, "ErrorReason"),
								ServerMinReq = String.IsNullOrEmpty(ParseHelper.GetValue(serverMinReq, "ServerMinReq")) ? null : (uint?)Convert.ToUInt32(ParseHelper.GetValue(serverMinReq, "ServerMinReq")),
								SendDate = DateTime.ParseExact(ParseHelper.GetValue(sendDate, "SendDate"), "yyyy-MM-dd HH:mm:ss", null),
							};

			if (positionResults.Count > 0)
			{
				var index = 0;
				while (index < positionResults.Count)
				{
					if (index + 3 < positionResults.Count)
					{
						var position = OrderPositionResult.Parse(
							positionResults[index],
							positionResults[index+1],
							positionResults[index+2],
							positionResults[index+3]);
						responce.Positions.Add(position);
					}
					else
						Assert.Fail("Невозможно распарсить результат позиции начиная с индекса {0}: {1}", index, positionResults.Implode());
					index += 4;
				}
			}

			return responce;
		}

	}

	[TestFixture]
	public class ReorderHelperFixture
	{
		private TestClient client;
		private TestUser user;
		private TestAddress address;

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
			ServiceContext.GetUserHost = () => "127.0.0.1";

			ConfigurationManager.AppSettings["WaybillPath"] = "FtpRoot\\";
			if (Directory.Exists("FtpRoot"))
				FileHelper.DeleteDir("FtpRoot");

			using (var transaction = new TransactionScope())
			{

				var permission = TestUserPermission.ByShortcut("AF");


				client = TestClient.CreateSimple();
				user = client.Users[0];

				client.Users.Each(u =>
				{
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				user.Update();

				address = user.AvaliableAddresses[0];

				oldClient = TestOldClient.CreateTestClient();
				oldUser = oldClient.Users[0];

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


			Directory.CreateDirectory("FtpRoot");
			CreateFolders(oldClient.Id.ToString());
			CreateFolders(address.Id.ToString());

			MySqlHelper.ExecuteNonQuery(Settings.ConnectionString(), @"
delete 
from orders.OrdersHead 
where 
	ClientCode = ?ClientCode 
and WriteTime > now() - interval 2 week"
				,
				new MySqlParameter("?ClientCode", oldClient.Id));
			MySqlHelper.ExecuteNonQuery(Settings.ConnectionString(), @"
delete 
from orders.OrdersHead 
where 
	ClientCode = ?ClientCode 
and WriteTime > now() - interval 2 week"
				,
				new MySqlParameter("?ClientCode", client.Id));

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
					new ulong[] { Convert.ToUInt64(firstOffer["Id"].ToString().RightSlice(9)) }, //ClientServerCoreID
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
					new string[] { firstOffer["Cost"].ToString() },  //minCost
					new string[] { activePrice["PriceCode"].ToString() },  //MinPriceCode
					new string[] { firstOffer["Cost"].ToString() },  //leaderMinCost
					new string[] { activePrice["PriceCode"].ToString() },  //leaderMinPriceCode
					new string[] { "" },  //supplierPriceMarkup
					new string[] { "" }, //delayOfPayment,
					new string[] { firstOffer["Quantity"].ToString() }, //coreQuantity,
					new string[] { "" }, //unit,
					new string[] { "" }, //volume,
					new string[] { "" }, //note,
					new string[] { "" }, //period,
					new string[] { "" }, //doc,
					new string[] { "" }, //registryCost,
					new bool[] { false }, //vitallyImportant,
					new string[] { "" }, //retailMarkup,
					new string[] { "" }, //producerCost,
					new string[] { "" }, //nds
					new string[] { "" } //retailCost,
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
					new string[] { "", "" }, //nds
					new string[] { "", "" } //retailCost,
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
					new ulong[] { 2L, 3L },
					new ulong[] { 2L, 3L },
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
					new string[] { "", "" }, //nds
					new string[] { "", "" } //retailCost,
					);
		}

		public void ParseOrderWithSimpleDouble(ReorderHelper orderHelper)
		{
			orderHelper.ParseOrders(
					1,
					new ulong[] { 1L },
					new ulong[] { Convert.ToUInt64(activePrice["PriceCode"]) },
					new ulong[] { Convert.ToUInt64(activePrice["RegionCode"]) },
					new DateTime[] { DateTime.Now }, //pricedate
					new string[] { "" },             //clientaddition
					new ushort[] { 3 },              //rowCount
					new ulong[] { 1L, 2L, 3L },              //clientPositionId
					new ulong[] { Convert.ToUInt64(firstOffer["Id"].ToString().RightSlice(9)), Convert.ToUInt64(secondOffer["Id"].ToString().RightSlice(9)), Convert.ToUInt64(firstOffer["Id"].ToString().RightSlice(9)) }, //ClientServerCoreID
					new ulong[] { Convert.ToUInt64(firstOffer["ProductId"]), Convert.ToUInt64(secondOffer["ProductId"]), Convert.ToUInt64(firstOffer["ProductId"]) },     //ProductId
					new string[] { firstOffer["CodeFirmCr"].ToString(), secondOffer["CodeFirmCr"].ToString(), firstOffer["CodeFirmCr"].ToString() },
					new ulong[] { Convert.ToUInt64(firstOffer["SynonymCode"]), Convert.ToUInt64(secondOffer["SynonymCode"]), Convert.ToUInt64(firstOffer["SynonymCode"]) }, //SynonymCode
					new string[] { firstOffer["SynonymFirmCrCode"].ToString(), secondOffer["SynonymFirmCrCode"].ToString(), firstOffer["SynonymFirmCrCode"].ToString() },
					new string[] { firstOffer["Code"].ToString(), secondOffer["Code"].ToString(), firstOffer["Code"].ToString() },
					new string[] { firstOffer["CodeCr"].ToString(), secondOffer["CodeCr"].ToString(), firstOffer["CodeCr"].ToString() },
					new bool[] { Convert.ToBoolean(firstOffer["Junk"]), Convert.ToBoolean(secondOffer["Junk"]), Convert.ToBoolean(firstOffer["Junk"]) },
					new bool[] { Convert.ToBoolean(firstOffer["Await"]), Convert.ToBoolean(secondOffer["Await"]), Convert.ToBoolean(firstOffer["Await"]) },
					new string[] { firstOffer["RequestRatio"].ToString(), secondOffer["RequestRatio"].ToString(), firstOffer["RequestRatio"].ToString() },
					new string[] { firstOffer["OrderCost"].ToString(), secondOffer["OrderCost"].ToString(), firstOffer["OrderCost"].ToString() },
					new string[] { firstOffer["MinOrderCount"].ToString(), secondOffer["MinOrderCount"].ToString(), firstOffer["MinOrderCount"].ToString() },
					new ushort[] { 2, 1, 1 }, //Quantity
					new decimal[] { Convert.ToDecimal(firstOffer["Cost"]), Convert.ToDecimal(secondOffer["Cost"]), Convert.ToDecimal(firstOffer["Cost"]) },
					new string[] { firstOffer["Cost"].ToString(), secondOffer["Cost"].ToString(), firstOffer["Cost"].ToString() },  //minCost
					new string[] { activePrice["PriceCode"].ToString(), activePrice["PriceCode"].ToString(), activePrice["PriceCode"].ToString() },  //MinPriceCode
					new string[] { firstOffer["Cost"].ToString(), secondOffer["Cost"].ToString(), firstOffer["Cost"].ToString() },  //leaderMinCost
					new string[] { activePrice["PriceCode"].ToString(), activePrice["PriceCode"].ToString(), activePrice["PriceCode"].ToString() },  //leaderMinPriceCode
					new string[] { "", "", "" },  //supplierPriceMarkup
					new string[] { "" }, //delayOfPayment,
					new string[] { firstOffer["Quantity"].ToString(), secondOffer["Quantity"].ToString(), firstOffer["Quantity"].ToString() }, //coreQuantity,
					new string[] { "", "", "" }, //unit,
					new string[] { "", "", "" }, //volume,
					new string[] { "", "", "" }, //note,
					new string[] { "", "", "" }, //period,
					new string[] { "", "", "" }, //doc,
					new string[] { "", "", "" }, //registryCost,
					new bool[] { false, false, false }, //vitallyImportant,
					new string[] { "", "", "" }, //retailMarkup,
					new string[] { "", "", "" }, //producerCost,
					new string[] { "", "", "" }, //nds
					new string[] { "", "", "" } //retailCost,
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
				var orderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false);

				ParseSimpleOrder(orderHelper);

				var result = orderHelper.PostSomeOrders();

				var firstServerOrderId = CheckServiceResponse(result);

				Assert.That(firstServerOrderId, Is.Not.Null);
				Assert.That(firstServerOrderId, Is.Not.Empty);

				Assert.That(GetOrderCount(connection, firstServerOrderId), Is.EqualTo(1), "Не совпадает кол-во позиций в заказе");

				orderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false);

				ParseSimpleOrder(orderHelper);

				result = orderHelper.PostSomeOrders();

				var secondServerOrderId = CheckServiceResponse(result);

				Assert.That(secondServerOrderId, Is.Not.Null);
				Assert.That(secondServerOrderId, Is.Not.Empty);

				Assert.That(firstServerOrderId, Is.EqualTo(secondServerOrderId), "Заказ не помечен как дублирующийся");
			}
		}

		[Test]
		public void Check_double_order_for_old_client()
		{
			Check_simple_double_order(oldUser.OSUserName, oldClient.Id);
		}

		[Test]
		public void Check_double_order_for_future_client()
		{
			Check_simple_double_order(user.Login, address.Id);
		}

		public void Check_double_order_without_FullDuplicated(string userName, uint orderedClientId)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, userName);
				var orderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false);

				ParseFirstOrder(orderHelper);

				var result = orderHelper.PostSomeOrders();

				var firstServerOrderId = CheckServiceResponse(result);
				Assert.That(firstServerOrderId, Is.Not.Null);
				Assert.That(firstServerOrderId, Is.Not.Empty);

				Assert.That(GetOrderCount(connection, firstServerOrderId), Is.EqualTo(2), "Не совпадает кол-во позиций в заказе");

				orderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false);

				ParseSecondOrder(orderHelper);

				result = orderHelper.PostSomeOrders();

				var secondServerOrderId = CheckServiceResponse(result);
				Assert.That(secondServerOrderId, Is.Not.Null);
				Assert.That(secondServerOrderId, Is.Not.Empty);

				Assert.That(firstServerOrderId, Is.Not.EqualTo(secondServerOrderId), "Заказ помечен как дублирующийся");

				Assert.That(GetOrderCount(connection, secondServerOrderId), Is.EqualTo(1), "Не совпадает кол-во позиций в заказе");
			}
		}

		[Test]
		public void Check_double_order_without_FullDuplicated_for_old_client()
		{
			Check_double_order_without_FullDuplicated(oldUser.OSUserName, oldClient.Id);
		}

		[Test]
		public void Check_double_order_without_FullDuplicated_for_future_client()
		{
			Check_double_order_without_FullDuplicated(user.Login, address.Id);
		}

		public void Check_simple_double_order_with_correctorders(string userName, uint orderedClientId)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, userName);
				var orderHelper = new ReorderHelper(updateData, connection, false, orderedClientId, true);

				ParseSimpleOrder(orderHelper);

				var result = orderHelper.PostSomeOrders();

				var firstServerOrderId = CheckServiceResponse(result);
				Assert.That(firstServerOrderId, Is.Not.Null);
				Assert.That(firstServerOrderId, Is.Not.Empty);

				Assert.That(GetOrderCount(connection, firstServerOrderId), Is.EqualTo(1), "Не совпадает кол-во позиций в заказе");

				orderHelper = new ReorderHelper(updateData, connection, false, orderedClientId, true);

				ParseSimpleOrder(orderHelper);

				result = orderHelper.PostSomeOrders();

				var secondServerOrderId = CheckServiceResponse(result);
				Assert.That(secondServerOrderId, Is.Not.Null);
				Assert.That(secondServerOrderId, Is.Not.Empty);

				Assert.That(firstServerOrderId, Is.EqualTo(secondServerOrderId), "Заказ не помечен как дублирующийся");
			}
		}

		[Test]
		public void Check_duplicate_order_and_useCorrectOrders()
		{
			Check_simple_double_order_with_correctorders(oldUser.OSUserName, oldClient.Id);
		}

		public void Check_order_with_ImpersonalPrice(string userName, uint orderedClientId)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, userName);

				updateData.EnableImpersonalPrice = true;

				var orderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false);

				ParseFirstOrder(orderHelper);

				var result = orderHelper.PostSomeOrders();

				var firstServerOrderId = CheckServiceResponse(result);
				Assert.That(firstServerOrderId, Is.Not.Null);
				Assert.That(firstServerOrderId, Is.Not.Empty);

				Assert.That(GetOrderCount(connection, firstServerOrderId), Is.EqualTo(2), "Не совпадает кол-во позиций в заказе");

				var countWithSynonymNull = MySqlHelper
					.ExecuteScalar(
						connection,
						"select count(*) from orders.OrdersList ol where ol.OrderId = ?OrderId and ol.SynonymCode is null and ol.SynonymFirmCrCode is null",
						new MySqlParameter("?OrderId", firstServerOrderId));
				Assert.That(countWithSynonymNull, Is.EqualTo(2), "Не совпадает кол-во позиций в заказе, у которых поля SynonymCode SynonymFirmCr в null");
			}
		}

		[Test]
		public void Check_order_with_ImpersonalPrice_for_old_client()
		{
			Check_order_with_ImpersonalPrice(oldUser.OSUserName, oldClient.Id);
		}

		private string CheckServiceResponse(string response)
		{
			var serverParams = response.Split(';');
			Assert.That(serverParams.Length, Is.EqualTo(5), "Неожидаемое кол-во секций в ответе сервера");
			Assert.That(serverParams[0], Is.StringStarting("ClientOrderId=").IgnoreCase);
			Assert.That(serverParams[1], Is.EqualTo("PostResult=0").IgnoreCase, "Возникла ошибка при отправке заказа");
			Assert.That(serverParams[2], Is.StringStarting("ServerOrderId=").IgnoreCase);
			Assert.That(serverParams[3], Is.EqualTo("ErrorReason=").IgnoreCase, "Возникла ошибка при отправке заказа");
			Assert.That(serverParams[4], Is.StringStarting("ServerMinReq=").IgnoreCase);

			return serverParams[2].Substring(serverParams[2].IndexOf('=') + 1);
		}

		private string CheckServiceResponseWithSendDate(string response)
		{
			var serverParams = response.Split(';');
			Assert.That(serverParams.Length, Is.EqualTo(6), "Неожидаемое кол-во секций в ответе сервера");
			Assert.That(serverParams[0], Is.StringStarting("ClientOrderId=").IgnoreCase);
			Assert.That(serverParams[1], Is.EqualTo("PostResult=0").IgnoreCase, "Возникла ошибка при отправке заказа");
			Assert.That(serverParams[2], Is.StringStarting("ServerOrderId=").IgnoreCase);
			Assert.That(serverParams[3], Is.EqualTo("ErrorReason=").IgnoreCase, "Возникла ошибка при отправке заказа");
			Assert.That(serverParams[4], Is.StringStarting("ServerMinReq=").IgnoreCase);
			Assert.That(serverParams[5], Is.StringStarting("SendDate=").IgnoreCase);

			return serverParams[2].Substring(serverParams[2].IndexOf('=') + 1);
		}

		private OrderServiceResponce ConvertServiceResponse(string response)
		{
			var serverParams = response.Split(';');
			Assert.That(serverParams.Length, Is.GreaterThanOrEqualTo(6), "Неожидаемое кол-во секций в ответе сервера");

			var positionResults = new List<string>();
			if (serverParams.Length > 6)
				positionResults = serverParams.ToList().GetRange(6, serverParams.Length - 6);

			var orderResponce = OrderServiceResponce.Parse(
				serverParams[0], 
				serverParams[1], 
				serverParams[2], 
				serverParams[3],
				serverParams[4], 
				serverParams[5],
				positionResults);

			return orderResponce;
		}

		public void Check_simple_order_with_leaders(string userName, uint orderedClientId)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, userName);

				MySqlHelper.ExecuteNonQuery(
					connection,
					"update usersettings.RetClientsSet set CalculateLeader = 1 where ClientCode = ?ClientCode",
					new MySqlParameter("?ClientCode", updateData.ClientId));

				var orderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false);

				ParseSimpleOrder(orderHelper);

				var result = orderHelper.PostSomeOrders();

				var firstServerOrderId = CheckServiceResponse(result);

				Assert.That(firstServerOrderId, Is.Not.Null);
				Assert.That(firstServerOrderId, Is.Not.Empty);

				Assert.That(GetOrderCount(connection, firstServerOrderId), Is.EqualTo(1), "Не совпадает кол-во позиций в заказе");

				var leaderInfoCount = 
					Convert.ToInt32(MySqlHelper
						.ExecuteScalar(
							connection,
							@"
select 
  count(*) 
from 
  orders.orderslist 
  inner join orders.leaders on leaders.OrderListId = orderslist.RowId
where 
  orderslist.OrderId = ?OrderId",
							new MySqlParameter("?OrderId", firstServerOrderId)));
				Assert.That(leaderInfoCount, Is.EqualTo(1), "Не совпадает кол-во позиций в информации о лидерах в заказе");
			}
		}

		[Test(Description = "Проверяем создание информации о лидерах в заказе для старых клиентов")]
		public void Check_simple_order_with_leaders_for_old_client()
		{
			Check_simple_order_with_leaders(oldUser.OSUserName, oldClient.Id);
		}

		[Test(Description = "Проверяем создание информации о лидерах в заказе для клиентов из новой реальности")]
		public void Check_simple_order_with_leaders_for_future_client()
		{
			Check_simple_order_with_leaders(user.Login, address.Id);
		}

		[Test(Description = "Проверка отправки заказа приложением с версией больше 1271")]
		public void Check_order_with_buildNumber_greater_than_1271()
		{
			string userName = user.Login;
			uint orderedClientId = address.Id;
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, userName);

				updateData.BuildNumber = 1272;

				MySqlHelper.ExecuteNonQuery(
					connection,
					"update usersettings.RetClientsSet set CalculateLeader = 1 where ClientCode = ?ClientCode",
					new MySqlParameter("?ClientCode", updateData.ClientId));

				var orderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false);

				ParseFirstOrder(orderHelper);

				var result = orderHelper.PostSomeOrders();

				var firstServerOrderId = CheckServiceResponseWithSendDate(result);

				Assert.That(firstServerOrderId, Is.Not.Null);
				Assert.That(firstServerOrderId, Is.Not.Empty);

				Assert.That(GetOrderCount(connection, firstServerOrderId), Is.EqualTo(2), "Не совпадает кол-во позиций в заказе");
			}
		}

		private List<ClientOrderHeader> GetOrders(ReorderHelper helper)
		{
			var field = helper.GetType().GetField("_orders", BindingFlags.NonPublic | BindingFlags.Instance);
			return (List<ClientOrderHeader>)field.GetValue(helper);

		}

		[Test(Description = "Отправляем заказ, в котором пристутсвуют позиции с одинаковым CoreId, сохраниться должны только по одной позиции из дублей и при повторной отправке заказа весь заказ должен быть помечен как дублирующийся")]
		public void Check_order_with_double_CoreId()
		{
			string userName = user.Login;
			uint orderedClientId = address.Id;
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, userName);
				updateData.BuildNumber = 1272;


				try
				{
					var memoryAppender = new MemoryAppender();
					memoryAppender.AddFilter(new LoggerMatchFilter { AcceptOnMatch = true, LoggerToMatch = "PrgData", Next = new DenyAllFilter() });
					BasicConfigurator.Configure(memoryAppender);

					var firstOrderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false);

					ParseOrderWithSimpleDouble(firstOrderHelper);

					var firstResult = firstOrderHelper.PostSomeOrders();

					var firstResponse = ConvertServiceResponse(firstResult);

					var firstParsedOrders = GetOrders(firstOrderHelper);

					Assert.That(firstResponse.ServerOrderId, Is.GreaterThan(0));

					Assert.That(GetOrderCount(connection, firstResponse.ServerOrderId.ToString()), Is.EqualTo(2), "Не совпадает кол-во позиций в заказе, должны были сохранить только не дублирующиеся");

					Assert.That(firstParsedOrders.Count, Is.EqualTo(1));
					Assert.That(firstParsedOrders[0].Positions.Count, Is.EqualTo(3));
					Assert.That(firstParsedOrders[0].Positions[0].Duplicated, Is.EqualTo(false));
					Assert.That(firstParsedOrders[0].Positions[1].Duplicated, Is.EqualTo(false));
					Assert.That(firstParsedOrders[0].Positions[2].Duplicated, Is.EqualTo(true));

					var coreIdFillCount =
						Convert.ToInt32(MySqlHelper
							.ExecuteScalar(
								connection,
								@"
select 
  count(*) 
from 
  orders.orderslist 
where 
	orderslist.OrderId = ?OrderId
and orderslist.Coreid is not null",
								new MySqlParameter("?OrderId", firstResponse.ServerOrderId)));
					Assert.That(coreIdFillCount, Is.EqualTo(2), "Не совпадает кол-во позиций с заполненным полем CoreId");

					var events = memoryAppender.GetEvents();
					var lastEvent = events[events.Length - 1];

					Assert.That(lastEvent.Level, Is.EqualTo(Level.Error));
					Assert.That(lastEvent.RenderedMessage, Is.StringStarting(String.Format("Заказ {0} содержит дублирующиеся позиции по CoreId", firstResponse.ServerOrderId)));

					var secondOrderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false);

					ParseOrderWithSimpleDouble(secondOrderHelper);

					var secondResult = secondOrderHelper.PostSomeOrders();

					var secondResponse = ConvertServiceResponse(secondResult);

					Assert.That(secondResponse.ServerOrderId, Is.EqualTo(firstResponse.ServerOrderId));

					var secondParsedOrders = GetOrders(secondOrderHelper);
					Assert.That(secondParsedOrders.Count, Is.EqualTo(1));
					Assert.That(secondParsedOrders[0].Positions.Count, Is.EqualTo(3));
					Assert.That(secondParsedOrders[0].Positions.TrueForAll(item => item.Duplicated), Is.EqualTo(true), "Не все позиции помеченны как дублирующиеся");

					events = memoryAppender.GetEvents();
					lastEvent = events[events.Length - 1];

					Assert.That(lastEvent.Level, Is.EqualTo(Level.Error));
					Assert.That(lastEvent.RenderedMessage, Is.StringStarting(String.Format("Заказ {0} содержит дублирующиеся позиции по CoreId", secondResponse.ServerOrderId)));
				}
				finally
				{
					LogManager.ResetConfiguration();
				}
			}
		}

		[Test(Description = "Отправляем заказ и создаем дубль по позиции с таким же CoreId, при попытке отправить заказ повторно мы должны получить, что позиция дублируюущаяся")]
		public void Check_double_order_for_future_client_with_same_exists_OrderItems()
		{
			var userName = user.Login;
			var orderedClientId = address.Id;

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, userName);
				updateData.BuildNumber = 1272;

				var firstOrderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false);

				ParseSimpleOrder(firstOrderHelper);

				var firstResult = firstOrderHelper.PostSomeOrders();

				var firstResponse = ConvertServiceResponse(firstResult);

				Assert.That(firstResponse.ServerOrderId, Is.GreaterThan(0));

				Assert.That(GetOrderCount(connection, firstResponse.ServerOrderId.ToString()), Is.EqualTo(1), "Не совпадает кол-во позиций в заказе");

				var insertCount = MySqlHelper.ExecuteNonQuery(
					connection,
					@"
insert into Orders.OrdersList 
(`OrderID`, `CoreId`, `ProductId`, `CodeFirmCr`, `SynonymCode`, `SynonymFirmCrCode`, `Code`, `CodeCr`, `Quantity`, `Junk`, `Await`, `RequestRatio`, `OrderCost`, `MinOrderCount`, `Cost`, `SupplierPriceMarkup`, `RetailMarkup`, `RetailCost`) 
select 
  `OrdersList`.`OrderID`, `OrdersList`.`CoreId`, `OrdersList`.`ProductId`, `OrdersList`.`CodeFirmCr`, `OrdersList`.`SynonymCode`, `OrdersList`.`SynonymFirmCrCode`, `OrdersList`.`Code`, `OrdersList`.`CodeCr`, `OrdersList`.`Quantity`, `OrdersList`.`Junk`, `OrdersList`.`Await`, `OrdersList`.`RequestRatio`, `OrdersList`.`OrderCost`, `OrdersList`.`MinOrderCount`, `OrdersList`.`Cost`, `OrdersList`.`SupplierPriceMarkup`, `OrdersList`.`RetailMarkup`, `OrdersList`.`RetailCost`
from Orders.OrdersList where OrderId = ?OrderId limit 1;
update Orders.OrdersHead set RowCount = RowCount + 1 where RowId = ?OrderId;",
					new MySqlParameter("?OrderId", firstResponse.ServerOrderId));
				Assert.That(insertCount, Is.EqualTo(2), "Количество обновленных или добавленных записей должно быть 2");

				var secondOrderHelper = new ReorderHelper(updateData, connection, true, orderedClientId, false);

				ParseSimpleOrder(secondOrderHelper);

				var secondResult = secondOrderHelper.PostSomeOrders();

				var secondResponse = ConvertServiceResponse(secondResult);

				Assert.That(secondResponse.ServerOrderId, Is.GreaterThan(0));
				Assert.That(secondResponse.ServerOrderId, Is.EqualTo(firstResponse.ServerOrderId), "Заказ не помечен как дублирующийся");
			}

		}

		[Test(Description = "Отправляем несколько заказов, один из которых должен быть с нарушением минимальной суммы заказа")]
		public void Send_some_orders_with_MinReq()
		{
			TestPrice price;
			TestPrice minReqPrice;

			TestCore core;
			TestCore minReqCore;

			using (var transaction = new TransactionScope())
			{
				var prices = user.GetActivePrices();
				price = prices[0];
				minReqPrice = prices.First(p => p.Supplier != price.Supplier);

				core =
					TestCore.FindFirst(Expression.Eq("Price", price));
				minReqCore =
					TestCore.FindFirst(Expression.Eq("Price", minReqPrice));

				NHibernateUtil.Initialize(core);
				NHibernateUtil.Initialize(minReqCore);

				var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof(ActiveRecordBase));
				try
				{
					session.CreateSQLQuery(@"
update
  Future.Users u
  join future.Clients c on u.ClientId = c.Id
  join Future.UserAddresses ua on ua.UserId = u.Id
  join future.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
  join future.Intersection i on i.ClientId = c.Id
  join future.AddressIntersection ai on (ai.IntersectionId = i.Id) and (ai.AddressId = a.Id)
set
  ai.ControlMinReq = 0
where
	(u.Id = :UserId)
and (a.Id = :AddressId)
and (i.PriceId = :PriceId)
")
						.SetParameter("UserId", user.Id)
						.SetParameter("AddressId", address.Id)
						.SetParameter("PriceId", price.Id)
						.ExecuteUpdate();

					session.CreateSQLQuery(@"
update
  Future.Users u
  join future.Clients c on u.ClientId = c.Id
  join Future.UserAddresses ua on ua.UserId = u.Id
  join future.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
  join future.Intersection i on i.ClientId = c.Id
  join future.AddressIntersection ai on (ai.IntersectionId = i.Id) and (ai.AddressId = a.Id)
set
  ai.ControlMinReq = 1,
  ai.MinReq = 10000
where
	(u.Id = :UserId)
and (a.Id = :AddressId)
and (i.PriceId = :PriceId)
")
						.SetParameter("UserId", user.Id)
						.SetParameter("AddressId", address.Id)
						.SetParameter("PriceId", minReqPrice.Id)
						.ExecuteUpdate();
				}
				finally
				{
					ActiveRecordMediator.GetSessionFactoryHolder().ReleaseSession(session);
				}

				transaction.VoteCommit();
			}

			var firstOrder = new ClientOrderHeader
			                 	{
									ActivePrice = new ActivePrice
									{
										Id = new PriceKey(new PriceList{PriceCode = price.Id, Firm = new Firm{FirmCode = price.Supplier.Id, ShortName = price.Supplier.ShortName}}) { RegionCode = client.RegionCode },
										PriceDate = DateTime.Now,
									},
			                 		ClientOrderId = 1,
			                 	};
			firstOrder.Positions.Add(
				new ClientOrderPosition
				{
					ClientPositionID = 1,
					ClientServerCoreID = core.Id,
					OrderedQuantity = 1,
					Offer = new Offer
					        	{
					        		Id = core.Id, 
									ProductId = core.Product.Id,
									CodeFirmCr = core.Producer != null ? (uint?)core.Producer.Id : null,
									SynonymCode = core.ProductSynonym.Id,
									SynonymFirmCrCode = core.ProducerSynonym != null ? (uint?)core.ProducerSynonym.Id : null,
					        	}
				});

			var minReqOrder = new ClientOrderHeader
			{
				ActivePrice = new ActivePrice
				{
					Id = new PriceKey(new PriceList { PriceCode = minReqPrice.Id, Firm = new Firm { FirmCode = minReqPrice.Supplier.Id, ShortName = minReqPrice.Supplier.ShortName } }) { RegionCode = client.RegionCode },
					PriceDate = DateTime.Now,
				},
				ClientOrderId = 2,
			};
			minReqOrder.Positions.Add(
				new ClientOrderPosition
				{
					ClientPositionID = 2,
					ClientServerCoreID = minReqCore.Id,
					OrderedQuantity = 1,
					Offer = new Offer
					{
						Id = minReqCore.Id,
						ProductId = minReqCore.Product.Id,
						CodeFirmCr = minReqCore.Producer != null ? (uint?)minReqCore.Producer.Id : null,
						SynonymCode = minReqCore.ProductSynonym.Id,
						SynonymFirmCrCode = minReqCore.ProducerSynonym != null ? (uint?)minReqCore.ProducerSynonym.Id : null,
					}
				});


			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, user.Login);
				updateData.BuildNumber = 1272;

				var firstOrderHelper = new ReorderHelper(updateData, connection, true, address.Id, false);

				var firstParsedOrders = GetOrders(firstOrderHelper);

				firstParsedOrders.Add(firstOrder);
				firstParsedOrders.Add(minReqOrder);

				var firstResult = firstOrderHelper.PostSomeOrders();

				var orderResults = firstResult.Split(new []{"ClientOrderID="}, StringSplitOptions.RemoveEmptyEntries);

				Assert.That(orderResults.Length, Is.EqualTo(2), "Должно быть два ответа");

				var firstOrderResponse = ConvertServiceResponse("ClientOrderID=" + orderResults[0].TrimEnd(';'));
				var minReqOrderResponse = ConvertServiceResponse("ClientOrderID=" + orderResults[1].TrimEnd(';'));

				Assert.That(firstOrderResponse.ServerOrderId, Is.GreaterThan(0));

				Assert.That(minReqOrderResponse.ServerOrderId, Is.EqualTo(0));
				Assert.That(minReqOrderResponse.PostResult, Is.EqualTo(OrderSendResult.LessThanMinReq));
				Assert.That(minReqOrderResponse.ErrorReason, Is.StringContaining("Сумма заказа меньше минимально допустимой").IgnoreCase);
			}

			using (new SessionScope())
			{
				var logs = TestAnalitFUpdateLog.Queryable.Where(updateLog => updateLog.UserId == user.Id && updateLog.UpdateType == Convert.ToUInt32(RequestType.SendOrders)).ToList();
				Assert.That(logs.Count, Is.EqualTo(1));
				Assert.That(logs[0].Addition, Is.StringContaining("был отклонен из-за нарушения минимальной суммы заказа").IgnoreCase, "В поле Addition должна быть запись об заказах с ошибками");
			}
		}

	}
}
