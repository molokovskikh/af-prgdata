using System;
using System.IO;
using System.Linq;
using Castle.ActiveRecord;
using Common.Tools;
using Common.Tools.Calendar;
using Inforoom.Common;
using Integration.BaseTests;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData;
using System.Data;
using PrgData.Common;
using Test.Support;
using Test.Support.Logs;

namespace Integration
{
	[TestFixture]
	public class PostSomeOrdersFixture : PrepareDataFixture
	{
		private TestClient _client;
		private TestUser _user;
		private TestAddress _address;

		private bool getOffers;
		private DataRow activePrice;
		private DataRow firstOffer;
		private DataRow secondOffer;
		private DataRow thirdOffer;
		private DataRow fourOffer;

		[SetUp]
		public void Setup()
		{
			InitClient();

			MySqlHelper.ExecuteNonQuery(Settings.ConnectionString(), @"
delete 
from orders.OrdersHead 
where 
	ClientCode = ?ClientCode 
and WriteTime > now() - interval 2 week"
				,
				new MySqlParameter("?ClientCode", _client.Id));

			GetOffers();
		}

		private void InitClient()
		{
			_user = CreateUser();
			_client = _user.Client;
			_address = _client.Addresses[0];

			SetCurrentUser(_user.Login);
			CreateFolders(_address.Id.ToString());
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
call Customers.GetOffers(?UserId)",
						new MySqlParameter("?UserId", _user.Id));

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

				var UniqueId = MySqlHelper.ExecuteScalar(connection, "select AFCopyId from usersettings.UserUpdateInfo where UserId = " + _user.Id).ToString();

				string serverResponse;
				using (var prgData = new PrgDataEx())
				{
					serverResponse = prgData.PostSomeOrders(
						UniqueId,
						true,         //ForceSend
						false,        //UseCorrectOrders
						_address.Id, //ClientId => AddressId
						1,            //OrderCount
						new ulong[] { 1L },  //ClientOrderId
						new ulong[] { Convert.ToUInt64(activePrice["PriceCode"]) },
						new ulong[] { Convert.ToUInt64(activePrice["RegionCode"]) },
						new DateTime[] { DateTime.Now }, //PriceDate
						new string[] { "" },             //ClientAddition
						new ushort[] { 1 },              //RowCount
						new ulong[] { 1 },               //ClientPositionId
						new ulong[] { Convert.ToUInt64(firstOffer["Id"].ToString().RightSlice(9)) },  //ClientServerCoreId
						new ulong[] { Convert.ToUInt64(firstOffer["ProductId"]) },
						new string[] { firstOffer["CodeFirmCr"].ToString() },
						new ulong[] { Convert.ToUInt64(firstOffer["SynonymCode"]) },
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

		[Test]
		public void Send_order_for_705()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var UniqueId = MySqlHelper.ExecuteScalar(connection, "select AFCopyId from usersettings.UserUpdateInfo where UserId = " + _user.Id).ToString();

				string serverResponse;
				using (var prgData = new PrgDataEx())
				{
					serverResponse = prgData.PostOrder(
						UniqueId,
						0,
						_address.Id, //ClientId => AddressId
						Convert.ToUInt32(activePrice["PriceCode"]),
						Convert.ToUInt64(activePrice["RegionCode"]),
						DateTime.Now, //PriceDate
						"",             //ClientAddition
						1 ,              //RowCount
						new uint[] { Convert.ToUInt32(firstOffer["ProductId"]) },
						1u, //ClientOrderId
						new string[] { firstOffer["CodeFirmCr"].ToString() },
						new uint[] { Convert.ToUInt32(firstOffer["SynonymCode"]) },
						new string[] { firstOffer["SynonymFirmCrCode"].ToString() },
						new string[] { firstOffer["Code"].ToString() },
						new string[] { firstOffer["CodeCr"].ToString() },
						new ushort[] { 1 }, //Quantity
						new bool[] { Convert.ToBoolean(firstOffer["Junk"]) },
						new bool[] { Convert.ToBoolean(firstOffer["Await"]) },
						new decimal[] { Convert.ToDecimal(firstOffer["Cost"]) },
						new string[] { firstOffer["Cost"].ToString() },        //minCost
						new string[] { activePrice["PriceCode"].ToString() },  //MinPriceCode
						new string[] { firstOffer["Cost"].ToString() },        //leaderMinCost
						new string[] { firstOffer["RequestRatio"].ToString() },
						new string[] { firstOffer["OrderCost"].ToString() },
						new string[] { firstOffer["MinOrderCount"].ToString() },
						new string[] { activePrice["PriceCode"].ToString() }  //leaderMinPriceCode
						); 
				}

				Assert.That(serverResponse, Is.Not.Null);
				Assert.That(serverResponse, Is.Not.Empty);
				var serverParams = serverResponse.Split(';');
				Assert.That(serverParams[0], Is.StringStarting("OrderId=").IgnoreCase);

				var serverOrderId = serverParams[0].Substring(serverParams[0].IndexOf('=') + 1);
				Assert.That(serverOrderId, Is.Not.Null);
				Assert.That(serverOrderId, Is.Not.Empty);

				var realOrderId = MySqlHelper.ExecuteScalar(connection, "select RowId from orders.ordershead where RowId = " + serverOrderId);
				Assert.That(realOrderId, Is.Not.Null);
				Assert.That(realOrderId.ToString(), Is.Not.Empty);
				Assert.That(realOrderId.ToString(), Is.EqualTo(serverOrderId));

				var calculateLeader = MySqlHelper.ExecuteScalar(connection, "select CalculateLeader from orders.ordershead where RowId = " + serverOrderId);
				Assert.That(calculateLeader, Is.Not.Null);

				var insertedCount = MySqlHelper.ExecuteScalar(connection, "select count(*) from orders.ordershead inner join orders.orderslist on orderslist.OrderId = ordershead.RowId where ordershead.RowId = " + serverOrderId);
				Assert.That(insertedCount, Is.EqualTo(1), "Не совпадает кол-во записей в заказе");

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

				CheckInsertedPosition(connection, serverOrderId, firstOffer);
			}
		}

		[Test]
		public void Send_order_with_some_positions_for_705()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var UniqueId = MySqlHelper.ExecuteScalar(connection, "select AFCopyId from usersettings.UserUpdateInfo where UserId = " + _user.Id).ToString();

				string serverResponse;
				using (var prgData = new PrgDataEx())
				{
					serverResponse = prgData.PostOrder(
						UniqueId,
						0,
						_address.Id, //ClientId => AddressId
						Convert.ToUInt32(activePrice["PriceCode"]),
						Convert.ToUInt64(activePrice["RegionCode"]),
						DateTime.Now, //PriceDate
						"",             //ClientAddition
						3,              //RowCount
						new uint[] { Convert.ToUInt32(firstOffer["ProductId"]), Convert.ToUInt32(secondOffer["ProductId"]), Convert.ToUInt32(thirdOffer["ProductId"]) },
						1u, //ClientOrderId
						new string[] { firstOffer["CodeFirmCr"].ToString(), secondOffer["CodeFirmCr"].ToString(), thirdOffer["CodeFirmCr"].ToString() },
						new uint[] { Convert.ToUInt32(firstOffer["SynonymCode"]), Convert.ToUInt32(secondOffer["SynonymCode"]), Convert.ToUInt32(thirdOffer["SynonymCode"]) },
						new string[] { firstOffer["SynonymFirmCrCode"].ToString(), secondOffer["SynonymFirmCrCode"].ToString(), thirdOffer["SynonymFirmCrCode"].ToString() },
						new string[] { firstOffer["Code"].ToString(), secondOffer["Code"].ToString(), thirdOffer["Code"].ToString() },
						new string[] { firstOffer["CodeCr"].ToString(), secondOffer["CodeCr"].ToString(), thirdOffer["CodeCr"].ToString() },
						new ushort[] { 1, 1, 1 }, //Quantity
						new bool[] { Convert.ToBoolean(firstOffer["Junk"]), Convert.ToBoolean(secondOffer["Junk"]), Convert.ToBoolean(thirdOffer["Junk"]) },
						new bool[] { Convert.ToBoolean(firstOffer["Await"]), Convert.ToBoolean(secondOffer["Await"]), Convert.ToBoolean(thirdOffer["Await"]) },
						new decimal[] { Convert.ToDecimal(firstOffer["Cost"]), Convert.ToDecimal(secondOffer["Cost"]), Convert.ToDecimal(thirdOffer["Cost"]) },
						new string[] { firstOffer["Cost"].ToString(), secondOffer["Cost"].ToString(), thirdOffer["Cost"].ToString() },        //minCost
						new string[] { activePrice["PriceCode"].ToString(), activePrice["PriceCode"].ToString(), activePrice["PriceCode"].ToString() },  //MinPriceCode
						new string[] { firstOffer["Cost"].ToString(), secondOffer["Cost"].ToString(), thirdOffer["Cost"].ToString() },        //leaderMinCost
						new string[] { firstOffer["RequestRatio"].ToString(), secondOffer["RequestRatio"].ToString(), thirdOffer["RequestRatio"].ToString() },
						new string[] { firstOffer["OrderCost"].ToString(), secondOffer["OrderCost"].ToString(), thirdOffer["OrderCost"].ToString() },
						new string[] { firstOffer["MinOrderCount"].ToString(), secondOffer["MinOrderCount"].ToString(), thirdOffer["MinOrderCount"].ToString() },
						new string[] { activePrice["PriceCode"].ToString(), activePrice["PriceCode"].ToString(), activePrice["PriceCode"].ToString() }  //leaderMinPriceCode
						);
				}

				Assert.That(serverResponse, Is.Not.Null);
				Assert.That(serverResponse, Is.Not.Empty);
				var serverParams = serverResponse.Split(';');
				Assert.That(serverParams[0], Is.StringStarting("OrderId=").IgnoreCase);

				var serverOrderId = serverParams[0].Substring(serverParams[0].IndexOf('=') + 1);
				Assert.That(serverOrderId, Is.Not.Null);
				Assert.That(serverOrderId, Is.Not.Empty);

				var realOrderId = MySqlHelper.ExecuteScalar(connection, "select RowId from orders.ordershead where RowId = " + serverOrderId);
				Assert.That(realOrderId, Is.Not.Null);
				Assert.That(realOrderId.ToString(), Is.Not.Empty);
				Assert.That(realOrderId.ToString(), Is.EqualTo(serverOrderId));

				var insertedCount = MySqlHelper.ExecuteScalar(connection, "select count(*) from orders.ordershead inner join orders.orderslist on orderslist.OrderId = ordershead.RowId where ordershead.RowId = " + serverOrderId);
				Assert.That(insertedCount, Is.EqualTo(3), "Не совпадает кол-во записей в заказе");

				CheckInsertedPosition(connection, serverOrderId, firstOffer);
				CheckInsertedPosition(connection, serverOrderId, secondOffer);
				CheckInsertedPosition(connection, serverOrderId, thirdOffer);
			}
		}

		private void CheckInsertedPosition(MySqlConnection connection, string serverOrderId, DataRow checkedOffer)
		{
			var insertPosition = ExecuteDataRow(
				connection,
				@"
select 
 orderslist.*,
 OrderedOffers.*,
 OrderedOffers.Quantity as CoreQuantity
from 
  orders.orderslist 
  inner join orders.OrderedOffers on OrderedOffers.Id = ordersList.RowId
where
	ordersList.OrderId = ?OrderId
and orderslist.ProductId = ?ProductId
and orderslist.SynonymCode = ?SynonymCode",
				new MySqlParameter("?OrderId", serverOrderId),
				new MySqlParameter("?ProductId", checkedOffer["ProductId"]),
				new MySqlParameter("?SynonymCode", checkedOffer["SynonymCode"]));
			Assert.That(insertPosition, Is.Not.Null);
			ComparePosition(checkedOffer, insertPosition);
		}

		private void ComparePosition(DataRow offer, DataRow insertPosition)
		{
			var compareColumns = new[] { 
				"ProductId", "CodeFirmCr", "SynonymCode", "SynonymFirmCrCode", "Code", "CodeCr", "Junk", "Await", "Cost", 
				"RequestRatio", "OrderCost", "MinOrderCount", "Unit", "Volume", "Note", "Doc", "MinBoundCost", 
				"RegistryCost", "MaxBoundCost", "ProducerCost"};

			foreach (var column in compareColumns)
				Assert.That(insertPosition[column], Is.EqualTo(offer[column]), "Не сопадает значение столбца {0}", column);


			Assert.That(Convert.ToBoolean(insertPosition["VitallyImportant"]), Is.EqualTo(Convert.ToBoolean(offer["VitallyImportant"])), "Не сопадает значение столбца {0}", "VitallyImportant");
		}

		[Test(Description = "Отправляем заказы с указанием отсрочки платежа")]
		public void SendOrdersWithDelays()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var UniqueId = MySqlHelper.ExecuteScalar(connection, "select AFCopyId from usersettings.UserUpdateInfo where UserId = " + _user.Id).ToString();

				string serverResponse;
				using (var prgData = new PrgDataEx())
				{
					serverResponse = prgData.PostSomeOrdersWithDelays(
						UniqueId,
						"7.0.0.1385",
						true,         //ForceSend
						false,        //UseCorrectOrders
						_address.Id, //ClientId => AddressId
						1,            //OrderCount
						new ulong[] { 1L },  //ClientOrderId
						new ulong[] { Convert.ToUInt64(activePrice["PriceCode"]) },
						new ulong[] { Convert.ToUInt64(activePrice["RegionCode"]) },
						new DateTime[] { DateTime.Now }, //PriceDate
						new string[] { "" },             //ClientAddition
						new ushort[] { 1 },              //RowCount
						new string[] { "-10.0" },             //DelayOfPayment
						new ulong[] { 1 },               //ClientPositionId
						new ulong[] { Convert.ToUInt64(firstOffer["Id"].ToString().RightSlice(9)) },  //ClientServerCoreId
						new ulong[] { Convert.ToUInt64(firstOffer["ProductId"]) },
						new string[] { firstOffer["CodeFirmCr"].ToString() },
						new ulong[] { Convert.ToUInt64(firstOffer["SynonymCode"]) },
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
						new string[] { firstOffer["Cost"].ToString() },        //minCost
						new string[] { activePrice["PriceCode"].ToString() },  //MinPriceCode
						new string[] { firstOffer["Cost"].ToString() },        //leaderMinCost
						new string[] { activePrice["PriceCode"].ToString() }, //leaderMinPriceCode
						new string[] { "3.0" },        //SupplierPriceMarkup
						new string[] { firstOffer["Quantity"].ToString() },        //CoreQuantity
						new string[] { firstOffer["Unit"].ToString() },        //Unit
						new string[] { firstOffer["Volume"].ToString() },        //Volume
						new string[] { firstOffer["Note"].ToString() },        //Note
						new string[] { firstOffer["Period"].ToString() },        //Period
						new string[] { firstOffer["Doc"].ToString() },        //Doc
						new string[] { firstOffer["RegistryCost"].ToString() },        //RegistryCost
						new bool[] { Convert.ToBoolean(firstOffer["VitallyImportant"]) },        //VitallyImportant
						new string[] { "" },        //RetailCost
						new string[] { "" },        //ProducerCost
						new string[] { "" },        //NDS
						new string[] { "-20.0" },             //VitallyImportantDelayOfPayment
						new decimal[] { Convert.ToDecimal(firstOffer["Cost"])+ 3 } //CostWithDelayOfPayment
						);
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

				var vitallyImportantDelayOfPayment =
					Convert.ToDecimal(MySqlHelper
										.ExecuteScalar(
											connection,
											@"
select 
  VitallyImportantDelayOfPayment 
from 
  orders.ordershead 
where 
  ordershead.RowId = ?OrderId",
											new MySqlParameter("?OrderId", serverOrderId))
						);

				Assert.That(vitallyImportantDelayOfPayment, Is.EqualTo(-20.0m));

				var orderItem = MySqlHelper.ExecuteDataRow(
					Settings.ConnectionString(),
					@"
select 
  orderslist.* 
from 
  orders.orderslist 
where 
  orderslist.OrderId = ?OrderId
limit 1
"
					,
					new MySqlParameter("?OrderId", serverOrderId));
				Assert.That(Convert.ToDecimal(orderItem["Cost"]), Is.EqualTo(Convert.ToDecimal(firstOffer["Cost"])));
				Assert.That(Convert.ToDecimal(orderItem["CostWithDelayOfPayment"]), Is.EqualTo(Convert.ToDecimal(firstOffer["Cost"]) + 3));
			}
		}

		[Test(Description = "Отправляем заказы без указания отсрочки платежа")]
		public void SendOrdersWithRetailCostAndNullDelayOfPayment()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var serverResponse = PostOrder();

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

				var vitallyImportantDelayOfPayment =
					Convert.ToDecimal(MySqlHelper
										.ExecuteScalar(
											connection,
											@"
select 
  VitallyImportantDelayOfPayment 
from 
  orders.ordershead 
where 
  ordershead.RowId = ?OrderId",
											new MySqlParameter("?OrderId", serverOrderId))
						);

				Assert.That(vitallyImportantDelayOfPayment, Is.EqualTo(0m));

				var orderItem = MySqlHelper.ExecuteDataRow(
					Settings.ConnectionString(),
					@"
select 
  orderslist.* 
from 
  orders.orderslist 
where 
  orderslist.OrderId = ?OrderId
limit 1
"
					,
					new MySqlParameter("?OrderId", serverOrderId));
				Assert.That(Convert.ToDecimal(orderItem["Cost"]), Is.EqualTo(Convert.ToDecimal(firstOffer["Cost"])));
				Assert.That(Convert.ToDecimal(orderItem["CostWithDelayOfPayment"]), Is.EqualTo(Convert.ToDecimal(orderItem["Cost"])));
			}
		}

		[Test(Description = "Отправляем заказы с указанием отсрочки платежа")]
		public void SendOrdersWithRetailCost()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var UniqueId = MySqlHelper.ExecuteScalar(connection, "select AFCopyId from usersettings.UserUpdateInfo where UserId = " + _user.Id).ToString();

				string serverResponse;
				using (var prgData = new PrgDataEx())
				{
					serverResponse = prgData.PostSomeOrdersFullExtend(
						UniqueId,
						"7.0.0.1385",
						true,         //ForceSend
						false,        //UseCorrectOrders
						_address.Id, //ClientId => AddressId
						1,            //OrderCount
						new ulong[] { 1L },  //ClientOrderId
						new ulong[] { Convert.ToUInt64(activePrice["PriceCode"]) },
						new ulong[] { Convert.ToUInt64(activePrice["RegionCode"]) },
						new DateTime[] { DateTime.Now }, //PriceDate
						new string[] { "" },             //ClientAddition
						new ushort[] { 1 },              //RowCount
						new string[] { "-10.0" },             //DelayOfPayment
						new ulong[] { 1 },               //ClientPositionId
						new ulong[] { Convert.ToUInt64(firstOffer["Id"].ToString().RightSlice(9)) },  //ClientServerCoreId
						new ulong[] { Convert.ToUInt64(firstOffer["ProductId"]) },
						new string[] { firstOffer["CodeFirmCr"].ToString() },
						new ulong[] { Convert.ToUInt64(firstOffer["SynonymCode"]) },
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
						new string[] { firstOffer["Cost"].ToString() },        //minCost
						new string[] { activePrice["PriceCode"].ToString() },  //MinPriceCode
						new string[] { firstOffer["Cost"].ToString() },        //leaderMinCost
						new string[] { activePrice["PriceCode"].ToString() }, //leaderMinPriceCode
						new string[] { "3.0" },        //SupplierPriceMarkup
						new string[] { firstOffer["Quantity"].ToString() },        //CoreQuantity
						new string[] { firstOffer["Unit"].ToString() },        //Unit
						new string[] { firstOffer["Volume"].ToString() },        //Volume
						new string[] { firstOffer["Note"].ToString() },        //Note
						new string[] { firstOffer["Period"].ToString() },        //Period
						new string[] { firstOffer["Doc"].ToString() },        //Doc
						new string[] { firstOffer["RegistryCost"].ToString() },        //RegistryCost
						new bool[] { Convert.ToBoolean(firstOffer["VitallyImportant"]) },        //VitallyImportant
						new string[] { "" },        //RetailCost
						new string[] { "" },        //ProducerCost
						new string[] { "" }        //NDS
						);
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

				var vitallyImportantDelayOfPayment =
					Convert.ToDecimal(MySqlHelper
										.ExecuteScalar(
											connection,
											@"
select 
  VitallyImportantDelayOfPayment 
from 
  orders.ordershead 
where 
  ordershead.RowId = ?OrderId",
											new MySqlParameter("?OrderId", serverOrderId))
						);

				Assert.That(vitallyImportantDelayOfPayment, Is.EqualTo(0m));

				var orderItem = MySqlHelper.ExecuteDataRow(
					Settings.ConnectionString(),
					@"
select 
  orderslist.* 
from 
  orders.orderslist 
where 
  orderslist.OrderId = ?OrderId
limit 1
"
					,
					new MySqlParameter("?OrderId", serverOrderId));
				Assert.That(Convert.ToDecimal(orderItem["Cost"]), Is.EqualTo(Convert.ToDecimal(firstOffer["Cost"])));
				Assert.That(
					Convert.ToDecimal(orderItem["CostWithDelayOfPayment"]), 
					Is.EqualTo( Math.Round(Convert.ToDecimal(orderItem["Cost"]) * (1m + -10.0m / 100m), 2)) );
			}
		}

		[Test]
		public void Reject_order_if_group_over_max_orders_sum()
		{
			var maxSum = Convert.ToDecimal(firstOffer["Cost"]) * 150;
			var priceId = Convert.ToUInt32(activePrice["PriceCode"]);
			var supplier = TestPrice.Find(priceId).Supplier;
			var group = new TestRuleGroup();

			using (new TransactionScope()) {
				group.Rules.Add(new TestOrderRule(group, supplier, maxSum));
				group.Save();

				_address.RuleGroup = group;
				_address.Save();
			}

			var error = GetError(PostOrder(100));
			Assert.That(error, Is.Empty);

			InitClient();

			using (new TransactionScope()) {
				_address.RuleGroup = group;
				_address.Save();
			}

			error = GetError(PostOrder(100));

			using(new SessionScope())
			{
				var orderCount = TestOrder.Queryable.Count(o => o.Address == _address);
				Assert.That(orderCount, Is.EqualTo(0));
			}

			Assert.That(error, Is.EqualTo(
				String.Format(
					"Ваша заявка на {0} НЕ Принята, поскольку Сумма заказов в этом месяце по Вашему предприятию на поставщика {0} превысила установленный лимит.",
					supplier.Name)));
		}

		private string GetError(string error)
		{
			var part = error.Split(';')[3];
			return part.Slice(part.IndexOf("=") + 1, -1);
		}

		private string PostOrder(ushort quantity = 1)
		{
			string serverResponse;
			using (var prgData = new PrgDataEx()) {
				serverResponse = prgData.PostSomeOrdersFullExtend(
					UniqueId,
					"7.0.0.1385",
					true, //ForceSend
					false, //UseCorrectOrders
					_address.Id, //ClientId => AddressId
					1, //OrderCount
					new ulong[] {1L}, //ClientOrderId
					new ulong[] {Convert.ToUInt64(activePrice["PriceCode"])},
					new ulong[] {Convert.ToUInt64(activePrice["RegionCode"])},
					new DateTime[] {DateTime.Now}, //PriceDate
					new string[] {""}, //ClientAddition
					new ushort[] {1}, //RowCount
					new string[] {""}, //DelayOfPayment
					new ulong[] {1}, //ClientPositionId
					new ulong[] {Convert.ToUInt64(firstOffer["Id"].ToString().RightSlice(9))}, //ClientServerCoreId
					new ulong[] {Convert.ToUInt64(firstOffer["ProductId"])},
					new string[] {firstOffer["CodeFirmCr"].ToString()},
					new ulong[] {Convert.ToUInt64(firstOffer["SynonymCode"])},
					new string[] {firstOffer["SynonymFirmCrCode"].ToString()},
					new string[] {firstOffer["Code"].ToString()},
					new string[] {firstOffer["CodeCr"].ToString()},
					new bool[] {Convert.ToBoolean(firstOffer["Junk"])},
					new bool[] {Convert.ToBoolean(firstOffer["Await"])},
					new string[] {firstOffer["RequestRatio"].ToString()},
					new string[] {firstOffer["OrderCost"].ToString()},
					new string[] {firstOffer["MinOrderCount"].ToString()},
					new ushort[] {quantity}, //Quantity
					new decimal[] {Convert.ToDecimal(firstOffer["Cost"])},
					new string[] {firstOffer["Cost"].ToString()}, //minCost
					new string[] {activePrice["PriceCode"].ToString()}, //MinPriceCode
					new string[] {firstOffer["Cost"].ToString()}, //leaderMinCost
					new string[] {activePrice["PriceCode"].ToString()}, //leaderMinPriceCode
					new string[] {"3.0"}, //SupplierPriceMarkup
					new string[] {firstOffer["Quantity"].ToString()}, //CoreQuantity
					new string[] {firstOffer["Unit"].ToString()}, //Unit
					new string[] {firstOffer["Volume"].ToString()}, //Volume
					new string[] {firstOffer["Note"].ToString()}, //Note
					new string[] {firstOffer["Period"].ToString()}, //Period
					new string[] {firstOffer["Doc"].ToString()}, //Doc
					new string[] {firstOffer["RegistryCost"].ToString()}, //RegistryCost
					new bool[] {Convert.ToBoolean(firstOffer["VitallyImportant"])}, //VitallyImportant
					new string[] {""}, //RetailCost
					new string[] {""}, //ProducerCost
					new string[] {""} //NDS
					);
			}
			return serverResponse;
		}

		[Test(Description = "Отправляем заказ с групповым контролем суммы заказа с заказом, созданным в начале месяца")]
		public void Reject_order_if_group_over_max_orders_sum_by_FirstDayOfMonth()
		{
			var maxSum = Convert.ToDecimal(firstOffer["Cost"]) * 150;
			var priceId = Convert.ToUInt32(activePrice["PriceCode"]);
			var supplier = TestPrice.Find(priceId).Supplier;
			var group = new TestRuleGroup();

			using (new TransactionScope()) {
				group.Rules.Add(new TestOrderRule(group, supplier, maxSum));
				group.Save();

				_address.RuleGroup = group;
				_address.Save();
			}

			var response = PostOrder(100);

			var error = GetError(response);
			Assert.That(error, Is.Empty);

			var orderId = GetFirstOrderId(response);

			//Дату отправленного заказа перемещаем на начало месяца
			using (new TransactionScope()) {
				var order = TestOrder.Find(orderId);
				order.WriteTime = DateTime.Now.FirstDayOfMonth();

				//Если количество часов у даты больше 1, то уменьшаем дату на один час
				if (order.WriteTime.Hour > 1)
					order.WriteTime = order.WriteTime.AddHours(-1);

				order.Save();
			}

			InitClient();

			using (new TransactionScope()) {
				_address.RuleGroup = group;
				_address.Save();
			}

			error = GetError(PostOrder(51));

			using(new SessionScope())
			{
				var orderCount = TestOrder.Queryable.Count(o => o.Address == _address);
				Assert.That(orderCount, Is.EqualTo(0));
				var lastLog = TestAnalitFUpdateLog.Queryable.Where(l => l.UserId == _user.Id && l.UpdateType == (int)RequestType.SendOrders).OrderByDescending(l => l.Id).FirstOrDefault();
				Assert.That(lastLog, Is.Not.Null);
				//В Addition лога должен быть корректный текст причины отказа
				Assert.That(lastLog.Addition, Is.StringContaining(
					String.Format(
						"Заказ №{0} на сумму {1} на поставщика {2} был отклонен из-за нарушения максимальной суммы заказов",
						1,
						Convert.ToDecimal(firstOffer["Cost"]) * 51,
						supplier.Name)));
			}

			Assert.That(error, Is.EqualTo(
				String.Format(
					"Ваша заявка на {0} НЕ Принята, поскольку Сумма заказов в этом месяце по Вашему предприятию на поставщика {0} превысила установленный лимит.",
					supplier.Name)));
		}

		private uint GetFirstOrderId(string response)
		{
			var serverParams = response.Split(';');
			Assert.That(serverParams.Length, Is.GreaterThanOrEqualTo(3));

			var serverOrderId = serverParams[2].Substring(serverParams[2].IndexOf('=') + 1);
			Assert.That(serverOrderId, Is.Not.Null);
			Assert.That(serverOrderId, Is.Not.Empty);

			return Convert.ToUInt32(serverOrderId);
		}
	}
}
