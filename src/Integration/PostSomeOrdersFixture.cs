using System;
using System.IO;
using System.Linq;
using Castle.ActiveRecord;
using Common.MySql;
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
using MySqlHelper = MySql.Data.MySqlClient.MySqlHelper;

namespace Integration
{
	[TestFixture]
	public class PostSomeOrdersFixture : BaseOrderFixture
	{
		private TestClient _client;
		private TestUser _user;
		private TestAddress _address;

		[SetUp]
		public void Setup()
		{
			InitClient();
			GetOffers(_user);
		}

		private void InitClient()
		{
			_user = CreateUser();
			_client = _user.Client;
			_address = _client.Addresses[0];

			SetCurrentUser(_user.Login);
			CreateFolders(_address.Id.ToString());
		}

		[Test(Description = "Отправляем заказы с указанием отсрочки платежа")]
		public void SendOrdersWithDelays()
		{
			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();

				var UniqueId = MySqlHelper.ExecuteScalar(connection, "select AFCopyId from usersettings.UserUpdateInfo where UserId = " + _user.Id).ToString();

				string serverResponse;
				using (var prgData = new PrgDataEx()) {
					serverResponse = prgData.PostSomeOrdersWithDelays(
						UniqueId,
						"7.0.0.1385",
						true, //ForceSend
						false, //UseCorrectOrders
						_address.Id, //ClientId => AddressId
						1, //OrderCount
						new ulong[] { 1L }, //ClientOrderId
						new ulong[] { Convert.ToUInt64(activePrice["PriceCode"]) },
						new ulong[] { Convert.ToUInt64(activePrice["RegionCode"]) },
						new DateTime[] { DateTime.Now }, //PriceDate
						new string[] { "" }, //ClientAddition
						new ushort[] { 1 }, //RowCount
						new string[] { "-10.0" }, //DelayOfPayment
						new ulong[] { 1 }, //ClientPositionId
						new ulong[] { Convert.ToUInt64(firstOffer["Id"].ToString().RightSlice(9)) }, //ClientServerCoreId
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
						new string[] { firstOffer["Cost"].ToString() }, //minCost
						new string[] { activePrice["PriceCode"].ToString() }, //MinPriceCode
						new string[] { firstOffer["Cost"].ToString() }, //leaderMinCost
						new string[] { activePrice["PriceCode"].ToString() }, //leaderMinPriceCode
						new string[] { "3.0" }, //SupplierPriceMarkup
						new string[] { firstOffer["Quantity"].ToString() }, //CoreQuantity
						new string[] { firstOffer["Unit"].ToString() }, //Unit
						new string[] { firstOffer["Volume"].ToString() }, //Volume
						new string[] { firstOffer["Note"].ToString() }, //Note
						new string[] { firstOffer["Period"].ToString() }, //Period
						new string[] { firstOffer["Doc"].ToString() }, //Doc
						new string[] { firstOffer["RegistryCost"].ToString() }, //RegistryCost
						new bool[] { Convert.ToBoolean(firstOffer["VitallyImportant"]) }, //VitallyImportant
						new string[] { "" }, //RetailCost
						new string[] { "" }, //ProducerCost
						new string[] { "" }, //NDS
						new string[] { "-20.0" }, //VitallyImportantDelayOfPayment
						new decimal[] { Convert.ToDecimal(firstOffer["Cost"]) + 3 }); //CostWithDelayOfPayment
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
							new MySqlParameter("?OrderId", serverOrderId)));

				Assert.That(vitallyImportantDelayOfPayment, Is.EqualTo(-20.0m));

				var orderItem = MySqlHelper.ExecuteDataRow(
					ConnectionHelper.GetConnectionString(),
					@"
select 
  orderslist.* 
from 
  orders.orderslist 
where 
  orderslist.OrderId = ?OrderId
limit 1
",
					new MySqlParameter("?OrderId", serverOrderId));
				Assert.That(Convert.ToDecimal(orderItem["Cost"]), Is.EqualTo(Convert.ToDecimal(firstOffer["Cost"])));
				Assert.That(Convert.ToDecimal(orderItem["CostWithDelayOfPayment"]), Is.EqualTo(Convert.ToDecimal(firstOffer["Cost"]) + 3));
			}
		}

		[Test(Description = "Отправляем заказы без указания отсрочки платежа")]
		public void SendOrdersWithRetailCostAndNullDelayOfPayment()
		{
			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
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
							new MySqlParameter("?OrderId", serverOrderId)));

				Assert.That(vitallyImportantDelayOfPayment, Is.EqualTo(0m));

				var orderItem = MySqlHelper.ExecuteDataRow(
					ConnectionHelper.GetConnectionString(),
					@"
select 
  orderslist.* 
from 
  orders.orderslist 
where 
  orderslist.OrderId = ?OrderId
limit 1
",
					new MySqlParameter("?OrderId", serverOrderId));
				Assert.That(Convert.ToDecimal(orderItem["Cost"]), Is.EqualTo(Convert.ToDecimal(firstOffer["Cost"])));
				Assert.That(Convert.ToDecimal(orderItem["CostWithDelayOfPayment"]), Is.EqualTo(Convert.ToDecimal(orderItem["Cost"])));
			}
		}

		[Test(Description = "Отправляем заказы с указанием отсрочки платежа")]
		public void SendOrdersWithRetailCost()
		{
			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();

				var UniqueId = MySqlHelper.ExecuteScalar(connection, "select AFCopyId from usersettings.UserUpdateInfo where UserId = " + _user.Id).ToString();

				string serverResponse;
				using (var prgData = new PrgDataEx()) {
					serverResponse = prgData.PostSomeOrdersFullExtend(
						UniqueId,
						"7.0.0.1385",
						true, //ForceSend
						false, //UseCorrectOrders
						_address.Id, //ClientId => AddressId
						1, //OrderCount
						new ulong[] { 1L }, //ClientOrderId
						new ulong[] { Convert.ToUInt64(activePrice["PriceCode"]) },
						new ulong[] { Convert.ToUInt64(activePrice["RegionCode"]) },
						new DateTime[] { DateTime.Now }, //PriceDate
						new string[] { "" }, //ClientAddition
						new ushort[] { 1 }, //RowCount
						new string[] { "-10.0" }, //DelayOfPayment
						new ulong[] { 1 }, //ClientPositionId
						new ulong[] { Convert.ToUInt64(firstOffer["Id"].ToString().RightSlice(9)) }, //ClientServerCoreId
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
						new string[] { firstOffer["Cost"].ToString() }, //minCost
						new string[] { activePrice["PriceCode"].ToString() }, //MinPriceCode
						new string[] { firstOffer["Cost"].ToString() }, //leaderMinCost
						new string[] { activePrice["PriceCode"].ToString() }, //leaderMinPriceCode
						new string[] { "3.0" }, //SupplierPriceMarkup
						new string[] { firstOffer["Quantity"].ToString() }, //CoreQuantity
						new string[] { firstOffer["Unit"].ToString() }, //Unit
						new string[] { firstOffer["Volume"].ToString() }, //Volume
						new string[] { firstOffer["Note"].ToString() }, //Note
						new string[] { firstOffer["Period"].ToString() }, //Period
						new string[] { firstOffer["Doc"].ToString() }, //Doc
						new string[] { firstOffer["RegistryCost"].ToString() }, //RegistryCost
						new bool[] { Convert.ToBoolean(firstOffer["VitallyImportant"]) }, //VitallyImportant
						new string[] { "" }, //RetailCost
						new string[] { "" }, //ProducerCost
						new string[] { "" }); //NDS
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
							new MySqlParameter("?OrderId", serverOrderId)));

				Assert.That(vitallyImportantDelayOfPayment, Is.EqualTo(0m));

				var orderItem = MySqlHelper.ExecuteDataRow(
					ConnectionHelper.GetConnectionString(),
					@"
select 
  orderslist.* 
from 
  orders.orderslist 
where 
  orderslist.OrderId = ?OrderId
limit 1
",
					new MySqlParameter("?OrderId", serverOrderId));
				Assert.That(Convert.ToDecimal(orderItem["Cost"]), Is.EqualTo(Convert.ToDecimal(firstOffer["Cost"])));
				Assert.That(
					Convert.ToDecimal(orderItem["CostWithDelayOfPayment"]),
					Is.EqualTo(Math.Round(Convert.ToDecimal(orderItem["Cost"]) * (1m + -10.0m / 100m), 2)));
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

			using (new SessionScope()) {
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

		private string PostOrder(ushort quantity = 1, bool useCorrectOrders = false)
		{
			bool forceSend = !useCorrectOrders;
			string serverResponse;
			using (var prgData = new PrgDataEx()) {
				serverResponse = prgData.PostSomeOrdersFullExtend(
					UniqueId,
					"7.0.0.1385",
					forceSend, //ForceSend
					useCorrectOrders, //UseCorrectOrders
					_address.Id, //ClientId => AddressId
					1, //OrderCount
					new ulong[] { 1L }, //ClientOrderId
					new ulong[] { Convert.ToUInt64(activePrice["PriceCode"]) },
					new ulong[] { Convert.ToUInt64(activePrice["RegionCode"]) },
					new DateTime[] { DateTime.Now }, //PriceDate
					new string[] { "" }, //ClientAddition
					new ushort[] { 1 }, //RowCount
					new string[] { "" }, //DelayOfPayment
					new ulong[] { 1 }, //ClientPositionId
					new ulong[] { Convert.ToUInt64(firstOffer["Id"].ToString().RightSlice(9)) }, //ClientServerCoreId
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
					new ushort[] { quantity }, //Quantity
					new decimal[] { Convert.ToDecimal(firstOffer["Cost"]) },
					new string[] { firstOffer["Cost"].ToString() }, //minCost
					new string[] { activePrice["PriceCode"].ToString() }, //MinPriceCode
					new string[] { firstOffer["Cost"].ToString() }, //leaderMinCost
					new string[] { activePrice["PriceCode"].ToString() }, //leaderMinPriceCode
					new string[] { "3.0" }, //SupplierPriceMarkup
					new string[] { firstOffer["Quantity"].ToString() }, //CoreQuantity
					new string[] { firstOffer["Unit"].ToString() }, //Unit
					new string[] { firstOffer["Volume"].ToString() }, //Volume
					new string[] { firstOffer["Note"].ToString() }, //Note
					new string[] { firstOffer["Period"].ToString() }, //Period
					new string[] { firstOffer["Doc"].ToString() }, //Doc
					new string[] { firstOffer["RegistryCost"].ToString() }, //RegistryCost
					new bool[] { Convert.ToBoolean(firstOffer["VitallyImportant"]) }, //VitallyImportant
					new string[] { "" }, //RetailCost
					new string[] { "" }, //ProducerCost
					new string[] { "" }); //NDS
			}
			return serverResponse;
		}

		[Test]
		public void Post_optimaized_cost_with_offer_check()
		{
			CostOptimizaerConf.MakeUserOptimazible(_user, Convert.ToUInt32(activePrice["FirmCode"]));
			firstOffer["Cost"] = Math.Round(Convert.ToDecimal(firstOffer["Cost"]) * 1.13m, 2);
			var response = PostOrder(useCorrectOrders: true);

			var error = GetError(response);
			var id = GetFirstOrderId(response);
			Assert.That(error, Is.Empty, response);
			Assert.That(id, Is.GreaterThan(0), response);
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

			using (new SessionScope()) {
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