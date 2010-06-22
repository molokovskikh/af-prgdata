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

		[SetUp]
		public void Setup()
		{
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
					new ulong[] { 1L },
					new ulong[] { 1L },
					new DateTime[] { DateTime.Now },
					new string[] { "" },
					new ushort[] { 1 },
					new ulong[] { 1L },
					new ulong[] { 1L },
					new ulong[] { 14L },
					new string[] { "" },
					new ulong[] { 2472463L },
					new string[] { "" },
					new string[] { "" },
					new string[] { "" },
					new bool[] { false },
					new bool[] { false },
					new string[] { "" },
					new string[] { "" },
					new string[] { "" },
					new ushort[] { 1 },
					new decimal[] { 100m },
					new string[] { "" },
					new string[] { "" },
					new string[] { "" },
					new string[] { "" },
					new string[] { "" },
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
					new ulong[] { 14L, 23L },
					new string[] { "", "" },
					new ulong[] { 2472463L, 582024L },
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
					new ulong[] { 14L, 29L },
					new string[] { "", "" },
					new ulong[] { 2472463L, 1869325L },
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
				var orderHelper = new ReorderHelper(updateData, connection, connection, true, orderedClientId, false);

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

				Console.WriteLine("PostSomeOrders = {0}", result);

				orderHelper = new ReorderHelper(updateData, connection, connection, true, orderedClientId, false);

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

				Console.WriteLine("PostSomeOrders = {0}", result);
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
				var orderHelper = new ReorderHelper(updateData, connection, connection, true, orderedClientId, false);

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

				Console.WriteLine("PostSomeOrders = {0}", result);

				orderHelper = new ReorderHelper(updateData, connection, connection, true, orderedClientId, false);

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


				Console.WriteLine("PostSomeOrders = {0}", result);
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
	}
}
