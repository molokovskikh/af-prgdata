using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Castle.ActiveRecord;
using Common.Tools;
using NUnit.Framework;
using MySql.Data.MySqlClient;
using PrgData.Common;
using System.Data;
using Test.Support;

namespace Integration
{
	[TestFixture]
	public class OrderHelperFixture
	{
		TestClient _client;
		TestUser _user;

		TestOldClient _oldClient;
		TestOldUser _oldUser;

		[SetUp]
		public void SetUp()
		{
			Test.Support.Setup.Initialize();

			using (var transaction = new TransactionScope())
			{
				_client = TestClient.CreateSimple();
				_user = _client.Users[0];

				var permission = TestUserPermission.ByShortcut("AF");
				_client.Users.Each(u =>
				{
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				_user.Update();


				_oldClient = TestOldClient.CreateTestClient();
				_oldUser = _oldClient.Users[0];

				var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof(ActiveRecordBase));
				try
				{
					session.CreateSQLQuery(@"
insert into usersettings.AssignedPermissions (PermissionId, UserId) values (:permissionid, :userid)")
						.SetParameter("permissionid", permission.Id)
						.SetParameter("userid", _oldUser.Id)
						.ExecuteUpdate();
				}
				finally
				{
					ActiveRecordMediator.GetSessionFactoryHolder().ReleaseSession(session);
				}
			}
		}

		private DataSet GetActivePrices(MySqlConnection connection, UpdateData updateData)
		{
			MySqlHelper.ExecuteNonQuery(connection, "drop temporary table if exists Usersettings.Prices");
			MySqlHelper.ExecuteNonQuery(connection, "drop temporary table if exists Usersettings.ActivePrices");
			if (updateData.IsFutureClient)
				MySqlHelper.ExecuteNonQuery(connection, "call future.GetActivePrices(" + updateData.ClientId + ")");
			else
				MySqlHelper.ExecuteNonQuery(connection, "call usersettings.GetActivePrices(" + updateData.ClientId + ")");
			return MySqlHelper.ExecuteDataset(connection, "select * from usersettings.ActivePrices limit 1");
		}

		private void CheckOrder(ulong orderId, MySqlConnection connection, Action action)
		{
			Assert.That(orderId > 0, "не получилось сохранить заказ");
			try
			{
				action();
			}
			finally
			{
				MySqlHelper.ExecuteNonQuery(connection, "delete from orders.OrdersHead where RowId = " + orderId);
			}
		}

		[Test(Description = "отправляем заказы под старым клиентом и проверяем время прайс-листа")]
		public void check_PriceDate_in_order_by_old_client()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				//Пользователь "sergei" - это клиент с кодом 1349, он должен быть старым
				var updateData = UpdateHelper.GetUpdateData(connection, _oldUser.OSUserName);
				var orderHelper = new OrderHelper(updateData, connection);
				var dsPrice = GetActivePrices(connection, updateData);
				var sendPrice = dsPrice.Tables[0].Rows[0];
				var orderid = orderHelper.SaveOrder(
					updateData.ClientId, 
					Convert.ToUInt32(sendPrice["PriceCode"]), 
					Convert.ToUInt64(sendPrice["RegionCode"]), 
					Convert.ToDateTime(sendPrice["PriceDate"]).ToUniversalTime(), 
					1, 
					1, 
					null);

				CheckOrder(orderid, connection, () =>
				{
					var dsOrder = MySqlHelper.ExecuteDataset(connection, "select * from orders.OrdersHead where RowId = " + orderid);
					var drOrder = dsOrder.Tables[0].Rows[0];
					Assert.That(drOrder["PriceDate"], Is.EqualTo(Convert.ToDateTime(sendPrice["PriceDate"])), "не совпадает дата прайс-листа в заказе с датой прайс-листа");
				});
			}
		}

		[Test(Description = "отправляем заказы под клиентом из 'новой реальности' и проверяем время прайс-листа")]
		public void check_PriceDate_in_order_by_future_client()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				//Пользователь "10081" - это пользователь, привязанный к клиенту с кодом 10005, который должен быть клиентом из "Новой реальности"
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var orderHelper = new OrderHelper(updateData, connection);
				var dsPrice = GetActivePrices(connection, updateData);
				var sendPrice = dsPrice.Tables[0].Rows[0];
				var orderid = orderHelper.SaveOrder(
					updateData.ClientId,
					Convert.ToUInt32(sendPrice["PriceCode"]),
					Convert.ToUInt64(sendPrice["RegionCode"]),
					Convert.ToDateTime(sendPrice["PriceDate"]).ToUniversalTime(),
					1,
					1,
					null);

				CheckOrder(orderid, connection, () =>
				{
					var dsOrder = MySqlHelper.ExecuteDataset(connection, "select * from orders.OrdersHead where RowId = " + orderid);
					var drOrder = dsOrder.Tables[0].Rows[0];
					Assert.That(drOrder["PriceDate"], Is.EqualTo(Convert.ToDateTime(sendPrice["PriceDate"])), "не совпадает дата прайс-листа в заказе с датой прайс-листа");
				});
			}
		}

		[Test(Description = "отправляем заказы под клиентом из 'новой реальности' и проверяем корректность установки полей ClientCode, AddressId и UserId")]
		public void check_ClientCode_and_AddressId_in_order_by_future_client()
		{

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				//Пользователь "10081" - это пользователь, привязанный к клиенту с кодом 10005, который должен быть клиентом из "Новой реальности"
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var orderHelper = new OrderHelper(updateData, connection);
				var updateHelper = new UpdateHelper(updateData, connection);

				var clients = MySqlHelper.ExecuteDataset(
					connection, 
					updateHelper.GetClientsCommand(false),
					new MySqlParameter("?OffersRegionCode", updateData.OffersRegionCode),
					new MySqlParameter("?UserId", updateData.UserId));
				Assert.That(clients.Tables.Count, Is.GreaterThan(0), "У пользователя {0} нет привязанных адресов доставки", updateData.UserId);
				Assert.That(clients.Tables[0].Rows.Count, Is.GreaterThan(0), "У пользователя {0} нет привязанных адресов доставки", updateData.UserId);

				//берем последний адрес доставки
				var address = clients.Tables[0].Rows[clients.Tables[0].Rows.Count - 1];

				var dsPrice = GetActivePrices(connection, updateData);
				var sendPrice = dsPrice.Tables[0].Rows[0];
				var orderid = orderHelper.SaveOrder(
					Convert.ToUInt32(address["FirmCode"]),
					Convert.ToUInt32(sendPrice["PriceCode"]),
					Convert.ToUInt64(sendPrice["RegionCode"]),
					Convert.ToDateTime(sendPrice["PriceDate"]).ToUniversalTime(),
					1,
					1,
					null);

				CheckOrder(orderid, connection, () =>
				{
					var dsOrder = MySqlHelper.ExecuteDataset(connection, "select * from orders.OrdersHead where RowId = " + orderid);
					var drOrder = dsOrder.Tables[0].Rows[0];
					Assert.That(drOrder["ClientCode"], Is.EqualTo(updateData.ClientId), "не совпадает код клиента в заказе");
					Assert.That(drOrder["UserId"], Is.EqualTo(updateData.UserId), "не совпадает код пользователя в заказе");
					Assert.That(drOrder["AddressId"], Is.EqualTo(address["FirmCode"]), "не совпадает код адреса доставки в заказе");
				});
			}
		}

		[Test(Description = "отправляем заказы под старым клиентом без подчинений и проверяем корректность установки полей ClientCode, AddressId и UserId")]
		public void check_ClientCode_and_AddressId_in_order_by_old_client_without_subordination()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				//Пользователь "sergei" - это клиент с кодом 1349, он должен быть старым
				var updateData = UpdateHelper.GetUpdateData(connection, _oldUser.OSUserName);
				var orderHelper = new OrderHelper(updateData, connection);
				var updateHelper = new UpdateHelper(updateData, connection);

				var clients = MySqlHelper.ExecuteDataset(
					connection,
					updateHelper.GetClientsCommand(false),
					new MySqlParameter("?OffersRegionCode", updateData.OffersRegionCode),
					new MySqlParameter("?ClientCode", updateData.ClientId));
				Assert.That(clients.Tables.Count, Is.GreaterThan(0), "У пользователя {0} нет клиентов", updateData.UserId);
				Assert.That(clients.Tables[0].Rows.Count, Is.EqualTo(1), "У пользователя {0} кол-ов клиентов должно быть равно 1", updateData.UserId);

				//берем единственного доступного клиента
				var client = clients.Tables[0].Rows[0];

				var dsPrice = GetActivePrices(connection, updateData);
				var sendPrice = dsPrice.Tables[0].Rows[0];
				var orderid = orderHelper.SaveOrder(
					Convert.ToUInt32(client["FirmCode"]),
					Convert.ToUInt32(sendPrice["PriceCode"]),
					Convert.ToUInt64(sendPrice["RegionCode"]),
					Convert.ToDateTime(sendPrice["PriceDate"]).ToUniversalTime(),
					1,
					1,
					null);

				CheckOrder(orderid, connection, () =>
				{
					var dsOrder = MySqlHelper.ExecuteDataset(connection, "select * from orders.OrdersHead where RowId = " + orderid);
					var drOrder = dsOrder.Tables[0].Rows[0];
					Assert.That(drOrder["ClientCode"], Is.EqualTo(client["FirmCode"]), "не совпадает код клиента в заказе");
					Assert.That(drOrder["UserId"], Is.EqualTo(DBNull.Value), "поле код пользователя должно быть null");
					Assert.That(drOrder["AddressId"], Is.EqualTo(DBNull.Value), "поле код адреса доставки должно быть null");
				});
			}
		}

		[Test(Description = "отправляем заказы под старым клиентом c базовым подчинением и проверяем корректность установки полей ClientCode, AddressId и UserId")]
		public void check_ClientCode_and_AddressId_in_order_by_old_client_with_subordination()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				//Пользователь "melehina" - это клиент с кодом 1725, он должен быть старым и у него должны быть клиенты в базовом подчинении
				var updateData = UpdateHelper.GetUpdateData(connection, "melehina");
				var orderHelper = new OrderHelper(updateData, connection);
				var updateHelper = new UpdateHelper(updateData, connection);

				var clients = MySqlHelper.ExecuteDataset(
					connection,
					updateHelper.GetClientsCommand(false),
					new MySqlParameter("?OffersRegionCode", updateData.OffersRegionCode),
					new MySqlParameter("?ClientCode", updateData.ClientId));
				Assert.That(clients.Tables.Count, Is.GreaterThan(0), "У пользователя {0} нет клиентов", updateData.UserId);
				Assert.That(clients.Tables[0].Rows.Count, Is.GreaterThan(1), "У пользователя {0} кол-ов клиентов должно быть больше 1", updateData.UserId);

				//берем последнего клиента
				var client = clients.Tables[0].Rows[clients.Tables[0].Rows.Count-1];

				var dsPrice = GetActivePrices(connection, updateData);
				var sendPrice = dsPrice.Tables[0].Rows[0];
				var orderid = orderHelper.SaveOrder(
					Convert.ToUInt32(client["FirmCode"]),
					Convert.ToUInt32(sendPrice["PriceCode"]),
					Convert.ToUInt64(sendPrice["RegionCode"]),
					Convert.ToDateTime(sendPrice["PriceDate"]).ToUniversalTime(),
					1,
					1,
					null);

				CheckOrder(orderid, connection, () =>
				{
					var dsOrder = MySqlHelper.ExecuteDataset(connection, "select * from orders.OrdersHead where RowId = " + orderid);
					var drOrder = dsOrder.Tables[0].Rows[0];
					Assert.That(drOrder["ClientCode"], Is.EqualTo(client["FirmCode"]), "не совпадает код клиента в заказе");
					Assert.That(drOrder["UserId"], Is.EqualTo(DBNull.Value), "поле код пользователя должно быть null");
					Assert.That(drOrder["AddressId"], Is.EqualTo(DBNull.Value), "поле код адреса доставки должно быть null");
				});
			}
		}

		[Test(Description = "проверяем, что маска регионов загружается из future.Users")]
		public void check_OrderRegions_for_future_client()
		{
			//Пользователь "10081" - это пользователь, привязанный к клиенту с кодом 10005, который должен быть клиентом из "Новой реальности"
			var userName = _user.Login;

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var trans = connection.BeginTransaction();
				try
				{
					var updateData = UpdateHelper.GetUpdateData(connection, userName);

					var command = new MySqlCommand(@"
update usersettings.RetClientsSet set OrderRegionMask = 2 where ClientCode = ?clientId ;
update future.Users set OrderRegionMask = 3 where Id = ?userId ;", connection, trans);
					command.Parameters.AddWithValue("?userId", updateData.UserId);
					command.Parameters.AddWithValue("?clientId", updateData.ClientId);
					command.ExecuteNonQuery();					

					var updateHelper = new UpdateHelper(updateData, connection);

					var clients = MySqlHelper.ExecuteDataset(
						connection,
						updateHelper.GetClientsCommand(false),
						new MySqlParameter("?OffersRegionCode", updateData.OffersRegionCode),
						new MySqlParameter("?UserId", updateData.UserId));

					Assert.AreEqual(2, clients.Tables[0].Rows[0]["OrderRegionMask"], "Не выбрали регион из Users");
				}
				finally
				{
					trans.Rollback();
				}
			}
		}
	}
}
