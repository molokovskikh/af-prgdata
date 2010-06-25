using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Castle.ActiveRecord;
using Common.Models;
using Common.Models.Repositories;
using Common.Models.Tests.Repositories;
using Common.Tools;
using MySql.Data.MySqlClient;
using NHibernate;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;


namespace Integration
{
	public class SmartOrderHelperFixture
	{
		TestClient client;
		TestUser user;
		TestAddress address;

		[SetUp]
		public void SetUp()
		{
			Test.Support.Setup.Initialize();
			ContainerInitializer.InitializerContainerForTests();

			using (new TransactionScope())
			{
				client = TestClient.CreateSimple();
				user = client.Users[0];
				address = client.Addresses[0];

				var permission = TestUserPermission.ByShortcut("AF");
				client.Users.Each(u =>
				{
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				user.Update();
			}
		}

		[Test]
		public void Lazy_read()
		{

			User orderable;
			Address realAddress;
			using (var unitOfWork = new UnitOfWork())
			{
				orderable = IoC.Resolve<IRepository<User>>().Get(user.Id);
				NHibernateUtil.Initialize(orderable.AvaliableAddresses);

				realAddress = IoC.Resolve<IRepository<Address>>().Get(address.Id);
				NHibernateUtil.Initialize(realAddress.Users);
			}

			Assert.IsTrue(NHibernateUtil.IsInitialized(orderable), "Пользователь не инициализирован");
			Assert.IsTrue(NHibernateUtil.IsInitialized(realAddress), "Адрес не инициализирован");
			Assert.IsTrue(NHibernateUtil.IsInitialized(orderable.AvaliableAddresses), "Список доступных адресов не инициализирован");
			Assert.IsTrue(NHibernateUtil.IsInitialized(realAddress.Users), "Список пользователей не инициализирован");
			Assert.That(orderable.AvaliableAddresses.Count, Is.GreaterThan(0));
			Assert.IsTrue(NHibernateUtil.IsInitialized(orderable.AvaliableAddresses[0]), "Адрес внутри списка не инициализирован");
			Assert.IsTrue(NHibernateUtil.IsInitialized(orderable.AvaliableAddresses[0].Users), "Пользователь у адреса внутри списка не инициализирован");
			Assert.That(orderable.AvaliableAddresses[0].Users.Count, Is.GreaterThan(0), "Пуст список пользователей внутри адреса, взятого из списка адресов клиента");
		}

		public void SimpleSmart()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				//Пользователь "sergei" - это клиент с кодом 1349, он должен быть старым
				var updateData = UpdateHelper.GetUpdateData(connection, "sergei");
				var orderHelper = new OrderHelper(updateData, connection, connection);

				//var dsPrice = GetActivePrices(connection, updateData);
				//var sendPrice = dsPrice.Tables[0].Rows[0];
				//var orderid = orderHelper.SaveOrder(
				//    updateData.ClientId,
				//    Convert.ToUInt32(sendPrice["PriceCode"]),
				//    Convert.ToUInt64(sendPrice["RegionCode"]),
				//    Convert.ToDateTime(sendPrice["PriceDate"]).ToUniversalTime(),
				//    1,
				//    1,
				//    null);

				//CheckOrder(orderid, connection, () =>
				//{
				//    var dsOrder = MySqlHelper.ExecuteDataset(connection, "select * from orders.OrdersHead where RowId = " + orderid);
				//    var drOrder = dsOrder.Tables[0].Rows[0];
				//    Assert.That(drOrder["PriceDate"], Is.EqualTo(Convert.ToDateTime(sendPrice["PriceDate"])), "не совпадает дата прайс-листа в заказе с датой прайс-листа");
				//});
			}

		}

	}
}
