using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Castle.ActiveRecord;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Tests.Repositories;
using Common.Tools;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using MySql.Data.MySqlClient;
using NHibernate;
using NUnit.Framework;
using PrgData;
using PrgData.Common;
using SmartOrderFactory.Domain;
using SmartOrderFactory.Repositories;
using Test.Support;

namespace Integration
{
	[TestFixture]
	public class ProcedureFixture
	{
		private ISmartOfferRepository repository;
		private TestOldClient testOldClient;
		private TestClient testClient;
		private Client client;
		private User futureUser;
		private Address futureAddress;

		private static bool StopThreads;

		private string UniqueId;

		[SetUp]
		public void SetUp()
		{
			UniqueId = "123";
			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ServiceContext.GetResultPath = () => "results\\";

			Test.Support.Setup.Initialize();
			ContainerInitializer.InitializerContainerForTests(typeof(SmartOrderRule).Assembly);
			IoC.Container.Register(
				Component.For<ISmartOfferRepository>().ImplementedBy<SmartOfferRepository>()
				);

			repository = IoC.Resolve<ISmartOfferRepository>();

			//Это не нужно, т.к. используются только существующие клиенты 10081 и 10068
			//testOldClient = TestOldClient.CreateTestClient();
			//testClient = TestClient.CreateSimple();

			testOldClient = new TestOldClient() { Id = 1349 };
			testClient = new TestClient() { Id = 10005 };

			testClient.Users = new List<TestUser>() { new TestUser() { Id = 10081, Login = "10081"} };
			testClient.Addresses = new List<TestAddress>() { new TestAddress() { Id = 10068 } };

			//Это не нужно
			//using (var unitOfWork = new UnitOfWork())
			//{

			//    NHibernateUtil.Initialize(testOldClient);
			//    NHibernateUtil.Initialize(testClient);
			//    NHibernateUtil.Initialize(testClient.Users);
			//    NHibernateUtil.Initialize(testClient.Addresses);
			//}

			client = new Client { FirmCode = testOldClient.Id };
			futureUser = new User
			             	{
			             		Id = testClient.Users[0].Id,
			             		Login = testClient.Users[0].Login,
			             		Client = new FutureClient {Id = testClient.Id}
			             	};
			futureAddress = new Address { Id = testClient.Addresses[0].Id };
			futureUser.AvaliableAddresses = new List<Address> {futureAddress};
		}

		public static void Execute(string commnad)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
				try
				{

					var command = new MySqlCommand(commnad, connection);
					command.ExecuteNonQuery();

					transaction.Commit();
				}
				catch (Exception)
				{
					if (transaction != null)
						try { transaction.Rollback(); }
						catch { }
					throw;
				}
			}
		}

		[Test]
		public void Get_active_prices()
		{
			Execute(@"
drop temporary table if exists Usersettings.Prices;
drop temporary table if exists Usersettings.ActivePrices;
call future.GetActivePrices(758);");
		}

		[Test]
		public void Get_prices()
		{
			Execute(@"
drop temporary table if exists Usersettings.Prices;
drop temporary table if exists Usersettings.ActivePrices;
call future.GetPrices(10005);");
		}

		[Test]
		public void Get_offers()
		{
			Execute(@"
drop temporary table if exists Usersettings.Prices;
drop temporary table if exists Usersettings.ActivePrices;
call future.GetOffers(10005);");
		}

		public void CallGetOffers()
		{
			Execute(@"
DROP TEMPORARY TABLE IF EXISTS usersettings.Core;
DROP TEMPORARY TABLE IF EXISTS usersettings.MinCosts;
DROP TEMPORARY TABLE IF EXISTS usersettings.Prices;
DROP TEMPORARY TABLE IF EXISTS usersettings.ActivePrices;

#drop temporary table if exists Usersettings.Prices;
#drop temporary table if exists Usersettings.ActivePrices;
call future.GetOffers(10081);");
			Execute(@"
DROP TEMPORARY TABLE IF EXISTS usersettings.Core;
DROP TEMPORARY TABLE IF EXISTS usersettings.MinCosts;
DROP TEMPORARY TABLE IF EXISTS usersettings.Prices;
DROP TEMPORARY TABLE IF EXISTS usersettings.ActivePrices;

#drop temporary table if exists Usersettings.Prices;
#drop temporary table if exists Usersettings.ActivePrices;
call usersettings.GetOffers(1349, 2);");
		}

		[Test, Ignore("Используется для получения ситуации с lock wait")]
		public void Get_deadlock()
		{
			for (int i = 0; i < 10; i++)
			{
				CallGetOffers();
			}
		}

		private void InteralFindAllReducedForSmartOrder(IOrderable orderable, Address address)
		{
			var reducedOffers = repository.FindAllReducedForSmartOrder(orderable, address, new SmartOrderRule(), new OrderRules()).ToList();
			Assert.That(reducedOffers.Count, Is.GreaterThan(0), "Нулевое кол-во предложений");
		}

		public void FindAllReducedForSmartOrder()
		{
			InteralFindAllReducedForSmartOrder(client, null);
		}

		public void FutureFindAllReducedForSmartOrder()
		{
			InteralFindAllReducedForSmartOrder(futureUser, futureAddress);
		}

		[Test, Ignore("Используется для получения ситуации с lock wait")]
		public void Get_deadlock_with_offersrepository()
		{
			for (int i = 0; i < 10; i++)
			{
				FindAllReducedForSmartOrder();
				FutureFindAllReducedForSmartOrder();
			}
		}

		public static void DoWork(object clientId)
		{
			Console.WriteLine("Запущена нитка: {0}", clientId);

			try
			{
				while (!StopThreads)
				{
					Execute(string.Format(@"
DROP TEMPORARY TABLE IF EXISTS usersettings.Core;
DROP TEMPORARY TABLE IF EXISTS usersettings.MinCosts;
DROP TEMPORARY TABLE IF EXISTS usersettings.Prices;
DROP TEMPORARY TABLE IF EXISTS usersettings.ActivePrices;

#drop temporary table if exists Usersettings.Prices;
#drop temporary table if exists Usersettings.ActivePrices;
call usersettings.GetOffers({0}, 0);", clientId));

					Thread.Sleep(5 * 1000);
				}

			}
			catch (Exception exception)
			{
				Console.WriteLine("Error for client {0} : {1}", clientId, exception);
			}

			Console.WriteLine("Остановлена нитка: {0}", clientId);
		}

		public static void DoWorkFactory(object clientId)
		{
			Console.WriteLine("Запущена нитка с factory: {0}", clientId);
			var _repository = IoC.Resolve<ISmartOfferRepository>();
			var _client = new Client {FirmCode = Convert.ToUInt32(clientId)};
			long elapsedMili = 0;
			long count = 0;

			try
			{
				while (!StopThreads)
				{
					var loadWithHiber = Stopwatch.StartNew();
					var reducedOffers = _repository.FindAllReducedForSmartOrder(_client, null, new SmartOrderRule(), new OrderRules()).ToList();
					//Assert.That(reducedOffers.Count, Is.GreaterThan(0), "Нулевое кол-во предложений");
					loadWithHiber.Stop();
					elapsedMili += loadWithHiber.ElapsedMilliseconds;
					count++;

					Thread.Sleep(5 * 1000);
				}

			}
			catch (Exception exception)
			{
				Console.WriteLine("Error for client {0} с factory: {1}", clientId, exception);
				if (count > 0)
					Console.WriteLine("Статистика для клиента {0} : {1}", clientId, elapsedMili / count);
			}

			Console.WriteLine("Остановлена нитка с factory: {0}", clientId);
			if (count > 0)
				Console.WriteLine("Статистика для клиента {0} : {1}", clientId, elapsedMili / count);
		}

		public static void DoWorkLogLockWaits()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				try
				{
					while (!StopThreads)
					{
						var lockCount = Convert.ToInt32( MySqlHelper.ExecuteScalar(connection, "select count(*) from information_schema.INNODB_LOCK_WAITS"));
						if (lockCount > 0)
						{
							var dataDump = MySqlHelper.ExecuteDataset(connection, @"
SELECT * FROM information_schema.INNODB_TRX;
SELECT * FROM information_schema.INNODB_LOCKS;
SELECT * FROM information_schema.INNODB_LOCK_WAITS;
show full processlist;
");
							var writer = new StringWriter();
							dataDump.WriteXml(writer);
							Console.WriteLine("InnoDB dump:\r\n{0}", writer);
						}

						Thread.Sleep(5 * 1000);
					}

				}
				catch (Exception exception)
				{
					Console.WriteLine("Error on log thread", exception);
				}
			}
		}

		[Test, Ignore("Используется для получения ситуации с lock wait")]
		public void Get_deadlock_with_threads()
		{
			StopThreads = false;
			Console.WriteLine("Запуск теста");

			var dataSet = MySqlHelper.ExecuteDataset(Settings.ConnectionString(),
			                           @"
select
#*
ou.RowId as UserId,
rcs.ClientCode
from
  usersettings.OSUserAccessRight ou,
  usersettings.RetClientsSet rcs,
  usersettings.clientsdata cd
where
    rcs.ClientCode = ou.ClientCode
and rcs.ServiceClient = 1
and cd.FirmCode = rcs.ClientCode
and cd.FirmStatus = 1
and cd.BillingStatus = 1
and cd.BillingCode = 921
limit 6;");

			var dataTable = dataSet.Tables[0];
			var threadList = new List<Thread>();

			foreach (DataRow row in dataTable.Rows)
			{
				threadList.Add(new Thread(DoWork));
				threadList[threadList.Count-1].Start(row["ClientCode"]);
			}

			//foreach (DataRow row in dataTable.Rows)
			//{
			//    threadList.Add(new Thread(DoWork));
			//    threadList[threadList.Count - 1].Start(row["ClientCode"]);
			//}

			foreach (DataRow row in dataTable.Rows)
			{
				threadList.Add(new Thread(DoWorkFactory));
				threadList[threadList.Count - 1].Start(row["ClientCode"]);
			}

			//Нитка с дампом
			threadList.Add(new Thread(DoWorkLogLockWaits));
			threadList[threadList.Count - 1].Start();

			Console.WriteLine("Запуск ожидания теста");
			Thread.Sleep(5 * 60 * 1000);

			StopThreads = true;
			Console.WriteLine("Попытка останова ниток");
			threadList.ForEach(item => item.Join());

			Console.WriteLine("Останов теста");
		}

		private TestClient CreateClient()
		{
			using (var transaction = new TransactionScope())
			{

				var permission = TestUserPermission.ByShortcut("AF");

				var createSimple = TestClient.CreateSimple();
				var user = createSimple.Users[0];

				createSimple.Users.Each(u =>
				                  	{
				                  		u.AssignedPermissions.Add(permission);
				                  		u.SendRejects = true;
				                  		u.SendWaybills = true;
				                  	});
				user.Update();

				return createSimple;
			}
		}

		private void TestGetUserData(string appVersion)
		{
			var _client = CreateClient();
			var _user = _client.Users[0];

			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				"update usersettings.RetClientsSet set EnableUpdate = 0 where ClientCode = ?ClientCode",
				new MySqlParameter("?ClientCode", _client.Id));

			SetCurrentUser(_user.Login);

			try
			{
				var memoryAppender = new MemoryAppender();
				BasicConfigurator.Configure(memoryAppender);


				try
				{
					var cumulativeResponse = LoadData(true, DateTime.Now, appVersion);
					var cumulativeUpdateId = ParseUpdateId(cumulativeResponse);
					var cumulativeTime = CommitExchange(cumulativeUpdateId, true);

					var responce = LoadData(false, cumulativeTime, appVersion);
					var simpleUpdateId = ParseUpdateId(responce);
					var simpleTime = CommitExchange(simpleUpdateId, false);
				}
				catch
				{
					var logEvents = memoryAppender.GetEvents();
					Console.WriteLine("Ошибки при подготовке данных:\r\n{0}", logEvents.Select(item =>
					                                                                           	{
																									if (string.IsNullOrEmpty(item.GetExceptionString()))
																										return item.RenderedMessage;
																									else
																										return item.RenderedMessage + Environment.NewLine + item.GetExceptionString();
					                                                                           	}).Implode("\r\n"));
					throw;
				}

				var events = memoryAppender.GetEvents();
				var errors = events.Where(item => item.Level >= Level.Warn);
				Assert.That(errors.Count(), Is.EqualTo(0), "При подготовке данных возникли ошибки:\r\n{0}", errors.Select(item => item.RenderedMessage).Implode("\r\n"));
			}
			finally
			{
				LogManager.ResetConfiguration();
			}
		}

		[Test(Description = "Производим запрос данных для версии 705")]
		public void TestGetUserDataFor705()
		{
			TestGetUserData("1.1.1.705");
		}

		[Test(Description = "Производим запрос данных для версии 1299")]
		public void TestGetUserDataFor1299()
		{
			TestGetUserData("1.1.1.1299");
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}

		private string LoadData(bool getEtalonData, DateTime accessTime, string appVersion)
		{
			var service = new PrgDataEx();
			var responce = service.GetUserData(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", false);

			Assert.That(responce, Is.StringStarting("URL=").IgnoreCase);

			return responce;
		}

		private uint ParseUpdateId(string responce)
		{
			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
				return Convert.ToUInt32(match);

			Assert.Fail("Не найден номер UpdateId в ответе сервера: {0}", responce);
			return 0;
		}

		private DateTime CommitExchange(uint updateId, bool cumulative)
		{
			var service = new PrgDataEx();

			var updateTime = service.CommitExchange(updateId, false);

			var updateRow = MySqlHelper.ExecuteDataRow(
								Settings.ConnectionString(),
				@"
select 
  uui.UpdateDate,
  afu.UpdateType
from 
  logs.AnalitFUpdates afu
  join usersettings.UserUpdateInfo uui on uui.UserId = afu.UserId
where
  afu.UpdateId = ?UpdateId"
				,
				new MySqlParameter("?UpdateId", updateId));
			var dbUpdateTime = Convert.ToDateTime(updateRow["UpdateDate"]);
			var updateType = Convert.ToInt32(updateRow["UpdateType"]);

			if (cumulative)
				Assert.That(updateType, Is.EqualTo((int)RequestType.GetCumulative), "Не совпадает тип обновления");
			else
				Assert.That(updateType, Is.EqualTo((int)RequestType.GetData), "Не совпадает тип обновления");

			Assert.That(updateTime, Is.EqualTo(dbUpdateTime.ToUniversalTime()), "Не совпадает дата обновления, выбранная из базы, для UpdateId: {0}", updateId);

			return updateTime;
		}

		private TestUser CreateUserForAnalitF()
		{
			using (var transaction = new TransactionScope())
			{
				var client = TestClient.CreateSimple();
				var user = client.Users[0];

				var permission = TestUserPermission.ByShortcut("AF");
				client.Users.Each(u =>
				                   	{
				                   		u.AssignedPermissions.Add(permission);
				                   		u.SendRejects = true;
				                   		u.SendWaybills = true;
				                   	});
				user.Update();

				return user;
			}
		}

		[Test(Description = "При несуществовании таблицы CurrentReplicationInfo должно вызываться исключение")]
		public void GetActivePricesWithoutCurrentReplicationInfo()
		{
			var testUser = CreateUserForAnalitF();

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, testUser.Login);
				var helper = new UpdateHelper(updateData, connection);

				helper.Cleanup();

				try
				{
					helper.SelectActivePrices();

					Assert.Fail("В предыдущем операторе должно быть вызвано исключение, т.к. таблицы CurrentReplicationInfo не существует");
				}
				catch (MySqlException mySqlException)
				{
					Assert.That(mySqlException.Number, Is.EqualTo(1146), "Неожидаемое исключение: {0}", mySqlException);
					Assert.That(mySqlException.Message, Is.EqualTo("Table 'usersettings.currentreplicationinfo' doesn't exist").IgnoreCase, "Неожидаемое исключение: {0}", mySqlException);
				}
			}
		}

		[Test(Description = "При несуществовании таблицы CurrentReplicationInfo должно вызываться исключение")]
		public void GetActivePricesWithCurrentReplicationInfo()
		{
			var testUser = CreateUserForAnalitF();

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, testUser.Login);
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				var SelProc = new MySqlCommand();
				SelProc.Connection = connection;

				helper.SetUpdateParameters(SelProc, false, DateTime.Now.AddHours(-1), DateTime.Now);

				helper.Cleanup();

				helper.SelectPrices();
				helper.PreparePricesData(SelProc);
				helper.SelectReplicationInfo();
				helper.SelectActivePrices();

				var activePriceCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					connection,
					"select count(*) from ActivePrices"));

				Assert.That(activePriceCount, Is.GreaterThan(0), "Для вновь созданного пользователя обязательно должны существовать активные прайс-листы");
			}
		}
	}
}
