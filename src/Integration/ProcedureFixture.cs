using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Web;
using Castle.ActiveRecord;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Tests.Repositories;
using Common.Tools;
using Integration.BaseTests;
using Test.Support.Logs;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Filter;
using MySql.Data.MySqlClient;
using NHibernate;
using NUnit.Framework;
using PrgData;
using PrgData.Common;
using PrgData.Common.Models;
using PrgData.Common.Repositories;
using PrgData.FileHandlers;
using SmartOrderFactory.Domain;
using SmartOrderFactory.Repositories;
using Test.Support;
using Test.Support.Helpers;
using Test.Support.Suppliers;

namespace Integration
{
	[TestFixture]
	public class ProcedureFixture : PrepareDataFixture
	{
		private ISmartOfferRepository repository;
		private TestClient testClient;
		private User futureUser;
		private Address futureAddress;

		private static bool StopThreads;

		[SetUp]
		public void Setup()
		{
			repository = IoC.Resolve<ISmartOfferRepository>();

			testClient = new TestClient() { Id = 10005 };

			testClient.Users = new List<TestUser>() { new TestUser() { Id = 10081, Login = "10081" } };
			testClient.Addresses = new List<TestAddress>() { new TestAddress() { Id = 10068 } };

			futureUser = new User {
				Id = testClient.Users[0].Id,
				Login = testClient.Users[0].Login,
				Client = new Client { Id = testClient.Id }
			};
			futureAddress = new Address { Id = testClient.Addresses[0].Id };
			futureUser.AvaliableAddresses = new List<Address> { futureAddress };
		}

		public static void Execute(string commnad)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString())) {
				connection.Open();
				var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
				try {
					var command = new MySqlCommand(commnad, connection);
					command.ExecuteNonQuery();

					transaction.Commit();
				}
				catch (Exception) {
					if (transaction != null)
						try {
							transaction.Rollback();
						}
						catch {
						}
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
call Customers.GetActivePrices(758);");
		}

		[Test]
		public void Get_prices()
		{
			Execute(@"
drop temporary table if exists Usersettings.Prices;
drop temporary table if exists Usersettings.ActivePrices;
call Customers.GetPrices(10005);");
		}

		[Test]
		public void Get_offers()
		{
			Execute(@"
drop temporary table if exists Usersettings.Prices;
drop temporary table if exists Usersettings.ActivePrices;
call Customers.GetOffers(10005);");
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
call Customers.GetOffers(10081);");
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
			for (int i = 0; i < 10; i++) {
				CallGetOffers();
			}
		}

		private void InteralFindAllReducedForSmartOrder(User user, Address address)
		{
			var reducedOffers = repository.FindAllReducedForSmartOrder(user, address, new SmartOrderRule(), new OrderRules()).ToList();
			Assert.That(reducedOffers.Count, Is.GreaterThan(0), "Нулевое кол-во предложений");
		}

		public void FutureFindAllReducedForSmartOrder()
		{
			InteralFindAllReducedForSmartOrder(futureUser, futureAddress);
		}

		[Test, Ignore("Используется для получения ситуации с lock wait")]
		public void Get_deadlock_with_offersrepository()
		{
			for (int i = 0; i < 10; i++) {
				FutureFindAllReducedForSmartOrder();
			}
		}

		public static void DoWork(object clientId)
		{
			Console.WriteLine("Запущена нитка: {0}", clientId);

			try {
				while (!StopThreads) {
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
			catch (Exception exception) {
				Console.WriteLine("Error for client {0} : {1}", clientId, exception);
			}

			Console.WriteLine("Остановлена нитка: {0}", clientId);
		}

		public static void DoWorkFactory(object userId)
		{
			Console.WriteLine("Запущена нитка с factory: {0}", userId);
			var repository = IoC.Resolve<ISmartOfferRepository>();
			long elapsedMili = 0;
			long count = 0;

			try {
				while (!StopThreads) {
					var loadWithHiber = Stopwatch.StartNew();
					User user;
					Address address;
					using (var unit = new UnitOfWork()) {
						user = unit.CurrentSession.Get<User>(userId);
						if (user.AvaliableAddresses.Count == 0)
							return;
						address = user.AvaliableAddresses.First();
					}
					var reducedOffers = repository.FindAllReducedForSmartOrder(user, address, new SmartOrderRule(), new OrderRules()).ToList();
					loadWithHiber.Stop();
					elapsedMili += loadWithHiber.ElapsedMilliseconds;
					count++;

					Thread.Sleep(5 * 1000);
				}
			}
			catch (Exception exception) {
				Console.WriteLine("Error for client {0} с factory: {1}", userId, exception);
				if (count > 0)
					Console.WriteLine("Статистика для клиента {0} : {1}", userId, elapsedMili / count);
			}

			Console.WriteLine("Остановлена нитка с factory: {0}", userId);
			if (count > 0)
				Console.WriteLine("Статистика для клиента {0} : {1}", userId, elapsedMili / count);
		}

		public static void DoWorkLogLockWaits()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString())) {
				connection.Open();
				try {
					while (!StopThreads) {
						var lockCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(connection, "select count(*) from information_schema.INNODB_LOCK_WAITS"));
						if (lockCount > 0) {
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
				catch (Exception exception) {
					Console.WriteLine("Error on log thread", exception);
				}
			}
		}

		[Test, Ignore("Используется для получения ситуации с lock wait")]
		public void Get_deadlock_with_threads()
		{
			StopThreads = false;
			Console.WriteLine("Запуск теста");

			uint[] userIds;
			using (new SessionScope()) {
				userIds = TestUser.Queryable.Where(u => u.Enabled && !u.RootService.Disabled
					&& u.RootService.Type == ServiceType.Drugstore
					&& u.Client.Settings.ServiceClient
					&& u.Payer.Id == 921u)
					.Select(u => u.Id)
					.ToArray();
			}

			var threadList = new List<Thread>();

			foreach (var id in userIds) {
				threadList.Add(new Thread(DoWork));
				threadList[threadList.Count - 1].Start(id);
			}
			foreach (var id in userIds) {
				threadList.Add(new Thread(DoWorkFactory));
				threadList[threadList.Count - 1].Start(id);
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
			var createSimple = TestClient.Create();

			using (var transaction = new TransactionScope()) {
				var user = createSimple.Users[0];

				createSimple.Users.Each(u => {
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				user.Update();
			}

			return createSimple;
		}

		private void TestGetUserData(string appVersion)
		{
			var _client = CreateClient();
			var _user = _client.Users[0];

			SetCurrentUser(_user.Login);

			try {
				var memoryAppender = new MemoryAppender();
				memoryAppender.AddFilter(new LoggerMatchFilter { AcceptOnMatch = true, LoggerToMatch = "PrgData", Next = new DenyAllFilter() });
				BasicConfigurator.Configure(memoryAppender);


				try {
					var cumulativeResponse = LoadData(true, DateTime.Now, appVersion);
					var cumulativeUpdateId = ParseUpdateId(cumulativeResponse);
					ProcessFileHandler(cumulativeUpdateId);
					var cumulativeTime = CommitExchange(cumulativeUpdateId, RequestType.GetCumulative);

					var responce = LoadData(false, cumulativeTime, appVersion);
					var simpleUpdateId = ParseUpdateId(responce);
					ProcessFileHandler(simpleUpdateId);
					var simpleTime = CommitExchange(simpleUpdateId, RequestType.GetData);
				}
				catch {
					var logEvents = memoryAppender.GetEvents();
					Console.WriteLine("Ошибки при подготовке данных:\r\n{0}", logEvents.Select(item => {
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
			finally {
				LogManager.ResetConfiguration();
			}
		}

		[Test(Description = "Производим запрос данных для версии 1299")]
		public void TestGetUserDataFor1299()
		{
			TestGetUserData("1.1.1.1299");
		}

		[Test(Description = "Производим запрос данных для версии 1510, чтобы проверить механизм выгрузки расписаний обновлений")]
		public void TestGetUserDataFor1510()
		{
			TestGetUserData("1.1.1.1510");
		}

		private string PostOrderBatch(bool getEtalonData, DateTime accessTime, string appVersion, uint adresssId, string batchFileName)
		{
			var service = new PrgDataEx();
			var responce = service.PostOrderBatch(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", new uint[] { }, adresssId, batchFileName, 1, 1, 1);

			Assert.That(responce, Is.StringStarting("URL=").IgnoreCase);

			return responce;
		}

		private void ProcessFileHandler(uint updateId)
		{
			var fileName = "GetFileHandler.ashx";

			var output = new StringBuilder();
			using (var sw = new StringWriter(output)) {
				var response = new HttpResponse(sw);
				var request = new HttpRequest(fileName, UpdateHelper.GetDownloadUrl() + fileName, "Id=" + updateId);
				var context = new HttpContext(request, response);

				var fileHandler = new GetFileHandler();
				fileHandler.ProcessRequest(context);

				Assert.That(response.StatusCode, Is.EqualTo(200), "Неожидаемый код ответа от сервера для обновления: {0}", updateId);

				Assert.That(context.Error, Is.Null);
			}
		}

		private void ConfirmUserMessage(uint userId, string appVersion, string confirmedMessage)
		{
			var maxUpdateId = Convert.ToInt32(MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				"select max(UpdateId) from logs.AnalitFUpdates where UserId = ?UserId",
				new MySqlParameter("?UserId", userId)));

			var service = new PrgDataEx();

			var responce = service.ConfirmUserMessage(appVersion, UniqueId, confirmedMessage);

			Assert.That(responce, Is.EqualTo("Res=Ok").IgnoreCase, "Неожидаемый ответ от сервера");

			var logsAfterConfirm = Convert.ToInt32(MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				"select count(*) from logs.AnalitFUpdates where UserId = ?UserId and UpdateId > ?MaxUpdateId",
				new MySqlParameter("?UserId", userId),
				new MySqlParameter("?MaxUpdateId", maxUpdateId)));
			Assert.That(logsAfterConfirm, Is.EqualTo(1), "Должно быть одно логирующее сообщение с подтверждением");

			var confirmLog = MySqlHelper.ExecuteDataRow(
				Settings.ConnectionString(),
				"select * from logs.AnalitFUpdates where UserId = ?UserId order by UpdateId desc limit 1",
				new MySqlParameter("?UserId", userId));
			Assert.That(confirmLog["UpdateType"], Is.EqualTo((int)RequestType.ConfirmUserMessage), "Не совпадает тип обновления");
			Assert.That(confirmLog["Addition"], Is.EqualTo(confirmedMessage), "Не совпадает значение поля Addition");
		}

		private TestUser CreateUserForAnalitF()
		{
			var client = TestClient.Create();
			using (var transaction = new TransactionScope()) {
				var user = client.Users[0];

				client.Users.Each(u => {
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

			using (var connection = new MySqlConnection(Settings.ConnectionString())) {
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, testUser.Login);
				var helper = new UpdateHelper(updateData, connection);

				helper.Cleanup();

				try {
					helper.SelectActivePrices();

					Assert.Fail("В предыдущем операторе должно быть вызвано исключение, т.к. таблицы CurrentReplicationInfo не существует");
				}
				catch (MySqlException mySqlException) {
					Assert.That(mySqlException.Number, Is.EqualTo(1146), "Неожидаемое исключение: {0}", mySqlException);
					Assert.That(mySqlException.Message, Is.EqualTo("Table 'usersettings.currentreplicationinfo' doesn't exist").IgnoreCase, "Неожидаемое исключение: {0}", mySqlException);
				}
			}
		}

		[Test(Description = "Проверяем нормальную работу с CurrentReplicationInfo - все прайс-листы должны быть свежими")]
		public void GetActivePricesWithCurrentReplicationInfo()
		{
			var testUser = CreateUserForAnalitF();

			using (var connection = new MySqlConnection(Settings.ConnectionString())) {
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, testUser.Login);
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				var SelProc = new MySqlCommand();
				SelProc.Connection = connection;

				updateData.OldUpdateTime = DateTime.Now.AddHours(-1);
				helper.SetUpdateParameters(SelProc, DateTime.Now);

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

		[Test(Description = "Производим запрос данных кумулятивного обновления после неподтвержденного накопительного")]
		public void GetCumulativeAfterSimple()
		{
			var appVersion = "1.1.1.1299";
			var _client = CreateClient();
			var _user = _client.Users[0];

			var simpleUpdateTime = GetLastUpdateTime(_user);

			SetCurrentUser(_user.Login);

			ProcessWithLog(() => {
				var responce = LoadData(false, simpleUpdateTime.ToUniversalTime(), appVersion);
				var simpleUpdateId = ParseUpdateId(responce);

				var requestType = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select UpdateType from logs.AnalitFUpdates where UpdateId = ?UpdateId",
					new MySqlParameter("?UpdateId", simpleUpdateId)));
				Assert.That(requestType, Is.EqualTo((int)RequestType.GetData), "Неожидаемый тип обновления: должно быть накопительное");

				var afterSimpleFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_*.zip".Format(_user.Id));
				Assert.That(afterSimpleFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterSimpleFiles.Implode());
				Assert.That(afterSimpleFiles[0], Is.StringEnding("{0}_{1}.zip".Format(_user.Id, simpleUpdateId)));

				var cumulativeResponse = LoadData(true, DateTime.Now, appVersion);
				var cumulativeUpdateId = ParseUpdateId(cumulativeResponse);

				Assert.That(cumulativeUpdateId, Is.Not.EqualTo(simpleUpdateId));

				var afterCumulativeFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_*.zip".Format(_user.Id));
				Assert.That(afterCumulativeFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterCumulativeFiles.Implode());
				Assert.That(afterCumulativeFiles[0], Is.StringEnding("{0}_{1}.zip".Format(_user.Id, cumulativeUpdateId)));
			});
		}

		[Test(Description = "Производим запрос данных кумулятивного обновления после неподтвержденного кумулятивного")]
		public void GetResumeDataAfterCumulative()
		{
			var appVersion = "1.1.1.1299";
			var _client = CreateClient();
			var _user = _client.Users[0];

			var simpleUpdateTime = GetLastUpdateTime(_user);

			SetCurrentUser(_user.Login);

			ProcessWithLog(() => {
				var responce = LoadData(true, simpleUpdateTime.ToUniversalTime(), appVersion);
				var firstCumulativeId = ParseUpdateId(responce);

				var requestType = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select UpdateType from logs.AnalitFUpdates where UpdateId = ?UpdateId",
					new MySqlParameter("?UpdateId", firstCumulativeId)));
				Assert.That(requestType, Is.EqualTo((int)RequestType.GetCumulative), "Неожидаемый тип обновления: должно быть кумулятивное");

				var afterFirstFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_*.zip".Format(_user.Id));
				Assert.That(afterFirstFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterFirstFiles.Implode());
				Assert.That(afterFirstFiles[0], Is.StringEnding("{0}_{1}.zip".Format(_user.Id, firstCumulativeId)));

				var cumulativeResponse = LoadData(true, DateTime.Now, appVersion);
				var cumulativeUpdateId = ParseUpdateId(cumulativeResponse);

				Assert.That(cumulativeUpdateId, Is.EqualTo(firstCumulativeId));

				var afterCumulativeFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_*.zip".Format(_user.Id));
				Assert.That(afterCumulativeFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterCumulativeFiles.Implode());
				Assert.That(afterCumulativeFiles[0], Is.StringEnding("{0}_{1}.zip".Format(_user.Id, cumulativeUpdateId)));
			});
		}

		[Test(Description = "Проверка старого механизма передачи пользовательского сообщения")]
		public void CheckOldTransmitUserMessage()
		{
			var appVersion = "1.1.1.1299";
			var _client = CreateClient();
			var _user = _client.Users[0];

			var userMessage = "test User Message 123";

			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				@"
update usersettings.UserUpdateInfo set Message = ?Message, MessageShowCount = 1 where UserId = ?UserId
",
				new MySqlParameter("?Message", userMessage),
				new MySqlParameter("?UserId", _user.Id));

			SetCurrentUser(_user.Login);

			ProcessWithLog(() => {
				var responce = LoadData(false, DateTime.Now, appVersion);
				var updateId = ParseUpdateId(responce);

				var messageStart = "Addition=";
				var index = responce.IndexOf(messageStart);
				Assert.That(index, Is.GreaterThan(0), "Не найден блок сообщения в ответе сервера: {0}", responce);

				var realMessage = responce.Substring(index + messageStart.Length);
				Assert.That(realMessage, Is.EqualTo(userMessage), "Не совпадает сообщение в ответе сервера: {0}", responce);

				var messageShowCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select MessageShowCount from usersettings.UserUpdateInfo where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id)));
				Assert.That(messageShowCount, Is.EqualTo(1), "Сообщение не должно быть подтверждено");

				CommitExchange(updateId, RequestType.GetCumulative);

				messageShowCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select MessageShowCount from usersettings.UserUpdateInfo where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id)));
				Assert.That(messageShowCount, Is.EqualTo(0), "Сообщение должно быть подтверждено");
			});
		}

		[Test(Description = "Проверка простого подтверждения пользовательского сообщения")]
		public void CheckSimpleConfirmUserMessage()
		{
			var appVersion = "1.1.1.1300";
			var _client = CreateClient();
			var _user = _client.Users[0];

			var userMessage = "test User Message 123";

			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				@"
update usersettings.UserUpdateInfo set Message = ?Message, MessageShowCount = 1 where UserId = ?UserId
",
				new MySqlParameter("?Message", userMessage),
				new MySqlParameter("?UserId", _user.Id));

			SetCurrentUser(_user.Login);

			ProcessWithLog(() => {
				var responce = LoadData(false, DateTime.Now, appVersion);
				var updateId = ParseUpdateId(responce);

				var messageStart = "Addition=";
				var index = responce.IndexOf(messageStart);
				Assert.That(index, Is.GreaterThan(0), "Не найден блок сообщения в ответе сервера: {0}", responce);

				var realMessage = responce.Substring(index + messageStart.Length);
				Assert.That(realMessage, Is.EqualTo(userMessage), "Не совпадает сообщение в ответе сервера: {0}", responce);

				var messageShowCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select MessageShowCount from usersettings.UserUpdateInfo where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id)));
				Assert.That(messageShowCount, Is.EqualTo(1), "Сообщение не должно быть подтверждено");

				CommitExchange(updateId, RequestType.GetCumulative);

				messageShowCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select MessageShowCount from usersettings.UserUpdateInfo where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id)));
				Assert.That(messageShowCount, Is.EqualTo(1), "Сообщение не должно быть подтверждено");

				ConfirmUserMessage(_user.Id, appVersion, realMessage);

				messageShowCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select MessageShowCount from usersettings.UserUpdateInfo where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id)));
				Assert.That(messageShowCount, Is.EqualTo(0), "Сообщение должно быть подтверждено");
			});
		}

		[Test(Description = "Проверка подтверждения пользовательского сообщения после изменения сообщения из билинга")]
		public void CheckConfirmUserMessageAfterChange()
		{
			var appVersion = "1.1.1.1300";
			var _client = CreateClient();
			var _user = _client.Users[0];

			var userMessage = "test User Message 123";

			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				@"
update usersettings.UserUpdateInfo set Message = ?Message, MessageShowCount = 1 where UserId = ?UserId
",
				new MySqlParameter("?Message", userMessage),
				new MySqlParameter("?UserId", _user.Id));

			SetCurrentUser(_user.Login);

			ProcessWithLog(() => {
				var responce = LoadData(false, DateTime.Now, appVersion);
				var updateId = ParseUpdateId(responce);

				var messageStart = "Addition=";
				var index = responce.IndexOf(messageStart);
				Assert.That(index, Is.GreaterThan(0), "Не найден блок сообщения в ответе сервера: {0}", responce);

				var realMessage = responce.Substring(index + messageStart.Length);
				Assert.That(realMessage, Is.EqualTo(userMessage), "Не совпадает сообщение в ответе сервера: {0}", responce);

				var messageShowCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select MessageShowCount from usersettings.UserUpdateInfo where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id)));
				Assert.That(messageShowCount, Is.EqualTo(1), "Сообщение не должно быть подтверждено");

				CommitExchange(updateId, RequestType.GetCumulative);

				messageShowCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select MessageShowCount from usersettings.UserUpdateInfo where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id)));
				Assert.That(messageShowCount, Is.EqualTo(1), "Сообщение не должно быть подтверждено");

				MySqlHelper.ExecuteNonQuery(
					Settings.ConnectionString(),
					@"
update usersettings.UserUpdateInfo set Message = ?Message, MessageShowCount = 1 where UserId = ?UserId
",
					new MySqlParameter("?Message", "это новый текст пользовательского сообщения"),
					new MySqlParameter("?UserId", _user.Id));

				ConfirmUserMessage(_user.Id, appVersion, realMessage);

				messageShowCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select MessageShowCount from usersettings.UserUpdateInfo where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id)));
				Assert.That(messageShowCount, Is.EqualTo(1), "Сообщение не должно быть подтверждено, т.к. текст сообщения уже другой");
			});
		}

		[Test(Description = "Проверка подтверждения пользовательского сообщения, которое содежит пробемы и переводы строк в начале и в конце")]
		public void CheckConfirmUserMessageWithSpaces()
		{
			var appVersion = "1.1.1.1300";
			var _client = CreateClient();
			var _user = _client.Users[0];

			var userMessage = "   test User Message это сообщение 123  \r\n   ";

			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				@"
update usersettings.UserUpdateInfo set Message = ?Message, MessageShowCount = 1 where UserId = ?UserId
",
				new MySqlParameter("?Message", userMessage),
				new MySqlParameter("?UserId", _user.Id));

			SetCurrentUser(_user.Login);

			ProcessWithLog(() => {
				var responce = LoadData(false, DateTime.Now, appVersion);
				var updateId = ParseUpdateId(responce);

				var messageStart = "Addition=";
				var index = responce.IndexOf(messageStart);
				Assert.That(index, Is.GreaterThan(0), "Не найден блок сообщения в ответе сервера: {0}", responce);

				var realMessage = responce.Substring(index + messageStart.Length);
				Assert.That(realMessage, Is.EqualTo(userMessage.Trim()), "Не совпадает сообщение в ответе сервера: {0}", responce);

				//Обрезаем сообщение, т.к. оно тоже обрезается при сохранении в базу в AnalitF
				realMessage = realMessage.Trim();

				var messageShowCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select MessageShowCount from usersettings.UserUpdateInfo where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id)));
				Assert.That(messageShowCount, Is.EqualTo(1), "Сообщение не должно быть подтверждено");

				CommitExchange(updateId, RequestType.GetCumulative);

				messageShowCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select MessageShowCount from usersettings.UserUpdateInfo where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id)));
				Assert.That(messageShowCount, Is.EqualTo(1), "Сообщение не должно быть подтверждено");

				ConfirmUserMessage(_user.Id, appVersion, realMessage);

				messageShowCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select MessageShowCount from usersettings.UserUpdateInfo where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id)));
				Assert.That(messageShowCount, Is.EqualTo(0), "Сообщение должно быть подтверждено");
			});
		}

		private void CheckConfirmUserMessage(MySqlConnection connection, uint userId, string login, string dbMessage, string fromUserMessage)
		{
			MySqlHelper.ExecuteNonQuery(
				connection,
				@"
update usersettings.UserUpdateInfo set Message = ?Message, MessageShowCount = 1 where UserId = ?UserId
",
				new MySqlParameter("?Message", dbMessage),
				new MySqlParameter("?UserId", userId));

			var updateData = UpdateHelper.GetUpdateData(connection, login);
			var helper = new UpdateHelper(updateData, connection);

			helper.ConfirmUserMessage(fromUserMessage);

			var messageShowCount = Convert.ToInt32(
				MySqlHelper.ExecuteScalar(
					connection,
					@"
select MessageShowCount from usersettings.UserUpdateInfo where UserId = ?UserId",
					new MySqlParameter("?UserId", userId)));
			Assert.That(messageShowCount, Is.EqualTo(0), "Неподтвердились сообщения\r\nв базе:{0}\r\nот пользователя:{1}", dbMessage, fromUserMessage);
		}

		[Test(Description = "надо потверждать пользовательское сообщение по совпадению начала строк")]
		public void ConfirmUserMessageByMatchingStringStarting()
		{
			var _client = CreateClient();
			var _user = _client.Users[0];

			using (var connection = new MySqlConnection(Settings.ConnectionString())) {
				connection.Open();

				CheckConfirmUserMessage(connection, _user.Id, _user.Login, "aaa", "aaa");

				CheckConfirmUserMessage(connection, _user.Id, _user.Login, new string('a', 250), new string('a', 250));

				CheckConfirmUserMessage(connection, _user.Id, _user.Login, new string('a', 250) + new string('b', 5), new string('a', 250) + new string('b', 5));

				CheckConfirmUserMessage(connection, _user.Id, _user.Login, new string('a', 250) + new string('b', 5) + "c", new string('a', 250) + new string('b', 5) + "d");

				CheckConfirmUserMessage(connection, _user.Id, _user.Login, new string('a', 250) + new string('b', 5) + new string('m', 10), new string('a', 250) + new string('b', 5) + new string('l', 10));
			}
		}

		private string GetTestFileName(string fileName)
		{
			return Path.Combine(ServiceContext.GetResultPath(), fileName);
		}

		private void CreateTestFile(string fileName)
		{
			File.WriteAllText(GetTestFileName(fileName), "this is test file");
		}

		[Test(Description = "Проверяем метод PrgDataEx.DeletePreviousFiles, чтобы он удалял только необходимые файлы")]
		public void TestDeletePreviousFiles()
		{
			var _client = CreateClient();
			var _user = _client.Users[0];

			SetCurrentUser(_user.Login);

			ProcessWithLog(memoryAppender => {
				Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_*.zip".Format(_user.Id)).ToList().ForEach(File.Delete);

				CreateTestFile("{0}3_dsds.zip".Format(_user.Id));
				CreateTestFile("{0}6.zip".Format(_user.Id));
				CreateTestFile("{0}.zip".Format(_user.Id));
				CreateTestFile("r{0}.zip".Format(_user.Id));
				CreateTestFile("{0}d.zip".Format(_user.Id));
				CreateTestFile("{0}_.txt".Format(_user.Id));

				CreateTestFile("{0}_.zip".Format(_user.Id));
				CreateTestFile("{0}_mm.zip".Format(_user.Id));
				CreateTestFile("{0}_203.zip".Format(_user.Id));

				var service = new PrgDataEx();

				service.SendClientLog(1, null);

				var methodDeletePreviousFiles = service.GetType().GetMethod("DeletePreviousFiles",
					BindingFlags.NonPublic | BindingFlags.Instance);

				methodDeletePreviousFiles.Invoke(service, new object[] { });

				Assert.That(File.Exists(GetTestFileName("{0}3_dsds.zip".Format(_user.Id))));
				Assert.That(File.Exists(GetTestFileName("{0}6.zip".Format(_user.Id))));
				Assert.That(File.Exists(GetTestFileName("{0}.zip".Format(_user.Id))));
				Assert.That(File.Exists(GetTestFileName("r{0}.zip".Format(_user.Id))));
				Assert.That(File.Exists(GetTestFileName("{0}d.zip".Format(_user.Id))));
				Assert.That(File.Exists(GetTestFileName("{0}_.txt".Format(_user.Id))));

				Assert.That(!File.Exists(GetTestFileName("{0}_.zip".Format(_user.Id))));
				Assert.That(!File.Exists(GetTestFileName("{0}_mm.zip".Format(_user.Id))));
				Assert.That(!File.Exists(GetTestFileName("{0}_203.zip".Format(_user.Id))));

				var eventsWithFiles = memoryAppender.GetEvents();
				Assert.That(eventsWithFiles.Length, Is.EqualTo(3));
				Assert.That(eventsWithFiles.ToList().TrueForAll(e => e.RenderedMessage.StartsWith("Удалили файл с предыдущими подготовленными данными:")));
			});
		}

		[Test(Description = "После выполнения BatchOrder должно обновиться UserUpdateInfo.UpdateTime")]
		public void TestUpdateTimeAfterBatchOrder()
		{
			var appVersion = "1.1.1.1300";
			var _client = CreateClient();
			var _user = _client.Users[0];

			using (new TransactionScope()) {
				var smartRule = new TestSmartOrderRule();
				smartRule.OffersClientCode = null;
				smartRule.AssortimentPriceCode = 4662;
				smartRule.UseOrderableOffers = true;
				smartRule.ParseAlgorithm = "TestSource";
				smartRule.SaveAndFlush();

				var orderRule = TestDrugstoreSettings.Find(_client.Id);
				orderRule.SmartOrderRule = smartRule;
				orderRule.EnableSmartOrder = true;
				orderRule.UpdateAndFlush();
			}

			SetCurrentUser(_user.Login);

			ProcessWithLog(() => {
				var responce = LoadData(false, DateTime.Now, appVersion);
				var updateId = ParseUpdateId(responce);

				var previosUpdateTime = CommitExchange(updateId, RequestType.GetCumulative);

				var batchFileBytes = File.ReadAllBytes("TestData\\TestOrderSmall.7z");
				Assert.That(batchFileBytes.Length, Is.GreaterThan(0), "Файл с дефектурой оказался пуст, возможно, его нет в папке");

				var batchFile = Convert.ToBase64String(batchFileBytes);

				var postBatchResponce = PostOrderBatch(false, previosUpdateTime, appVersion, _user.AvaliableAddresses[0].Id, batchFile);
				var postBatchUpdateId = ParseUpdateId(postBatchResponce);
				var postBatchUpdateTime = CommitExchange(postBatchUpdateId, RequestType.PostOrderBatch);
			});
		}

		[Test(Description = "Производим запрос данных накопительного обновления после неподтвержденного автозаказа")]
		public void GetSimpleDataAfterPostOrderBatch()
		{
			var appVersion = "1.1.1.1299";
			var _client = CreateClient();
			var _user = _client.Users[0];

			var simpleUpdateTime = GetLastUpdateTime(_user);

			var postBatchId = Convert.ToUInt32(
				MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					@"
insert into logs.AnalitFUpdates
  (RequestTime, UpdateType, UserId, Commit, AppVersion) 
values 
  (now(), ?UpdateType, ?UserId, 0, ?AppVersion);
set @postBatchId = last_insert_id();
update usersettings.UserUpdateInfo, logs.AnalitFUpdates 
set 
  UserUpdateInfo.UncommitedUpdateDate = AnalitFUpdates.RequestTime
where AnalitFUpdates.UpdateId = @postBatchId and UserUpdateInfo.UserId = ?UserId;
select @postBatchId;",
					new MySqlParameter("?UpdateType", (int)RequestType.PostOrderBatch),
					new MySqlParameter("?UserId", _user.Id),
					new MySqlParameter("?AppVersion", appVersion)));

			SetCurrentUser(_user.Login);

			ProcessWithLog(() => {
				//Создаем файл с подготовленными данными
				File.WriteAllText(Path.Combine(ServiceContext.GetResultPath(), "{0}_{1}.zip".Format(_user.Id, postBatchId)), "Это файл с данными автозаказа");

				var responce = LoadData(false, simpleUpdateTime.ToUniversalTime(), appVersion);
				var simpleId = ParseUpdateId(responce);

				Assert.That(simpleId, Is.Not.EqualTo(postBatchId), "UpdateId не должны совпадать");

				var requestType = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select UpdateType from logs.AnalitFUpdates where UpdateId = ?UpdateId",
					new MySqlParameter("?UpdateId", simpleId)));
				Assert.That(requestType, Is.EqualTo((int)RequestType.GetData), "Неожидаемый тип обновления: должно быть накопительное");

				var afterFirstFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_*.zip".Format(_user.Id));
				Assert.That(afterFirstFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterFirstFiles.Implode());
				Assert.That(afterFirstFiles[0], Is.StringEnding("{0}_{1}.zip".Format(_user.Id, simpleId)));
			});
		}

		[Test(Description = "Производим запрос данных кумулятивного обновления после неподтвержденного автозаказа")]
		public void GetCumulativeDataAfterPostOrderBatch()
		{
			var appVersion = "1.1.1.1299";
			var _client = CreateClient();
			var _user = _client.Users[0];

			var simpleUpdateTime = GetLastUpdateTime(_user);

			var postBatchId = Convert.ToUInt32(
				MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					@"
insert into logs.AnalitFUpdates
  (RequestTime, UpdateType, UserId, Commit, AppVersion) 
values 
  (now(), ?UpdateType, ?UserId, 0, ?AppVersion);
set @postBatchId = last_insert_id();
update usersettings.UserUpdateInfo, logs.AnalitFUpdates 
set 
  UserUpdateInfo.UncommitedUpdateDate = AnalitFUpdates.RequestTime
where AnalitFUpdates.UpdateId = @postBatchId and UserUpdateInfo.UserId = ?UserId;
select @postBatchId;",
					new MySqlParameter("?UpdateType", (int)RequestType.PostOrderBatch),
					new MySqlParameter("?UserId", _user.Id),
					new MySqlParameter("?AppVersion", appVersion)));

			SetCurrentUser(_user.Login);

			ProcessWithLog(() => {
				//Создаем файл с подготовленными данными
				File.WriteAllText(Path.Combine(ServiceContext.GetResultPath(), "{0}_{1}.zip".Format(_user.Id, postBatchId)), "Это файл с данными автозаказа");

				var responce = LoadData(true, simpleUpdateTime.ToUniversalTime(), appVersion);
				var cumulativeId = ParseUpdateId(responce);

				Assert.That(cumulativeId, Is.Not.EqualTo(postBatchId), "UpdateId не должны совпадать");

				var requestType = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select UpdateType from logs.AnalitFUpdates where UpdateId = ?UpdateId",
					new MySqlParameter("?UpdateId", cumulativeId)));
				Assert.That(requestType, Is.EqualTo((int)RequestType.GetCumulative), "Неожидаемый тип обновления: должно быть кумулятивное");

				var afterFirstFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_*.zip".Format(_user.Id));
				Assert.That(afterFirstFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterFirstFiles.Implode());
				Assert.That(afterFirstFiles[0], Is.StringEnding("{0}_{1}.zip".Format(_user.Id, cumulativeId)));
			});
		}

		[Test(Description = "Проверка подготовки данных для отключенного пользователя")]
		public void CheckGetUserDataOnDisabledUser()
		{
			var appVersion = "1.1.1.1299";
			var _client = CreateClient();
			var _user = _client.Users[0];
			using (var transaction = new TransactionScope()) {
				_user.Enabled = false;
				_user.Update();
			}

			SetCurrentUser(_user.Login);

			var service = new PrgDataEx();
			var responce = service.GetUserDataWithPriceCodes(DateTime.Now, true, appVersion, 50, UniqueId, "", "", false, null, null);

			Assert.That(responce, Is.StringContaining("Desc=В связи с неоплатой услуг доступ закрыт.").IgnoreCase);
			Assert.That(responce, Is.StringContaining("Error=Пожалуйста, обратитесь в бухгалтерию АК \"Инфорум\".[1]").IgnoreCase);
		}

		[Test(Description = "Проверка подготовки данных для отключенного пользователя")]
		public void CheckGetUserDataOnDisabledClient()
		{
			var appVersion = "1.1.1.1299";
			var _client = CreateClient();
			var _user = _client.Users[0];
			using (var transaction = new TransactionScope()) {
				_client.Status = ClientStatus.Off;
				_client.Update();
			}

			SetCurrentUser(_user.Login);

			var service = new PrgDataEx();
			var responce = service.GetUserDataWithPriceCodes(DateTime.Now, true, appVersion, 50, UniqueId, "", "", false, null, null);

			Assert.That(responce, Is.StringContaining("Desc=В связи с неоплатой услуг доступ закрыт.").IgnoreCase);
			Assert.That(responce, Is.StringContaining("Error=Пожалуйста, обратитесь в бухгалтерию АК \"Инфорум\".[1]").IgnoreCase);
		}

		[Test(Description = "попытка получить данные для пользователя, который не привязан к системе")]
		public void CheckGetUserDataOnUnknowUser()
		{
			var appVersion = "1.1.1.1299";
			var unknownUser = "dsdsdsdsds";

			SetCurrentUser(unknownUser);

			ProcessWithLog(memoryAppender => {
				var service = new PrgDataEx();
				var responce = service.GetUserDataWithPriceCodes(DateTime.Now, true, appVersion, 50, UniqueId, "", "", false, null, null);

				Assert.That(responce, Is.StringContaining("Desc=Доступ закрыт.").IgnoreCase);
				Assert.That(responce, Is.StringContaining("Error=Пожалуйста, обратитесь в АК \"Инфорум\".[1]").IgnoreCase);

				var events = memoryAppender.GetEvents();

				var lastEvent = events[events.Length - 1];
				Assert.That(lastEvent.Level, Is.EqualTo(Level.Error));
				Assert.That(lastEvent.MessageObject, Is.TypeOf(typeof(UpdateException)));
				var updateException = (UpdateException)lastEvent.MessageObject;
				Assert.That(updateException.Message, Is.EqualTo("Доступ закрыт."));
				Assert.That(updateException.Addition, Is.StringStarting("Для логина " + unknownUser + " услуга не предоставляется;"));
				Assert.That(updateException.UpdateType, Is.EqualTo(RequestType.Forbidden));
			},
				false);
		}

		[Test(Description = "попытка получить данные для пользователя, который привязан к поставщику")]
		public void CheckGetUserDataOnSupplierUser()
		{
			var appVersion = "1.1.1.1299";

			var supplier = TestSupplier.Create();
			TestUser supplierUser;
			using (new SessionScope()) {
				supplierUser = supplier.Users.First();
			}

			SetCurrentUser(supplierUser.Login);

			ProcessWithLog(memoryAppender => {
				var service = new PrgDataEx();
				var responce = service.GetUserDataWithPriceCodes(DateTime.Now, true, appVersion, 50, UniqueId, "", "", false, null, null);

				Assert.That(responce, Is.StringContaining("Desc=Доступ закрыт.").IgnoreCase);
				Assert.That(responce, Is.StringContaining("Error=Пожалуйста, обратитесь в АК \"Инфорум\".[1]").IgnoreCase);

				var events = memoryAppender.GetEvents();

				var lastEvent = events[events.Length - 1];

				Assert.That(lastEvent.Level, Is.EqualTo(Level.Warn));
				Assert.That(lastEvent.MessageObject, Is.TypeOf(typeof(UpdateException)));
				var updateException = (UpdateException)lastEvent.MessageObject;
				Assert.That(updateException.Message, Is.EqualTo("Доступ закрыт."));
				Assert.That(updateException.Addition, Is.StringStarting("Для логина " + supplierUser.Login + " услуга не предоставляется;"));
				Assert.That(updateException.UpdateType, Is.EqualTo(RequestType.Forbidden));
			},
				false);
		}

		private bool GetForceReplication(uint firmCode, uint userId)
		{
			var forceReplication = SessionHelper.WithSession<byte>(
				s => {
					return s.CreateSQLQuery(
						@"
select ForceReplication from usersettings.AnalitFReplicationInfo where FirmCode = :firmCode and UserId = :parentUserId
")
						.SetParameter("firmCode", firmCode)
						.SetParameter("parentUserId", userId)
						.UniqueResult<byte>();
				});
			return forceReplication > 0;
		}

		[Test(Description = "Флаг ForceReplication должен установиться после подключения прайс-листа родительскому пользователю")]
		public void ForceReplicationOnInheritPrices()
		{
			var client = CreateClient();
			var parentUser = client.Users[0];
			TestUser childUser;

			using (var transaction = new TransactionScope()) {
				childUser = client.CreateUser();

				childUser.SendRejects = true;
				childUser.SendWaybills = true;
				childUser.InheritPricesFrom = parentUser;
				childUser.Update();
			}

			using (var connection = new MySqlConnection(Settings.ConnectionString())) {
				connection.Open();

				var parentUpdateData = UpdateHelper.GetUpdateData(connection, parentUser.Login);
				var parentHelper = new UpdateHelper(parentUpdateData, connection);
				parentHelper.MaintainReplicationInfo();

				var childUpdateData = UpdateHelper.GetUpdateData(connection, childUser.Login);
				var childHelper = new UpdateHelper(childUpdateData, connection);
				childHelper.MaintainReplicationInfo();
			}

			var deletedPrice = parentUser.GetActivePrices().First();
			Assert.That(childUser.GetActivePrices().Any(item => item.Id == deletedPrice.Id), Is.True, "У подчиненного клиента не найден прайс-лист: {0}", deletedPrice);

			//Отключаем прайс-лист
			SessionHelper.WithSession(
				s => {
					s.CreateSQLQuery(
						@"
update usersettings.AnalitFReplicationInfo set ForceReplication = 0 where FirmCode = :firmCode and UserId = :parentUserId;
update usersettings.AnalitFReplicationInfo set ForceReplication = 0 where FirmCode = :firmCode and UserId = :childUserId;
delete from Customers.UserPrices where UserId = :parentUserId and PriceId = :priceId;
")
						.SetParameter("firmCode", deletedPrice.Supplier.Id)
						.SetParameter("priceId", deletedPrice.Id)
						.SetParameter("parentUserId", parentUser.Id)
						.SetParameter("childUserId", childUser.Id)
						.ExecuteUpdate();
				});

			Assert.That(parentUser.GetActivePrices().Any(item => item.Id == deletedPrice.Id), Is.False, "У родительского клиента найден отключенный прайс-лист: {0}", deletedPrice);
			Assert.That(childUser.GetActivePrices().Any(item => item.Id == deletedPrice.Id), Is.False, "У подчиненного клиента найден отключенный прайс-лист: {0}", deletedPrice);

			var parentForceReplication = GetForceReplication(deletedPrice.Supplier.Id, parentUser.Id);
			Assert.That(parentForceReplication, Is.True, "При удалении прайс-листа не был установлен флаг ForceReplication для пользователя: {0}", parentUser);

			var childForceReplication = GetForceReplication(deletedPrice.Supplier.Id, childUser.Id);
			Assert.That(childForceReplication, Is.True, "При удалении прайс-листа не был установлен флаг ForceReplication для пользователя: {0}", childUser);

			//Включаем прайс-лист
			SessionHelper.WithSession(
				s => {
					s.CreateSQLQuery(
						@"
update usersettings.AnalitFReplicationInfo set ForceReplication = 0 where FirmCode = :firmCode and UserId = :parentUserId;
update usersettings.AnalitFReplicationInfo set ForceReplication = 0 where FirmCode = :firmCode and UserId = :childUserId;
insert into Customers.UserPrices (UserId, PriceId, RegionId) values (:parentUserId, :priceId, :regionId);
")
						.SetParameter("firmCode", deletedPrice.Supplier.Id)
						.SetParameter("priceId", deletedPrice.Id)
						.SetParameter("parentUserId", parentUser.Id)
						.SetParameter("childUserId", childUser.Id)
						.SetParameter("regionId", client.RegionCode)
						.ExecuteUpdate();
				});

			Assert.That(parentUser.GetActivePrices().Any(item => item.Id == deletedPrice.Id), Is.True, "У родительского клиента найден включенный прайс-лист: {0}", deletedPrice);
			Assert.That(childUser.GetActivePrices().Any(item => item.Id == deletedPrice.Id), Is.True, "У подчиненного клиента найден включенный прайс-лист: {0}", deletedPrice);

			Console.WriteLine(deletedPrice.Id);
			parentForceReplication = GetForceReplication(deletedPrice.Supplier.Id, parentUser.Id);
			Assert.That(parentForceReplication, Is.True, "При включении прайс-листа поставщика {1} не был установлен флаг ForceReplication для пользователя: {0}", parentUser, deletedPrice.Supplier.Id);

			childForceReplication = GetForceReplication(deletedPrice.Supplier.Id, childUser.Id);
			Assert.That(childForceReplication, Is.True, "При включении прайс-листа не был установлен флаг ForceReplication для пользователя: {0}", childUser);
		}

		[Test(Description = "попытка получить данные при неккорректном номере версии")]
		public void CheckGetUserDataOnBuildNumber()
		{
			var appVersion = "";

			var testUser = CreateUserForAnalitF();

			SetCurrentUser(testUser.Login);

			ProcessWithLog(memoryAppender => {
				var service = new PrgDataEx();
				var responce = service.GetUserDataWithPriceCodes(DateTime.Now, true, appVersion, 50, UniqueId, "", "", false, null, null);

				Assert.That(responce, Is.StringContaining("Desc=Пожалуйста, повторите попытку через несколько минут.").IgnoreCase);
				Assert.That(responce, Is.StringContaining("Error=При выполнении Вашего запроса произошла ошибка.").IgnoreCase);

				var events = memoryAppender.GetEvents();

				var lastEvent = events[events.Length - 1];
				Assert.That(lastEvent.Level, Is.EqualTo(Level.Warn));
				Assert.That(lastEvent.MessageObject, Is.TypeOf(typeof(UpdateException)));
				var updateException = (UpdateException)lastEvent.MessageObject;
				Assert.That(updateException.Message, Is.EqualTo("Пожалуйста, повторите попытку через несколько минут."));
				Assert.That(updateException.Addition, Is.StringStarting("Ошибка при разборе номера версии '';"));
				Assert.That(updateException.UpdateType, Is.EqualTo(RequestType.Error));
			},
				false);

			using (new SessionScope()) {
				var logs = TestAnalitFUpdateLog.Queryable.Where(updateLog => updateLog.UserId == testUser.Id).ToList();
				Assert.That(logs.Count, Is.EqualTo(1), "Не найдена запись в логах для пользователя {0}", testUser.Id);
				var log = logs[0];
				Assert.That(log.UpdateType, Is.EqualTo((uint)RequestType.Error));
				Assert.That(log.Addition, Is.StringContaining("Ошибка при разборе номера версии '';"));
			}
		}

		[Test(Description = "Производим запрос данных накопительного обновления с датой клиента больше, чем на сервере, при этом клиент должен получить КО")]
		public void GetDataWithBiggerUpdateDate()
		{
			var appVersion = "1.1.1.1299";
			var _client = CreateClient();
			var _user = _client.Users[0];

			var simpleUpdateTime = GetLastUpdateTime(_user);

			SetCurrentUser(_user.Login);

			ProcessWithLog(() => {
				var responce = LoadData(false, simpleUpdateTime.AddMinutes(2).ToUniversalTime(), appVersion);
				var cumulativeId = ParseUpdateId(responce);

				var requestType = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select UpdateType from logs.AnalitFUpdates where UpdateId = ?UpdateId",
					new MySqlParameter("?UpdateId", cumulativeId)));
				Assert.That(requestType, Is.EqualTo((int)RequestType.GetCumulative), "Неожидаемый тип обновления: должно быть кумулятивное");

				var afterFirstFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_*.zip".Format(_user.Id));
				Assert.That(afterFirstFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterFirstFiles.Implode());
				Assert.That(afterFirstFiles[0], Is.StringEnding("{0}_{1}.zip".Format(_user.Id, cumulativeId)));
			});
		}

		[Test(Description = "Производим запрос данных накопительного обновления с датой клиента меньше, чем на сервере, при этом клиент должен получить частичное КО")]
		public void GetDataWithLesserUpdateDate()
		{
			var appVersion = "1.1.1.1299";
			var _client = CreateClient();
			var _user = _client.Users[0];

			var simpleUpdateTime = GetLastUpdateTime(_user);

			SetCurrentUser(_user.Login);

			ProcessWithLog(() => {
				var responce = LoadData(false, simpleUpdateTime.AddMinutes(-2).ToUniversalTime(), appVersion);
				var cumulativeId = ParseUpdateId(responce);

				var requestType = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select UpdateType from logs.AnalitFUpdates where UpdateId = ?UpdateId",
					new MySqlParameter("?UpdateId", cumulativeId)));
				Assert.That(requestType, Is.EqualTo((int)RequestType.GetLimitedCumulative), "Неожидаемый тип обновления: должно быть кумулятивное");

				var afterFirstFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_*.zip".Format(_user.Id));
				Assert.That(afterFirstFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterFirstFiles.Implode());
				Assert.That(afterFirstFiles[0], Is.StringEnding("{0}_{1}.zip".Format(_user.Id, cumulativeId)));
			});
		}

		[Test(Description = "производим запрос каталога при отсутствующих продуктах")]
		public void GetCatalogsOnRequest()
		{
			var appVersion = "1.1.1.1835";
			var _client = CreateClient();
			var _user = _client.Users[0];

			var simpleUpdateTime = GetLastUpdateTime(_user);

			SetCurrentUser(_user.Login);

			var productId = Convert.ToUInt32(MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				"select Id from catalogs.Products where Hidden = 0 and UpdateTime < ?simpleUpdateTime order by UpdateTime desc limit 1",
				new MySqlParameter("?simpleUpdateTime", simpleUpdateTime)));
			Assert.That(productId, Is.GreaterThan(0), "Не смогли найти продукт с датой обновления меньше даты обновления клиента");
			var catalogId = Convert.ToUInt32(MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				"select CatalogId from catalogs.Products where Id = ?id",
				new MySqlParameter("?id", productId)));

			var productsFileName = "Products" + _user.Id + ".txt";
			var catalogsFileName = "Catalogs" + _user.Id + ".txt";

			ProcessWithLog(() => {
				//Делаем обновление без продуктов - в списке продуктов не должно быть выбранного продукта
				var responce = LoadDataWithMissingProductsAsync(false, simpleUpdateTime.ToUniversalTime(), appVersion, null, null);

				var simpleUpdateId = ShouldBeSuccessfull(responce);

				WaitAsyncResponse(simpleUpdateId);

				var requestType = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select UpdateType from logs.AnalitFUpdates where UpdateId = ?UpdateId",
					new MySqlParameter("?UpdateId", simpleUpdateId)));
				Assert.That(requestType, Is.EqualTo((int)RequestType.GetData), "Неожидаемый тип обновления: должно быть накопительное");

				var archiveFileName = CheckArchive(_user, simpleUpdateId);

				var extractFolder = ExtractArchive(archiveFileName);

				Assert.That(File.Exists(Path.Combine(extractFolder, productsFileName)), Is.True);

				var productsContent = File.ReadAllText(Path.Combine(extractFolder, productsFileName));
				Assert.That(productsContent, Is.Not.StringContaining(productId + "\t"));

				Assert.That(File.Exists(Path.Combine(extractFolder, catalogsFileName)), Is.True);

				var catalogsContent = File.ReadAllText(Path.Combine(extractFolder, catalogsFileName));
				Assert.That(catalogsContent, Is.Not.StringContaining(catalogId + "\t"));

				simpleUpdateTime = CommitExchange(simpleUpdateId, RequestType.GetData);

				//Делаем обновление с запросом продуктов - в списке продуктов должен быть выбранный продукт
				responce = LoadDataWithMissingProductsAsync(false, simpleUpdateTime, appVersion, null, new uint[] { productId });

				simpleUpdateId = ShouldBeSuccessfull(responce);

				WaitAsyncResponse(simpleUpdateId);

				requestType = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select UpdateType from logs.AnalitFUpdates where UpdateId = ?UpdateId",
					new MySqlParameter("?UpdateId", simpleUpdateId)));
				Assert.That(requestType, Is.EqualTo((int)RequestType.GetData), "Неожидаемый тип обновления: должно быть накопительное");

				archiveFileName = CheckArchive(_user, simpleUpdateId);

				extractFolder = ExtractArchive(archiveFileName);

				Assert.That(File.Exists(Path.Combine(extractFolder, productsFileName)), Is.True);

				productsContent = File.ReadAllText(Path.Combine(extractFolder, productsFileName));
				Assert.That(productsContent, Is.StringContaining(productId + "\t"));

				Assert.That(File.Exists(Path.Combine(extractFolder, catalogsFileName)), Is.True);

				catalogsContent = File.ReadAllText(Path.Combine(extractFolder, catalogsFileName));
				Assert.That(catalogsContent, Is.StringContaining(catalogId + "\t"));

				simpleUpdateTime = CommitExchange(simpleUpdateId, RequestType.GetData);
			});
		}
	}
}