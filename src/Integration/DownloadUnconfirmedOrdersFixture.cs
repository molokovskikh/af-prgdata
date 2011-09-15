using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Castle.ActiveRecord;
using Common.Models;
using Common.Models.Tests;
using Common.Tools;
using Inforoom.Common;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Filter;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData;
using PrgData.Common;
using PrgData.Common.Orders;
using Test.Support;
using Test.Support.Helpers;
using Test.Support.Logs;

namespace Integration
{
	[TestFixture]
	public class DownloadUnconfirmedOrdersFixture
	{
		private TestClient _client;
		private TestUser _officeUser;
		private TestUser _drugstoreUser;
		private TestAddress _drugstoreAddress;

		private string _uniqueId;
		private string _afAppVersion;
		private DateTime _lastUpdateTime;

		[TestFixtureSetUp]
		public void FixtureSetUp()
		{
			ArchiveHelper.SevenZipExePath = @".\7zip\7z.exe";

			_uniqueId = "123";
			_afAppVersion = "1.1.1.1413";

			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ServiceContext.GetResultPath = () => "results\\";

			_client = TestClient.Create();

			using (var transaction = new TransactionScope())
			{
				_officeUser = _client.Users[0];
				_officeUser.AllowDownloadUnconfirmedOrders = true;

				_drugstoreAddress = _client.CreateAddress();

				_drugstoreUser = _client.CreateUser();

				_drugstoreUser.JoinAddress(_drugstoreAddress);
				_officeUser.JoinAddress(_drugstoreAddress);

				_client.Users.Each(u =>
				{
					u.SendRejects = true;
					u.SendWaybills = true;
				});


				_officeUser.UpdateInfo.AFAppVersion = 1413;

				_drugstoreUser.InheritPricesFrom = _officeUser;
				_drugstoreUser.SubmitOrders = true;

				_client.Update();

				transaction.VoteCommit();
			}

			SessionHelper.WithSession(
				s =>
				{
					var prices = _officeUser.GetActivePricesList().Where(p => p.PositionCount > 800).OrderBy(p => p.PositionCount);
					var newPrices = new List<uint>();
					foreach (var testActivePrice in prices)
					{
						if (testActivePrice.CoreCount() > 0)
							newPrices.Add(testActivePrice.Id.PriceId);
						if (newPrices.Count == 4)
							break;
					}

					Assert.That(newPrices.Count, Is.EqualTo(4), "Не нашли достаточное кол-во прайс-листов для тестов");

					s.CreateSQLQuery(
						"delete from future.UserPrices where UserId = :userId and PriceId not in (:priceIds);")
						.SetParameter("userId", _officeUser.Id)
						.SetParameterList("priceIds", newPrices.ToArray())
						.ExecuteUpdate();
				});
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}

		[SetUp]
		public void SetUp()
		{
			_lastUpdateTime = GetLastUpdateTime();
			SetCurrentUser(_officeUser.Login);
			TestDataManager.DeleteAllOrdersForClient(_client.Id);
		}

		private DateTime GetLastUpdateTime()
		{
			var simpleUpdateTime = DateTime.Now;
			//Такое извращение используется, чтобы исключить из даты мусор в виде учтенного времени меньше секунды,
			//чтобы сравнение при проверке сохраненного времени обновления отрабатывало
			simpleUpdateTime = simpleUpdateTime.Date
				.AddHours(simpleUpdateTime.Hour)
				.AddMinutes(simpleUpdateTime.Minute)
				.AddSeconds(simpleUpdateTime.Second);

			_officeUser.UpdateInfo.UpdateDate = simpleUpdateTime;
			_officeUser.Update();

			return simpleUpdateTime;
		}

		[Test(Description = "Проверяем, что использование NHibernate-сессии от текущего подключение не закрывает подключение и транзакцию")]
		public void TestNHibernateSession()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
				{
					using (var session = IoC.Resolve<ISessionFactoryHolder>().SessionFactory.OpenSession(connection))
					{
						var tmpClientId = session
							.CreateSQLQuery("select ClientCode from UserSettings.RetClientsSet where ClientCode = :clientId")
							.SetParameter("clientId", _client.Id)
							.UniqueResult();
						Assert.That(tmpClientId, Is.Not.Null);
						Assert.That(tmpClientId, Is.EqualTo(_client.Id));
					}

					Assert.That(connection.State, Is.EqualTo(ConnectionState.Open));

					connection.Ping();

					var transactionClientId = MySqlHelper.ExecuteScalar(
						connection,
						"select ClientCode from UserSettings.RetClientsSet where ClientCode = ?clientId",
						new MySqlParameter("?clientId", _client.Id));
					Assert.That(transactionClientId, Is.Not.Null);
					Assert.That(transactionClientId, Is.EqualTo(_client.Id));

					var updateCount = MySqlHelper.ExecuteNonQuery(
						connection,
						"update UserSettings.RetClientsSet set AllowDelayOfPayment = 1 where ClientCode = ?clientId",
						new MySqlParameter("?clientId", _client.Id));
					Assert.That(updateCount, Is.EqualTo(1));

					transaction.Rollback();
				}

				var allowDelayOfPayments = Convert.ToBoolean(MySqlHelper.ExecuteScalar(
					connection,
					"select AllowDelayOfPayment from UserSettings.RetClientsSet where ClientCode = ?clientId",
					new MySqlParameter("?clientId", _client.Id)));
				Assert.That(allowDelayOfPayments, Is.False);
			}
		}

		[Test(Description = "проверяем работу класса Orders2StringConverter")]
		public void TestOrders2StringConverter()
		{
			var price = _drugstoreUser.GetActivePricesList()[0];

			var order = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id, price.Id.PriceId);

			var converter = new Orders2StringConverter(new List<Order> {order}, 1, 1, false);

			Assert.That(converter.OrderHead, Is.Not.Null);
			Assert.That(converter.OrderItems, Is.Not.Null);

			Assert.That(converter.OrderHead.ToString(), Is.StringStarting("{0}\t{1}".Format(1, _drugstoreAddress.Id)), "Не корректно выгружен заголовок заказа");
			Assert.That(converter.OrderItems.ToString(), Is.StringStarting("{0}\t{1}\t{2}".Format(1, 1, _drugstoreAddress.Id)), "Не корректно выгружен список позиций заказа");

			var columns = converter.OrderHead.ToString().Split('\t');
			Assert.That(columns.Length, Is.EqualTo(4), "Неожидаемое количество элементов, разделенных tab");

			converter = new Orders2StringConverter(new List<Order> { order }, 1, 1, true);

			Assert.That(converter.OrderHead, Is.Not.Null);
			Assert.That(converter.OrderItems, Is.Not.Null);

			Assert.That(converter.OrderHead.ToString(), Is.StringStarting("{0}\t{1}".Format(1, _drugstoreAddress.Id)), "Не корректно выгружен заголовок заказа");
			Assert.That(converter.OrderItems.ToString(), Is.StringStarting("{0}\t{1}\t{2}".Format(1, 1, _drugstoreAddress.Id)), "Не корректно выгружен список позиций заказа");

			columns = converter.OrderHead.ToString().Split('\t');
			Assert.That(columns.Length, Is.EqualTo(5), "Неожидаемое количество элементов, разделенных tab");
			Assert.That(columns[4], Is.StringStarting(order.WriteTime.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss")), "Дата заказа экспортированна некорректно");
		}

		[Test(Description = "проверяем удаление неподтвержденных заказов при подтверждении обновления")]
		public void DeleteUnconfirmedOrders()
		{
			var orderFirst = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id);
			var orderSecond = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id);

			TestAnalitFUpdateLog updateLog;
			using (new TransactionScope())
			{
				updateLog = new TestAnalitFUpdateLog();
				updateLog.RequestTime = DateTime.Now;
				updateLog.UpdateType = (uint) RequestType.GetData;
				updateLog.UserId = _officeUser.Id;
				updateLog.Create();

				var sendLog = new TestUnconfirmedOrdersSendLog();
				sendLog.OrderId = orderFirst.RowId;
				sendLog.User = _officeUser;
				sendLog.UpdateId = updateLog.Id;
				sendLog.Create();

				sendLog = new TestUnconfirmedOrdersSendLog();
				sendLog.OrderId = orderSecond.RowId;
				sendLog.User = _officeUser;
				sendLog.UpdateId = updateLog.Id;
				sendLog.Create();
			}


			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _officeUser.Login);

				var unconfirmedOrdersCount = MySqlHelper.ExecuteScalar(
					connection,
					"select count(*) from orders.OrdersHead oh where oh.ClientCode = ?clientId and deleted = 0 and submited = 0",
					new MySqlParameter("?clientId", _client.Id));
				Assert.That(unconfirmedOrdersCount, Is.EqualTo(2));

				UnconfirmedOrdersExporter.DeleteUnconfirmedOrders(updateData, connection, updateLog.Id);

				var unconfirmedOrdersCountAfterDelete = MySqlHelper.ExecuteScalar(
					connection,
					"select count(*) from orders.OrdersHead oh where oh.ClientCode = ?clientId and deleted = 1 and submited = 0 and (RowId = ?firstId or RowId = ?secondId)",
					new MySqlParameter("?clientId", _client.Id),
					new MySqlParameter("?firstId", orderFirst.RowId),
					new MySqlParameter("?secondId", orderSecond.RowId));
				Assert.That(unconfirmedOrdersCountAfterDelete, Is.EqualTo(2));
			}
		}

		[Test(Description = "Проверяем загрузку заказов в Exporter'е")]
		public void TestLoadOrders()
		{
			var prices = _drugstoreUser.GetActivePricesList();
			var orders = new List<Order>();
			for (int i = 0; i < 3; i++)
			{
				var order = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[i].Id.PriceId);
				orders.Add(order);
			}

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _officeUser.Login);
				var helper = new UpdateHelper(updateData, connection);

				var fileForArchives = new Queue<FileForArchive>();
				var exporter = new UnconfirmedOrdersExporter(updateData, helper, "results\\", fileForArchives);
				
				exporter.LoadOrders();

				Assert.That(exporter.LoadedOrders.Count, Is.EqualTo(3));
				Assert.That(updateData.UnconfirmedOrders.Count, Is.EqualTo(3));
				foreach (var order in orders)
				{
					Assert.That(exporter.LoadedOrders.Contains(o => o.RowId == order.RowId), Is.True, "Не найден заказ OrderId = {0}", order.RowId);
					Assert.That(updateData.UnconfirmedOrders.Contains(o => o == order.RowId), Is.True, "Не найден заказ OrderId = {0}", order.RowId);
				}

				exporter.UnionOrders();

				Assert.That(exporter.ExportedOrders.Count, Is.EqualTo(3));

				exporter.ExportOrders();

				Assert.That(fileForArchives.Count, Is.EqualTo(2));
			}
		}

		[Test(Description = "Проверяем объединение заказов в Exporter'е")]
		public void TestUnionOrders()
		{
			var prices = _drugstoreUser.GetActivePricesList();
			var orders = new List<Order>();
			orders.Add(TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[0].Id.PriceId));
			orders.Add(TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[1].Id.PriceId));
			orders.Add(TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[0].Id.PriceId));
			orders.Add(TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[2].Id.PriceId));

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _officeUser.Login);
				var helper = new UpdateHelper(updateData, connection);

				var fileForArchives = new Queue<FileForArchive>();
				var exporter = new UnconfirmedOrdersExporter(updateData, helper, "results\\", fileForArchives);

				exporter.LoadOrders();

				Assert.That(exporter.LoadedOrders.Count, Is.EqualTo(4));
				Assert.That(updateData.UnconfirmedOrders.Count, Is.EqualTo(4));
				foreach (var order in orders)
				{
					Assert.That(exporter.LoadedOrders.Contains(o => o.RowId == order.RowId), Is.True, "Не найден заказ OrderId = {0}", order.RowId);
					Assert.That(updateData.UnconfirmedOrders.Contains(o => o == order.RowId), Is.True, "Не найден заказ OrderId = {0}", order.RowId);
				}

				exporter.UnionOrders();

				Assert.That(exporter.ExportedOrders.Count, Is.EqualTo(3));

				Assert.That(exporter.ExportedOrders.Contains(o => o.RowId == orders[0].RowId), Is.True);
				Assert.That(exporter.ExportedOrders.Contains(o => o.RowId == orders[1].RowId), Is.True);
				Assert.That(exporter.ExportedOrders.Contains(o => o.RowId == orders[3].RowId), Is.True);

				Assert.That(exporter.ExportedOrders[0].RowCount, Is.EqualTo(6));
			}
		}

		private string LoadData(bool getEtalonData, DateTime accessTime, string appVersion)
		{
			var service = new PrgDataEx();
			var responce = service.GetUserDataWithOrders(accessTime, getEtalonData, appVersion, 50, _uniqueId, "", "", false, null, 1, 1, null);

			Assert.That(responce, Is.StringStarting("URL=").IgnoreCase);

			return responce;
		}

		private string LoadDataAsync(bool getEtalonData, DateTime accessTime, string appVersion)
		{
			var service = new PrgDataEx();
			var responce = service.GetUserDataWithOrdersAsync(accessTime, getEtalonData, appVersion, 50, _uniqueId, "", "", false, null, 1, 1, null);

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

		private string CheckAsyncRequest(ulong updateId)
		{
			var service = new PrgDataEx();
			return service.CheckAsyncRequest(updateId);
		}

		[Test(Description = "Проверяем простой запрос данных с выгружаемыми заказами")]
		public void SimpleLoadData()
		{
			var extractFolder = Path.Combine(Path.GetFullPath(ServiceContext.GetResultPath()), "ExtractZip");
			if (Directory.Exists(extractFolder))
				Directory.Delete(extractFolder, true);
			Directory.CreateDirectory(extractFolder);

			var order = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id);

			try
			{
				var memoryAppender = new MemoryAppender();
				memoryAppender.AddFilter(new LoggerMatchFilter { AcceptOnMatch = true, LoggerToMatch = "PrgData", Next = new DenyAllFilter() });
				BasicConfigurator.Configure(memoryAppender);


				try
				{
					var responce = LoadData(false, _lastUpdateTime.ToUniversalTime(), _afAppVersion);

					var simpleUpdateId = ParseUpdateId(responce);

					var afterSimpleFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_*.zip".Format(_officeUser.Id));
					Assert.That(afterSimpleFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterSimpleFiles.Implode());
					Assert.That(afterSimpleFiles[0], Is.StringEnding("{0}_{1}.zip".Format(_officeUser.Id, simpleUpdateId)));

					ArchiveHelper.Extract(afterSimpleFiles[0], "*.*", extractFolder);

					var rootFiles = Directory.GetFiles(extractFolder);

					var headFile =
						rootFiles.FirstOrDefault(
							file => file.EndsWith("CurrentOrderHeads{0}.txt".Format(_officeUser.Id), StringComparison.OrdinalIgnoreCase));
					Assert.That(headFile, Is.Not.Null.And.Not.Empty, "Не найден файл с заголовком заказа");
					Assert.That(new FileInfo(headFile).Length, Is.GreaterThan(0), "Файл с заголовком заказа оказался пустым");

					var listFile =
						rootFiles.FirstOrDefault(
							file => file.EndsWith("CurrentOrderLists{0}.txt".Format(_officeUser.Id), StringComparison.OrdinalIgnoreCase));
					Assert.That(listFile, Is.Not.Null.And.Not.Empty, "Не найден файл со списком позиций заказа");
					Assert.That(new FileInfo(listFile).Length, Is.GreaterThan(0), "Файл со списком позиций заказа оказался пустым");

					Directory.Delete(extractFolder, true);

					var sendLogs = TestUnconfirmedOrdersSendLog.Queryable.Where(l => l.UpdateId == simpleUpdateId).ToList();
					Assert.That(sendLogs.Count, Is.EqualTo(1), "Должен быть один заказ, экспортированный пользователю в данном обновлении");
					Assert.That(sendLogs[0].OrderId, Is.EqualTo(order.RowId), "Номер экспортированного заказа не совпадает");
					Assert.That(sendLogs[0].User.Id, Is.EqualTo(_officeUser.Id), "Код пользователя не совпадает");

					var service = new PrgDataEx();
					var updateTime = service.CommitExchange(simpleUpdateId, false);

					//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
					Thread.Sleep(3000);

					var deletedStatus = Convert.ToBoolean(
						MySqlHelper.ExecuteScalar(
							Settings.ConnectionString(),
							"select Deleted from orders.OrdersHead where RowId = ?OrderId",
							new MySqlParameter("?OrderId", order.RowId)));
					Assert.That(deletedStatus, Is.True, "Неподтвержденный заказ {0} не помечен как удаленный", order.RowId);

					var addition = Convert.ToString(MySqlHelper.ExecuteScalar(
						Settings.ConnectionString(),
						"select Addition from logs.AnalitFUpdates where UpdateId = ?UpdateId",
						new MySqlParameter("?UpdateId", simpleUpdateId)));
					Assert.That(addition, Is.StringContaining("Экспортированные неподтвержденные заказы: {0}".Format(order.RowId)), "Неподтвержденный заказ {0} не содержится в поле Addition", order.RowId);
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

		[Test(Description = "Попытка загружить заказы, когда нет неподтвержденных заказов")]
		public void LoadOrdersOnNonExistsUnconfirmedOrders()
		{
			var prices = _drugstoreUser.GetActivePricesList();
			var processedOrder = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[0].Id.PriceId);
			processedOrder.Processed = true;

			var deletedOrder = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[1].Id.PriceId);
			deletedOrder.Deleted = true;

			var submitedOrder = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[2].Id.PriceId);
			submitedOrder.Submited = true;

			With.Transaction(
				s => 
				{ 
					s.SaveOrUpdate(processedOrder);
					s.SaveOrUpdate(deletedOrder);
					s.SaveOrUpdate(submitedOrder);
				});

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _officeUser.Login);
				var helper = new UpdateHelper(updateData, connection);

				var fileForArchives = new Queue<FileForArchive>();
				var exporter = new UnconfirmedOrdersExporter(updateData, helper, "results\\", fileForArchives);

				exporter.Export();

				Assert.That(exporter.LoadedOrders.Count, Is.EqualTo(0), "Не должно быть неподтвержденных заказов для клиента {0}", _client.Id);
				Assert.That(updateData.UnconfirmedOrders.Count, Is.EqualTo(0), "Не должно быть неподтвержденных заказов для клиента {0}", _client.Id);

				Assert.That(fileForArchives.Count, Is.EqualTo(0), "В очереди не должно быть файлов, т.к. нет неподтвержденных заказов для клиента {0}", _client.Id);
			}
		}

		[Test(Description = "Проверяем простой запрос данных без выгружаемых заказов")]
		public void SimpleLoadDataWithoutUnconfirmedOrders()
		{
			var extractFolder = Path.Combine(Path.GetFullPath(ServiceContext.GetResultPath()), "ExtractZip");
			if (Directory.Exists(extractFolder))
				Directory.Delete(extractFolder, true);
			Directory.CreateDirectory(extractFolder);

			try
			{
				var memoryAppender = new MemoryAppender();
				memoryAppender.AddFilter(new LoggerMatchFilter { AcceptOnMatch = true, LoggerToMatch = "PrgData", Next = new DenyAllFilter() });
				BasicConfigurator.Configure(memoryAppender);

				try
				{
					var responce = LoadData(false, _lastUpdateTime.ToUniversalTime(), _afAppVersion);

					var simpleUpdateId = ParseUpdateId(responce);

					var afterSimpleFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_*.zip".Format(_officeUser.Id));
					Assert.That(afterSimpleFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterSimpleFiles.Implode());
					Assert.That(afterSimpleFiles[0], Is.StringEnding("{0}_{1}.zip".Format(_officeUser.Id, simpleUpdateId)));

					var sendLogs = TestUnconfirmedOrdersSendLog.Queryable.Where(l => l.UpdateId == simpleUpdateId).ToList();
					Assert.That(sendLogs.Count, Is.EqualTo(0), "Не должно быть заказов, экспортированных пользователю");

					var service = new PrgDataEx();
					var updateTime = service.CommitExchange(simpleUpdateId, false);
					
					//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
					Thread.Sleep(3000);

					var addition = Convert.ToString(MySqlHelper.ExecuteScalar(
						Settings.ConnectionString(),
						"select Addition from logs.AnalitFUpdates where UpdateId = ?UpdateId",
						new MySqlParameter("?UpdateId", simpleUpdateId)));
					Assert.That(addition, Is.Not.StringContaining("Экспортированные неподтвержденные заказы: "), "Список экспортированных неподтвержденные заказов должен быть пустым");
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

		[Test(Description = "Проверям поддержку таблицы UnconfirmedOrdersSendLogs при работе с неподтвержденными заказами")]
		public void SupportUnconfirmedOrdersSendLog()
		{
			var firstOrder = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id);
			var secondOrder = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id);

			using (new TransactionScope())
			{
				var log = new TestUnconfirmedOrdersSendLog();
				log.User = _officeUser;
				log.OrderId = secondOrder.RowId;
				log.Create();
			}

			var responce = LoadData(false, _lastUpdateTime.ToUniversalTime(), _afAppVersion);

			var firstUpdateId = ParseUpdateId(responce);

			var sendLogs = TestUnconfirmedOrdersSendLog.Queryable.Where(l => l.UpdateId == firstUpdateId).ToList();
			Assert.That(sendLogs.Count, Is.EqualTo(2), "Должен быть 2 заказа, экспортированных пользователю в данном обновлении");
			Assert.That(sendLogs.Contains(l => l.OrderId == firstOrder.RowId), Is.True, "Номер экспортированного заказа не совпадает");
			Assert.That(sendLogs.Contains(l => l.OrderId == secondOrder.RowId), Is.True, "Номер экспортированного заказа не совпадает");
			Assert.That(sendLogs.All(l => l.User.Id == _officeUser.Id), Is.True, "Код пользователя не совпадает");
			Assert.That(sendLogs.All(l => !l.Committed), Is.True, "Код пользователя не совпадает");

			var thirdOrder = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id);

			var service = new PrgDataEx();
			var updateTime = service.CommitExchange(firstUpdateId, false);

			//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
			Thread.Sleep(3000);

			var thirdOrderSendLogs = TestUnconfirmedOrdersSendLog.Queryable.Where(l => l.UpdateId == firstUpdateId && l.OrderId == thirdOrder.RowId).ToList();
			Assert.That(thirdOrderSendLogs.Count, Is.EqualTo(0), "Неэкспортированный заказ {0} был добавлен в таблицы логов", thirdOrder.RowId);

			using (new SessionScope())
			{
				sendLogs.ForEach(l => l.Refresh());
				Assert.That(sendLogs.All(l => l.Committed), Is.True, "Имееются неподтвержденные заказы");
				Assert.That(sendLogs.All(l => l.UpdateId == firstUpdateId), Is.True, "В логе изменилось значение UpdateId");
			}

			var deletedStatusFirst = Convert.ToBoolean(
				MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select Deleted from orders.OrdersHead where RowId = ?OrderId",
					new MySqlParameter("?OrderId", firstOrder.RowId)));
			Assert.That(deletedStatusFirst, Is.True, "Неподтвержденный заказ {0} не помечен как удаленный", firstOrder.RowId);

			var deletedStatusSecond = Convert.ToBoolean(
				MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select Deleted from orders.OrdersHead where RowId = ?OrderId",
					new MySqlParameter("?OrderId", secondOrder.RowId)));
			Assert.That(deletedStatusSecond, Is.True, "Неподтвержденный заказ {0} не помечен как удаленный", secondOrder.RowId);

			var deletedStatusThird = Convert.ToBoolean(
				MySqlHelper.ExecuteScalar(
					Settings.ConnectionString(),
					"select Deleted from orders.OrdersHead where RowId = ?OrderId",
					new MySqlParameter("?OrderId", thirdOrder.RowId)));
			Assert.That(deletedStatusThird, Is.False, "Неподтвержденный заказ {0} помечен как удаленный", thirdOrder.RowId);

			var addition = Convert.ToString(MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				"select Addition from logs.AnalitFUpdates where UpdateId = ?UpdateId",
				new MySqlParameter("?UpdateId", firstUpdateId)));
			Assert.That(addition, Is.StringContaining("Экспортированные неподтвержденные заказы: {0}, {1}".Format(firstOrder.RowId, secondOrder.RowId)), "Неподтвержденный заказы {0}, {1} не содержатся в поле Addition", firstOrder.RowId, secondOrder.RowId);
		}

		[Test(Description = "Простой асинхронный запрос данных")]
		public void SimpleAsyncGetData()
		{
			try
			{
				var memoryAppender = new MemoryAppender();
				memoryAppender.AddFilter(new LoggerMatchFilter { AcceptOnMatch = true, LoggerToMatch = "PrgData", Next = new DenyAllFilter() });
				BasicConfigurator.Configure(memoryAppender);


				try
				{
					var firstAsyncResponse = CheckAsyncRequest(1);
					Assert.That(firstAsyncResponse, Is.StringStarting("Error=При выполнении Вашего запроса произошла ошибка."));

					var responce = LoadDataAsync(false, _lastUpdateTime.ToUniversalTime(), _afAppVersion);

					var simpleUpdateId = ParseUpdateId(responce);

					var afterAsyncFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_{1}.zip".Format(_officeUser.Id, simpleUpdateId));
					Assert.That(afterAsyncFiles.Length, Is.EqualTo(0), "Файлов быть не должно, т.к. это асинхронный запрос: {0}", afterAsyncFiles.Implode());

					var log = TestAnalitFUpdateLog.Find(Convert.ToUInt32(simpleUpdateId));
					Assert.That(log.Commit, Is.False, "Запрос не должен быть подтвержден");
					Assert.That(log.UserId, Is.EqualTo(_officeUser.Id));
					Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.GetDataAsync)).Or.EqualTo(Convert.ToUInt32(RequestType.GetCumulativeAsync)), "Не совпадает тип обновления");

					var asyncResponse = String.Empty;
					var sleepCount = 0;
					do
					{
						asyncResponse = CheckAsyncRequest(Convert.ToUInt64(simpleUpdateId));
						if (asyncResponse == "Res=Wait")
						{
							sleepCount++;
							Thread.Sleep(1000);
						}

					} while (asyncResponse == "Res=Wait" && sleepCount < 5*60);

					Assert.That(asyncResponse, Is.EqualTo("Res=OK"), "Неожидаемый ответ от сервера при проверке асинхронного запроса, sleepCount: {0}", sleepCount);

					log.Refresh();
					Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.GetData)).Or.EqualTo(Convert.ToUInt32(RequestType.GetCumulative)), "Не совпадает тип обновления");

					var afterAsyncRequestFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_{1}.zip".Format(_officeUser.Id, simpleUpdateId));
					Assert.That(afterAsyncRequestFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterAsyncRequestFiles.Implode());

					var service = new PrgDataEx();
					var updateTime = service.CommitExchange(simpleUpdateId, false);

					//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
					Thread.Sleep(3000);

					log.Refresh();
					Assert.That(log.Commit, Is.True, "Запрос не подтвержден");
					Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.GetData)).Or.EqualTo(Convert.ToUInt32(RequestType.GetCumulative)), "Не совпадает тип обновления");

					//var addition = Convert.ToString(MySqlHelper.ExecuteScalar(
					//    Settings.ConnectionString(),
					//    "select Addition from logs.AnalitFUpdates where UpdateId = ?UpdateId",
					//    new MySqlParameter("?UpdateId", simpleUpdateId)));
					//Assert.That(addition, Is.StringContaining("Экспортированные неподтвержденные заказы: {0}".Format(order.RowId)), "Неподтвержденный заказ {0} не содержится в поле Addition", order.RowId);
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

	}
}