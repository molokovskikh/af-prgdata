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
using Integration.BaseTests;
using Test.Support.Documents;
using Test.Support.Suppliers;
using Test.Support.log4net;
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
	public class DownloadUnconfirmedOrdersFixture : PrepareDataFixture
	{
		private TestClient _client;
		private TestUser _officeUser;
		private TestUser _drugstoreUser;
		private TestAddress _drugstoreAddress;

		private string _afAppVersion;
		private DateTime _lastUpdateTime;

		[TestFixtureSetUp]
		public void FixtureSetUp()
		{
			_afAppVersion = "1.1.1.1413";

			FixtureSetup();
		}

		[SetUp]
		public void SetUp()
		{
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

			SessionHelper.WithSession(s => {
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
					"delete from Customers.UserPrices where UserId = :userId and PriceId not in (:priceIds);")
					.SetParameter("userId", _officeUser.Id)
					.SetParameterList("priceIds", newPrices.ToArray())
					.ExecuteUpdate();
			});

			_lastUpdateTime = GetLastUpdateTime(_officeUser);
			SetCurrentUser(_officeUser.Login);
			TestDataManager.DeleteAllOrdersForClient(_client.Id);

			RegisterLogger();
		}

		[TearDown]
		public void TearDown()
		{
			CheckForErrors();
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

			var order = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, price.Id.PriceId);

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
			var orderFirst = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id);
			var orderSecond = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id);

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
				var order = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[i].Id.PriceId);
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
					Assert.That(exporter.LoadedOrders.Any(o => o.RowId == order.RowId), Is.True, "Не найден заказ OrderId = {0}", order.RowId);
					Assert.That(updateData.UnconfirmedOrders.Any(o => o == order.RowId), Is.True, "Не найден заказ OrderId = {0}", order.RowId);
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
			orders.Add(TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[0].Id.PriceId));
			orders.Add(TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[1].Id.PriceId));
			orders.Add(TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[0].Id.PriceId));
			orders.Add(TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[2].Id.PriceId));

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
					Assert.That(exporter.LoadedOrders.Any(o => o.RowId == order.RowId), Is.True, "Не найден заказ OrderId = {0}", order.RowId);
					Assert.That(updateData.UnconfirmedOrders.Any(o => o == order.RowId), Is.True, "Не найден заказ OrderId = {0}", order.RowId);
				}

				exporter.UnionOrders();

				Assert.That(exporter.ExportedOrders.Count, Is.EqualTo(3));

				Assert.That(exporter.ExportedOrders.Any(o => o.RowId == orders[0].RowId), Is.True);
				Assert.That(exporter.ExportedOrders.Any(o => o.RowId == orders[1].RowId), Is.True);
				Assert.That(exporter.ExportedOrders.Any(o => o.RowId == orders[3].RowId), Is.True);

				Assert.That(exporter.ExportedOrders[0].RowCount, Is.EqualTo(6));
			}
		}

		[Test(Description = "Проверяем простой запрос данных с выгружаемыми заказами")]
		public void SimpleLoadData()
		{
			var extractFolder = Path.Combine(Path.GetFullPath(ServiceContext.GetResultPath()), "ExtractZip");
			if (Directory.Exists(extractFolder))
				Directory.Delete(extractFolder, true);
			Directory.CreateDirectory(extractFolder);

			var order = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id);

			var responce = LoadData(false, _lastUpdateTime.ToUniversalTime(), _afAppVersion);
			var simpleUpdateId = ShouldBeSuccessfull(responce);

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

			using (new SessionScope()) {
				var sendLogs = TestUnconfirmedOrdersSendLog.Queryable.Where(l => l.UpdateId == simpleUpdateId).ToList();
				Assert.That(sendLogs.Count, Is.EqualTo(1), "Должен быть один заказ, экспортированный пользователю в данном обновлении");
				Assert.That(sendLogs[0].OrderId, Is.EqualTo(order.RowId), "Номер экспортированного заказа не совпадает");
				Assert.That(sendLogs[0].User.Id, Is.EqualTo(_officeUser.Id), "Код пользователя не совпадает");
			}

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

		[Test(Description = "Попытка загружить заказы, когда нет неподтвержденных заказов")]
		public void LoadOrdersOnNonExistsUnconfirmedOrders()
		{
			var prices = _drugstoreUser.GetActivePricesList();
			var processedOrder = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[0].Id.PriceId);
			processedOrder.Processed = true;

			var deletedOrder = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[1].Id.PriceId);
			deletedOrder.Deleted = true;

			var submitedOrder = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[2].Id.PriceId);
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

			var responce = LoadData(false, _lastUpdateTime.ToUniversalTime(), _afAppVersion);
			var simpleUpdateId = ShouldBeSuccessfull(responce);

			var afterSimpleFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_*.zip".Format(_officeUser.Id));
			Assert.That(afterSimpleFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterSimpleFiles.Implode());
			Assert.That(afterSimpleFiles[0], Is.StringEnding("{0}_{1}.zip".Format(_officeUser.Id, simpleUpdateId)));

			using (new SessionScope()) {
				var sendLogs = TestUnconfirmedOrdersSendLog.Queryable.Where(l => l.UpdateId == simpleUpdateId).ToList();
				Assert.That(sendLogs.Count, Is.EqualTo(0), "Не должно быть заказов, экспортированных пользователю");
			}

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

		[Test(Description = "Проверям поддержку таблицы UnconfirmedOrdersSendLogs при работе с неподтвержденными заказами")]
		public void SupportUnconfirmedOrdersSendLog()
		{
			var firstOrder = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id);
			var secondOrder = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id);

			using (new TransactionScope())
			{
				var log = new TestUnconfirmedOrdersSendLog();
				log.User = _officeUser;
				log.OrderId = secondOrder.RowId;
				log.Create();
			}

			var responce = LoadData(false, _lastUpdateTime.ToUniversalTime(), _afAppVersion);
			var firstUpdateId = ShouldBeSuccessfull(responce);

			List<TestUnconfirmedOrdersSendLog> sendLogs;
			using (new SessionScope()) {
				sendLogs = TestUnconfirmedOrdersSendLog.Queryable.Where(l => l.UpdateId == firstUpdateId).ToList();
				Assert.That(sendLogs.Count, Is.EqualTo(2), "Должен быть 2 заказа, экспортированных пользователю в данном обновлении");
				Assert.That(sendLogs.Any(l => l.OrderId == firstOrder.RowId), Is.True, "Номер экспортированного заказа не совпадает");
				Assert.That(sendLogs.Any(l => l.OrderId == secondOrder.RowId), Is.True, "Номер экспортированного заказа не совпадает");
				Assert.That(sendLogs.All(l => l.User.Id == _officeUser.Id), Is.True, "Код пользователя не совпадает");
				Assert.That(sendLogs.All(l => !l.Committed), Is.True, "Код пользователя не совпадает");
			}

			var thirdOrder = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id);

			var service = new PrgDataEx();
			var updateTime = service.CommitExchange(firstUpdateId, false);

			//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
			Thread.Sleep(3000);

			using (new SessionScope())
			{
				var thirdOrderSendLogs = TestUnconfirmedOrdersSendLog.Queryable.Where(l => l.UpdateId == firstUpdateId && l.OrderId == thirdOrder.RowId).ToList();
				Assert.That(thirdOrderSendLogs.Count, Is.EqualTo(0), "Неэкспортированный заказ {0} был добавлен в таблицы логов", thirdOrder.RowId);

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


		[Test(Description = "Простой запрос данных с получением сертификатов")]
		public void SimpleGetDataWithCertificates()
		{
			TestWaybill document;
			TestCertificateFile certificateFile;
			TestProduct product;
			using(new SessionScope())
			{
				var builder = new DocumentBuilder(_client);
				document = builder.Build();
				product = builder.Product;

				var certificate = new TestCertificate(product.CatalogProduct, "20111226");
				certificateFile = certificate.NewFile(new TestCertificateFile(builder.Source));
				document.Lines[0].Certificate = certificate;

				certificate.Save();
				document.Save();

				product.CatalogProduct.Refresh();
			}

			var certificatePath = "results\\Certificates";
			if (!Directory.Exists(certificatePath))
				Directory.CreateDirectory(certificatePath);

			File.WriteAllBytes(Path.Combine(certificatePath, String.Format("{0}.tif", certificateFile.Id)), new byte[0]);

			var responce = LoadDataAsyncDocs(false, _lastUpdateTime.ToUniversalTime(), "1.1.1.1571", new[] {document.Lines[0].Id});
			var simpleUpdateId = ShouldBeSuccessfull(responce);

			var log = TestAnalitFUpdateLog.Find(Convert.ToUInt32(simpleUpdateId));
			var afterAsyncRequestFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_{1}.zip".Format(_officeUser.Id, simpleUpdateId));
			Assert.That(afterAsyncRequestFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterAsyncRequestFiles.Implode());

			var service = new PrgDataEx();
			var updateTime = service.CommitExchange(simpleUpdateId, true);

			//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
			Thread.Sleep(3000);

			log.Refresh();
			Assert.That(log.Commit, Is.True, "Запрос не подтвержден");
			Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.GetDocs)), "Не совпадает тип обновления");


			var message = String.Format(@"Отправлены сертификаты:
Номер документа = {0}, Сопоставленный продукт = {1}, Файл = {2}.tif
",
				document.Log.Id, product.CatalogProduct.Name, certificateFile.Id);

			using(new SessionScope())
			{
				var logs = TestCertificateRequestLog.Queryable.Where(l => l.Update.Id == simpleUpdateId).ToList();
				Assert.That(logs.Count, Is.EqualTo(1));
			}

			Assert.That(log.Log, Is.EqualTo(message));
		}

		[Test]
		public void Log_fail_secrificate_request()
		{
			TestWaybill document;
			using(new SessionScope())
			{
				var builder = new DocumentBuilder(_client);
				document = builder.Build();
			}

			var responce = LoadDataAsyncDocs(false, _lastUpdateTime.ToUniversalTime(), "1.1.1.1571", new[] {document.Lines[0].Id});
			var simpleUpdateId = ShouldBeSuccessfull(responce);

			using(new SessionScope())
			{
				var logs = TestCertificateRequestLog.Queryable.Where(l => l.Update.Id == simpleUpdateId).ToList();
				Assert.That(logs.Count, Is.EqualTo(1));
				Assert.That(logs[0].Filename, Is.Null);
			}
		}

		[Test(Description = "при запросе частичного КО должен быть сброшен статус доставки для уже выгруженных неподтвержденных заказов")]
		public void ResetUnconfirmedOrdersSendLogAfterCumulative()
		{
			var firstOrder = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id);
			var secondOrder = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id);

			using (new TransactionScope())
			{
				var log = new TestUnconfirmedOrdersSendLog();
				log.User = _officeUser;
				log.OrderId = secondOrder.RowId;
				log.Create();
			}

			var responce = LoadData(false, _lastUpdateTime.ToUniversalTime(), _afAppVersion);
			var firstUpdateId = ShouldBeSuccessfull(responce);

			List<TestUnconfirmedOrdersSendLog> sendLogs;
			using (new SessionScope()) {
				sendLogs = TestUnconfirmedOrdersSendLog.Queryable.Where(l => l.UpdateId == firstUpdateId).ToList();
				Assert.That(sendLogs.Count, Is.EqualTo(2), "Должен быть 2 заказа, экспортированных пользователю в данном обновлении");
				Assert.That(sendLogs.Any(l => l.OrderId == firstOrder.RowId), Is.True, "Номер экспортированного заказа не совпадает");
				Assert.That(sendLogs.Any(l => l.OrderId == secondOrder.RowId), Is.True, "Номер экспортированного заказа не совпадает");
				Assert.That(sendLogs.All(l => l.User.Id == _officeUser.Id), Is.True, "Код пользователя не совпадает");
				Assert.That(sendLogs.All(l => !l.Committed), Is.True, "Код пользователя не совпадает");
			}

			var thirdOrder = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id);

			var service = new PrgDataEx();
			var updateTime = service.CommitExchange(firstUpdateId, false);

			//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
			Thread.Sleep(3000);

			using (new SessionScope())
			{
				var thirdOrderSendLogs = TestUnconfirmedOrdersSendLog.Queryable.Where(l => l.UpdateId == firstUpdateId && l.OrderId == thirdOrder.RowId).ToList();
				Assert.That(thirdOrderSendLogs.Count, Is.EqualTo(0), "Неэкспортированный заказ {0} был добавлен в таблицы логов", thirdOrder.RowId);

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

			var secondResponce = LoadData(false, _lastUpdateTime.ToUniversalTime(), _afAppVersion);
			var secondUpdateId = ShouldBeSuccessfull(secondResponce);

			using (new SessionScope()) {
				var sendLogsAfterCumulative = TestUnconfirmedOrdersSendLog.Queryable.Where(l => l.UpdateId == secondUpdateId).ToList();
				Assert.That(sendLogsAfterCumulative.Count, Is.EqualTo(3), "Должен быть 3 заказа, экспортированных пользователю в данном обновлении");
				Assert.That(sendLogsAfterCumulative.Any(l => l.OrderId == firstOrder.RowId), Is.True, "Не найден номер экспортированного заказа {0}", firstOrder.RowId);
				Assert.That(sendLogsAfterCumulative.Any(l => l.OrderId == secondOrder.RowId), Is.True, "Не найден номер экспортированного заказа {0}", secondOrder.RowId);
				Assert.That(sendLogsAfterCumulative.Any(l => l.OrderId == thirdOrder.RowId), Is.True, "Не найден номер экспортированного заказа {0}", thirdOrder.RowId);
				Assert.That(sendLogsAfterCumulative.All(l => l.User.Id == _officeUser.Id), Is.True, "Код пользователя не совпадает");
				Assert.That(sendLogsAfterCumulative.All(l => !l.Committed), Is.True, "Есть подтвержденные заказы");
			}
		}

	}

	public class DocumentBuilder
	{
		public TestSupplier Supplier;
		public TestProduct Product;
		public TestCertificateSource Source;
		public TestClient Client;

		public DocumentBuilder(TestClient client)
		{
			Client = client;
		}

		public TestWaybill Build()
		{
			Supplier = TestSupplier.Create();
			Source = new TestCertificateSource(Supplier);

			Product = new TestProduct("Тестовый продукт");

			var documentLog = new TestDocumentLog(Supplier, Client);
			var document = new TestWaybill(documentLog);
			document.Lines.Add(new TestWaybillLine {
				CatalogProduct = Product,
				Waybill = document,
			});

			Source.Save();
			Product.Save();
			document.Save();
			return document;
		}
	}
}