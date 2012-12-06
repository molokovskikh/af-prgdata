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
		private MySqlConnection connection;

		[TestFixtureSetUp]
		public void FixtureSetUp()
		{
			_afAppVersion = "1.1.1.1413";
		}

		[SetUp]
		public void SetUp()
		{
			_client = TestClient.Create();

			using (var transaction = new TransactionScope()) {
				_officeUser = _client.Users[0];
				_officeUser.AllowDownloadUnconfirmedOrders = true;

				_drugstoreAddress = _client.CreateAddress();

				_drugstoreUser = _client.CreateUser();

				_drugstoreUser.JoinAddress(_drugstoreAddress);
				_officeUser.JoinAddress(_drugstoreAddress);

				_client.Users.Each(u => {
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
				foreach (var testActivePrice in prices) {
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

			connection = new MySqlConnection(Settings.ConnectionString());
			connection.Open();
		}

		[TearDown]
		public void TearDown()
		{
			CheckForErrors();
			connection.Dispose();
		}

		[Test(Description = "Проверяем, что использование NHibernate-сессии от текущего подключение не закрывает подключение и транзакцию")]
		public void TestNHibernateSession()
		{
			using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted)) {
				using (var session = IoC.Resolve<ISessionFactoryHolder>().SessionFactory.OpenSession(connection)) {
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

		[Test(Description = "проверяем работу класса Orders2StringConverter")]
		public void TestOrders2StringConverter()
		{
			var price = _drugstoreUser.GetActivePricesList()[1];

			var order = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, price.Id.PriceId);

			var converter = new Orders2StringConverter(new List<UnconfirmedOrderInfo> { new UnconfirmedOrderInfo(order) { ClientOrderId = 1 } }, 1, false);

			Assert.That(converter.OrderHead, Is.Not.Null);
			Assert.That(converter.OrderItems, Is.Not.Null);

			Assert.That(converter.OrderHead.ToString(), Is.StringStarting("{0}\t{1}".Format(1, _drugstoreAddress.Id)), "Не корректно выгружен заголовок заказа");
			Assert.That(converter.OrderItems.ToString(), Is.StringStarting("{0}\t{1}\t{2}".Format(1, 1, _drugstoreAddress.Id)), "Не корректно выгружен список позиций заказа");

			var columns = converter.OrderHead.ToString().Split('\t');
			Assert.That(columns.Length, Is.EqualTo(4), "Неожидаемое количество элементов, разделенных tab");

			converter = new Orders2StringConverter(new List<UnconfirmedOrderInfo> { new UnconfirmedOrderInfo(order) { ClientOrderId = 1 } }, 1, true);

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
			using (new TransactionScope()) {
				updateLog = new TestAnalitFUpdateLog();
				updateLog.RequestTime = DateTime.Now;
				updateLog.UpdateType = (uint)RequestType.GetData;
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

		[Test(Description = "Проверяем загрузку заказов в Exporter'е")]
		public void TestLoadOrders()
		{
			var prices = _drugstoreUser.GetActivePricesList();
			var orders = new List<Order>();
			for (int i = 0; i < 3; i++) {
				var order = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[i].Id.PriceId);
				orders.Add(order);
			}

			var exporter = InitExporter();
			var updateData = exporter.Data;
			var fileForArchives = exporter.FilesForArchive;

			Assert.That(updateData.UnconfirmedOrders.Count, Is.EqualTo(3));
			foreach (var order in orders)
				Assert.That(updateData.UnconfirmedOrders.Any(o => o.OrderId == order.RowId), Is.True, "Не найден заказ OrderId = {0}", order.RowId);

			exporter.UnionOrders();

			Assert.That(exporter.ExportedOrders.Count, Is.EqualTo(3));

			exporter.ExportOrders();

			Assert.That(fileForArchives.Count, Is.EqualTo(2));
		}

		private UnconfirmedOrdersExporter InitExporter()
		{
			var updateData = UpdateHelper.GetUpdateData(connection, _officeUser.Login);
			var helper = new UpdateHelper(updateData, connection);

			var fileForArchives = new Queue<FileForArchive>();
			var exporter = new UnconfirmedOrdersExporter(updateData, helper, "results\\", fileForArchives);
			exporter.Helper.MaintainReplicationInfo();
			exporter.Helper.SelectActivePricesFull();
			exporter.LoadOrders();
			return exporter;
		}

		[Test(Description = "Проверяем объединение заказов в Exporter'е")]
		public void TestUnionOrders()
		{
			var prices = _drugstoreUser.GetActivePricesList();
			var orders = new List<Order>();
			orders.Add(TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[3].Id.PriceId));
			orders.Add(TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[1].Id.PriceId));
			orders.Add(TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[3].Id.PriceId));
			orders.Add(TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id, prices[2].Id.PriceId));

			var exporter = InitExporter();
			var updateData = exporter.Data;

			Assert.That(updateData.UnconfirmedOrders.Count, Is.EqualTo(4));
			foreach (var order in orders)
				Assert.That(updateData.UnconfirmedOrders.Any(o => o.OrderId == order.RowId), Is.True, "Не найден заказ OrderId = {0}", order.RowId);

			exporter.UnionOrders();

			Assert.That(exporter.ExportedOrders.Count, Is.EqualTo(3));

			Assert.That(exporter.ExportedOrders.Any(o => o.Order.RowId == orders[0].RowId), Is.True);
			Assert.That(exporter.ExportedOrders.Any(o => o.Order.RowId == orders[1].RowId), Is.True);
			Assert.That(exporter.ExportedOrders.Any(o => o.Order.RowId == orders[3].RowId), Is.True);

			Assert.That(exporter.ExportedOrders[0].Order.RowCount, Is.EqualTo(6));

			Assert.That(
				updateData.UnconfirmedOrders.All(orderInfo => orderInfo.ClientOrderId.HasValue),
				Is.True,
				"Для всех экспортированных заказов должно быть выставлено поле ClientOrderId");

			Assert.That(updateData.UnconfirmedOrders[0].ClientOrderId, Is.Not.EqualTo(updateData.UnconfirmedOrders[1].ClientOrderId), "Ид заказов для экспорта клиенту не должен совпадать, т.к. это уникальные заказы");
			Assert.That(updateData.UnconfirmedOrders[0].ClientOrderId, Is.Not.EqualTo(updateData.UnconfirmedOrders[3].ClientOrderId), "Ид заказов для экспорта клиенту не должен совпадать, т.к. это уникальные заказы");
			Assert.That(updateData.UnconfirmedOrders[1].ClientOrderId, Is.Not.EqualTo(updateData.UnconfirmedOrders[3].ClientOrderId), "Ид заказов для экспорта клиенту не должен совпадать, т.к. это уникальные заказы");
			Assert.That(updateData.UnconfirmedOrders[0].ClientOrderId, Is.EqualTo(updateData.UnconfirmedOrders[2].ClientOrderId), "Значение должны совпадать, т.к. заказы объединяются в один заказ при экспорте клиенту");
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
				Assert.That(sendLogs[0].ExportedClientOrderId, Is.Not.Null, "Поле ExportedClientOrderId не установлено");
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
				s => {
					s.SaveOrUpdate(processedOrder);
					s.SaveOrUpdate(deletedOrder);
					s.SaveOrUpdate(submitedOrder);
				});

			var updateData = UpdateHelper.GetUpdateData(connection, _officeUser.Login);
			var helper = new UpdateHelper(updateData, connection);

			var fileForArchives = new Queue<FileForArchive>();
			var exporter = new UnconfirmedOrdersExporter(updateData, helper, "results\\", fileForArchives);

			exporter.Export();

			Assert.That(updateData.UnconfirmedOrders.Count, Is.EqualTo(0), "Не должно быть неподтвержденных заказов для клиента {0}", _client.Id);

			Assert.That(fileForArchives.Count, Is.EqualTo(0), "В очереди не должно быть файлов, т.к. нет неподтвержденных заказов для клиента {0}", _client.Id);
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

			using (new TransactionScope()) {
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
				Assert.That(sendLogs.All(l => l.ExportedClientOrderId.HasValue), Is.True, "Для заказов не установлено поле ExportedClientOrderId");
			}

			var thirdOrder = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id);

			var service = new PrgDataEx();
			var updateTime = service.CommitExchange(firstUpdateId, false);

			//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
			Thread.Sleep(3000);

			using (new SessionScope()) {
				var thirdOrderSendLogs = TestUnconfirmedOrdersSendLog.Queryable.Where(l => l.UpdateId == firstUpdateId && l.OrderId == thirdOrder.RowId).ToList();
				Assert.That(thirdOrderSendLogs.Count, Is.EqualTo(0), "Неэкспортированный заказ {0} был добавлен в таблицы логов", thirdOrder.RowId);

				sendLogs.ForEach(l => l.Refresh());
				Assert.That(sendLogs.All(l => l.Committed), Is.True, "Имееются неподтвержденные заказы");
				Assert.That(sendLogs.All(l => l.UpdateId == firstUpdateId), Is.True, "В логе изменилось значение UpdateId");
			}

			CheckOrderStatus(Tuple.Create(firstOrder, true), Tuple.Create(secondOrder, true), Tuple.Create(thirdOrder, false));

			var addition = Convert.ToString(MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				"select Addition from logs.AnalitFUpdates where UpdateId = ?UpdateId",
				new MySqlParameter("?UpdateId", firstUpdateId)));
			Assert.That(
				addition,
				Is.StringContaining("Экспортированные неподтвержденные заказы: {0}+{1}->{2}".Format(firstOrder.RowId, secondOrder.RowId, sendLogs[0].ExportedClientOrderId)),
				"Неподтвержденный заказы {0}, {1} не содержатся в поле Addition", firstOrder.RowId, secondOrder.RowId);
		}


		[Test(Description = "Простой запрос данных с получением сертификатов")]
		public void SimpleGetDataWithCertificates()
		{
			TestWaybill document;
			TestCertificateFile certificateFile;
			TestProduct product;
			using (new SessionScope()) {
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

			var responce = LoadDataAsyncDocs(false, _lastUpdateTime.ToUniversalTime(), "1.1.1.1571", new[] { document.Lines[0].Id });
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

			using (new SessionScope()) {
				var logs = TestCertificateRequestLog.Queryable.Where(l => l.Update.Id == simpleUpdateId).ToList();
				Assert.That(logs.Count, Is.EqualTo(1));
			}

			Assert.That(log.Log, Is.EqualTo(message));
		}

		[Test]
		public void Log_fail_secrificate_request()
		{
			TestWaybill document;
			using (new SessionScope()) {
				var builder = new DocumentBuilder(_client);
				document = builder.Build();
			}

			var responce = LoadDataAsyncDocs(false, _lastUpdateTime.ToUniversalTime(), "1.1.1.1571", new[] { document.Lines[0].Id });
			var simpleUpdateId = ShouldBeSuccessfull(responce);

			using (new SessionScope()) {
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

			using (new TransactionScope()) {
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

			using (new SessionScope()) {
				var thirdOrderSendLogs = TestUnconfirmedOrdersSendLog.Queryable.Where(l => l.UpdateId == firstUpdateId && l.OrderId == thirdOrder.RowId).ToList();
				Assert.That(thirdOrderSendLogs.Count, Is.EqualTo(0), "Неэкспортированный заказ {0} был добавлен в таблицы логов", thirdOrder.RowId);

				sendLogs.ForEach(l => l.Refresh());
				Assert.That(sendLogs.All(l => l.Committed), Is.True, "Имееются неподтвержденные заказы");
				Assert.That(sendLogs.All(l => l.UpdateId == firstUpdateId), Is.True, "В логе изменилось значение UpdateId");
			}

			CheckOrderStatus(Tuple.Create(firstOrder, true), Tuple.Create(secondOrder, true), Tuple.Create(thirdOrder, false));

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

		private static void CheckOrderStatus(params Tuple<Order, bool>[] orders)
		{
			foreach (var order in orders) {
				var deletedStatusFirst = Convert.ToBoolean(
					MySqlHelper.ExecuteScalar(
						Settings.ConnectionString(),
						"select Deleted from orders.OrdersHead where RowId = ?OrderId",
						new MySqlParameter("?OrderId", order.Item1.RowId)));
				Assert.That(deletedStatusFirst, Is.EqualTo(order.Item2), "Неподтвержденный заказ {0} не помечен как удаленный", order.Item1.RowId);
			}
		}

		[Test]
		public void Do_not_export_order_for_unavailable_price()
		{
			var order = TestDataManager.GenerateOrder(3, _drugstoreUser.Id, _drugstoreAddress.Id);
			MySqlHelper.ExecuteScalar(connection,
				"update Customers.Intersection set AvailableForClient = 0 where ClientId = ?clientId and PriceId = ?priceId",
				new MySqlParameter("clientId", _drugstoreUser.Client.Id),
				new MySqlParameter("priceId", order.PriceList.PriceCode));
			var exporter = InitExporter();

			Assert.That(exporter.Data.UnconfirmedOrders.Count, Is.EqualTo(0));
		}

		[Test(Description = "проверяем работу функции UnconfirmedOrderInfosToString")]
		public void CheckUnconfirmedOrderInfosToString()
		{
			Assert.That(UnconfirmedOrdersExporter.UnconfirmedOrderInfosToString(null), Is.EqualTo(String.Empty));
			Assert.That(UnconfirmedOrdersExporter.UnconfirmedOrderInfosToString(new List<UnconfirmedOrderInfo>()), Is.EqualTo(String.Empty));
			Assert.That(UnconfirmedOrdersExporter.UnconfirmedOrderInfosToString(
				new List<UnconfirmedOrderInfo> {
					new UnconfirmedOrderInfo(1, 3),
					new UnconfirmedOrderInfo(2, 4),
				}),
				Is.EqualTo("1->3, 2->4"));
			Assert.That(UnconfirmedOrdersExporter.UnconfirmedOrderInfosToString(
				new List<UnconfirmedOrderInfo> {
					new UnconfirmedOrderInfo(1, 3),
					new UnconfirmedOrderInfo(2, 4),
					new UnconfirmedOrderInfo(3, 3),
				}),
				Is.EqualTo("1+3->3, 2->4"));
			Assert.That(UnconfirmedOrdersExporter.UnconfirmedOrderInfosToString(
				new List<UnconfirmedOrderInfo> {
					new UnconfirmedOrderInfo(1, 3),
					new UnconfirmedOrderInfo(2, 4),
					new UnconfirmedOrderInfo(3, 3),
					new UnconfirmedOrderInfo(5, 5),
					new UnconfirmedOrderInfo(6, 6),
					new UnconfirmedOrderInfo(7, 5),
				}),
				Is.EqualTo("1+3->3, 2->4, 5+7->5, 6->6"));
			Assert.That(UnconfirmedOrdersExporter.UnconfirmedOrderInfosToString(
				new List<UnconfirmedOrderInfo> {
					new UnconfirmedOrderInfo(1, 3),
					new UnconfirmedOrderInfo(2, 4),
					new UnconfirmedOrderInfo(3, 3),
					new UnconfirmedOrderInfo(5, 5),
					new UnconfirmedOrderInfo(6, 6),
					new UnconfirmedOrderInfo(7, 5),
					new UnconfirmedOrderInfo(8, null),
					new UnconfirmedOrderInfo(9, 7),
					new UnconfirmedOrderInfo(10, null),
				}),
				Is.EqualTo("1+3->3, 2->4, 5+7->5, 6->6, 8->(неизвестно), 10->(неизвестно), 9->7"));
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