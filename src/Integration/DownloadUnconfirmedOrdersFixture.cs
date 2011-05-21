using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
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

				var permission = TestUserPermission.ByShortcut("AF");
				_client.Users.Each(u =>
				{
					u.AssignedPermissions.Add(permission);
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

					Assert.That(newPrices.Count, Is.EqualTo(4), "�� ����� ����������� ���-�� �����-������ ��� ������");

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
			//����� ���������� ������������, ����� ��������� �� ���� ����� � ���� ��������� ������� ������ �������,
			//����� ��������� ��� �������� ������������ ������� ���������� ������������
			simpleUpdateTime = simpleUpdateTime.Date
				.AddHours(simpleUpdateTime.Hour)
				.AddMinutes(simpleUpdateTime.Minute)
				.AddSeconds(simpleUpdateTime.Second);

			_officeUser.UpdateInfo.UpdateDate = simpleUpdateTime;
			_officeUser.Update();

			return simpleUpdateTime;
		}

		[Test(Description = "���������, ��� ������������� NHibernate-������ �� �������� ����������� �� ��������� ����������� � ����������")]
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

		[Test(Description = "��������� ������ ������ Orders2StringConverter")]
		public void TestOrders2StringConverter()
		{
			var price = _drugstoreUser.GetActivePricesList()[0];

			var order = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id, price.Id.PriceId);

			var converter = new Orders2StringConverter(new List<Order> {order}, 1, 1);

			Assert.That(converter.OrderHead, Is.Not.Null);
			Assert.That(converter.OrderItems, Is.Not.Null);

			Assert.That(converter.OrderHead.ToString(), Is.StringStarting("{0}\t{1}".Format(1, _drugstoreAddress.Id)), "�� ��������� �������� ��������� ������");
			Assert.That(converter.OrderItems.ToString(), Is.StringStarting("{0}\t{1}\t{2}".Format(1, 1, _drugstoreAddress.Id)), "�� ��������� �������� ������ ������� ������");
		}

		[Test(Description = "��������� �������� ���������������� ������� ��� ������������� ����������")]
		public void DeleteUnconfirmedOrders()
		{
			var orderFirst = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id);
			var orderSecond = TestDataManager.GenerateOrderForFutureUser(3, _drugstoreUser.Id, _drugstoreAddress.Id);

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _officeUser.Login);

				var unconfirmedOrdersCount = MySqlHelper.ExecuteScalar(
					connection,
					"select count(*) from orders.OrdersHead oh where oh.ClientCode = ?clientId and deleted = 0 and submited = 0",
					new MySqlParameter("?clientId", _client.Id));
				Assert.That(unconfirmedOrdersCount, Is.EqualTo(2));

				UnconfirmedOrdersExporter.DeleteUnconfirmedOrders(updateData, connection);

				var unconfirmedOrdersCountAfterDelete = MySqlHelper.ExecuteScalar(
					connection,
					"select count(*) from orders.OrdersHead oh where oh.ClientCode = ?clientId and deleted = 1 and submited = 0 and (RowId = ?firstId or RowId = ?secondId)",
					new MySqlParameter("?clientId", _client.Id),
					new MySqlParameter("?firstId", orderFirst.RowId),
					new MySqlParameter("?secondId", orderSecond.RowId));
				Assert.That(unconfirmedOrdersCountAfterDelete, Is.EqualTo(2));
			}
		}

		[Test(Description = "��������� �������� ������� � Exporter'�")]
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
				foreach (var order in orders)
					Assert.That(exporter.LoadedOrders.Contains(o => o.RowId == order.RowId), Is.True, "�� ������ ����� OrderId = {0}", order.RowId);

				exporter.UnionOrders();

				Assert.That(exporter.ExportedOrders.Count, Is.EqualTo(3));

				exporter.ExportOrders();

				Assert.That(fileForArchives.Count, Is.EqualTo(2));
			}
		}

		[Test(Description = "��������� ����������� ������� � Exporter'�")]
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
				foreach (var order in orders)
					Assert.That(exporter.LoadedOrders.Contains(o => o.RowId == order.RowId), Is.True, "�� ������ ����� OrderId = {0}", order.RowId);

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

		private uint ParseUpdateId(string responce)
		{
			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
				return Convert.ToUInt32(match);

			Assert.Fail("�� ������ ����� UpdateId � ������ �������: {0}", responce);
			return 0;
		}

		[Test(Description = "��������� ������� ������ ������ � ������������ ��������")]
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
					Assert.That(afterSimpleFiles.Length, Is.EqualTo(1), "����������� ������ ������ ����� ���������� ����������: {0}", afterSimpleFiles.Implode());
					Assert.That(afterSimpleFiles[0], Is.StringEnding("{0}_{1}.zip".Format(_officeUser.Id, simpleUpdateId)));

					ArchiveHelper.Extract(afterSimpleFiles[0], "*.*", extractFolder);

					var rootFiles = Directory.GetFiles(extractFolder);

					var headFile =
						rootFiles.FirstOrDefault(
							file => file.EndsWith("CurrentOrderHeads{0}.txt".Format(_officeUser.Id), StringComparison.OrdinalIgnoreCase));
					Assert.That(headFile, Is.Not.Null.And.Not.Empty, "�� ������ ���� � ���������� ������");
					Assert.That(new FileInfo(headFile).Length, Is.GreaterThan(0), "���� � ���������� ������ �������� ������");

					var listFile =
						rootFiles.FirstOrDefault(
							file => file.EndsWith("CurrentOrderLists{0}.txt".Format(_officeUser.Id), StringComparison.OrdinalIgnoreCase));
					Assert.That(listFile, Is.Not.Null.And.Not.Empty, "�� ������ ���� �� ������� ������� ������");
					Assert.That(new FileInfo(listFile).Length, Is.GreaterThan(0), "���� �� ������� ������� ������ �������� ������");

					Directory.Delete(extractFolder, true);

					var service = new PrgDataEx();
					var updateTime = service.CommitExchange(simpleUpdateId, false);

					var deletedStatus = Convert.ToBoolean(
						MySqlHelper.ExecuteScalar(
							Settings.ConnectionString(),
							"select Deleted from orders.OrdersHead where RowId = ?OrderId",
							new MySqlParameter("?OrderId", order.RowId)));
					Assert.That(deletedStatus, Is.True, "���������������� ����� {0} �� ������� ��� ���������", order.RowId);
				}
				catch
				{
					var logEvents = memoryAppender.GetEvents();
					Console.WriteLine("������ ��� ���������� ������:\r\n{0}", logEvents.Select(item =>
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
				Assert.That(errors.Count(), Is.EqualTo(0), "��� ���������� ������ �������� ������:\r\n{0}", errors.Select(item => item.RenderedMessage).Implode("\r\n"));
			}
			finally
			{
				LogManager.ResetConfiguration();
			}
		}

		[Test(Description = "������� ��������� ������, ����� ��� ���������������� �������")]
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

				Assert.That(exporter.LoadedOrders.Count, Is.EqualTo(0), "�� ������ ���� ���������������� ������� ��� ������� {0}", _client.Id);

				Assert.That(fileForArchives.Count, Is.EqualTo(0), "� ������� �� ������ ���� ������, �.�. ��� ���������������� ������� ��� ������� {0}", _client.Id);
			}
		}

	}
}