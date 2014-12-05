using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Castle.ActiveRecord;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Repositories;
using Common.Models.Tests.Repositories;
using Common.MySql;
using Common.Tools;
using Inforoom.Common;
using Integration.BaseTests;
using LumiSoft.Net.Mime.vCard;
using MySql.Data.MySqlClient;
using NHibernate;
using NHibernate.Linq;
using NUnit.Framework;
using PrgData;
using PrgData.Common;
using SmartOrderFactory;
using SmartOrderFactory.Domain;
using SmartOrderFactory.Repositories;
using Test.Support;
using Test.Support.Logs;
using MySqlHelper = MySql.Data.MySqlClient.MySqlHelper;


namespace Integration
{
	[TestFixture]
	public class SmartOrderHelperFixture : PrepareDataFixture
	{
		private TestClient _client;
		private TestUser _user;
		private TestAddress _address;

		[SetUp]
		public void SetUp()
		{
			_user = CreateUser();
			_client = _user.Client;
			_address = _client.Addresses[0];
			new UnitOfWork(session);
		}

		[TearDown]
		public void Teardown()
		{
			if (UnitOfWork.Current != null)
				UnitOfWork.Current.Dispose();
		}

		[Test]
		[ExpectedException(typeof(UpdateException), ExpectedMessage = "Услуга 'АвтоЗаказ' не предоставляется")]
		public void Get_UpdateException_on_disabled_EnableSmartOrder()
		{
			FlushAndCommit();
			var updateData = UpdateHelper.GetUpdateData((MySqlConnection)session.Connection, _user.Login);
			var smartHelper = new SmartOrderHelper(updateData, _address.Id, 1, 1, 1);
		}

		[Test]
		[ExpectedException(typeof(UpdateException), ExpectedMessage = "Не настроены правила для автоматического формирования заказа")]
		public void Get_UpdateException_on_null_SmartOrderRuleId()
		{
			_client.Settings.EnableSmartOrder = true;

			FlushAndCommit();
			var updateData = UpdateHelper.GetUpdateData((MySqlConnection)session.Connection, _user.Login);
			var smartHelper = new SmartOrderHelper(updateData, _address.Id, 1, 1, 1);
		}

		[Test]
		public void SimpleSmartOrder()
		{
			Configure();
			FlushAndCommit();

			var updateData = UpdateHelper.GetUpdateData((MySqlConnection)session.Connection, _user.Login);

			var smartHelper = new SmartOrderHelper(updateData, _address.Id, 1, 1, 1);

			var batchFileBytes = File.ReadAllBytes("TestData\\TestOrderSmall.7z");
			Assert.That(batchFileBytes.Length, Is.GreaterThan(0), "Файл с дефектурой оказался пуст, возможно, его нет в папке");

			var batchFile = Convert.ToBase64String(batchFileBytes);

			try {
				smartHelper.PrepareBatchFile(batchFile);

				smartHelper.ProcessBatchFile();

				var fileInfo = new FileInfo(smartHelper.BatchReportFileName);
				Assert.That(fileInfo.Length, Is.GreaterThan(0), "Файл с отчетом АвтоЗаказа оказался пустым");
				fileInfo = new FileInfo(smartHelper.BatchOrderFileName);
				Assert.That(fileInfo.Length, Is.GreaterThanOrEqualTo(0), "Файл с заголовками заказов не существует");
				fileInfo = new FileInfo(smartHelper.BatchOrderItemsFileName);
				Assert.That(fileInfo.Length, Is.GreaterThanOrEqualTo(0), "Файл с содержимым заказов не существует");
			}
			finally {
				smartHelper.DeleteTemporaryFiles();
			}
		}

		[Test(Description = "Позиции в отчете должны сохраняться в зависимости от созданного заказа")]
		public void TestReportByAddress()
		{
			var newAddress = _client.CreateAddress();
			newAddress.LegalEntity = _address.LegalEntity;
			_user.JoinAddress(newAddress);

			Configure();
			FlushAndCommit();

			var realUser = IoC.Resolve<IRepository<User>>().Load(_user.Id);
			var firstAddress = IoC.Resolve<IRepository<Address>>().Load(_address.Id);
			var secondAddress = IoC.Resolve<IRepository<Address>>().Load(newAddress.Id);

			var updateData = UpdateHelper.GetUpdateData((MySqlConnection)session.Connection, _user.Login);

			var smartHelper = new SmartOrderHelper(updateData, _address.Id, 1, 4, 7);

			try {
				var activePrice = new ActivePrice { Id = new PriceKey(new PriceList()) };
				var orders = new List<Order> {
					new Order(activePrice, realUser, firstAddress, new OrderRules()),
					new Order(activePrice, realUser, secondAddress, new OrderRules())
				};

				var firstOffer = new Offer { ProductId = 1, Id = new OfferKey(1, 1) };
				var firstOrderItem = orders[0].AddOrderItem(firstOffer, 1);
				var secondOffer = new Offer { ProductId = 2, Id = new OfferKey(2, 1) };
				var secondOrderItem = orders[1].AddOrderItem(secondOffer, 1);

				var firstReducedOffer = new ReducedOffer { Id = new OfferKey(1, 1), ProductId = firstOffer.ProductId };
				var secondReducedOffer = new ReducedOffer { Id = new OfferKey(1, 1), ProductId = secondOffer.ProductId };
				var item1 = new OrderBatchItem(null) {
					Code = "456",
					ProductName = "test1",
					Item = new ItemToOrder(1, 1, null, 1) {
						MinimalCostOffer = firstReducedOffer,
						Status = ItemToOrderStatus.Ordered,
					}
				};
				item1.Item.RegisterOrderItem(firstOrderItem, firstReducedOffer);
				var item2 = new OrderBatchItem(null) {
					Code = "789",
					ProductName = "test2",
					Item = new ItemToOrder(2, 1, null, 1) {
						MinimalCostOffer = secondReducedOffer,
						Status = ItemToOrderStatus.Ordered,
					}
				};
				item2.Item.RegisterOrderItem(secondOrderItem, secondReducedOffer);
				var items = new List<OrderBatchItem> {
					new OrderBatchItem(null) { Code = "123", ProductName = "test0" },
					item1,
					item2
				};

				smartHelper.SaveToFile(items, orders);

				var fileInfo = new FileInfo(smartHelper.BatchOrderFileName);
				Assert.That(fileInfo.Length, Is.GreaterThan(0), "Файл с заголовками заказов оказался пустым");

				var ordersContent = File.ReadAllLines(smartHelper.BatchOrderFileName);
				Assert.That(ordersContent.Length, Is.EqualTo(2));
				Assert.That(ordersContent[0], Is.StringStarting("1\t{0}".Format(firstAddress.Id)));
				Assert.That(ordersContent[1], Is.StringStarting("2\t{0}".Format(secondAddress.Id)));

				fileInfo = new FileInfo(smartHelper.BatchOrderItemsFileName);
				Assert.That(fileInfo.Length, Is.GreaterThan(0), "Файл с содержимым заказов оказался пустым");

				var orderItemsContent = File.ReadAllLines(smartHelper.BatchOrderItemsFileName);
				Assert.That(orderItemsContent.Length, Is.EqualTo(2));
				Assert.That(orderItemsContent[0], Is.StringStarting("4\t1\t{0}".Format(firstAddress.Id)));
				Assert.That(orderItemsContent[1], Is.StringStarting("5\t2\t{0}".Format(secondAddress.Id)));

				fileInfo = new FileInfo(smartHelper.BatchReportFileName);
				Assert.That(fileInfo.Length, Is.GreaterThan(0), "Файл с отчетом АвтоЗаказа оказался пустым");

				var reportContent = File.ReadAllLines(smartHelper.BatchReportFileName);
				Assert.That(reportContent.Length, Is.EqualTo(3));
				Assert.That(reportContent[0], Is.StringStarting("7\t{0}".Format(firstAddress.Id)));
				Assert.That(reportContent[1], Is.StringStarting("8\t{0}".Format(firstAddress.Id)));
				Assert.That(reportContent[2], Is.StringStarting("9\t{0}".Format(secondAddress.Id)));
			}
			finally {
				smartHelper.DeleteTemporaryFiles();
			}
		}


		[Test]
		public void CheckBatchSave()
		{
			var appVersion = "1.1.1.1300";
			Configure();

			SetCurrentUser(_user.Login);
			MySqlHelper.ExecuteScalar(
				(MySqlConnection)session.Connection,
				"update Customers.Users set SaveAFDataFiles = 1 where Id = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

			var updateData = UpdateHelper.GetUpdateData((MySqlConnection)session.Connection, _user.Login);

			FlushAndCommit();
			var smartHelper = new SmartOrderHelper(updateData, _address.Id, 1, 1, 1);

			var batchFileBytes = File.ReadAllBytes("TestData\\TestOrderSmall.7z");
			Assert.That(batchFileBytes.Length, Is.GreaterThan(0), "Файл с дефектурой оказался пуст, возможно, его нет в папке");

			var batchFile = Convert.ToBase64String(batchFileBytes);

			var postBatchResponce = String.Empty;
			FoldersHelper.CheckTempFolders(() => { postBatchResponce = PostOrderBatch(false, DateTime.Now, appVersion, _user.AvaliableAddresses[0].Id, batchFile); });

			var postBatchUpdateId = ParseUpdateId(postBatchResponce);
			Assert.That(File.Exists(Path.Combine("results", "Archive", _user.Id.ToString(), postBatchUpdateId + "_Batch.7z")), Is.True);
		}

		[Test(Description = "Попытка выполнить разбор дефектуры с некорректным форматом файла")]
		public void SmartOrderWithErrorFile()
		{
			var appVersion = "1.1.1.1300";

			Configure();

			FlushAndCommit();
			SetCurrentUser(_user.Login);
			MySqlHelper.ExecuteScalar(
				(MySqlConnection)session.Connection,
				"update Customers.Users set SaveAFDataFiles = 1 where Id = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

			var batchFileBytes = File.ReadAllBytes("TestData\\TestOrderError.7z");
			Assert.That(batchFileBytes.Length, Is.GreaterThan(0), "Файл с дефектурой оказался пуст, возможно, его нет в папке");

			var batchFile = Convert.ToBase64String(batchFileBytes);

			FoldersHelper.CheckTempFolders(() => {
				var service = new PrgDataEx();

				var postBatchResponce = service.PostOrderBatch(DateTime.Now, false, appVersion, 50, UniqueId, "", "", new uint[] { }, _user.AvaliableAddresses[0].Id, batchFile, 1, 1, 1);

				Assert.That(postBatchResponce, Is.EqualTo("Error=Не удалось разобрать дефектуру.;Desc=Проверьте корректность формата файла дефектуры."));
			});

			var exception = new IndexOutOfRangeException();
			var lastUpdate = TestAnalitFUpdateLog.Queryable.Where(updateLog => updateLog.UserId == _user.Id).OrderByDescending(l => l.Id).First();
			Assert.That(lastUpdate.UpdateType, Is.EqualTo(TestRequestType.Error), "Не совпадает тип обновления");
			Assert.That(lastUpdate.Addition, Is.StringContaining("Ошибка при разборе дефектуры: " + exception.Message));
		}

		private void Configure()
		{
			_client.Settings.SmartOrderRule = new TestSmartOrderRule {
				OffersClientCode = null,
				AssortmentPriceCode = session.Query<TestPrice>().First().Id,
				UseOrderableOffers = true,
				ParseAlgorithm = "TestSource"
			};
			_client.Settings.EnableSmartOrder = true;
		}

		[Test(Description = "Попытка выполнить обработку дефектуры и получить ошибку в процессе обработки")]
		public void SmartOrderWithErrorOnProcess()
		{
			var appVersion = "1.1.1.1300";
			Configure();

			FlushAndCommit();
			SetCurrentUser(_user.Login);

			MySqlHelper.ExecuteScalar(
				(MySqlConnection)session.Connection,
				"update Customers.Users set SaveAFDataFiles = 1 where Id = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

			var batchFileBytes = File.ReadAllBytes("TestData\\TestOrderSmall.7z");
			Assert.That(batchFileBytes.Length, Is.GreaterThan(0), "Файл с дефектурой оказался пуст, возможно, его нет в папке");

			var batchFile = Convert.ToBase64String(batchFileBytes);

			SmartOrderHelper.raiseException = true;
			try {
				FoldersHelper.CheckTempFolders(() => {
					var service = new PrgDataEx();

					var postBatchResponce = service.PostOrderBatch(DateTime.Now, false, appVersion, 50, UniqueId, "", "", new uint[] { }, _user.AvaliableAddresses[0].Id, batchFile, 1, 1, 1);

					Assert.That(postBatchResponce, Is.EqualTo("Error=Отправка дефектуры завершилась неудачно.;Desc=Пожалуйста, повторите попытку через несколько минут."));
				});
			}
			finally {
				SmartOrderHelper.raiseException = false;
			}

			var lastUpdate = TestAnalitFUpdateLog.Queryable.Where(updateLog => updateLog.UserId == _user.Id).OrderByDescending(l => l.Id).First();
			Assert.That(lastUpdate.UpdateType, Is.EqualTo(TestRequestType.Error), "Не совпадает тип обновления");
			Assert.That(lastUpdate.Addition, Is.StringContaining("Ошибка при обработке дефектуры\r\nSystem.Exception: Тестовое исключение при обработке дефектуры"));
		}

		[Test(Description = "Попытка выполнить обработку дефектуры повторно после получения ошибки EmptyOffersListException")]
		public void SmartOrderWithErrorOnEmptyOffersListException()
		{
			var appVersion = "1.1.1.1300";
			var updateTime = GetLastUpdateTime(_user);
			Configure();

			FlushAndCommit();
			SetCurrentUser(_user.Login);

			MySqlHelper.ExecuteScalar(
				(MySqlConnection)session.Connection,
				"update Customers.Users set SaveAFDataFiles = 1 where Id = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

			var batchFileBytes = File.ReadAllBytes("TestData\\TestOrderSmall.7z");
			Assert.That(batchFileBytes.Length, Is.GreaterThan(0), "Файл с дефектурой оказался пуст, возможно, его нет в папке");

			var batchFile = Convert.ToBase64String(batchFileBytes);

			SmartOrderHelper.raiseExceptionOnEmpty = true;
			try {
				FoldersHelper.CheckTempFolders(() => {
					var service = new PrgDataEx();

					var postBatchResponce = service.PostOrderBatch(updateTime, false, appVersion, 50, UniqueId, "", "", new uint[] { }, _user.AvaliableAddresses[0].Id, batchFile, 1, 1, 1);

					var postBatchUpdateId = ParseUpdateId(postBatchResponce);
					Assert.That(postBatchUpdateId, Is.GreaterThan(0));
				});
			}
			finally {
				SmartOrderHelper.raiseExceptionOnEmpty = false;
			}

			var lastUpdate = TestAnalitFUpdateLog.Queryable.Where(updateLog => updateLog.UserId == _user.Id).OrderByDescending(l => l.Id).First();
			Assert.That(lastUpdate.UpdateType, Is.EqualTo(TestRequestType.PostOrderBatch), "Не совпадает тип обновления");
		}

		private string PostOrderBatch(bool getEtalonData, DateTime accessTime, string appVersion, uint adresssId, string batchFileName)
		{
			var service = new PrgDataEx();
			var responce = service.PostOrderBatch(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", new uint[] { }, adresssId, batchFileName, 1, 1, 1);

			Assert.That(responce, Is.StringStarting("URL=").IgnoreCase);

			return responce;
		}

		[Test(Description = "попытка сформировать автозаказ по пользователю без адресов")]
		public void UserDoesNotHaveAddresses()
		{
			Configure();

			_user.AvaliableAddresses.Clear();
			_user.Save();

			FlushAndCommit();
			SetCurrentUser(_user.Login);

			MySqlHelper.ExecuteScalar(
				(MySqlConnection)session.Connection,
				"update Customers.Users set SaveAFDataFiles = 1 where Id = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

			var updateData = UpdateHelper.GetUpdateData((MySqlConnection)session.Connection, _user.Login);

			var exception = Assert.Throws<UpdateException>(
				() => new SmartOrderHelper(updateData, _address.Id, 1, 1, 1));
			Assert.That(exception.Message, Is.EqualTo("Услуга 'АвтоЗаказ' не предоставляется"));
			Assert.That(exception.Addition, Is.StringContaining("У пользователя нет доступных адресов доставки"));
		}

		[Test(Description = "попытка сформировать автозаказ по пользователю с указанием недоступного адреса доставки")]
		public void UserDoesNotAllowAddress()
		{
			Configure();

			var notAllowAddress = _client.CreateAddress();
			notAllowAddress.Save();

			FlushAndCommit();
			SetCurrentUser(_user.Login);

			MySqlHelper.ExecuteScalar(
				(MySqlConnection)session.Connection,
				"update Customers.Users set SaveAFDataFiles = 1 where Id = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

			var updateData = UpdateHelper.GetUpdateData((MySqlConnection)session.Connection, _user.Login);

			var exception = Assert.Throws<UpdateException>(
				() => new SmartOrderHelper(updateData, notAllowAddress.Id, 1, 1, 1));
			Assert.That(exception.Message, Is.EqualTo("Услуга 'АвтоЗаказ' не предоставляется"));
			Assert.That(exception.Addition, Is.StringContaining("Пользователю не доступен адрес с кодом"));
		}
	}
}