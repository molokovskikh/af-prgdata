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
using Common.Tools;
using Inforoom.Common;
using MySql.Data.MySqlClient;
using NHibernate;
using NUnit.Framework;
using PrgData;
using PrgData.Common;
using SmartOrderFactory;
using SmartOrderFactory.Domain;
using SmartOrderFactory.Repositories;
using Test.Support;


namespace Integration
{
	[TestFixture]
	public class SmartOrderHelperFixture
	{
		TestClient client;
		TestUser user;
		TestAddress address;

		private string UniqueId;

		[SetUp]
		public void SetUp()
		{
			UniqueId = "123";

			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ServiceContext.GetResultPath = () => "results\\";

			client = TestClient.Create();

			using (new TransactionScope())
			{
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
				orderable = IoC.Resolve<IRepository<User>>().Load(user.Id);
				NHibernateUtil.Initialize(orderable.AvaliableAddresses);

				realAddress = IoC.Resolve<IRepository<Address>>().Load(address.Id);
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

		[Test]
		[ExpectedException(typeof(UpdateException), ExpectedMessage = "Услуга 'АвтоЗаказ' не предоставляется")]
		public void Get_UpdateException_on_disabled_EnableSmartOrder()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, user.Login);

				var smartHelper = new SmartOrderHelper(updateData, address.Id, 1, 1, 1);
			}
		}

		[Test]
		[ExpectedException(typeof(UpdateException), ExpectedMessage = "Не настроены правила для автоматического формирования заказа")]
		public void Get_UpdateException_on_null_SmartOrderRuleId()
		{
			using (new TransactionScope())
			{
				var orderRule = TestDrugstoreSettings.Find(client.Id);
				orderRule.EnableSmartOrder = true;
				orderRule.Update();
			}

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, user.Login);

				var smartHelper = new SmartOrderHelper(updateData, address.Id, 1, 1, 1);
			}
		}

		[Test]
		public void SimpleSmartOrder()
		{
			ArchiveHelper.SevenZipExePath = @"7zip\7z.exe";

			using (new TransactionScope())
			{
				var smartRule = new TestSmartOrderRule();
				smartRule.OffersClientCode = null;
				smartRule.AssortimentPriceCode = 4662;
				smartRule.UseOrderableOffers = true;
				smartRule.ParseAlgorithm = "TestSource";
				smartRule.SaveAndFlush();

				var orderRule = TestDrugstoreSettings.Find(client.Id);
				orderRule.SmartOrderRule = smartRule;
				orderRule.EnableSmartOrder = true;
				orderRule.UpdateAndFlush();
			}

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, user.Login);

				var smartHelper = new SmartOrderHelper(updateData, address.Id, 1, 1, 1);

				var batchFileBytes = File.ReadAllBytes("TestData\\TestOrderSmall.7z");
				Assert.That(batchFileBytes.Length, Is.GreaterThan(0), "Файл с дефектурой оказался пуст, возможно, его нет в папке");

				var batchFile = Convert.ToBase64String(batchFileBytes);

				try
				{
					smartHelper.PrepareBatchFile(batchFile);

					smartHelper.ProcessBatchFile();

					var fileInfo = new FileInfo(smartHelper.BatchOrderFileName);
					Assert.That(fileInfo.Length, Is.GreaterThan(0), "Файл с заголовками заказов оказался пустым");
					fileInfo = new FileInfo(smartHelper.BatchOrderItemsFileName);
					Assert.That(fileInfo.Length, Is.GreaterThan(0), "Файл с содержимым заказов оказался пустым");
					fileInfo = new FileInfo(smartHelper.BatchReportFileName);
					Assert.That(fileInfo.Length, Is.GreaterThan(0), "Файл с отчетом АвтоЗаказа оказался пустым");
				}
				finally
				{
					smartHelper.DeleteTemporaryFiles();
				}
			}
		}

		[Test(Description = "Позиции в отчете должны сохраняться в зависимости от созданного заказа")]
		public void TestReportByAddress()
		{
			TestAddress newAddress;

			using (new TransactionScope())
			{
				newAddress = client.CreateAddress();
				newAddress.LegalEntity = address.LegalEntity;
				user.JoinAddress(newAddress);

				client.Update();

				var smartRule = new TestSmartOrderRule();
				smartRule.OffersClientCode = null;
				smartRule.AssortimentPriceCode = 4662;
				smartRule.UseOrderableOffers = true;
				smartRule.ParseAlgorithm = "TestSource";
				smartRule.SaveAndFlush();

				var orderRule = TestDrugstoreSettings.Find(client.Id);
				orderRule.SmartOrderRule = smartRule;
				orderRule.EnableSmartOrder = true;
				orderRule.UpdateAndFlush();
			}

			User realUser;
			Address firstAddress;
			Address secondAddress;

			using (var unitOfWork = new UnitOfWork())
			{
				realUser = IoC.Resolve<IRepository<User>>().Load(user.Id);
				NHibernateUtil.Initialize(realUser.AvaliableAddresses);

				firstAddress = IoC.Resolve<IRepository<Address>>().Load(address.Id);
				NHibernateUtil.Initialize(firstAddress.Users);

				secondAddress = IoC.Resolve<IRepository<Address>>().Load(newAddress.Id);
				NHibernateUtil.Initialize(secondAddress.Users);
			}

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, user.Login);

				var smartHelper = new SmartOrderHelper(updateData, address.Id, 1, 4, 7);

				var methodSaveToFile = smartHelper.GetType().GetMethod("SaveToFile", BindingFlags.NonPublic | BindingFlags.Instance);

				try
				{
					var activePrice = new ActivePrice {Id = new PriceKey(new PriceList())};
					var orders = new List<Order>
					             	{
					             		new Order(activePrice, realUser, firstAddress, new OrderRules()),
										new Order(activePrice, realUser, secondAddress, new OrderRules())
					             	};

					var firstOffer = new Offer {ProductId = 1};
					var firstOrderItem = orders[0].AddOrderItem(firstOffer, 1);
					var secondOffer = new Offer {ProductId = 2};
					var secondOrderItem = orders[1].AddOrderItem(secondOffer, 1);

					var items = new List<OrderBatchItem>
					            	{
					            		new OrderBatchItem(null){Code = "123", ProductName = "test0"},
										new OrderBatchItem(null)
											{
												Code = "456", 
												ProductName = "test1", 
												Item = new ItemToOrder(1, 1, null, 1)
												       	{
												       		OrderItem = firstOrderItem, 
															Status = ItemToOrderStatus.Ordered,
															Offer = new ReducedOffer{Id = new OfferKey(1, 1), ProductId = firstOffer.ProductId}
												       	}
											},
										new OrderBatchItem(null)
											{
												Code = "789", 
												ProductName = "test2", 
												Item = new ItemToOrder(2, 1, null, 1)
												       	{
												       		OrderItem = secondOrderItem, 
															Status = ItemToOrderStatus.Ordered,
															Offer = new ReducedOffer{Id = new OfferKey(1, 1), ProductId = secondOffer.ProductId}
												       	}
											},
					            	};

					methodSaveToFile.Invoke(smartHelper, new object[] { items, orders });

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
				finally
				{
					smartHelper.DeleteTemporaryFiles();
				}
			}
		}


		[Test]
		public void CheckBatchSave()
		{
			var appVersion = "1.1.1.1300";
			ArchiveHelper.SevenZipExePath = @"7zip\7z.exe";

			using (new TransactionScope())
			{
				var smartRule = new TestSmartOrderRule();
				smartRule.OffersClientCode = null;
				smartRule.AssortimentPriceCode = 4662;
				smartRule.UseOrderableOffers = true;
				smartRule.ParseAlgorithm = "TestSource";
				smartRule.SaveAndFlush();

				var orderRule = TestDrugstoreSettings.Find(client.Id);
				orderRule.SmartOrderRule = smartRule;
				orderRule.EnableSmartOrder = true;
				orderRule.UpdateAndFlush();
			}

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				SetCurrentUser(user.Login);

				MySqlHelper.ExecuteScalar(
					connection,
					"update future.Users set SaveAFDataFiles = 1 where Id = ?UserId",
					new MySqlParameter("?UserId", user.Id));

				var updateData = UpdateHelper.GetUpdateData(connection, user.Login);

				var smartHelper = new SmartOrderHelper(updateData, address.Id, 1, 1, 1);

				var batchFileBytes = File.ReadAllBytes("TestData\\TestOrderSmall.7z");
				Assert.That(batchFileBytes.Length, Is.GreaterThan(0), "Файл с дефектурой оказался пуст, возможно, его нет в папке");

				var batchFile = Convert.ToBase64String(batchFileBytes);

				var postBatchResponce = PostOrderBatch(false, DateTime.Now, appVersion, user.AvaliableAddresses[0].Id, batchFile);
				var postBatchUpdateId = ParseUpdateId(postBatchResponce);
				Assert.That(File.Exists(Path.Combine("results", "Archive", user.Id.ToString(), postBatchUpdateId + "_Batch.7z")), Is.True);
			}
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}

		private string PostOrderBatch(bool getEtalonData, DateTime accessTime, string appVersion, uint adresssId, string batchFileName)
		{
			var service = new PrgDataEx();
			var responce = service.PostOrderBatch(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", new uint[] { }, adresssId, batchFileName, 1, 1, 1);

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


	}
}
