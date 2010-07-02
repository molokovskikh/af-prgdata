using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
using PrgData.Common;
using SmartOrderFactory;
using SmartOrderFactory.Domain;
using SmartOrderFactory.Repositories;
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
			ContainerInitializer.InitializerContainerForTests(typeof(SmartOrderRule).Assembly);
			IoC.Container.Register(
				Component.For<ISmartOrderFactoryRepository>().ImplementedBy<SmartOrderFactoryRepository>(),
				Component.For<ISmartOfferRepository>().ImplementedBy<SmartOfferRepository>(),
				Component.For<IOrderFactory>().ImplementedBy<SmartOrderFactory.SmartOrderFactory>()
				);


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

		[Test]
		[ExpectedException(typeof(UpdateException), ExpectedMessage = "Услуга 'АвтоЗаказ' не предоставляется")]
		public void Get_UpdateException_on_disabled_EnableSmartOrder()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, user.Login);

				var smartHelper = new SmartOrderHelper(updateData, connection, connection, address.Id, 1, 1, 1);
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

				var smartHelper = new SmartOrderHelper(updateData, connection, connection, address.Id, 1, 1, 1);
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

				var smartHelper = new SmartOrderHelper(updateData, connection, connection, address.Id, 1, 1, 1);

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

	}
}
