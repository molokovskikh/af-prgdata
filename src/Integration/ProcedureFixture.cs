using System.Collections.Generic;
using System.Linq;
using Castle.ActiveRecord;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Tests.Repositories;
using MySql.Data.MySqlClient;
using NHibernate;
using NUnit.Framework;
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

		[SetUp]
		public void SetUp()
		{
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

		public void Execute(string commnad)
		{
			using(var connection = new MySqlConnection("Database=usersettings;Data Source=testsql.analit.net;User Id=system;Password=newpass;pooling=true;default command timeout=0;Allow user variables=true"))
			{
				connection.Open();
				var command = new MySqlCommand(commnad, connection);
				command.ExecuteNonQuery();
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

	}
}
