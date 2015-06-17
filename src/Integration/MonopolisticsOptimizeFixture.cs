using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Common.Models;
using Common.Tools;
using MySql.Data.MySqlClient;
using NHibernate.Linq;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.Models;
using Test.Support;
using Test.Support.Suppliers;

namespace Integration
{
	[TestFixture]
	public class MonopolisticsOptimizeFixture : IntegrationFixture
	{
		private TestUser user;
		private TestSupplier supplier;
		private FileCleaner cleaner;
		private TestClient client;
		private CostOptimizationRule rule;
		private TestSupplier concurrent;

		[SetUp]
		public void Setup()
		{
			supplier = TestSupplier.CreateNaked(session);
			supplier.CreateSampleCore(session);
			client = TestClient.CreateNaked(session);
			user = client.Users[0];
			rule = new CostOptimizationRule(session.Load<Supplier>(supplier.Id), RuleType.MaxCost);
			session.Save(rule);
			concurrent = TestSupplier.CreateNaked(session);
			rule.Concurrents.Add(session.Load<Supplier>(concurrent.Id));
			cleaner = new FileCleaner();
		}

		[TearDown]
		public void TearDown()
		{
			cleaner.Dispose();
		}

		[Test]
		public void Optimize()
		{
			var files = Export(user);
			Assert.AreEqual(1, files.Count());
			Assert.That(new FileInfo(files.First()).Length, Is.GreaterThan(0));
		}

		[Test]
		public void Mark_as_changed_if_concurrent_changed()
		{
			client.MaintainIntersection(session);
			session.CreateSQLQuery(@"call Customers.GetActivePrices(:userId);
update Usersettings.ActivePrices set Fresh = 0 where FirmCode = :supplierId;
update Usersettings.ActivePrices set Fresh = 1 where FirmCode = :concurrentId;")
				.SetParameter("userId", user.Id)
				.SetParameter("supplierId", supplier.Id)
				.SetParameter("concurrentId", concurrent.Id)
				.ExecuteUpdate();
			Export(user);
			var price = session.Query<AFActivePrice>().First(p => p.Id.Price.Supplier.Id == supplier.Id);
			Assert.IsTrue(price.Fresh);

			var keys = session.Query<CachedCostKey>().Where(k => k.User.Id == user.Id).ToArray();
			Assert.That(keys.Length, Is.GreaterThan(0));
			Assert.That(keys[0].Date, Is.GreaterThan(DateTime.MinValue));
			var cacheCount = session
				.CreateSQLQuery(@"select count(*)
from Farm.CachedCostKeys k
	join Farm.CachedCosts c on c.KeyId = k.Id
where k.ClientId = :clientId")
				.SetParameter("clientId", user.Client.Id)
				.UniqueResult<long>();
			Assert.That(cacheCount, Is.GreaterThan(0), "Не кеша оптимизации для клиента {0}", user.Client.Id);
		}

		[Test(Description = "Проверка того, что оптимизация не произойдет, если задано исключение в правилах")]
		public void Exept_client_from_optimization()
		{
			var price = supplier.Prices[0];
			session.CreateSQLQuery("UPDATE farm.core0 SET MaxBoundCost = 250 WHERE pricecode=" + price.Id)
				.ExecuteUpdate();

			//Добавляем правило, чтобы не оптимизирвоать цены - тест проверяет именно его работу
			session.Save(new CostOptimizationException(session.Load<Supplier>(supplier.Id), session.Load<Client>(client.Id)));

			Export(user);
			var logCount = session
				.CreateSQLQuery("select count(*) from Logs.CostOptimizationLogs where UserId = :userId and SupplierId = :supplierId")
				.SetParameter("userId", user.Id)
				.SetParameter("supplierId", supplier.Id)
				.UniqueResult<long>();
			Assert.AreEqual(0, logCount);
		}

		[Test(Description = "Проверка пропуска оптимизации при наличии флага OptimizationSkip в таблице core0")]
		public void Core_optimizationSkipFlag()
		{
			var price = supplier.Prices[0];
			//Добавляем максимальную цену, чтобы процесс оптимизации мог начаться
			//Но также добавляем флаг, который пропускает оптимизацию - тест проверяет именно его работу
			session.CreateSQLQuery("UPDATE farm.core0 SET MaxBoundCost = 250, OptimizationSkip=1 WHERE pricecode=" + price.Id)
				.ExecuteUpdate();
			Export(user);
			var logCount = session
				.CreateSQLQuery("select count(*) from Logs.CostOptimizationLogs where UserId = :userId and SupplierId = :supplierId")
				.SetParameter("userId", user.Id)
				.SetParameter("supplierId", supplier.Id)
				.UniqueResult<long>();
			Assert.AreEqual(0, logCount);
		}

		private ConcurrentQueue<string> Export(TestUser user)
		{
			var files = new ConcurrentQueue<string>();
			var data = UpdateHelper.GetUpdateData((MySqlConnection)session.Connection, user.Login);
			data.BuildNumber = uint.MaxValue;
			data.FilesForArchive = files;
			var helper = new UpdateHelper(data, (MySqlConnection)session.Connection);

			helper.ExportOffers();
			cleaner.Watch(files);
			return files;
		}
	}
}