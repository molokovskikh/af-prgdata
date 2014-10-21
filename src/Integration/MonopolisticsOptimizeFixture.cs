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

		[SetUp]
		public void Setup()
		{
			supplier = TestSupplier.CreateNaked(session);
			supplier.CreateSampleCore(session);
			client = TestClient.CreateNaked(session);
			user = client.Users[0];
			rule = new CostOptimizationRule(session.Load<Supplier>(supplier.Id), RuleType.MaxCost);
			session.Save(rule);
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
			var concurrent = TestSupplier.CreateNaked(session);
			rule.Concurrents.Add(session.Load<Supplier>(concurrent.Id));
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