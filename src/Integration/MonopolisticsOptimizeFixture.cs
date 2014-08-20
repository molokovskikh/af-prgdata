using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Common.Models;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;
using Test.Support.Suppliers;

namespace Integration
{
	[TestFixture]
	public class MonopolisticsOptimizeFixture : IntegrationFixture
	{
		[Test]
		public void Optimize()
		{
			var supplier = TestSupplier.CreateNaked(session);
			supplier.CreateSampleCore(session);
			var client = TestClient.CreateNaked(session);
			var user = client.Users[0];
			session.Save(new CostOptimizationRule(session.Load<Supplier>(supplier.Id), RuleType.MaxCost));

			var files = new ConcurrentQueue<string>();
			var data = UpdateHelper.GetUpdateData((MySqlConnection)session.Connection, user.Login);
			data.BuildNumber = uint.MaxValue;
			data.FilesForArchive = files;
			var helper = new UpdateHelper(data, (MySqlConnection)session.Connection);

			helper.ExportOffers();

			Assert.AreEqual(1, files.Count());
			Assert.That(new FileInfo(files.First()).Length, Is.GreaterThan(0));
		}
	}
}