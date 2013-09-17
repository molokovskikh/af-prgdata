using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Castle.ActiveRecord;
using Common.Models;
using Common.Models.Helpers;
using Common.Models.Tests.Repositories;
using Common.MySql;
using Common.Tools;
using Integration.BaseTests;
using log4net;
using log4net.Config;
using NUnit.Framework;
using MySql.Data.MySqlClient;
using PrgData.Common;
using System.Data.Common;
using System.Data;
using Test.Support;

namespace Integration
{
	[TestFixture]
	public class CostOptimizationFixture
	{
		private CostOptimizaerConf costOptimizaerConf;
		private TestClient _client;
		private TestUser _user;
		private MySqlConnection connection;

		[SetUp]
		public void SetUp()
		{
			_client = TestClient.Create();
			using (var transaction = new TransactionScope()) {
				_user = _client.Users[0];

				_client.Users.Each(u => {
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				_user.Update();
			}

			costOptimizaerConf = CostOptimizaerConf.MakeUserOptimazible(_user);
			connection = new MySqlConnection(Settings.ConnectionString());
			connection.Open();
		}

		[TearDown]
		public void TearDown()
		{
			connection.Close();
		}

		[Test(Description = "проверяем создание записей в логах оптимизации для клиента из новой реальности")]
		public void CostOptimizerShouldCreateLogsWithSupplierForFuture()
		{
			var begin = DateTime.Now;
			var costSql = @"select cs.Id, min(ccc.Cost) Cost
from farm.Core0 cs
	join farm.Core0 cc on cc.ProductId = cs.ProductId and cs.Id <> cc.Id and cs.ProductId = cc.ProductId
	join farm.CoreCosts ccc on ccc.Core_Id = cc.Id
	join usersettings.PricesData pds on pds.PriceCode = cs.PriceCode
	join usersettings.PricesData pdc on pdc.PriceCode = cc.PriceCode
where pds.PriceCode = ?priceId
and pdc.FirmCode = ?concurentId
group by cs.Id
limit 0, 50";

			var update = new StringBuilder();
			var parameters = new {
				priceId = costOptimizaerConf.OptimizationPriceId,
				concurentId = costOptimizaerConf.ConcurentSupplierId
			};
			foreach (var row in connection.Read(costSql, parameters))
				update.Append("update farm.CoreCosts set Cost=").Append(row["Cost"]).Append("+1 where Core_Id=").Append(row["Id"]).Append(";");

			connection.Execute(update.ToString().Replace(',', '.'));
			connection.Execute("call Customers.GetOffers(?UserId);", new { userId = _user.Id });

			var optimizer = new CostOptimizer(connection, _client.Id, _user.Id);
			optimizer.Oprimize();

			var sql = "select * from logs.CostOptimizationLogs where LoggedOn > ?begin and ClientId = ?clientId";
			foreach (var row in connection.Read(sql, new { begin, clientId = _client.Id })) {
				Assert.AreEqual(costOptimizaerConf.OptimizationSupplierId, row["SupplierId"]);
				Assert.That(Convert.ToDecimal(row["SelfCost"]), Is.LessThanOrEqualTo(Convert.ToDecimal(row["ResultCost"])));
				Assert.That(row["UserId"], Is.EqualTo(_user.Id));
			}
		}
	}
}