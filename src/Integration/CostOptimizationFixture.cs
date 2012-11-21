using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Castle.ActiveRecord;
using Common.Models;
using Common.Models.Tests.Repositories;
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
		}

		private void CostOptimizerShouldCreateLogsWithSupplier(uint clientId, Action<MySqlCommand> getOffers)
		{
			using (var conn = new MySqlConnection(Settings.ConnectionString())) {
				conn.Open();
				var command = new MySqlCommand("select Now()", conn);
				var startTime = Convert.ToDateTime(command.ExecuteScalar());

				command = new MySqlCommand(
					@"select cs.Id, min(ccc.Cost) Cost
  from farm.Core0 cs
	   join farm.Core0 cc on cc.ProductId = cs.ProductId and cs.Id <> cc.Id and cs.ProductId = cc.ProductId
	   join farm.CoreCosts ccc on ccc.Core_Id = cc.Id
	   join usersettings.PricesData pds on pds.PriceCode = cs.PriceCode
	   join usersettings.PricesData pdc on pdc.PriceCode = cc.PriceCode
 where pds.PriceCode = ?priceId
   and pdc.FirmCode = ?concurentId
group by cs.Id
limit 0, 50", conn);
				command.Parameters.AddWithValue("?priceId", costOptimizaerConf.OptimizationPriceId);
				command.Parameters.AddWithValue("?concurentId", costOptimizaerConf.ConcurentSupplierId);

				var cores = command.ExecuteReader();
				var update = new StringBuilder();
				foreach (var row in cores.Cast<DbDataRecord>())
					update.Append("update farm.CoreCosts set Cost=").Append(row["Cost"]).Append("+1 where Core_Id=").Append(row["Id"]).Append(";");
				cores.Close();

				command.CommandText = update.ToString().Replace(',', '.');
				command.ExecuteNonQuery();

				command.Parameters.Clear();
				getOffers(command);
				command.Parameters.Clear();

				command.CommandType = CommandType.Text;

				var optimizer = new CostOptimizer(conn, clientId);
				optimizer.Oprimize();

				command.CommandText = "select * from logs.CostOptimizationLogs where LoggedOn > ?startTime and ClientId = ?clientId";
				command.Parameters.AddWithValue("?startTime", startTime);
				command.Parameters.AddWithValue("?clientId", clientId);
				var reader = command.ExecuteReader();

				foreach (var row in reader.Cast<DbDataRecord>()) {
					Assert.AreEqual(45, row["SupplierId"]);
					Assert.That(Convert.ToDecimal(row["SelfCost"]), Is.LessThanOrEqualTo(Convert.ToDecimal(row["ResultCost"])));
				}
			}
		}

		[Test(Description = "проверяем создание записей в логах оптимизации для клиента из новой реальности")]
		public void CostOptimizerShouldCreateLogsWithSupplierForFuture()
		{
			CostOptimizerShouldCreateLogsWithSupplier(
				_client.Id,
				command => {
					command.CommandText = "call Customers.GetOffers(?UserId);";
					command.Parameters.AddWithValue("?UserId", _user.Id);
					command.ExecuteNonQuery();
				});
		}
	}
}