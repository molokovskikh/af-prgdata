using System;
using System.Linq;
using System.Text;
using NUnit.Framework;
using MySql.Data.MySqlClient;
using PrgData.Common;
using System.Data.Common;
using System.Data;

namespace Integration
{
	[TestFixture]
	public class CostOptimizationFixture
	{
		private uint _clientId = 456;
		private uint _concurentId = 14;
		private uint _priceId = 4596;

		[Test]
		public void CostOptimizerShouldCreateLogsWithSupplier()
		{
			using (var conn = new MySqlConnection(Settings.ConnectionString()))
			{
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
				command.Parameters.AddWithValue("?priceId", _priceId);
				command.Parameters.AddWithValue("?concurentId", _concurentId);

				var cores = command.ExecuteReader();
				var update = new StringBuilder();
				foreach (var row in cores.Cast<DbDataRecord>())
					update.Append("update farm.CoreCosts set Cost=").Append(row["Cost"]).Append("+1 where Core_Id=").Append(row["Id"]).Append(";");
				cores.Close();

				command.CommandText = update.ToString().Replace(',', '.');
				command.ExecuteNonQuery();

				command.Parameters.Clear();
				command.CommandText = "usersettings.GetOffers";
				command.Parameters.AddWithValue("ClientCodeParam", _clientId);
				command.Parameters.AddWithValue("FreshOnly", false);
				command.CommandType = CommandType.StoredProcedure;
				command.ExecuteNonQuery();
				command.CommandType = CommandType.Text;

				var optimizer = new CostOptimizer(conn, _clientId);
				optimizer.Oprimize();

				command.CommandText = "select * from logs.CostOptimizationLogs where LoggedOn > ?startTime and ClientId = ?clientId";
				command.Parameters.AddWithValue("?startTime", startTime);
				command.Parameters.AddWithValue("?clientId", _clientId);
				var reader = command.ExecuteReader();

				foreach (var row in reader.Cast<DbDataRecord>())
				{
					Assert.AreEqual(5, row["SupplierId"]);
				}
			}
		}
	}
}
