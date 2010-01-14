using System;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;

namespace Integration
{
	[TestFixture]
	public class CostCorrectionFixture
	{
		[SetUp]
		public void Setup()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var transaction = connection.BeginTransaction();
				try
				{
					var command = new MySqlCommand(@"
delete from usersettings.ConcurentGroup;
delete from usersettings.CostCorrectorSettings;

insert into CostCorrectorSettings(FirmCode, ClientCode) values(220, 2575);
insert into ConcurentGroup(SettingsId, FirmCode) values (220, 14);
", connection, transaction);
					command.ExecuteNonQuery();
					transaction.Commit();
				}
				catch (Exception)
				{
					transaction.Rollback();
					throw;
				}
			}
		}

		[Test]
		public void Try_to_optimize_cost()
		{
			using(var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var command = new MySqlCommand("CALL GetOffers(2575,0); ", connection);
				command.ExecuteNonQuery();

				var corrector = new CostOptimizer(connection, connection, 2575);
				Assert.That(corrector.IsCostOptimizationNeeded(), Is.True);
				corrector.Oprimize();
			}
		}

		[Test]
		public void Test_make_cost()
		{
			Assert.That(MakeCost(65, 70), Is.EqualTo(66.53));
			Assert.That(MakeCost(175.47, 174.77), Is.InRange(173, 175));
		}

		public double MakeCost(double selfCost, double concurentCost)
		{
			using(var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var command = new MySqlCommand("select MakeCost(?selfCost, ?concurentCost);", connection);
				command.Parameters.AddWithValue("?selfCost", selfCost);
				command.Parameters.AddWithValue("?concurentCost", concurentCost);
				return Convert.ToDouble(command.ExecuteScalar());
			}
		}
	}
}
