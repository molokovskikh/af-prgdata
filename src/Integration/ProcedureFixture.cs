using MySql.Data.MySqlClient;
using NUnit.Framework;

namespace Integration
{
	[TestFixture]
	public class ProcedureFixture
	{
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
			Execute("call future.GetActivePrices(758);");
		}

		[Test]
		public void Get_prices()
		{
			Execute("call future.GetPrices(2575);");
		}

		[Test]
		public void Get_offers()
		{
			Execute("call future.GetOffers(2575);");
		}
	}
}
