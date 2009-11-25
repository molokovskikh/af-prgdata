using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;

namespace Integration
{
	[TestFixture]
	public class GetUpdateDataFixture
	{
		[Test]
		public void Get_update_data_for_old_client()
		{
			using(var connection = new MySqlConnection("Database=usersettings;Data Source=testsql.analit.net;Port=3306;User Id=system;Password=newpass;pooling=true;default command timeout=0;Allow user variables=true"))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, "kvasov");
				Assert.That(updateData, Is.Not.Null);
				Assert.That(updateData.UserId, Is.EqualTo(758u));
			}
		}

		[Test]
		public void Get_update_data_for_future_client()
		{
			using(var connection = new MySqlConnection("Database=usersettings;Data Source=testsql.analit.net;Port=3306;User Id=system;Password=newpass;pooling=true;default command timeout=0;Allow user variables=true"))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, "semein");
				Assert.That(updateData, Is.Not.Null);
				Assert.That(updateData.UserId, Is.EqualTo(1281u));
			}
		}
	}
}
