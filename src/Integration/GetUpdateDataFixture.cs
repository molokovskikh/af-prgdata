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
				var updateData = UpdateHelper.GetUpdateData(connection, "sergei");
				Assert.That(updateData, Is.Not.Null);
				Assert.That(updateData.UserId, Is.EqualTo(1289u));
				Assert.That(updateData.ClientId, Is.EqualTo(1349u));
				Assert.That(updateData.ShortName, Is.Not.Null);
				Assert.That(updateData.ShortName, Is.Not.Empty);
				Assert.That(updateData.ShortName, Is.EqualTo("ТестерСВоронеж"));
			}
		}

		[Test]
		public void Get_update_data_for_future_client()
		{
			using(var connection = new MySqlConnection("Database=usersettings;Data Source=testsql.analit.net;Port=3306;User Id=system;Password=newpass;pooling=true;default command timeout=0;Allow user variables=true"))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, "10081");
				Assert.That(updateData, Is.Not.Null);
				Assert.That(updateData.UserId, Is.EqualTo(10081u));
				Assert.That(updateData.ClientId, Is.EqualTo(10005u));
				Assert.That(updateData.ShortName, Is.Not.Null);
				Assert.That(updateData.ShortName, Is.Not.Empty);
				Assert.That(updateData.ShortName, Is.EqualTo("ТестерСВоронеж Future"));
			}
		}
	}
}
