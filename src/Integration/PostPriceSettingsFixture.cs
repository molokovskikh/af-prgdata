using System;
using System.IO;
using System.Configuration;
using System.Data;
using System.Text.RegularExpressions;
using Castle.ActiveRecord;
using Common.Tools;
using Inforoom.Common;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;
using PrgData;
using MySql.Data.MySqlClient;
using NHibernate.Criterion;

namespace Integration
{
	[TestFixture]
	public class PostPriceSettingsFixture
	{
		private TestClient client;
		private TestUser user;

		private TestOldClient oldClient;
		private TestOldUser oldUser;

		private DataTable offers;

		private string UniqueId;

		[SetUp]
		public void Setup()
		{
			UniqueId = "123";
			ServiceContext.GetUserHost = () => "127.0.0.1";
			ServiceContext.GetResultPath = () => "results\\";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";

			using (var transaction = new TransactionScope())
			{

				var permission = TestUserPermission.ByShortcut("AF");

				client = TestClient.CreateSimple();
				user = client.Users[0];

				client.Users.Each(u =>
				{
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				user.Update();

				oldClient = TestOldClient.CreateTestClient();
				oldUser = oldClient.Users[0];

				ServiceContext.GetUserName = () => oldUser.OSUserName;

				var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof(ActiveRecordBase));
				try
				{
					session.CreateSQLQuery(@"
				insert into usersettings.AssignedPermissions (PermissionId, UserId) values (:permissionid, :userid)")
						.SetParameter("permissionid", permission.Id)
						.SetParameter("userid", oldUser.Id)
						.ExecuteUpdate();
				}
				finally
				{
					ActiveRecordMediator.GetSessionFactoryHolder().ReleaseSession(session);
				}
			}

		}

		private void PostPriceSettings(string login)
		{
			SetCurrentUser(login);

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, login);
				var helper = new UpdateHelper(updateData, connection);
				helper.Cleanup();
				helper.SelectPrices();

				var pricesSet = MySqlHelper.ExecuteDataset(connection, "select * from Prices limit 2");
				var prices = pricesSet.Tables[0];
				Assert.That(prices.Rows.Count, Is.EqualTo(2), "Нет необходимого количества прайс-листов для теста");

				var injobs = new bool[] {false, true};
				var priceIds = new int[]
				               	{Convert.ToInt32(prices.Rows[0]["PriceCode"]), Convert.ToInt32(prices.Rows[1]["PriceCode"])};
				var regionIds = new long[] { Convert.ToInt64(prices.Rows[0]["RegionCode"]), Convert.ToInt64(prices.Rows[1]["RegionCode"]) };

				PostSettings(priceIds, regionIds, injobs);

				injobs = new bool[] { true, false };
				PostSettings(priceIds, regionIds, injobs);
			}
		}

		[Test]
		public void Post_settings_for_future()
		{
			PostPriceSettings(user.Login);
		}

		[Test]
		public void Post_settings_for_old()
		{
			PostPriceSettings(oldUser.OSUserName);
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}

		private string PostSettings(int[] priceIds, long[] regionIds, bool[] injobs)
		{
			var service = new PrgDataEx();
			var responce = service.PostPriceDataSettings(UniqueId, priceIds, regionIds, injobs);

			Assert.That(responce, Is.EqualTo("Res=OK").IgnoreCase, "Отправка настроек прайс-листов завершилась ошибкой.");

			return responce;
		}


	}
}