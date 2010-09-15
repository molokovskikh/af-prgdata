using System.Configuration;
using System.IO;
using Castle.ActiveRecord;
using Common.Tools;
using Inforoom.Common;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;
using PrgData;
using System;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;
using System.Data;
using NHibernate.Criterion;

namespace Integration
{
	[TestFixture]
	public class GetHistoryOrdersFixture
	{
		private TestClient client;
		private TestUser user;

		TestOldClient oldClient;
		TestOldUser oldUser;

		private uint lastUpdateId;
		private string responce;

		private string UniqueId;

		[SetUp]
		public void Setup()
		{
			UniqueId = "123";
			Test.Support.Setup.Initialize();
			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ConfigurationManager.AppSettings["WaybillPath"] = "FtpRoot\\";

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

			if (Directory.Exists("FtpRoot"))
				FileHelper.DeleteDir("FtpRoot");

			Directory.CreateDirectory("FtpRoot");
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}

		private string SimpleLoadData()
		{
			return LoadData("6.0.7.1183");
		}

		private string LoadData(string appVersion)
		{
			var service = new PrgDataEx();
			service.ResultFileName = "results";
			responce = service.GetHistoryOrders(appVersion, UniqueId, new ulong[0], 1, 1);

			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
				lastUpdateId = Convert.ToUInt32(match);
			return responce;
		}

		private void CommitExchange()
		{
			var service = new PrgDataEx();
			service.ResultFileName = "results";

			service.CommitHistoryOrders(lastUpdateId);
		}

		private void CheckGetHistoryOrders(string login)
		{
			SetCurrentUser(login);
			lastUpdateId = 0;
			SimpleLoadData();
			Assert.That(responce, Is.Not.StringContaining("Error=").IgnoreCase, "����� �� ������� ���������, ��� ������� ������");
			Assert.That(lastUpdateId, Is.GreaterThan(0), "UpdateId �� ����������");
		}

		[Test]
		public void Get_history_orders()
		{
			CheckGetHistoryOrders(user.Login);

			var commit =
				Convert.ToBoolean(MySqlHelper.ExecuteScalar(Settings.ConnectionString(),
				                                            "select Commit from logs.AnalitFUpdates where UpdateId = " +
				                                            lastUpdateId));
			Assert.IsFalse(commit, "������ � �������� ������� ��������� ��������������");

			CommitExchange();

			commit =
				Convert.ToBoolean(MySqlHelper.ExecuteScalar(Settings.ConnectionString(),
															"select Commit from logs.AnalitFUpdates where UpdateId = " +
															lastUpdateId));
			Assert.IsTrue(commit, "������ � �������� ������� ��������� ����������������");

		}

	}
}