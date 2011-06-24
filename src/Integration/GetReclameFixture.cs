using System;
using System.IO;
using Castle.ActiveRecord;
using Common.Models.Tests.Repositories;
using Common.Tools;
using Inforoom.Common;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Filter;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData;
using PrgData.Common;
using Test.Support;
using System.Text.RegularExpressions;
using log4net;

namespace Integration
{
	[TestFixture]
	public class GetReclameFixture
	{
		TestClient _client;
		TestUser _user;

		//TestOldClient _oldClient;

		private string resultsDir = "results\\";

		private TestClient _disabledClient;
		private TestUser _disabledUser;

		[TestFixtureSetUp]
		public void FixtureSetUp()
		{
			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ServiceContext.GetResultPath = () => resultsDir;

			_client = TestClient.Create();
			_disabledClient = TestClient.Create();
			//_oldClient = TestOldClient.CreateTestClient();

			using (var transaction = new TransactionScope())
			{
				_user = _client.Users[0];
				_client.Users.Each(u =>
				{
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				_user.Update();

				_disabledUser = _disabledClient.Users[0];

//                var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof(ActiveRecordBase));
//                try
//                {
//                    session.CreateSQLQuery(@"
//insert into usersettings.AssignedPermissions (PermissionId, UserId) values (:permissionid, :userid)")
//                        .SetParameter("permissionid", permission.Id)
//                        .SetParameter("userid", _oldClient.Users[0].Id)
//                        .ExecuteUpdate();
//                }
//                finally
//                {
//                    ActiveRecordMediator.GetSessionFactoryHolder().ReleaseSession(session);
//                }
			}
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}

		private string GetReclame()
		{
			var service = new PrgDataEx();

			return service.GetReclame();
		}

		private bool ReclameComplete()
		{
			var service = new PrgDataEx();
			return service.ReclameComplete();
		}

		private DateTime SetReclameDir(string region)
		{
			var mainReclameDir = resultsDir + "Reclame";
			if (Directory.Exists(mainReclameDir))
				FileHelper.DeleteDir(mainReclameDir);

			Directory.CreateDirectory(mainReclameDir);

			var regionReclameDir = Path.Combine(mainReclameDir, region);
			Directory.CreateDirectory(regionReclameDir);

			File.WriteAllText(Path.Combine(regionReclameDir, "main.htm"), "contents main.htm");
			File.WriteAllText(Path.Combine(regionReclameDir, "main.gif"), "contents main.gif");
			var info = new FileInfo(Path.Combine(regionReclameDir, "main.gif"));
			return info.CreationTime;
		}

		private void GetReclameForUser(string login, uint userId)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				MySqlHelper.ExecuteNonQuery(
					connection,
					"update usersettings.UserUpdateInfo uui set uui.ReclameDate = null where uui.UserId = ?UserId",
					new MySqlParameter("?UserId", userId));
				var updateData = UpdateHelper.GetUpdateData(connection, login);
				var helper = new UpdateHelper(updateData, connection);
				var reclame = helper.GetReclame();
				Assert.IsTrue(reclame.ShowAdvertising, "������� �� ��������");
				Assert.IsNotNullOrEmpty(reclame.Region, "�� ���������� ������ �������");
				Assert.That(reclame.ReclameDate, Is.EqualTo(new DateTime(2003, 1, 1)), "���� ������� �� �����������");

				var maxFileTime = SetReclameDir(reclame.Region);

				SetCurrentUser(login);
				var response = GetReclame();
				Assert.IsNotNullOrEmpty(response, "������������ ����� �� ������ �������");
				Assert.That(response, Is.StringEnding("New=True"), "������������ ����� �� ������ �������");
				Assert.That(response, Is.StringStarting("URL="), "������������ ����� �� ������ �������");
				var comlete = ReclameComplete();
				Assert.IsTrue(comlete, "������� �� ������� �����������");

				var date = MySqlHelper.ExecuteScalar(
					connection,
					"select uui.ReclameDate from usersettings.UserUpdateInfo uui where uui.UserId = ?UserId",
					new MySqlParameter("?UserId", userId));
				Assert.That(date, Is.Not.Null);
				Assert.That(date.GetType(), Is.EqualTo(typeof(DateTime)));
				Assert.IsTrue(maxFileTime.Subtract((DateTime)date).TotalSeconds < 1, "�� ��������� ����");
			}
		}

		private void GetReclameForErrorUser(string login)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, login);
				var helper = new UpdateHelper(updateData, connection);

				SetCurrentUser(login);
				var response = GetReclame();
				Assert.IsNullOrEmpty(response, "����� �� ������� ������ ���� ������");
			}
		}

		//[Test]
		//public void Get_reclame_for_old_client()
		//{
		//    GetReclameForUser(_oldClient.Users[0].OSUserName, _oldClient.Users[0].Id);
		//}

		[Test]
		public void Get_reclame_for_future_client()
		{
			GetReclameForUser(_user.Login, _user.Id);
		}

		[Test(Description = "�������� �������� ������� ��� ������������, ������� �� �������� � �������")]
		public void Get_reclame_for_non_exists_user()
		{
			try
			{
				var memoryAppender = new MemoryAppender();
				memoryAppender.AddFilter(new LoggerMatchFilter { AcceptOnMatch = true, LoggerToMatch = "PrgData", Next = new DenyAllFilter() });
				BasicConfigurator.Configure(memoryAppender);
				GetReclameForErrorUser("dsdsdsdsdsds");
				var events = memoryAppender.GetEvents();
				var lastEvent = events[events.Length - 1];
				Assert.That(lastEvent.Level, Is.EqualTo(Level.Error));
				Assert.That(lastEvent.MessageObject, Is.TypeOf(typeof(UpdateException)));
				Assert.That(((UpdateException)lastEvent.MessageObject).Message, Is.EqualTo("������ ������."));
			}
			finally
			{
				LogManager.ResetConfiguration();			
			}
		}

		[Test(Description = "�������� �������� ������� ��� ������������ ��� ����� ��������� AnalitF")]
		public void Get_reclame_for_disabled_user()
		{
			try
			{
				var memoryAppender = new MemoryAppender();
				memoryAppender.AddFilter(new LoggerMatchFilter { AcceptOnMatch = true, LoggerToMatch = "PrgData", Next = new DenyAllFilter() });
				BasicConfigurator.Configure(memoryAppender);
				GetReclameForErrorUser(_disabledUser.Login);
				var events = memoryAppender.GetEvents();
				var lastEvent = events[events.Length - 1];
				Assert.That(lastEvent.Level, Is.EqualTo(Level.Warn));
				Assert.That(lastEvent.MessageObject, Is.TypeOf(typeof(UpdateException)));
				var updateException = (UpdateException) lastEvent.MessageObject;
				Assert.That(updateException.Message, Is.EqualTo("������ ������."));
				Assert.That(updateException.Addition, Is.StringStarting("��� ������ " + _disabledUser.Login + " ������ �� ���������������: ������������ �� ��������� ��������� AnalitF;"));
			}
			finally
			{
				LogManager.ResetConfiguration();
			}
		}

		[Test(Description = "��������, ��� ���� ReclameDate ����� �������� null ����� ������������� ������������� ����������")]
		public void Check_ReclameDate_is_null_after_LimitedCumulative()
		{
			SetCurrentUser(_user.Login);

			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				"update usersettings.UserUpdateInfo uui set uui.ReclameDate = now() where uui.UserId = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

			ProcessGetUserData(true, DateTime.Now);

			var reclameDate = MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				"select uui.ReclameDate from usersettings.UserUpdateInfo uui where uui.UserId = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

			Assert.That(reclameDate, Is.EqualTo(DBNull.Value), "����� �� ������� ReclameDate �� ����� DBNull");

			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				"update usersettings.UserUpdateInfo uui set uui.ReclameDate = now() where uui.UserId = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

			ProcessGetUserData(false, DateTime.Now.AddHours(-1));

			reclameDate = MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				"select uui.ReclameDate from usersettings.UserUpdateInfo uui where uui.UserId = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

			Assert.That(reclameDate, Is.EqualTo(DBNull.Value), "����� ������������ �� ������� ReclameDate �� ����� DBNull");
		}

		private void ProcessGetUserData(bool cumulative, DateTime updateTime)
		{
			var service = new PrgDataEx();
			var responce = service.GetUserData(updateTime, cumulative, "6.0.0.1183", 50, "123", "", "", false);

			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
			{
				var lastUpdateId = Convert.ToUInt32(match);
				service = new PrgDataEx();
				service.CommitExchange(lastUpdateId, false);
			}
			else
				Assert.Fail("������������ ����� �� ������� ��� ��������� ������: {0}", responce);
		}
	}
}