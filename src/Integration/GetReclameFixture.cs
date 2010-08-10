using System;
using System.IO;
using Castle.ActiveRecord;
using Common.Models.Tests.Repositories;
using Common.Tools;
using Inforoom.Common;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData;
using PrgData.Common;
using Test.Support;

namespace Integration
{
	[TestFixture]
	public class GetReclameFixture
	{
		TestClient _client;
		TestUser _user;

		TestOldClient _oldClient;

		private string resultsDir = "results\\";

		private TestClient _disabledClient;
		private TestUser _disabledUser;

		[TestFixtureSetUp]
		public void FixtureSetUp()
		{
			Test.Support.Setup.Initialize();
			ContainerInitializer.InitializerContainerForTests();

			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";

			using (var transaction = new TransactionScope())
			{
				var permission = TestUserPermission.ByShortcut("AF");

				_client = TestClient.CreateSimple();
				_user = _client.Users[0];
				_client.Users.Each(u =>
				{
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				_user.Update();

				_disabledClient = TestClient.CreateSimple();
				_disabledUser = _disabledClient.Users[0];

				_oldClient = TestOldClient.CreateTestClient();
				var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof(ActiveRecordBase));
				try
				{
					session.CreateSQLQuery(@"
insert into usersettings.AssignedPermissions (PermissionId, UserId) values (:permissionid, :userid)")
						.SetParameter("permissionid", permission.Id)
						.SetParameter("userid", _oldClient.Users[0].Id)
						.ExecuteUpdate();
				}
				finally
				{
					ActiveRecordMediator.GetSessionFactoryHolder().ReleaseSession(session);
				}

			}
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}

		private string GetReclame()
		{
			var service = new PrgDataEx();
			service.ResultFileName = resultsDir;

			return service.GetReclame();
		}

		private bool ReclameComplete()
		{
			var service = new PrgDataEx();
			service.ResultFileName = resultsDir;
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
				Assert.IsTrue(reclame.ShowAdvertising, "Реклама не включена");
				Assert.IsNotNullOrEmpty(reclame.Region, "Не установлен регион рекламы");
				Assert.That(reclame.ReclameDate, Is.EqualTo(new DateTime(2003, 1, 1)), "Дата рекламы не установлена");

				var maxFileTime = SetReclameDir(reclame.Region);

				SetCurrentUser(login);
				var response = GetReclame();
				Assert.IsNotNullOrEmpty(response, "Некорректный ответ на запрос рекламы");
				Assert.That(response, Is.StringEnding("New=True"), "Некорректный ответ на запрос рекламы");
				Assert.That(response, Is.StringStarting("URL="), "Некорректный ответ на запрос рекламы");
				var comlete = ReclameComplete();
				Assert.IsTrue(comlete, "Рекламу не удалось подтвердить");

				var date = MySqlHelper.ExecuteScalar(
					connection,
					"select uui.ReclameDate from usersettings.UserUpdateInfo uui where uui.UserId = ?UserId",
					new MySqlParameter("?UserId", userId));
				Assert.That(date, Is.Not.Null);
				Assert.That(date.GetType(), Is.EqualTo(typeof(DateTime)));
				Assert.IsTrue(maxFileTime.Subtract((DateTime)date).TotalSeconds < 1, "Не совпадают даты");
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
				Assert.IsNullOrEmpty(response, "Ответ от сервера должен быть пустым");
			}
		}

		[Test]
		public void Get_reclame_for_old_client()
		{
			GetReclameForUser(_oldClient.Users[0].OSUserName, _oldClient.Users[0].Id);
		}

		[Test]
		public void Get_reclame_for_future_client()
		{
			GetReclameForUser(_user.Login, _user.Id);
		}

		[Test(Description = "пытаемся получить рекламу для пользователя, который не привязан к системе")]
		public void Get_reclame_for_non_exists_user()
		{
			var memoryAppender = new MemoryAppender();
			BasicConfigurator.Configure(memoryAppender);
			GetReclameForErrorUser("dsdsdsdsdsds");
			var events = memoryAppender.GetEvents();
			var lastEvent = events[events.Length - 1];
			Assert.That(lastEvent.Level, Is.EqualTo(Level.Error));
			Assert.That(lastEvent.MessageObject, Is.TypeOf(typeof(UpdateException)));
			Assert.That(((UpdateException)lastEvent.MessageObject).Message, Is.EqualTo("Доступ закрыт."));
		}

		[Test(Description = "пытаемся получить рекламу для отключенного пользователя")]
		public void Get_reclame_for_disabled_user()
		{
			var memoryAppender = new MemoryAppender();
			BasicConfigurator.Configure(memoryAppender);
			GetReclameForErrorUser(_disabledUser.Login);
			var events = memoryAppender.GetEvents();
			var lastEvent = events[events.Length - 1];
			Assert.That(lastEvent.Level, Is.EqualTo(Level.Warn));
			Assert.That(lastEvent.MessageObject, Is.TypeOf(typeof(UpdateException)));
			Assert.That(((UpdateException)lastEvent.MessageObject).Message, Is.EqualTo("Доступ закрыт."));
		}
	}
}