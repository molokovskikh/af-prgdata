using System;
using System.IO;
using System.Linq;
using System.Threading;
using Castle.ActiveRecord;
using Common.Models.Tests.Repositories;
using Common.Tools;
using Inforoom.Common;
using Integration.BaseTests;
using PrgData.Common.AnalitFVersions;
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
	public class GetReclameFixture : PrepareDataFixture
	{
		TestClient _client;
		TestUser _user;

		private string resultsDir = "results\\";

		private TestClient _disabledClient;
		private TestUser _disabledUser;

		[TestFixtureSetUp]
		public void FixtureSetup()
		{
			_client = TestClient.Create();
			_disabledClient = TestClient.Create();

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
				var permissionAF = TestUserPermission.ByShortcut("AF");
				var afIndex = _disabledUser.AssignedPermissions.IndexOf(item => item.Id == permissionAF.Id);
				if (afIndex > -1)
				{
					_disabledUser.AssignedPermissions.RemoveAt(afIndex);
					_disabledUser.Update();
				}
			}
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

			File.WriteAllText(Path.Combine(regionReclameDir, "index.htm"), "contents index.htm");
			File.WriteAllText(Path.Combine(regionReclameDir, "2block.gif"), "contents 2block.gif");
			File.WriteAllText(Path.Combine(regionReclameDir, "01.htm"), "contents 01.htm");
			File.WriteAllText(Path.Combine(regionReclameDir, "02.htm"), "contents 02.htm");
			File.WriteAllText(Path.Combine(regionReclameDir, "2b.gif"), "contents 2b.gif");
			File.WriteAllText(Path.Combine(regionReclameDir, "any.jpg"), "contents any.jpg");
			File.WriteAllText(Path.Combine(regionReclameDir, "main.htm"), "contents main.htm");
			File.WriteAllText(Path.Combine(regionReclameDir, "main.gif"), "contents main.gif");
			var info = new FileInfo(Path.Combine(regionReclameDir, "main.gif"));
			return info.LastWriteTime;
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

				CheckOldReclameArchive(_user, 0, reclame.Region, "r{0}.zip");

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
		public void Get_reclame_for_future_client()
		{
			try {
				ServiceContext.GetResultPath = () => Path.GetFullPath("results\\");
				GetReclameForUser(_user.Login, _user.Id);
			}
			finally {
				ServiceContext.GetResultPath = () => "results\\";
			}
		}

		[Test(Description = "пытаемся получить рекламу для пользователя, который не привязан к системе")]
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
				Assert.That(((UpdateException)lastEvent.MessageObject).Message, Is.EqualTo("Доступ закрыт."));
			}
			finally
			{
				LogManager.ResetConfiguration();			
			}
		}

		[Test(Description = "пытаемся получить рекламу для пользователя без права обновлять AnalitF")]
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
				Assert.That(updateException.Message, Is.EqualTo("Доступ закрыт."));
				Assert.That(updateException.Addition, Is.StringStarting("Для логина " + _disabledUser.Login + " услуга не предоставляется: пользователю не разрешено обновлять AnalitF;"));
			}
			finally
			{
				LogManager.ResetConfiguration();
			}
		}

		[Test(Description = "Проверям, что поле ReclameDate имеет значение null после ограниченного кумулятивного обновления")]
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

			Assert.That(reclameDate, Is.EqualTo(DBNull.Value), "После КО столбец ReclameDate не равен DBNull");

			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				"update usersettings.UserUpdateInfo uui set uui.ReclameDate = now() where uui.UserId = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

			ProcessGetUserData(false, DateTime.Now.AddHours(-1));

			reclameDate = MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				"select uui.ReclameDate from usersettings.UserUpdateInfo uui where uui.UserId = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

			Assert.That(reclameDate, Is.EqualTo(DBNull.Value), "После ограниченнго КО столбец ReclameDate не равен DBNull");
		}

		private void ProcessGetUserData(bool cumulative, DateTime updateTime)
		{
			var service = new PrgDataEx();
			var responce = service.GetUserDataWithPriceCodes(updateTime, cumulative, "6.0.0.1183", 50, "123", "", "", false, null, null);

			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
			{
				var lastUpdateId = Convert.ToUInt32(match);
				service = new PrgDataEx();
				service.CommitExchange(lastUpdateId, false);

				//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
				Thread.Sleep(3000);
			}
			else
				Assert.Fail("Некорректный ответ от сервера при получении данных: {0}", responce);
		}

		private void CheckOldReclameArchive(TestUser user, uint updateId, string regionName, string  mask = null)
		{
			var archiveName = CheckArchive(user, updateId, mask);

			var archFolder = ExtractArchive(archiveName);

			var userReclameDir = string.IsNullOrEmpty(mask) ? Path.Combine(archFolder, "Reclame", regionName) : archFolder;
			Assert.That(Directory.Exists(userReclameDir), "В архиве с обновлением не найден каталог с рекламой");

			var files = Directory.GetFiles(userReclameDir);
			Assert.That(files.Length, Is.GreaterThan(0), "В каталоге с рекламой нет файлов");

			//Список файлов для новой рекламы, которые должны отсутствовать в версии до 1833
			Assert.IsFalse(files.Any(f => f.EndsWith("\\index.htm")));
			Assert.IsFalse(files.Any(f => f.EndsWith("\\2block.gif")));

			//Список файлов для старой рекламы, которые должны присутствовать в версии до 1833
			Assert.IsTrue(files.Any(f => f.EndsWith("\\01.htm")));
			Assert.IsTrue(files.Any(f => f.EndsWith("\\02.htm")));
			Assert.IsTrue(files.Any(f => f.EndsWith("\\2b.gif")));

			//Список файлов, которые должны присутствовать независимо от версии
			Assert.IsTrue(files.Any(f => f.EndsWith("\\any.jpg")));
			Assert.IsTrue(files.Any(f => f.EndsWith("\\main.htm")));
			Assert.IsTrue(files.Any(f => f.EndsWith("\\main.gif")));
		}

		[Test(Description = "Получаем рекламу вместе с обновлением данных")]
		public void GetReclameWithUpdate()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				MySqlHelper.ExecuteNonQuery(
					connection,
					"update usersettings.UserUpdateInfo uui set uui.ReclameDate = null where uui.UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id));
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);
				var reclame = helper.GetReclame();
				Assert.IsTrue(reclame.ShowAdvertising, "Реклама не включена");
				Assert.IsNotNullOrEmpty(reclame.Region, "Не установлен регион рекламы");
				Assert.That(reclame.ReclameDate, Is.EqualTo(new DateTime(2003, 1, 1)), "Дата рекламы не установлена");

				var maxFileTime = SetReclameDir(reclame.Region);

				SetCurrentUser(_user.Login);

				var response = LoadDataAttachments(false, DateTime.Now, "1.0.0.1821", null);

				var updateId = ShouldBeSuccessfull(response);

				CheckOldReclameArchive(_user, updateId, reclame.Region);

				var updateTime = CommitExchange(updateId, RequestType.GetCumulative);

				var date = MySqlHelper.ExecuteScalar(
					connection,
					"select uui.ReclameDate from usersettings.UserUpdateInfo uui where uui.UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id));
				Assert.That(date, Is.Not.Null);
				Assert.That(date.GetType(), Is.EqualTo(typeof(DateTime)));

				//Максимальная дата файла рекламы должна быть больше или равна дате рекламы из UserUpdateInfo.ReclameDate, установленной после обновления
				Assert.That(maxFileTime, Is.EqualTo((DateTime)date).Within(0.9).Seconds, "Не совпадают даты maxFileTime: {0}  и  reclameDate: {1}", maxFileTime, date);

				//Производим повторный запрос данных - дата рекламы не должна измениться
				response = LoadDataAttachments(false, updateTime, "1.0.0.1821", null);
				updateId = ShouldBeSuccessfull(response);
				CommitExchange(updateId, RequestType.GetData);
				var secondDate = MySqlHelper.ExecuteScalar(
					connection,
					"select uui.ReclameDate from usersettings.UserUpdateInfo uui where uui.UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id));
				Assert.That(secondDate, Is.Not.Null);
				Assert.That(secondDate.GetType(), Is.EqualTo(typeof(DateTime)));
				Assert.That(date, Is.EqualTo(secondDate), "дата рекламы не должна измениться");
			}
		}

		[Test(Description = "Получаем рекламу вместе с обновлением данных")]
		public void GetReclameWithUpdateAfterNewReclame()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				MySqlHelper.ExecuteNonQuery(
					connection,
					"update usersettings.UserUpdateInfo uui set uui.ReclameDate = null where uui.UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id));
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);
				var reclame = helper.GetReclame();
				Assert.IsTrue(reclame.ShowAdvertising, "Реклама не включена");
				Assert.IsNotNullOrEmpty(reclame.Region, "Не установлен регион рекламы");
				Assert.That(reclame.ReclameDate, Is.EqualTo(new DateTime(2003, 1, 1)), "Дата рекламы не установлена");

				var maxFileTime = SetReclameDir(reclame.Region);

				SetCurrentUser(_user.Login);

				var response = LoadDataAttachments(false, DateTime.Now, "1.0.0.1840", null);

				var updateId = ShouldBeSuccessfull(response);

				var archiveName = CheckArchive(_user, updateId);

				var archFolder = ExtractArchive(archiveName);

				var userReclameDir = Path.Combine(archFolder, "Reclame", reclame.Region);
				Assert.That(Directory.Exists(userReclameDir), "В архиве с обновлением не найден каталог с рекламой");

				var files = Directory.GetFiles(userReclameDir);
				Assert.That(files.Length, Is.GreaterThan(0), "В каталоге с рекламой нет файлов");

				//Список файлов для новой рекламы, которые должны присутствовать в версии после 1833
				Assert.IsTrue(files.Any(f => f.EndsWith("\\index.htm")));
				Assert.IsTrue(files.Any(f => f.EndsWith("\\2block.gif")));

				//Список файлов для старой рекламы, которые должны отсутствовать в версии после 1833
				Assert.IsFalse(files.Any(f => f.EndsWith("\\01.htm")));
				Assert.IsFalse(files.Any(f => f.EndsWith("\\02.htm")));
				Assert.IsFalse(files.Any(f => f.EndsWith("\\2b.gif")));

				//Список файлов, которые должны присутствовать независимо от версии
				Assert.IsTrue(files.Any(f => f.EndsWith("\\any.jpg")));
				Assert.IsTrue(files.Any(f => f.EndsWith("\\main.htm")));
				Assert.IsTrue(files.Any(f => f.EndsWith("\\main.gif")));

				var updateTime = CommitExchange(updateId, RequestType.GetCumulative);

				var date = MySqlHelper.ExecuteScalar(
					connection,
					"select uui.ReclameDate from usersettings.UserUpdateInfo uui where uui.UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id));
				Assert.That(date, Is.Not.Null);
				Assert.That(date.GetType(), Is.EqualTo(typeof(DateTime)));

				//Максимальная дата файла рекламы должна быть больше или равна дате рекламы из UserUpdateInfo.ReclameDate, установленной после обновления
				Assert.That(maxFileTime, Is.EqualTo((DateTime)date).Within(0.9).Seconds, "Не совпадают даты maxFileTime: {0}  и  reclameDate: {1}", maxFileTime, date);

				//Производим повторный запрос данных - дата рекламы не должна измениться
				response = LoadDataAttachments(false, updateTime, "1.0.0.1840", null);
				updateId = ShouldBeSuccessfull(response);
				CommitExchange(updateId, RequestType.GetData);
				var secondDate = MySqlHelper.ExecuteScalar(
					connection,
					"select uui.ReclameDate from usersettings.UserUpdateInfo uui where uui.UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id));
				Assert.That(secondDate, Is.Not.Null);
				Assert.That(secondDate.GetType(), Is.EqualTo(typeof(DateTime)));
				Assert.That(date, Is.EqualTo(secondDate), "дата рекламы не должна измениться");
			}
		}

		[Test(Description = "проверяем работу метода ExcludeFileNames для пустого списка файлов")]
		public void CheckExludeFileNamesOnEmptyList()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);

				var helper = new UpdateHelper(updateData, connection);

				var reclame = helper.GetReclame();
				
				var files = reclame.ExcludeFileNames(new string[] {});
				var resultFiles = new string[] {};

				Assert.That(files, Is.EquivalentTo(resultFiles));
			}
		}

		[Test(Description = "проверяем работу метода ExcludeFileNames для старого алгоритма рекламы")
		]
		public void CheckExludeFileNamesforOldReclame()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);
				var reclame = helper.GetReclame();

				var files = reclame.ExcludeFileNames(new string[] {"index.htm", "2block.gif", "01.htm", "02.htm", "2b.gif"});
				var resultFiles = new string[] {"01.htm", "02.htm", "2b.gif"};
				Assert.That(files, Is.EquivalentTo(resultFiles));
			}
		}

		[Test(Description = "проверяем работу метода ExcludeFileNames для нового алгоритма рекламы")]
		public void CheckExludeFileNamesforNewReclame()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				updateData.BuildNumber = 1840;
				var helper = new UpdateHelper(updateData, connection);
				var reclame = helper.GetReclame();

				var files = reclame.ExcludeFileNames(new string[] {"index.htm", "2block.gif", "01.htm", "02.htm", "2b.gif"});
				var resultFiles = new string[] {"index.htm", "2block.gif"};
				Assert.That(files, Is.EquivalentTo(resultFiles));
			}
		}

		[Test(Description = "при автообновлении версий (если доступна новая версия) будем всегда передавать заново все файлы рекламы, чтобы не было проблем в будущем")]
		public void ResetReclameDateOnUpdateExe()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				MySqlHelper.ExecuteNonQuery(
					connection,
					"update usersettings.UserUpdateInfo uui set uui.ReclameDate = null where uui.UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id));

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				updateData.BuildNumber = 1840;
				var helper = new UpdateHelper(updateData, connection);

				var reclame = helper.GetReclame();
				Assert.That(reclame.ReclameDate, Is.EqualTo(new DateTime(2003, 1, 1)), "при значении ReclameDate = null дата рекламы должна содержать 01.01.2003");


				MySqlHelper.ExecuteNonQuery(
					connection,
					"update usersettings.UserUpdateInfo uui set uui.ReclameDate = now() where uui.UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id));
				reclame = helper.GetReclame();
				Assert.That(reclame.ReclameDate, Is.GreaterThan(DateTime.Today), "дата рекламы должна содержать текущую дату");


				updateData.UpdateExeVersionInfo = new VersionInfo(1869);
				reclame = helper.GetReclame();
				Assert.That(reclame.ReclameDate, Is.EqualTo(new DateTime(2003, 1, 1)), "при автообновлении дата рекламы должна быть сброшена в 01.01.2003");
			}
		}

	}

}