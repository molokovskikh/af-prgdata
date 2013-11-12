using System;
using System.IO;
using System.Linq;
using Castle.ActiveRecord;
using Common.MySql;
using Common.Tools;
using Inforoom.Common;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;
using Test.Support.Logs;

namespace Integration
{
	[TestFixture(Description = "Тесты механизма статистики пользователя")]
	public class SendUserActionsFixture
	{
		private TestClient _client;
		private TestUser _user;

		private string _afAppVersion;

		[TestFixtureSetUp]
		public void FixtureSetUp()
		{
			_afAppVersion = "1.1.1.1413";

			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ServiceContext.GetResultPath = () => "results\\";
		}

		[SetUp]
		public void SetUp()
		{
			_client = TestClient.Create();
			_user = _client.Users[0];

			using (var transaction = new TransactionScope()) {
				_client.Users.Each(u => {
					u.SendRejects = true;
					u.SendWaybills = true;
				});

				_client.Update();

				transaction.VoteCommit();
			}

			SetCurrentUser(_user.Login);
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}

		private string GetLogContent()
		{
			var logFileBytes = File.ReadAllBytes("..\\..\\Data\\UserActionLog.7z");
			Assert.That(logFileBytes.Length, Is.GreaterThan(0), "Файл со статистикой оказался пуст, возможно, его нет в папке");

			return Convert.ToBase64String(logFileBytes);
		}

		[Test(Description = "Отправляем статистику пользователя")]
		public void SimpleSendLog()
		{
			using (new SessionScope()) {
				var logs = TestAnalitFUserActionLog.Queryable.Where(l => l.UserId == _user.Id).ToList();
				Assert.That(logs.Count, Is.EqualTo(0), "Найдена статистика для созданного пользователя");
			}

			var service = new PrgData.PrgDataEx();

			var response = service.SendUserActions(_afAppVersion, 1, GetLogContent());
			Assert.That(response, Is.EqualTo("Res=OK"), "Неожидаемый ответ от сервера");

			using (new SessionScope()) {
				var logsAftreImport = TestAnalitFUserActionLog.Queryable.Where(l => l.UserId == _user.Id && l.UpdateId == 1).ToList();
				Assert.That(logsAftreImport.Count, Is.GreaterThan(0), "Статистика для пользователя не импортировалась");
			}
		}

		[Test(Description = "Попытка разбора полученного от пользователя 12061 архива"), Ignore("Разбор конкретной проблемы")]
		public void ParseErrorArchive()
		{
			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);

				var helper = new SendUserActionsHandler(updateData, 1, connection);

				try {
					helper.PrepareLogFile("N3q8ryccAAKNm9UPAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
				}
				finally {
					helper.DeleteTemporaryFiles();
				}
			}
		}

		[Test(Description = "проверка работы функций GetRandomFileName и GetTempFileName")]
		public void TestTempFileName()
		{
			var fullFileName = Path.GetTempFileName();
			try {
				Assert.That(File.Exists(fullFileName), Is.True, "после вызова функции GetTempFileName должен существовать временный файл");
				var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fullFileName);
				Assert.That(fileNameWithoutExtension, Is.Not.EqualTo(Path.GetFileName(fullFileName)), "Временный файл должен создаваться с расширением");
			}
			finally {
				if (File.Exists(fullFileName))
					File.Delete(fullFileName);
			}

			var randomFileName = Path.GetRandomFileName();
			Assert.That(randomFileName, Is.EqualTo(Path.GetFileName(randomFileName)), "Случайный файл не должен содержать путь");
			Assert.That(File.Exists(randomFileName), Is.False, "Случайный файл не должен существовать в текущем каталоге");
			Assert.That(File.Exists(Path.Combine(Path.GetTempPath(), randomFileName)), Is.False, "Случайный файл не должен существовать во временном каталоге");
		}

		[Test(Description = "После работы Handler'а не должно быть новых файлов в папке с временными файлами")]
		public void EmptyTempFoldersAfterWork()
		{
			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);

				FoldersHelper.CheckTempFolders(() => {
					var helper = new SendUserActionsHandler(updateData, 1, connection);

					try {
						helper.PrepareLogFile("N3q8ryccAAKNm9UPAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
					}
					catch (Exception e) {
						Assert.That(e.Message, Is.EqualTo("Полученный архив не содержит файлов."));
					}
					finally {
						helper.DeleteTemporaryFiles();
					}
				});

				FoldersHelper.CheckTempFolders(() => {
					var helper = new SendUserActionsHandler(updateData, 1, connection);

					try {
						helper.PrepareLogFile(GetLogContent());
					}
					finally {
						helper.DeleteTemporaryFiles();
					}
				});
			}
		}
	}
}