using System;
using System.IO;
using System.Linq;
using Castle.ActiveRecord;
using Common.Tools;
using Inforoom.Common;
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
			ArchiveHelper.SevenZipExePath = @".\7zip\7z.exe";

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

			using (var transaction = new TransactionScope())
			{
				_client.Users.Each(u =>
				{
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
			var logs = TestAnalitFUserActionLog.Queryable.Where(l => l.UserId == _user.Id).ToList();
			Assert.That(logs.Count, Is.EqualTo(0), "Найдена статистика для созданного пользователя");

			var service = new PrgData.PrgDataEx();

			var response = service.SendUserActions(_afAppVersion, 1, GetLogContent());
			Assert.That(response, Is.EqualTo("Res=OK"), "Неожидаемый ответ от сервера");

			var logsAftreImport = TestAnalitFUserActionLog.Queryable.Where(l => l.UserId == _user.Id && l.UpdateId == 1).ToList();
			Assert.That(logsAftreImport.Count, Is.GreaterThan(0), "Статистика для пользователя не импортировалась");
		}

	}
}