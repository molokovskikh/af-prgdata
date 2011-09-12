using System;
using System.IO;
using System.Linq;
using Castle.ActiveRecord;
using Common.Tools;
using Inforoom.Common;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;
using Test.Support.Logs;

namespace Integration
{
	[TestFixture(Description = "����� ��������� ���������� ������������")]
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
			Assert.That(logFileBytes.Length, Is.GreaterThan(0), "���� �� ����������� �������� ����, ��������, ��� ��� � �����");

			return Convert.ToBase64String(logFileBytes);
		}

		[Test(Description = "���������� ���������� ������������")]
		public void SimpleSendLog()
		{
			var logs = TestAnalitFUserActionLog.Queryable.Where(l => l.UserId == _user.Id).ToList();
			Assert.That(logs.Count, Is.EqualTo(0), "������� ���������� ��� ���������� ������������");

			var service = new PrgData.PrgDataEx();

			var response = service.SendUserActions(_afAppVersion, 1, GetLogContent());
			Assert.That(response, Is.EqualTo("Res=OK"), "����������� ����� �� �������");

			var logsAftreImport = TestAnalitFUserActionLog.Queryable.Where(l => l.UserId == _user.Id && l.UpdateId == 1).ToList();
			Assert.That(logsAftreImport.Count, Is.GreaterThan(0), "���������� ��� ������������ �� ���������������");
		}

		[Test(Description = "������� ������� ����������� �� ������������ 12061 ������"), Ignore("������ ���������� ��������")]
		public void ParseErrorArchive()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);

				var helper = new SendUserActionsHandler(updateData, 1, connection);

				try
				{
					helper.PrepareLogFile("N3q8ryccAAKNm9UPAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
				}
				finally
				{
					helper.DeleteTemporaryFiles();
				}
			}
		}

	}
}