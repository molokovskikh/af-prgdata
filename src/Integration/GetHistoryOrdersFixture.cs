using System.Configuration;
using System.IO;
using System.Linq;
using Castle.ActiveRecord;
using Common.Tools;
using Inforoom.Common;
using Integration.BaseTests;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;
using PrgData;
using System;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;
using System.Data;
using NHibernate.Criterion;
using Test.Support.Documents;

namespace Integration
{
	[TestFixture]
	public class GetHistoryOrdersFixture : PrepareDataFixture
	{
		private TestUser _user;

		private uint _lastUpdateId;
		private bool _fullHistory;
		private string _responce;

		[SetUp]
		public override void Setup()
		{
			FixtureSetup();

			base.Setup();

			_user = CreateUser();

			RegisterLogger();
		}

		[TearDown]
		public void TearDown()
		{
			CheckForErrors();
		}

		private string LoadData(string appVersion)
		{
			var service = new PrgDataEx();
			_responce = service.GetHistoryOrders(appVersion, UniqueId, new ulong[0], 1, 1);

			if (_responce.Contains("FullHistory=True"))
				_fullHistory = true;
			else
			{
				if (_responce.Contains("GetFileHistoryHandler.ashx?Id="))
				{
					var match = Regex.Match(_responce, @"\d+").Value;
					if (match.Length > 0)
						_lastUpdateId = Convert.ToUInt32(match);
				}
				else
					Assert.Fail("Нераспознанный ответ от сервера при запросе истории заказов: {0}", _responce);
			}
			return _responce;
		}

		private void CommitExchange()
		{
			var service = new PrgDataEx();

			service.CommitHistoryOrders(_lastUpdateId);
		}

		private void CheckGetHistoryOrders(string login, string appVersion)
		{
			SetCurrentUser(login);
			_lastUpdateId = 0;
			_fullHistory = false;
			LoadData(appVersion);

			Assert.That(_responce, Is.Not.StringContaining("Error=").IgnoreCase, "Ответ от сервера указывает, что имеется ошибка");

			if (!_fullHistory)
				Assert.That(_lastUpdateId, Is.GreaterThan(0), "UpdateId не установлен");
		}

		[Test]
		public void Get_history_orders()
		{
			CheckGetHistoryOrders(_user.Login, "6.0.7.1183");

			if (!_fullHistory)
			{
				var commit =
					Convert.ToBoolean(MySqlHelper.ExecuteScalar(Settings.ConnectionString(),
																"select Commit from logs.AnalitFUpdates where UpdateId = " +
																_lastUpdateId));
				Assert.IsFalse(commit, "Запрос с историей заказов считается подтвержденным");

				CommitExchange();

				commit =
					Convert.ToBoolean(MySqlHelper.ExecuteScalar(Settings.ConnectionString(),
																"select Commit from logs.AnalitFUpdates where UpdateId = " +
																_lastUpdateId));
				Assert.IsTrue(commit, "Запрос с историей заказов считается неподтвержденным");
			}
		}

		[Test(Description = "запрос истории заказов с документами")]
		public void GetHistoryOrdersWithDocs()
		{
			var doc = CreateDocument(_user);
			TestDocumentSendLog log;
			using (new SessionScope())
			{
				log = TestDocumentSendLog.Queryable.First(t => t.Document == doc);
				Assert.That(log.Committed, Is.False);
				log.Committed = true;
				log.Save();
			}

			CheckGetHistoryOrders(_user.Login, "6.0.7.1821");

			Assert.That(_fullHistory, Is.False, "Не должна быть загружена вся история заказов");

			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.False);
			}

			var commit =
				Convert.ToBoolean(MySqlHelper.ExecuteScalar(Settings.ConnectionString(),
															"select Commit from logs.AnalitFUpdates where UpdateId = " +
															_lastUpdateId));
			Assert.IsFalse(commit, "Запрос с историей заказов считается подтвержденным");

			var archiveName = CheckArchive(_user, _lastUpdateId, "Orders{0}.zip");

			var archFolder = ExtractArchive(archiveName);

			var files = Directory.GetFiles(archFolder);
			Assert.That(files.Length, Is.GreaterThan(0), "В каталоге с рекламой нет файлов");

			CommitExchange();

			commit =
				Convert.ToBoolean(MySqlHelper.ExecuteScalar(Settings.ConnectionString(),
															"select Commit from logs.AnalitFUpdates where UpdateId = " +
															_lastUpdateId));
			Assert.IsTrue(commit, "Запрос с историей заказов считается неподтвержденным");

			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.True);
			}
		}

		[Test(Description = "запрос истории заказов с документами и сопоставление документов заказам")]
		public void GetHistoryOrdersWithDocsAndMatching()
		{
			var doc = CreateDocument(_user);
			TestDocumentSendLog log;
			using (new SessionScope())
			{
				log = TestDocumentSendLog.Queryable.First(t => t.Document == doc);
				Assert.That(log.Committed, Is.False);
				log.Committed = true;
				log.Save();
			}

			CheckGetHistoryOrders(_user.Login, "6.0.7.1828");

			Assert.That(_fullHistory, Is.False, "Не должна быть загружена вся история заказов");

			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.False);
			}

			var commit =
				Convert.ToBoolean(MySqlHelper.ExecuteScalar(Settings.ConnectionString(),
															"select Commit from logs.AnalitFUpdates where UpdateId = " +
															_lastUpdateId));
			Assert.IsFalse(commit, "Запрос с историей заказов считается подтвержденным");

			CommitExchange();

			commit =
				Convert.ToBoolean(MySqlHelper.ExecuteScalar(Settings.ConnectionString(),
															"select Commit from logs.AnalitFUpdates where UpdateId = " +
															_lastUpdateId));
			Assert.IsTrue(commit, "Запрос с историей заказов считается неподтвержденным");

			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.True);
			}
		}

	}
}