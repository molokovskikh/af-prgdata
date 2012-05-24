using System.Configuration;
using System.IO;
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
		}

		private string SimpleLoadData()
		{
			return LoadData("6.0.7.1183");
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

		private void CheckGetHistoryOrders(string login)
		{
			SetCurrentUser(login);
			_lastUpdateId = 0;
			_fullHistory = false;
			SimpleLoadData();

			Assert.That(_responce, Is.Not.StringContaining("Error=").IgnoreCase, "Ответ от сервера указывает, что имеется ошибка");

			if (!_fullHistory)
				Assert.That(_lastUpdateId, Is.GreaterThan(0), "UpdateId не установлен");
		}

		[Test]
		public void Get_history_orders()
		{
			CheckGetHistoryOrders(_user.Login);

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

	}
}