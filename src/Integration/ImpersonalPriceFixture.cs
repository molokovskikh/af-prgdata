using System.Configuration;
using System.IO;
using Castle.ActiveRecord;
using Common.Tools;
using Inforoom.Common;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Layout;
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
	public class ImpersonalPriceFixture
	{
		private TestClient client;
		private TestUser user;

		private TestOldClient offersOldClient;
		private TestClient offersFutureClient;
		private TestUser offersFutureUser;

		private TestSmartOrderRule smartRuleOld;
		private TestSmartOrderRule smartRuleFuture;
		private TestDrugstoreSettings orderRuleFuture;
		private TestDrugstoreSettings orderRuleOld;

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

				var offersRegion = TestRegion.FindFirst(Expression.Like("Name", "Петербург", MatchMode.Anywhere));
				Assert.That(offersRegion, Is.Not.Null, "Не нашли регион 'Санкт-Петербург' для offersClient");

				offersOldClient = TestOldClient.CreateTestClient(offersRegion.Id);

				offersFutureClient = TestClient.Create(offersRegion.Id);
				offersFutureUser = offersFutureClient.Users[0];
				offersFutureClient.Users.Each(u =>
				{
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				offersFutureUser.Update();

				client = TestClient.Create(offersRegion.Id);
				user = client.Users[0];

				client.Users.Each(u =>
									{
										u.AssignedPermissions.Add(permission);
										u.SendRejects = true;
										u.SendWaybills = true;
									});
				user.Update();

				oldClient = TestOldClient.CreateTestClient(offersRegion.Id);
				oldUser = oldClient.Users[0];

				smartRuleOld = new TestSmartOrderRule();
				smartRuleOld.OffersClientCode = offersOldClient.Id;
				smartRuleOld.SaveAndFlush();

				smartRuleFuture = new TestSmartOrderRule();
				smartRuleFuture.OffersClientCode = offersFutureUser.Id;
				smartRuleFuture.SaveAndFlush();

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

			using (var transaction = new TransactionScope())
			{
				orderRuleFuture = TestDrugstoreSettings.Find(client.Id);
				orderRuleFuture.SmartOrderRule = smartRuleFuture;
				orderRuleFuture.EnableImpersonalPrice = true;
				orderRuleFuture.UpdateAndFlush();

				orderRuleOld = TestDrugstoreSettings.Find(oldClient.Id);
				orderRuleOld.SmartOrderRule = smartRuleOld;
				orderRuleOld.EnableImpersonalPrice = true;
				orderRuleOld.UpdateAndFlush();
			}

			if (Directory.Exists("FtpRoot"))
				FileHelper.DeleteDir("FtpRoot");

			Directory.CreateDirectory("FtpRoot");
		}

		[Test]
		public void Check_update_helper_for_Future()
		{
			CheckUpdateHelper(user.Login, offersFutureUser.Id, offersFutureClient.RegionCode);
		}

		[Test]
		public void Check_update_helper_for_old()
		{
			CheckUpdateHelper(oldUser.OSUserName, offersOldClient.Id, offersOldClient.RegionCode);
		}

		public void CheckUpdateHelper(string login, uint offersClientId, ulong offersRegionId)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, login);
				var helper = new UpdateHelper(updateData, connection);

				Assert.That(updateData.EnableImpersonalPrice, Is.True, "Не включен механизм 'Обезличенный прайс'");
				Assert.That(updateData.OffersClientCode, Is.EqualTo(offersClientId), "Не совпадает ид OffersClientCode");
				Assert.That(updateData.OffersRegionCode, Is.EqualTo(offersRegionId), "Не совпадает код региона у OffersClientCode");

				CheckSQL(false, connection, updateData, helper);

				CheckSQL(true, connection, updateData, helper);

				//var selectCommand = new MySqlCommand() { Connection = connection };
				//helper.SetUpdateParameters(selectCommand, true, DateTime.Now.AddDays(-10), DateTime.Now);

				//helper.PrepareImpersonalOffres(selectCommand);
				//selectCommand.CommandText = "select Count(*) from CoreAssortment A WHERE A.CodeFirmCr IS NOT NULL";
				//var countWithProducers = Convert.ToUInt32(selectCommand.ExecuteScalar());
				//selectCommand.CommandText = "select Count(*) from CoreProducts A ";
				//var countProducts = Convert.ToUInt32(selectCommand.ExecuteScalar());

				//Console.WriteLine("Offers count = {0} : withProducers : {1}  Products : {2}", countWithProducers + countProducts, countWithProducers, countProducts);
			}
		}

		private void CheckSQL(bool cumulative, MySqlConnection connection, UpdateData updateData, UpdateHelper helper)
		{
			var selectCommand = new MySqlCommand() { Connection = connection };
			helper.SetUpdateParameters(selectCommand, cumulative, DateTime.Now.AddDays(-10), DateTime.Now);

			CheckFillData(selectCommand, helper.GetSynonymCommand(cumulative), updateData);

			CheckFillData(selectCommand, helper.GetSynonymFirmCrCommand(cumulative), updateData);

			CheckFillData(selectCommand, helper.GetRegionsCommand(), updateData);

			CheckFillData(selectCommand, helper.GetMinReqRuleCommand(), updateData);

			try
			{
				//selectCommand.CommandText =
				//    "drop temporary table if exists UserSettings.Prices; create temporary table UserSettings.Prices ENGINE = MEMORY select ClientCode from usersettings.RetClientsSet limit 1;";
				//selectCommand.ExecuteNonQuery();
				CheckFillData(selectCommand, helper.GetPricesRegionalDataCommand(), updateData);

				CheckFillData(selectCommand, helper.GetRegionalDataCommand(), updateData);
				
				
			}
			finally
			{
				//selectCommand.CommandText =
				//    "drop temporary table if exists UserSettings.Prices;";
				//selectCommand.ExecuteNonQuery();
			}
		}

		private void CheckFillData(MySqlCommand selectCommand, string sqlCommand, UpdateData updateData)
		{
			var dataAdapter = new MySqlDataAdapter(selectCommand);
			selectCommand.CommandText = sqlCommand;
			var dataTable = new DataTable();
			dataAdapter.Fill(dataTable);
			Assert.That(dataTable.Rows.Count, Is.GreaterThan(0), "Запрос не вернул данные: {0}", sqlCommand);

			if (dataTable.Columns.Contains("RegionCode"))
			{
				var rows = dataTable.Select("RegionCode = " + updateData.OffersRegionCode);
				Assert.That(rows.Length, Is.EqualTo(dataTable.Rows.Count), 
					"Не все записи в таблице в столбце RegionCode имеют значение {0}: {1}", updateData.OffersRegionCode, sqlCommand);
			}

			if (dataTable.Columns.Contains("PriceCode"))
			{
				var rows = dataTable.Select("PriceCode = " + updateData.ImpersonalPriceId);
				Assert.That(rows.Length, Is.EqualTo(dataTable.Rows.Count),
					"Не все записи в таблице в столбце PriceCode имеют значение {0}: {1}", updateData.ImpersonalPriceId, sqlCommand);
			}

		}

		[Test]
		public void Check_GetUserData_for_Future()
		{
			CheckGetUserData(user.Login);
		}

		[Test]
		public void Check_GetUserData_for_Old()
		{
			CheckGetUserData(oldUser.OSUserName);
		}

		[Test]
		public void Check_AnalitFReplicationInfo_after_GetUserData()
		{
			var ExistsFirms = MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				@"
call future.GetPrices(?OffersClientCode);
select
  count(*)
from
  Prices
  left join usersettings.AnalitFReplicationInfo afi on afi.FirmCode = Prices.FirmCode and afi.UserId = ?UserId
where
  afi.UserId is null;",
				new MySqlParameter("?OffersClientCode", offersFutureUser.Id),
				new MySqlParameter("?UserId", user.Id));

			Assert.That(
				ExistsFirms,
				Is.GreaterThan(0),
				"Хотя клиент {0} создан в другом регионе {1} у него в AnalitFReplicationInfo добавлены все фирмы из региона {2}",
				client.Id,
				client.RegionCode,
				offersFutureClient.RegionCode);

			CheckGetUserData(user.Login);

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				MySqlHelper.ExecuteNonQuery(
					connection,
					"call future.GetPrices(?OffersClientCode)",
					new MySqlParameter("?OffersClientCode", offersFutureUser.Id));

				var nonExistsFirms = MySqlHelper.ExecuteScalar(
					connection,
					@"
select
  count(*)
from
  Prices
  left join usersettings.AnalitFReplicationInfo afi on afi.FirmCode = Prices.FirmCode and afi.UserId = ?UserId
where
  afi.UserId is null",
					new MySqlParameter("?UserId", user.Id));

				Assert.That(
					nonExistsFirms,
					Is.EqualTo(0),
					"У клиента {0} в AnalitFReplicationInfo должны быть все фирмы из региона {1}",
					client.Id,
					offersFutureClient.RegionCode);

				var nonExistsForce = MySqlHelper.ExecuteScalar(
					connection,
					@"
select
  count(*)
from
  Prices
  left join usersettings.AnalitFReplicationInfo afi on afi.FirmCode = Prices.FirmCode and afi.UserId = ?UserId
where
	afi.UserId is not null
and afi.ForceReplication = 0",
					new MySqlParameter("?UserId", user.Id));

				Assert.That(
					nonExistsForce,
					Is.EqualTo(0),
					"У клиента {0} в AnalitFReplicationInfo не должно быть строк с ForceReplication в 0 для фирм из региона {1}",
					client.Id,
					offersFutureClient.RegionCode);
			}

			CommitExchange();

			var nonExistsForceGt0 = MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				@"
call future.GetPrices(?OffersClientCode);
select
  count(*)
from
  Prices
  left join usersettings.AnalitFReplicationInfo afi on afi.FirmCode = Prices.FirmCode and afi.UserId = ?UserId
where
	afi.UserId is not null
and afi.ForceReplication > 0",
				new MySqlParameter("?OffersClientCode", offersFutureUser.Id),
				new MySqlParameter("?UserId", user.Id));

			Assert.That(
				nonExistsForceGt0,
				Is.EqualTo(0),
				"У клиента {0} в AnalitFReplicationInfo не должно быть строк с ForceReplication > 0 для фирм из региона {1}",
				client.Id,
				offersFutureClient.RegionCode);
		}

		[Test(Description = "Проверка на используемую версию программы AnalitF")]
		public void CheckBuildNo()
		{
			CheckGetUserData(user.Login);

			var updateTime = CommitExchange();

			var serviceResult = LoadData(false, updateTime, "6.0.7.100");

			Assert.That(serviceResult, Is.StringStarting("Error=Используемая версия программы не актуальна").IgnoreCase, "Неожидаемый ответ от сервера");
			Assert.That(serviceResult, Is.StringContaining("Desc=Доступ закрыт").IgnoreCase, "Неожидаемый ответ от сервера");
		}

		private void CheckGetUserData(string login)
		{
			using (var writer = new StringWriter())
			{
				try
				{
					var textAppender = new TextWriterAppender()
										{
											Writer = writer,
											Layout = new PatternLayout("%d{dd.MM.yyyy HH:mm:ss.fff} %property{user} [%t] %-5p %c - %m%n")
										};
					BasicConfigurator.Configure(textAppender);

					SetCurrentUser(login);
					lastUpdateId = 0;
					SimpleLoadData();

					Assert.That(responce, Is.Not.StringContaining("Error=").IgnoreCase, "Ответ от сервера указывает, что имеется ошибка.\r\nLog:\r\n:{0}", writer);
					Assert.That(lastUpdateId, Is.GreaterThan(0), "UpdateId не установлен.\r\nLog:\r\n:{0}", writer);
				}
				finally
				{
					LogManager.ResetConfiguration();			
				}
			}
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}

		private string SimpleLoadData()
		{
			return LoadData(false, DateTime.Now, "6.0.7.1183");
		}

		private string LoadData(bool getEtalonData, DateTime accessTime, string appVersion)
		{
			var service = new PrgDataEx();
			service.ResultFileName = "results";
			responce = service.GetUserData(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", false);

			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
				lastUpdateId = Convert.ToUInt32(match);
			return responce;
		}

		private DateTime CommitExchange()
		{
			var service = new PrgDataEx();
			service.ResultFileName = "results";

			var updateTime = service.CommitExchange(lastUpdateId, false);

			var dbUpdateTime = Convert.ToDateTime( MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				@"
select 
  uui.UpdateDate 
from 
  logs.AnalitFUpdates afu
  join usersettings.UserUpdateInfo uui on uui.UserId = afu.UserId
where
  afu.UpdateId = ?UpdateId"
				,
				new MySqlParameter("?UpdateId", lastUpdateId)));

			Assert.That(updateTime, Is.EqualTo(dbUpdateTime.ToUniversalTime()), "Не совпадает дата обновления, выбранная из базы, для UpdateId: {0}", lastUpdateId);

			return updateTime;
		}

		[Test]
		public void Check_send_letter()
		{
			SetCurrentUser(user.Login);
			var service = new PrgDataEx();
			var letterResponse = service.SendLetter("Test subject", "test body", null);
			Assert.That(letterResponse, Is.EqualTo("Res=OK").IgnoreCase, "Неожидаемый ответ сервера при отправке письма");
		}

		[Test]
		public void Check_send_letter_by_unknow_user()
		{
			try
			{
				var memoryAppender = new MemoryAppender();
				BasicConfigurator.Configure(memoryAppender);

				SetCurrentUser("dsdsdsdsds");
				var service = new PrgDataEx();
				var letterResponse = service.SendLetter("Test subject", "test body", null);
				Assert.That(letterResponse, Is.EqualTo("Error=Не удалось отправить письмо. Попробуйте позднее.").IgnoreCase, "Неожидаемый ответ сервера при отправке письма");

				var events = memoryAppender.GetEvents();
				var lastEvent = events[events.Length - 1];
				Assert.That(lastEvent.Level, Is.EqualTo(Level.Error));
				Assert.That(lastEvent.ExceptionObject, Is.TypeOf(typeof(Exception)));
				Assert.That(((Exception)lastEvent.ExceptionObject).Message, Is.StringStarting("Не удалось найти клиента для указанных учетных данных:").IgnoreCase);
			}
			finally
			{
				LogManager.ResetConfiguration();
			}
		}
		
	}
}