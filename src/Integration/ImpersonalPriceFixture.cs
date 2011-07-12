using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Reflection;
using System.Threading;
using Castle.ActiveRecord;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Tests.Repositories;
using Common.Tools;
using Inforoom.Common;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Filter;
using log4net.Layout;
using LumiSoft.Net.IMAP;
using LumiSoft.Net.Mail;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.Model;
using PrgData.Common.Repositories;
using SmartOrderFactory.Domain;
using Test.Support;
using PrgData;
using System;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;
using System.Data;
using NHibernate.Criterion;
using LumiSoft.Net.IMAP.Client;

namespace Integration
{
	[TestFixture]
	public class ImpersonalPriceFixture
	{
		private TestClient client;
		private TestUser user;

		//private TestOldClient offersOldClient;
		private TestClient offersFutureClient;
		private TestUser offersFutureUser;

		//private TestSmartOrderRule smartRuleOld;
		private TestSmartOrderRule smartRuleFuture;
		private TestDrugstoreSettings orderRuleFuture;
		//private TestDrugstoreSettings orderRuleOld;

		//TestOldClient oldClient;
		//TestOldUser oldUser;

		private uint lastUpdateId;
		private string responce;

		private string UniqueId;

		[SetUp]
		public void Setup()
		{
			UniqueId = "123";

			ServiceContext.GetUserHost = () => "127.0.0.1";
			ServiceContext.GetResultPath = () => "results\\";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ConfigurationManager.AppSettings["DocumentsPath"] = "FtpRoot\\";

			var offersRegion = TestRegion.FindFirst(Expression.Like("Name", "���������", MatchMode.Anywhere));
			Assert.That(offersRegion, Is.Not.Null, "�� ����� ������ '�����-���������' ��� offersClient");

			//offersOldClient = TestOldClient.CreateTestClient(offersRegion.Id);
			offersFutureClient = TestClient.Create(offersRegion.Id, offersRegion.Id);

			client = TestClient.Create(offersRegion.Id, offersRegion.Id);

			using (var transaction = new TransactionScope())
			{
				offersFutureUser = offersFutureClient.Users[0];
				offersFutureClient.Users.Each(u =>
				{
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				offersFutureUser.Update();

				user = client.Users[0];
				client.Users.Each(u =>
									{
										u.SendRejects = true;
										u.SendWaybills = true;
									});
				user.Update();

				//oldClient = TestOldClient.CreateTestClient(offersRegion.Id);
				//oldUser = oldClient.Users[0];

				//smartRuleOld = new TestSmartOrderRule();
				//smartRuleOld.OffersClientCode = offersOldClient.Id;
				//smartRuleOld.SaveAndFlush();

				smartRuleFuture = new TestSmartOrderRule();
				smartRuleFuture.OffersClientCode = offersFutureUser.Id;
				smartRuleFuture.SaveAndFlush();

//                var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof(ActiveRecordBase));
//                try
//                {
//                    session.CreateSQLQuery(@"
//				insert into usersettings.AssignedPermissions (PermissionId, UserId) values (:permissionid, :userid)")
//                        .SetParameter("permissionid", permission.Id)
//                        .SetParameter("userid", oldUser.Id)
//                        .ExecuteUpdate();
//                }
//                finally
//                {
//                    ActiveRecordMediator.GetSessionFactoryHolder().ReleaseSession(session);
//                }
			}

			using (var transaction = new TransactionScope())
			{
				orderRuleFuture = TestDrugstoreSettings.Find(client.Id);
				orderRuleFuture.SmartOrderRule = smartRuleFuture;
				orderRuleFuture.EnableImpersonalPrice = true;
				orderRuleFuture.UpdateAndFlush();

				//orderRuleOld = TestDrugstoreSettings.Find(oldClient.Id);
				//orderRuleOld.SmartOrderRule = smartRuleOld;
				//orderRuleOld.EnableImpersonalPrice = true;
				//orderRuleOld.UpdateAndFlush();
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

		[Test, Ignore("���� ��� ������ ��������")]
		public void Check_update_helper_for_old()
		{
			//CheckUpdateHelper(oldUser.OSUserName, offersOldClient.Id, offersOldClient.RegionCode);
		}

		public void CheckUpdateHelper(string login, uint offersClientId, ulong offersRegionId)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, login);
				var helper = new UpdateHelper(updateData, connection);

				Assert.That(updateData.EnableImpersonalPrice, Is.True, "�� ������� �������� '������������ �����'");
				Assert.That(updateData.OffersClientCode, Is.EqualTo(offersClientId), "�� ��������� �� OffersClientCode");
				Assert.That(updateData.OffersRegionCode, Is.EqualTo(offersRegionId), "�� ��������� ��� ������� � OffersClientCode");

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
			Assert.That(dataTable.Rows.Count, Is.GreaterThan(0), "������ �� ������ ������: {0}", sqlCommand);

			if (dataTable.Columns.Contains("RegionCode"))
			{
				var rows = dataTable.Select("RegionCode = " + updateData.OffersRegionCode);
				Assert.That(rows.Length, Is.EqualTo(dataTable.Rows.Count), 
					"�� ��� ������ � ������� � ������� RegionCode ����� �������� {0}: {1}", updateData.OffersRegionCode, sqlCommand);
			}

			if (dataTable.Columns.Contains("PriceCode"))
			{
				var rows = dataTable.Select("PriceCode = " + updateData.ImpersonalPriceId);
				Assert.That(rows.Length, Is.EqualTo(dataTable.Rows.Count),
					"�� ��� ������ � ������� � ������� PriceCode ����� �������� {0}: {1}", updateData.ImpersonalPriceId, sqlCommand);
			}

		}

		[Test]
		public void Check_GetUserData_for_Future()
		{
			CheckGetUserData(user.Login);
		}

		[Test, Ignore("���� ��� ������ ��������")]
		public void Check_GetUserData_for_Old()
		{
			//CheckGetUserData(oldUser.OSUserName);
		}

		[Test]
		public void Check_AnalitFReplicationInfo_after_GetUserData()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var ExistsFirms = MySqlHelper.ExecuteScalar(
					connection,
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
					"���� ������ {0} ������ � ������ ������� {1} � ���� � AnalitFReplicationInfo ��������� ��� ����� �� ������� {2}",
					client.Id,
					client.RegionCode,
					offersFutureClient.RegionCode);
			}

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
					"� ������� {0} � AnalitFReplicationInfo ������ ���� ��� ����� �� ������� {1}",
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
					"� ������� {0} � AnalitFReplicationInfo �� ������ ���� ����� � ForceReplication � 0 ��� ���� �� ������� {1}",
					client.Id,
					offersFutureClient.RegionCode);
			}

			CommitExchange();

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var nonExistsForceGt0 = MySqlHelper.ExecuteScalar(
					connection,
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
					"� ������� {0} � AnalitFReplicationInfo �� ������ ���� ����� � ForceReplication > 0 ��� ���� �� ������� {1}",
					client.Id,
					offersFutureClient.RegionCode);
			}
		}

		[Test(Description = "�������� �� ������������ ������ ��������� AnalitF")]
		public void CheckBuildNo()
		{
			CheckGetUserData(user.Login);

			var updateTime = CommitExchange();

			var serviceResult = LoadData(false, updateTime, "6.0.7.100");

			Assert.That(serviceResult, Is.StringStarting("Error=������������ ������ ��������� �� ���������").IgnoreCase, "����������� ����� �� �������");
			Assert.That(serviceResult, Is.StringContaining("Desc=������ ������").IgnoreCase, "����������� ����� �� �������");
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
											Layout = new PatternLayout("%d{dd.MM.yyyy HH:mm:ss.fff} %property{user} [%t] %-5p %c - %m%n"),
										};
					textAppender.AddFilter(new LoggerMatchFilter{ AcceptOnMatch = true, LoggerToMatch = "PrgData", Next = new DenyAllFilter()});
					BasicConfigurator.Configure(textAppender);

					SetCurrentUser(login);
					lastUpdateId = 0;
					SimpleLoadData();

					Assert.That(responce, Is.Not.StringContaining("Error=").IgnoreCase, "����� �� ������� ���������, ��� ������� ������.\r\nLog:\r\n:{0}", writer);
					Assert.That(lastUpdateId, Is.GreaterThan(0), "UpdateId �� ����������.\r\nLog:\r\n:{0}", writer);
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
			responce = service.GetUserData(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", false);

			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
				lastUpdateId = Convert.ToUInt32(match);
			return responce;
		}

		private DateTime CommitExchange()
		{
			var service = new PrgDataEx();

			var updateTime = service.CommitExchange(lastUpdateId, false);

			//����� �������, �.�. �� �������� ������������ ����� ������������� ����������
			Thread.Sleep(3000);

			var dbUpdateTime = Convert.ToDateTime(MySqlHelper.ExecuteScalar(
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

			Assert.That(updateTime, Is.EqualTo(dbUpdateTime.ToUniversalTime()), "�� ��������� ���� ����������, ��������� �� ����, ��� UpdateId: {0}", lastUpdateId);

			return updateTime;
		}

		[Test]
		public void Check_send_letter()
		{
			using(var imapClient = new IMAP_Client())
			{
				imapClient.Connect("box.analit.net", 143);
				try
				{
					var allset = new IMAP_SequenceSet();
					allset.Parse("1:*", long.MaxValue);

					imapClient.Login("kvasovtest@analit.net", "12345678");
					imapClient.SelectFolder("INBOX");

					var fetchDataItems = new IMAP_Fetch_DataItem[]{new IMAP_Fetch_DataItem_Envelope()};
					var handler = new IMAP_Client_FetchHandler();
					var envelops = new List<IMAP_Envelope>();

					handler.Envelope += (object sender, LumiSoft.Net.EventArgs<IMAP_Envelope> e) => envelops.Add(e.Value);

					imapClient.Fetch(false, allset, fetchDataItems, handler);

					imapClient.StoreMessageFlags(false, allset, IMAP_Flags_SetType.Replace, IMAP_MessageFlags.Deleted);

					imapClient.Expunge();

					SetCurrentUser(user.Login);
					var service = new PrgDataEx();
					var letterResponse = service.SendLetter("Test subject", "test body", null);
					Assert.That(letterResponse, Is.EqualTo("Res=OK").IgnoreCase, "����������� ����� ������� ��� �������� ������");

					envelops.Clear();

					imapClient.CloseFolder();
					imapClient.SelectFolder("INBOX");

					var fetchHandler = new IMAP_Client_FetchHandler();
					fetchHandler.Envelope += (object sender, LumiSoft.Net.EventArgs<IMAP_Envelope> e) => envelops.Add(e.Value);
					var dataItems = new IMAP_Fetch_DataItem[] { new IMAP_Fetch_DataItem_Envelope() };

					imapClient.Fetch(false, allset, dataItems, fetchHandler);

					Assert.That(envelops.Count, Is.EqualTo(1), "������ ������ ���� ����");

					var message = envelops[0];

					Assert.That(message.From, Is.Not.Null);
					Assert.That(message.From.Length, Is.EqualTo(1));
					Assert.That(message.From[0].GetType(), Is.EqualTo(typeof(Mail_t_Mailbox)));
					Assert.That(((Mail_t_Mailbox)message.From[0]).Address, Is.EqualTo("afmail@analit.net").IgnoreCase);

					Assert.That(message.To, Is.Not.Null);
					Assert.That(message.To.Length, Is.EqualTo(1));
					Assert.That(message.To[0].GetType(), Is.EqualTo(typeof(Mail_t_Mailbox)));
					Assert.That(((Mail_t_Mailbox)message.To[0]).Address, Is.EqualTo(ConfigurationManager.AppSettings["TechMail"]).IgnoreCase);

					Assert.That(message.Subject, Is.StringContaining(String.Format("UserId:{0}:", user.Id)).IgnoreCase);

					imapClient.StoreMessageFlags(false, allset, IMAP_Flags_SetType.Replace, IMAP_MessageFlags.Deleted);

					imapClient.Expunge();
				}
				finally
				{
					imapClient.Disconnect();
				}
			}
		}

		[Test]
		public void Check_send_letter_by_unknow_user()
		{
			try
			{
				var memoryAppender = new MemoryAppender();
				memoryAppender.AddFilter(new LoggerMatchFilter { AcceptOnMatch = true, LoggerToMatch = "PrgData", Next = new DenyAllFilter() });
				BasicConfigurator.Configure(memoryAppender);

				SetCurrentUser("dsdsdsdsds");
				var service = new PrgDataEx();
				var letterResponse = service.SendLetter("Test subject", "test body", null);
				Assert.That(letterResponse, Is.EqualTo("Error=�� ������� ��������� ������. ���������� �������.").IgnoreCase, "����������� ����� ������� ��� �������� ������");

				var events = memoryAppender.GetEvents();
				var lastEvent = events[events.Length - 1];
				Assert.That(lastEvent.Level, Is.EqualTo(Level.Error));
				Assert.That(lastEvent.ExceptionObject, Is.TypeOf(typeof(Exception)));
				Assert.That(((Exception)lastEvent.ExceptionObject).Message, Is.StringStarting("�� ������� ����� ������� ��� ��������� ������� ������:").IgnoreCase);
			}
			finally
			{
				LogManager.ResetConfiguration();
			}
		}


		private void SendLetterToGroup(byte emailGroup)
		{
			SetCurrentUser(user.Login);
			var service = new PrgDataEx();
			var letterResponse = service.SendLetterEx("Test subject to " + emailGroup, "test body to " + emailGroup, null, emailGroup);
			Assert.That(letterResponse, Is.EqualTo("Res=OK").IgnoreCase, "����������� ����� ������� ��� �������� ������");
		}

		[Test]
		public void CheckSendLetterToBilling()
		{
			SendLetterToGroup(1);
		}

		[Test]
		public void CheckSendLetterToOffice()
		{
			SendLetterToGroup(2);
		}

		[Test(Description = "���������� ������ ��� ������������ ������������")]
		public void SendLetterOnDisabledUser()
		{
			using (var transaction = new TransactionScope())
			{
				user.Enabled = false;
				user.Update();
			}
			SendLetterToGroup(0);
			SendLetterToGroup(1);
			SendLetterToGroup(2);
		}

		[Test(Description = "��� ����� � ������������ �����-������ ���������� �������� ������� �������")]
		public void CheckGetHistoryOrders()
		{
			SetCurrentUser(user.Login);
			var service = new PrgDataEx();
			var historyResponse = service.GetHistoryOrders("6.0.7.1183", UniqueId, new ulong[0], 1, 1);
			Assert.That(historyResponse, Is.StringContaining("Error=��� ����� � ������������ �����-������ ���������� �������� ������� �������.").IgnoreCase);
			Assert.That(historyResponse, Is.StringContaining("Desc=������ ������.").IgnoreCase);
		}

	}
}