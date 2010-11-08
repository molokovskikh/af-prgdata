using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Castle.ActiveRecord;
using Common.Tools;
using Inforoom.Common;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Filter;
using NUnit.Framework;
using PrgData;
using PrgData.Common;
using Test.Support;
using Test.Support.Documents;
using Test.Support.Logs;

namespace Integration
{
	[TestFixture]
	public class MultiUserDocumentFixture
	{
		uint lastUpdateId;
		string responce;
		TestDocumentLog document;
		TestDocumentLog fakeDocument;

		TestClient client;
		string waybills;

		[SetUp]
		public void Setup()
		{
			Test.Support.Setup.Initialize();

			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ServiceContext.GetResultPath = () => "results\\";
			using (new TransactionScope())
			{
				ConfigurationManager.AppSettings["WaybillPath"] = "FtpRoot\\";
				client = TestClient.CreateSimple();
				var user = client.Users[0];
				var permission = TestUserPermission.ByShortcut("AF");
				client.Users.Each(u => {
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				user.Update();

				if (Directory.Exists("FtpRoot"))
					FileHelper.DeleteDir("FtpRoot");

				Directory.CreateDirectory("FtpRoot");
				waybills = Path.Combine("FtpRoot", client.Addresses[0].Id.ToString(), "Waybills");
				Directory.CreateDirectory(waybills);

				document = CreateDocument(user);

				fakeDocument = CreateFakeDocument(user);

				SetCurrentUser(user.Login);
			}
		}

		private TestDocumentLog CreateDocument(TestUser user)
		{
			TestDocumentLog doc;
			using (var transaction = new TransactionScope(OnDispose.Rollback))
			{
				var supplierId = user.GetActivePrices()[0].Supplier.Id;
				doc = new TestDocumentLog {
					LogTime = DateTime.Now,
					FirmCode = supplierId,
					DocumentType = DocumentType.Waybill,
					ClientCode = client.Id,
					AddressId = client.Addresses[0].Id,
					FileName = "test.data",
					Ready = true
				};
				doc.Save();
				new TestDocumentSendLog {
					ForUser = user,
					Document = doc
				}.Save();
				transaction.VoteCommit();
			}
			File.WriteAllText(Path.Combine(waybills, String.Format("{0}_test.data", doc.Id)), "");
			var waybillsPath = Path.Combine("FtpRoot", client.Addresses[0].Id.ToString(), "Waybills");
			doc.LocalFile = Path.Combine(waybillsPath, String.Format("{0}_test.data", doc.Id));

			return doc;
		}

		private TestDocumentLog CreateFakeDocument(TestUser user)
		{
			TestDocumentLog doc;
			using (var transaction = new TransactionScope(OnDispose.Rollback))
			{
				var supplierId = user.GetActivePrices()[0].Supplier.Id;
				doc = new TestDocumentLog
				{
					LogTime = DateTime.Now,
					FirmCode = supplierId,
					DocumentType = DocumentType.Waybill,
					ClientCode = client.Id,
					AddressId = client.Addresses[0].Id,
					Ready = true,
					IsFake = true
				};
				doc.Save();
				new TestDocumentSendLog
				{
					ForUser = user,
					Document = doc
				}.Save();
				transaction.VoteCommit();
			}

			return doc;
		}

		[Test]
		public void Documents_should_be_sended_to_all_user_whom_address_avaliable()
		{
			CreateUser();

			LoadDocuments();
			ShouldBeSuccessfull();
			Confirm();

			var url = LoadDocuments();
			Assert.That(url, Is.StringContaining("Новых файлов документов нет"));

			SetCurrentUser(client.Users[1].Login);
			LoadDocuments();
			ShouldBeSuccessfull();
			Assert.That(File.Exists(document.LocalFile), Is.True, "удалил файл с накладной чего не нужно было делать");

			using (new SessionScope())
			{
				var logs = TestAnalitFUpdateLog.Queryable.Where(updateLog => (updateLog.UserId == client.Users[0].Id || updateLog.UserId == client.Users[1].Id) && updateLog.Addition.Contains("При подготовке документов в папке")).ToList();
				var finded = logs.FindAll(l => l.Addition.Contains(String.Format("№ {0}", fakeDocument.Id)));
				Assert.That(finded.Count, Is.EqualTo(0), "При подготовке данных попытались найти фиктивный документ, чтобы заархивировать его.");
				Assert.That(logs.Count, Is.EqualTo(0), "При архивировании не был найден документ: {0}", logs.Select(l => l.Addition).Implode("; "));
			}
		}

		[Test]
		public void Send_documents_if_update_was_not_confirmed()
		{
			LoadDocuments();
			ShouldBeSuccessfull();
			TestDocumentSendLog log;
			TestDocumentSendLog fakelog;
			using (new SessionScope())
			{
				log = TestDocumentSendLog.Queryable.First(t => t.Document == document);
				Assert.That(log.Committed, Is.False);
				fakelog = TestDocumentSendLog.Queryable.First(t => t.Document == fakeDocument);
				Assert.That(fakelog.Committed, Is.False);
			}

			LoadDocuments();
			ShouldBeSuccessfull();
			Confirm();
			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.True);
				fakelog.Refresh();
				Assert.That(fakelog.Committed, Is.True);

				var logs = TestAnalitFUpdateLog.Queryable.Where(updateLog => (updateLog.UserId == client.Users[0].Id) && updateLog.Addition.Contains("При подготовке документов в папке")).ToList();
				var finded = logs.FindAll(l => l.Addition.Contains(String.Format("№ {0}", fakeDocument.Id)));
				Assert.That(finded.Count, Is.EqualTo(0), "При подготовке данных попытались найти фиктивный документ, чтобы заархивировать его.");
				Assert.That(logs.Count, Is.EqualTo(0), "При архивировании не был найден документ: {0}", logs.Select(l => l.Addition).Implode("; "));
			}
		}

		[Test]
		public void Warn_only_once_if_document_file_not_exists()
		{
			var brokenDoc = CreateDocument(client.Users[0]);
			File.Delete(brokenDoc.LocalFile);

			LoadDocuments();
			ShouldBeSuccessfull();
			Confirm();

			var log = TestAnalitFUpdateLog.Find(lastUpdateId);
			Assert.That(log.Addition, Is.StringContaining("не найден документ"));

			var url = LoadDocuments();
			Assert.That(url, Is.StringContaining("Новых файлов документов нет"));

			using (new SessionScope())
			{
				var logs = TestAnalitFUpdateLog.Queryable.Where(updateLog => (updateLog.UserId == client.Users[0].Id) && updateLog.Addition.Contains("При подготовке документов в папке")).ToList();
				var finded = logs.FindAll(l => l.Addition.Contains(String.Format("№ {0}", fakeDocument.Id)));
				Assert.That(finded.Count, Is.EqualTo(0), "При подготовке данных попытались найти фиктивный документ, чтобы заархивировать его.");
			}
		}

		[Test]
		public void Confirm_update_if_new_document_not_found()
		{
			document.Delete();
			fakeDocument.Delete();

			var responce = LoadDocuments();
			Assert.That(responce, Is.StringContaining("Новых файлов документов нет"));

			using (new SessionScope())
			{
				var maxId = TestAnalitFUpdateLog.Queryable.Max(l => l.Id);
				var log = TestAnalitFUpdateLog.Queryable.Single(l => l.Id == maxId);
				Assert.That(log.Commit, Is.True);
			}
		}

		[Test]
		public void After_document_send_only_sender_receive_it()
		{
			CreateUser();
			document.Delete();
			fakeDocument.Delete();

			SendWaybill();

			LoadDocuments();
			ShouldBeSuccessfull();
			Confirm();

			SetCurrentUser(client.Users[1].Login);
			LoadDocuments();
			ShouldNotBeDocuments();
		}

		[Test]
		public void Check_empty_UIN_on_send()
		{

			var response = SendWaybillEx(null);
			Assert.That(response, Is.StringStarting("Status=0"));
			Console.WriteLine(response);

			using (new SessionScope())
			{
				var maxId = TestAnalitFUpdateLog.Queryable.Where(l => l.UserId == client.Users[0].Id).Max(l => l.Id);
				var log = TestAnalitFUpdateLog.Queryable.Single(l => l.Id == maxId);
				Assert.That(log.Commit, Is.True);
				Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.SendWaybills)), "Не совпадает UpdateType");
				var info = TestUserUpdateInfo.Find(client.Users[0].Id);
				Assert.IsNullOrEmpty(info.AFCopyId, "AFCopyId не корректен");
			}
		}

		[Test]
		public void Check_UIN_on_send()
		{
			var uin = "12345678";

			var response = SendWaybillEx(uin);
			Assert.That(response, Is.StringStarting("Status=0"));

			using (new SessionScope())
			{
				var maxId = TestAnalitFUpdateLog.Queryable.Where(l => l.UserId == client.Users[0].Id).Max(l => l.Id);
				var log = TestAnalitFUpdateLog.Queryable.Single(l => l.Id == maxId);
				Assert.That(log.Commit, Is.True);
				Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.SendWaybills)), "Не совпадает UpdateType");
				var info = TestUserUpdateInfo.Find(client.Users[0].Id);
				Assert.That(info.AFCopyId, Is.EqualTo(uin), "Не совпадает AFCopyId");
			}
		}

		[Test]
		public void Check_dictinct_UIN_on_send()
		{
			var uin = "12345678";

			using (var transaction = new TransactionScope(OnDispose.Rollback))
			{
				var info = TestUserUpdateInfo.Find(client.Users[0].Id);
				info.AFCopyId = "87654321";
				info.Save();
				transaction.VoteCommit();
			}

			var response = SendWaybillEx(uin);
			Assert.That(response, Is.StringStarting("Status=1"));

			using (new SessionScope())
			{
				var maxId = TestAnalitFUpdateLog.Queryable.Where(l => l.UserId == client.Users[0].Id).Max(l => l.Id);
				var log = TestAnalitFUpdateLog.Queryable.Single(l => l.Id == maxId);
				Assert.That(log.Commit, Is.False);
				Assert.That(log.Addition, Is.StringContaining("Несоответствие UIN").IgnoreCase);
				Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.Forbidden)), "Не совпадает UpdateType");
				var info = TestUserUpdateInfo.Find(client.Users[0].Id);
				Assert.That(info.AFCopyId, Is.Not.EqualTo(uin), "Совпадает AFCopyId");
			}
		}

		[Test(Description = "Написал тест для проверки выгрузки документа с признаком IsFake, чтобы проверить, что он корректно подтверждается")]
		public void Check_Fake_for_old_user()
		{
			TestDocumentLog fakeDoc;
			TestOldClient _oldClient;
			TestOldUser _oldUser;
			using (var transaction = new TransactionScope(OnDispose.Rollback))
			{
				var permission = TestUserPermission.ByShortcut("AF");

				_oldClient = TestOldClient.CreateTestClient();
				_oldUser = _oldClient.Users[0];

				var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof(ActiveRecordBase));
				try
				{
					session.CreateSQLQuery(@"
				insert into usersettings.AssignedPermissions (PermissionId, UserId) values (:permissionid, :userid)")
						.SetParameter("permissionid", permission.Id)
						.SetParameter("userid", _oldUser.Id)
						.ExecuteUpdate();
				}
				finally
				{
					ActiveRecordMediator.GetSessionFactoryHolder().ReleaseSession(session);
				}

				var supplierId = _oldClient.GetActivePrices()[0].Supplier.Id;
				fakeDoc = new TestDocumentLog
				{
					LogTime = DateTime.Now,
					FirmCode = supplierId,
					DocumentType = DocumentType.Waybill,
					ClientCode = _oldClient.Id,
					Ready = true,
					IsFake = true
				};
				fakeDoc.Save();

				transaction.VoteCommit();
			}

			var waybillsFolder = Path.Combine("FtpRoot", _oldClient.Id.ToString(), "Waybills");
			Directory.CreateDirectory(waybillsFolder);

			SetCurrentUser(_oldUser.OSUserName);
			LoadDocuments();
			ShouldBeSuccessfull();
			Confirm();

			//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
			Thread.Sleep(2000);

			using (new SessionScope())
			{
				var logs = TestAnalitFUpdateLog.Queryable.Where(updateLog => (updateLog.UserId == _oldUser.Id) && updateLog.Addition.Contains("При подготовке документов в папке")).ToList();
				var finded = logs.FindAll(l => l.Addition.Contains(String.Format("№ {0}", fakeDocument.Id)));
				Assert.That(finded.Count, Is.EqualTo(0), "При подготовке данных попытались найти фиктивный документ, чтобы заархивировать его.");
				Assert.That(logs.Count, Is.EqualTo(0), "При архивировании не был найден документ: {0}", logs.Select(l => l.Addition).Implode("; "));

				var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof(ActiveRecordBase));
				try
				{
					var commited = session.CreateSQLQuery(@"select Committed from usersettings.AnalitFDocumentsProcessing where DocumentId = :DocumentId and UpdateId = :UpdateId")
						.SetParameter("DocumentId", fakeDoc.Id)
						.SetParameter("UpdateId", lastUpdateId)
						.UniqueResult();
					Assert.That(commited, Is.Null, "Ненастоящий документ не подтвержден.");
				}
				finally
				{
					ActiveRecordMediator.GetSessionFactoryHolder().ReleaseSession(session);
				}

			}
		}

		[Test(Description = "проверяем получение разобранного ненастоящего документа клиентом")]
		public void Get_parsed_fake_docs()
		{
			ArchiveHelper.SevenZipExePath = @".\7zip\7z.exe";

			TestWaybill waybill;
			using (var transaction = new TransactionScope(OnDispose.Rollback))
			{
				waybill = new TestWaybill
							{
								DocumentType = DocumentType.Waybill,
								DownloadId = fakeDocument.Id,
								ClientCode = client.Id,
								FirmCode = fakeDocument.FirmCode.Value,
								WriteTime = DateTime.Now
							};
				waybill.Lines = new List<TestWaybillLine> { new TestWaybillLine { Waybill = waybill } };
				waybill.Save();
				waybill.Lines[0].Save();
				transaction.VoteCommit();
			}

			LoadDocuments();
			ShouldBeSuccessfull();

			var resultFileName = ServiceContext.GetResultPath() + client.Users[0].Id + ".zip";
			Assert.That(File.Exists(resultFileName), Is.True, "Не найден файл с подготовленными данными");

			var extractFolder = "ResultExtract";
			if (Directory.Exists(extractFolder))
				FileHelper.DeleteDir(extractFolder);
			Directory.CreateDirectory(extractFolder);

			ArchiveHelper.Extract(resultFileName, "*.*", extractFolder);
			var files = Directory.GetFiles(extractFolder, "Document*" + client.Users[0].Id + ".txt");
			Assert.That(files.Length, Is.GreaterThanOrEqualTo(2), "Не все файлы найдены в архиве: {0}", files.Implode());
			var documentHeadersFile = files.First(item => item.Contains("DocumentHeaders"));
			Assert.IsNotNullOrEmpty(documentHeadersFile, "Не найден файл DocumentHeaders: {0}", files.Implode());
			Assert.IsNotNullOrEmpty(files.First(item => item.Contains("DocumentBodies")), "Не найден файл DocumentBodies: {0}", files.Implode());

			var contentHeader = File.ReadAllText(documentHeadersFile);
			Assert.That(contentHeader, Is.StringStarting(String.Format("{0}\t{1}", waybill.Id, fakeDocument.Id)), "В содержимом DocumentHeaders нет искомого разобранного документа");

			Confirm();
		}

		[Test(Description = "Должен быть сброшен флаг Commited при частичном КО")]
		public void ResetCommittedAfterLimitedCumulative()
		{
			var updateTime = DateTime.Now.AddMinutes(-1);

			TestDocumentSendLog log;
			TestDocumentSendLog fakelog;
			using (new SessionScope())
			{
				log = TestDocumentSendLog.Queryable.First(t => t.Document == document);
				Assert.That(log.Committed, Is.False);
				fakelog = TestDocumentSendLog.Queryable.First(t => t.Document == fakeDocument);
				Assert.That(fakelog.Committed, Is.False);
			}

			GetUserData(updateTime);
			ShouldBeSuccessfull();
			ConfirmData();

			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.True);
				fakelog.Refresh();
				Assert.That(fakelog.Committed, Is.True);
			}

			GetUserData(updateTime);
			ShouldBeSuccessfull();

			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.False);
				fakelog.Refresh();
				Assert.That(fakelog.Committed, Is.False);
			}

			ConfirmData();

			//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
			Thread.Sleep(3000);

			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.True);
				fakelog.Refresh();
				Assert.That(fakelog.Committed, Is.True);
			}
		}

		private void ShouldNotBeDocuments()
		{
			Assert.That(responce, Is.StringContaining("Новых файлов документов нет"));
		}

		private void SendWaybill()
		{
			var service = new PrgDataEx();
			uint supplierId;
			using (new TransactionScope())
			{
				supplierId = client.Users[0].GetActivePrices()[0].Supplier.Id;
			}
			
			service.SendWaybills(client.Addresses[0].Id,
				new ulong[] {supplierId},
				new [] {"3687747_Протек-21_3687688_Протек-21_8993929-001__.sst"},
				File.ReadAllBytes(@"..\..\Data\3687747_Протек-21_3687688_Протек-21_8993929-001__.zip"));
		}

		private string SendWaybillEx(string uin)
		{
			var service = new PrgDataEx();
			uint supplierId;
			using (new TransactionScope())
			{
				supplierId = client.Users[0].GetActivePrices()[0].Supplier.Id;
			}

			return service.SendWaybillsEx(client.Addresses[0].Id,
				new ulong[] { supplierId },
				new[] { "3687747_Протек-21_3687688_Протек-21_8993929-001__.sst" },
				File.ReadAllBytes(@"..\..\Data\3687747_Протек-21_3687688_Протек-21_8993929-001__.zip"),
				uin,
				"6.0.0.1183");
		}

		private void CreateUser()
		{
			using (new TransactionScope())
			{
				var user = client.CreateUser();
				var permission = TestUserPermission.ByShortcut("AF");
				user.SendWaybills = true;
				user.SendRejects = true;
				user.AssignedPermissions.Add(permission);
				client.Addresses[0].AvaliableForUsers.Add(user);
				client.Update();

				new TestDocumentSendLog {
					ForUser = user,
					Document = document
				}.Save();
				new TestDocumentSendLog
				{
					ForUser = user,
					Document = fakeDocument
				}.Save();
			}
		}

		private void ShouldBeSuccessfull()
		{
			Assert.That(responce, Is.StringStarting("URL=http://localhost//GetFileHandler.ashx?Id"));
		}

		private void Confirm()
		{
			var service = new PrgDataEx();
			service.MaxSynonymCode("", new uint[0], lastUpdateId, true);
		}

		private string LoadDocuments()
		{
			var service = new PrgDataEx();
			responce = service.GetUserData(DateTime.Now, false, "1065", 50, "123", "", "", true);

			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
				lastUpdateId = Convert.ToUInt32(match);
			return responce;
		}

		private string GetUserData(DateTime updateTime)
		{
			var service = new PrgDataEx();
			responce = service.GetUserData(updateTime, false, "5.3.16.1101", 50, "123", "", "", false);

			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
				lastUpdateId = Convert.ToUInt32(match);
			return responce;
		}

		private void ConfirmData()
		{
			var service = new PrgDataEx();
			service.CommitExchange(lastUpdateId, false);
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}
	}
}