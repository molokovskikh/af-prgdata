using System;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Castle.ActiveRecord;
using Common.Tools;
using Inforoom.Common;
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

		TestClient client;
		string waybills;

		[SetUp]
		public void Setup()
		{
			Test.Support.Setup.Initialize();
			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
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

				SetCurrentUser(user.Login);
			}
		}

		private TestDocumentLog CreateDocument(TestUser user)
		{
			TestDocumentLog doc;
			using (var transaction = new TransactionScope(OnDispose.Rollback))
			{
				var supplierId = user.GetActivePrices()[0].FirmCode;
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
		}

		[Test]
		public void Send_documents_if_update_was_not_confirmed()
		{
			LoadDocuments();
			ShouldBeSuccessfull();
			TestDocumentSendLog log;
			using(new SessionScope())
			{
				log = TestDocumentSendLog.Queryable.First(t => t.Document == document);
				Assert.That(log.Committed, Is.False);
			}

			LoadDocuments();
			ShouldBeSuccessfull();
			Confirm();
			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.True);
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
		}

		[Test]
		public void Confirm_update_if_new_document_not_found()
		{
			document.Delete();

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

		private void ShouldNotBeDocuments()
		{
			Assert.That(responce, Is.StringContaining("Новых файлов документов нет"));
		}

		private void SendWaybill()
		{
			var service = new PrgDataEx();
			service.ResultFileName = "results";
			uint supplierId;
			using (new TransactionScope())
			{
				supplierId = client.Users[0].GetActivePrices()[0].FirmCode;
			}
			
			service.SendWaybills(client.Addresses[0].Id,
				new ulong[] {supplierId},
				new [] {"3687747_Протек-21_3687688_Протек-21_8993929-001__.sst"},
				File.ReadAllBytes(@"..\..\Data\3687747_Протек-21_3687688_Протек-21_8993929-001__.zip"));
		}

		private string SendWaybillEx(string uin)
		{
			var service = new PrgDataEx();
			service.ResultFileName = "results";
			uint supplierId;
			using (new TransactionScope())
			{
				supplierId = client.Users[0].GetActivePrices()[0].FirmCode;
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
			}
		}

		private void ShouldBeSuccessfull()
		{
			Assert.That(responce, Is.StringStarting("URL=http://localhost//GetFileHandler.ashx?Id"));
		}

		private void Confirm()
		{
			var service = new PrgDataEx();
			service.ResultFileName = "results";
			service.MaxSynonymCode("", new uint[0], lastUpdateId, true);
		}

		private string LoadDocuments()
		{
			var service = new PrgDataEx();
			service.ResultFileName = "results";
			responce = service.GetUserData(DateTime.Now, false, "1065", 50, "123", "", "", true);

			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
				lastUpdateId = Convert.ToUInt32(match);
			return responce;
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}
	}
}