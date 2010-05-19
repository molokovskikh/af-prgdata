using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using Castle.ActiveRecord;
using Common.Tools;
using Common.Tools.Calendar;
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

		[SetUp]
		public void Setup()
		{
			Test.Support.Setup.Initialize();
			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			using (new TransactionScope())
			{
				client = TestClient.CreateSimple();
				var user = client.Users[0];
				var supplierId = user.GetActivePrices()[0].FirmCode;
				var permission = TestUserPermission.ByShortcut("AF");
				client.Users.Each(u => {
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				user.Update();

				document = new TestDocumentLog {
					LogTime = DateTime.Now,
					FirmCode = supplierId,
					DocumentType = DocumentType.Waybill,
					ClientCode = client.Id,
					AddressId = client.Addresses[0].Id,
					FileName = "test.data",
				};
				document.Save();
				new TestDocumentSendLog {
					ForUser = user,
					Document = document
				}.Save();

				if (Directory.Exists("FtpRoot"))
					FileHelper.DeleteDir("FtpRoot");
				Directory.CreateDirectory("FtpRoot");
				var waybills = Path.Combine("FtpRoot", client.Addresses[0].Id.ToString(), "Waybills");
				Directory.CreateDirectory(waybills);
				File.WriteAllText(Path.Combine(waybills, String.Format("{0}_test.data", document.Id)), "");
				var waybillsPath = Path.Combine("FtpRoot", client.Addresses[0].Id.ToString(), "Waybills");
				document.LocalFile = Path.Combine(waybillsPath, String.Format("{0}_test.data", document.Id));
				SetCurrentUser(user.Login);
			}
		}

		[Test]
		public void Documents_should_be_sended_to_all_user_whom_address_avaliable()
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

			Update();
			ShouldBeSuccessfull();
			Confirm();

			var url = Update();
			Assert.That(url, Is.StringContaining("Новых файлов документов нет"));

			SetCurrentUser(client.Users[1].Login);
			Update();
			ShouldBeSuccessfull();
			Assert.That(File.Exists(document.LocalFile), Is.True, "удалил файл с накладной чего не нужно было делать");
		}

		[Test]
		public void Send_documents_if_update_was_not_confirmed()
		{
			Update();
			ShouldBeSuccessfull();

			Update();
			ShouldBeSuccessfull();
			Confirm();
		}

		[Test]
		public void Warn_only_once_if_document_file_not_exists()
		{
			File.Delete(document.LocalFile);

			Update();
			ShouldBeSuccessfull();
			Confirm();

			var log = TestAnalitFUpdateLog.Find(lastUpdateId);
			Assert.That(log.Addition, Is.StringContaining("Не найден документ"));

			Update();
			ShouldBeSuccessfull();
			Confirm();

			Assert.That(log.Id, Is.Not.EqualTo(lastUpdateId));
			Assert.That(log.Addition, Is.Not.StringContaining("Не найден документ"));
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

		private string Update()
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