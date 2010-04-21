using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using Castle.ActiveRecord;
using Common.Tools;
using NUnit.Framework;
using PrgData;
using PrgData.Common;
using Test.Support;

namespace Integration
{
	[TestFixture]
	public class MultiUserDocumentFixture
	{
		[SetUp]
		public void Setup()
		{
			Test.Support.Setup.Initialize();
			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
		}

		[Test]
		public void Documents_should_be_sended_to_all_user_whom_address_avaliable()
		{
			TestClient client;
			TestDocumentLog document;
			using (new TransactionScope())
			{
				client = TestClient.CreateSimple();
				var user = client.CreateUser();
				var permission = TestUserPermission.ByShortcut("AF");
				client.Users.Each(u => {
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				client.Addresses[0].AvaliableForUsers.Add(user);
				client.Update();

				var supplierId = client.Users[0].GetActivePrices()[0].FirmCode;
				document = new TestDocumentLog {
					LogTime = DateTime.Now,
					FirmCode = supplierId,
					DocumentType = DocumentType.Waybill,
					ClientCode = client.Id,
					AddressId = client.Addresses[0].Id,
					FileName = "test.data",
				};
				document.Save();

				if (Directory.Exists("FtpRoot"))
					Directory.Delete("FtpRoot", true);
				Directory.CreateDirectory("FtpRoot");
				var waybills = Path.Combine("FtpRoot", client.Addresses[0].Id.ToString(), "Waybills");
				Directory.CreateDirectory(waybills);
				File.WriteAllText(Path.Combine(waybills, String.Format("{0}_test.data", document.Id)), "");
			}

			SetCurrentUser(client.Users[0].Login);
			var url = Update();
			Assert.That(url, Is.StringStarting("URL=http://localhost//GetFileHandler.ashx?Id"));
			Confirm(url);

			url = Update();
			Assert.That(url, Is.StringContaining("Новых файлов документов нет"));

			SetCurrentUser(client.Users[1].Login);
			url = Update();
			Assert.That(url, Is.StringContaining("URL=http://localhost//GetFileHandler.ashx?Id"));
			var waybillsPath = Path.Combine("FtpRoot", client.Addresses[0].Id.ToString(), "Waybills");
			Assert.That(File.Exists(Path.Combine(waybillsPath, String.Format("{0}_test.data", document.Id))), Is.True, "удалил файл с накладной чего не нужно было делать");
		}

		private void Confirm(string url)
		{
			var updateId = Convert.ToUInt32(Regex.Match(url, @"\d+").Value);
			var service = new PrgDataEx();
			service.ResultFileName = "results";
			service.MaxSynonymCode("", new uint[0], updateId, true);
		}

		private string Update()
		{
			var service = new PrgDataEx();
			service.ResultFileName = "results";
			return service.GetUserData(DateTime.Now, false, "1065", 50, "123", "", "", true);
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}
	}
}