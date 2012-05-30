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
using MySql.Data.MySqlClient;
using Test.Support.Suppliers;
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
			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ServiceContext.GetResultPath = () => "results\\";
			ConfigurationManager.AppSettings["DocumentsPath"] = "FtpRoot\\";

			client = TestClient.Create();

			using (new TransactionScope())
			{
				var user = client.Users[0];
				client.Users.Each(u => {
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
				var supplier = user.GetActivePrices()[0].Supplier;
				doc = new TestDocumentLog {
					LogTime = DateTime.Now,
					Supplier = supplier,
					DocumentType = DocumentType.Waybill,
					Client = client,
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
				var supplier = user.GetActivePrices()[0].Supplier;
				doc = new TestDocumentLog {
					LogTime = DateTime.Now,
					Supplier = supplier,
					DocumentType = DocumentType.Waybill,
					Client = client,
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

		private void CheckDelivered(TestDocumentSendLog docLog, bool fileDelivered, bool documentDelivered)
		{
			Assert.That(docLog.FileDelivered, Is.EqualTo(fileDelivered), "Некорректно установлено свойство 'Файл доставлен' для лога {0}", docLog.Id);
			Assert.That(docLog.DocumentDelivered, Is.EqualTo(documentDelivered), "Некорректно установлено свойство 'Документ доставлен' для лога {0}", docLog.Id);
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
				CheckDelivered(log, true, false);

				fakelog.Refresh();
				Assert.That(fakelog.Committed, Is.True);
				CheckDelivered(fakelog, false, false);

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

			using (new TransactionScope())
			{
				//Документ должен быть старше часа, чтобы сформировалось уведомление
				brokenDoc.LogTime = DateTime.Now.AddHours(-1).AddMinutes(-1);
				brokenDoc.Update();
			}

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

				var brokenDocLog = TestDocumentSendLog.Queryable.First(t => t.Document == brokenDoc);
				CheckDelivered(brokenDocLog, false, false);
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
				var log = LastLog(client.Users[0]);
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
				var log = LastLog(client.Users[0]);
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
				var log = LastLog(client.Users[0]);
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
				var log = LastLog(client.Users[0]);
				Assert.That(log.Commit, Is.False);
				Assert.That(log.Addition, Is.StringContaining("Несоответствие UIN").IgnoreCase);
				Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.Forbidden)), "Не совпадает UpdateType");
				var info = TestUserUpdateInfo.Find(client.Users[0].Id);
				Assert.That(info.AFCopyId, Is.Not.EqualTo(uin), "Совпадает AFCopyId");
			}
		}

		[Test(Description = "проверяем получение разобранного ненастоящего документа клиентом")]
		public void Get_parsed_fake_docs()
		{
			ArchiveHelper.SevenZipExePath = @".\7zip\7z.exe";

			TestWaybill waybill;
			using (var transaction = new TransactionScope(OnDispose.Rollback))
			{
				waybill = new TestWaybill(fakeDocument);
				waybill.Lines = new List<TestWaybillLine> { new TestWaybillLine { Waybill = waybill } };
				waybill.Save();
				waybill.Lines[0].Save();
				transaction.VoteCommit();
			}

			LoadDocuments();
			ShouldBeSuccessfull();

			var resultFileName = ServiceContext.GetResultPath() + client.Users[0].Id + "_" + lastUpdateId +".zip";
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

			using (new SessionScope())
			{
				var fakelog = TestDocumentSendLog.Queryable.First(t => t.Document == fakeDocument);
				Assert.That(fakelog.Committed, Is.True);
				CheckDelivered(fakelog, false, true);
			}
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

			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.True);
				CheckDelivered(log, true, false);

				fakelog.Refresh();
				Assert.That(fakelog.Committed, Is.True);
				CheckDelivered(fakelog, false, false);
			}
		}

		[Test(Description = "Документы, отправленные пользователем для разбора, не возвращаются в архиве")]
		public void SendedWaybillsNotReturned()
		{
			CreateUser();
			document.Delete();
			fakeDocument.Delete();

			SendWaybill();

			LoadDocuments();
			ShouldBeSuccessfull();

			var resultFileName = ServiceContext.GetResultPath() + client.Users[0].Id + "_" + lastUpdateId + ".zip";
			Assert.That(File.Exists(resultFileName), Is.True, "Не найден файл с подготовленными данными");

			var extractFolder = "ResultExtract";
			if (Directory.Exists(extractFolder))
				FileHelper.DeleteDir(extractFolder);
			Directory.CreateDirectory(extractFolder);

			ArchiveHelper.Extract(resultFileName, "*.*", extractFolder);
			var files = Directory.GetFiles(extractFolder);

			Assert.IsNotNullOrEmpty(files.First(item => item.Contains("DocumentHeaders")), "Не найден файл DocumentHeaders: {0}", files.Implode());
			Assert.IsNotNullOrEmpty(files.First(item => item.Contains("DocumentBodies")), "Не найден файл DocumentBodies: {0}", files.Implode());

			Assert.That(files.Length, Is.EqualTo(2), "В полученном архиве переданы дополнительные файлы в корневую папку: {0}", files.Implode());

			if (Directory.Exists(Path.Combine(extractFolder, "Waybills")))
			{
				var waybillsFiles = Directory.GetFiles(Path.Combine(extractFolder, "Waybills"));
				Assert.That(waybillsFiles.Length, Is.EqualTo(0), "В папке с накладными имеются файлы, хотя там не должно быть файлов, которые пользователь отправил для разбора: {0}", waybillsFiles.Implode());
			}

			Confirm();
		}

		[Test(Description = "Не показываем сообщение о ненайденном файле документа, если документ моложе часа")]
		public void DontShowErrorOnNotFindFile()
		{
			var brokenDoc = CreateDocument(client.Users[0]);
			if (File.Exists(brokenDoc.LocalFile))
				File.Delete(brokenDoc.LocalFile);

			LoadDocuments();
			ShouldBeSuccessfull();
			Confirm();

			using (new SessionScope())
			{
				var documentSendLog = TestDocumentSendLog.Queryable.First(t => t.Document == document);
				Assert.That(documentSendLog.Committed, Is.True);
				var brokenDocumentSendLog = TestDocumentSendLog.Queryable.First(t => t.Document == brokenDoc);
				Assert.That(brokenDocumentSendLog.Committed, Is.False);
			}

			var log = TestAnalitFUpdateLog.Find(lastUpdateId);
			Assert.That(log.Addition, Is.Not.StringContaining("не найден документ № ".Format(brokenDoc.Id)));

			File.WriteAllText(brokenDoc.LocalFile, "");

			LoadDocuments();
			ShouldBeSuccessfull();
			Confirm();

			using (new SessionScope())
			{
				var brokenDocumentSendLog = TestDocumentSendLog.Queryable.First(t => t.Document == brokenDoc);
				Assert.That(brokenDocumentSendLog.Committed, Is.True);
				CheckDelivered(brokenDocumentSendLog, true, false);
			}

			var logAfterCreateFile = TestAnalitFUpdateLog.Find(lastUpdateId);
			Assert.That(logAfterCreateFile.Addition, Is.Not.StringContaining("не найден документ № ".Format(brokenDoc.Id)));
		}

		[Test(Description = "Показываем сообщение о ненайденном файле документа, если документ старше часа")]
		public void ShowErrorOnNotFindFileAfterHour()
		{
			var brokenDoc = CreateDocument(client.Users[0]);
			if (File.Exists(brokenDoc.LocalFile))
				File.Delete(brokenDoc.LocalFile);

			LoadDocuments();
			ShouldBeSuccessfull();
			Confirm();

			using (new SessionScope())
			{
				var documentSendLog = TestDocumentSendLog.Queryable.First(t => t.Document == document);
				Assert.That(documentSendLog.Committed, Is.True);
				var brokenDocumentSendLog = TestDocumentSendLog.Queryable.First(t => t.Document == brokenDoc);
				Assert.That(brokenDocumentSendLog.Committed, Is.False);
			}

			var log = TestAnalitFUpdateLog.Find(lastUpdateId);
			Assert.That(log.Addition, Is.Not.StringContaining("не найден документ № ".Format(brokenDoc.Id)));

			using (new TransactionScope())
			{
				brokenDoc.LogTime = DateTime.Now.AddHours(-1).AddMinutes(-1);
				brokenDoc.Update();
			}

			LoadDocuments();
			ShouldBeSuccessfull();
			Confirm();

			using (new SessionScope())
			{
				var brokenDocumentSendLog = TestDocumentSendLog.Queryable.First(t => t.Document == brokenDoc);
				Assert.That(brokenDocumentSendLog.Committed, Is.True);
				CheckDelivered(brokenDocumentSendLog, false, false);
			}

			var logAfterCreateFile = TestAnalitFUpdateLog.Find(lastUpdateId);
			Assert.That(logAfterCreateFile.Addition, Is.StringContaining("не найден документ № ".Format(brokenDoc.Id)));
		}

		[Test(Description = "Счет-фактура должна выгружаться после опеределенной версии")]
		public void InvoiceHeadersNotExported()
		{
			LoadDocuments();
			ShouldBeSuccessfull();

			var resultFileName = ServiceContext.GetResultPath() + client.Users[0].Id + "_" + lastUpdateId + ".zip";
			Assert.That(File.Exists(resultFileName), Is.True, "Не найден файл с подготовленными данными");

			var extractFolder = "ResultExtract";
			if (Directory.Exists(extractFolder))
				FileHelper.DeleteDir(extractFolder);
			Directory.CreateDirectory(extractFolder);

			ArchiveHelper.Extract(resultFileName, "*.*", extractFolder);
			var files = Directory.GetFiles(extractFolder);

			Assert.IsNotNullOrEmpty(files.First(item => item.Contains("DocumentHeaders")), "Не найден файл DocumentHeaders: {0}", files.Implode());
			Assert.IsNotNullOrEmpty(files.First(item => item.Contains("DocumentBodies")), "Не найден файл DocumentBodies: {0}", files.Implode());
			Assert.That(files.Any(item => item.Contains("InvoiceHeaders")), Is.False, "Найден файл InvoiceHeaders: {0}", files.Implode());

			Assert.That(files.Length, Is.EqualTo(2), "В полученном архиве переданы дополнительные файлы в корневую папку: {0}", files.Implode());

			Confirm();
		}

		[Test(Description = "Счет-фактура должна выгружаться после опеределенной версии")]
		public void ExportInvoiceHeadersNotExported()
		{
			LoadDocuments("1.1.1.1462");
			ShouldBeSuccessfull();

			var resultFileName = ServiceContext.GetResultPath() + client.Users[0].Id + "_" + lastUpdateId + ".zip";
			Assert.That(File.Exists(resultFileName), Is.True, "Не найден файл с подготовленными данными");

			var extractFolder = "ResultExtract";
			if (Directory.Exists(extractFolder))
				FileHelper.DeleteDir(extractFolder);
			Directory.CreateDirectory(extractFolder);

			ArchiveHelper.Extract(resultFileName, "*.*", extractFolder);
			var files = Directory.GetFiles(extractFolder);

			Assert.IsNotNullOrEmpty(files.First(item => item.Contains("DocumentHeaders")), "Не найден файл DocumentHeaders: {0}", files.Implode());
			Assert.IsNotNullOrEmpty(files.First(item => item.Contains("DocumentBodies")), "Не найден файл DocumentBodies: {0}", files.Implode());
			Assert.That(files.Any(item => item.Contains("InvoiceHeaders")), Is.True, "Не найден файл InvoiceHeaders: {0}", files.Implode());

			Assert.That(files.Length, Is.EqualTo(3), "В полученном архиве переданы дополнительные файлы в корневую папку: {0}", files.Implode());

			Confirm();
		}

		[Test(Description = "После разбора накладных не должно быть новых файлов в папке с временными файлами")]
		public void EmptyTempFoldersAfterWork()
		{
			FoldersHelper.CheckTempFolders(() => {
				var response = SendWaybillEx("12345678");
				Assert.That(response, Is.StringStarting("Status=0"));
			});
		}

		[Test(Description = "После разбора накладных не должно быть новых файлов в папке с временными файлами даже если возникла ошибка")]
		public void EmptyTempFoldersAfterErrorWork()
		{
			FoldersHelper.CheckTempFolders(() =>
			{
				var service = new PrgDataEx();
				uint supplierId;
				using (new TransactionScope())
				{
					supplierId = client.Users[0].GetActivePrices()[0].Supplier.Id;
				}

				var response = service.SendWaybillsEx(client.Addresses[0].Id,
					new ulong[] { supplierId },
					new[] { "3687747_Протек-21_3687688_Протек-21_8993929-001__.sst" },
					new byte[]{}, 
					"12345678",
					"6.0.0.1183");
				
				Assert.That(response, Is.StringStarting("Status=1"));
			});
		}

		[Test(Description = "Должен быть сброшен флаг Commited при явном запросе КО")]
		public void ResetCommittedAfterCumulative()
		{
			var updateTime = DateTime.Now.AddMinutes(-1);

			TestDocumentSendLog log;
			TestDocumentSendLog fakelog;

			//Т.к. пользователь еще не обновлялся, то документы не должны быть подтверждены
			using (new SessionScope())
			{
				log = TestDocumentSendLog.Queryable.First(t => t.Document == document);
				Assert.That(log.Committed, Is.False);
				fakelog = TestDocumentSendLog.Queryable.First(t => t.Document == fakeDocument);
				Assert.That(fakelog.Committed, Is.False);
			}

			//Делаем первое КО
			GetUserData(updateTime, true);
			ShouldBeSuccessfull();
			ConfirmData();

			//Новые документы после первого КО должны быть подтверждены
			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.True);
				CheckDelivered(log, true, false);

				fakelog.Refresh();
				Assert.That(fakelog.Committed, Is.True);
				CheckDelivered(fakelog, false, false);
			}

			//Запрашиваем КО еще раз
			GetUserData(updateTime, true);
			ShouldBeSuccessfull();

			//Для документов, отданых в прошлое обновление, должен быть сброшен статус доставки
			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.False);
				fakelog.Refresh();
				Assert.That(fakelog.Committed, Is.False);
			}

			ConfirmData();

			//После подтверждения КО для этих же документов статус доставки должен быть подтвержден
			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.True);
				CheckDelivered(log, true, false);

				fakelog.Refresh();
				Assert.That(fakelog.Committed, Is.True);
				CheckDelivered(fakelog, false, false);
			}
		}

		[Test(Description = "Должен быть сброшен флаг Commited при явном запросе КО для даты обновления старше чем 1 месяц")]
		public void ResetCommittedAfterCumulativeWithOldDate()
		{
			var user = client.Users[0];

			var updateTime = DateTime.Now.AddMinutes(-1);

			TestDocumentSendLog log;
			TestDocumentSendLog fakelog;

			//Т.к. пользователь еще не обновлялся, то документы не должны быть подтверждены
			using (new SessionScope())
			{
				log = TestDocumentSendLog.Queryable.First(t => t.Document == document);
				Assert.That(log.Committed, Is.False);
				fakelog = TestDocumentSendLog.Queryable.First(t => t.Document == fakeDocument);
				Assert.That(fakelog.Committed, Is.False);
			}

			//Делаем первое КО
			GetUserData(updateTime, true);
			ShouldBeSuccessfull();

			//Проверяем статус обновления для первого КО
			TestAnalitFUpdateLog oldUpdate;
			using (new SessionScope()) {
				oldUpdate = TestAnalitFUpdateLog.Find(lastUpdateId);
				Assert.That(oldUpdate.UserId, Is.EqualTo(user.Id));
				Assert.That(oldUpdate.Commit, Is.False);
			}

			ConfirmData();

			//Новые документы после первого КО должны быть подтверждены
			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.True);
				CheckDelivered(log, true, false);

				fakelog.Refresh();
				Assert.That(fakelog.Committed, Is.True);
				CheckDelivered(fakelog, false, false);
			}

			//Создаем новый документ, который будет получен при следующем запросе документов
			var newDocument = CreateDocument(user);
			TestDocumentSendLog newDocumentLog;
			using (new SessionScope())
			{
				newDocumentLog = TestDocumentSendLog.Queryable.First(t => t.Document == newDocument);
				Assert.That(newDocumentLog.Committed, Is.False);
			}

			LoadDocuments("5.3.16.1101");
			ShouldBeSuccessfull();
			Confirm();

			//Новый документ должен быть подтвержден
			using (new SessionScope())
			{
				newDocumentLog.Refresh();
				Assert.That(newDocumentLog.Committed, Is.True);
			}

			//Для первого КО изменяем дату обновления, отодвигая ее больше чем на месяц
			using (var transaction = new TransactionScope(OnDispose.Rollback)) {
				oldUpdate.Refresh();
				Assert.That(oldUpdate.Commit, Is.True);

				oldUpdate.RequestTime = DateTime.Now.AddMonths(-1).AddDays(-5);
				oldUpdate.Update();

				transaction.VoteCommit();
			}
	
			//Будем запрашивать КО с датой обновления старше чем 2 месяца
			updateTime = DateTime.Now.AddMonths(-2);
			GetUserData(updateTime, true);
			ShouldBeSuccessfull();

			using (new SessionScope())
			{
				//эти два документа не будут отдаваться, т.к. у них дата обновления больше чем 1 месяц
				log.Refresh();
				Assert.That(log.Committed, Is.True);
				CheckDelivered(log, true, false);

				fakelog.Refresh();
				Assert.That(fakelog.Committed, Is.True);
				CheckDelivered(fakelog, false, false);

				//Этот документ должен отдаваться
				newDocumentLog.Refresh();
				Assert.That(newDocumentLog.Committed, Is.False);
				CheckDelivered(newDocumentLog, true, false);
			}

			ConfirmData();

			using (new SessionScope())
			{
				log.Refresh();
				Assert.That(log.Committed, Is.True);
				CheckDelivered(log, true, false);

				fakelog.Refresh();
				Assert.That(fakelog.Committed, Is.True);
				CheckDelivered(fakelog, false, false);

			    //После подтверждения КО для этого документа статус доставки должен быть подтвержден
				newDocumentLog.Refresh();
				Assert.That(newDocumentLog.Committed, Is.True);
				CheckDelivered(newDocumentLog, true, false);
			}
		}

		[Test(Description = "При запросе КО после неподтвержденного частичного КО архив должен быть подготовлен заново")]
		public void GetCumulativeAfterUnconfirmedLimitedCumulative()
		{
			var updateTime = DateTime.Now.AddMinutes(-1);

			GetUserData(updateTime, true);
			ShouldBeSuccessfull();
			var firstCumulativeUpdateId = lastUpdateId;
			ConfirmData();

			GetUserData(updateTime);
			ShouldBeSuccessfull();
			var limitedCumulativeUpdateId = lastUpdateId;

			Assert.That(limitedCumulativeUpdateId, Is.Not.EqualTo(firstCumulativeUpdateId), "Запрос с частичным КО должен быть новым");

			GetUserData(updateTime, true);
			ShouldBeSuccessfull();
			var secondCumulativeUpdateId = lastUpdateId;

			Assert.That(secondCumulativeUpdateId, Is.Not.EqualTo(firstCumulativeUpdateId));
			Assert.That(secondCumulativeUpdateId, Is.Not.EqualTo(limitedCumulativeUpdateId), "Запрос с явным КО после неподтвержденного частичного должен быть новым");

			ConfirmData();

			using (new SessionScope()) {
				var limitedLog = TestAnalitFUpdateLog.Find(limitedCumulativeUpdateId);
				var secondCumulativeLog = TestAnalitFUpdateLog.Find(secondCumulativeUpdateId);

				Assert.That(limitedLog.Commit, Is.False, "Частиное КО не должно быть подтверждено");
				Assert.That(secondCumulativeLog.Commit, Is.True, "Повторный запрос явного КО должен быть подтвержден");
			}
		}

		[Test(Description = "При запросе частичного КО после неподтвержденного частичного КО архив не должен готовиться заново при совпадении дат")]
		public void GetLimitedCumulativeAfterUnconfirmedLimitedCumulative()
		{
			var updateTime = DateTime.Now.AddMinutes(-1);

			GetUserData(updateTime, true);
			ShouldBeSuccessfull();
			var firstCumulativeUpdateId = lastUpdateId;
			ConfirmData();

			GetUserData(updateTime);
			ShouldBeSuccessfull();
			var limitedCumulativeUpdateId = lastUpdateId;

			Assert.That(limitedCumulativeUpdateId, Is.Not.EqualTo(firstCumulativeUpdateId), "Запрос с частичным КО должен быть новым");

			GetUserData(updateTime);
			ShouldBeSuccessfull();
			var secondlimitedCumulativeUpdateId = lastUpdateId;

			Assert.That(secondlimitedCumulativeUpdateId, Is.Not.EqualTo(firstCumulativeUpdateId));
			Assert.That(secondlimitedCumulativeUpdateId, Is.EqualTo(limitedCumulativeUpdateId), "Повторный запрос частичного КО после неподтвержденного частичного должен быть тем же, если совпадают даты");

			ConfirmData();

			using (new SessionScope()) {
				var limitedLog = TestAnalitFUpdateLog.Find(limitedCumulativeUpdateId);

				Assert.That(limitedLog.Commit, Is.True, "Частиное КО должно быть подтверждено");
			}
		}

		[Test(Description = "При запросе частичного КО после неподтвержденного частичного КО архив должен готовиться заново при разных датах")]
		public void GetLimitedCumulativeAfterUnconfirmedLimitedCumulativeWithDifferentDates()
		{
			var updateTime = DateTime.Now.AddMinutes(-1);

			GetUserData(updateTime, true);
			ShouldBeSuccessfull();
			var firstCumulativeUpdateId = lastUpdateId;
			ConfirmData();

			GetUserData(updateTime);
			ShouldBeSuccessfull();
			var limitedCumulativeUpdateId = lastUpdateId;

			Assert.That(limitedCumulativeUpdateId, Is.Not.EqualTo(firstCumulativeUpdateId), "Запрос с частичным КО должен быть новым");

			updateTime = updateTime.AddMinutes(-1);
			GetUserData(updateTime);
			ShouldBeSuccessfull();
			var secondlimitedCumulativeUpdateId = lastUpdateId;

			Assert.That(secondlimitedCumulativeUpdateId, Is.Not.EqualTo(firstCumulativeUpdateId));
			Assert.That(secondlimitedCumulativeUpdateId, Is.Not.EqualTo(limitedCumulativeUpdateId), "Повторный запрос частичного КО после неподтвержденного частичного должен быть новым, если даты не совпадают");

			ConfirmData();

			using (new SessionScope()) {
				var limitedLog = TestAnalitFUpdateLog.Find(limitedCumulativeUpdateId);
				var secondlimitedLog = TestAnalitFUpdateLog.Find(secondlimitedCumulativeUpdateId);

				Assert.That(limitedLog.Commit, Is.False, "Первое частиное КО должно быть не подтверждено");
				Assert.That(secondlimitedLog.Commit, Is.True, "Второе частиное КО должно быть подтверждено");
			}
		}

		[Test(Description = "при успешном запросе документов поле Log в AnalitFUpdates должно быть равно null")]
		public void SuccessLoadDocumentsSetNullInLog()
		{
			LoadDocuments();
			ShouldBeSuccessfull();
			ConfirmData();

			var log = TestAnalitFUpdateLog.Find(lastUpdateId);
			Assert.That(log.Log, Is.Null);
		}

		[Test(Description = "проверяем работу метода WaitParsedDocs при отсутствии запроса на разбор документов")]
		public void SuccessWaitParsedDocs()
		{
			var user = client.Users[0];

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, user.Login);
				var helper = new UpdateHelper(updateData, connection);

				var startTime = DateTime.Now;
				helper.WaitParsedDocs();
				Assert.That(DateTime.Now.Subtract(startTime).TotalSeconds, Is.LessThan(10), "Выполнение метода производилось больше чем 10 секунд");
			}
		}

		[Test(Description = "проверяем работу метода WaitParsedDocs при длительном ожидании")]
		public void LongWaitParsedDocs()
		{
			var user = client.Users[0];

			SendWaybill();

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, user.Login);
				var helper = new UpdateHelper(updateData, connection);

				var startTime = DateTime.Now;
				helper.WaitParsedDocs();
				Assert.That(DateTime.Now.Subtract(startTime).TotalSeconds, Is.GreaterThan(55), "Выполнение метода производилось меньше чем 55 секунд");
			}
		}

		[Test(Description = "проверяем работу метода WaitParsedDocs при успешных разобранных документах")]
		public void SuccessWaitParsedDocsWithParse()
		{
			var user = client.Users[0];

			SendWaybill();

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, user.Login);
				var helper = new UpdateHelper(updateData, connection);

				MySqlHelper.ExecuteNonQuery(
					connection,
					@"
insert into Documents.DocumentHeaders (DownloadId, FirmCode, ClientCode, AddressId, DocumentType)
select
	dl.RowId,
	dl.FirmCode,
	dl.ClientCode,
	dl.AddressId,
	1
from
	logs.AnalitFUpdates afu
	inner join logs.document_logs dl on dl.SendUpdateId = afu.UpdateId
where
	afu.UserId = ?UserId
and afu.UpdateType = ?UpdateType"
					,
					new MySqlParameter("?UserId", user.Id),
					new MySqlParameter("?UpdateType", (int)RequestType.SendWaybills));

				var startTime = DateTime.Now;
				helper.WaitParsedDocs();
				Assert.That(DateTime.Now.Subtract(startTime).TotalSeconds, Is.LessThan(10), "Выполнение метода производилось больше чем 10 секунд");
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
				user.SendWaybills = true;
				user.SendRejects = true;
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
			Assert.That(responce, Is.StringStarting("URL=http://localhost/GetFileHandler.ashx?Id"));
		}

		private void Confirm()
		{
			var service = new PrgDataEx();
			service.MaxSynonymCode("", new uint[0], lastUpdateId, true);

			//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
			Thread.Sleep(3000);
		}

		private string LoadDocuments(string appversion = "1065")
		{
			var service = new PrgDataEx();
			responce = service.GetUserData(DateTime.Now, false, appversion, 50, "123", "", "", true);

			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
				lastUpdateId = Convert.ToUInt32(match);
			return responce;
		}

		private string GetUserData(DateTime updateTime, bool cumulative = false)
		{
			var service = new PrgDataEx();
			responce = service.GetUserData(updateTime, cumulative, "5.3.16.1101", 50, "123", "", "", false);

			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
				lastUpdateId = Convert.ToUInt32(match);
			return responce;
		}

		private void ConfirmData()
		{
			var service = new PrgDataEx();
			service.CommitExchange(lastUpdateId, false);

			//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
			Thread.Sleep(3000);
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}

		private TestAnalitFUpdateLog LastLog(TestUser user)
		{
			return TestAnalitFUpdateLog.Queryable.Where(l => l.UserId == user.Id).ToList().OrderByDescending(l => l.Id).First();
		}
	}
}