﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Castle.ActiveRecord;
using Common.MySql;
using Common.Tools;
using Integration.BaseTests;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData;
using PrgData.Common;
using PrgData.Common.Models;
using Test.Support;
using Test.Support.Documents;
using Test.Support.Logs;
using Test.Support.Suppliers;

namespace Integration
{
	[TestFixture]
	public class MiniMailFixture : PrepareDataFixture
	{
		private TestUser _user;
		private string attachmentsFolder = "results\\Attachments";

		[TestFixtureSetUp]
		public void FixtureSetup()
		{
			if (!Directory.Exists(attachmentsFolder))
				Directory.CreateDirectory(attachmentsFolder);
		}

		[SetUp]
		public void Setup()
		{
			_user = CreateUser();

			SetCurrentUser(_user.Login);
		}

		private TestMailSendLog CreateTestMail()
		{
			TestMail mail;
			TestMailSendLog log;

			var supplier = _user.GetActivePrices()[0].Supplier;

			using (var transaction = new TransactionScope(OnDispose.Rollback)) {
				mail = new TestMail(supplier);

				var attachment = new TestAttachment {
					FileName = "test.data",
					Extension = ".data",
					Mail = mail,
					Size = 10
				};
				mail.Attachments.Add(attachment);

				mail.CreateAndFlush();

				log = new TestMailSendLog {
					Mail = mail,
					User = _user
				};
				log.CreateAndFlush();

				var attachmentLog = new TestAttachmentSendLog {
					Attachment = attachment,
					User = _user
				};
				attachmentLog.CreateAndFlush();

				File.WriteAllText(Path.Combine(attachmentsFolder, attachment.Id + attachment.Extension), "this is test attachment " + attachment.Id);

				transaction.VoteCommit();
			}

			return log;
		}

		[Test(Description = "Проверяем корректность SQL для работы с почтой")]
		public void CheckMailSql()
		{
			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);
				var mailsExport = new MailsExport(updateData, connection, new ConcurrentQueue<string>());

				var selectComand = new MySqlCommand();
				selectComand.Connection = connection;
				updateData.Cumulative = true;
				updateData.OldUpdateTime = DateTime.Now.AddHours(-1);
				helper.SetUpdateParameters(selectComand);


				mailsExport.FillExportMails(selectComand);

				Assert.That(updateData.ExportMails.Count, Is.EqualTo(0));

				var log = CreateTestMail();

				mailsExport.FillExportMails(selectComand);
				Assert.That(updateData.ExportMails.Count, Is.EqualTo(1));
				Assert.That(updateData.ExportMails[0].MiniMailId, Is.EqualTo(log.Mail.Id));

				var dataAdapter = new MySqlDataAdapter(selectComand);

				selectComand.CommandText = mailsExport.GetMailsCommand();
				var mailsTable = new DataTable();
				dataAdapter.Fill(mailsTable);
				Assert.That(mailsTable.Rows.Count, Is.EqualTo(1));
				Assert.That(mailsTable.Rows[0]["Id"], Is.EqualTo(log.Mail.Id));

				selectComand.CommandText = mailsExport.GetAttachmentsCommand();
				var attachmentsTable = new DataTable();
				dataAdapter.Fill(attachmentsTable);
				Assert.That(attachmentsTable.Rows.Count, Is.EqualTo(1));
				Assert.That(attachmentsTable.Rows[0]["Id"], Is.EqualTo(log.Mail.Attachments[0].Id));
			}
		}

		[Test(Description = "простой запрос данных с получением почты")]
		public void SimpleRequestData()
		{
			var log = CreateTestMail();
			Assert.That(log.Committed, Is.False);
			Assert.That(log.UpdateLogEntry, Is.Null);

			ProcessWithLog(() => {
				var response = LoadData(true, DateTime.Now, "1.1.1.1413");
				var simpleUpdateId = ShouldBeSuccessfull(response);

				using (new SessionScope()) {
					log.Refresh();
					Assert.That(log.Committed, Is.False);
					Assert.That(log.UpdateLogEntry, Is.Not.Null);
					Assert.That(log.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));
				}

				CommitExchange(simpleUpdateId, RequestType.GetCumulative);

				using (new SessionScope()) {
					log.Refresh();
					Assert.That(log.Committed, Is.True);
					Assert.That(log.UpdateLogEntry, Is.Not.Null);
					Assert.That(log.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));
				}
			});
		}

		[Test(Description = "запрос данных с получением вложений почты")]
		public void RequestAttachments()
		{
			var log = CreateTestMail();
			Assert.That(log.Committed, Is.False);
			Assert.That(log.UpdateLogEntry, Is.Null);

			TestAttachmentSendLog attachmentSendLog;
			using (new SessionScope()) {
				attachmentSendLog =
					TestAttachmentSendLog.Queryable.FirstOrDefault(
						l => l.Attachment.Id == log.Mail.Attachments[0].Id && l.User.Id == _user.Id);
			}
			Assert.That(attachmentSendLog.Committed, Is.False);
			Assert.That(attachmentSendLog.UpdateLogEntry, Is.Null);

			ProcessWithLog(() => {
				var response = LoadDataAttachments(true, DateTime.Now, "1.1.1.1413", new[] { attachmentSendLog.Attachment.Id });

				var simpleUpdateId = ShouldBeSuccessfull(response);

				using (new SessionScope()) {
					log.Refresh();
					Assert.That(log.Committed, Is.False);
					Assert.That(log.UpdateLogEntry, Is.Not.Null);
					Assert.That(log.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));

					attachmentSendLog.Refresh();
					Assert.That(attachmentSendLog.Committed, Is.False);
					Assert.That(attachmentSendLog.UpdateLogEntry, Is.Not.Null);
					Assert.That(attachmentSendLog.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));
				}

				var archiveName = CheckArchive(_user, simpleUpdateId);

				var extractFolder = ExtractArchive(archiveName);

				var attachmentFileName =
					Path.Combine("Docs",
						attachmentSendLog.Attachment.Id + attachmentSendLog.Attachment.Extension);
				Assert.That(File.Exists(Path.Combine(extractFolder, attachmentFileName)), Is.True);

				CommitExchange(simpleUpdateId, RequestType.GetCumulative);

				using (new SessionScope()) {
					log.Refresh();
					Assert.That(log.Committed, Is.True);
					Assert.That(log.UpdateLogEntry, Is.Not.Null);
					Assert.That(log.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));

					attachmentSendLog.Refresh();
					Assert.That(attachmentSendLog.Committed, Is.True);
					Assert.That(attachmentSendLog.UpdateLogEntry, Is.Not.Null);
					Assert.That(attachmentSendLog.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));
				}
			});
		}

		[Test(Description = "асинхронный запрос данных с получением вложений почты")]
		public void RequestAttachmentsAsync()
		{
			var log = CreateTestMail();
			Assert.That(log.Committed, Is.False);
			Assert.That(log.UpdateLogEntry, Is.Null);

			TestAttachmentSendLog attachmentSendLog;
			using (new SessionScope()) {
				attachmentSendLog =
					TestAttachmentSendLog.Queryable.FirstOrDefault(
						l => l.Attachment.Id == log.Mail.Attachments[0].Id && l.User.Id == _user.Id);
			}
			Assert.That(attachmentSendLog.Committed, Is.False);
			Assert.That(attachmentSendLog.UpdateLogEntry, Is.Null);

			ProcessWithLog(() => {
				var response = LoadDataAttachmentsAsync(true, DateTime.Now, "1.1.1.1413", new[] { attachmentSendLog.Attachment.Id });

				var simpleUpdateId = ShouldBeSuccessfull(response);

				WaitAsyncResponse(simpleUpdateId);

				using (new SessionScope()) {
					log.Refresh();
					Assert.That(log.Committed, Is.False);
					Assert.That(log.UpdateLogEntry, Is.Not.Null);
					Assert.That(log.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));

					attachmentSendLog.Refresh();
					Assert.That(attachmentSendLog.Committed, Is.False);
					Assert.That(attachmentSendLog.UpdateLogEntry, Is.Not.Null);
					Assert.That(attachmentSendLog.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));
				}

				var archiveName = CheckArchive(_user, simpleUpdateId);

				var extractFolder = ExtractArchive(archiveName);

				var attachmentFileName =
					Path.Combine("Docs",
						attachmentSendLog.Attachment.Id + attachmentSendLog.Attachment.Extension);
				Assert.That(File.Exists(Path.Combine(extractFolder, attachmentFileName)), Is.True);

				CommitExchange(simpleUpdateId, RequestType.GetCumulative);

				using (new SessionScope()) {
					log.Refresh();
					Assert.That(log.Committed, Is.True);
					Assert.That(log.UpdateLogEntry, Is.Not.Null);
					Assert.That(log.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));

					attachmentSendLog.Refresh();
					Assert.That(attachmentSendLog.Committed, Is.True);
					Assert.That(attachmentSendLog.UpdateLogEntry, Is.Not.Null);
					Assert.That(attachmentSendLog.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));
				}
			});
		}

		private string GetLogContent()
		{
			var logFileBytes = File.ReadAllBytes("..\\..\\Data\\ClientLog.7z");
			Assert.That(logFileBytes.Length, Is.GreaterThan(0), "Файл с протоколированием оказался пуст, возможно, его нет в папке");

			return Convert.ToBase64String(logFileBytes);
		}

		[Test(Description = "Отправляем лог клиента в архивированном виде")]
		public void SimpleSendClientArchivedLog()
		{
			ProcessWithLog(() => {
				var response = LoadDataAttachmentsAsync(true, DateTime.Now, "1.1.1.1413", null);

				var simpleUpdateId = ShouldBeSuccessfull(response);

				WaitAsyncResponse(simpleUpdateId);

				TestAnalitFUpdateLog log;
				using (new SessionScope()) {
					log = TestAnalitFUpdateLog.Find(Convert.ToUInt32(simpleUpdateId));
					Assert.That(log.Commit, Is.False);
					Assert.IsNullOrEmpty(log.Log);
				}

				CommitExchange(simpleUpdateId, RequestType.GetCumulative);

				using (new SessionScope()) {
					log.Refresh();
					Assert.That(log.Commit, Is.True);
					Assert.IsNullOrEmpty(log.Log);
				}

				var logContent = GetLogContent();

				var service = new PrgDataEx();

				service.SendClientArchivedLog(simpleUpdateId, logContent, 77);

				using (new SessionScope()) {
					log.Refresh();
					Assert.That(log.Commit, Is.True);
					Assert.That(log.Log, Is.EqualTo(@"это тестовый лог протоколирования

это новая строка

и это новая строка
"));
				}
			});
		}

		[Test(Description = "проверка запроса только вложений минипочты")]
		public void RequestAttachmentsOnly()
		{
			var log = CreateTestMail();
			Assert.That(log.Committed, Is.False);
			Assert.That(log.UpdateLogEntry, Is.Null);

			TestAttachmentSendLog attachmentSendLog;
			using (new SessionScope()) {
				attachmentSendLog =
					TestAttachmentSendLog.Queryable.FirstOrDefault(
						l => l.Attachment.Id == log.Mail.Attachments[0].Id && l.User.Id == _user.Id);
			}
			Assert.That(attachmentSendLog.Committed, Is.False);
			Assert.That(attachmentSendLog.UpdateLogEntry, Is.Null);

			ProcessWithLog(() => {
				var response = LoadDataAttachmentsOnly(true, DateTime.Now, "1.1.1.1413", new[] { attachmentSendLog.Attachment.Id });

				var simpleUpdateId = ShouldBeSuccessfull(response);

				using (new SessionScope()) {
					log.Refresh();
					Assert.That(log.Committed, Is.False);
					Assert.That(log.UpdateLogEntry, Is.Not.Null);
					Assert.That(log.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));

					attachmentSendLog.Refresh();
					Assert.That(attachmentSendLog.Committed, Is.False);
					Assert.That(attachmentSendLog.UpdateLogEntry, Is.Not.Null);
					Assert.That(attachmentSendLog.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));
				}

				var archiveName = CheckArchive(_user, simpleUpdateId);

				var extractFolder = ExtractArchive(archiveName);


				var files = Directory.GetFiles(extractFolder, "*", SearchOption.AllDirectories);

				Assert.That(files.Length, Is.EqualTo(4), "В архиве должны быть четыре файла: таблица писем, таблица вложений, вложение мини-почты и результат запроса вложений мини-почты (AttachmentRequests): {0}", files.Implode());
				var attachmentFileName =
					Path.Combine("Docs",
						attachmentSendLog.Attachment.Id + attachmentSendLog.Attachment.Extension);
				Assert.That(files.Any(f => f.EndsWith(attachmentFileName, StringComparison.CurrentCultureIgnoreCase)), Is.True, "Не найден файл с вложением мини-почты: {0}", attachmentFileName);
				Assert.That(files.Any(f => f.EndsWith("AttachmentRequests" + _user.Id + ".txt", StringComparison.CurrentCultureIgnoreCase)), Is.True, "Не найден файл результат запроса вложений мини-почты: AttachmentRequests");
				Assert.That(files.Any(f => f.EndsWith("Mails" + _user.Id + ".txt", StringComparison.CurrentCultureIgnoreCase)), Is.True, "Не найден файл c письмами мини-почты: Mails");
				Assert.That(files.Any(f => f.EndsWith("Attachments" + _user.Id + ".txt", StringComparison.CurrentCultureIgnoreCase)), Is.True, "Не найден файл c вложениями мини-почты: Attachments");

				CommitExchange(simpleUpdateId, RequestType.RequestAttachments);

				using (new SessionScope()) {
					log.Refresh();
					Assert.That(log.Committed, Is.True);
					Assert.That(log.UpdateLogEntry, Is.Not.Null);
					Assert.That(log.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));

					attachmentSendLog.Refresh();
					Assert.That(attachmentSendLog.Committed, Is.True);
					Assert.That(attachmentSendLog.UpdateLogEntry, Is.Not.Null);
					Assert.That(attachmentSendLog.UpdateLogEntry.Id, Is.EqualTo(simpleUpdateId));
				}
			});
		}

		[Test(Description = "проверяем список запросов вложений на простом поставщике")]
		public void CheckAttachmentRequestsOnSimpleSupplier()
		{
			var log = CreateTestMail();

			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);
				var mailsExport = new MailsExport(updateData, connection, new ConcurrentQueue<string>());

				var selectComand = new MySqlCommand();
				selectComand.Connection = connection;
				updateData.Cumulative = true;
				updateData.OldUpdateTime = DateTime.Now.AddHours(-1);
				helper.SetUpdateParameters(selectComand);

				mailsExport.FillExportMails(selectComand);

				Assert.That(updateData.ExportMails.Count, Is.EqualTo(1));
				Assert.That(updateData.ExportMails[0].MiniMailId, Is.EqualTo(log.Mail.Id));
				Assert.That(updateData.AttachmentRequests.Count, Is.EqualTo(0), "Список запрашиваемых вложений должен быть пуст");
			}
		}

		[Test(Description = "проверяем список запросов вложений на VIP-поставщике")]
		public void CheckAttachmentRequestsOnVIPSupplier()
		{
			var log = CreateTestMail();
			using (new TransactionScope()) {
				log.Mail.SupplierEmail = "test" + log.Mail.Supplier.Id + "@analit.net";
				log.Mail.Save();
				var payer = TestPayer.Find(921u);
				log.Mail.Supplier.Payer = payer;
				log.Mail.Supplier.Save();
			}

			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);
				var mailsExport = new MailsExport(updateData, connection, new ConcurrentQueue<string>());

				var selectComand = new MySqlCommand();
				selectComand.Connection = connection;
				updateData.Cumulative = true;
				updateData.OldUpdateTime = DateTime.Now.AddHours(-1);
				helper.SetUpdateParameters(selectComand);

				mailsExport.FillExportMails(selectComand);

				Assert.That(updateData.ExportMails.Count, Is.EqualTo(1));
				Assert.That(updateData.ExportMails[0].MiniMailId, Is.EqualTo(log.Mail.Id));
				Assert.That(updateData.AttachmentRequests.Count, Is.EqualTo(1), "В списоке запрашиваемых вложений должно быть одно вложение");
				Assert.That(updateData.AttachmentRequests[0].AttachmentId, Is.EqualTo(log.Mail.Attachments[0].Id));
			}
		}
	}
}