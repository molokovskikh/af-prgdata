using System;
using System.Data;
using System.IO;
using System.Linq;
using Castle.ActiveRecord;
using Integration.BaseTests;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
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
		public override void FixtureSetup()
		{
			base.FixtureSetup();

			if (!Directory.Exists(attachmentsFolder))
				Directory.CreateDirectory(attachmentsFolder);
		}

		[SetUp]
		public override void Setup()
		{
			base.Setup();

			_user = CreateUserWithMinimumPrices();

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

				log = new TestMailSendLog{
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
			using (var connection = new MySqlConnection(Settings.ConnectionString())) {
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				var selectComand = new MySqlCommand();
				selectComand.Connection = connection;
				helper.SetUpdateParameters(selectComand, true, DateTime.Now.AddHours(-1), DateTime.Now);

				helper.FillExportMails(selectComand);

				Assert.That(updateData.ExportMails.Count, Is.EqualTo(0));

				var log = CreateTestMail();

				helper.FillExportMails(selectComand);
				Assert.That(updateData.ExportMails.Count, Is.EqualTo(1));
				Assert.That(updateData.ExportMails[0], Is.EqualTo(log.Mail.Id));

				var dataAdapter = new MySqlDataAdapter(selectComand);

				selectComand.CommandText = helper.GetMailsCommand();
				var mailsTable = new DataTable();
				dataAdapter.Fill(mailsTable);
				Assert.That(mailsTable.Rows.Count, Is.EqualTo(1));
				Assert.That(mailsTable.Rows[0]["Id"], Is.EqualTo(log.Mail.Id));

				selectComand.CommandText = helper.GetAttachmentsCommand();
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
				ShouldBeSuccessfull(response);
				var simpleUpdateId = ParseUpdateId(response);

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

				var response = LoadDataAttachments(true, DateTime.Now, "1.1.1.1413", new[] {attachmentSendLog.Attachment.Id});

				ShouldBeSuccessfull(response);
				var simpleUpdateId = ParseUpdateId(response);

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
	}
}