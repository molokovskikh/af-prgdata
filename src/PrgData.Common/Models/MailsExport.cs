using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Common.Tools;
using MySql.Data.MySqlClient;
using PrgData.Common.SevenZip;
using log4net;

namespace PrgData.Common.Models
{
	public class MailsExport : BaseExport
	{
		public MailsExport(UpdateData updateData, MySqlConnection connection, Queue<FileForArchive> files)
			: base(updateData, connection, files)
		{
		}

		public override int RequiredVersion
		{
			get { return -1; }
		}

		public override RequestType[] AllowedArchiveRequests
		{
			get
			{
				return base.AllowedArchiveRequests.Concat(new[] { RequestType.RequestAttachments }).ToArray();
			}
		}

		public override void Export()
		{
		}

		public override void ArchiveFiles(string archiveFile)
		{
			var sufix = "Docs";

			var tempPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
			var docsPath = Path.Combine(tempPath, sufix);

			try {
				if (!Directory.Exists(tempPath))
					Directory.CreateDirectory(tempPath);
				if (!Directory.Exists(docsPath))
					Directory.CreateDirectory(docsPath);

				var exportMailscommand = new MySqlCommand();
				exportMailscommand.Connection = connection;
				var helper = new UpdateHelper(updateData, connection);
				helper.SetUpdateParameters(exportMailscommand, DateTime.Now);
				FillExportMails(exportMailscommand);

				if (updateData.ExportMails.Count > 0) {
					var mailsFile = Process("Mails", GetMailsCommand(), false);
					SevenZipHelper.ArchiveFiles(archiveFile, mailsFile);
					ShareFileHelper.MySQLFileDelete(mailsFile);
					ShareFileHelper.WaitDeleteFile(mailsFile);

					var attachmentsFile = Process("Attachments", GetAttachmentsCommand(), false);
					SevenZipHelper.ArchiveFiles(archiveFile, attachmentsFile);
					ShareFileHelper.MySQLFileDelete(attachmentsFile);
					ShareFileHelper.WaitDeleteFile(attachmentsFile);
				}

				if (updateData.NeedExportAttachments()) {
					//здесь экспортируются вложения
					ArchiveAttachments(tempPath, docsPath, archiveFile);
				}
			}
			finally {
				try {
					Directory.Delete(tempPath, true);
				}
				catch (Exception e) {
					log.Error(String.Format("Ошибка при удалении временной папки {0}", tempPath), e);
				}
			}
		}

		public void FillExportMails(MySqlCommand selectCommand)
		{
			updateData.ExportMails.Clear();
			//начинаем отдавать документы с самых новых что бы 
			//отдать наиболее актуальные
			var sql = @"
select 
	Mails.Id,
	if(s.Payer = 921 and (Mails.SupplierEmail like '%@analit.net' or Mails.SupplierEmail like '%.analit.net'), 1, 0) as ForceExportAttachments
from 
	documents.Mails 
	inner join Logs.MailSendLogs ms on ms.MailId = Mails.Id
	inner join customers.Suppliers s on s.Id = Mails.SupplierId
where 
	Mails.LogTime > curdate() - interval 30 day
and ms.UserId = ?UserId 
and ms.Committed = 0
order by Mails.LogTime desc
limit 200;
";
			selectCommand.CommandText = sql;
			using (var reader = selectCommand.ExecuteReader()) {
				while (reader.Read())
					updateData.ExportMails.Add(
						new ExportedMiniMail {
							MiniMailId = reader.GetUInt32(0),
							ForceExportAttachments = reader.GetBoolean(1)
						});
			}

			var forceAttachments = updateData.ExportMails.Where(m => m.ForceExportAttachments).Select(m => m.MiniMailId).ToList();
			if (forceAttachments.Count > 0) {
				selectCommand.CommandText = @"
select
	Attachments.Id
from
	Documents.Mails
	inner join Documents.Attachments on Attachments.MailId = Mails.Id
where
  Mails.Id in (" + forceAttachments.Implode() + ")";
				using (var reader = selectCommand.ExecuteReader()) {
					while (reader.Read()) {
						var attachmentId = reader.GetUInt32(0);

						//если запрос вложения не находится в списке запросов, то добавляем его туда
						if (updateData.AttachmentRequests.All(r => r.AttachmentId != attachmentId))
							updateData.AttachmentRequests.Add(
								new AttachmentRequest {
									AttachmentId = attachmentId
								});
					}
				}
			}
		}

		public string GetMailsCommand()
		{
			return @"
select
	Mails.Id,
	Mails.LogTime,
	Mails.SupplierId,
	Suppliers.Name as SupplierName,
	Mails.IsVIPMail,
	Mails.Subject,
	Mails.Body
from
	Documents.Mails
	inner join Customers.Suppliers on Suppliers.Id = Mails.SupplierId
where
  Mails.Id in (" + updateData.ExportMails.Select(m => m.MiniMailId).Implode() + ")";
		}

		public string GetAttachmentsCommand()
		{
			return @"
select
	Attachments.Id,
	Attachments.MailId,
	Attachments.FileName,
	Attachments.Extension,
	Attachments.Size
from
	Documents.Mails
	inner join Documents.Attachments on Attachments.MailId = Mails.Id
where
  Mails.Id in (" + updateData.ExportMails.Select(m => m.MiniMailId).Implode() + ")";
		}

		private void ProcessArchiveFile(string processedFile, string archiveFileName)
		{
			var fullPathFile = ServiceContext.GetFileByLocal(processedFile);
#if DEBUG
			ShareFileHelper.WaitFile(fullPathFile);
#endif

			try {
				SevenZipHelper.ArchiveFiles(archiveFileName, fullPathFile);
			}
			catch {
				ShareFileHelper.MySQLFileDelete(archiveFileName);
				throw;
			}

			ShareFileHelper.MySQLFileDelete(fullPathFile);

			ShareFileHelper.WaitDeleteFile(fullPathFile);
		}

		private string DeleteFileByPrefix(string prefix)
		{
			var deletedFile = prefix + updateData.UserId + ".txt";
			ShareFileHelper.MySQLFileDelete(ServiceContext.GetFileByLocal(deletedFile));

			ShareFileHelper.WaitDeleteFile(ServiceContext.GetFileByLocal(deletedFile));

			return deletedFile;
		}

		public void ArchiveAttachments(string tempPath, string docsPath, string archiveFileName)
		{
			var log = LogManager.GetLogger(typeof(MailsExport));

			try {
				log.Debug("Будем выгружать вложения");

				var command = new MySqlCommand();
				command.Connection = connection;
				command.Parameters.AddWithValue("?UserId", updateData.UserId);
				command.Parameters.Add("?AttachmentId", MySqlDbType.UInt32);

				var attachmentRequestsFile = DeleteFileByPrefix("AttachmentRequests");

				ArchiveAttachmentFiles(tempPath, docsPath, archiveFileName, command);

				if (updateData.SuccesAttachmentsExists()) {
					File.WriteAllText(ServiceContext.GetFileByLocal(attachmentRequestsFile), updateData.GetAttachmentsResult());
					ProcessArchiveFile(attachmentRequestsFile, archiveFileName);
				}
			}
			catch (Exception exception) {
				log.Error("Ошибка при архивировании почтовых вложений", exception);
				throw;
			}
		}

		private void ArchiveAttachmentFiles(string tempPath, string docsPath, string archiveFileName, MySqlCommand command)
		{
			var attachmentsFolder = "Attachments";
			var attachmentsPath = Path.Combine(updateData.ResultPath, attachmentsFolder);

			command.CommandText = @"
select 
	Attachments.Extension 
from 
	logs.AttachmentSendLogs 
	inner join documents.Attachments on Attachments.Id = AttachmentSendLogs.AttachmentId
where 
	AttachmentSendLogs.UserId = ?UserId
and AttachmentSendLogs.AttachmentId = ?AttachmentId";

			foreach (var request in updateData.AttachmentRequests) {
				command.Parameters["?AttachmentId"].Value = request.AttachmentId;
				var extension = command.ExecuteScalar();
				if (extension != null && !String.IsNullOrEmpty((string)extension)) {
					File.Copy(
						Path.Combine(attachmentsPath, request.AttachmentId + (string)extension),
						Path.Combine(docsPath, request.AttachmentId + (string)extension));
					request.Success = true;
				}
			}

			if (updateData.SuccesAttachmentsExists())
				SevenZipHelper.ArchiveFilesWithNames(
					archiveFileName,
					Path.Combine("Docs", "*.*"),
					tempPath);
		}
	}
}