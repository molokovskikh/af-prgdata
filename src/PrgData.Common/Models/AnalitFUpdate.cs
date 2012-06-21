using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Common.Tools;
using MySql.Data.MySqlClient;

namespace PrgData.Common.Models
{
	public class AnalitFUpdate
	{
		public static void UpdateLog(MySqlConnection connection, uint updateId, string log)
		{
			MySqlHelper.ExecuteNonQuery(
				connection,
				"update logs.AnalitFUpdates set Log=?Log  where UpdateId=?UpdateId",
				new MySqlParameter("?Log", log),
				new MySqlParameter("?UpdateId", updateId));
		}

		public static void UpdateLog(MySqlConnection connection, uint updateId, RequestType updateType, string addition)
		{
			MySqlHelper.ExecuteNonQuery(
				connection,
				"update logs.AnalitFUpdates set Addition=?Addition, UpdateType=?UpdateType  where UpdateId=?UpdateId",
				new MySqlParameter("?Addition", addition),
				new MySqlParameter("?UpdateType", (int)updateType),
				new MySqlParameter("?UpdateId", updateId));
		}

		public static uint InsertAnalitFUpdatesLog(MySqlConnection connection, UpdateData updateData, RequestType request)
		{
			return InsertAnalitFUpdatesLog(connection, updateData, request, null);
		}

		public static uint InsertAnalitFUpdatesLog(MySqlConnection connection, UpdateData updateData, RequestType request,
			string addition,
			bool commit = false,
			uint? resultSize = null,
			string log = null)
		{
			var uid = Convert.ToUInt32(MySqlHelper.ExecuteScalar(
				connection,
				@"
insert into logs.AnalitFUpdates (RequestTime, UpdateType, UserId, Commit, Addition, AppVersion, ClientHost, ResultSize, Log)
values (now(), ?UpdateType, ?UserId, ?Commit, ?Addition, ?AppVersion, ?ClientHost, ?ResultSize, ?Log);
select last_insert_id()",
				new MySqlParameter("?UpdateType", (int)request),
				new MySqlParameter("?UserId", updateData.UserId),
				new MySqlParameter("?Addition", addition),
				new MySqlParameter("?AppVersion", updateData.BuildNumber),
				new MySqlParameter("?ClientHost", updateData.ClientHost),
				new MySqlParameter("?ResultSize", resultSize),
				new MySqlParameter("?Commit", commit),
				new MySqlParameter("?Log", log)
			));
			return uid;
		}

		public static void Log(UpdateData data, MySqlConnection connection, uint? updateId)
		{
			ProcessExportMails(data, connection, updateId);
			LogCertificates(data, connection, updateId);
		}

		public static void ProcessExportMails(UpdateData updateData, MySqlConnection connection, uint? updateId)
		{
			if (updateData.ExportMails.Count > 0 || updateData.SuccesAttachmentsExists())
			{
				var transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead);
				try {
					String sql = String.Empty;

					if (updateData.ExportMails.Count > 0)
						sql += "update Logs.MailSendLogs set UpdateId = ?UpdateId where UserId = ?UserId and MailId in (" + updateData.ExportMails.Select(e => e.MiniMailId).Implode() + ");";

					if (updateData.SuccesAttachmentsExists())
						sql += "update Logs.AttachmentSendLogs set UpdateId = ?UpdateId where UserId = ?UserId and AttachmentId in ("+ updateData.SuccesAttachmentIds().Implode() + ");";

					if (!String.IsNullOrEmpty(sql))
						MySqlHelper.ExecuteNonQuery(
							connection,
							sql,
							new MySqlParameter("?UserId", updateData.UserId),
							new MySqlParameter("?UpdateId", updateId));

					transaction.Commit();
				}
				catch
				{
					ConnectionHelper.SafeRollback(transaction);
					throw;
				}
			}
		}

		public static void LogCertificates(UpdateData data, MySqlConnection connection, uint? updateId)
		{
			var sql = @"insert into Logs.CertificateRequestLogs(UpdateId, DocumentBodyId, CertificateId, Filename)
values (?UpdateId, ?DocumentBodyId, ?CertificateId, ?Filename)";
			var command = new MySqlCommand(sql, connection);
			command.Parameters.Add("UpdateId", MySqlDbType.UInt32);
			command.Parameters.Add("DocumentBodyId", MySqlDbType.UInt32);
			command.Parameters.Add("CertificateId", MySqlDbType.UInt32);
			command.Parameters.Add("Filename", MySqlDbType.VarChar);
			foreach (var request in data.CertificateRequests) {
				var files = request.SendedFiles;
				//нам нужно запротоколировать то что файл мы не нашли
				if (files.Count == 0) {
					files = new List<string>{ null };
				}
				foreach (var file in files)
				{
					command.Parameters["UpdateId"].Value = updateId;
					command.Parameters["DocumentBodyId"].Value = request.DocumentBodyId;
					command.Parameters["CertificateId"].Value = request.CertificateId;
					command.Parameters["Filename"].Value = Path.GetFileName(file);
					command.ExecuteNonQuery();
				}
			}
		}
	}
}