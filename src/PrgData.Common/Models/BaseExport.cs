using System.Collections.Generic;
using System.IO;
using MySql.Data.MySqlClient;
using log4net;

namespace PrgData.Common.Models
{
	public abstract class BaseExport
	{
		protected ILog log;
		protected Queue<FileForArchive> files;
		protected MySqlConnection connection;
		protected UpdateData updateData;

		protected BaseExport(UpdateData updateData, MySqlConnection connection, Queue<FileForArchive> files)
		{
			log = LogManager.GetLogger(GetType());
			this.updateData = updateData;
			this.connection = connection;
			this.files = files;
		}

		public abstract int RequiredVersion { get; }

		public bool SupportDocumentRequest { get; protected set; }

		public bool SupportAttachmentsRequest { get; protected set; }

		public abstract void Export();

		public virtual void ArchiveFiles(string archiveFile) {}

		protected void Process(string name, string sql)
		{
			var file = name + updateData.UserId + ".txt";
			var importFile = Path.Combine(ServiceContext.MySqlLocalImportPath(), file);
			ShareFileHelper.WaitDeleteFile(importFile);

			var exportFile = Path.Combine(ServiceContext.MySqlSharedExportPath(), file);
			exportFile = MySqlHelper.EscapeString(exportFile);

			sql += " INTO OUTFILE '" + exportFile + "' ";
			var command = new MySqlCommand(sql, connection);
			command.Parameters.AddWithValue("?UpdateTime", updateData.OldUpdateTime);
			command.ExecuteNonQuery();

			files.Enqueue(new FileForArchive(name, false));
		}
	}
}