using System.Collections.Generic;
using System.IO;
using System.Linq;
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

		public abstract RequestType[] AllowedArchiveRequests { get; }

		public abstract void Export();

		public virtual void ArchiveFiles(string archiveFile)
		{
		}

		protected string Process(string name, string sql, bool addToQueue = true)
		{
			var file = name + updateData.UserId + ".txt";
			var importFile = ServiceContext.GetFileByLocal(file);
			//удаляем файл из папки перед экспортом
			ShareFileHelper.MySQLFileDelete(importFile);
			//ожидаем удаление файла
			ShareFileHelper.WaitDeleteFile(importFile);

			var waitedExportFile = ServiceContext.GetFileByShared(file);
			var exportFile = MySqlHelper.EscapeString(waitedExportFile);

			sql += " INTO OUTFILE '" + exportFile + "' ";
			var command = new MySqlCommand(sql, connection);
			command.Parameters.AddWithValue("?UpdateTime", updateData.OldUpdateTime);
			command.ExecuteNonQuery();

			if (addToQueue)
				files.Enqueue(new FileForArchive(name, false));

#if DEBUG
			//в отладочной версии ожидаем экспортирование файла из базы данных MySql
			ShareFileHelper.WaitFile(waitedExportFile);
#endif

			return importFile;
		}

		public bool AllowArchiveFiles(RequestType request)
		{
			return AllowedArchiveRequests.Any(r => r == request);
		}
	}
}