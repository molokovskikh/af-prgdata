using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using MySql.Data.MySqlClient;
using log4net;

namespace PrgData.Common.Models
{
	public abstract class BaseExport
	{
		protected ILog log;
		protected ConcurrentQueue<string> files;
		protected MySqlConnection connection;
		protected UpdateData updateData;

		public bool UnderTest;
		public Dictionary<string, DataTable> data = new Dictionary<string, DataTable>();

		protected BaseExport(UpdateData updateData, MySqlConnection connection, ConcurrentQueue<string> files)
		{
			log = LogManager.GetLogger(GetType());
			this.updateData = updateData;
			this.connection = connection;
			this.files = files;
		}

		public abstract int RequiredVersion { get; }

		public virtual RequestType[] AllowedArchiveRequests
		{
			get
			{
				return new[] {
					RequestType.GetData, RequestType.GetCumulative, RequestType.PostOrderBatch, RequestType.GetDataAsync,
					RequestType.GetCumulativeAsync, RequestType.GetLimitedCumulative, RequestType.GetLimitedCumulativeAsync
				};
			}
		}

		public abstract void Export();

		public virtual void ArchiveFiles(string archiveFile)
		{
		}

		protected string Process(string name, string sql, bool addToQueue = true)
		{
			if (UnderTest) {
				FakeProcess(name, sql);
				return null;
			}

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
			SetParameters(command);
			command.ExecuteNonQuery();

			if (addToQueue)
				files.Enqueue(importFile);

#if DEBUG
			//в отладочной версии ожидаем экспортирование файла из базы данных MySql
			ShareFileHelper.WaitFile(waitedExportFile);
#endif

			return importFile;
		}

		protected void SetParameters(MySqlCommand command)
		{
			command.Parameters.AddWithValue("?UpdateTime", updateData.OldUpdateTime);
			command.Parameters.AddWithValue("?RegionMask", updateData.RegionMask);
		}

		private void FakeProcess(string name, string sql)
		{
			var adapter = new MySqlDataAdapter(sql, connection);
			SetParameters(adapter.SelectCommand);
			var result = new DataTable();
			adapter.Fill(result);
			data.Add(name, result);
		}

		public bool AllowArchiveFiles(RequestType request)
		{
			return AllowedArchiveRequests.Contains(request);
		}
	}
}