using System;
using System.IO;
using Inforoom.Common;
using log4net;
using MySql.Data.MySqlClient;

namespace PrgData.Common
{
	public class SendUserActionsHandler
	{
		private UpdateData _updateData;
		private uint _updateId;

		private string _tmpLogsFolder;

		private string _tmpLogArchive;

		private readonly ILog _log = LogManager.GetLogger(typeof(SendUserActionsHandler));

		private string _tmpExtractLogFileName;

		private MySqlConnection _connection;


		public SendUserActionsHandler(UpdateData updateData, uint updateId, MySqlConnection connection)
		{
			_updateData = updateData;
			_updateId = updateId;
			_connection = connection;

			_tmpLogsFolder = Path.GetTempPath() + Path.GetFileNameWithoutExtension(Path.GetTempFileName());
			_tmpLogArchive = _tmpLogsFolder + @"\logs.7z";
			Directory.CreateDirectory(_tmpLogsFolder);
		}

		public void DeleteTemporaryFiles()
		{ 
			if (Directory.Exists(_tmpLogsFolder))
				try
				{
					Directory.Delete(_tmpLogsFolder, true);
				}
				catch (Exception exception)
				{
					_log.Error("Ошибка при удалении временнной директории при обработке статистики пользователя", exception);
				}
		}

		public void PrepareLogFile(string logFile)
		{
			var batchFileBytes = Convert.FromBase64String(logFile);
			using (var fileBatch = new FileStream(_tmpLogArchive, FileMode.CreateNew))
				fileBatch.Write(batchFileBytes, 0, batchFileBytes.Length);

			if (!ArchiveHelper.TestArchive(_tmpLogArchive))
				throw new Exception("Полученный архив поврежден.");

			var extractDir = Path.GetDirectoryName(_tmpLogArchive) + "\\LogExtract";
			if (!Directory.Exists(extractDir))
				Directory.CreateDirectory(extractDir);

			ArchiveHelper.Extract(_tmpLogArchive, "*.*", extractDir);
			var files = Directory.GetFiles(extractDir);
			if (files.Length == 0)
			{
				_log.DebugFormat("Содержимое полученного архива со статистикой: {0}", logFile);
				throw new Exception("Полученный архив не содержит файлов.");
			}

			_tmpExtractLogFileName = Path.Combine(Path.GetDirectoryName(files[0]), "UserActionLogs" + _updateData.UserId + ".txt");

			File.Copy(files[0], _tmpExtractLogFileName);
		}

		private string MySqlFilePath()
		{
#if DEBUG
			return System.Configuration.ConfigurationManager.AppSettings["MySqlFilePath"] + "\\";
#else
			return
				Path.Combine("\\\\" + Environment.MachineName,
				             System.Configuration.ConfigurationManager.AppSettings["MySqlFilePath"]) + "\\";
#endif
		}

		public int ImportLogFile()
		{
			var importFileName = Path.GetFileName(_tmpExtractLogFileName);
			var localImportFileName = Path.Combine(System.Configuration.ConfigurationManager.AppSettings["MySqlLocalFilePath"],
			                                       importFileName);

			var serverImportFileName = Path.Combine(MySqlFilePath(), importFileName); 

			try
			{
				File.Copy(_tmpExtractLogFileName, localImportFileName);

				var command = String.Format(@"
LOAD DATA INFILE '{0}' into table logs.AnalitFUserActionLogs
( LogTime, UserActionId, Context)
set UserId = {1}, UpdateId = {2};
"
					,
					MySqlHelper.EscapeString(serverImportFileName),
					_updateData.UserId,
					_updateId);

				return MySqlHelper.ExecuteNonQuery(_connection, command);
			}
			finally
			{
				if (File.Exists(localImportFileName))
					File.Delete(localImportFileName);
			}
		}

	}
}