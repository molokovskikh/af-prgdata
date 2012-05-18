using System;
using System.Data;
using System.IO;
using Common.MySql;
using Common.Tools.Calendar;
using Inforoom.Common;
using log4net;
using MySql.Data.MySqlClient;
using MySqlHelper = MySql.Data.MySqlClient.MySqlHelper;

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

			_tmpLogsFolder = Path.GetTempPath() + Path.GetRandomFileName();
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

		public int ImportLogFile()
		{
			var importFileName = Path.GetFileName(_tmpExtractLogFileName);
			var localImportFileName = Path.Combine(ServiceContext.MySqlLocalImportPath(), importFileName);

			var serverImportFileName = Path.Combine(ServiceContext.MySqlSharedExportPath(), importFileName); 

			try
			{
				File.Copy(_tmpExtractLogFileName, localImportFileName);

				var temporaryTableName = "UserSettings.tempUserActions" + _updateData.UserId;

				return With.DuplicateEntryWraper<int>(() => {
					var transaction = _connection.BeginTransaction(IsolationLevel.ReadCommitted);
					try
					{
						var command = new MySqlCommand();
						command.Connection = _connection;
						command.Transaction = transaction;

						command.CommandText = String.Format(@"
	drop temporary table IF EXISTS {0};
	create temporary table {0} (   
	  Id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	  LogTime DATETIME NOT NULL,
	  UserId INT(10) UNSIGNED NOT NULL,
	  UpdateId INT(10) UNSIGNED NOT NULL,
	  UserActionId INT(10) UNSIGNED NOT NULL,
	  Context VARCHAR(255) DEFAULT NULL ,
	  PRIMARY KEY (Id)
	 ) engine=MEMORY;
	LOAD DATA INFILE '{1}' into table {0}
	( LogTime, UserActionId, Context)
	set UserId = {2}, UpdateId = {3};
	"
						,
						temporaryTableName,
						MySqlHelper.EscapeString(serverImportFileName),
						_updateData.UserId,
						_updateId);

						var insertCount = command.ExecuteNonQuery();

						command.CommandText = String.Format(@"
	insert into logs.AnalitFUserActionLogs (LogTime, UserId, UpdateId, UserActionId, Context)
	select LogTime, UserId, UpdateId, UserActionId, Context from {0};
	drop temporary table IF EXISTS {0};
	", temporaryTableName);

						command.ExecuteNonQuery();

						transaction.Commit();

						return insertCount;
					}
					catch
					{
						ConnectionHelper.SafeRollback(transaction);
						throw;
					}
				},
				3,
				2.Second(),
				30.Second());

			}
			finally
			{
				if (File.Exists(localImportFileName))
					File.Delete(localImportFileName);
			}
		}

	}
}