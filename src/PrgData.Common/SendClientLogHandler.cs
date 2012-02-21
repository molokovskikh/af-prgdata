using System;
using System.IO;
using System.Text;
using Inforoom.Common;
using log4net;

namespace PrgData.Common
{
	public class SendClientLogHandler
	{
		 
		private UpdateData _updateData;
		private uint _updateId;

		private string _tmpLogsFolder;

		private string _tmpLogArchive;

		private readonly ILog _log = LogManager.GetLogger(typeof(SendClientLogHandler));

		private string _tmpExtractLogFileName;

		public SendClientLogHandler(UpdateData updateData, uint updateId)
		{
			_updateData = updateData;
			_updateId = updateId;

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
					_log.Error("Ошибка при удалении временнной директории при обработке файла протоколирования пользователя", exception);
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
				_log.DebugFormat("Содержимое полученного архива с протоколированием: {0}", logFile);
				throw new Exception("Полученный архив не содержит файлов.");
			}

			_tmpExtractLogFileName = Path.Combine(Path.GetDirectoryName(files[0]), "UserLog" + _updateId + ".txt");

			File.Copy(files[0], _tmpExtractLogFileName);
		}

		public string GetLogContent()
		{
			return File.ReadAllText(_tmpExtractLogFileName, Encoding.GetEncoding(1251));
		}

	}

}