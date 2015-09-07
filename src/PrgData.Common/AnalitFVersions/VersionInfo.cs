using System;
using System.Diagnostics;
using System.IO;
using Common.Tools;

namespace PrgData.Common.AnalitFVersions
{
	public class VersionInfo
	{
		public uint VersionNumber { get; private set; }
		public string Folder { get; private set; }
		public bool IsNet;

		public VersionInfo(uint versionNumber)
		{
			VersionNumber = versionNumber;
		}

		public VersionInfo(string folder)
		{
			IsNet = File.Exists(Path.Combine(folder, "Exe", "AnalitF.exe.config"));
			if (!Directory.Exists(folder))
				throw new ArgumentException(String.Format("Указанная директория не существует: {0}", folder), "folder");

			var dirInfo = new DirectoryInfo(folder);

			if (!dirInfo.Name.StartsWith("Release", StringComparison.OrdinalIgnoreCase))
				throw new ArgumentException(String.Format("Название директории не начинается с Release: {0}", folder), "folder");

			if (dirInfo.Name.Length == "Release".Length)
				throw new ArgumentException(String.Format("В названии директории не содержися номер версии: {0}", folder), "folder");

			var versionInfo = dirInfo.Name.Substring("Release".Length);
			uint version;
			if (uint.TryParse(versionInfo, out version))
				VersionNumber = version;
			else
				throw new Exception(String.Format("Невозможно конвертировать номер версии: {0}", versionInfo));

			Folder = folder;

			var exeFolder = dirInfo.GetDirectories("Exe");
			if (exeFolder.Length <= 0)
				throw new Exception(String.Format("Не найдена вложенная директория Exe: {0}", folder));

			var exeFile = exeFolder[0].GetFiles("*.exe");
			if (exeFile.Length <= 0)
				throw new Exception(String.Format("Во вложенной директории Exe не найден файл с расширением .exe: {0}", folder));

			if (exeFile.Length > 1)
				throw new Exception(String.Format("Во вложенной директории Exe найдено более одного файла с расширением .exe: {0}", folder));
		}

		public string ExeFolder()
		{
			return Path.Combine(Folder, "Exe");
		}
	}
}