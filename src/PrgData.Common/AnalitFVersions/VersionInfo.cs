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

		public FileVersionInfo ExeVersionInfo { get; private set; }

		public VersionInfo(string folder)
		{
			if(!Directory.Exists(folder))
				throw new ArgumentException(String.Format("��������� ���������� �� ����������: {0}", folder), "folder");

			var dirInfo = new DirectoryInfo(folder);

			if (!dirInfo.Name.StartsWith("Release", StringComparison.OrdinalIgnoreCase))
				throw new ArgumentException(String.Format("�������� ���������� �� ���������� � Release: {0}", folder), "folder");

			if (dirInfo.Name.Length == "Release".Length)
				throw new ArgumentException(String.Format("� �������� ���������� �� ��������� ����� ������: {0}", folder), "folder");

			var versionInfo = dirInfo.Name.Substring("Release".Length);
			uint version;
			if (uint.TryParse(versionInfo, out version))
				VersionNumber = version;
			else
				throw new Exception(String.Format("�� �������� �������������� ����� ������: {0}", versionInfo));

			Folder = folder;

			var exeFolder = dirInfo.GetDirectories("Exe");
			if (exeFolder.Length < 0)
				throw new Exception(String.Format("�� ������� ��������� ���������� Exe: {0}", folder));

			var exeFile = exeFolder[0].GetFiles("AnalitF.exe");
			if (exeFile.Length < 0)
				throw new Exception(String.Format("�� ��������� ���������� Exe �� ������ ���� AnalitF.Exe: {0}", folder));

			try
			{
				ExeVersionInfo = FileVersionInfo.GetVersionInfo(exeFile[0].FullName);
			}
			catch(Exception exception)
			{
				throw new Exception(String.Format("�� �������� �������� ���������� � ������ ��� �����: {0}", exeFile[0].FullName), exception);
			}

			if (VersionNumber != ExeVersionNumber())
				throw new Exception("�� ��������� ������ ������ � �������� ����� = {0} � ����� AnalitF.exe = {1}".Format(VersionNumber, ExeVersionNumber()));
		}

		public uint ExeVersionNumber()
		{
			if (ExeVersionInfo != null)
				return Convert.ToUInt32(ExeVersionInfo.FilePrivatePart);
			return 0;
		}

		public string ExeFolder()
		{
			return Path.Combine(Folder, "Exe");
		}

	}
}