using System;
using System.Diagnostics;
using System.IO;

namespace PrgData.Common.SevenZip
{
	public class SevenZipHelper
	{
		private const string SevenZipExe = @"C:\Program Files\7-Zip\7z.exe";

		private const string SevenZipParam = " -mx7 -bd -slp -mmt=6 ";

		public static void ArchiveFiles(string archiveFileName, string fileMask)
		{
			using (var process = new Process())
			{
				var startInfo = new ProcessStartInfo(SevenZipExe);
				startInfo.CreateNoWindow = true;
				startInfo.RedirectStandardOutput = true;
				startInfo.RedirectStandardError = true;
				startInfo.UseShellExecute = false;
				startInfo.StandardOutputEncoding = System.Text.Encoding.GetEncoding(866);
				startInfo.Arguments = String
					.Format(
						" a \"{0}\" \"{1}\" {2}",
						archiveFileName,
						fileMask,
						SevenZipParam + " -w" + Path.GetTempPath());

				startInfo.FileName = SevenZipExe;

				process.StartInfo = startInfo;

				process.Start();

				process.WaitForExit();

				if (process.ExitCode != 0)
				{
					throw new SevenZipArchiveException(
						process.ExitCode, 
						startInfo.Arguments, 
						process.StandardOutput.ReadToEnd(), 
						process.StandardError.ReadToEnd());
				}
			}
		}

		public static void ArchiveFilesWithNames(string archiveFileName, string fileMask, string workingFolder)
		{
			var startInfo = new ProcessStartInfo(SevenZipExe);
			startInfo.CreateNoWindow = true;
			startInfo.RedirectStandardOutput = true;
			startInfo.RedirectStandardError = true;
			startInfo.UseShellExecute = false;
			startInfo.StandardOutputEncoding = System.Text.Encoding.GetEncoding(866);

			startInfo.Arguments = "a \"" + archiveFileName + "\" " + " -i!\"" + fileMask + "\" " + SevenZipParam + " -w" + Path.GetTempPath();

			startInfo.WorkingDirectory = workingFolder;

			using(var process = new Process())
			{
				process.StartInfo = startInfo;
				process.Start();
				process.WaitForExit();

				if (process.ExitCode != 0)
				{
					throw new SevenZipArchiveException(
						process.ExitCode, 
						startInfo.Arguments, 
						process.StandardOutput.ReadToEnd(), 
						process.StandardError.ReadToEnd());
				}
			}
		}

	}
}