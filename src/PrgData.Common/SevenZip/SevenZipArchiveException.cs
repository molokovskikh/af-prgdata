using System;

namespace PrgData.Common.SevenZip
{
	public class SevenZipArchiveException : Exception
	{
			public int ExitCode { get; private set; }
			
			public string Output { get; private set; }

			public string ErrorOutput { get; private set; }

			public string Command { get; private set; }

			public SevenZipArchiveException()
				: base("Процесс архивирования завершился ошибкой.")
			{
				ExitCode = -1;
			}

			public SevenZipArchiveException(int exitCode, string output)
				: base(String.Format("Процесс архивирования завершился с ошибкой : {0}. {1}", exitCode, output))
			{
				ExitCode = exitCode;
				Output = output;
			}

			public SevenZipArchiveException(int exitCode, string command, string output, string errorOutput)
				: base(String.Format("Процесс архивирования завершился с ошибкой : {0}. Комманда {2}. {1} {3}", exitCode, output, command, errorOutput))
			{
				ExitCode = exitCode;
				Output = output;
				ErrorOutput = errorOutput;
				Command = command;
			}
	}
}