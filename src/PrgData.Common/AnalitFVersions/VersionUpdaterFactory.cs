using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Common.Models;
using log4net;
using PrgData.Common.Models;
using PrgData.Common.Repositories;

namespace PrgData.Common.AnalitFVersions
{
	public static class VersionUpdaterFactory
	{
		private static ILog _logger = LogManager.GetLogger(typeof(VersionUpdaterFactory));
		private static string _lastVersionErrorMessage;

		public static ExeVersionUpdater GetUpdater()
		{
			var infos = GetVersionInfos();
			var rules = GetVersionRules();
			return new ExeVersionUpdater(infos, rules);
		}

		public static List<AnalitFVersionRule> GetVersionRules()
		{
			var rules = IoC.Resolve<IVersionRuleRepository>().FindAllRules();
			return rules;
		}

		public static List<VersionInfo> GetVersionInfos()
		{
			var stringBuilder = new StringBuilder();
			var infos = new List<VersionInfo>();

			var dirInfo = new DirectoryInfo(Path.Combine(ServiceContext.GetResultPath(), "Updates"));

			if (dirInfo.Exists)
			{
				var releaseInfos = dirInfo.GetDirectories("Release*");

				foreach (var releaseInfo in releaseInfos)
				{
					try
					{
						var info = new VersionInfo(releaseInfo.FullName);
						infos.Add(info);
					}
					catch (Exception exception)
					{
						stringBuilder.AppendLine("Папка: " + releaseInfo.FullName + " => " + exception.ToString());
						stringBuilder.AppendLine();
					}
				}

				var currentErrorMessage = stringBuilder.ToString();
				if (String.IsNullOrEmpty(currentErrorMessage))
					_lastVersionErrorMessage = null;
				else
					if (!currentErrorMessage.Equals(_lastVersionErrorMessage))
					{
						_logger.ErrorFormat("При разборе версий возникли ошибки:\r\n{0}", currentErrorMessage);
						_lastVersionErrorMessage = currentErrorMessage;
					}
			}

			return infos;
		}

	}
}