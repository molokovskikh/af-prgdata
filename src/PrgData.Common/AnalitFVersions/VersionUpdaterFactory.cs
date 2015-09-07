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
			var infos = new List<VersionInfo>();

			var dirInfo = new DirectoryInfo(Path.Combine(ServiceContext.GetResultPath(), "Updates"));

			if (dirInfo.Exists) {
				var releaseInfos = dirInfo.GetDirectories("Release*");

				foreach (var releaseInfo in releaseInfos) {
					try {
						var info = new VersionInfo(releaseInfo.FullName);
						infos.Add(info);
					}
					catch (Exception e) {
						_logger.Error("При разборе версий возникли ошибки", e);
					}
				}
			}

			return infos;
		}
	}
}