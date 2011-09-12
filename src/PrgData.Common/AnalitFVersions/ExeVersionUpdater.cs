using System.Collections.Generic;
using System.Linq;
using PrgData.Common.Model;

namespace PrgData.Common.AnalitFVersions
{
	public class ExeVersionUpdater
	{
		public List<VersionInfo> VersionInfos;

		public IList<AnalitFVersionRule> Rules;

		public ExeVersionUpdater(List<VersionInfo> infos, IList<AnalitFVersionRule> rules)
		{
			VersionInfos = infos;
			Rules = rules;
		}

		public VersionInfo GetVersionInfo(uint buildNumber, uint? targetVersion)
		{
			var rules = Rules
				.Where(rule => rule.SourceVersion == buildNumber && (!targetVersion.HasValue || rule.DestinationVersion <= targetVersion))
				.OrderByDescending(r => r.DestinationVersion);

			foreach (var rule in rules)
			{
				var info = VersionInfos.FirstOrDefault(item => item.VersionNumber == rule.DestinationVersion);
				if (info != null)
					return info;
			}

			return null;
		}
	}
}