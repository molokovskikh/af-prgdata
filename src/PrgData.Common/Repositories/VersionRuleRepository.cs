using System.Linq;
using System.Collections.Generic;
using Common.Models.Repositories;
using PrgData.Common.Models;

namespace PrgData.Common.Repositories
{
	public class VersionRuleRepository : BaseRepository, IVersionRuleRepository
	{

		public const string SelectAllRules = @"
SELECT
	r.Id as {AnalitFVersionRule.Id}, 
	r.SourceVersion as {AnalitFVersionRule.SourceVersion}, 
	r.DestinationVersion as {AnalitFVersionRule.DestinationVersion}
from
  UserSettings.AnalitFVersionRules r
order by r.SourceVersion, r.DestinationVersion";

		public List<AnalitFVersionRule> FindAllRules()
		{
			var rules = CurrentSession.CreateSQLQuery(SelectAllRules)
				.AddEntity("AnalitFVersionRule", typeof(AnalitFVersionRule))
				.List<AnalitFVersionRule>();
			return rules.ToList();
		}
	}
}