using System.Collections.Generic;
using PrgData.Common.Model;

namespace PrgData.Common.Repositories
{
	public interface IVersionRuleRepository
	{
		List<AnalitFVersionRule> FindAllRules();
	}
}