using System.Collections.Generic;
using PrgData.Common.Models;

namespace PrgData.Common.Repositories
{
	public interface IVersionRuleRepository
	{
		List<AnalitFVersionRule> FindAllRules();
	}
}