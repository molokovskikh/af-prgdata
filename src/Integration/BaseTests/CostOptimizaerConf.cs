using System;
using MySql.Data.MySqlClient;
using PrgData.Common;
using Test.Support;

namespace Integration.BaseTests
{
	public class CostOptimizaerConf
	{
		public uint OptimizationSupplierId = 5;
		public uint ConcurentSupplierId = 14;
		public uint OptimizationPriceId;

		public static CostOptimizaerConf MakeUserOptimazible(TestUser user, uint supplierId = 0)
		{
			var conf = new CostOptimizaerConf();
			if (supplierId != 0)
				conf.OptimizationSupplierId = supplierId;

			using (var connection = new MySqlConnection(Settings.ConnectionString())) {
				connection.Open();

				MySqlHelper.ExecuteNonQuery(
					connection,
					"call Customers.GetPrices(?UserId)",
					new MySqlParameter("?UserId", user.Id));

				conf.OptimizationPriceId = Convert.ToUInt32(MySqlHelper.ExecuteScalar(
					connection,
					@"
select
	Prices.PriceCode, count(*)
from
	Prices
	inner join farm.Core0 c on c.PriceCode = Prices.PriceCode
where
	Prices.FirmCode = ?OptimizationSupplierId
group by Prices.PriceCode
order by 2 desc",
					new MySqlParameter("?OptimizationSupplierId", conf.OptimizationSupplierId)));

				MySqlHelper.ExecuteScalar(
					connection,
					@"
insert into usersettings.costoptimizationrules (SupplierId, MinAmount, MinPercent, MaxPercent, MinDelta, MaxDelta) 
values (?OptimizationSupplierId, 1, 0.8, 23, 0.2, 0.7);
set @LastRuleId = last_insert_id();
insert into usersettings.costoptimizationconcurrents (RuleId, SupplierId) values (@LastRuleId, ?ConcurentSupplierId);
insert into usersettings.costoptimizationclients (RuleId, ClientId) values (@LastRuleId, ?NewClientId);
",
					new MySqlParameter("?OptimizationSupplierId", conf.OptimizationSupplierId),
					new MySqlParameter("?ConcurentSupplierId", conf.ConcurentSupplierId),
					new MySqlParameter("?NewClientId", user.Client.Id));
			}
			return conf;
		}
	}
}