using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Test.Support;
using Test.Support.Suppliers;

namespace Integration
{
	[TestFixture]
	public class MakeCostNoLessFixture : IntegrationFixture
	{
		private TestSupplier _supplier;

		[SetUp]
		public void SetUp()
		{
			_supplier = TestSupplier.Create();
		}

		[Test]
		public void CalculateTest()
		{
			var queryString = String.Format(@"
insert into usersettings.costoptimizationrules (SupplierId, MinAmount, MinPercent, MaxPercent, MinDelta, MaxDelta) 
values ({0}, 1, 0.8, 23, 0.2, 0.7);
set @LastRuleId = last_insert_id();
drop temporary table IF EXISTS usersettings.TmpData;
CREATE temporary table usersettings.TmpData(
  Cost1 decimal(12,6),
  Cost2 decimal(12,6)
  ) engine=MEMORY;
  
  insert into usersettings.TmpData values(10, 20);  -- меньше рублевого порога
  insert into usersettings.TmpData values(99, 100); -- считаем
  insert into usersettings.TmpData values(50, 100); -- больше крайнего порога в 23%
  insert into usersettings.TmpData values(900, 1000); -- считаем
  insert into usersettings.TmpData values(950, 1000); -- считаем
  insert into usersettings.TmpData values(999, 1000); -- меньше меньшего порога в 0.8%
  insert into usersettings.TmpData values(38, 40); -- считаем
  insert into usersettings.TmpData values(51, 55); -- считаем
", _supplier.Id);
			for (int i = 0; i < 100; i++) {
				queryString += @"insert into usersettings.TmpData values(90, 100); -- считаем
";
			}
			queryString += @"select Cost1, Cost2, MakeCostNoLess(Cost1, Cost2, @LastRuleId)  from usersettings.TmpData;
";
			var query = session.CreateSQLQuery(queryString);
			var result = query.List<object[]>();

			foreach (var obj in result) {
				var selfCost = (decimal)obj[0];
				var opponentCost = (decimal)obj[1];
				var resultCost = (decimal)obj[2];
				var diff = ((selfCost - opponentCost) * 100) / opponentCost;
				if (selfCost <= 30 || diff > -0.8m || diff < -23) {
					// если цена не больше 30 р. или не укладывается в порог, то результат==исходной
					Assert.That(resultCost, Is.EqualTo(selfCost));
				}
				else {
					// иначе разница должна укладываться в промежуток 0.2-0.7 процентов от цены конкурента
					var resultDiff = (opponentCost - resultCost) / opponentCost * 100;
					Assert.That(resultDiff, Is.GreaterThanOrEqualTo(0.2));
					Assert.That(resultDiff, Is.LessThanOrEqualTo(0.7));
				}
			}
		}
	}
}