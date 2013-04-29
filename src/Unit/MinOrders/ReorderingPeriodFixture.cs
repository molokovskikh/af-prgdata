using System;
using NUnit.Framework;
using PrgData.Common.Orders.MinOrders;

namespace Unit.MinOrders
{
	[TestFixture]
	public class ReorderingPeriodFixture
	{
		[Test(Description = "создаем период с некорректными датами")]
		public void InvalidReorderingPeriod()
		{
			var date = DateTime.Now;
			var exception = Assert.Throws<ArgumentOutOfRangeException>(() => new ReorderingPeriod(date, date));
			Assert.That(exception.Message, Is.StringStarting("Дата окончания должна быть больше даты начала"));
		}
	}
}