using System;
using Common.Tools;

namespace PrgData.Common.Orders.MinOrders
{
	public class ReorderingPeriod : IReorderingPeriod
	{
		public DateTime StartTime { get; private set; }
		public DateTime EndTime { get; private set; }

		public ReorderingPeriod(DateTime startTime, DateTime endTime)
		{
			if (endTime.CompareTo(startTime) > 0) {
				StartTime = startTime;
				EndTime = endTime;
			}
			else
				throw new ArgumentOutOfRangeException("endTime", endTime, "Дата окончания должна быть больше даты начала {0}".Format(startTime));
		}
	}
}