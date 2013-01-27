using System;

namespace PrgData.Common.Orders.MinOrders
{
	public interface IReorderingPeriod
	{
		DateTime StartTime { get; }
		DateTime EndTime { get; }
	}
}