using Common.Models;

namespace PrgData.Common.Orders
{
	public class UnconfirmedOrderInfo
	{
		public uint OrderId { get; set; }

		public uint? ClientOrderId { get; set; }

		public Order Order { get; set; }

		public UnconfirmedOrderInfo(Order order)
		{
			Order = order;
			OrderId = order.RowId;
		}

		public UnconfirmedOrderInfo(uint orderId, uint? clientOrderId)
		{
			OrderId = orderId;
			ClientOrderId = clientOrderId;
		}
	}
}