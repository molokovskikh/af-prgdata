using System;

namespace PrgData.Common.Orders
{
	public class OrderException : Exception
	{
		public OrderException(string message) :
			base(message)
		{ }
	}
}
