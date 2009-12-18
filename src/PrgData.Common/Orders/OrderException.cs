using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PrgData.Common.Orders
{
	public class OrderException : Exception
	{
		public OrderException(string message) :
			base(message)
		{ }
	}

	public class OrderUpdateException : OrderException
	{
		public bool ErrorFlag { get; private set; }
		public int UpdateType { get; private set; }
		public string MessageHeader { get; private set; }
		public string MessageDescription { get; private set; }

		public OrderUpdateException(
			bool errorFlag,
			int updateType, 
			string messageHeader,
			string messageDescription) : 
			base("Ошибка при UpdateType.")
		{
			ErrorFlag = errorFlag;
			UpdateType = updateType;
			MessageHeader = messageHeader;
			MessageDescription = messageDescription;
		}
	}
}
