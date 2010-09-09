using System;
using System.Collections.Generic;
using System.Linq;
using Common.Models;
using NHibernate;
using NHibernate.Mapping.Attributes;

namespace PrgData.Common.Orders
{
	public enum OrderSendResult
	{		
		Success = 0,
		LessThanMinReq = 1,
		NeedCorrect = 2
	}

	public class ClientOrderHeader //: Order
	{
		public bool FullDuplicated { get; set; }

		public OrderSendResult SendResult { get; set; }

		public List<ClientOrderPosition> Positions { get; set; }

		public ulong ServerOrderId { get; set; }

		public string ErrorReason { get; set; }

		public uint? MinReq { get; set; }

		public Order Order;

		public ClientOrderHeader()
			: base()
		{
			ClearOnCreate();
		}

		public string GetResultToClient()
		{
			var result = String.Format(
				"ClientOrderID={0};PostResult={1};ServerOrderId={2};ErrorReason={3};ServerMinReq={4}",
				Order.ClientOrderId, 
				Convert.ToInt32(SendResult),
				ServerOrderId,
				ErrorReason,
				MinReq);

			if (SendResult == OrderSendResult.NeedCorrect)
			{
				foreach (var position in Positions)
				{
					if ((position.SendResult != PositionSendResult.Success) && !position.Duplicated)
						result += ";" + position.GetResultToClient();
				}
			}

			return result;
		}

		private void ClearOnCreate()
		{
			this.Positions = new List<ClientOrderPosition>();
			SendResult = OrderSendResult.Success;
			ServerOrderId = 0;
			ErrorReason = null;
			FullDuplicated = false;
		}

		public uint GetSavedRowCount()
		{
			return Convert.ToUInt32( Positions.Count((item) => { return !item.Duplicated; }));
		}

		public void PrepareBeforPost(ISession session)
		{
			if (SendResult == OrderSendResult.Success && !FullDuplicated)
			{
				ServerOrderId = 0;
				Positions.ForEach(position => position.PrepareBeforPost(session));
			}
		}
	}
}
