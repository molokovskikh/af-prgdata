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

	public class ClientOrderHeader
	{
		public bool FullDuplicated { get; set; }

		public OrderSendResult SendResult { get; set; }

		public List<ClientOrderPosition> Positions { get; set; }

		public ulong ServerOrderId { get; set; }

		public string ErrorReason { get; set; }

		public uint? MinReq { get; set; }


		public Order Order { get; set; }

		public ActivePrice ActivePrice { get; set; }

		public string ClientAddition { get; set; }

		public uint? ClientOrderId { get; set; }

		public ClientOrderHeader()
		{
			Positions = new List<ClientOrderPosition>();
			ClearOnCreate();
		}

		public string GetResultToClient(int? buildNumber)
		{
			var result = String.Format(
				"ClientOrderID={0};PostResult={1};ServerOrderId={2};ErrorReason={3};ServerMinReq={4}{5}",
				Order.ClientOrderId, 
				Convert.ToInt32(SendResult),
				ServerOrderId,
				ErrorReason,
				MinReq,
				buildNumber > 1271 ? ";SendDate=" + Order.WriteTime.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss") : "");

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

		public string GetResultToAddition()
		{
			if (SendResult != OrderSendResult.LessThanMinReq)
				return null;

			return String.Format(
				"Заказ №{0} на сумму {1} на поставщика {2} был отклонен из-за нарушения минимальной суммы заказа {3}.",
				ClientOrderId,
				Order.CalculateSum(),
				ActivePrice.Id.Price.Firm.ShortName,
				MinReq);
		}

		public void ClearOnCreate()
		{
			SendResult = OrderSendResult.Success;
			ServerOrderId = 0;
			ErrorReason = null;
			FullDuplicated = false;
			Order = null;
			Positions.ForEach(item => item.ClearOnCreate());
		}

		public uint GetSavedRowCount()
		{
			return Convert.ToUInt32( Positions.Count((item) => { return !item.Duplicated; }));
		}
	}
}
