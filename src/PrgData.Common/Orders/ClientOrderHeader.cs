using System;
using System.Collections.Generic;
using System.Linq;
using Common.Models;

namespace PrgData.Common.Orders
{
	public enum OrderSendResult
	{
		Success = 0,
		LessThanMinReq = 1,
		NeedCorrect = 2,
		GreateThanMaxOrderSum = 3,
	}

	public class ClientOrderHeader
	{
		public bool FullDuplicated { get; set; }

		public OrderSendResult SendResult { get; set; }

		public List<ClientOrderPosition> Positions { get; set; }

		public ulong ServerOrderId { get; set; }

		public string ErrorReason { get; set; }

		public uint? MinReq { get; set; }

		public decimal MaxSum { get; set; }

		public Order Order { get; set; }

		public ActivePrice ActivePrice { get; set; }

		public string ClientAddition { get; set; }

		public uint? ClientOrderId { get; set; }

		public decimal VitallyImportantDelayOfPayment { get; set; }

		public ClientOrderHeader()
		{
			Positions = new List<ClientOrderPosition>();
			ClearOnCreate();
		}

		public string GetResultToClient(uint? buildNumber)
		{
			var postResult = Convert.ToInt32(SendResult);
			//analitf ничего не знает о коде ошибки 3, поэтому передаем понятный код ошибки
			if (postResult == 3)
				postResult = 2;
			var result = String.Format(
				"ClientOrderID={0};PostResult={1};ServerOrderId={2};ErrorReason={3};ServerMinReq={4}{5}",
				Order.ClientOrderId, 
				postResult,
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
			if (SendResult == OrderSendResult.LessThanMinReq) {
				return String.Format(
					"Заказ №{0} на сумму {1} на поставщика {2} был отклонен из-за нарушения минимальной суммы заказа {3}.",
					ClientOrderId,
					Order.CalculateSum(),
					ActivePrice.Id.Price.Supplier.Name,
					MinReq);
			}

			if (SendResult == OrderSendResult.GreateThanMaxOrderSum) {
				return String.Format(
					"Заказ №{0} на сумму {1} на поставщика {2} был отклонен из-за нарушения максимальной суммы заказов {3}.",
					ClientOrderId,
					Order.CalculateSum(),
					ActivePrice.Id.Price.Supplier.Name,
					MaxSum);
			}
			return "";
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
			return Convert.ToUInt32( Positions.Count(item => !item.Duplicated));
		}
	}
}
