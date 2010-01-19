﻿using System;
using System.Collections.Generic;
using System.Linq;

namespace PrgData.Common.Orders
{
	public enum OrderSendResult
	{		
		Unknown = -1,
		Success = 0,
		LessThanMinReq = 1,
		NeedCorrect = 2
	}

	public class ClientOrderHeader
	{
		public ulong ClientOrderId { get; set; }
		public ulong PriceCode { get; set; }
		public ulong RegionCode { get; set; }
		public DateTime PriceDate { get; set; }
		public string ClientAddition { get; set; }
		public ushort RowCount { get; set; }

		public bool FullDuplicated { get; set; }

		public OrderSendResult SendResult { get; set; }

		public List<ClientOrderPosition> Positions { get; set; }

		public ulong ServerOrderId { get; set; }

		public string ErrorReason { get; set; }

		public uint? MinReq { get; set; }

		public ClientOrderHeader()
		{
			this.Positions = new List<ClientOrderPosition>();
		}

		public string GetResultToClient()
		{
			var result = String.Format(
				"ClientOrderID={0};PostResult={1};ServerOrderId={2};ErrorReason={3};ServerMinReq={4}",
				ClientOrderId, 
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

		public void ClearBeforPost()
		{
			SendResult = OrderSendResult.Success;
			ServerOrderId = 0;
			ErrorReason = null;
			FullDuplicated = false;
			Positions.ForEach((item) => {item.ClearBeforPost();});
		}

		public decimal GetSumOrder()
		{ 
			decimal result = 0;
			Positions.ForEach((item) => { result += item.Quantity * item.Cost; });
			return result;
		}

		public uint GetSavedRowCount()
		{
			return Convert.ToUInt32( Positions.Count((item) => { return !item.Duplicated; }));
		}
	}
}