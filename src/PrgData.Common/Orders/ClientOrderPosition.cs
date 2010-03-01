using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PrgData.Common.Orders
{
	public enum PositionSendResult
	{
		Success = -1,
		NotExists = 0,
		DifferentCost = 1,
		DifferentQuantity = 2,
		DifferentCostAndQuantity = 3,
	}

	public class ClientOrderPosition
	{
		public ulong ClientPositionID { get; set; }
		public ulong ClientServerCoreID { get; set; }
		public ulong ProductID { get; set; }
		public ulong? CodeFirmCr { get; set; }
		public ulong SynonymCode { get; set; }
		public ulong? SynonymFirmCrCode { get; set; }
		public string Code { get; set; }
		public string CodeCr { get; set; }
		public bool Junk { get; set; }
		public bool Await { get; set; }
		public ushort? RequestRatio { get; set; }
		public decimal? OrderCost { get; set; }
		public ulong? MinOrderCount { get; set; }
		public ushort Quantity { get; set; }
		public decimal Cost { get; set; }
		public decimal? MinCost { get; set; }
		public ulong? MinPriceCode { get; set; }
		public decimal? LeaderMinCost { get; set; }
		public ulong? LeaderMinPriceCode { get; set; }

		public decimal? SupplierPriceMarkup { get; set; }

		public string CoreQuantity { get; set; }
		public string Unit { get; set; }
		public string Volume { get; set; }
		public string Note { get; set; }
		public string Period { get; set; }
		public string Doc { get; set; }
		public decimal? RegistryCost { get; set; }
		public bool VitallyImportant { get; set; }

		public PositionSendResult SendResult { get; set; }

		public decimal ServerCost { get; set; }

		public ushort ServerQuantity { get; set; }

		public bool Duplicated { get; set; }

		public string GetResultToClient()
		{
			return String.Format(
				"ClientPositionID={0};DropReason={1};ServerCost={2};ServerQuantity={3}", 
				ClientPositionID,
				Convert.ToInt32(SendResult),
				ServerCost,
				ServerQuantity);
		}

		public void ClearBeforPost()
		{
			SendResult = PositionSendResult.Success;
			ServerCost = 0;
			ServerQuantity = 0;
			Duplicated = false;
		}

		public string GetFilterForDuplicatedOrder()
		{
			return String.Format(@"
(ProductId = {0})
and (SynonymCode = {1})
and (SynonymFirmCrCode {2})
and (Code = '{3}')
and (CodeCr = '{4}')
and (Junk = {5})
and (Await = {6})",
				  ProductID,
				  SynonymCode,
				  SynonymFirmCrCode.HasValue ? " = " + SynonymFirmCrCode.ToString() : "is Null",
				  Code,
				  CodeCr,
				  Junk ? "True" : "False",
				  Await ? "True" : "False"
				  );
		}

		public string GetFilter()
		{
			return GetFilterForDuplicatedOrder() +
				String.Format(@"
and (RequestRatio {0})
and (OrderCost {1})
and (MinOrderCount {2})",
				  RequestRatio.HasValue ? " = " + RequestRatio.ToString() : "is Null",
				  OrderCost.HasValue ? " = " + OrderCost.ToString() : "is Null",
				  MinOrderCount.HasValue ? " = " + MinOrderCount.ToString() : "is Null"
				  );
		}
	}
}
