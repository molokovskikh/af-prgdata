using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Common.Models;
using NHibernate;
using NHibernate.Mapping.Attributes;

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

		public OrderItem OrderPosition { get; set; }
		public Offer Offer { get; set; }
		public OrderItemLeadersInfo LeaderInfo { get; set; }

		public PositionSendResult SendResult { get; set; }
		public float ServerCost { get; set; }
		public uint ServerQuantity { get; set; }
		public bool Duplicated { get; set; }

		public uint OrderedQuantity { get; set; }
		public float? SupplierPriceMarkup { get; set; }
		public float? RetailMarkup { get; set; }
		public ushort? NDS { get; set; }

		public ClientOrderPosition()
		{
			ClearOnCreate();
		}

		public string GetResultToClient()
		{
			return String.Format(
				"ClientPositionID={0};DropReason={1};ServerCost={2};ServerQuantity={3}", 
				ClientPositionID,
				Convert.ToInt32(SendResult),
				ServerCost.ToString(CultureInfo.InvariantCulture.NumberFormat),
				ServerQuantity);
		}

		public void ClearOnCreate()
		{
			SendResult = PositionSendResult.Success;
			ServerCost = 0;
			ServerQuantity = 0;
			Duplicated = false;
			OrderPosition = null;
		}

		public string GetFilterForDuplicatedOrder()
		{
			return String.Format(@"
(ProductId = {0})
and (SynonymCode {1})
and (SynonymFirmCrCode {2})
and (Code = '{3}')
and (CodeCr = '{4}')
and (Junk = {5})
and (Await = {6})",
				  OrderPosition.ProductId,
				  OrderPosition.SynonymCode.HasValue ? " = " + OrderPosition.SynonymCode.ToString() : "is Null",
				  OrderPosition.SynonymFirmCrCode.HasValue ? " = " + OrderPosition.SynonymFirmCrCode.ToString() : "is Null",
				  OrderPosition.Code,
				  OrderPosition.CodeCr,
				  OrderPosition.Junk ? "True" : "False",
				  OrderPosition.Await ? "True" : "False"
				  );
		}

		public string GetFilter()
		{
			return GetFilterForDuplicatedOrder() +
				String.Format(@"
and (RequestRatio {0})
and (OrderCost {1})
and (MinOrderCount {2})",
				  OrderPosition.RequestRatio.HasValue ? " = " + OrderPosition.RequestRatio.ToString() : "is Null",
				  OrderPosition.OrderCost.HasValue ? " = " + OrderPosition.OrderCost.ToString() : "is Null",
				  OrderPosition.MinOrderCount.HasValue ? " = " + OrderPosition.MinOrderCount.ToString() : "is Null"
				  );
		}

		internal void PrepareBeforPost(ISession session)
		{
			if (!Duplicated)
			{
				if (OrderPosition.SynonymCode.HasValue)
				{
					var synonymCodeFromDb = session
						.CreateSQLQuery(
							@"
 SELECT 
        syn.synonymcode
 FROM   
    farm.synonymarchive syn
 WHERE  syn.synonymcode = :SynonymCode")
						.SetParameter("SynonymCode", OrderPosition.SynonymCode)
						.UniqueResult<uint?>();
					OrderPosition.SynonymCode = synonymCodeFromDb;
				}

				if (OrderPosition.SynonymFirmCrCode.HasValue)
				{
					var synonymFirmCrCodeFromDb = session
						.CreateSQLQuery(
							@"
 SELECT 
        sfcr.SynonymFirmCrCode
 FROM   
    farm.synonymfirmcr sfcr
 WHERE  sfcr.SynonymFirmCrCode = :SynonymFirmCrCode")
						.SetParameter("SynonymFirmCrCode", OrderPosition.SynonymFirmCrCode)
						.UniqueResult<uint?>();
					OrderPosition.SynonymFirmCrCode = synonymFirmCrCodeFromDb;
				}

				if (OrderPosition.CodeFirmCr.HasValue)
				{
					var codeFirmCrFromDb = session
						.CreateSQLQuery(
							@"
 SELECT 
        IF(Prod.Id IS NULL, sfcr.codefirmcr, Prod.Id) as CodeFirmCr
 FROM   
    catalogs.products
	LEFT JOIN farm.synonymfirmcr sfcr
	ON     sfcr.SynonymFirmCrCode = :SynonymFirmCrCode    
	LEFT JOIN catalogs.Producers Prod
	ON     Prod.Id = :CodeFirmCr
 WHERE  products.ID = :ProductID")
						.SetParameter("ProductID", OrderPosition.ProductId)
						.SetParameter("SynonymFirmCrCode", OrderPosition.SynonymFirmCrCode)
						.SetParameter("CodeFirmCr", OrderPosition.CodeFirmCr)
						.UniqueResult<uint?>();
					OrderPosition.CodeFirmCr = codeFirmCrFromDb;
				}
			}
		}
	}
}
