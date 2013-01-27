using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Common.Models;
using Common.MySql;

namespace PrgData.Common.Orders
{
	public class Orders2StringConverter
	{
		public StringBuilder OrderHead;
		public StringBuilder OrderItems;

		public Orders2StringConverter(List<UnconfirmedOrderInfo> orderInfos, uint maxOrderListId, bool exportSendDate)
		{
			OrderHead = new StringBuilder();
			OrderItems = new StringBuilder();

			foreach (var orderInfo in orderInfos) {
				OrderHead.AppendFormat(
					"{0}\t{1}\t{2}\t{3}{4}{5}\n",
					orderInfo.ClientOrderId,
					orderInfo.Order.AddressId.Value,
					orderInfo.Order.PriceList.PriceCode,
					orderInfo.Order.RegionCode,
					exportSendDate ? string.Format("\t{0}", orderInfo.Order.WriteTime.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss")) : "",
					exportSendDate ? string.Format("\t{0}", orderInfo.Order.ClientAddition.ToMySqlExportString()) : "");
				foreach (var item in orderInfo.Order.OrderItems) {
					item.RowId = maxOrderListId;
					maxOrderListId++;

					OrderItems.AppendFormat(
						"{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}\t{13}\t{14}\t{15}\t{16}\t{17}\t{18}\t{19}\n",
						item.RowId,
						orderInfo.ClientOrderId,
						orderInfo.Order.AddressId.Value,
						item.CoreId,
						item.ProductId,
						item.CodeFirmCr.HasValue ? item.CodeFirmCr.Value.ToString() : "\\N",
						item.SynonymCode.HasValue ? item.SynonymCode.Value.ToString() : "\\N",
						item.SynonymFirmCrCode.HasValue ? item.SynonymFirmCrCode.Value.ToString() : "\\N",
						item.Code,
						item.CodeCr,
						item.Cost.ToString(CultureInfo.InvariantCulture.NumberFormat),
						item.CostWithDelayOfPayment.ToString(CultureInfo.InvariantCulture.NumberFormat),
						item.Await ? "1" : "0",
						item.Junk ? "1" : "0",
						item.Quantity,
						item.RequestRatio.HasValue ? item.RequestRatio.Value.ToString() : "\\N",
						item.OrderCost.HasValue ? item.OrderCost.Value.ToString(CultureInfo.InvariantCulture.NumberFormat) : "\\N",
						item.MinOrderCount.HasValue ? item.MinOrderCount.Value.ToString() : "\\N",
						item.OfferInfo.Period,
						item.OfferInfo.ProducerCost.HasValue ? item.OfferInfo.ProducerCost.Value.ToString(CultureInfo.InvariantCulture.NumberFormat) : "\\N");
				}
			}
		}
	}
}