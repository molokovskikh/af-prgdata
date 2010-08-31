using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using Common.MySql;
using System.Data;
using log4net;
using System.IO;
using Common.Models;
using SmartOrderFactory;
using Common.Models.Repositories;
using SmartOrderFactory.Repositories;

namespace PrgData.Common.Orders
{
	public class ReorderHelper : OrderHelper
	{
		private bool _forceSend;
		private bool _useCorrectOrders;
		//Это в старой системе код клиента, а в новой системе код адреса доставки
		private uint _orderedClientCode;
		private List<ClientOrderHeader> _orders = new List<ClientOrderHeader>();

		private bool _calculateLeaders;
		private int? _appVersion;

		public ReorderHelper(
			UpdateData data, 
			MySqlConnection readWriteConnection, 
			bool forceSend,
			uint orderedClientCode,
			bool useCorrectOrders,
			int? appVersion) :
			base(data, readWriteConnection)
		{
			_forceSend = forceSend;
			_orderedClientCode = orderedClientCode;
			_useCorrectOrders = useCorrectOrders;
			_appVersion = appVersion;
		}

		public string PostSomeOrders()
		{
			CheckCanPostOrder();

			CheckWeeklySumOrder();

			CheckImpersonalPrice();

			InternalSendOrders();

			return GetOrdersResult();
		}

		private void CheckImpersonalPrice()
		{
			if (_data.EnableImpersonalPrice)
				_orders.ForEach(order =>
					order.Positions.ForEach(position =>
					{
						position.SynonymCode = null;
						position.SynonymFirmCrCode = null;
					}));
		}

		private void InternalSendOrders()
		{
			//Сбрасываем перед заказом
			_orders.ForEach((item) => { item.ClearBeforPost(); });

			//Получить значение флага "Расчитывать лидеров"
			_calculateLeaders = GetCalculateLeaders();

			//делаем проверки минимального заказа
			CheckOrdersByMinRequest();

			//делаем проверку на дублирующиеся заказы
			CheckDuplicatedOrders();

			if (_useCorrectOrders && !_forceSend && AllOrdersIsSuccess())
				//делаем сравнение с существующим прайсом
				CheckWithExistsPrices();

			if (!_useCorrectOrders || AllOrdersIsSuccess())
				global::Common.MySql.With.DeadlockWraper(() =>
				{
					var transaction = _readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
					try
					{
						//Сбрасываем ServerOrderId перед заказом только у заказов, 
						//которые не являются полностью дублированными
						_orders.ForEach((item) =>
						{
							if ((item.SendResult == OrderSendResult.Success) && !item.FullDuplicated) 
								item.ServerOrderId = 0; 
						});

						//сохраняем сами заявки в базу
						SaveOrders();

#if DEBUG
						if ((_data.ClientId == 1349) || (_data.ClientId == 10005))
							GenerateDocsHelper.GenerateDocs(_readWriteConnection, _data, _orders.FindAll(item => item.SendResult == OrderSendResult.Success));
#endif

						UpdateHelper.InsertAnalitFUpdatesLog(transaction.Connection, _data, RequestType.SendOrders, null, _appVersion);

						transaction.Commit();
					}
					catch
					{
						try
						{
							transaction.Rollback();
						}
						catch (Exception rollbackException)
						{
							ILog _logger = LogManager.GetLogger(this.GetType());
							_logger.Error("Ошибка при rollback'е транзакции сохранения заказов", rollbackException);
						}
						throw;
					}
				});
		}

		private List<Offer> GetOffers(List<uint> productIds)
		{
			var offersRepository = IoC.Resolve<ISmartOfferRepository>();
			IOrderable Orderable;
			if (_data.IsFutureClient)
			{
				Orderable = IoC.Resolve<IRepository<User>>().Get(_data.UserId);
			}
			else
				Orderable = IoC.Resolve<IRepository<Client>>().Get(_data.ClientId);

			return offersRepository.GetByProductIds(Orderable, productIds).ToList();
		}

		private List<uint> GetSearchedProductIds()
		{
			var productIds = new List<uint>();
			foreach (var order in _orders)
			{
				foreach (var position in order.Positions)
				{
					if (!position.Duplicated)
						productIds.Add(Convert.ToUInt32(position.ProductID));
				}
			}

			return productIds;
		}

		private void CheckWithExistsPrices()
		{
			var productIds = GetSearchedProductIds();

			if (productIds.Count > 0)
			{
				var offers = GetOffers(productIds);

				foreach (var order in _orders)
				{
					foreach (var position in order.Positions)
					{
						if (!position.Duplicated)
						{
							var offer = GetDataRowByPosition(offers, order, position);
							if (offer == null)
								position.SendResult = PositionSendResult.NotExists;
							else
								CheckExistsCorePosition(offer, position);
						}
					}

					if (order.Positions.Any((item) => { return item.SendResult != PositionSendResult.Success; }))
						order.SendResult = OrderSendResult.NeedCorrect;
				}
			}
		}

		private void CheckExistsCorePosition(Offer offer, ClientOrderPosition position)
		{
			uint? serverQuantity = null;

			if (offer.Quantity.HasValue)
			{
				serverQuantity = offer.Quantity;
			}

			if (!position.Cost.Equals(Convert.ToDecimal(offer.Cost)))
			{
				position.SendResult = PositionSendResult.DifferentCost;
				position.ServerCost = Convert.ToDecimal(offer.Cost);
				if (serverQuantity.HasValue)
					position.ServerQuantity = serverQuantity.Value;
			}

			if (serverQuantity.HasValue && (serverQuantity.Value < position.Quantity))
			{
				//Если имеется различие по цене, то говорим, что есть различие по цене и кол-ву
				if (position.SendResult == PositionSendResult.DifferentCost)
					position.SendResult = PositionSendResult.DifferentCostAndQuantity;
				else
				{
					position.SendResult = PositionSendResult.DifferentQuantity;
					position.ServerCost = Convert.ToDecimal(offer.Cost);
					position.ServerQuantity = serverQuantity.Value;
				}
			}
		}

		private Offer GetDataRowByPosition(List<Offer> offers, ClientOrderHeader order, ClientOrderPosition position)
		{
			var clientServerCoreIdOffers = offers.FindAll(item => { 
				return 
					order.PriceCode.Equals(Convert.ToUInt64(item.PriceList.Id.Price.PriceCode)) &&
					order.RegionCode.Equals(item.PriceList.Id.RegionCode) &&
					item.Id.ToString().EndsWith(position.ClientServerCoreID.ToString()); });

			if (clientServerCoreIdOffers.Count == 1)
				return clientServerCoreIdOffers[0];
			else
				if (clientServerCoreIdOffers.Count == 0)
				{
					var filterOffers = offers.FindAll(item => 
					{ 
						return
							order.PriceCode.Equals(Convert.ToUInt64(item.PriceList.Id.Price.PriceCode)) &&
							order.RegionCode.Equals(item.PriceList.Id.RegionCode) &&
							item.ProductId == position.ProductID &&
							item.SynonymCode == position.SynonymCode &&
							item.SynonymFirmCrCode == position.SynonymFirmCrCode &&
							item.Code == position.Code &&
							item.CodeCr == position.CodeCr &&
							item.Junk == position.Junk &&
							item.Await == position.Await &&
							item.RequestRatio == position.RequestRatio &&
							position.OrderCost.Equals(item.OrderCost.HasValue ? (decimal?)Convert.ToDecimal(item.OrderCost.Value) : (decimal?)null) &&
							item.MinOrderCount == position.MinOrderCount;						 
					});
					if (filterOffers.Count > 0)
						return filterOffers[0];
				}
				else
					throw new OrderException(String.Format("По ID = {0} нашли больше одной позиции.", position.ClientServerCoreID));

			return null;
		}

		private void SaveOrders()
		{
			foreach (var order in _orders)
			{
				if ((order.SendResult == OrderSendResult.Success) && !order.FullDuplicated)
				{
					order.ServerOrderId = SaveOrder(
						_orderedClientCode,
						Convert.ToUInt32(order.PriceCode),
						order.RegionCode,
						order.PriceDate,
						order.GetSavedRowCount(),
						Convert.ToUInt32(order.ClientOrderId),
						order.ClientAddition,
						order.DelayOfPayment);
					foreach (var position in order.Positions)
						if (!position.Duplicated)
							SaveOrderDetail(order, position);
				}
			}
		}

		private void SaveOrderDetail(ClientOrderHeader order, ClientOrderPosition position)
		{
			var command = new MySqlCommand(@"
 INSERT
 INTO   orders.orderslist
        (
               OrderID          ,
               ProductId        ,
               CodeFirmCr       ,
               SynonymCode      ,
               SynonymFirmCrCode,
               Code             ,
               CodeCr           ,
               Quantity         ,
               Junk             ,
               Await            ,
               Cost             ,
               RequestRatio     ,
               MinOrderCount    ,
               OrderCost        ,
               SupplierPriceMarkup,
               RetailMarkup
        )
 SELECT ?OrderID                                      ,
        products.ID                                   ,
        IF(Prod.Id IS NULL, sfcr.codefirmcr, Prod.Id) ,
        syn.synonymcode                               ,
        sfcr.SynonymFirmCrCode                        ,
        ?Code                                         ,
        ?CodeCr                                       ,
        ?Quantity                                     ,
        ?Junk                                         ,
        ?Await                                        ,
        ?Cost                                         ,
        ?RequestRatio                                 ,
        ?MinOrderCount                                ,
        ?OrderCost                                    ,
        ?SupplierPriceMarkup                          ,
        ?RetailMarkup
 FROM   catalogs.products
        LEFT JOIN farm.synonymarchive syn
        ON     syn.synonymcode=?SynonymCode
        LEFT JOIN farm.synonymfirmcr sfcr
        ON     sfcr.SynonymFirmCrCode=?SynonymFirmCrCode
        LEFT JOIN catalogs.Producers Prod
        ON     Prod.Id=?CodeFirmCr
 WHERE  products.ID   =?ProductID;"
				, 
				_readWriteConnection);

			command.Parameters.Clear();
			//
			command.CommandText += "set @LastOrderDetailId = last_insert_id();";

			if (_calculateLeaders
				&& (position.MinCost.HasValue || position.LeaderMinCost.HasValue)
				&& (position.MinPriceCode.HasValue || position.LeaderMinPriceCode.HasValue))
			{
				command.CommandText += @"
insert into orders.leaders 
values (@LastOrderDetailId, nullif(?MinCost, 0), nullif(?LeaderMinCost, 0), nullif(?MinPriceCode, 0), nullif(?LeaderMinPriceCode, 0));";
				command.Parameters.AddWithValue("?MinCost", position.MinCost);
				command.Parameters.AddWithValue("?LeaderMinCost", position.LeaderMinCost);
				command.Parameters.AddWithValue("?MinPriceCode", position.MinPriceCode);
				command.Parameters.AddWithValue("?LeaderMinPriceCode", position.LeaderMinPriceCode);
			}

			command.CommandText += @"
insert into orders.OrderedOffers
(Id, Unit, Volume, Note, Period, Doc, VitallyImportant, RegistryCost, Quantity, ProducerCost, NDS) 
values (@LastOrderDetailId, ?Unit, ?Volume, ?Note, ?Period, ?Doc, ?VitallyImportant, ?RegistryCost, ?CoreQuantity, ?ProducerCost, ?NDS);";

			command.Parameters.AddWithValue("?OrderId", order.ServerOrderId);

			command.Parameters.AddWithValue("?ProductId", position.ProductID);
			command.Parameters.AddWithValue("?CodeFirmCr", position.CodeFirmCr);
			command.Parameters.AddWithValue("?SynonymCode", position.SynonymCode);
			command.Parameters.AddWithValue("?SynonymFirmCrCode", position.SynonymFirmCrCode);
			command.Parameters.AddWithValue("?Code", position.Code);
			command.Parameters.AddWithValue("?CodeCr", position.CodeCr);
			command.Parameters.AddWithValue("?Quantity", position.Quantity);
			command.Parameters.AddWithValue("?Junk", position.Junk);
			command.Parameters.AddWithValue("?Await", position.Await);
			command.Parameters.AddWithValue("?Cost", position.Cost);

			command.Parameters.AddWithValue("?RequestRatio", position.RequestRatio);
			command.Parameters.AddWithValue("?MinOrderCount", position.MinOrderCount);
			command.Parameters.AddWithValue("?OrderCost", position.OrderCost);

			command.Parameters.AddWithValue("?SupplierPriceMarkup", position.SupplierPriceMarkup);

			command.Parameters.AddWithValue("?Unit", position.Unit);
			command.Parameters.AddWithValue("?Volume", position.Volume);
			command.Parameters.AddWithValue("?Note", position.Note);
			command.Parameters.AddWithValue("?Period", position.Period);
			command.Parameters.AddWithValue("?Doc", position.Doc);
			command.Parameters.AddWithValue("?VitallyImportant", position.VitallyImportant);
			command.Parameters.AddWithValue("?RegistryCost", position.RegistryCost);
			command.Parameters.AddWithValue("?CoreQuantity", position.CoreQuantity);

			command.Parameters.AddWithValue("?RetailMarkup", position.RetailMarkup);

			command.Parameters.AddWithValue("?ProducerCost", position.ProducerCost);
			command.Parameters.AddWithValue("?NDS", position.NDS);

			command.ExecuteNonQuery();
		}

		private void CheckOrdersByMinRequest()
		{
			foreach (var order in _orders)
			{
				var minReq = GetMinReq(_orderedClientCode, order.RegionCode, Convert.ToUInt32(order.PriceCode));
				order.SendResult = OrderSendResult.Success;
				if ((minReq != null) && minReq.ControlMinReq && (minReq.MinReq > 0))
					if (order.GetSumOrder() < minReq.MinReq)
					{
						order.SendResult = OrderSendResult.LessThanMinReq;
						order.MinReq = minReq.MinReq;
						order.ErrorReason = "Поставщик отказал в приеме заказа.\n Сумма заказа меньше минимально допустимой.";
					}
			}
		}

		private bool GetCalculateLeaders()
		{
			var command = new MySqlCommand("select CalculateLeader from retclientsset where clientcode=?ClientId", _readWriteConnection);
			command.Parameters.AddWithValue("?ClientId", _data.ClientId);
			return Convert.ToBoolean(command.ExecuteScalar());
		}

		private bool AllOrdersIsSuccess()
		{
			return _orders.All((item) => { return (item.SendResult == OrderSendResult.Success); });
		}

		private string GetOrdersResult()
		{
			var result = String.Empty;

			foreach(var order in _orders)
			{
				if (order.SendResult != OrderSendResult.Unknown)
					if (String.IsNullOrEmpty(result))
						result += order.GetResultToClient();
					else
						result += ";" + order.GetResultToClient();
			}

			return result;
		}

		private void CheckCanPostOrder()
		{
			CheckCanPostOrder(_orderedClientCode);
		}

		private void CheckWeeklySumOrder()
		{
			var WeeklySumOrder = Convert.ToUInt32(MySql.Data.MySqlClient.MySqlHelper
				.ExecuteScalar(
				_readWriteConnection, @"
SELECT ROUND(IF(SUM(cost            *quantity)>RCS.MaxWeeklyOrdersSum
AND    CheCkWeeklyOrdersSum,SUM(cost*quantity), 0),0)
FROM   orders.OrdersHead Oh,
       orders.OrdersList Ol,
       RetClientsSet RCS
WHERE  WriteTime               >curdate() - interval dayofweek(curdate())-2 DAY
AND    Oh.RowId                =ol.OrderId
AND    RCS.ClientCode          =oh.ClientCode
AND    RCS.CheCkWeeklyOrdersSum=1
AND    RCS.clientcode          = ?ClientCode"
					,
					new MySqlParameter("?ClientCode", _data.ClientId)
											 ));
			if (WeeklySumOrder > 0)
				throw new UpdateException(
					String.Format("Превышен недельный лимит заказа (уже заказано на {0} руб).", WeeklySumOrder),
					String.Empty,
					RequestType.Forbidden);
		}

		public void ParseOrders(
			ushort orderCount,
			ulong[] clientOrderId,
			ulong[] priceCode,
			ulong[] regionCode,
			DateTime[] priceDate,
			string[] clientAddition,
			ushort[] rowCount,
			ulong[] clientPositionID,
			ulong[] clientServerCoreID,
            ulong[] productID,
            string[] codeFirmCr,
			ulong[] synonymCode,
			string[] synonymFirmCrCode,
            string[] code,
            string[] codeCr, 
            bool[] junk,
            bool[] await,
			string[] requestRatio,
			string[] orderCost,
			string[] minOrderCount,
			ushort[] quantity,
			decimal[] cost, 
            string[] minCost, 
            string[] minPriceCode,
            string[] leaderMinCost, 
            string[] leaderMinPriceCode,
			string[] supplierPriceMarkup,
			string[] delayOfPayment,
			string[] coreQuantity,
			string[] unit,
			string[] volume,
			string[] note,
			string[] period,
			string[] doc,
			string[] registryCost,
			bool[] vitallyImportant,
			string[] retailMarkup,
			string[] producerCost,
			string[] nds
			)
		{
			CheckArrayCount(orderCount, clientOrderId.Length, "clientOrderId");
			CheckArrayCount(orderCount, priceCode.Length, "priceCode");
			CheckArrayCount(orderCount, regionCode.Length, "regionCode");
			CheckArrayCount(orderCount, priceDate.Length, "priceDate");
			CheckArrayCount(orderCount, clientAddition.Length, "clientAddition");
			CheckArrayCount(orderCount, rowCount.Length, "rowCount");
			CheckArrayCount(orderCount, delayOfPayment.Length, "delayOfPayment");						

			int allPositionCount = rowCount.Sum(item => item);

			CheckArrayCount(allPositionCount, clientPositionID.Length, "clientPositionID");
			CheckArrayCount(allPositionCount, clientServerCoreID.Length, "clientServerCoreID");
			CheckArrayCount(allPositionCount, productID.Length, "productID");
			CheckArrayCount(allPositionCount, codeFirmCr.Length, "codeFirmCr");
			CheckArrayCount(allPositionCount, synonymCode.Length, "synonymCode");
			CheckArrayCount(allPositionCount, synonymFirmCrCode.Length, "synonymFirmCrCode");
			CheckArrayCount(allPositionCount, code.Length, "code");
			CheckArrayCount(allPositionCount, codeCr.Length, "codeCr");
			CheckArrayCount(allPositionCount, junk.Length, "junk");
			CheckArrayCount(allPositionCount, await.Length, "await");
			CheckArrayCount(allPositionCount, requestRatio.Length, "requestRatio");
			CheckArrayCount(allPositionCount, orderCost.Length, "orderCost");
			CheckArrayCount(allPositionCount, minOrderCount.Length, "minOrderCount");
			CheckArrayCount(allPositionCount, quantity.Length, "quantity");

			CheckArrayCount(allPositionCount, cost.Length, "cost");
			CheckArrayCount(allPositionCount, minCost.Length, "minCost");
			CheckArrayCount(allPositionCount, minPriceCode.Length, "minPriceCode");
			CheckArrayCount(allPositionCount, leaderMinCost.Length, "leaderMinCost");
			CheckArrayCount(allPositionCount, leaderMinPriceCode.Length, "leaderMinPriceCode");
			CheckArrayCount(allPositionCount, supplierPriceMarkup.Length, "supplierPriceMarkup");

			CheckArrayCount(allPositionCount, coreQuantity.Length, "coreQuantity");
			CheckArrayCount(allPositionCount, unit.Length, "unit");
			CheckArrayCount(allPositionCount, volume.Length, "volume");
			CheckArrayCount(allPositionCount, note.Length, "note");
			CheckArrayCount(allPositionCount, period.Length, "period");
			CheckArrayCount(allPositionCount, doc.Length, "doc");
			CheckArrayCount(allPositionCount, registryCost.Length, "registryCost");
			CheckArrayCount(allPositionCount, vitallyImportant.Length, "vitallyImportant");

			CheckArrayCount(allPositionCount, retailMarkup.Length, "retailMarkup");

			CheckArrayCount(allPositionCount, producerCost.Length, "producerCost");
			CheckArrayCount(allPositionCount, nds.Length, "nds");

			var detailsPosition = 0;
			for (int i = 0; i < orderCount; i++)
			{
				var clientOrder = new ClientOrderHeader()
				{
					ClientOrderId = clientOrderId[i],
					PriceCode = priceCode[i],
					RegionCode = regionCode[i],
					PriceDate = priceDate[i],
					ClientAddition = DecodedDelphiString(clientAddition[i]),
					RowCount = rowCount[i],
					DelayOfPayment =
						String.IsNullOrEmpty(delayOfPayment[i]) ? null : (decimal?)decimal
							.Parse(
								delayOfPayment[i],
								System.Globalization.NumberStyles.Currency,
								System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
				};

				_orders.Add(clientOrder);

				for (int detailIndex = detailsPosition; detailIndex < (detailsPosition + clientOrder.RowCount); detailIndex++)
				{ 
					var position = new ClientOrderPosition()
					{
						ClientPositionID = clientPositionID[detailIndex],
						ClientServerCoreID = clientServerCoreID[detailIndex],
						ProductID = productID[detailIndex],
						CodeFirmCr =
							String.IsNullOrEmpty(codeFirmCr[detailIndex]) ? null : (ulong?)ulong.Parse(codeFirmCr[detailIndex]),
						SynonymCode = synonymCode[detailIndex],
						SynonymFirmCrCode =
							String.IsNullOrEmpty(synonymFirmCrCode[detailIndex]) ? null : (ulong?)ulong.Parse(synonymFirmCrCode[detailIndex]),
						Code = code[detailIndex],
						CodeCr = codeCr[detailIndex],
						Junk = junk[detailIndex],
						Await = await[detailIndex],
						RequestRatio =
							String.IsNullOrEmpty(requestRatio[detailIndex]) ? null : (ushort?)ushort.Parse(requestRatio[detailIndex]),
						OrderCost =
							String.IsNullOrEmpty(orderCost[detailIndex]) ? null : (decimal?)decimal
								.Parse(
									orderCost[detailIndex],
									System.Globalization.NumberStyles.Currency,
									System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
						MinOrderCount =
							String.IsNullOrEmpty(minOrderCount[detailIndex]) ? null : (ulong?)ulong.Parse(minOrderCount[detailIndex]),
						Quantity = quantity[detailIndex],
						Cost = cost[detailIndex],
						MinCost =
							String.IsNullOrEmpty(minCost[detailIndex]) ? null : (decimal?)decimal
								.Parse(
									minCost[detailIndex],
									System.Globalization.NumberStyles.Currency,
									System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
						MinPriceCode =
							String.IsNullOrEmpty(minPriceCode[detailIndex]) ? null : (ulong?)ulong.Parse(minPriceCode[detailIndex]),
						LeaderMinCost =
							String.IsNullOrEmpty(leaderMinCost[detailIndex]) ? null : (decimal?)decimal
								.Parse(
									leaderMinCost[detailIndex],
									System.Globalization.NumberStyles.Currency,
									System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
						LeaderMinPriceCode =
							String.IsNullOrEmpty(leaderMinPriceCode[detailIndex]) ? null : (ulong?)ulong.Parse(leaderMinPriceCode[detailIndex]),
						SupplierPriceMarkup =
							String.IsNullOrEmpty(supplierPriceMarkup[detailIndex]) ? null : (decimal?)decimal
								.Parse(
									supplierPriceMarkup[detailIndex],
									System.Globalization.NumberStyles.Currency,
									System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
						CoreQuantity = coreQuantity[detailIndex],
						Unit = unit[detailIndex],
						Volume = volume[detailIndex],
						Note = note[detailIndex],
						Period = period[detailIndex],
						Doc = doc[detailIndex],
						RegistryCost =
							String.IsNullOrEmpty(registryCost[detailIndex]) ? null : (decimal?)decimal
									.Parse(
										registryCost[detailIndex],
										System.Globalization.NumberStyles.Currency,
										System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
						VitallyImportant = vitallyImportant[detailIndex],
						RetailMarkup =
							String.IsNullOrEmpty(retailMarkup[detailIndex]) ? null : (decimal?)decimal
								.Parse(
									retailMarkup[detailIndex],
									System.Globalization.NumberStyles.Currency,
									System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
						ProducerCost =
							String.IsNullOrEmpty(producerCost[detailIndex]) ? null : (decimal?)decimal
								.Parse(
									producerCost[detailIndex],
									System.Globalization.NumberStyles.Currency,
									System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
						NDS =
							String.IsNullOrEmpty(nds[detailIndex]) ? null : (ushort?)ushort.Parse(nds[detailIndex])
					};

					clientOrder.Positions.Add(position);
				}

				detailsPosition += clientOrder.RowCount;
			}
		}

		void CheckArrayCount(int expectedCount, int count, string arrayName)
		{
			if (count != expectedCount)
				throw new NotEnoughElementsException(
					String.Format(
						"В массиве {0} недостаточное кол-во элементов: текущее значение: {1}, необходимое значение: {2}.",
						arrayName,
						count,
						expectedCount));
		}

		public string DecodedDelphiString(string value)
		{
			if (String.IsNullOrEmpty(value))
				return null;

			var i = 0;
			var bytes = new List<byte>();

			while (i < value.Length - 2)
			{
				bytes.Add(
					Convert.ToByte(
						String.Format(
								"{0}{1}{2}",
								value[i],
								value[i + 1],
								value[i + 2]
						)
					)
				);
				i += 3;
			}

			if (bytes.Count > 0)
				return Encoding.GetEncoding(1251).GetString(bytes.ToArray());
			else
				return null;
		}

		private void CheckDuplicatedOrders()
		{
			ILog _logger = LogManager.GetLogger(this.GetType());

			foreach (var order in _orders)
			{
				//проверку производим только на заказах, которые помечены как успешные
				if (order.SendResult != OrderSendResult.Success)
					continue;

				var existsOrders = new DataTable();
				MySqlDataAdapter dataAdapter;

				if (_data.IsFutureClient)
				{
					dataAdapter = new MySqlDataAdapter(@"
select ol.*
from
  (
SELECT oh.RowId as OrderId
FROM   orders.ordershead oh
WHERE  clientorderid = ?ClientOrderID
AND    writetime    >ifnull(
       (SELECT MAX(requesttime)
       FROM    logs.AnalitFUpdates px
       WHERE   updatetype =2
       AND     px.UserId  = ?UserId
       )
       , now() - interval 2 week)
AND    clientcode = ?ClientCode
AND    UserId = ?UserId
AND    AddressId = ?AddressId
order by oh.RowId desc
limit 1
  ) DuplicateOrderId,
  orders.orderslist ol
where
  ol.OrderId = DuplicateOrderId.OrderId
", _readWriteConnection);
					dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientOrderID", order.ClientOrderId);
					dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", _data.ClientId);
					dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _data.UserId);
					dataAdapter.SelectCommand.Parameters.AddWithValue("?AddressId", _orderedClientCode);
				}
				else
				{
					dataAdapter = new MySqlDataAdapter(@"
select ol.*
from
  (
SELECT oh.RowId as OrderId
FROM   orders.ordershead oh
WHERE  clientorderid = ?ClientOrderID
AND    writetime    >ifnull(
       (SELECT MAX(requesttime)
       FROM    logs.AnalitFUpdates px
       WHERE   updatetype =2
       AND     px.UserId  = ?UserId
       )
       , now() - interval 2 week)
AND    clientcode = ?ClientCode
order by oh.RowId desc
limit 1
  ) DuplicateOrderId,
  orders.orderslist ol
where
  ol.OrderId = DuplicateOrderId.OrderId
", _readWriteConnection);
					dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientOrderID", order.ClientOrderId);
					dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", _orderedClientCode);
					dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _data.UserId);
				}

				dataAdapter.Fill(existsOrders);

				if (existsOrders.Rows.Count == 0)
					continue;

				order.ServerOrderId = Convert.ToUInt64(existsOrders.Rows[0]["OrderId"]);

				foreach (var position in order.Positions)
				{
					var existsOrderList = existsOrders.Select(position.GetFilterForDuplicatedOrder());
					if (existsOrderList.Length == 1)
					{
						var serverQuantity = Convert.ToUInt16(existsOrderList[0]["Quantity"]);
						//Если меньше или равняется, то считаем, что заказ был уже отправлен
						if (position.Quantity <= serverQuantity)
						{
							position.Duplicated = true;
							_logger.InfoFormat(
								"В новом заказе №{0} (ClientOrderId) от клиента {1} от пользователя {2} "
								+ "удалена дублирующаяся строка с заказом №{3}, строка №{4}",
								order.ClientOrderId,
								_orderedClientCode,
								_data.UserId,
								existsOrderList[0]["OrderId"],
								existsOrderList[0]["RowId"]);
						}
						else
						{
							position.Quantity = (ushort)(position.Quantity - serverQuantity);
							_logger.InfoFormat(
								"В новом заказе №{0} (ClientOrderId) от клиента {1} от пользователя {2} "
								+ "изменено количество товара в связи с дублированием с заказом №{3}, строка №{4}",
								order.ClientOrderId,
								_orderedClientCode,
								_data.UserId,
								existsOrderList[0]["OrderId"],
								existsOrderList[0]["RowId"]);
						}
						//удаляем позицию, чтобы больше не находить ее
						existsOrderList[0].Delete();
					}
					else
						if (existsOrderList.Length > 1)
						{
							var stringBuilder = new StringBuilder();
							stringBuilder.AppendFormat(
								"В новом заказе №{0} (ClientOrderId) от клиента {1} от пользователя {2}"
								+ "поиск вернул несколько позиций: {3}\r\n",
								order.ClientOrderId,
								_orderedClientCode,
								_data.UserId,
								existsOrderList.Length);
							existsOrderList.ToList().ForEach((row) => { stringBuilder.AppendLine(row.ItemArray.ToString()); });
							//Это надо залогировать
						}
				}

				//Если все заказы были помечены как дублирующиеся, то весь заказ помечаем как полностью дублирующийся
				order.FullDuplicated = (order.GetSavedRowCount() == 0);
			}
		}
	}
}
