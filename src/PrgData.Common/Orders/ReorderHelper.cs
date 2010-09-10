using System;
using System.Collections;
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
using NHibernate;
using With = Common.Models.With;
using Common.Tools;

namespace PrgData.Common.Orders
{
	public class ReorderHelper : OrderHelper
	{
		private bool _forceSend;
		private bool _useCorrectOrders;
		//Это в старой системе код клиента, а в новой системе код адреса доставки
		private uint _orderedClientCode;
		private List<ClientOrderHeader> _orders = new List<ClientOrderHeader>();

		private IOrderable _client;
		private Address _address;
		private OrderRules _orderRule;

		public ReorderHelper(
			UpdateData data, 
			MySqlConnection readWriteConnection, 
			bool forceSend,
			uint orderedClientCode,
			bool useCorrectOrders) :
			base(data, readWriteConnection)
		{
			_forceSend = forceSend;
			_orderedClientCode = orderedClientCode;
			_useCorrectOrders = useCorrectOrders;

			_orderRule = IoC.Resolve<IRepository<OrderRules>>().Get(data.ClientId);

			using (var unitOfWork = new UnitOfWork())
			{
				if (data.IsFutureClient)
				{
					_client = IoC.Resolve<IRepository<User>>().Get(data.UserId);
					NHibernateUtil.Initialize(((User)_client).AvaliableAddresses);
					_address = IoC.Resolve<IRepository<Address>>().Get(orderedClientCode);
					NHibernateUtil.Initialize(_address.Users);
				}
				else
					_client = IoC.Resolve<IRepository<Client>>().Get(orderedClientCode);
			}

			CheckCanPostOrder();

			CheckWeeklySumOrder();
		}

		public string PostSomeOrders()
		{
			global::Common.MySql.With.DeadlockWraper(
				() =>
				{
					With.Session(
						session =>
						{
							var transaction = session.BeginTransaction();
							try
							{
								InternalSendOrders(session);

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
									_logger.Error(
										"Ошибка при rollback'е транзакции сохранения заказов",
										rollbackException);
								}
								throw;
							}
						});
				});

			return GetOrdersResult();
		}

		private void InternalSendOrders(ISession session)
		{
			CreateOrders(session);

			//делаем проверки минимального заказа
			CheckOrdersByMinRequest();

			//делаем проверку на дублирующиеся заказы
			CheckDuplicatedOrders();

			if (_useCorrectOrders && !_forceSend && AllOrdersIsSuccess())
				//делаем сравнение с существующим прайсом
				CheckWithExistsPrices();

			if (!_useCorrectOrders || AllOrdersIsSuccess())
			{
				//Сбрасываем ServerOrderId перед заказом только у заказов, 
				//которые не являются полностью дублированными
				//_orders.ForEach(item => item.PrepareBeforPost(session));

				//сохраняем сами заявки в базу
				SaveOrders(session);

#if DEBUG
				//if ((_data.ClientId == 1349) || (_data.ClientId == 10005))
				//    GenerateDocsHelper.GenerateDocs(_readWriteConnection,
				//                                    _data,
				//                                    _orders.FindAll(
				//                                        item =>
				//                                        item.SendResult ==
				//                                        OrderSendResult.Success));
#endif

				session.CreateSQLQuery(@"
insert into logs.AnalitFUpdates 
  (RequestTime, UpdateType, UserId, Commit, AppVersion, ClientHost) 
values 
  (now(), :UpdateType, :UserId, 1, :AppVersion, :ClientHost);
"
					)
					.SetParameter("UpdateType", (int)RequestType.SendOrders)
					.SetParameter("UserId", _data.UserId)
					.SetParameter("AppVersion", _data.BuildNumber)
					.SetParameter("ClientHost", _data.ClientHost)
					.ExecuteUpdate();
			}
		}

		private void CreateOrders(ISession session)
		{
			_orders.ForEach(
				clientOrder => 
				{ 
					clientOrder.ClearOnCreate();

					Order order;
					if (_client is IClient)
						order = new Order(clientOrder.ActivePrice, (IClient)_client, _orderRule);
					else
						order = new Order(clientOrder.ActivePrice, (User)_client, _address, _orderRule);

					order.ClientAddition = clientOrder.ClientAddition;
					order.ClientOrderId = clientOrder.ClientOrderId;
					order.CalculateLeader = false;

					clientOrder.Order = order;

					clientOrder.Positions.ForEach(
						position => 
						{
							var orderPosition = clientOrder.Order.AddOrderItem(position.Offer, position.OrderedQuantity);

							orderPosition.CoreId = null;

							orderPosition.RetailMarkup = position.RetailMarkup;
							orderPosition.SupplierPriceMarkup = position.SupplierPriceMarkup;
							orderPosition.OfferInfo.NDS = position.NDS;

							if (position.LeaderInfo != null)
							{
								orderPosition.LeaderInfo = position.LeaderInfo.Clone();
								orderPosition.LeaderInfo.OrderItem = orderPosition;
							}

							if (_data.EnableImpersonalPrice)
							{
								//Если готовимся для обезличенного прайс-листа, то сбрасываем коды синонимов
								orderPosition.SynonymCode = null;
								orderPosition.SynonymFirmCrCode = null;
							}

							position.OrderPosition = orderPosition;

							position.PrepareBeforPost(session);
						});
				});
		}

		private List<Offer> GetOffers(List<uint> productIds)
		{
			var offersRepository = IoC.Resolve<ISmartOfferRepository>();

			return offersRepository.SimpleGetByProductIds(_client, productIds).ToList();
		}

		private List<uint> GetSearchedProductIds()
		{
			var productIds = new List<uint>();
			foreach (var order in _orders)
			{
				foreach (ClientOrderPosition position in order.Positions)
				{
					if (!position.Duplicated)
						productIds.Add(position.OrderPosition.ProductId);
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
					foreach (ClientOrderPosition position in order.Positions)
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

					if (order.Positions.Any((item) => { return ((ClientOrderPosition)item).SendResult != PositionSendResult.Success; }))
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

			if (!position.OrderPosition.Cost.Equals(offer.Cost))
			{
				position.SendResult = PositionSendResult.DifferentCost;
				position.ServerCost = offer.Cost;
				if (serverQuantity.HasValue)
					position.ServerQuantity = serverQuantity.Value;
			}

			if (serverQuantity.HasValue && (serverQuantity.Value < position.OrderPosition.Quantity))
			{
				//Если имеется различие по цене, то говорим, что есть различие по цене и кол-ву
				if (position.SendResult == PositionSendResult.DifferentCost)
					position.SendResult = PositionSendResult.DifferentCostAndQuantity;
				else
				{
					position.SendResult = PositionSendResult.DifferentQuantity;
					position.ServerCost = offer.Cost;
					position.ServerQuantity = serverQuantity.Value;
				}
			}
		}

		private Offer GetDataRowByPosition(List<Offer> offers, ClientOrderHeader order, ClientOrderPosition position)
		{
			var clientServerCoreIdOffers = offers.FindAll(item => { 
				return
					order.Order.ActivePrice.Id.Price.PriceCode.Equals(item.PriceList.Id.Price.PriceCode) &&
					order.Order.RegionCode.Equals(item.PriceList.Id.RegionCode) &&
					item.Id.ToString().EndsWith(position.ClientServerCoreID.ToString()); });

			if (clientServerCoreIdOffers.Count == 1)
				return clientServerCoreIdOffers[0];
			else
				if (clientServerCoreIdOffers.Count == 0)
				{
					var filterOffers = offers.FindAll(
						item =>
						{
							var newOrder = position.OrderPosition;
							return
								order.Order.ActivePrice.Id.Price.PriceCode.Equals(item.PriceList.Id.Price.PriceCode) &&
								order.Order.RegionCode.Equals(item.PriceList.Id.RegionCode) &&
								item.ProductId == newOrder.ProductId &&
								item.SynonymCode == newOrder.SynonymCode &&
								item.SynonymFirmCrCode == newOrder.SynonymFirmCrCode &&
								item.Code == newOrder.Code &&
								item.CodeCr == newOrder.CodeCr &&
								item.Junk == newOrder.Junk &&
								item.Await == newOrder.Await &&
								item.RequestRatio == newOrder.RequestRatio &&
								newOrder.OrderCost == item.OrderCost &&
								item.MinOrderCount == newOrder.MinOrderCount;						 
					});
					if (filterOffers.Count > 0)
						return filterOffers[0];
				}
				else
					throw new OrderException(String.Format("По ID = {0} нашли больше одной позиции.", position.ClientServerCoreID));

			return null;
		}

		private void SaveOrders(ISession session)
		{
			foreach (var order in _orders)
			{
				if ((order.SendResult == OrderSendResult.Success) && !order.FullDuplicated)
				{
					session.Save(order.Order);
					order.ServerOrderId = order.Order.RowId;
				}
			}
		}

		private void CheckOrdersByMinRequest()
		{
			foreach (var order in _orders)
			{
				var minReq = GetMinReq(_orderedClientCode, order.Order.RegionCode, order.Order.ActivePrice.Id.Price.PriceCode);
				order.SendResult = OrderSendResult.Success;
				if ((minReq != null) && minReq.ControlMinReq && (minReq.MinReq > 0))
					if (order.Order.CalculateSum() < minReq.MinReq)
					{
						order.SendResult = OrderSendResult.LessThanMinReq;
						order.MinReq = minReq.MinReq;
						order.ErrorReason = "Поставщик отказал в приеме заказа.\n Сумма заказа меньше минимально допустимой.";
					}
			}
		}

		private bool AllOrdersIsSuccess()
		{
			return _orders.All(item => item.SendResult == OrderSendResult.Success);
		}

		private string GetOrdersResult()
		{
			var result = String.Empty;

			foreach(var order in _orders)
			{
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


			using (var unitOfWork = new UnitOfWork())
			{
				var detailsPosition = 0u;
				for (int i = 0; i < orderCount; i++)
				{
					var priceList = IoC.Resolve<IRepository<PriceList>>().Get(Convert.ToUInt32(priceCode[i]));
					var activePrice = new ActivePrice
					{
						Id = new PriceKey(priceList) { RegionCode = regionCode[i] },
						PriceDate = priceDate[i].ToLocalTime(),
						DelayOfPayment =
							String.IsNullOrEmpty(delayOfPayment[i]) ? 0m : decimal
								.Parse(
									delayOfPayment[i],
									System.Globalization.NumberStyles.Currency,
									System.Globalization.CultureInfo.InvariantCulture.NumberFormat)
					};

					var clientOrder = 
						new ClientOrderHeader
						{
							ActivePrice = activePrice,
							ClientAddition = DecodedDelphiString(clientAddition[i]),
							ClientOrderId = Convert.ToUInt32(clientOrderId[i]),
						};

					var currentRowCount = rowCount[i];
					_orders.Add(clientOrder);

					for (uint detailIndex = detailsPosition; detailIndex < (detailsPosition + currentRowCount); detailIndex++)
					{
						var offer = new Offer()
						{
						    ProductId = Convert.ToUInt32(productID[detailIndex]),
						    CodeFirmCr =
						        String.IsNullOrEmpty(codeFirmCr[detailIndex]) ? null : (uint?) uint.Parse(codeFirmCr[detailIndex]),
						    SynonymCode = Convert.ToUInt32(synonymCode[detailIndex]),
						    SynonymFirmCrCode =
						        String.IsNullOrEmpty(synonymFirmCrCode[detailIndex])
						            ? null
						            : (uint?) uint.Parse(synonymFirmCrCode[detailIndex]),
						    Code = code[detailIndex],
						    CodeCr = codeCr[detailIndex],
						    Junk = junk[detailIndex],
						    Await = await[detailIndex],
							RequestRatio =
								String.IsNullOrEmpty(requestRatio[detailIndex]) ? null : (ushort?)ushort.Parse(requestRatio[detailIndex]),
							OrderCost =
								String.IsNullOrEmpty(orderCost[detailIndex]) ? null : (float?)float
									.Parse(
										orderCost[detailIndex],
										System.Globalization.NumberStyles.Currency,
										System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
							MinOrderCount =
								String.IsNullOrEmpty(minOrderCount[detailIndex]) ? null : (uint?)uint.Parse(minOrderCount[detailIndex]),

							Cost = Convert.ToSingle(cost[detailIndex]),
							Quantity =
								String.IsNullOrEmpty(coreQuantity[detailIndex]) ? null : (uint?)uint.Parse(coreQuantity[detailIndex]),

							Unit = unit[detailIndex],
							Volume = volume[detailIndex],
							Note = note[detailIndex],
							Period = period[detailIndex],
							Doc = doc[detailIndex],
							RegistryCost =
								String.IsNullOrEmpty(registryCost[detailIndex]) ? null : (float?)float
										.Parse(
											registryCost[detailIndex],
											System.Globalization.NumberStyles.Currency,
											System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
							VitallyImportant = vitallyImportant[detailIndex],
							ProducerCost =
								String.IsNullOrEmpty(producerCost[detailIndex]) ? null : (float?)float
									.Parse(
										producerCost[detailIndex],
										System.Globalization.NumberStyles.Currency,
										System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
						};

						OrderItemLeadersInfo leaderInfo = null;
						if (_orderRule.CalculateLeader)
						{
							leaderInfo =
								new OrderItemLeadersInfo 
								{
									MinCost =
										String.IsNullOrEmpty(minCost[detailIndex]) ? null : (float?)float
											.Parse(
												minCost[detailIndex],
												System.Globalization.NumberStyles.Currency,
												System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
									PriceCode =
										String.IsNullOrEmpty(minPriceCode[detailIndex]) ? null : (uint?)uint.Parse(minPriceCode[detailIndex]),
									LeaderMinCost =
										String.IsNullOrEmpty(leaderMinCost[detailIndex]) ? null : (float?)float
											.Parse(
												leaderMinCost[detailIndex],
												System.Globalization.NumberStyles.Currency,
												System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
									LeaderPriceCode =
										String.IsNullOrEmpty(leaderMinPriceCode[detailIndex]) ? null : (uint?)uint.Parse(leaderMinPriceCode[detailIndex]),
								};

							if ((!leaderInfo.MinCost.HasValue && !leaderInfo.LeaderMinCost.HasValue) ||
							    (!leaderInfo.PriceCode.HasValue && !leaderInfo.LeaderPriceCode.HasValue))
								leaderInfo = null;
						}

						var position =
							new ClientOrderPosition
								{
									ClientPositionID = clientPositionID[detailIndex],
									ClientServerCoreID = clientServerCoreID[detailIndex],
									OrderedQuantity = quantity[detailIndex],
									Offer = offer,
									LeaderInfo = leaderInfo,
									RetailMarkup =
										String.IsNullOrEmpty(retailMarkup[detailIndex])
											? null
											: (float?)float
															.Parse(
																retailMarkup[detailIndex],
																System.Globalization.NumberStyles.Currency,
																System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
									SupplierPriceMarkup =
										String.IsNullOrEmpty(supplierPriceMarkup[detailIndex])
										? null
										: (float?)float
											.Parse(
												supplierPriceMarkup[detailIndex],
												System.Globalization.NumberStyles.Currency,
												System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
									NDS =
										String.IsNullOrEmpty(nds[detailIndex]) ? null : (ushort?)ushort.Parse(nds[detailIndex]),
								};

						clientOrder.Positions.Add(position);
					}

					detailsPosition += currentRowCount;
				}
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
					dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientOrderID", order.Order.ClientOrderId);
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
					dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientOrderID", order.Order.ClientOrderId);
					dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", _orderedClientCode);
					dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _data.UserId);
				}

				dataAdapter.Fill(existsOrders);

				if (existsOrders.Rows.Count == 0)
					continue;

				order.ServerOrderId = Convert.ToUInt64(existsOrders.Rows[0]["OrderId"]);

				foreach (ClientOrderPosition position in order.Positions)
				{
					var existsOrderList = existsOrders.Select(position.GetFilterForDuplicatedOrder());
					if (existsOrderList.Length == 1)
					{
						var serverQuantity = Convert.ToUInt32(existsOrderList[0]["Quantity"]);
						//Если меньше или равняется, то считаем, что заказ был уже отправлен
						if (position.OrderPosition.Quantity <= serverQuantity)
						{
							position.Duplicated = true;
							order.Order.RemoveItem(position.OrderPosition);
							_logger.InfoFormat(
								"В новом заказе №{0} (ClientOrderId) от клиента {1} от пользователя {2} "
								+ "удалена дублирующаяся строка с заказом №{3}, строка №{4}",
								order.Order.ClientOrderId,
								_orderedClientCode,
								_data.UserId,
								existsOrderList[0]["OrderId"],
								existsOrderList[0]["RowId"]);
						}
						else
						{
							position.OrderPosition.Quantity = (ushort)(position.OrderPosition.Quantity - serverQuantity);
							_logger.InfoFormat(
								"В новом заказе №{0} (ClientOrderId) от клиента {1} от пользователя {2} "
								+ "изменено количество товара в связи с дублированием с заказом №{3}, строка №{4}",
								order.Order.ClientOrderId,
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
								order.Order.ClientOrderId,
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
