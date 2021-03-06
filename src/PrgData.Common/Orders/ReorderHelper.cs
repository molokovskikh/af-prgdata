﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Common.Models.Helpers;
using MySql.Data.MySqlClient;
using Common.MySql;
using System.Data;
using log4net;
using System.IO;
using Common.Models;
using NHibernate.Linq;
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
		private ILog logger = LogManager.GetLogger(typeof(ReorderHelper));

		private bool _forceSend;
		private bool _useCorrectOrders;
		//Это в старой системе код клиента, а в новой системе код адреса доставки
		private uint _orderedClientCode;
		private List<ClientOrderHeader> _orders = new List<ClientOrderHeader>();

		private User _user;
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

			using (var unitOfWork = new UnitOfWork()) {
				_orderRule = IoC.Resolve<IOrderFactoryRepository>().GetOrderRule(data.ClientId);
				NHibernateUtil.Initialize(_orderRule);
				_user = IoC.Resolve<IRepository<User>>().Load(data.UserId);
				NHibernateUtil.Initialize(_user.AvaliableAddresses);
				NHibernateUtil.Initialize(_user.Client);
				_address = IoC.Resolve<IRepository<Address>>().Load(orderedClientCode);
				NHibernateUtil.Initialize(_address.Users);
			}

			CheckCanPostOrder();

			var helper = new PreorderChecker(_readWriteConnection, _user.Client);
			try {
				helper.Check();
			}
			catch (global::Common.Models.OrderException e) {
				throw new UpdateException(e.Message, "", RequestType.Forbidden);
			}
		}

		private void ProcessDeadlock()
		{
			global::Common.MySql.With.DeadlockWraper(
				() => {
					With.Session(
						session => {
							var transaction = session.BeginTransaction();
							try {
								InternalSendOrders(session);

								transaction.Commit();
							}
							catch (global::Common.Models.OrderException e) {
								throw new UpdateException(e.Message, "", RequestType.Forbidden);
							}
							catch {
								try {
									transaction.Rollback();
								}
								catch (Exception rollbackException) {
									logger.Error(
										"Ошибка при rollback'е транзакции сохранения заказов",
										rollbackException);
								}
								throw;
							}
						});
				});

#if DEBUG
			if ((_data.ClientId == 1349) || (_data.ClientId == 10005))
				global::Common.MySql.With.DeadlockWraper(
					() => {
						var transaction = _readWriteConnection.BeginTransaction(IsolationLevel.ReadCommitted);
						try {
							GenerateDocsHelper.GenerateDocs(_readWriteConnection,
								_data,
								_orders.FindAll(item => item.SendResult == OrderSendResult.Success && item.Order.RowId > 0));
							transaction.Commit();
						}
						catch {
							global::Common.MySql.With.SafeRollback(transaction);
							throw;
						}
					});
#endif
		}

		public string PostSomeOrders()
		{
			ProcessDeadlock();

			CheckDoubleCoreId();

			return GetOrdersResult();
		}

		private void CheckDoubleCoreId()
		{
			_orders.ForEach(
				order => {
					//Группируем элементы по ClientServerCoreId
					var groupedItems = order.Positions.GroupBy(position => new { position.ClientServerCoreID })
						//Формируем новый элемент со значением CoreId, кол-вом и самим списком элементов
						.Select(g => new { g.Key, ItemCount = g.Count(), GroupedElems = g.ToList() }).ToList();

					var stringBuilder = new StringBuilder();

					groupedItems
						//Выбираем только тех, у которых кол-во элементов больше одного
						.Where(g => g.Key.ClientServerCoreID > 0 && g.ItemCount > 1)
						.Each(
							g => {
								stringBuilder.AppendLine("ClientServerCoreId : " + g.Key.ClientServerCoreID);
								g.GroupedElems.OrderByDescending(item => item.OrderedQuantity)
									.Each(elem => stringBuilder.AppendLine("   " + elem.OrderPosition));
							});

					if (stringBuilder.Length > 0)
						logger.ErrorFormat("Заказ {0} содержит дублирующиеся позиции по CoreId: {1}", order.ServerOrderId, stringBuilder);
				});
		}

		private void InternalSendOrders(ISession session)
		{
			CreateOrders(session);

			//делаем проверки минимального заказа
			CheckOrders(session);

			//делаем проверку на дублирующиеся заказы
			CheckDuplicatedOrders(session);

			if (((_useCorrectOrders && !_forceSend)) && AllOrdersIsSuccess())
				//делаем сравнение с существующим прайсом
				CheckWithExistsPrices();

			if (!_useCorrectOrders || AllOrdersIsSuccess()) {
				//сохраняем сами заявки в базу
				SaveOrders(session);

				session.CreateSQLQuery(@"
insert into logs.AnalitFUpdates 
(RequestTime, UpdateType, UserId, Commit, AppVersion, ClientHost, Addition)
values 
(now(), :UpdateType, :UserId, 1, :AppVersion, :ClientHost, :Addition);
")
					.SetParameter("UpdateType", (int)RequestType.SendOrders)
					.SetParameter("UserId", _data.UserId)
					.SetParameter("AppVersion", _data.BuildNumber)
					.SetParameter("ClientHost", _data.ClientHost)
					.SetParameter("Addition", GetOrdersResultToAddition())
					.ExecuteUpdate();
			}
		}

		private string GetOrdersResultToAddition()
		{
			var result = _orders
				.Select(o => o.GetResultToAddition())
				.Where(r => !String.IsNullOrEmpty(r))
				.Implode("\r\n");

			if (String.IsNullOrEmpty(result))
				return null;
			return result;
		}

		private void CreateOrders(ISession session)
		{
			_orders.ForEach(
				clientOrder => {
					clientOrder.ClearOnCreate();

					var order = new Order(clientOrder.ActivePrice, _user, _address, _orderRule);

					order.ClientAddition = clientOrder.ClientAddition;
					order.ClientOrderId = clientOrder.ClientOrderId;
					order.VitallyImportantDelayOfPayment = clientOrder.VitallyImportantDelayOfPayment;
					order.CalculateLeader = false;

					clientOrder.Order = order;

					clientOrder.Positions.ForEach(
						position => {
							var orderPosition = clientOrder.Order.AddOrderItem(position.Offer, position.OrderedQuantity);

							if (position.ClientServerCoreID > 0)
								orderPosition.CoreId = position.ClientServerCoreID;
							else
								orderPosition.CoreId = null;

							orderPosition.RetailMarkup = position.RetailMarkup;
							orderPosition.RetailCost = position.RetailCost;
							orderPosition.SupplierPriceMarkup = position.SupplierPriceMarkup;
							orderPosition.CostWithDelayOfPayment = position.CostWithDelayOfPayment;
							orderPosition.OfferInfo.NDS = position.NDS;

							if (position.LeaderInfo != null) {
								orderPosition.LeaderInfo = position.LeaderInfo.Clone();
								orderPosition.LeaderInfo.OrderItem = orderPosition;
							}

							if (_data.EnableImpersonalPrice) {
								//Если готовимся для обезличенного прайс-листа, то сбрасываем коды синонимов
								orderPosition.SynonymCode = null;
								orderPosition.SynonymFirmCrCode = null;
							}

							position.OrderPosition = orderPosition;
						});

					//Опеределяем дублирующиеся по ClientServerCoreId и помечаем их как дублирующиеся
					//Группируем элементы по ClientServerCoreId
					var groupedItems = clientOrder.Positions.GroupBy(position => new { position.ClientServerCoreID })
						//Формируем новый элемент со значением CoreId, кол-вом и самим списком элементов
						.Select(g => new { g.Key, ItemCount = g.Count(), GroupedElems = g.ToList() }).ToList();

					groupedItems
						//Выбираем только тех, у которых кол-во элементов больше одного
						.Where(g => g.Key.ClientServerCoreID > 0 && g.ItemCount > 1)
						.Each(
							g => {
								//Сортируем по заказанному количеству по убыванию
								var orderByQuantity = g.GroupedElems.OrderByDescending(item => item.OrderedQuantity).ToList();
								//Первый элемент с наибольшим количеством оставляем, а остальные помечаем как дублирующиеся
								for (int i = 1; i < orderByQuantity.Count(); i++) {
									orderByQuantity[i].Duplicated = true;
									orderByQuantity[i].OrderPosition.Order.RemoveItem(orderByQuantity[i].OrderPosition);
								}
							});

					clientOrder.Positions.ForEach(position => {
						if (!position.Duplicated)
							position.PrepareBeforPost(session);
					});
				});
		}

		private List<Offer> GetOffers(List<uint> productIds)
		{
			var offersRepository = IoC.Resolve<ISmartOfferRepository>();

			return offersRepository.GetByProductIds(_user, productIds, false).ToList();
		}

		private List<uint> GetSearchedProductIds()
		{
			var productIds = new List<uint>();
			foreach (var order in _orders) {
				foreach (ClientOrderPosition position in order.Positions) {
					if (!position.Duplicated)
						productIds.Add(position.OrderPosition.ProductId);
				}
			}

			return productIds;
		}

		private void CheckWithExistsPrices()
		{
			var productIds = GetSearchedProductIds();

			if (productIds.Count > 0) {
				var optimizer = new CostOptimizer(_readWriteConnection, _user.Client.Id, _user.Id);
				var offers = GetOffers(productIds);
				var orderWithPosibleOptimizedCosts = _orders.Where(o => o.ActivePrice.Id.Price.Supplier.Id == optimizer.SupplierId);
				foreach (var position in orderWithPosibleOptimizedCosts.SelectMany(o => o.Positions)) {
					position.IgnoreCostReducing = true;
				}

				System.Tuple<uint, ulong>[] activePrices;
				using (var session = UpdateHelper.SessionFactory.OpenSession(_readWriteConnection)) {
					var supplierIds = _orders.Select(o => o.ActivePrice.Id.Price.Supplier.Id).ToArray();
					var rules = session.Query<CostOptimizationRule>()
						.Where(r => r.RuleType == RuleType.MaxCost
							&& (r.Clients.Count == 0 || r.Clients.Contains(_user.Client))
							&& supplierIds.Contains(r.Supplier.Id))
						.ToArray()
						.Select(r => r.Supplier.Id)
						.ToArray();
					_orders.Where(o => rules.Contains(o.ActivePrice.Id.Price.Supplier.Id))
						.SelectMany(o => o.Positions)
						.Each(l => l.IgnoreCostReducing = true);
					using(OfferRepository.InvokeGetActivePrices(session, _user))
						activePrices = session.Query<ActivePrice>()
							.Select(p => Tuple.Create(p.Id.Price.PriceCode, p.Id.RegionCode))
							.ToArray();
				}

				foreach (var order in _orders) {
					var key = Tuple.Create(order.ActivePrice.Id.Price.PriceCode, order.ActivePrice.Id.RegionCode);
					if (!activePrices.Contains(key))
						continue;
					foreach (var position in order.Positions) {
						if (!position.Duplicated) {
							var offer = GetDataRowByPosition(offers, order, position);
							if (offer == null)
								position.SendResult = PositionSendResult.NotExists;
							else
								CheckExistsCorePosition(offer, position);
						}
					}

					if (order.Positions.Any(item => item.SendResult != PositionSendResult.Success))
						order.SendResult = OrderSendResult.NeedCorrect;
				}
			}
		}

		private void CheckExistsCorePosition(Offer offer, ClientOrderPosition position)
		{
			uint? serverQuantity = null;

			if (offer.Quantity.HasValue) {
				serverQuantity = offer.Quantity;
			}

			if (position.IsCostChanged(offer)) {
				position.SendResult = PositionSendResult.DifferentCost;
				position.ServerCost = offer.Cost;
				if (serverQuantity.HasValue)
					position.ServerQuantity = serverQuantity.Value;
			}

			if (serverQuantity.HasValue && (serverQuantity.Value < position.OrderPosition.Quantity)) {
				//Если имеется различие по цене, то говорим, что есть различие по цене и кол-ву
				if (position.SendResult == PositionSendResult.DifferentCost)
					position.SendResult = PositionSendResult.DifferentCostAndQuantity;
				else {
					position.SendResult = PositionSendResult.DifferentQuantity;
					position.ServerCost = offer.Cost;
					position.ServerQuantity = serverQuantity.Value;
				}
			}
		}

		private Offer GetDataRowByPosition(List<Offer> offers, ClientOrderHeader order, ClientOrderPosition position)
		{
			var clientServerCoreIdOffers = new List<Offer>();

			//Если код CoreID не установлен, то поиск по CoreId не производим
			if (position.ClientServerCoreID > 0)
				//Если длина в символах его меньше 9, то ищем как есть
				if (position.ClientServerCoreID.ToString().Length < 9)
					clientServerCoreIdOffers = offers.FindAll(
						item => {
							return
								order.Order.ActivePrice.Id.Price.PriceCode.Equals(item.PriceList.Id.Price.PriceCode) &&
									order.Order.RegionCode.Equals(item.PriceList.Id.RegionCode) &&
									item.Id.CoreId.ToString().Equals(position.ClientServerCoreID.ToString());
						});
				else
					//Если длина в символах = 9, то ищем с конца
					clientServerCoreIdOffers = offers.FindAll(
						item => {
							return
								order.Order.ActivePrice.Id.Price.PriceCode.Equals(item.PriceList.Id.Price.PriceCode) &&
									order.Order.RegionCode.Equals(item.PriceList.Id.RegionCode) &&
									item.Id.CoreId.ToString().EndsWith(position.ClientServerCoreID.ToString());
						});

			if (clientServerCoreIdOffers.Count == 1)
				return clientServerCoreIdOffers[0];
			else if (clientServerCoreIdOffers.Count == 0) {
				var filterOffers = offers.FindAll(
					item => {
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
			foreach (var order in _orders) {
				if ((order.SendResult == OrderSendResult.Success) && !order.FullDuplicated) {
					session.Save(order.Order);
					order.ServerOrderId = order.Order.RowId;
				}
			}
		}

		private void CheckOrders(ISession session)
		{
			PreorderChecker.CheckDailyOrdersSum(session, _address, _orders.Select(o => o.Order));

			if (!_user.IgnoreCheckMinOrder) {
				foreach (var order in _orders) {
					order.SendResult = OrderSendResult.Success;
					var minReqController = new MinReqController(session, order.Order, _data.SupportedMinReordering());
					var status = minReqController.ProcessOrder();
					order.Apply(status);
				}
			}

			var checker = new GroupSumOrderChecker(session);
			var rejectedOrders = checker.Check(_orders.Select(o => o.Order));
			foreach (var order in _orders) {
				if (rejectedOrders.ContainsKey(order.Order)) {
					order.SendResult = OrderSendResult.GreateThanMaxOrderSum;
					order.MaxSum = rejectedOrders[order.Order];
					order.ErrorReason = String.Format("Ваша заявка на {0} НЕ Принята, поскольку Сумма заказов в этом месяце по Вашему предприятию на поставщика {0} превысила установленный лимит.",
						order.Order.PriceList.Supplier.Name);
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

			foreach (var order in _orders) {
				if (String.IsNullOrEmpty(result))
					result += order.GetResultToClient(_data);
				else
					result += ";" + order.GetResultToClient(_data);
			}

			return result;
		}

		private void CheckCanPostOrder()
		{
			CheckCanPostOrder(_orderedClientCode);
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
			bool[] awaitValues,
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
			string[] nds,
			string[] retailCost,
			string[] vitallyImportantDelayOfPayment,
			decimal[] costWithDelayOfPayment,
			string[] ean13,
			string[] codeOKP,
			string[] series)
		{
			CheckArrayCount(orderCount, clientOrderId.Length, "clientOrderId");
			CheckArrayCount(orderCount, priceCode.Length, "priceCode");
			CheckArrayCount(orderCount, regionCode.Length, "regionCode");
			CheckArrayCount(orderCount, priceDate.Length, "priceDate");
			CheckArrayCount(orderCount, clientAddition.Length, "clientAddition");
			CheckArrayCount(orderCount, rowCount.Length, "rowCount");
			CheckArrayCount(orderCount, delayOfPayment.Length, "delayOfPayment");
			CheckArrayCount(orderCount, vitallyImportantDelayOfPayment.Length, "vitallyImportantDelayOfPayment");

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
			CheckArrayCount(allPositionCount, awaitValues.Length, "await");
			CheckArrayCount(allPositionCount, requestRatio.Length, "requestRatio");
			CheckArrayCount(allPositionCount, orderCost.Length, "orderCost");
			CheckArrayCount(allPositionCount, minOrderCount.Length, "minOrderCount");
			CheckArrayCount(allPositionCount, quantity.Length, "quantity");

			CheckArrayCount(allPositionCount, cost.Length, "cost");
			CheckArrayCount(allPositionCount, costWithDelayOfPayment.Length, "costWithDelayOfPayment");
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
			CheckArrayCount(allPositionCount, retailCost.Length, "retailCost");

			CheckArrayCount(allPositionCount, producerCost.Length, "producerCost");
			CheckArrayCount(allPositionCount, nds.Length, "nds");

			CheckArrayCount(allPositionCount, ean13.Length, "ean13");
			CheckArrayCount(allPositionCount, codeOKP.Length, "codeOKP");
			CheckArrayCount(allPositionCount, series.Length, "series");

			using (var unitOfWork = new UnitOfWork()) {
				var detailsPosition = 0u;
				for (int i = 0; i < orderCount; i++) {
					var priceList = IoC.Resolve<IRepository<PriceList>>().Load(Convert.ToUInt32(priceCode[i]));
					var activePrice = new ActivePrice {
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
						new ClientOrderHeader {
							ActivePrice = activePrice,
							ClientAddition = DecodedDelphiString(clientAddition[i]),
							ClientOrderId = Convert.ToUInt32(clientOrderId[i]),
							VitallyImportantDelayOfPayment =
								String.IsNullOrEmpty(vitallyImportantDelayOfPayment[i]) ? 0m : decimal
									.Parse(
										vitallyImportantDelayOfPayment[i],
										System.Globalization.NumberStyles.Currency,
										System.Globalization.CultureInfo.InvariantCulture.NumberFormat)
						};

					var currentRowCount = rowCount[i];
					_orders.Add(clientOrder);

					for (uint detailIndex = detailsPosition; detailIndex < (detailsPosition + currentRowCount); detailIndex++) {
						uint tempCoreQuantity;
						var offer = new Offer() {
							Id = new OfferKey(0, regionCode[i]),
							ProductId = Convert.ToUInt32(productID[detailIndex]),
							CodeFirmCr =
								String.IsNullOrEmpty(codeFirmCr[detailIndex]) ? null : (uint?)uint.Parse(codeFirmCr[detailIndex]),
							SynonymCode = Convert.ToUInt32(synonymCode[detailIndex]),
							SynonymFirmCrCode =
								String.IsNullOrEmpty(synonymFirmCrCode[detailIndex])
									? null
									: (uint?)uint.Parse(synonymFirmCrCode[detailIndex]),
							Code = code[detailIndex],
							CodeCr = codeCr[detailIndex],
							Junk = junk[detailIndex],
							Await = awaitValues[detailIndex],
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
							Quantity = !String.IsNullOrEmpty(coreQuantity[detailIndex]) && uint.TryParse(coreQuantity[detailIndex], out tempCoreQuantity) ? (uint?)tempCoreQuantity : null,
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
							EAN13 = ean13[detailIndex],
							CodeOKP = codeOKP[detailIndex],
							Series = series[detailIndex],
						};

						OrderItemLeadersInfo leaderInfo = null;
						if (_orderRule.CalculateLeader) {
							leaderInfo =
								new OrderItemLeadersInfo {
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
							new ClientOrderPosition {
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
								RetailCost =
									String.IsNullOrEmpty(retailCost[detailIndex])
										? null
										: (float?)float
											.Parse(
												retailCost[detailIndex],
												System.Globalization.NumberStyles.Currency,
												System.Globalization.CultureInfo.InvariantCulture.NumberFormat),
								CostWithDelayOfPayment = Convert.ToSingle(costWithDelayOfPayment[detailIndex]),
							};

						clientOrder.Positions.Add(position);
					}

					detailsPosition += currentRowCount;
				}
			}
		}

		public static decimal[] PrepareCostWithDelayOfPayment(
			decimal[] cost,
			ushort orderCount,
			string[] delayOfPayment,
			ushort[] rowCount)
		{
			var results = (decimal[])cost.Clone();

			if (delayOfPayment.Length > 0 && delayOfPayment.Length == orderCount && delayOfPayment.Length == rowCount.Length) {
				var detailsPosition = 0u;
				for (int i = 0; i < orderCount; i++) {
					var delay = String.IsNullOrEmpty(delayOfPayment[i])
						? 0m
						: decimal
							.Parse(
								delayOfPayment[i],
								System.Globalization.NumberStyles.Currency,
								System.Globalization.CultureInfo.InvariantCulture.NumberFormat);
					for (uint detailIndex = detailsPosition; detailIndex < (detailsPosition + rowCount[i]); detailIndex++) {
						results[detailIndex] = cost[detailIndex] * (1m + delay / 100m);
					}
					detailsPosition += rowCount[i];
				}
			}

			return results;
		}

		private void CheckArrayCount(int expectedCount, int count, string arrayName)
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

			while (i < value.Length - 2) {
				bytes.Add(
					Convert.ToByte(
						String.Format(
							"{0}{1}{2}",
							value[i],
							value[i + 1],
							value[i + 2])));
				i += 3;
			}

			if (bytes.Count > 0)
				return Encoding.GetEncoding(1251).GetString(bytes.ToArray());
			else
				return null;
		}

		private void CheckDuplicatedOrders(ISession session)
		{
			string logMessage;

			foreach (var order in _orders) {
				//проверку производим только на заказах, которые помечены как успешные
				if (order.SendResult != OrderSendResult.Success) {
					logger.DebugFormat("Для заказа (UserId: {0}, AddressId: {1}, ClientOrderId: {2}) не будем проверять дубликаты, т.к. он не успешен {3}\r\nПозиций: {4}\r\n{5}",
						_data.UserId,
						_orderedClientCode,
						order.Order.ClientOrderId,
						order.SendResult,
						order.Positions.Count,
						order.Positions.Implode("\r\n"));
					continue;
				}

				var existsOrders = new DataTable();
				var dataAdapter = new MySqlDataAdapter(@"
select ol.*
from
(
SELECT oh.RowId as OrderId
FROM   orders.ordershead oh
WHERE  
writetime > now() - interval 2 week
AND clientorderid = ?ClientOrderID  
AND clientcode = ?ClientCode
AND UserId = ?UserId
AND AddressId = ?AddressId
AND PriceCode = ?PriceCode
AND RegionCode = ?RegionCode
and oh.Deleted = 0
order by oh.RowId
) DuplicateOrderId,
orders.orderslist ol
where
ol.OrderId = DuplicateOrderId.OrderId
order by ol.RowId
", _readWriteConnection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientOrderID", order.Order.ClientOrderId);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", _data.ClientId);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _data.UserId);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?AddressId", _orderedClientCode);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?PriceCode", order.Order.PriceList.PriceCode);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?RegionCode", order.Order.RegionCode);

				dataAdapter.Fill(existsOrders);

				if (existsOrders.Rows.Count == 0) {
					logger.DebugFormat("Для заказа (UserId: {0}, ClientId: {1}, AddressId: {2}, ClientOrderId: {3}) не будем проверять дубликаты, т.к. не найдены предыдущие заказы\r\nПозиций: {4}\r\n{5}",
						_data.UserId,
						_data.ClientId,
						_orderedClientCode,
						order.Order.ClientOrderId,
						order.Positions.Count,
						order.Positions.Implode("\r\n"));

					continue;
				}

				//Берем последний номер заказа
				order.ServerOrderId = Convert.ToUInt64(existsOrders.Rows[existsOrders.Rows.Count - 1]["OrderId"]);
				var orderList = new List<string>();
				foreach (DataRow dataRow in existsOrders.Rows) {
					var orderId = dataRow["OrderId"].ToString();
					if (!orderList.Contains(orderId))
						orderList.Add(orderId);
				}

				logMessage = String.Format(
					"Для заказа (UserId: {0}, ClientId: {1}, AddressId: {2}, ClientOrderId: {3}) будем проверять дубликаты по заказам: ({4})\r\nПоследний заказ: {5}\r\nПозиций: {6}\r\n{7}",
					_data.UserId,
					_data.ClientId,
					_orderedClientCode,
					order.Order.ClientOrderId,
					orderList.Implode(),
					order.ServerOrderId,
					order.Positions.Count,
					order.Positions.Implode("\r\n"));
				logger.DebugFormat(logMessage);

				foreach (ClientOrderPosition position in order.Positions) {
					//позиция может быть дублированной из-за ClientServerCoreId
					if (!position.Duplicated) {
						var existsOrderList = existsOrders.Select(position.GetFilterForDuplicatedOrder());
						if (existsOrderList.Length == 1) {
							var serverQuantity = Convert.ToUInt32(existsOrderList[0]["Quantity"]);
							//Если меньше или равняется, то считаем, что заказ был уже отправлен
							if (position.OrderPosition.Quantity <= serverQuantity) {
								position.Duplicated = true;
								order.Order.RemoveItem(position.OrderPosition);
								logMessage = String.Format(
									"В новом заказе №{0} (ClientOrderId) от клиента {1} от пользователя {2} "
										+ "удалена дублирующаяся строка с заказом №{3}, строка №{4}",
									order.Order.ClientOrderId,
									_orderedClientCode,
									_data.UserId,
									existsOrderList[0]["OrderId"],
									existsOrderList[0]["RowId"]);
								logger.InfoFormat(logMessage);
							}
							else {
								position.OrderPosition.Quantity = (ushort)(position.OrderPosition.Quantity - serverQuantity);
								logMessage = String.Format(
									"В новом заказе №{0} (ClientOrderId) от клиента {1} от пользователя {2} "
										+ "изменено количество товара в связи с дублированием с заказом №{3}, строка №{4}",
									order.Order.ClientOrderId,
									_orderedClientCode,
									_data.UserId,
									existsOrderList[0]["OrderId"],
									existsOrderList[0]["RowId"]);
								logger.InfoFormat(logMessage);
							}
							//удаляем позицию, чтобы больше не находить ее
							existsOrderList[0].Delete();
						}
						else if (existsOrderList.Length > 1) {
							var byQuantity = existsOrderList.OrderByDescending(item => item["Quantity"]).ToList();

							var existsOrderedQuantity = Convert.ToUInt32(byQuantity[0]["Quantity"]);

							var stringBuilder = new StringBuilder();
							existsOrderList.ToList().ForEach(row => stringBuilder.AppendLine(row.ItemArray.Implode()));

							//Если меньше или равняется, то считаем, что заказ был уже отправлен
							if (position.OrderPosition.Quantity <= existsOrderedQuantity) {
								position.Duplicated = true;
								order.Order.RemoveItem(position.OrderPosition);

								logMessage = String.Format(
									"В новом заказе №{0} (ClientOrderId) от клиента {1} от пользователя {2} "
										+ "удалена дублирующаяся строка с заказом №{3}, поиск вернул несколько позиций ({4}):\r\n{5}",
									order.Order.ClientOrderId,
									_orderedClientCode,
									_data.UserId,
									existsOrderList[0]["OrderId"],
									existsOrderList.Length,
									stringBuilder);
								logger.InfoFormat(logMessage);
							}
							else {
								position.OrderPosition.Quantity = (ushort)(position.OrderPosition.Quantity - existsOrderedQuantity);

								logMessage = String.Format(
									"В новом заказе №{0} (ClientOrderId) от клиента {1} от пользователя {2} "
										+ "изменено количество товара в связи с дублированием с заказом №{3}, поиск вернул несколько позиций ({4}):\r\n{5}",
									order.Order.ClientOrderId,
									_orderedClientCode,
									_data.UserId,
									existsOrderList[0]["OrderId"],
									existsOrderList.Length,
									stringBuilder);
								logger.InfoFormat(logMessage);
							}
							//удаляем позиции, чтобы больше не находить их
							byQuantity.ForEach(row => row.Delete());
						}
					}
				}

				//Если все заказы были помечены как дублирующиеся, то весь заказ помечаем как полностью дублирующийся
				order.FullDuplicated = (order.GetSavedRowCount() == 0);
				if (order.FullDuplicated) {
					logMessage = String.Format(
						"Заказ (UserId: {0}, ClientId: {1}, AddressId: {2}, ClientOrderId: {3}) помечен как полностью дублированный",
						_data.UserId,
						_data.ClientId,
						_orderedClientCode,
						order.Order.ClientOrderId);
					logger.DebugFormat(logMessage);

					var serverOrder = session.Load<Order>(Convert.ToUInt32(order.ServerOrderId));
					order.Order.WriteTime = serverOrder.WriteTime;
				}
			}
		}
	}
}