using System;
using System.Collections.Generic;
using System.Linq;
using PrgData.Common.Models;
using log4net;

namespace PrgData.Common.Orders.MinOrders
{
	public static class ReordingRulesExtensions
	{
		public static ReorderingRule GetRuleByDayOfWeek(this List<ReorderingRule> rules, DateTime date)
		{
			return rules.Find(r => r.DayOfWeek == date.DayOfWeek);
		}

		public static TimeSpan GetMinTimeOfStopsOrders(this List<ReorderingRule> rules)
		{
			var minTimes = rules
				.Where(r => r.TimeOfStopsOrders.HasValue && r.TimeOfStopsOrders.Value.Ticks > 0)
				.Select(r => r.TimeOfStopsOrders.Value)
				.OrderBy(t => t)
				.ToList();

			if (minTimes.Count > 0)
				return minTimes[0];

			return new TimeSpan(0);
		}
	}

	public class MinReqController
	{
		public IMinOrderContext Context { get; private set; }

		public List<ReorderingRule> Rules { get; private set; }

		/// <summary>
		/// Временной интервал, который добавляется к текущему времени формирования заказа для проверки:
		/// приблизилось время заказа к времени окончания приема заявок?
		/// Если с учетом этого интервала время заказа стало большим чем время окончания приема заявок,
		/// то будет использоваться следующий интервал.
		/// Пример, время заказа = 18:59:59, OrderEpsilon = 10с, время окончания приема заявок = 19:00:00.
		/// Время заказа с учетом OrderEpsilon больше время окончания приема заявок, поэтому будем рассматривать
		/// интервал [19:00:00 - _следующее время окончания приема заявок_>)
		/// </summary>
		public TimeSpan OrderEpsilon { get; set; }

		public DateTime CurrentOrderTime
		{
			get { return Context.CurrentDateTime.Add(OrderEpsilon); }
		}

		public TimeSpan MinTimeOfStopsOrders { get; private set; }

		public MinReqController(IMinOrderContext context)
		{
			OrderEpsilon = new TimeSpan(0, 0, 10);
			Context = context;
			Rules = null;
		}

		public void ProcessOrder(ClientOrderHeader order)
		{
			if (Context.MinReqEnabled && AllowMinReq()) {
				if (AllowMinReordering()) {
					var period = GetOrderPeriod();
					var ordersExists = Context.OrdersExists(period);
					if (ordersExists)
						CheckMinReordering(order);
					else
						CheckMinOrder(order);
				}
				else
					CheckMinOrder(order);
			}
		}

		public bool AllowMinReq()
		{
			return Context.ControlMinReq && Context.MinReq > 0;
		}

		public bool AllowMinReordering()
		{
			return Context.MinReordering > 0 && AllowReorderingRules();
		}

		public bool AllowReorderingRules()
		{
			var rules = Context.GetRules();

			//если правила не определены, то проверять нечего
			if (rules.Count == 0)
				return false;

			//Если правила опеределены только для некоторых дней недели, то проверять нечего
			if (rules.Count < 7) {
				var logger = LogManager.GetLogger(this.GetType());
				logger.ErrorFormat("Для поставщика {0} ({1}) в регионе {2} ({3}) не определены правила дозаказа для некоторых дней недели.",
					Context.SupplierName, Context.SupplierId,
					Context.RegionName, Context.RegionCode);
				return false;
			}

			//Если для всех дней недели неопределено время окончания приема заказов, то проверять нечего
//			if (rules.TrueForAll(r => !r.TimeOfStopsOrders.HasValue))
//				return false;

//			var currentRule = rules.GetRuleByDayOfWeek(Context.CurrentDateTime);
//			//Если правило для текущего дня недели не содержит
//			if (!currentRule.TimeOfStopsOrders.HasValue)
//				return false;

			Rules = rules;
			MinTimeOfStopsOrders = Rules.GetMinTimeOfStopsOrders();
			return true;
		}

		private void CheckMinOrder(ClientOrderHeader order)
		{
			if (order.Order.CalculateSum() < Context.MinReq) {
				order.SendResult = OrderSendResult.LessThanMinReq;
				order.MinReq = Context.MinReq;
				order.ErrorReason = "Поставщик отказал в приеме заказа.\n Сумма заказа меньше минимально допустимой.";
			}
		}

		public virtual ReorderingPeriod GetOrderPeriod()
		{
			//правило дозаказа для текущего дня недели
			var currentDateRule = Rules.GetRuleByDayOfWeek(CurrentOrderTime);
			//дата и время окончания приема заказов для текущего дня недели
			var currentTimeOfStopsOrders = currentDateRule.GetTimeOfStopsOrders(CurrentOrderTime, MinTimeOfStopsOrders);

			//Если время заказа равно или больше времени окончания приема заказов, то берем следующий период
			if (CurrentOrderTime.CompareTo(currentTimeOfStopsOrders) >= 0) {
				var nextDay = CurrentOrderTime.Date.AddDays(1);
				var nextDateRule = Rules.GetRuleByDayOfWeek(nextDay);
				var nextTimeOfStopsOrders = nextDateRule.GetTimeOfStopsOrders(nextDay, MinTimeOfStopsOrders);
				return new ReorderingPeriod(currentTimeOfStopsOrders, nextTimeOfStopsOrders);
			}
			else {
				//Если время заказа меньше времени окончания приема заказов, то берем предыдущий период
				var prevDay = CurrentOrderTime.Date.AddDays(-1);
				var prevDateRule = Rules.GetRuleByDayOfWeek(prevDay);
				var prevTimeOfStopsOrders = prevDateRule.GetTimeOfStopsOrders(prevDay, MinTimeOfStopsOrders);
				return new ReorderingPeriod(prevTimeOfStopsOrders, currentTimeOfStopsOrders);
			}
		}

		private void CheckMinReordering(ClientOrderHeader order)
		{
			if (order.Order.CalculateSum() < Context.MinReordering) {
				order.SendResult = OrderSendResult.LessThanReorderingMinReq;
				order.MinReq = Context.MinReq;
				order.ErrorReason = "Поставщик отказал в приеме дозаказа.\n Сумма дозаказа меньше минимально допустимой.";
			}
		}
	}
}