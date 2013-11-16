using System;
using System.Collections.Generic;
using Common.Models;
using Common.Models.Helpers;
using NUnit.Framework;
using PrgData.Common.Models;
using PrgData.Common.Orders;
using Rhino.Mocks;
using Rhino.Mocks.Constraints;
using log4net.Config;
using Is = NUnit.Framework.Is;

namespace Unit.MinOrders
{
	public class BaseMinReqControllerFixture
	{
		protected DateTime GetDefaultDate()
		{
			return new DateTime(2013, 1, 24, 13, 0, 0);
		}

		protected List<ReorderingRule> GetDefaultRules()
		{
			return new List<ReorderingRule> {
				new ReorderingRule(DayOfWeek.Monday, 19),
				new ReorderingRule(DayOfWeek.Tuesday, 19),
				new ReorderingRule(DayOfWeek.Wednesday, 19),
				new ReorderingRule(DayOfWeek.Thursday, 19),
				new ReorderingRule(DayOfWeek.Friday, 19),
				new ReorderingRule(DayOfWeek.Saturday, 14),
				new ReorderingRule(DayOfWeek.Sunday, 0),
			};
		}
	}

	[TestFixture]
	public class MinReqControllerFixture : BaseMinReqControllerFixture
	{
		private MockRepository _mockRepository;

		private IMinOrderContext _minOrderContext;

		private DateTime _currentDateTime;
		private bool _minReqEnabled;
		private bool _controlMinReq;
		private uint _minReq;
		private uint _minReordering;
		private bool _supportedMinReordering;

		[SetUp]
		public void Setup()
		{
			_currentDateTime = GetDefaultDate();
			_minReqEnabled = false;
			_controlMinReq = false;
			_minReq = 0;
			_minReordering = 0;
			_supportedMinReordering = false;

			_mockRepository = new MockRepository();

			_minOrderContext = _mockRepository.StrictMock<IMinOrderContext>();

			_minOrderContext.Expect(x => x.CurrentRegionDateTime).Do((Func<DateTime>)(() => _currentDateTime)).Repeat.Any();
		}

		public ClientOrderHeader CreateOrderWithSum(float sum)
		{
			var clientOrder = new ClientOrderHeader();

			var client = new Client {
				Enabled = true
			};
			var user = new User(client);
			var address = new Address(client);
			user.AvaliableAddresses.Add(address);
			clientOrder.Order = new Order(new ActivePrice { Id = new PriceKey(new PriceList()) }, user, new OrderRules());
			clientOrder.Order.AddOrderItem(new Offer { Id = new OfferKey(654654879879, 1), Cost = sum }, 1);

			Assert.That(clientOrder.Order.CalculateSum(), Is.EqualTo(sum));
			clientOrder.SendResult = OrderSendResult.Success;

			return clientOrder;
		}

		[TearDown]
		public void TearDown()
		{
			_mockRepository.VerifyAll();
		}

		[Test(Description = "проверяем простейшее создание контроллера и установку свойств по умолчанию")]
		public void SimpleCreateMinReqController()
		{
			_mockRepository.ReplayAll();

			var controller = new MinReqController(_minOrderContext);
			Assert.That(controller.Rules, Is.Null);
			Assert.That(controller.OrderEpsilon.TotalSeconds, Is.EqualTo(10), "По умолчанию OrderEpsilon должно быть равно 10 секундам");
			Assert.That(controller.CurrentOrderTime, Is.EqualTo(new DateTime(2013, 1, 24, 13, 0, 10)));
		}

		private void OrderSuccess(ClientOrderHeader order, MinReqController controller)
		{
			order.Apply(controller.ProcessOrder(order.Order));
			Assert.That(order.SendResult, Is.EqualTo(OrderSendResult.Success));
		}

		private void OrderByMinReq(ClientOrderHeader order, MinReqController controller)
		{
			order.Apply(controller.ProcessOrder(order.Order));
			Assert.That(order.SendResult, Is.EqualTo(OrderSendResult.LessThanMinReq));
			Assert.That(order.MinReq, Is.EqualTo(_minOrderContext.MinReq));
			Assert.That(order.ErrorReason, Is.EqualTo("Поставщик отказал в приеме заказа.\n Сумма заказа меньше минимально допустимой."));
		}

		private void OrderByMinReordering(ClientOrderHeader order, MinReqController controller)
		{
			order.Apply(controller.ProcessOrder(order.Order));
			Assert.That(order.SendResult, Is.EqualTo(OrderSendResult.LessThanReorderingMinReq));
			Assert.That(order.MinReq, Is.EqualTo(_minOrderContext.MinReq));
			Assert.That(order.MinReordering, Is.EqualTo(_minOrderContext.MinReordering));
			Assert.That(order.ErrorReason, Is.EqualTo("Поставщик отказал в приеме дозаказа.\n Сумма дозаказа меньше минимально допустимой."));
		}

		[Test(Description = "фунционал проверки минимального заказа не должен быть вызван, т.к. не найдены записи с настройками")]
		public void DisableMinReq()
		{
			_minOrderContext.Expect(x => x.MinReqEnabled).Do((Func<bool>)(() => _minReqEnabled)).Repeat.Any();
			_mockRepository.ReplayAll();

			var controller = new MinReqController(_minOrderContext);
			var order = CreateOrderWithSum(10);
			OrderSuccess(order, controller);
		}

		[Test(Description = "фунционал проверки минимального заказа не должен быть вызван, т.к. сброшен флаг ControlMinReq")]
		public void ControlMinReqOff()
		{
			_minReqEnabled = true;
			_minOrderContext.Expect(x => x.MinReqEnabled).Do((Func<bool>)(() => _minReqEnabled)).Repeat.Any();
			_minOrderContext.Expect(x => x.ControlMinReq).Do((Func<bool>)(() => _controlMinReq)).Repeat.Any();
			_mockRepository.ReplayAll();

			var controller = new MinReqController(_minOrderContext);
			var order = CreateOrderWithSum(10);
			OrderSuccess(order, controller);
		}

		[Test(Description = "фунционал проверки минимального заказа не должен быть вызван, т.к. значение MinReq = 0")]
		public void MinReqIsZero()
		{
			_minReqEnabled = true;
			_controlMinReq = true;
			_minOrderContext.Expect(x => x.MinReqEnabled).Do((Func<bool>)(() => _minReqEnabled)).Repeat.Any();
			_minOrderContext.Expect(x => x.ControlMinReq).Do((Func<bool>)(() => _controlMinReq)).Repeat.Any();
			_minOrderContext.Expect(x => x.MinReq).Do((Func<uint>)(() => _minReq)).Repeat.Any();
			_mockRepository.ReplayAll();

			var controller = new MinReqController(_minOrderContext);
			var order = CreateOrderWithSum(10);
			OrderSuccess(order, controller);
		}

		[Test(Description = "фунционал проверки минимального дозаказа не должен быть вызван, т.к. приложение не поддерживает минимальный дозаказ")]
		public void MinReorderingDisabledThatDontSupported()
		{
			_minReqEnabled = true;
			_controlMinReq = true;
			_minReq = 9;
			_minOrderContext.Expect(x => x.MinReqEnabled).Do((Func<bool>)(() => _minReqEnabled)).Repeat.Any();
			_minOrderContext.Expect(x => x.ControlMinReq).Do((Func<bool>)(() => _controlMinReq)).Repeat.Any();
			_minOrderContext.Expect(x => x.MinReq).Do((Func<uint>)(() => _minReq)).Repeat.Any();
			_minOrderContext.Expect(x => x.SupportedMinReordering).Do((Func<bool>)(() => _supportedMinReordering)).Repeat.Any();
			_mockRepository.ReplayAll();

			var controller = new MinReqController(_minOrderContext);
			var order = CreateOrderWithSum(10);
			OrderSuccess(order, controller);
		}

		private void SetContext(Action action = null)
		{
			_minOrderContext.Expect(x => x.MinReqEnabled).Do((Func<bool>)(() => _minReqEnabled)).Repeat.Any();
			_minOrderContext.Expect(x => x.ControlMinReq).Do((Func<bool>)(() => _controlMinReq)).Repeat.Any();
			_minOrderContext.Expect(x => x.MinReq).Do((Func<uint>)(() => _minReq)).Repeat.Any();
			_minOrderContext.Expect(x => x.SupportedMinReordering).Do((Func<bool>)(() => _supportedMinReordering)).Repeat.Any();
			_minOrderContext.Expect(x => x.MinReordering).Do((Func<uint>)(() => _minReordering)).Repeat.Any();

			if (action != null)
				action();

			_mockRepository.ReplayAll();
		}

		[Test(Description = "заказ разрешен к отправке, т.к. сумма заказа больше минимальной суммы заказа")]
		public void MinReqLessThanOrder()
		{
			_minReqEnabled = true;
			_controlMinReq = true;
			_minReq = 5;
			SetContext();

			var controller = new MinReqController(_minOrderContext);
			var order = CreateOrderWithSum(10);
			OrderSuccess(order, controller);
		}

		[Test(Description = "заказ запрещен к отправке, т.к. сумма заказа меньше минимальной суммы заказа")]
		public void MinReqGreaterThanOrder()
		{
			_minReqEnabled = true;
			_controlMinReq = true;
			_minReq = 15;
			SetContext();

			var controller = new MinReqController(_minOrderContext);
			var order = CreateOrderWithSum(10);
			OrderByMinReq(order, controller);
		}

		[Test(Description = "заказ разрешен к отправке, т.к. сумма заказа больше минимальной суммы заказа, установлено значение 'Минимальная сумма дозаявки', но нет правил дозаявки")]
		public void SetMinReorderingButEmptyRules()
		{
			_minReqEnabled = true;
			_controlMinReq = true;
			_minReq = 9;
			_minReordering = 1;
			SetContext(() => { _minOrderContext.Expect(x => x.GetRules()).Return(new List<ReorderingRule>()).Repeat.Any(); });

			var controller = new MinReqController(_minOrderContext);
			var order = CreateOrderWithSum(10);
			OrderSuccess(order, controller);
		}

		[Test(Description = "заказ запрещен к отправке, т.к. сумма заказа больше минимальной суммы заказа, установлено значение 'Минимальная сумма дозаявки', но нет правил дозаявки")]
		public void SetMinReorderingButEmptyRulesAndDisabledByMinReq()
		{
			_minReqEnabled = true;
			_controlMinReq = true;
			_minReq = 15;
			_minReordering = 6;
			SetContext(() => { _minOrderContext.Expect(x => x.GetRules()).Return(new List<ReorderingRule>()).Repeat.Any(); });

			var controller = new MinReqController(_minOrderContext);
			var order = CreateOrderWithSum(10);
			OrderByMinReq(order, controller);
		}

		public class TestMinReqController : MinReqController
		{
			public TestMinReqController(IMinOrderContext context) : base(context)
			{
			}

			public override ReorderingPeriod GetOrderPeriod()
			{
				return new ReorderingPeriod(DateTime.Now, DateTime.Now.AddDays(1));
			}
		}

		private void SetupContextForMinReordering(bool orderExists)
		{
			_minReqEnabled = true;
			_controlMinReq = true;
			_minReq = 15;
			_minReordering = 6;
			_supportedMinReordering = true;
			SetContext(() => {
				_minOrderContext.Expect(x => x.GetRules()).Return(GetDefaultRules()).Repeat.Any();
				_minOrderContext.Expect(x => x.OrdersExists(null)).IgnoreArguments().Return(orderExists).Repeat.Any();
			});
		}

		[Test(Description = "настроены параметры для дозаказа, но не существуют заказа в периоде приема заявок")]
		public void SetMinReorderingButOrdersNotExists()
		{
			SetupContextForMinReordering(false);

			var controller = new TestMinReqController(_minOrderContext);
			var order = CreateOrderWithSum(8);
			OrderByMinReq(order, controller);
		}

		[Test(Description = "настроены параметры для дозаказа и существуют заказы в периоде приема заявок")]
		public void SetMinReorderingAndOrdersExists()
		{
			SetupContextForMinReordering(true);

			var controller = new TestMinReqController(_minOrderContext);
			var order = CreateOrderWithSum(8);
			OrderSuccess(order, controller);
		}

		[Test(Description = "настроены параметры для дозаказа и существуют заказы в периоде приема заявок, но заблокировано по сумме минимального дозаказа")]
		public void SetMinReorderingAndOrdersExistsAndDisabledByMinReordering()
		{
			SetupContextForMinReordering(true);

			var controller = new TestMinReqController(_minOrderContext);
			var order = CreateOrderWithSum(5);
			OrderByMinReordering(order, controller);
		}
	}

	[TestFixture]
	public class MinReqControllerOrderPeriodFixture : BaseMinReqControllerFixture
	{
		private MockRepository _mockRepository;

		private IMinOrderContext _minOrderContext;

		private DateTime _currentDateTime;

		private List<ReorderingRule> _rules;

		[SetUp]
		public void Setup()
		{
			_currentDateTime = GetDefaultDate();

			_rules = GetDefaultRules();

			_mockRepository = new MockRepository();

			_minOrderContext = _mockRepository.StrictMock<IMinOrderContext>();

			_minOrderContext.Expect(x => x.CurrentRegionDateTime).Do((Func<DateTime>)(() => _currentDateTime)).Repeat.Any();

			_minOrderContext.Expect(x => x.MinReqEnabled).Return(true).Repeat.Any();
			_minOrderContext.Expect(x => x.ControlMinReq).Return(true).Repeat.Any();
			_minOrderContext.Expect(x => x.MinReq).Return(20).Repeat.Any();
			_minOrderContext.Expect(x => x.MinReordering).Return(10).Repeat.Any();

			_minOrderContext.Expect(x => x.SupplierId).Return(1).Repeat.Any();
			_minOrderContext.Expect(x => x.SupplierName).Return("_тест поставщик_").Repeat.Any();
			_minOrderContext.Expect(x => x.RegionCode).Return(3).Repeat.Any();
			_minOrderContext.Expect(x => x.RegionName).Return("_тест регион_").Repeat.Any();

			_minOrderContext.Expect(x => x.GetRules()).Do((Func<List<ReorderingRule>>)(() => _rules)).Repeat.Any();
			_mockRepository.ReplayAll();
		}

		[Test(Description = "правила не приняты, т.к. не хватает записей")]
		public void RulesNotAllowed()
		{
			_rules.RemoveAt(0);

			var controller = new MinReqController(_minOrderContext);
			Assert.IsFalse(controller.AllowReorderingRules());
			Assert.IsNull(controller.Rules);
		}

		[Test(Description = "правила приняты")]
		public void RulesAllowed()
		{
			var controller = new MinReqController(_minOrderContext);
			Assert.IsTrue(controller.AllowReorderingRules());
			Assert.IsNotNull(controller.Rules);
		}

		private void ClearTimeOfStopsOrders()
		{
			_rules.ForEach(r => r.TimeOfStopsOrders = null);
		}

		[Test(Description = "правила не приняты, т.к. для всех записей не опеределено время окончания приема заявок")]
		public void RulesNotAllowedByNulls()
		{
			ClearTimeOfStopsOrders();

			var controller = new MinReqController(_minOrderContext);
			Assert.IsFalse(controller.AllowReorderingRules());
			Assert.IsNull(controller.Rules);
		}

		[Test(Description = "правила приняты, т.к. есть одна запись с установленным временем окончания приема заявок")]
		public void RulesAllowedByOne()
		{
			ClearTimeOfStopsOrders();
			_rules[3].TimeOfStopsOrders = new TimeSpan(0);

			var controller = new MinReqController(_minOrderContext);
			Assert.IsTrue(controller.AllowReorderingRules());
			Assert.IsNotNull(controller.Rules);
		}

		private void CheckOrderPeriod(DateTime startTime, DateTime endTime)
		{
			var controller = new MinReqController(_minOrderContext);
			Assert.IsTrue(controller.AllowReorderingRules());
			Assert.IsNotNull(controller.Rules);
			var period = controller.GetOrderPeriod();
			Assert.IsNotNull(period);
			Assert.That(period.StartTime, Is.EqualTo(startTime));
			Assert.That(period.EndTime, Is.EqualTo(endTime));
		}

		[Test(Description = "простой вызов метода GetOrderPeriod")]
		public void SimpleGetOrderPeriod()
		{
			CheckOrderPeriod(new DateTime(2013, 1, 23, 19, 0, 0), new DateTime(2013, 1, 24, 19, 0, 0));
		}

		[Test(Description = "вызов метода GetOrderPeriod при нулевой дате окончания")]
		public void GetOrderPeriodWithZeroEnd()
		{
			_rules[3].TimeOfStopsOrders = new TimeSpan(0);
			CheckOrderPeriod(new DateTime(2013, 1, 23, 19, 0, 0), new DateTime(2013, 1, 25, 0, 0, 0));
		}

		[Test(Description = "вызов метода GetOrderPeriod при нулевых датах окончания и начала")]
		public void GetOrderPeriodWithZeroEndAndStart()
		{
			_rules[2].TimeOfStopsOrders = new TimeSpan(0);
			_rules[3].TimeOfStopsOrders = new TimeSpan(0);
			CheckOrderPeriod(new DateTime(2013, 1, 24, 0, 0, 0), new DateTime(2013, 1, 25, 0, 0, 0));
		}

		[Test(Description = "вызов метода GetOrderPeriod при нулевых датах окончания")]
		public void GetOrderPeriodWithAllZeroEnd()
		{
			ClearTimeOfStopsOrders();
			_rules[0].TimeOfStopsOrders = new TimeSpan(0);

			CheckOrderPeriod(new DateTime(2013, 1, 22, 0, 0, 0), new DateTime(2013, 1, 29, 0, 0, 0));
		}

		[Test(Description = "вызов метода GetOrderPeriod при дате старшей даты окончания за текущий день недели")]
		public void GetOrderPeriodAfterEndTime()
		{
			_currentDateTime = new DateTime(2013, 1, 24, 20, 0, 0);
			CheckOrderPeriod(new DateTime(2013, 1, 24, 19, 0, 0), new DateTime(2013, 1, 25, 19, 0, 0));
		}

		[Test(Description = "вызов метода GetOrderPeriod при дате приближающейся к дате окончания за текущий день недели")]
		public void GetOrderPeriodWithEpsilon()
		{
			_currentDateTime = new DateTime(2013, 1, 24, 18, 59, 57);
			CheckOrderPeriod(new DateTime(2013, 1, 24, 19, 0, 0), new DateTime(2013, 1, 25, 19, 0, 0));
		}

		[Test(Description = "вызов метода GetOrderPeriod при дате заказа, переходящей на следующий день")]
		public void GetOrderPeriodWithEpsilonOnNextDay()
		{
			_currentDateTime = new DateTime(2013, 1, 24, 23, 59, 57);
			CheckOrderPeriod(new DateTime(2013, 1, 24, 19, 0, 0), new DateTime(2013, 1, 25, 19, 0, 0));
		}

		[Test(Description = "вызов метода GetOrderPeriod при сложно заданном расписании")]
		public void GetOrderPeriodByDifficult()
		{
			//пн - 19:00, вт - 19:00, ср - Null, чт - Null, пт - Null, сб - 14:00, вс - Null или пустота
			_rules[0].TimeOfStopsOrders = new TimeSpan(19, 0, 0);
			_rules[1].TimeOfStopsOrders = new TimeSpan(19, 0, 0);
			_rules[2].TimeOfStopsOrders = null;
			_rules[3].TimeOfStopsOrders = null;
			_rules[4].TimeOfStopsOrders = null;
			_rules[5].TimeOfStopsOrders = new TimeSpan(14, 0, 0);
			_rules[6].TimeOfStopsOrders = null;
			_currentDateTime = new DateTime(2013, 1, 24, 15, 0, 0);
			CheckOrderPeriod(new DateTime(2013, 1, 22, 19, 0, 0), new DateTime(2013, 1, 26, 14, 0, 0));
		}

		[Test(Description = "вызов метода GetOrderPeriod при дате приближающейся к дате окончания за текущий день недели с Epsilon = 0")]
		public void GetOrderPeriodWithZeroEpsilon()
		{
			_currentDateTime = new DateTime(2013, 1, 24, 18, 59, 57);

			var controller = new MinReqController(_minOrderContext);
			controller.OrderEpsilon = new TimeSpan(0);
			Assert.IsTrue(controller.AllowReorderingRules());
			Assert.IsNotNull(controller.Rules);
			var period = controller.GetOrderPeriod();
			Assert.IsNotNull(period);
			Assert.That(period.StartTime, Is.EqualTo(new DateTime(2013, 1, 23, 19, 0, 0)));
			Assert.That(period.EndTime, Is.EqualTo(new DateTime(2013, 1, 24, 19, 0, 0)));
		}
	}
}