using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Castle.ActiveRecord;
using Common.Models;
using Common.Models.Helpers;
using Common.Models.Tests;
using Common.MySql;
using Common.Tools;
using Integration.BaseTests;
using MySql.Data.MySqlClient;
using NHibernate.Exceptions;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.Models;
using Test.Support;
using With = Common.Models.With;

namespace Integration.MinOrders
{
	[TestFixture]
	public class MinOrderContextFixture : UserFixture
	{
		private UnitOfWork _unitOfWork;
		private MySqlConnection _connection;

		private TestUser _user;
		private TestAddress _address;
		private TestClient _client;
		private TestActivePrice _price;

		[SetUp]
		public void SetUp()
		{
			_connection = new MySqlConnection(Settings.ConnectionString());
			_connection.Open();
			_unitOfWork = new UnitOfWork();

			_user = CreateUserWithMinimumPrices();

			using (new SessionScope()) {
				Assert.That(_user.AvaliableAddresses.Count, Is.GreaterThan(0));
				_address = _user.AvaliableAddresses[0];
				_client = _user.Client;
			}

			//todo: прайс-листы сортируются по убыванию позиций, чтобы выбирались прайс-листы с большим кол-вом позиций,
			//т.к. в таких прайс-листах будет меньше Junk-позиций и заказа будет успешно производиться (TestDataManager.GenerateOrder)
			var prices = _user.GetActivePricesList().OrderByDescending(p => p.PositionCount).ToList();
			Assert.That(prices.Count, Is.GreaterThan(0), "У пользователя {0} не найдены активные прайс-листы", _user.Id);
			_price = prices[0];
		}

		[TearDown]
		public void TearDown()
		{
			if (_connection.State == ConnectionState.Open)
				_connection.Close();
			_connection.Dispose();
			_unitOfWork.Dispose();
		}

		private MinOrderContext CreateContext()
		{
			var client = new Client {
				Enabled = true,
				RegionCode = _price.Id.RegionCode,
			};
			var address = new Address(client);
			var order = new Order(new ActivePrice(),
				new User(client) {
					AvaliableAddresses = { address }
				},
				new OrderRules());
			return new MinOrderContext(_connection, _unitOfWork.CurrentSession, order);
		}

		[Test(Description = "Простой тест на создание контекста")]
		public void SimpleCreateContext()
		{
			var context = CreateContext();

			Assert.That(context.ClientId, Is.EqualTo(_client.Id));
			Assert.That(context.AddressId, Is.EqualTo(_address.Id));
			Assert.That(context.UserId, Is.EqualTo(_user.Id));
			Assert.That(context.PriceCode, Is.EqualTo(_price.Id.PriceId));
			Assert.That(context.RegionCode, Is.EqualTo(_price.Id.RegionCode));
			Assert.That(context.MoscowBias, Is.EqualTo(0), "При создании клиента без указания региона должен использоваться регион Воронеж со смещением = 0");

			Assert.IsTrue(context.MinReqEnabled);

			Assert.That(context.SupplierId, Is.EqualTo(_price.Supplier.Id));
			Assert.That(context.SupplierName, Is.EqualTo(_price.Supplier.Name));
			using (new SessionScope()) {
				var region = TestRegion.Find(_price.Id.RegionCode);
				Assert.That(context.RegionName, Is.EqualTo(region.Name));
			}

			//Значение CurrentRegionDateTime должно задаваться при создании контекста
			Assert.That(DateTime.Now.CompareTo(context.CurrentRegionDateTime), Is.GreaterThan(0));

			var rules = context.GetRules();
			Assert.IsNotNull(rules);
			Assert.That(rules.Count, Is.GreaterThanOrEqualTo(0));

			var startDate = DateTime.Now.AddYears(5);
			var ordersExists = context.OrdersExists(new ReorderingPeriod(startDate, startDate.AddDays(1)));
			Assert.IsFalse(ordersExists, "Не должны существовать в будущем заказы");
		}

		[Test(Description = "проверка того, что функционал заблокирован при отсутствии записей в intersection")]
		public void CheckMinReqDisabled()
		{
			var deleteCount = _unitOfWork.CurrentSession
				.CreateSQLQuery(@"
delete
from
	ai
using
Customers.Intersection i
join Customers.AddressIntersection ai on (ai.IntersectionId = i.Id)
join usersettings.pricesregionaldata prd on prd.pricecode = i.PriceId and prd.RegionCode = i.RegionId
where
	(i.ClientId = :clientId)
and (prd.PriceCode =  :priceCode)
and (prd.RegionCode = :regionCode)
and (ai.AddressId = :addressId)")
				.SetParameter("clientId", _client.Id)
				.SetParameter("addressId", _address.Id)
				.SetParameter("priceCode", _price.Id.PriceId)
				.SetParameter("regionCode", _price.Id.RegionCode)
				.ExecuteUpdate();
			Assert.That(deleteCount, Is.GreaterThan(0));

			var context = CreateContext();

			Assert.IsFalse(context.MinReqEnabled);
			Assert.IsFalse(context.ControlMinReq);
			Assert.That(context.MinReq, Is.EqualTo(0));
			Assert.That(context.MinReordering, Is.EqualTo(0));
			Assert.That(context.SupplierId, Is.EqualTo(0));
			Assert.IsNullOrEmpty(context.SupplierName);
			Assert.IsNullOrEmpty(context.RegionName);
		}

		private void DropRules(TestActivePrice price)
		{
			_unitOfWork.CurrentSession
				.CreateSQLQuery(@"
delete
from
	r
using
	UserSettings.pricesdata pd
	join UserSettings.regionalData rd on rd.FirmCode = pd.FirmCode
	inner join UserSettings.ReorderingRules r on r.RegionalDataId = rd.RowId
where
	pd.PriceCode = :priceCode
and rd.RegionCode = :regionCode")
				.SetParameter("priceCode", price.Id.PriceId)
				.SetParameter("regionCode", price.Id.RegionCode)
				.ExecuteUpdate();
		}

		private void CreateRules(TestActivePrice price, int?[] hours)
		{
			if (hours == null)
				return;

			Assert.That(hours.Length, Is.LessThanOrEqualTo(7), "Правил больше чем дней недели");

			var regionalDataId = _unitOfWork.CurrentSession
				.CreateSQLQuery(@"
select
	rd.RowId
from
	UserSettings.pricesdata pd
	join UserSettings.regionalData rd on rd.FirmCode = pd.FirmCode
where
	pd.PriceCode = :priceCode
and rd.RegionCode = :regionCode")
				.SetParameter("priceCode", price.Id.PriceId)
				.SetParameter("regionCode", price.Id.RegionCode)
				.UniqueResult<uint>();

			var weeks = new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday, DayOfWeek.Saturday, DayOfWeek.Sunday };

			With.Transaction(() => {
				for (int i = 0; i < hours.Length; i++) {
					var hour = hours[i];
					var rule = new ReorderingRule {
						DayOfWeek = weeks[i],
						TimeOfStopsOrders = null,
						RegionalDataId = regionalDataId
					};
					if (hour.HasValue)
						rule.TimeOfStopsOrders = new TimeSpan(hour.Value, 0, 0);
					_unitOfWork.CurrentSession.Save(rule);
				}
			});
		}

		[Test(Description = "читаем правила")]
		public void ReadRules()
		{
			DropRules(_price);

			var context = CreateContext();

			var rules = context.GetRules();
			Assert.That(rules.Count, Is.EqualTo(0));

			CreateRules(_price, new int?[] { 19, 19, 19, 19, null, 14, 0 });
			rules = context.GetRules();
			Assert.That(rules.Count, Is.EqualTo(7));
			Assert.That(rules[0].DayOfWeek, Is.EqualTo(DayOfWeek.Monday));
			Assert.That(rules[6].DayOfWeek, Is.EqualTo(DayOfWeek.Sunday));
			Assert.That(rules[4].TimeOfStopsOrders, Is.Null);
			Assert.That(rules[6].TimeOfStopsOrders.Value.Ticks, Is.EqualTo(0));
		}

		[Test(Description = "при попытке продублировать правило должно возникать исключение 'Duplicate entry'")]
		public void RulesUniqueConstraint()
		{
			DropRules(_price);
			CreateRules(_price, new int?[] { 19, 19, 19, 19, null, 14, 0 });

			var context = CreateContext();
			var rules = context.GetRules();
			Assert.That(rules.Count, Is.EqualTo(7));

			var exception = Assert.Throws<GenericADOException>(() => CreateRules(_price, new int?[] { 19 }));
			Assert.That(exception.Message, Is.StringStarting("could not insert"));
			Assert.IsTrue(ExceptionHelper.IsDuplicateEntryExceptionInChain(exception));
		}

		[Test(Description = "проверка работы метода OrderExists")]
		public void SimpleCheckOrderExists()
		{
			var order = TestDataManager.GenerateOrder(3, _user.Id, _address.Id, _price.Id.PriceId);

			var context = CreateContext();

			var existsPeriod = new ReorderingPeriod(order.WriteTime.AddHours(-1), order.WriteTime.AddHours(1));
			var nonExistsPeriod = new ReorderingPeriod(order.WriteTime.AddDays(1), order.WriteTime.AddDays(2));

			//Заказ не помечен, как обработанный, поэтому в обоих периодах не будет найден
			Assert.IsFalse(context.OrdersExists(existsPeriod));
			Assert.IsFalse(context.OrdersExists(nonExistsPeriod));

			With.Transaction(() => {
				order.Processed = true;
				_unitOfWork.CurrentSession.Save(order);
			});

			Assert.IsTrue(context.OrdersExists(existsPeriod));
			Assert.IsFalse(context.OrdersExists(nonExistsPeriod));
		}


		private void CheckOrderExistsByOtherParams(Func<Order> otherOrderCallback)
		{
			Assert.IsNotNull(otherOrderCallback, "Не установлен callback для формирования другого заказа");

			var order = TestDataManager.GenerateOrder(3, _user.Id, _address.Id, _price.Id.PriceId);
			var otherOrder = otherOrderCallback();

			var context = CreateContext();

			var existsPeriod = new ReorderingPeriod(order.WriteTime.AddHours(-1), order.WriteTime.AddHours(1));
			var nonExistsPeriod = new ReorderingPeriod(order.WriteTime.AddDays(1), order.WriteTime.AddDays(2));

			//Заказы не помечены, как обработанные, поэтому в обоих периодах не будут найдены
			Assert.IsFalse(context.OrdersExists(existsPeriod));
			Assert.IsFalse(context.OrdersExists(nonExistsPeriod));

			With.Transaction(() => {
				otherOrder.Processed = true;
				_unitOfWork.CurrentSession.Save(order);
			});
			Assert.IsFalse(context.OrdersExists(existsPeriod));
			Assert.IsFalse(context.OrdersExists(nonExistsPeriod));

			//Помечаем заказ под корректным пользователем как обработанный и тогда в периоде existsPeriod будут заказы
			With.Transaction(() => {
				order.Processed = true;
				_unitOfWork.CurrentSession.Save(order);
			});
			Assert.IsTrue(context.OrdersExists(existsPeriod));
			Assert.IsFalse(context.OrdersExists(nonExistsPeriod));
		}

		[Test(Description = "проверка работы метода OrderExists")]
		public void CheckOrderExistsByOtherUser()
		{
			TestUser otherUser;

			using (var transaction = new TransactionScope()) {
				otherUser = _client.CreateUser();
				otherUser.JoinAddress(_address);
				otherUser.InheritPricesFrom = _user;
				_client.Update();
				transaction.VoteCommit();
			}

			CheckOrderExistsByOtherParams(() => TestDataManager.GenerateOrder(3, otherUser.Id, _address.Id, _price.Id.PriceId));
		}

		[Test(Description = "проверка работы метода OrderExists")]
		public void CheckOrderExistsByOtherAddress()
		{
			TestAddress otherAddress;

			using (var transaction = new TransactionScope()) {
				otherAddress = _client.CreateAddress();
				_user.JoinAddress(otherAddress);
				_client.Update();
				transaction.VoteCommit();
			}

			CheckOrderExistsByOtherParams(() => TestDataManager.GenerateOrder(3, _user.Id, otherAddress.Id, _price.Id.PriceId));
		}

		[Test(Description = "проверка работы метода OrderExists")]
		public void CheckOrderExistsByClient()
		{
			var otherUser = CreateUserWithMinimumPrices();
			var prices = otherUser.GetActivePricesList();
			Assert.IsNotNull(prices.First(p => p.Id.Equals(_price.Id)));

			CheckOrderExistsByOtherParams(() => TestDataManager.GenerateOrder(3, otherUser.Id, otherUser.AvaliableAddresses[0].Id, _price.Id.PriceId));
		}

		[Test(Description = "проверка работы метода OrderExists")]
		public void CheckOrderExistsByPrice()
		{
			var prices = _user.GetActivePricesList().OrderByDescending(p => p.PositionCount).ToList();
			var otherPrice = prices.First(p => p.Id.PriceId != _price.Id.PriceId);

			CheckOrderExistsByOtherParams(() => TestDataManager.GenerateOrder(3, _user.Id, _address.Id, otherPrice.Id.PriceId));
		}

		private void CheckOrderExistsByContext(IMinOrderContext context, DateTime startTime, DateTime endTime, bool exists)
		{
			var period = new ReorderingPeriod(startTime, endTime);
			Assert.That(context.OrdersExists(period), Is.EqualTo(exists));
		}

		[Test(Description = "проверяем работу с региональным смещением")]
		public void CheckMoscowBias()
		{
			var client = TestClient.Create(64, 64);

			var region = TestRegion.Find(client.RegionCode);
			Assert.That(region.ShortAliase, Is.EqualTo("chel"));

			TestUser user;
			using (var transaction = new TransactionScope()) {
				user = client.Users[0];

				client.Users.Each(u => {
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				user.Update();
			}

			var prices = user.GetActivePricesList();
			var price = prices
				.OrderByDescending(p => p.PositionCount)
				.FirstOrDefault(p => p.CoreCount() > 0);
			Assert.That(price, Is.Not.Null, "Не найден прайс лист с предложениями в регионе Челябинск");

			//Выполняем проверки существования заказа относительно регионального времени
			var order = TestDataManager.GenerateOrder(3, user.Id, user.AvaliableAddresses[0].Id, price.Id.PriceId);

			var conext = new MinOrderContext(
				_connection, _unitOfWork.CurrentSession,
				order);

			//Для Челябинска смещение должно быть = 2
			Assert.That(conext.MoscowBias, Is.EqualTo(2));
			var timeSpan = conext.CurrentRegionDateTime.Subtract(DateTime.Now.AddMinutes(-1));
			Assert.That(timeSpan.TotalHours, Is.GreaterThanOrEqualTo(conext.MoscowBias));


			//Время отправки заказа относительно времени региона
			var regionalOrderTime = order.WriteTime.AddHours(conext.MoscowBias);

			//Заказ не помечен, как обработанный, поэтому не будет найден
			CheckOrderExistsByContext(conext, DateTime.Now.Date, DateTime.Now.Date.AddDays(1), false);

			With.Transaction(() => {
				order.Processed = true;
				_unitOfWork.CurrentSession.Save(order);
			});

			//проверка регионального периода: от (времени отправки заказа + 1 час) до начала следующего дня
			CheckOrderExistsByContext(conext, regionalOrderTime.AddHours(1), regionalOrderTime.Date.AddDays(1), false);
			//проверка регионального периода: от начал дня до (времени отправки заказа - 1 час)
			//проверка имеет смысл, если regionalOrderTime больше часа ночи, т.к. в ином случае
			//выражение regionalOrderTime.AddHours(-1) возвращает дату предыдущих суток, которая меньше чем
			//дата начала суток regionalOrderTime
			if (regionalOrderTime.Hour >= 1)
				CheckOrderExistsByContext(conext, regionalOrderTime.Date, regionalOrderTime.AddHours(-1), false);
			else
				//если попадаем не предыдущие сутки, то берем дату начала предыдущих суток
				CheckOrderExistsByContext(conext, regionalOrderTime.AddDays(-1).Date, regionalOrderTime.AddHours(-1), false);
			//проверка регионального периода: от (времени отправки заказа + 1 сек) до начала следующего дня
			CheckOrderExistsByContext(conext, regionalOrderTime.AddSeconds(1), regionalOrderTime.Date.AddDays(1), false);

			//проверка регионального периода: указываем период, в который заказ точно должен быть отправлен
			CheckOrderExistsByContext(conext, regionalOrderTime.AddSeconds(-10), regionalOrderTime.AddSeconds(10), true);
		}
	}
}