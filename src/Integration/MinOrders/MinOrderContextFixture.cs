using System;
using System.Data;
using Castle.ActiveRecord;
using Common.Models;
using Common.Models.Tests;
using Common.MySql;
using Integration.BaseTests;
using MySql.Data.MySqlClient;
using NHibernate.Exceptions;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.Models;
using PrgData.Common.Orders.MinOrders;
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

			var prices = _user.GetActivePricesList();
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
			return new MinOrderContext(
				_connection, _unitOfWork.CurrentSession,
				_client.Id, _address.Id, _user.Id,
				_price.Id.PriceId, _price.Id.RegionCode);
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

			Assert.IsTrue(context.MinReqEnabled);

			Assert.That(context.SupplierId, Is.EqualTo(_price.Supplier.Id));
			Assert.That(context.SupplierName, Is.EqualTo(_price.Supplier.Name));
			using (new SessionScope()) {
				var region = TestRegion.Find(_price.Id.RegionCode);
				Assert.That(context.RegionName, Is.EqualTo(region.Name));
			}

			//Значение CurrentDateTime должно задаваться при создании контекста
			Assert.That(DateTime.Now.CompareTo(context.CurrentDateTime), Is.GreaterThan(0));

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

		private void DropRules()
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
				.SetParameter("priceCode", _price.Id.PriceId)
				.SetParameter("regionCode", _price.Id.RegionCode)
				.ExecuteUpdate();
		}

		private void CreateRules(int?[] hours)
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
				.SetParameter("priceCode", _price.Id.PriceId)
				.SetParameter("regionCode", _price.Id.RegionCode)
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
			DropRules();

			var context = CreateContext();

			var rules = context.GetRules();
			Assert.That(rules.Count, Is.EqualTo(0));

			CreateRules(new int?[] { 19, 19, 19, 19, null, 14, 0 });
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
			DropRules();
			CreateRules(new int?[] { 19, 19, 19, 19, null, 14, 0 });

			var context = CreateContext();
			var rules = context.GetRules();
			Assert.That(rules.Count, Is.EqualTo(7));

			var exception = Assert.Throws<GenericADOException>(() => CreateRules(new int?[] { 19 }));
			Assert.That(exception.Message, Is.StringStarting("could not insert"));
			Assert.IsTrue(ExceptionHelper.IsDuplicateEntryExceptionInChain(exception));
		}

		[Test(Description = "проверка работы метода OrderExists")]
		public void CheckOrderExists()
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
	}
}