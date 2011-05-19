using System;
using System.Data;
using System.Linq;
using Castle.ActiveRecord;
using Common.Tools;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;
using Test.Support.Helpers;

namespace Integration
{

	public enum BuyinMatrixStatus
	{
		Allow = 0,
		Denied = 1,
		Warning = 2
	}

	[TestFixture(Description = "Тесты для матрицы закупок и матрицы предложений")]
	public class MatrixFixture
	{

		TestClient _client;
		TestUser _user;

		private TestActivePrice _buyingPrice;
		private TestActivePrice _offerPrice;

		private uint _intersectionProductId;
		private uint _buyingProductId;
		private uint _offerProductId;

		private long _buyingCoreCount;
		private long _offerCoreCount;

		[TestFixtureSetUp]
		public void FixtureSetUp()
		{
			_client = TestClient.Create();

			using (var transaction = new TransactionScope())
			{
				_user = _client.Users[0];

				var permission = TestUserPermission.ByShortcut("AF");
				_client.Users.Each(u =>
				{
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				_user.Update();
			}

			var list = _user.GetActivePricesList();
			_buyingPrice = list.First(item => item.PositionCount > 900);
			_buyingCoreCount = _buyingPrice.CoreCount();

			var otherList =
				list.Where(item => item.Supplier != _buyingPrice.Supplier && item.PositionCount > 800).OrderBy(
					item => item.PositionCount);

			_offerPrice = GetOfferPrice(otherList);

			Assert.That(_offerPrice, Is.Not.Null, "Не нашли прайс-лист, удовлетворяющий условию теста: должны быть пересечения и уникальные продукты с прайс-листом {0}", _buyingPrice.Id.PriceId);
			_offerCoreCount = _offerPrice.CoreCount();

			SessionHelper.WithSession(
				s =>
					{
						s.CreateSQLQuery(
							"delete from future.UserPrices where UserId = :userId and PriceId not in (:buyingPriceId, :offerPriceId);")
							.SetParameter("userId", _user.Id)
							.SetParameter("buyingPriceId", _buyingPrice.Id.PriceId)
							.SetParameter("offerPriceId", _offerPrice.Id.PriceId)
							.ExecuteUpdate();
					});
		}

		private TestActivePrice GetOfferPrice(IOrderedEnumerable<TestActivePrice> otherList)
		{
			TestActivePrice result = null;

			foreach (var testActivePrice in otherList)
			{
				_intersectionProductId = SessionHelper.WithSession<uint>(
					s => s.CreateSQLQuery(@"
select 
	c.ProductId
from
  farm.Core0 c
  join farm.CoreCosts cc on cc.Core_Id = c.Id and cc.PC_CostCode = :costId
  join farm.Core0 n on n.ProductId = c.ProductId and n.PriceCode = :otherPriceId
  join farm.CoreCosts nc on nc.Core_Id = n.Id and nc.PC_CostCode = :otherCostId
where
  c.PriceCode = :priceId
limit 1
"
							)
					     	.SetParameter("priceId", _buyingPrice.Id.PriceId)
							.SetParameter("costId", _buyingPrice.CostId)
							.SetParameter("otherPriceId", testActivePrice.Id.PriceId)
							.SetParameter("otherCostId", testActivePrice.CostId)
							.UniqueResult<uint>());

				_buyingProductId = SessionHelper.WithSession<uint>(
					s => s.CreateSQLQuery(@"
select 
	c.ProductId
from
  farm.Core0 c
  join farm.CoreCosts cc on cc.Core_Id = c.Id and cc.PC_CostCode = :costId
  left join farm.Core0 n on n.ProductId = c.ProductId and n.PriceCode = :otherPriceId
  left join farm.CoreCosts nc on nc.Core_Id = n.Id and nc.PC_CostCode = :otherCostId
where
  c.PriceCode = :priceId
and nc.Core_Id is null
limit 1
"
							)
					     	.SetParameter("priceId", _buyingPrice.Id.PriceId)
							.SetParameter("costId", _buyingPrice.CostId)
							.SetParameter("otherPriceId", testActivePrice.Id.PriceId)
							.SetParameter("otherCostId", testActivePrice.CostId)
							.UniqueResult<uint>());

				_offerProductId = SessionHelper.WithSession<uint>(
					s => s.CreateSQLQuery(@"
select 
	c.ProductId
from
  farm.Core0 c
  join farm.CoreCosts cc on cc.Core_Id = c.Id and cc.PC_CostCode = :costId
  left join farm.Core0 n on n.ProductId = c.ProductId and n.PriceCode = :otherPriceId
  left join farm.CoreCosts nc on nc.Core_Id = n.Id and nc.PC_CostCode = :otherCostId
where
  c.PriceCode = :priceId
and n.Id is null
limit 1
"
							)
					     	.SetParameter("priceId", testActivePrice.Id.PriceId)
							.SetParameter("costId", testActivePrice.CostId)
							.SetParameter("otherPriceId", _buyingPrice.Id.PriceId)
							.SetParameter("otherCostId", _buyingPrice.CostId)
							.UniqueResult<uint>());

				if (_intersectionProductId > 0 && _buyingProductId > 0 && _offerProductId > 0)
				{
					result = testActivePrice;
					break;
				}
			}

			return result;
		}

		[SetUp]
		public void SetUp()
		{
			var settings = TestDrugstoreSettings.Find(_client.Id);
			settings.BuyingMatrixPriceId = null;
			settings.OfferMatrixPriceId = null;
			settings.WarningOnBuyingMatrix = false;
			settings.UpdateAndFlush();

			ClearOfferMatrixSuppliers();
			ClearMatrix(_buyingPrice);
			ClearMatrix(_offerPrice);
		}

		private void ClearMatrix(TestActivePrice price)
		{
			SessionHelper.WithSession(
				s =>
				{
					s.CreateSQLQuery(
						"delete from farm.BuyingMatrix where PriceId = :priceId;")
						.SetParameter("priceId", price.Id.PriceId)
						.ExecuteUpdate();
				});
		}

		private void InsertMatrix(TestActivePrice price)
		{
			SessionHelper.WithSession(
				s =>
				{
					//здесь сделано без учета производителей
					s.CreateSQLQuery(
						@"
insert into farm.BuyingMatrix(PriceId, Code, ProductId, ProducerId)
select c0.PriceCode, c0.Code, c0.ProductId, null
from farm.Core0 c0
where pricecode = :priceId
group by c0.ProductId;")
						.SetParameter("priceId", price.Id.PriceId)
						.ExecuteUpdate();
				});
		}

		private void ClearOfferMatrixSuppliers()
		{
			SessionHelper.WithSession(
				s =>
				{
					s.CreateSQLQuery(
						"delete from UserSettings.OfferMatrixSuppliers where ClientId = :clientId;")
						.SetParameter("clientId", _client.Id)
						.ExecuteUpdate();
				});
		}

		private void InserOfferMatrixSuppliers(TestActivePrice price)
		{
			SessionHelper.WithSession(
				s =>
				{
					s.CreateSQLQuery(
						"insert into UserSettings.OfferMatrixSuppliers (ClientId, SupplierId) values (:clientId, :supplierId);")
						.SetParameter("clientId", _client.Id)
						.SetParameter("supplierId", price.Price.Supplier.Id)
						.ExecuteUpdate();
				});
		}

		private void CheckOffers(Action<UpdateHelper, DataTable>  action)
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				helper.Cleanup();

				helper.SelectPrices();
				helper.SelectReplicationInfo();
				helper.SelectActivePrices();

				helper.SelectOffers();

				var coreSql = helper.GetCoreCommand(false, true, true, false);

				var dataAdapter = new MySqlDataAdapter(coreSql, connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?Cumulative", 0);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", _client.Id);
				var coreTable = new DataTable();

				dataAdapter.Fill(coreTable);

				if (updateData.BuyingMatrixPriceId.HasValue)
					Assert.That(coreTable.Columns.Contains("BuyingMatrixType"), Is.True, "Не найден столбец BuyingMatrixType хотя механизм матрицы закупок подключен");

				action(helper, coreTable);
			}
		}

		[Test(Description = "Должны быть все предложения без настроенных матриц")]
		public void AllOffers()
		{
			CheckOffers(
				(helper, coreTable) =>
					{
						Assert.That(coreTable.Rows.Count, Is.EqualTo(_buyingCoreCount + _offerCoreCount), "Кол-во предложений не равно кол-ву предложений в обоих прайс-листах");

						var _buyingOffers = coreTable.Select("PriceCode = " + _buyingPrice.Id.PriceId);
						Assert.That(_buyingOffers.Length, Is.EqualTo(_buyingCoreCount), "Для прайс-листа {0} не все предложения", _buyingPrice.Id.PriceId);

						var _offerOffers = coreTable.Select("PriceCode = " + _offerPrice.Id.PriceId);
						Assert.That(_offerOffers.Length, Is.EqualTo(_offerCoreCount), "Для прайс-листа {0} не все предложения", _offerPrice.Id.PriceId);
					});
		}

		private void CheckBuyingMatrixType(DataTable core, TestActivePrice price, uint productId, BuyinMatrixStatus status)
		{
			var offers = core.Select("PriceCode = {0} and ProductId = {1}".Format(price.Id.PriceId, productId));
			Assert.That(offers.Length, Is.GreaterThan(0), "Не найдено предложение {0} в прайс-листе {1}", productId, price.Id.PriceId);
			Assert.That(Convert.ToInt32(offers[0]["BuyingMatrixType"]), Is.EqualTo((int)status), "У предложения {0} из прайс-листа {1} должен быть BuyingMatrixType = {2}", productId, price.Id.PriceId, status);
		}

		[Test(Description = "проверяем работу белой матрицы закупок")]
		public void WhiteBuyingMatrix()
		{
			var settings = TestDrugstoreSettings.Find(_client.Id);
			settings.BuyingMatrixPriceId = _buyingPrice.Id.PriceId;
			settings.BuyingMatrixType = 0;
			settings.WarningOnBuyingMatrix = false;
			settings.UpdateAndFlush();

			InsertMatrix(_buyingPrice);

			CheckOffers(
				(helper, coreTable) =>
				{
					Assert.That(coreTable.Rows.Count, Is.EqualTo(_buyingCoreCount + _offerCoreCount), "Кол-во предложений не равно кол-ву предложений в обоих прайс-листах");

					var _buyingOffers = coreTable.Select("PriceCode = " + _buyingPrice.Id.PriceId);
					Assert.That(_buyingOffers.Length, Is.EqualTo(_buyingCoreCount), "Для прайс-листа {0} не все предложения", _buyingPrice.Id.PriceId);

					var _offerOffers = coreTable.Select("PriceCode = " + _offerPrice.Id.PriceId);
					Assert.That(_offerOffers.Length, Is.EqualTo(_offerCoreCount), "Для прайс-листа {0} не все предложения", _offerPrice.Id.PriceId);

					CheckBuyingMatrixType(coreTable, _buyingPrice, _intersectionProductId, BuyinMatrixStatus.Allow);
					CheckBuyingMatrixType(coreTable, _offerPrice, _intersectionProductId, BuyinMatrixStatus.Allow);

					CheckBuyingMatrixType(coreTable, _buyingPrice, _buyingProductId, BuyinMatrixStatus.Allow);

					CheckBuyingMatrixType(coreTable, _offerPrice, _offerProductId, BuyinMatrixStatus.Denied);
				});
		}

		[Test(Description = "проверяем работу черной матрицы закупок")]
		public void BlackBuyingMatrix()
		{
			var settings = TestDrugstoreSettings.Find(_client.Id);
			settings.BuyingMatrixPriceId = _buyingPrice.Id.PriceId;
			settings.BuyingMatrixType = 1;
			settings.WarningOnBuyingMatrix = false;
			settings.UpdateAndFlush();

			InsertMatrix(_buyingPrice);

			CheckOffers(
				(helper, coreTable) =>
				{
					Assert.That(coreTable.Rows.Count, Is.EqualTo(_buyingCoreCount + _offerCoreCount), "Кол-во предложений не равно кол-ву предложений в обоих прайс-листах");

					var _buyingOffers = coreTable.Select("PriceCode = " + _buyingPrice.Id.PriceId);
					Assert.That(_buyingOffers.Length, Is.EqualTo(_buyingCoreCount), "Для прайс-листа {0} не все предложения", _buyingPrice.Id.PriceId);

					var _offerOffers = coreTable.Select("PriceCode = " + _offerPrice.Id.PriceId);
					Assert.That(_offerOffers.Length, Is.EqualTo(_offerCoreCount), "Для прайс-листа {0} не все предложения", _offerPrice.Id.PriceId);

					CheckBuyingMatrixType(coreTable, _buyingPrice, _intersectionProductId, BuyinMatrixStatus.Denied);
					CheckBuyingMatrixType(coreTable, _offerPrice, _intersectionProductId, BuyinMatrixStatus.Denied);

					CheckBuyingMatrixType(coreTable, _buyingPrice, _buyingProductId, BuyinMatrixStatus.Denied);

					CheckBuyingMatrixType(coreTable, _offerPrice, _offerProductId, BuyinMatrixStatus.Allow);
				});
		}

		private void CheckOffer(DataTable core, TestActivePrice price, uint productId, bool exists)
		{
			var offers = core.Select("PriceCode = {0} and ProductId = {1}".Format(price.Id.PriceId, productId));
			if (exists)
				Assert.That(offers.Length, Is.GreaterThan(0), "Не найдено предложение {0} в прайс-листе {1}", productId, price.Id.PriceId);
			else
				Assert.That(offers.Length, Is.EqualTo(0), "Найдено предложение {0} в прайс-листе {1}, хотя должно отсутствовать по матрице предложений", productId, price.Id.PriceId);
		}

		[Test(Description = "проверяем работу белой матрицы предложений")]
		public void WhiteOfferMatrix()
		{
			var settings = TestDrugstoreSettings.Find(_client.Id);
			settings.OfferMatrixPriceId = _offerPrice.Id.PriceId;
			settings.OfferMatrixType = 0;
			settings.UpdateAndFlush();

			InsertMatrix(_offerPrice);

			CheckOffers(
				(helper, coreTable) =>
				{
					Assert.That(coreTable.Rows.Count, Is.LessThan(_buyingCoreCount + _offerCoreCount), "Кол-во предложений должно быть меньше кол-ву предложений в обоих прайс-листах");

					var _buyingOffers = coreTable.Select("PriceCode = " + _buyingPrice.Id.PriceId);
					Assert.That(_buyingOffers.Length, Is.LessThan(_buyingCoreCount), "Для прайс-листа {0} должно быть меньше предложений", _buyingPrice.Id.PriceId);

					var _offerOffers = coreTable.Select("PriceCode = " + _offerPrice.Id.PriceId);
					Assert.That(_offerOffers.Length, Is.EqualTo(_offerCoreCount), "Для прайс-листа {0} не все предложения", _offerPrice.Id.PriceId);

					CheckOffer(coreTable, _buyingPrice, _intersectionProductId, true);
					CheckOffer(coreTable, _offerPrice, _intersectionProductId, true);

					CheckOffer(coreTable, _buyingPrice, _buyingProductId, false);

					CheckOffer(coreTable, _offerPrice, _offerProductId, true);
				});
		}

		[Test(Description = "проверяем работу черной матрицы предложений")]
		public void BlackOfferMatrix()
		{
			var settings = TestDrugstoreSettings.Find(_client.Id);
			settings.OfferMatrixPriceId = _offerPrice.Id.PriceId;
			settings.OfferMatrixType = 1;
			settings.UpdateAndFlush();

			InsertMatrix(_offerPrice);

			CheckOffers(
				(helper, coreTable) =>
				{
					Assert.That(coreTable.Rows.Count, Is.LessThan(_buyingCoreCount), "Кол-во предложений должно быть меньше кол-ву предложений в обоих прайс-листах");

					var _buyingOffers = coreTable.Select("PriceCode = " + _buyingPrice.Id.PriceId);
					Assert.That(_buyingOffers.Length, Is.LessThan(_buyingCoreCount), "Для прайс-листа {0} должно быть меньше предложений", _buyingPrice.Id.PriceId);

					var _offerOffers = coreTable.Select("PriceCode = " + _offerPrice.Id.PriceId);
					Assert.That(_offerOffers.Length, Is.EqualTo(0), "Все предложения прайс-листа {0} должны отсутствовать", _offerPrice.Id.PriceId);

					CheckOffer(coreTable, _buyingPrice, _intersectionProductId, false);
					CheckOffer(coreTable, _offerPrice, _intersectionProductId, false);

					CheckOffer(coreTable, _buyingPrice, _buyingProductId, true);

					CheckOffer(coreTable, _offerPrice, _offerProductId, false);
				});
		}

		[Test(Description = "проверяем работу черной матрицы предложений с заполнением списка поставщиков, для которых не будет применяться матрица предложений")]
		public void BlackOfferMatrixWithOfferSuppliers()
		{
			var settings = TestDrugstoreSettings.Find(_client.Id);
			settings.OfferMatrixPriceId = _offerPrice.Id.PriceId;
			settings.OfferMatrixType = 1;
			settings.UpdateAndFlush();

			InsertMatrix(_offerPrice);
			InserOfferMatrixSuppliers(_offerPrice);

			CheckOffers(
				(helper, coreTable) =>
				{
					Assert.That(coreTable.Rows.Count, Is.LessThan(_offerCoreCount + _buyingCoreCount), "Кол-во предложений должно быть меньше кол-ву предложений в обоих прайс-листах");

					var _buyingOffers = coreTable.Select("PriceCode = " + _buyingPrice.Id.PriceId);
					Assert.That(_buyingOffers.Length, Is.LessThan(_buyingCoreCount), "Для прайс-листа {0} должно быть меньше предложений", _buyingPrice.Id.PriceId);

					var _offerOffers = coreTable.Select("PriceCode = " + _offerPrice.Id.PriceId);
					Assert.That(_offerOffers.Length, Is.EqualTo(_offerCoreCount), "Все предложения прайс-листа {0} должны присутствовать", _offerPrice.Id.PriceId);

					CheckOffer(coreTable, _buyingPrice, _intersectionProductId, false);
					CheckOffer(coreTable, _offerPrice, _intersectionProductId, true);

					CheckOffer(coreTable, _buyingPrice, _buyingProductId, true);

					CheckOffer(coreTable, _offerPrice, _offerProductId, true);
				});
		}

	}
}