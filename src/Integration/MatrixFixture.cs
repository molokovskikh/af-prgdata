using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Castle.ActiveRecord;
using Common.Tools;
using MySql.Data.MySqlClient;
using NHibernate.Linq;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;
using Test.Support.Helpers;
using Test.Support.Suppliers;

namespace Integration
{
	public enum BuyinMatrixStatus
	{
		Allow = 0,
		Denied = 1,
		Warning = 2
	}

	[TestFixture(Description = "Тесты для матрицы закупок и матрицы предложений")]
	public class MatrixFixture : IntegrationFixture
	{
		private TestClient _client;
		private TestUser _user;

		private TestActivePrice _buyingPrice;
		private TestActivePrice _offerPrice;

		private uint _intersectionProductId;
		private uint _buyingProductId;
		private uint _offerProductId;
		private uint _intersectionProductIdWithProducerId;
		private uint _intersectionProductIdWithDistinctProducerId;

		private long _buyingCoreCount;
		private long _offerCoreCount;
		private IList<TestActivePrice> _activePrices;
		private TestDrugstoreSettings settings;

		private MySqlConnection Connection
		{
			get { return (MySqlConnection)session.Connection; }
		}

		[TestFixtureSetUp]
		public void FixtureSetUp()
		{
			Reopen();
			var supplier1 = TestSupplier.CreateNaked(session);
			var producers1 = TestProducer.RandomProducers(session).Take(1)
				.Concat(new TestProducer[] { null })
				.Concat(TestProducer.RandomProducers(session).Take(2))
				.ToArray();
			supplier1.CreateSampleCore(session,
				TestProduct.RandomProducts(session).Take(4).ToArray(),
				producers1);

			var supplier2 = TestSupplier.CreateNaked(session);
			var offers = supplier1.Prices.SelectMany(p => p.Core);
			var products = offers.Select(c => c.Product)
				.Distinct()
				.Take(3)
				.Concat(TestProduct.RandomProducts(session).Take(1))
				.ToArray();
			supplier2.CreateSampleCore(session, products, offers.Select(o => o.Producer).Take(1).ToArray());

			_client = TestClient.CreateNaked(session);
			_user = _client.Users[0];

			_client.Users.Each(u => {
				u.SendRejects = true;
				u.SendWaybills = true;
			});
			session.Save(_user);

			_activePrices = _user.GetActivePricesNaked(session);
			_buyingPrice = _activePrices.First(p => p.Supplier == supplier1);
			_buyingCoreCount = _buyingPrice.CoreCount();

			_offerPrice = _activePrices.First(p => p.Supplier == supplier2);
			AssertPrice(_offerPrice);
			Assert.That(_offerPrice,
				Is.Not.Null,
				"Не нашли прайс-лист, удовлетворяющий условию теста: должны быть пересечения и уникальные продукты с прайс-листом {0}",
				_buyingPrice.Id.PriceId);

			if (_buyingPrice.Price.Matrix == null) {
				_buyingPrice.Price.Matrix = new TestMatrix();
				session.Save(_buyingPrice.Price);
			}
			if (_offerPrice.Price.Matrix == null) {
				_offerPrice.Price.Matrix = new TestMatrix();
				session.Save(_offerPrice.Price);
			}

			_offerCoreCount = _offerPrice.CoreCount();

			session.CreateSQLQuery(
				"delete from Customers.UserPrices where UserId = :userId and PriceId not in (:buyingPriceId, :offerPriceId);")
				.SetParameter("userId", _user.Id)
				.SetParameter("buyingPriceId", _buyingPrice.Id.PriceId)
				.SetParameter("offerPriceId", _offerPrice.Id.PriceId)
				.ExecuteUpdate();
			Close();
		}

		private void AssertPrice(TestActivePrice testActivePrice)
		{
			_intersectionProductId = session.CreateSQLQuery(@"
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
")
				.SetParameter("priceId", _buyingPrice.Id.PriceId)
				.SetParameter("costId", _buyingPrice.CostId)
				.SetParameter("otherPriceId", testActivePrice.Id.PriceId)
				.SetParameter("otherCostId", testActivePrice.CostId)
				.UniqueResult<uint>();

			_buyingProductId = session.CreateSQLQuery(@"
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
")
				.SetParameter("priceId", _buyingPrice.Id.PriceId)
				.SetParameter("costId", _buyingPrice.CostId)
				.SetParameter("otherPriceId", testActivePrice.Id.PriceId)
				.SetParameter("otherCostId", testActivePrice.CostId)
				.UniqueResult<uint>();

			_offerProductId = session.CreateSQLQuery(@"
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
")
				.SetParameter("priceId", testActivePrice.Id.PriceId)
				.SetParameter("costId", testActivePrice.CostId)
				.SetParameter("otherPriceId", _buyingPrice.Id.PriceId)
				.SetParameter("otherCostId", _buyingPrice.CostId)
				.UniqueResult<uint>();

			_intersectionProductIdWithProducerId = session.CreateSQLQuery(@"
select 
	c.ProductId
from
  farm.Core0 c
  join farm.CoreCosts cc on cc.Core_Id = c.Id and cc.PC_CostCode = :costId
  join farm.Core0 n on n.ProductId = c.ProductId and n.PriceCode = :otherPriceId and n.CodeFirmCr is not null 
  join farm.CoreCosts nc on nc.Core_Id = n.Id and nc.PC_CostCode = :otherCostId
where
  c.PriceCode = :priceId
and c.CodeFirmCr is null
and c.ProductId <> :intersectionProductId
limit 1
")
				.SetParameter("priceId", _buyingPrice.Id.PriceId)
				.SetParameter("costId", _buyingPrice.CostId)
				.SetParameter("otherPriceId", testActivePrice.Id.PriceId)
				.SetParameter("otherCostId", testActivePrice.CostId)
				.SetParameter("intersectionProductId", _intersectionProductId)
				.UniqueResult<uint>();

			_intersectionProductIdWithDistinctProducerId = session.CreateSQLQuery(@"
select 
	c.ProductId
from
  farm.Core0 c
  join farm.CoreCosts cc on cc.Core_Id = c.Id and cc.PC_CostCode = :costId
  join farm.Core0 n on n.ProductId = c.ProductId and n.PriceCode = :otherPriceId and n.CodeFirmCr is not null and n.CodeFirmCr <> c.CodeFirmCr
  join farm.CoreCosts nc on nc.Core_Id = n.Id and nc.PC_CostCode = :otherCostId
where
  c.PriceCode = :priceId
and c.CodeFirmCr is not null
and c.ProductId <> :intersectionProductId
and c.ProductId <> :intersectionProductIdWithProducerId
limit 1
")
				.SetParameter("priceId", _buyingPrice.Id.PriceId)
				.SetParameter("costId", _buyingPrice.CostId)
				.SetParameter("otherPriceId", testActivePrice.Id.PriceId)
				.SetParameter("otherCostId", testActivePrice.CostId)
				.SetParameter("intersectionProductId", _intersectionProductId)
				.SetParameter("intersectionProductIdWithProducerId", _intersectionProductIdWithProducerId)
				.UniqueResult<uint>();

			Assert.That(_intersectionProductId, Is.GreaterThan(0), "прайс 1 {0} прайс 2 {1}", _buyingPrice.Id, testActivePrice.Id);
			Assert.That(_buyingProductId, Is.GreaterThan(0), "прайс 1 {0} прайс 2 {1}", _buyingPrice.Id, testActivePrice.Id);
			Assert.That(_offerProductId, Is.GreaterThan(0), "прайс 1 {0} прайс 2 {1}", _buyingPrice.Id, testActivePrice.Id);
			Assert.That(_intersectionProductIdWithProducerId, Is.GreaterThan(0), "прайс 1 {0} прайс 2 {1}", _buyingPrice.Id, testActivePrice.Id);
			Assert.That(_intersectionProductIdWithDistinctProducerId, Is.GreaterThan(0), "прайс 1 {0} прайс 2 {1}", _buyingPrice.Id, testActivePrice.Id);
		}

		[SetUp]
		public void SetUp()
		{
			settings = session.Load<TestDrugstoreSettings>(_client.Id);
			settings.BuyingMatrixPriceId = null;
			settings.BuyingMatrix = null;
			settings.BuyingMatrixAction = TestMatrixAction.Block;
			settings.OfferMatrixPriceId = null;
			settings.OfferMatrix = null;
			settings.OfferMatrixAction = TestMatrixAction.Block;
			session.SaveOrUpdate(settings);

			ClearOfferMatrixSuppliers();
			ClearMatrix(_buyingPrice);
			ClearMatrix(_offerPrice);
		}

		private void ClearMatrix(TestActivePrice price)
		{
			session.CreateSQLQuery(
				"delete from farm.BuyingMatrix where PriceId = :priceId;")
				.SetParameter("priceId", price.Id.PriceId)
				.ExecuteUpdate();
		}

		private void InsertMatrix(TestActivePrice price)
		{
			//здесь сделано без учета производителей
			session.CreateSQLQuery(
				@"
insert into farm.BuyingMatrix(MatrixId, PriceId, Code, ProductId, ProducerId)
select :matrixId, c0.PriceCode, c0.Code, c0.ProductId, null
from farm.Core0 c0
where pricecode = :priceId
group by c0.ProductId;")
				.SetParameter("priceId", price.Id.PriceId)
				.SetParameter("matrixId", price.Price.Matrix.Id)
				.ExecuteUpdate();
		}

		private void InsertMatrixByProductWithProducer(TestActivePrice price,
			uint productId)
		{
			//Вставка записи с учетом производителя
			session.CreateSQLQuery(
				@"
delete from farm.BuyingMatrix where PriceId = :priceId and ProductId = :productId;
insert into farm.BuyingMatrix(MatrixId, PriceId, Code, ProductId, ProducerId)
select :matrixId, c0.PriceCode, c0.Code, c0.ProductId, c0.CodeFirmCr
from farm.Core0 c0
where 
	pricecode = :priceId
and c0.ProductId = :productId
and c0.CodeFirmCr is not null
limit 1;")
				.SetParameter("priceId", price.Id.PriceId)
				.SetParameter("productId", productId)
				.SetParameter("matrixId", price.Price.Matrix.Id)
				.ExecuteUpdate();
		}

		private void ClearOfferMatrixSuppliers()
		{
			session.CreateSQLQuery(
				"delete from UserSettings.OfferMatrixSuppliers where ClientId = :clientId;")
				.SetParameter("clientId", _client.Id)
				.ExecuteUpdate();
		}

		private void InserOfferMatrixSuppliers(TestActivePrice price)
		{
			session.CreateSQLQuery(
				"insert into UserSettings.OfferMatrixSuppliers (ClientId, SupplierId) values (:clientId, :supplierId);")
				.SetParameter("clientId", _client.Id)
				.SetParameter("supplierId", price.Price.Supplier.Id)
				.ExecuteUpdate();
		}

		private void CheckOffers(Action<UpdateHelper, DataTable> action)
		{
			Flush();
			var updateData = UpdateHelper.GetUpdateData(Connection, _user.Login);
			var helper = new UpdateHelper(updateData, Connection);

			helper.MaintainReplicationInfo();
			helper.Cleanup();
			helper.SelectActivePricesFull();
			helper.SelectOffers();

			var coreSql = helper.GetCoreCommand(false, true, true);

			var dataAdapter = new MySqlDataAdapter(coreSql, Connection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?Cumulative", 0);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", _client.Id);
			var coreTable = new DataTable();

			dataAdapter.Fill(coreTable);

			if (updateData.Settings.BuyingMatrix.HasValue)
				Assert.That(coreTable.Columns.Contains("BuyingMatrixType"),
					Is.True,
					"Не найден столбец BuyingMatrixType хотя механизм матрицы закупок подключен");

			action(helper, coreTable);
		}

		[Test(Description = "Должны быть все предложения без настроенных матриц")]
		public void AllOffers()
		{
			CheckOffers(
				(helper, coreTable) => { CheckRowCount(coreTable); });
		}

		[Test(Description = "проверяем работу белой матрицы закупок")]
		public void WhiteBuyingMatrix()
		{
			settings.BuyingMatrixPriceId = _buyingPrice.Id.PriceId;
			settings.BuyingMatrix = _buyingPrice.Price.Matrix;
			settings.BuyingMatrixType = 0;
			InsertMatrix(_buyingPrice);

			CheckOffers(
				(helper, coreTable) => {
					CheckRowCount(coreTable);

					CheckBuyingMatrixType(coreTable, _buyingPrice, _intersectionProductId, BuyinMatrixStatus.Allow);
					CheckBuyingMatrixType(coreTable, _offerPrice, _intersectionProductId, BuyinMatrixStatus.Allow);

					CheckBuyingMatrixType(coreTable, _buyingPrice, _buyingProductId, BuyinMatrixStatus.Allow);

					CheckBuyingMatrixType(coreTable, _offerPrice, _offerProductId, BuyinMatrixStatus.Denied);
				});
		}

		[Test(Description = "проверяем работу черной матрицы закупок")]
		public void BlackBuyingMatrix()
		{
			settings.BuyingMatrixPriceId = _buyingPrice.Id.PriceId;
			settings.BuyingMatrix = _buyingPrice.Price.Matrix;
			settings.BuyingMatrixType = TestMatrixType.BlackList;
			InsertMatrix(_buyingPrice);

			CheckOffers(
				(helper, coreTable) => {
					CheckRowCount(coreTable);

					CheckBuyingMatrixType(coreTable, _buyingPrice, _intersectionProductId, BuyinMatrixStatus.Denied);
					CheckBuyingMatrixType(coreTable, _offerPrice, _intersectionProductId, BuyinMatrixStatus.Denied);

					CheckBuyingMatrixType(coreTable, _buyingPrice, _buyingProductId, BuyinMatrixStatus.Denied);

					CheckBuyingMatrixType(coreTable, _offerPrice, _offerProductId, BuyinMatrixStatus.Allow);
				});
		}

		[Test(Description = "Проверяет работу черной матрицы закупок, с учетом поставщика")]
		public void BlackMatrixWithSupplierCriteria()
		{
			settings.BuyingMatrix = _buyingPrice.Price.Matrix;
			settings.BuyingMatrixType = TestMatrixType.BlackList;
			settings.BuyingMatrixAction = TestMatrixAction.Block;
			InsertMatrix(_offerPrice);

			var supplierProduct = PrepareSupplierCoreDataForMatrix();

			CheckOffers((helper, coreTable) => {
				var offers = coreTable.Select("ProductId = {0}".Format(supplierProduct));
				Assert.AreEqual(offers.Length, 2);
				Assert.That(Convert.ToInt32(offers[0]["BuyingMatrixType"]), Is.EqualTo((int)BuyinMatrixStatus.Allow));
				Assert.That(Convert.ToInt32(offers[0]["PriceCode"]), Is.EqualTo((int)_buyingPrice.Price.Id));
				Assert.That(Convert.ToInt32(offers[1]["BuyingMatrixType"]), Is.EqualTo((int)BuyinMatrixStatus.Denied));
				Assert.That(Convert.ToInt32(offers[1]["PriceCode"]), Is.EqualTo((int)_offerPrice.Price.Id));
			});

			session.CreateSQLQuery(string.Format("delete from farm.Core0 where ProductId = {0}", supplierProduct)).ExecuteUpdate();
		}

		private uint PrepareSupplierCoreDataForMatrix()
		{
			var updateData = UpdateHelper.GetUpdateData((MySqlConnection)session.Connection, _user.Login);
			var helper = new UpdateHelper(updateData, (MySqlConnection)session.Connection);

			helper.MaintainReplicationInfo();
			helper.Cleanup();
			helper.SelectActivePricesFull();

			_buyingPrice = session.Get<TestActivePrice>(_buyingPrice.Id);
			_offerPrice = session.Get<TestActivePrice>(_offerPrice.Id);
			var name = Generator.Name();
			var product = new TestProduct(name);
			session.Save(product);
			var productSynonym1 = new TestProductSynonym(name, product, _buyingPrice.Price);
			session.Save(productSynonym1);
			var core1 = new TestCore(productSynonym1);
			session.Save(core1);
			var costCode1 = session.CreateSQLQuery(
				string.Format(@"select CostCode from ActivePrices where PriceCode = {0}", _buyingPrice.Price.Id))
				.UniqueResult<UInt32>();
			var coreCost1 = new TestCost(core1, _buyingPrice.Price.Costs.First(c => c.Id == costCode1), 100);
			session.Save(coreCost1);

			var productSynonym2 = new TestProductSynonym(name, product, _offerPrice.Price);
			session.Save(productSynonym2);
			var core2 = new TestCore(productSynonym2);
			session.Save(core2);
			var costCode2 = session.CreateSQLQuery(
				string.Format(@"select CostCode from ActivePrices where PriceCode = {0}", _offerPrice.Price.Id))
				.UniqueResult<UInt32>();
			var coreCost2 = new TestCost(core2, _offerPrice.Price.Costs.First(c => c.Id == costCode2), 100);
			session.Save(coreCost2);

			var matrixPosition = new TestBuyingMatrix {
				Product = product,
				Supplier = _offerPrice.Price.Supplier,
				Matrix = _buyingPrice.Price.Matrix
			};
			session.Save(matrixPosition);
			return product.Id;
		}

		private DataRow[] CheckOffer(DataTable core, TestActivePrice price, uint productId, bool exists, uint? producerId = null, bool checkProducerIsNull = false)
		{
			DataRow[] offers;
			if (producerId.HasValue)
				offers = core.Select("PriceCode = {0} and ProductId = {1} and ProducerId = {2}".Format(price.Id.PriceId, productId, producerId));
			else if (checkProducerIsNull)
				offers = core.Select("PriceCode = {0} and ProductId = {1} and ProducerId is Null".Format(price.Id.PriceId, productId));
			else
				offers = core.Select("PriceCode = {0} and ProductId = {1}".Format(price.Id.PriceId, productId));
			if (exists)
				Assert.That(offers.Length, Is.GreaterThan(0), "Не найдено предложение {0} в прайс-листе {1}", productId, price.Id.PriceId);
			else
				Assert.That(offers.Length, Is.EqualTo(0), "Найдено предложение {0} в прайс-листе {1}, хотя должно отсутствовать по матрице предложений", productId, price.Id.PriceId);
			return offers;
		}

		[Test(Description = "проверяем работу белой матрицы предложений")]
		public void WhiteOfferMatrix()
		{
			settings.OfferMatrixPriceId = _offerPrice.Id.PriceId;
			settings.OfferMatrix = _offerPrice.Price.Matrix;
			settings.OfferMatrixType = 0;
			InsertMatrix(_offerPrice);

			CheckOffers(
				(helper, coreTable) => {
					CheckRowCount(coreTable);

					CheckOffer(coreTable, _buyingPrice, _intersectionProductId, true);
					CheckBuyingMatrixType(coreTable, _buyingPrice, _intersectionProductId, BuyinMatrixStatus.Allow);

					CheckOffer(coreTable, _offerPrice, _intersectionProductId, true);
					CheckBuyingMatrixType(coreTable, _offerPrice, _intersectionProductId, BuyinMatrixStatus.Allow);

					CheckOffer(coreTable, _buyingPrice, _buyingProductId, true);
					CheckBuyingMatrixType(coreTable, _buyingPrice, _buyingProductId, BuyinMatrixStatus.Denied);

					CheckOffer(coreTable, _offerPrice, _offerProductId, true);
					CheckBuyingMatrixType(coreTable, _offerPrice, _offerProductId, BuyinMatrixStatus.Allow);
				});
		}

		[Test(Description = "проверяем работу черной матрицы предложений")]
		public void BlackOfferMatrix()
		{
			settings.OfferMatrixPriceId = _offerPrice.Id.PriceId;
			settings.OfferMatrix = _offerPrice.Price.Matrix;
			settings.OfferMatrixType = TestMatrixType.BlackList;
			InsertMatrix(_offerPrice);

			CheckOffers(
				(helper, coreTable) => {
					CheckRowCount(coreTable);

					CheckOffer(coreTable, _buyingPrice, _intersectionProductId, true);
					CheckBuyingMatrixType(coreTable, _buyingPrice, _intersectionProductId, BuyinMatrixStatus.Denied);

					CheckOffer(coreTable, _offerPrice, _intersectionProductId, true);
					CheckBuyingMatrixType(coreTable, _offerPrice, _intersectionProductId, BuyinMatrixStatus.Denied);

					CheckOffer(coreTable, _buyingPrice, _buyingProductId, true);
					CheckBuyingMatrixType(coreTable, _buyingPrice, _buyingProductId, BuyinMatrixStatus.Allow);

					CheckOffer(coreTable, _offerPrice, _offerProductId, true);
					CheckBuyingMatrixType(coreTable, _offerPrice, _offerProductId, BuyinMatrixStatus.Denied);
				});
		}

		[Test(Description = "проверяем работу черной матрицы предложений с заполнением списка поставщиков, для которых не будет применяться матрица предложений")]
		public void BlackOfferMatrixWithOfferSuppliers()
		{
			settings.OfferMatrixPriceId = _offerPrice.Id.PriceId;
			settings.OfferMatrix = _offerPrice.Price.Matrix;
			settings.OfferMatrixType = TestMatrixType.BlackList;
			InsertMatrix(_offerPrice);
			InserOfferMatrixSuppliers(_offerPrice);

			CheckOffers(
				(helper, coreTable) => {
					CheckRowCount(coreTable);

					CheckOffer(coreTable, _buyingPrice, _intersectionProductId, true);
					CheckBuyingMatrixType(coreTable, _buyingPrice, _intersectionProductId, BuyinMatrixStatus.Denied);

					CheckOffer(coreTable, _offerPrice, _intersectionProductId, true);
					CheckBuyingMatrixType(coreTable, _offerPrice, _intersectionProductId, BuyinMatrixStatus.Allow);

					CheckOffer(coreTable, _buyingPrice, _buyingProductId, true);
					CheckBuyingMatrixType(coreTable, _buyingPrice, _buyingProductId, BuyinMatrixStatus.Allow);

					CheckOffer(coreTable, _offerPrice, _offerProductId, true);
					CheckBuyingMatrixType(coreTable, _offerPrice, _offerProductId, BuyinMatrixStatus.Allow);
				});
		}

		[Test(Description = "проверяем работу черной матрицы предложений с учетом изготовителя с заполнением списка поставщиков, для которых не будет применяться матрица предложений")]
		public void BlackOfferMatrixWithOfferSuppliersWithProducer()
		{
			settings.OfferMatrixPriceId = _offerPrice.Id.PriceId;
			settings.OfferMatrix = _offerPrice.Price.Matrix;
			settings.OfferMatrixType = TestMatrixType.BlackList;
			InsertMatrix(_offerPrice);
			InsertMatrixByProductWithProducer(_offerPrice, _intersectionProductIdWithProducerId);
			InsertMatrixByProductWithProducer(_offerPrice, _intersectionProductIdWithDistinctProducerId);
			InserOfferMatrixSuppliers(_offerPrice);

			CheckOffers(
				(helper, coreTable) => {
					CheckRowCount(coreTable);

					CheckOffer(coreTable, _buyingPrice, _intersectionProductId, true);
					CheckBuyingMatrixType(coreTable, _buyingPrice, _intersectionProductId, BuyinMatrixStatus.Denied);

					CheckOffer(coreTable, _offerPrice, _intersectionProductId, true);
					CheckBuyingMatrixType(coreTable, _offerPrice, _intersectionProductId, BuyinMatrixStatus.Allow);

					CheckOffer(coreTable, _buyingPrice, _buyingProductId, true);
					CheckBuyingMatrixType(coreTable, _buyingPrice, _buyingProductId, BuyinMatrixStatus.Allow);

					CheckOffer(coreTable, _offerPrice, _offerProductId, true);
					CheckBuyingMatrixType(coreTable, _offerPrice, _offerProductId, BuyinMatrixStatus.Allow);


					var intersectionOffers = CheckOffer(coreTable, _offerPrice, _intersectionProductIdWithProducerId, true);

					//Списк предложений из прайс-листа _buyingPrice по ProductId = _intersectionProductIdWithProducerId, у которого есть запись с CodeFirmCr is null
					var intersectionOffersByBuying = CheckOffer(coreTable, _buyingPrice, _intersectionProductIdWithProducerId, true);

					foreach (var dataRow in intersectionOffersByBuying) {
						if (Convert.IsDBNull(dataRow["ProducerId"]))
							CheckBuyingMatrixTypeByOffer(dataRow, BuyinMatrixStatus.Denied);
						else {
							var producerId = Convert.ToUInt32(dataRow["ProducerId"]);
							if (producerId == 0)
								CheckBuyingMatrixTypeByOffer(dataRow, BuyinMatrixStatus.Denied);
							else if (producerId == Convert.ToUInt32(intersectionOffers[0]["ProducerId"]))
								CheckBuyingMatrixTypeByOffer(dataRow, BuyinMatrixStatus.Denied);
							else
								CheckBuyingMatrixTypeByOffer(dataRow, BuyinMatrixStatus.Allow);
						}
					}


					var intersectionDistinctOffers = CheckOffer(coreTable, _offerPrice, _intersectionProductIdWithDistinctProducerId, true);

					//Списк предложений из прайс-листа _buyingPrice по ProductId = _intersectionProductIdWithDistinctProducerId, у которого есть запись с CodeFirmCr is not null
					var intersectionDistinctOffersByBuying = CheckOffer(coreTable, _buyingPrice, _intersectionProductIdWithDistinctProducerId, true);

					foreach (var dataRow in intersectionDistinctOffersByBuying) {
						if (Convert.IsDBNull(dataRow["ProducerId"]))
							CheckBuyingMatrixTypeByOffer(dataRow, BuyinMatrixStatus.Denied);
						else {
							var producerId = Convert.ToUInt32(dataRow["ProducerId"]);
							if (producerId == 0)
								CheckBuyingMatrixTypeByOffer(dataRow, BuyinMatrixStatus.Denied);
							else if (producerId == Convert.ToUInt32(intersectionDistinctOffers[0]["ProducerId"]))
								CheckBuyingMatrixTypeByOffer(dataRow, BuyinMatrixStatus.Denied);
							else
								CheckBuyingMatrixTypeByOffer(dataRow, BuyinMatrixStatus.Allow);
						}
					}
				});
		}

		[Test]
		public void Remove_offers()
		{
			settings.OfferMatrixPriceId = _offerPrice.Id.PriceId;
			settings.OfferMatrix = _offerPrice.Price.Matrix;
			settings.OfferMatrixAction = TestMatrixAction.Delete;
			settings.OfferMatrixType = TestMatrixType.BlackList;
			InsertMatrix(_offerPrice);

			CheckOffers(
				(helper, coreTable) => {
					Assert.That(coreTable.Rows.Count, Is.GreaterThan(0));
					Assert.That(coreTable.Rows.Count, Is.LessThan(_buyingCoreCount + _offerCoreCount), "Предложений должно стать меньше тк матрица должна их удалить");

					var offers = coreTable.Select("PriceCode = " + _offerPrice.Id.PriceId);
					Assert.That(offers.Length, Is.LessThan(_offerCoreCount), "Для прайс листа {0} должны были удалить предложения", _offerPrice.Id.PriceId);

					var filtredOffers = offers.Where(o => Convert.ToUInt32(o["ProductId"]) == _intersectionProductId).ToArray();
					Assert.That(filtredOffers.Length, Is.EqualTo(0));

					filtredOffers = offers.Where(o => Convert.ToUInt32(o["ProductId"]) == _offerProductId).ToArray();
					Assert.That(filtredOffers.Length, Is.EqualTo(0));
				});
		}

		private void CheckRowCount(DataTable coreTable)
		{
			Assert.That(coreTable.Rows.Count, Is.EqualTo(_buyingCoreCount + _offerCoreCount), "Кол-во предложений должно быть равно кол-ву предложений в обоих прайс-листах, оно не должно меняться");

			var buyingOffers = coreTable.Select("PriceCode = " + _buyingPrice.Id.PriceId);
			Assert.That(buyingOffers.Length, Is.EqualTo(_buyingCoreCount), "Все предложения прайс-листа {0} должны присутствовать", _buyingPrice.Id.PriceId);

			var offerOffers = coreTable.Select("PriceCode = " + _offerPrice.Id.PriceId);
			Assert.That(offerOffers.Length, Is.EqualTo(_offerCoreCount), "Все предложения прайс-листа {0} должны присутствовать", _offerPrice.Id.PriceId);
		}

		private void CheckBuyingMatrixType(DataTable core, TestActivePrice price, uint productId, BuyinMatrixStatus status)
		{
			var offers = core.Select("PriceCode = {0} and ProductId = {1}".Format(price.Id.PriceId, productId));
			Assert.That(offers.Length, Is.GreaterThan(0), "Не найдено предложение {0} в прайс-листе {1}", productId, price.Id.PriceId);
			Assert.That(Convert.ToInt32(offers[0]["BuyingMatrixType"]), Is.EqualTo((int)status), "У предложения {0} из прайс-листа {1} должен быть BuyingMatrixType = {2}", productId, price.Id.PriceId, status);
		}

		private void CheckBuyingMatrixTypeByOffer(DataRow offer, BuyinMatrixStatus status)
		{
			Assert.That(Convert.ToInt32(offer["BuyingMatrixType"]), Is.EqualTo((int)status), "У предложения {0} из прайс-листа {1} должен быть BuyingMatrixType = {2}", offer["ProductId"], offer["PriceCode"], status);
		}
	}
}