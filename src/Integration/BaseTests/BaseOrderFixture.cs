using System;
using System.Data;
using System.IO;
using System.Linq;
using Common.MySql;
using Common.Tools;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using Test.Support;
using Test.Support.Suppliers;
using MySqlHelper = MySql.Data.MySqlClient.MySqlHelper;

namespace Integration.BaseTests
{
	public class BaseOrderFixture : PrepareDataFixture
	{
		protected bool getOffers;
		protected DataRow activePrice;
		protected DataRow firstOffer;
		protected DataRow secondOffer;
		protected DataRow thirdOffer;
		protected DataRow fourOffer;

		protected void GetOffers(TestUser user)
		{
			if (getOffers)
				return;

			var supplier = TestSupplier.CreateNaked(session);
			supplier.CreateSampleCore(session);
			var product = supplier.Prices[0].Core[0].Product;
			var random = Generator.Random().Take(2).ToArray();
			supplier.SaveCore(session, supplier.AddCore(Tuple.Create(random[0] + " " + product.Name, product)));
			supplier.SaveCore(session, supplier.AddCore(Tuple.Create(random[1] + " " + product.Name, product)));
			user.Client.MaintainIntersection(session);
			FlushAndCommit();

			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();

				MySqlHelper.ExecuteNonQuery(
					connection,
					@"
drop temporary table if exists Usersettings.Prices, Usersettings.ActivePrices, Usersettings.Core;
call Customers.GetOffers(?UserId)",
					new MySqlParameter("?UserId", user.Id));

				activePrice = ExecuteDataRow(
					connection, @"
select
*
from
ActivePrices
where
FirmCode = ?supplierId
limit 1", new MySqlParameter("?supplierId", supplier.Id));

				var firstProductId = MySqlHelper.ExecuteScalar(
					connection,
					@"
select
c.ProductId
from
Core
inner join farm.Core0 c on c.Id = Core.Id
where
Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
group by c.ProductId
having count(distinct c.SynonymCode) > 2
limit 1
",
					new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
					new MySqlParameter("?RegionCode", activePrice["RegionCode"]));

				var secondProductId = MySqlHelper.ExecuteScalar(
					connection,
					@"
select
c.ProductId
from
Core
inner join farm.Core0 c on c.Id = Core.Id
where
Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
and C.ProductId <> ?FirstProductId
limit 1
",
					new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
					new MySqlParameter("?RegionCode", activePrice["RegionCode"]),
					new MySqlParameter("?FirstProductId", firstProductId));

				var thirdProductId = MySqlHelper.ExecuteScalar(
					connection,
					@"
select
c.ProductId
from
Core
inner join farm.Core0 c on c.Id = Core.Id
where
Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
and C.ProductId <> ?FirstProductId
and C.ProductId <> ?SecondProductId
limit 1
",
					new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
					new MySqlParameter("?RegionCode", activePrice["RegionCode"]),
					new MySqlParameter("?FirstProductId", firstProductId),
					new MySqlParameter("?SecondProductId", secondProductId));

				firstOffer = ExecuteDataRow(
					connection,
					@"
select
Core.Cost,
Core.RegionCode,
c.*
from
Core
inner join farm.Core0 c on c.Id = Core.Id
where
Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
and C.ProductId = ?ProductId
limit 1
",
					new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
					new MySqlParameter("?RegionCode", activePrice["RegionCode"]),
					new MySqlParameter("?ProductId", firstProductId));
				Assert.IsNotNull(firstOffer, "Не найдено предложение по прайсу {0} для пользователя {1} продукт {2}",
					activePrice["PriceCode"], user.Id, firstProductId);

				secondOffer = ExecuteDataRow(
					connection,
					@"
select
Core.Cost,
Core.RegionCode,
c.*
from
Core
inner join farm.Core0 c on c.Id = Core.Id
where
Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
and C.ProductId = ?ProductId
and C.SynonymCode <> ?SynonymCode
limit 1
",
					new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
					new MySqlParameter("?RegionCode", activePrice["RegionCode"]),
					new MySqlParameter("?ProductId", firstProductId),
					new MySqlParameter("?SynonymCode", firstOffer["SynonymCode"]));
				Assert.IsNotNull(secondOffer, "Не найдено предложение");

				thirdOffer = ExecuteDataRow(
					connection,
					@"
select
Core.Cost,
Core.RegionCode,
c.*
from
Core
inner join farm.Core0 c on c.Id = Core.Id
where
Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
and C.ProductId = ?ProductId
limit 1
",
					new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
					new MySqlParameter("?RegionCode", activePrice["RegionCode"]),
					new MySqlParameter("?ProductId", secondProductId));
				Assert.IsNotNull(thirdOffer, "Не найдено предложение");

				fourOffer = ExecuteDataRow(
					connection,
					@"
select
Core.Cost,
Core.RegionCode,
c.*
from
Core
inner join farm.Core0 c on c.Id = Core.Id
where
Core.PriceCode = ?PriceCode
and Core.RegionCode = ?RegionCode
and C.ProductId = ?ProductId
limit 1
",
					new MySqlParameter("?PriceCode", activePrice["PriceCode"]),
					new MySqlParameter("?RegionCode", activePrice["RegionCode"]),
					new MySqlParameter("?ProductId", thirdProductId));
				Assert.IsNotNull(fourOffer, "Не найдено предложение");
				getOffers = true;
			}
		}

		public static DataRow ExecuteDataRow(MySqlConnection connection, string commandText, params MySqlParameter[] parms)
		{
			DataSet set = MySqlHelper.ExecuteDataset(connection, commandText, parms);
			if (set == null) {
				return null;
			}
			if (set.Tables.Count == 0) {
				return null;
			}
			if (set.Tables[0].Rows.Count == 0) {
				return null;
			}
			return set.Tables[0].Rows[0];
		}

		protected void CreateFolders(string folderName)
		{
			var fullName = Path.Combine("FtpRoot", folderName, "Waybills");
			Directory.CreateDirectory(fullName);
			fullName = Path.Combine("FtpRoot", folderName, "Rejects");
			Directory.CreateDirectory(fullName);
			fullName = Path.Combine("FtpRoot", folderName, "Docs");
			Directory.CreateDirectory(fullName);
		}
	}
}