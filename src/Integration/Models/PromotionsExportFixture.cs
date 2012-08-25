using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using Castle.ActiveRecord;
using Common.Tools;
using Integration.BaseTests;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.AnalitFVersions;
using PrgData.Common.Models;
using Test.Support;
using Test.Support.Suppliers;

namespace Integration.Models
{
	[TestFixture]
	public class PromotionsExportFixture : BaseExportFixture
	{
		private MySqlConnection connection;

		[SetUp]
		public void SetUp()
		{
			TestSupplier.Create();
			connection = new MySqlConnection(Settings.ConnectionString());
			connection.Open();
			updateData.UpdateExeVersionInfo = new VersionInfo(1500);
		}

		[TearDown]
		public void TearDown()
		{
			connection.Dispose();
		}

		[Test(Description = "Получаем список промо-акций при кумулятивном обновлении")]
		public void GetAllPromotionsOnCumulative()
		{
			var promotionCount = Convert.ToInt32(
				MySqlHelper.ExecuteScalar(
					connection,
					"select count(*) from usersettings.SupplierPromotions where Status = 1"));

			updateData.OldUpdateTime = DateTime.Now.AddHours(-1);
			updateData.Cumulative = true;

			var dataTable = Export<PromotionsExport>("SupplierPromotions");
			//При КО должны получить все промо-акции, которые на данный момент включены
			Assert.That(dataTable.Rows.Count, Is.GreaterThan(0));
			Assert.That(dataTable.Rows.Count, Is.LessThanOrEqualTo(promotionCount));

			updateData.OldUpdateTime = DateTime.Now;
			updateData.Cumulative = false;
			dataTable = Export<PromotionsExport>("SupplierPromotions");
			//При обычном обновлении должны получить акции, которые были обновлены с даты updateDate
			Assert.That(dataTable.Rows.Count, Is.GreaterThanOrEqualTo(0));
		}

		[Test(Description = "При отключении и удалении акций они должны помечаться на удаление")]
		public void DeleteDisableActions()
		{
			var promoId = CreatePromo();
			var promos = GetPromo(promoId);
			Assert.That(promos["Status"], Is.EqualTo(1), "Некорректный статус акции {0}", promoId);

			//После измененной даты обновления ее не должно быть в списке акций на обновление
			updateData.OldUpdateTime = DateTime.Now;
			Thread.Sleep(1000);

			GetPromo(promoId);

			//После отключения акции она должна быть в списке акций на обновление
			MySqlHelper.ExecuteScalar(
				connection,
				@"update usersettings.SupplierPromotions set Status = 0 where Id = ?promoId;",
				new MySqlParameter("?promoId", promoId));

			promos = GetPromo(promoId);
			Assert.That(promos["Status"], Is.EqualTo(0), "Некорректный статус акции {0}", promoId);

			//После измененной даты обновления ее не должно быть в списке акций на обновление
			Thread.Sleep(1000);

			GetPromo(promoId);

			//После удаления акции она должна быть в списке акций на обновление
			MySqlHelper.ExecuteScalar(
				connection,
				@"delete from usersettings.SupplierPromotions where Id = ?promoId;",
				new MySqlParameter("?promoId", promoId));

			promos = GetPromo(promoId);
			Assert.That(promos["Status"], Is.EqualTo(0), "Некорректный статус акции {0}", promoId);
		}

		[Test]
		public void Not_available_promo_should_be_disabled()
		{
			var promoId = CreatePromo();

			user.Client.RegionCode = 2;
			user.Client.MaskRegion = 2;
			user.Client.Save();
			updateData.RegionMask = 2;

			var promo = GetPromo(promoId);
			Assert.That(promo["Status"], Is.EqualTo(0));
		}

		private uint CreatePromo()
		{
			var priceWithPromo = user.GetActivePricesList()[0];
			//Создаем промо-акцию
			var promoId = Convert.ToUInt32(
				MySqlHelper.ExecuteScalar(
					connection,
					@"
insert into usersettings.SupplierPromotions (Status, SupplierId, Name, Annotation, Begin, End, RegionMask)
values (1, ?supplierId, 'test', 'test', curdate(), curdate(), ?regionMask);
select last_insert_id();",
					new MySqlParameter("?supplierId", priceWithPromo.Price.Supplier.Id),
					new MySqlParameter("?regionMask", user.Client.RegionCode)));
			return promoId;
		}

		private DataRow GetPromo(uint promoId)
		{
			var dataTable = Export<PromotionsExport>("SupplierPromotions");

			var promos = dataTable.Select("Id = " + promoId);
			Assert.That(promos.Length, Is.EqualTo(1), "Не найдена акция {0}, хотя она должна передаваться", promoId);
			return promos[0];
		}
	}
}