using System;
using System.Data;
using Castle.ActiveRecord;
using Common.Tools;
using Integration.BaseTests;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;
using PrgData;
using MySql.Data.MySqlClient;

namespace Integration
{
	[TestFixture]
	public class PostPriceSettingsFixture : PrepareDataFixture
	{
		private TestClient _client;
		private TestUser _user;

		[SetUp]
		public void Setup()
		{
			_user = CreateUser();
			_client = _user.Client;
		}

		[Test]
		public void PostPriceSettings()
		{
			SetCurrentUser(_user.Login);

			using (var connection = new MySqlConnection(Settings.ConnectionString())) {
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);
				helper.Cleanup();
				helper.SelectPrices();

				var pricesSet = MySqlHelper.ExecuteDataset(connection, "select * from Prices limit 2");
				var prices = pricesSet.Tables[0];
				Assert.That(prices.Rows.Count, Is.EqualTo(2), "Нет необходимого количества прайс-листов для теста");

				var injobs = new bool[] { false, true };
				var priceIds = new int[] {
					Convert.ToInt32(prices.Rows[0]["PriceCode"]),
					Convert.ToInt32(prices.Rows[1]["PriceCode"])
				};
				var regionIds = new long[] { Convert.ToInt64(prices.Rows[0]["RegionCode"]), Convert.ToInt64(prices.Rows[1]["RegionCode"]) };

				PostSettings(priceIds, regionIds, injobs);

				injobs = new bool[] { true, false };
				PostSettings(priceIds, regionIds, injobs);
			}
		}

		private string PostSettings(int[] priceIds, long[] regionIds, bool[] injobs)
		{
			var service = new PrgDataEx();
			var responce = service.PostPriceDataSettings(UniqueId, priceIds, regionIds, injobs);

			Assert.That(responce, Is.EqualTo("Res=OK").IgnoreCase, "Отправка настроек прайс-листов завершилась ошибкой.");

			return responce;
		}
	}
}