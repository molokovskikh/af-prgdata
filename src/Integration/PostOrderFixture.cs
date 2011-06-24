using System;
using System.IO;
using System.Linq;
using System.Configuration;
using System.Data;
using System.Text.RegularExpressions;
using Castle.ActiveRecord;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Tests.Repositories;
using Common.Tools;
using Inforoom.Common;
using NUnit.Framework;
using PrgData.Common;
using SmartOrderFactory.Domain;
using SmartOrderFactory.Repositories;
using Test.Support;
using PrgData;
using MySql.Data.MySqlClient;
using NHibernate.Criterion;
using Test.Support.Logs;

namespace Integration
{
	[TestFixture]
	public class PostOrderFixture
	{
		private TestClient _client;
		private TestUser _user;
		private TestAddress _address;

		private DataTable offers;

		private string UniqueId;

		[SetUp]
		public void Setup()
		{
			UniqueId = "123";
			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";

			_client = TestClient.Create();

			using (var transaction = new TransactionScope())
			{
				_user = _client.Users[0];
				_address = _client.Addresses[0];


				ServiceContext.GetUserName = () => _user.Login;

				_client.Users.Each(u =>
				{
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				_user.Update();
			}

			GetLimitOffers();
		}

		private void GetLimitOffers()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				MySqlHelper.ExecuteNonQuery(
					connection,
					@"
drop temporary table if exists Usersettings.Prices, Usersettings.ActivePrices, Usersettings.Core;
call future.GetOffers(?UserId);
drop temporary table if exists SelectedActivePrices;
create temporary table SelectedActivePrices engine=memory as select * from ActivePrices limit 3;
",
					new MySqlParameter("?UserId", _user.Id));

				offers = new DataTable();

				var dataAdapter = new MySqlDataAdapter(@"
select
  Core.Cost,
  Core.RegionCode,
  c.*
from
  SelectedActivePrices
  inner join Core on Core.PriceCode = SelectedActivePrices.PriceCode and Core.RegionCode = SelectedActivePrices.RegionCode
  inner join farm.Core0 c on c.Id = Core.Id
", connection);
				dataAdapter.Fill(offers);
			}
		}

		//[Test(Description = "Проверяем работу с MinReq с несущесвующей записью в Intersection")]
		//public void Check_nonExists_MinReq()
		//{
		//    var offer = offers.Rows[0];

		//    MySqlHelper.ExecuteNonQuery(
		//        Settings.ConnectionString(),
		//        "delete from usersettings.Intersection where ClientCode = ?ClientCode and PriceCode = ?PriceCode and RegionCode = ?RegionCode",
		//        new MySqlParameter("?ClientCode", _client.Id),
		//        new MySqlParameter("?PriceCode", offer["PriceCode"]),
		//        new MySqlParameter("?RegionCode", offer["RegionCode"]));

		//    var service = new PrgDataEx();
		//    var response = service.PostOrder(
		//        UniqueId,
		//        0,
		//        _client.Id,
		//        Convert.ToUInt32(offer["PriceCode"]),
		//        Convert.ToUInt64(offer["RegionCode"]),
		//        DateTime.Now,
		//        "",
		//        1,
		//        new uint[] {Convert.ToUInt32(offer["ProductId"])},
		//        1,
		//        new string[] {offer["CodeFirmCr"].ToString()},
		//        new uint[] {Convert.ToUInt32(offer["SynonymCode"])},
		//        new string[] {offer["SynonymFirmCrCode"].ToString()},
		//        new string[] {offer["Code"].ToString()},
		//        new string[] {offer["CodeCr"].ToString()},
		//        new ushort[] {1}, //Quantity
		//        new bool[] {false}, //Junk
		//        new bool[] {false}, //Await
		//        new decimal[] {Convert.ToDecimal(offer["Cost"])},
		//        new string[] {""}, //MinCost
		//        new string[] {""}, //MinPriceCode
		//        new string[] {""}, //LeaderMinCost
		//        new string[] {""}, //RequestRatio
		//        new string[] {""}, //OrderCost
		//        new string[] {""}, //MinOrderCount
		//        new string[] {""} //LeaderMinPriceCode
		//        );

		//    Assert.That(response, Is.StringStarting("OrderID=").IgnoreCase, "Отправка заказа завершилась ошибкой.");

		//    using (new SessionScope())
		//    {
		//        var logs = TestAnalitFUpdateLog.Queryable.Where(l => l.UserId == _user.Id).OrderByDescending(l => l.Id).ToList();
		//        Assert.That(logs.Count, Is.EqualTo(1), "Неожидаемое количество записей в логе {0}: {1}", logs.Count, logs.Implode());
		//        Assert.That(logs[0].UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.SendOrder)));
		//    }
		//}

		//[Test(Description = "Проверяем текст ошибки при нарушении MinReq")]
		//public void Check_error_on_MinReq()
		//{
		//    var offer = offers.Rows[0];

		//    MySqlHelper.ExecuteNonQuery(
		//        Settings.ConnectionString(),
		//        "update usersettings.Intersection set ControlMinReq = 1, MinReq = 10000 where ClientCode = ?ClientCode and PriceCode = ?PriceCode and RegionCode = ?RegionCode",
		//        new MySqlParameter("?ClientCode", _client.Id),
		//        new MySqlParameter("?PriceCode", offer["PriceCode"]),
		//        new MySqlParameter("?RegionCode", offer["RegionCode"]));

		//    var service = new PrgDataEx();
		//    var response = service.PostOrder(
		//        UniqueId,
		//        0,
		//        _client.Id,
		//        Convert.ToUInt32(offer["PriceCode"]),
		//        Convert.ToUInt64(offer["RegionCode"]),
		//        DateTime.Now,
		//        "",
		//        1,
		//        new uint[] { Convert.ToUInt32(offer["ProductId"]) },
		//        1,
		//        new string[] { offer["CodeFirmCr"].ToString() },
		//        new uint[] { Convert.ToUInt32(offer["SynonymCode"]) },
		//        new string[] { offer["SynonymFirmCrCode"].ToString() },
		//        new string[] { offer["Code"].ToString() },
		//        new string[] { offer["CodeCr"].ToString() },
		//        new ushort[] { 1 }, //Quantity
		//        new bool[] { false }, //Junk
		//        new bool[] { false }, //Await
		//        new decimal[] { Convert.ToDecimal(offer["Cost"]) },
		//        new string[] { "" }, //MinCost
		//        new string[] { "" }, //MinPriceCode
		//        new string[] { "" }, //LeaderMinCost
		//        new string[] { "" }, //RequestRatio
		//        new string[] { "" }, //OrderCost
		//        new string[] { "" }, //MinOrderCount
		//        new string[] { "" } //LeaderMinPriceCode
		//        );

		//    Assert.That(response, Is.StringContaining("Desc=Сумма заказа меньше минимально допустимой").IgnoreCase, "Неожидаемая ошибка при отправке заказа.");

		//    using (new SessionScope())
		//    {
		//        var logs = TestAnalitFUpdateLog.Queryable.Where(l => l.UserId == _user.Id).OrderByDescending(l => l.Id).ToList();
		//        Assert.That(logs.Count, Is.EqualTo(1), "Неожидаемое количество записей в логе {0}: {1}", logs.Count, logs.Implode());
		//        var log = logs[0];
		//        Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.Forbidden)));
		//        Assert.That(log.Addition, Is.StringStarting("Сумма заказа меньше минимально допустимой").IgnoreCase, "Неожидаемый Addtion в логе.");
		//    }
		//}

	}
}