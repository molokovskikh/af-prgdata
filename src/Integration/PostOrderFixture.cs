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
using Integration.BaseTests;
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
	public class PostOrderFixture : PrepareDataFixture
	{
		private TestClient _client;
		private TestUser _user;
		private TestAddress _address;

		private DataTable offers;

		[SetUp]
		public void Setup()
		{
			_user = CreateUser();
			_client = _user.Client;
			_address = _client.Addresses[0];

			SetCurrentUser(_user.Login);

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
call Customers.GetOffers(?UserId);
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

		[Test(Description = "Проверяем работу с MinReq с несущесвующей записью в Intersection"), 
		Ignore("Это поломанная функциональность при отправке заказов из 705 версии")]
		public void Check_nonExists_MinReq()
		{
		    var offer = offers.Rows[0];

		    MySqlHelper.ExecuteNonQuery(
		        Settings.ConnectionString(),
		        "delete from customers.Intersection where ClientId = ?ClientId and PriceId = ?PriceId and RegionId = ?RegionId",
		        new MySqlParameter("?ClientId", _client.Id),
		        new MySqlParameter("?PriceId", offer["PriceCode"]),
		        new MySqlParameter("?RegionId", offer["RegionCode"]));

		    var service = new PrgDataEx();
		    var response = service.PostOrder(
		        UniqueId,
		        0,
		        _address.Id,
		        Convert.ToUInt32(offer["PriceCode"]),
		        Convert.ToUInt64(offer["RegionCode"]),
		        DateTime.Now,
		        "",
		        1,
		        new uint[] {Convert.ToUInt32(offer["ProductId"])},
		        1,
		        new string[] {offer["CodeFirmCr"].ToString()},
		        new uint[] {Convert.ToUInt32(offer["SynonymCode"])},
		        new string[] {offer["SynonymFirmCrCode"].ToString()},
		        new string[] {offer["Code"].ToString()},
		        new string[] {offer["CodeCr"].ToString()},
		        new ushort[] {1}, //Quantity
		        new bool[] {false}, //Junk
		        new bool[] {false}, //Await
		        new decimal[] {Convert.ToDecimal(offer["Cost"])},
		        new string[] {""}, //MinCost
		        new string[] {""}, //MinPriceCode
		        new string[] {""}, //LeaderMinCost
		        new string[] {""}, //RequestRatio
		        new string[] {""}, //OrderCost
		        new string[] {""}, //MinOrderCount
		        new string[] {""} //LeaderMinPriceCode
		        );

		    Assert.That(response, Is.StringStarting("OrderID=").IgnoreCase, "Отправка заказа завершилась ошибкой.");

		    using (new SessionScope())
		    {
		        var logs = TestAnalitFUpdateLog.Queryable.Where(l => l.UserId == _user.Id).OrderByDescending(l => l.Id).ToList();
		        Assert.That(logs.Count, Is.EqualTo(1), "Неожидаемое количество записей в логе {0}: {1}", logs.Count, logs.Implode());
		        Assert.That(logs[0].UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.SendOrder)));
		    }
		}

		[Test(Description = "Проверяем текст ошибки при нарушении MinReq при отправке заказов из старых версий 705-716")]
		public void Check_error_on_MinReq()
		{
		    var offer = offers.Rows[0];

		    MySqlHelper.ExecuteNonQuery(
		        Settings.ConnectionString(),
		        @"
update
  Customers.Users u
  join Customers.Clients c on u.ClientId = c.Id
  join Customers.UserAddresses ua on ua.UserId = u.Id
  join Customers.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
  join Customers.Intersection i on i.ClientId = c.Id
  join Customers.AddressIntersection ai on (ai.IntersectionId = i.Id) and (ai.AddressId = a.Id)
set
  ai.ControlMinReq = 1,
  ai.MinReq = 10000
where
	(u.Id = ?UserId)
and (a.Id = ?AddressId)
and (i.PriceId = ?PriceId)",
		        new MySqlParameter("?UserId", _user.Id),
		        new MySqlParameter("?PriceId", offer["PriceCode"]),
		        new MySqlParameter("?AddressId", _address.Id));

		    var service = new PrgDataEx();
		    var response = service.PostOrder(
		        UniqueId,
		        0,
		        _address.Id,
		        Convert.ToUInt32(offer["PriceCode"]),
		        Convert.ToUInt64(offer["RegionCode"]),
		        DateTime.Now,
		        "",
		        1,
		        new uint[] { Convert.ToUInt32(offer["ProductId"]) },
		        1,
		        new string[] { offer["CodeFirmCr"].ToString() },
		        new uint[] { Convert.ToUInt32(offer["SynonymCode"]) },
		        new string[] { offer["SynonymFirmCrCode"].ToString() },
		        new string[] { offer["Code"].ToString() },
		        new string[] { offer["CodeCr"].ToString() },
		        new ushort[] { 1 }, //Quantity
		        new bool[] { false }, //Junk
		        new bool[] { false }, //Await
		        new decimal[] { Convert.ToDecimal(offer["Cost"]) },
		        new string[] { "" }, //MinCost
		        new string[] { "" }, //MinPriceCode
		        new string[] { "" }, //LeaderMinCost
		        new string[] { "" }, //RequestRatio
		        new string[] { "" }, //OrderCost
		        new string[] { "" }, //MinOrderCount
		        new string[] { "" } //LeaderMinPriceCode
		        );

		    Assert.That(response, Is.StringContaining("Desc=Сумма заказа меньше минимально допустимой").IgnoreCase, "Неожидаемая ошибка при отправке заказа.");

		    using (new SessionScope())
		    {
		        var logs = TestAnalitFUpdateLog.Queryable.Where(l => l.UserId == _user.Id).OrderByDescending(l => l.Id).ToList();
		        Assert.That(logs.Count, Is.EqualTo(1), "Неожидаемое количество записей в логе {0}: {1}", logs.Count, logs.Implode());
		        var log = logs[0];
		        Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.Forbidden)));
		        Assert.That(log.Addition, Is.StringStarting("Сумма заказа меньше минимально допустимой").IgnoreCase, "Неожидаемый Addtion в логе.");
		    }
		}

	}
}