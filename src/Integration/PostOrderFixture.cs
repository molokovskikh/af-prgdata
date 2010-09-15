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
		private TestOldClient oldClient;
		private TestOldUser oldUser;

		private DataTable offers;

		private string UniqueId;

		[SetUp]
		public void Setup()
		{
			Test.Support.Setup.Initialize();
			ContainerInitializer.InitializerContainerForTests(typeof(SmartOrderRule).Assembly);
			IoC.Container.Register(
				Component.For<ISmartOfferRepository>().ImplementedBy<SmartOfferRepository>()
				);
		
			UniqueId = "123";
			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";

			using (var transaction = new TransactionScope())
			{

				var permission = TestUserPermission.ByShortcut("AF");

				oldClient = TestOldClient.CreateTestClient();
				oldUser = oldClient.Users[0];

				ServiceContext.GetUserName = () => oldUser.OSUserName;

				var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof(ActiveRecordBase));
				try
				{
					session.CreateSQLQuery(@"
				insert into usersettings.AssignedPermissions (PermissionId, UserId) values (:permissionid, :userid)")
						.SetParameter("permissionid", permission.Id)
						.SetParameter("userid", oldUser.Id)
						.ExecuteUpdate();
				}
				finally
				{
					ActiveRecordMediator.GetSessionFactoryHolder().ReleaseSession(session);
				}
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
call usersettings.GetOffers(?ClientCode, 0);
drop temporary table if exists SelectedActivePrices;
create temporary table SelectedActivePrices engine=memory as select * from ActivePrices limit 3;
",
					new MySqlParameter("?ClientCode", oldClient.Id));

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

		[Test(Description = "Проверяем работу с MinReq с несущесвующей записью в Intersection")]
		public void Check_nonExists_MinReq()
		{
			var offer = offers.Rows[0];

			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				"delete from usersettings.Intersection where ClientCode = ?ClientCode and PriceCode = ?PriceCode and RegionCode = ?RegionCode",
				new MySqlParameter("?ClientCode", oldClient.Id),
				new MySqlParameter("?PriceCode", offer["PriceCode"]),
				new MySqlParameter("?RegionCode", offer["RegionCode"]));

			var service = new PrgDataEx();
			var response = service.PostOrder(
				UniqueId,
				0,
				oldClient.Id,
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
		}

		[Test(Description = "Проверяем текст ошибки при нарушении MinReq")]
		public void Check_error_on_MinReq()
		{
			var offer = offers.Rows[0];

			MySqlHelper.ExecuteNonQuery(
				Settings.ConnectionString(),
				"update usersettings.Intersection set ControlMinReq = 1, MinReq = 10000 where ClientCode = ?ClientCode and PriceCode = ?PriceCode and RegionCode = ?RegionCode",
				new MySqlParameter("?ClientCode", oldClient.Id),
				new MySqlParameter("?PriceCode", offer["PriceCode"]),
				new MySqlParameter("?RegionCode", offer["RegionCode"]));

			var service = new PrgDataEx();
			var response = service.PostOrder(
				UniqueId,
				0,
				oldClient.Id,
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
				var maxId = TestAnalitFUpdateLog.Queryable.Where(l => l.UserId == oldUser.Id).Max(l => l.Id);
				var log = TestAnalitFUpdateLog.Queryable.Single(l => l.Id == maxId);
				Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.Forbidden)));
				Assert.That(log.Addition, Is.StringStarting("Сумма заказа меньше минимально допустимой").IgnoreCase, "Неожидаемый Addtion в логе.");
			}
		}

	}
}