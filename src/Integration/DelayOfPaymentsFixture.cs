using System;
using System.Data;
using System.Linq;
using Castle.ActiveRecord;
using Common.Tools;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;

namespace Integration
{
	[TestFixture]
	public class DelayOfPaymentsFixture
	{
		TestClient _client;
		TestUser _user;

		private string UniqueId;

		[SetUp]
		public void SetUp()
		{
			UniqueId = "123";
			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ServiceContext.GetResultPath = () => "results\\";

			_client = TestClient.CreateSimple();

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

		}

		[Test(Description = "ѕолучаем значени€ DayOfWeek")]
		public void CheckDayOfWeek()
		{
			Assert.That((int)DayOfWeek.Sunday, Is.EqualTo(0));
			Assert.That((int)DayOfWeek.Monday, Is.EqualTo(1));
			Assert.That((int)DayOfWeek.Tuesday, Is.EqualTo(2));
			Assert.That((int)DayOfWeek.Wednesday, Is.EqualTo(3));
			Assert.That((int)DayOfWeek.Thursday, Is.EqualTo(4));
			Assert.That((int)DayOfWeek.Friday, Is.EqualTo(5));
			Assert.That((int)DayOfWeek.Saturday, Is.EqualTo(6));
		}

		[Test(Description = "ѕробуем загрузить отсрочку платежа из базы")]
		public void TestActiveRecordDelayOfPayment()
		{
			TestDelayOfPayment delay;
			using (new SessionScope())
			{
				delay = TestDelayOfPayment.Queryable.First();
			}
			Assert.That(delay, Is.Not.Null);
			Assert.That(delay.Id, Is.GreaterThan(0));

			var dayOfWeek = MySqlHelper.ExecuteScalar(
				Settings.ConnectionString(),
				"select DayOfWeek from usersettings.DelayOfPayments where Id = ?Id",
				new MySqlParameter("?Id", delay.Id));

			Assert.That(dayOfWeek, Is.Not.Null);
			Assert.That(dayOfWeek, Is.TypeOf<string>());
			Assert.That(dayOfWeek, Is.EqualTo(Enum.GetName(typeof(DayOfWeek), delay.DayOfWeek)));
		}

		private MySqlDataAdapter CreateAdapter(MySqlConnection connection, string sqlCommand, UpdateData updateData)
		{
			var dataAdapter = new MySqlDataAdapter(sqlCommand, connection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", updateData.ClientId);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", updateData.UserId);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?OffersClientCode", updateData.OffersClientCode);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?OffersRegionCode", updateData.OffersRegionCode);
			return dataAdapter;
		}

		[Test]
		public void GetOldDelayOfPayments()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				updateData.BuildNumber = 1385;
				var helper = new UpdateHelper(updateData, connection);

				var dataAdapter = CreateAdapter(connection, helper.GetDelayOfPaymentsCommand(), updateData);
				var table = new DataTable();
				dataAdapter.FillSchema(table, SchemaType.Source);
				Assert.That(table.Columns.Count, Is.EqualTo(2));
				Assert.That(table.Columns.Contains("SupplierId"), Is.True);
				Assert.That(table.Columns.Contains("DelayOfPayment"), Is.True);
			}
		}

		[Test]
		public void GetDelayOfPaymentsWithVitallyImportant()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				updateData.BuildNumber = 1386;
				var helper = new UpdateHelper(updateData, connection);

				var dataAdapter = CreateAdapter(connection, helper.GetDelayOfPaymentsCommand(), updateData);
				var table = new DataTable();
				dataAdapter.FillSchema(table, SchemaType.Source);
				Assert.That(table.Columns.Count, Is.EqualTo(4));
				Assert.That(table.Columns.Contains("SupplierId"), Is.True);
				Assert.That(table.Columns.Contains("DayOfWeek"), Is.True);
				Assert.That(table.Columns.Contains("VitallyImportantDelay"), Is.True);
				Assert.That(table.Columns.Contains("OtherDelay"), Is.True);
			}
		}

		[Test(Description = "ѕровер€ем создание записей в отсрочках платежа при создании новых клиентов")]
		public void CheckInsertToDelayOfPayments()
		{
			var beforeNewClientCount = TestDelayOfPayment.Queryable.Count();
			var newClient = TestClient.CreateSimple();
			var afterNewClientCount = TestDelayOfPayment.Queryable.Count();
			Assert.That(afterNewClientCount, Is.GreaterThan(beforeNewClientCount), "ѕосле создани€ нового клиента не были создано записи в отсрочках платежей, возможно, не работает триггер");

			var firstIntersection = TestSupplierIntersection.Queryable.Where(i => i.Client == newClient).FirstOrDefault();
			Assert.That(firstIntersection, Is.Not.Null, "Ќе найдена кака€-либо запись в SupplierIntersection по клиенту: {0}", newClient.Id);

			var firstDelayRule = TestDelayOfPayment.Queryable.Where(r => r.SupplierIntersectionId == firstIntersection.Id).FirstOrDefault();
			Assert.That(firstDelayRule, Is.Not.Null, "Ќе найдена кака€-либо запись в отсрочках платежа по клиенту: {0}", newClient.Id);

			var rulesBySupplier =
				TestDelayOfPayment.Queryable.Where(r => r.SupplierIntersectionId == firstIntersection.Id).
					ToList();
			Assert.That(
				rulesBySupplier.Count, 
				Is.EqualTo(7), 
				"«аписи в отсрочках платежей созданы не по всем дн€м недели дл€ клиента {0} и поставщика {1}", 
				newClient.Id, 
				firstIntersection.Supplier.Id);
			Assert.That(
				rulesBySupplier.Select(r => r.DayOfWeek), 
				Is.EquivalentTo(Enum.GetValues(typeof(DayOfWeek))), 
				"«аписи в отсрочках платежей дублируютс€ по некоторым дн€м недели дл€ клиента {0} и поставщика {1}", 
				newClient.Id, 
				firstIntersection.Supplier.Id);

			var newSupplier = TestOldClient.CreateTestSupplier();
			var afterNewSupplierCount = TestDelayOfPayment.Queryable.Count();
			Assert.That(afterNewSupplierCount, Is.GreaterThan(afterNewClientCount), "ѕосле создани€ нового поставщика не были создано записи в отсрочках платежей, возможно, не работает триггер");

			var intersectionByNewSupplier =
				TestSupplierIntersection.Queryable.Where(i => i.Client == newClient && i.Supplier == newSupplier).FirstOrDefault();
			Assert.That(intersectionByNewSupplier, Is.Not.Null, "Ќе найдена кака€-либо запись в SupplierIntersection после создани€ нового поставщика по клиенту: {0}", newClient.Id);

			var rulesByNewSupplier =
				TestDelayOfPayment.Queryable.Where(r => r.SupplierIntersectionId == intersectionByNewSupplier.Id).
					ToList();
			Assert.That(
				rulesByNewSupplier.Count,
				Is.EqualTo(7),
				"«аписи в отсрочках платежей созданы не по всем дн€м недели дл€ клиента {0} и поставщика {1}",
				newClient.Id,
				newSupplier.Id);
		}

	}
}