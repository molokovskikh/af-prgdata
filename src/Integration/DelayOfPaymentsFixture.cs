using System;
using System.Data;
using System.Linq;
using Castle.ActiveRecord;
using Common.Tools;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.AnalitFVersions;
using Test.Support;
using Test.Support.Suppliers;

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

			_client = TestClient.Create();

			using (var transaction = new TransactionScope())
			{
				_user = _client.Users[0];

				_client.Users.Each(u =>
				{
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				_user.Update();
			}

		}

		[Test(Description = "Получаем значения DayOfWeek")]
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

		[Test(Description = "Пробуем загрузить отсрочку платежа из базы")]
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
		public void GetDelayOfPaymentsWithVitallyImportantForUpdate()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				updateData.BuildNumber = 1385;
				updateData.UpdateExeVersionInfo = new VersionInfo(1386);
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

		[Test]
		public void GetDelayOfPaymentsByPrice()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				updateData.BuildNumber = 1405;
				var helper = new UpdateHelper(updateData, connection);

				var dataAdapter = CreateAdapter(connection, helper.GetDelayOfPaymentsCommand(), updateData);
				var table = new DataTable();
				dataAdapter.FillSchema(table, SchemaType.Source);
				Assert.That(table.Columns.Count, Is.EqualTo(4));
				Assert.That(table.Columns.Contains("PriceId"), Is.True);
				Assert.That(table.Columns.Contains("DayOfWeek"), Is.True);
				Assert.That(table.Columns.Contains("VitallyImportantDelay"), Is.True);
				Assert.That(table.Columns.Contains("OtherDelay"), Is.True);
			}
		}

		[Test]
		public void GetDelayOfPaymentsByPriceForUpdateFrom1385()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				updateData.BuildNumber = 1385;
				updateData.UpdateExeVersionInfo = new VersionInfo(1405);
				var helper = new UpdateHelper(updateData, connection);

				var dataAdapter = CreateAdapter(connection, helper.GetDelayOfPaymentsCommand(), updateData);
				var table = new DataTable();
				dataAdapter.FillSchema(table, SchemaType.Source);
				Assert.That(table.Columns.Count, Is.EqualTo(4));
				Assert.That(table.Columns.Contains("PriceId"), Is.True);
				Assert.That(table.Columns.Contains("DayOfWeek"), Is.True);
				Assert.That(table.Columns.Contains("VitallyImportantDelay"), Is.True);
				Assert.That(table.Columns.Contains("OtherDelay"), Is.True);
			}
		}

		[Test]
		public void GetDelayOfPaymentsByPriceForUpdateFrom1403()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				updateData.BuildNumber = 1403;
				updateData.UpdateExeVersionInfo = new VersionInfo(1405);
				var helper = new UpdateHelper(updateData, connection);

				var dataAdapter = CreateAdapter(connection, helper.GetDelayOfPaymentsCommand(), updateData);
				var table = new DataTable();
				dataAdapter.FillSchema(table, SchemaType.Source);
				Assert.That(table.Columns.Count, Is.EqualTo(4));
				Assert.That(table.Columns.Contains("PriceId"), Is.True);
				Assert.That(table.Columns.Contains("DayOfWeek"), Is.True);
				Assert.That(table.Columns.Contains("VitallyImportantDelay"), Is.True);
				Assert.That(table.Columns.Contains("OtherDelay"), Is.True);
			}
		}

		[Test(Description = "Проверяем создание записей в отсрочках платежа при создании новых клиентов")]
		public void CheckInsertToDelayOfPayments()
		{
			var beforeNewClientCount = TestDelayOfPayment.Queryable.Count();
			var newClient = TestClient.Create();
			var afterNewClientCount = TestDelayOfPayment.Queryable.Count();
			Assert.That(afterNewClientCount, Is.GreaterThan(beforeNewClientCount), "После создания нового клиента не были создано записи в отсрочках платежей, возможно, не работает триггер");

			var firstIntersection = TestSupplierIntersection.Queryable.Where(i => i.Client == newClient).FirstOrDefault();
			Assert.That(firstIntersection, Is.Not.Null, "Не найдена какая-либо запись в SupplierIntersection по клиенту: {0}", newClient.Id);
			Assert.That(firstIntersection.PriceIntersections.Count, Is.GreaterThan(0), "Не найдены записи в PriceIntersections по SupplierIntersectionId: {0}", firstIntersection.Id);

			var firstDelayRule = TestDelayOfPayment.Queryable.Where(r => r.PriceIntersectionId == firstIntersection.PriceIntersections[0].Id).FirstOrDefault();
			Assert.That(firstDelayRule, Is.Not.Null, "Не найдена какая-либо запись в отсрочках платежа по клиенту: {0}", newClient.Id);

			var rulesBySupplier =
				TestDelayOfPayment.Queryable.Where(r => r.PriceIntersectionId == firstIntersection.PriceIntersections[0].Id).
					ToList();
			Assert.That(
				rulesBySupplier.Count, 
				Is.EqualTo(7), 
				"Записи в отсрочках платежей созданы не по всем дням недели для клиента {0} и поставщика {1}", 
				newClient.Id, 
				firstIntersection.Supplier.Id);
			Assert.That(
				rulesBySupplier.Select(r => r.DayOfWeek), 
				Is.EquivalentTo(Enum.GetValues(typeof(DayOfWeek))), 
				"Записи в отсрочках платежей дублируются по некоторым дням недели для клиента {0} и поставщика {1}", 
				newClient.Id, 
				firstIntersection.Supplier.Id);

			var newSupplier = TestSupplier.Create();
			var afterNewSupplierCount = TestDelayOfPayment.Queryable.Count();
			Assert.That(afterNewSupplierCount, Is.GreaterThan(afterNewClientCount), "После создания нового поставщика не были создано записи в отсрочках платежей, возможно, не работает триггер");

			var intersectionByNewSupplier =
				TestSupplierIntersection.Queryable.Where(i => i.Client == newClient && i.Supplier == newSupplier).FirstOrDefault();
			Assert.That(intersectionByNewSupplier, Is.Not.Null, "Не найдена какая-либо запись в SupplierIntersection после создания нового поставщика по клиенту: {0}", newClient.Id);
			Assert.That(intersectionByNewSupplier.PriceIntersections.Count, Is.GreaterThan(0), "Не найдены записи в PriceIntersections по SupplierIntersectionId: {0}", intersectionByNewSupplier.Id);

			var rulesByNewSupplier =
				TestDelayOfPayment.Queryable.Where(r => r.PriceIntersectionId == intersectionByNewSupplier.PriceIntersections[0].Id).
					ToList();
			Assert.That(
				rulesByNewSupplier.Count,
				Is.EqualTo(7),
				"Записи в отсрочках платежей созданы не по всем дням недели для клиента {0} и поставщика {1}",
				newClient.Id,
				newSupplier.Id);
		}

	}
}