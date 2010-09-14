using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using Castle.ActiveRecord;
using Common.Models.Tests.Repositories;
using Common.Tools;
using NUnit.Framework;
using MySql.Data.MySqlClient;
using PrgData.Common;
using PrgData.Common.Counters;
using Test.Support;


namespace Integration
{
	[TestFixture]
	public class UpdateHelperFixture
	{
		TestClient _client;
		TestUser _user;

		TestOldClient _oldClient;
		TestOldUser _oldUser;

		[TestFixtureSetUp]
		public void FixtureSetUp()
		{
			Test.Support.Setup.Initialize();
			ContainerInitializer.InitializerContainerForTests();

			using (var transaction = new TransactionScope())
			{
				_client = TestClient.CreateSimple();
				_user = _client.Users[0];

				var permission = TestUserPermission.ByShortcut("AF");
				_client.Users.Each(u =>
				{
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				_user.Update();


				_oldClient = TestOldClient.CreateTestClient();
				_oldUser = _oldClient.Users[0];

				var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof(ActiveRecordBase));
				try
				{
					session.CreateSQLQuery(@"
insert into usersettings.AssignedPermissions (PermissionId, UserId) values (:permissionid, :userid)")
						.SetParameter("permissionid", permission.Id)
						.SetParameter("userid", _oldUser.Id)
						.ExecuteUpdate();
				}
				finally
				{
					ActiveRecordMediator.GetSessionFactoryHolder().ReleaseSession(session);
				}

			}
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

		private void CheckFieldLength(MySqlConnection connection, string sqlCommand, UpdateData updateData, KeyValuePair<string, int>[] columns)
		{
			var dataAdapter = CreateAdapter(connection, sqlCommand, updateData);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?UpdateTime", DateTime.Now);
			var table = new DataTable();
			dataAdapter.FillSchema(table, SchemaType.Source);
			foreach (var column in columns)
			{
				Assert.IsTrue(table.Columns.Contains(column.Key), "Не найден столбец {0}", column);
				var dataColumn = table.Columns[column.Key];
				Assert.That(dataColumn.DataType, Is.EqualTo(typeof(string)), "Не сопадает тип столбца {0}", column);
				Assert.That(dataColumn.MaxLength, Is.LessThanOrEqualTo(column.Value), "Не сопадает максимальный размер столбца {0}", column);
			}
		}

		private void CheckFields(UpdateData updateData, UpdateHelper helper, MySqlConnection connection)
		{
			CheckFieldLength(
				connection,
				helper.GetClientCommand(),
				updateData,
				new KeyValuePair<string, int>[]
						{
							new KeyValuePair<string, int>("Name", 50)
						});
			CheckFieldLength(
				connection,
				helper.GetClientsCommand(false),
				updateData,
				new KeyValuePair<string, int>[]
						{
							new KeyValuePair<string, int>("ShortName", 50),
							new KeyValuePair<string, int>("FullName", 255)
						});
			CheckFieldLength(
				connection,
				helper.GetClientsCommand(true),
				updateData,
				new KeyValuePair<string, int>[]
						{
							new KeyValuePair<string, int>("ShortName", 50)
						});
			CheckFieldLength(
				connection,
				helper.GetRegionsCommand(),
				updateData,
				new KeyValuePair<string, int>[]
						{
							new KeyValuePair<string, int>("Region", 25)
						});
			CheckFieldLength(
				connection,
				helper.GetRejectsCommand(false),
				updateData,
				new KeyValuePair<string, int>[]
						{
							new KeyValuePair<string, int>("FullName", 254),
							new KeyValuePair<string, int>("FirmCr", 150)
						});
		}

		[Test]
		public void Check_string_field_lengts_for_old_client()
		{
			using(var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, _oldClient.Users[0].OSUserName);
				var helper = new UpdateHelper(updateData, connection);
				CheckFields(updateData, helper, connection);
			}
		}

		[Test]
		public void Check_string_field_lengts_for_future_client()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);
				CheckFields(updateData, helper, connection);
			}
		}

		[Test(Description = "Получаем данные для пользователя, которому не назначен ни один адрес доставки")]
		public void Get_UserInfo_without_addresses()
		{
			TestUser userWithoutAddresses;
			using (var transaction = new TransactionScope())
			{
				userWithoutAddresses = _client.CreateUser();

				var permission = TestUserPermission.ByShortcut("AF");
				userWithoutAddresses.AssignedPermissions.Add(permission);
				userWithoutAddresses.SendRejects = true;
				userWithoutAddresses.SendWaybills = true;
				userWithoutAddresses.Update();
			}

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, userWithoutAddresses.Login);
				var helper = new UpdateHelper(updateData, connection);
				var dataAdapter = new MySqlDataAdapter(helper.GetUserCommand(), connection);
				var dataTable = new DataTable();
				dataAdapter.Fill(dataTable);
				Assert.That(dataTable.Rows.Count, Is.EqualTo(1), "Кол-во записей в UserInfo не равняется 1, хотя там всегда должна быть одна запись");
				Assert.That(dataTable.Rows[0]["ClientCode"], Is.EqualTo(DBNull.Value), "Столбец ClientCode не содержит значение DBNull, хотя должен, т.к. адреса к пользователю не привязаны");
				Assert.That(dataTable.Rows[0]["RowId"], Is.EqualTo(userWithoutAddresses.Id), "Столбец RowId не сопадает с Id пользователя");

				dataAdapter.SelectCommand.CommandText = helper.GetClientCommand();
				dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", userWithoutAddresses.Id);
				dataTable = new DataTable();
				dataAdapter.Fill(dataTable);
				Assert.That(dataTable.Rows.Count, Is.EqualTo(1), "Кол-во записей в Client не равняется 1, хотя там всегда должна быть одна запись");
				Assert.That(dataTable.Rows[0]["ClientId"], Is.EqualTo(_client.Id), "Столбец ClientId не сопадает с Id клиента");
			}
		}
		

		class CheckedState
		{
			public uint CoreID { get; set; }
			public float? ProducerCost { get; set; }
			public float Cost { get; set; }
			public float? NDS { get; set; }
			public float? SupplierPriceMarkup { get; set; }
		}

		private string GetStringRight(string value, int right)
		{
			if (String.IsNullOrEmpty(value))
				return value;
			if (value.Length <= right)
				return value;
			return value.Substring(value.Length - right, right);
		}

		[Test]
		public void Check_core_command_with_NDS()
		{
			var states = new List<CheckedState>()
			             	{
			             		new CheckedState {ProducerCost = null, Cost = 30, NDS = null, SupplierPriceMarkup = null},
			             		new CheckedState {ProducerCost = 0, Cost = 30, NDS = null, SupplierPriceMarkup = null},
			             		new CheckedState
			             			{ProducerCost = 10, Cost = 30, NDS = null, SupplierPriceMarkup = (30/(10*1.1f) - 1)*100},
			             		new CheckedState {ProducerCost = 10, Cost = 30, NDS = 0, SupplierPriceMarkup = (30f/10f - 1)*100},
			             		new CheckedState
			             			{ProducerCost = 10, Cost = 30, NDS = 10, SupplierPriceMarkup = (30/(10*(1 + 10f/100f)) - 1)*100},
			             		new CheckedState
			             			{ProducerCost = 10, Cost = 30, NDS = 18, SupplierPriceMarkup = (30/(10*(1 + 18f/100f)) - 1)*100},
			             	};

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _oldUser.OSUserName);
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				helper.Cleanup();

				helper.SelectActivePrices();

				helper.SelectOffers();

				var ids = MySqlHelper.ExecuteDataset(connection,
													 @"
select
  Core.Id
from
  Core
  inner join ActivePrices on ActivePrices.PriceCode = Core.PriceCode and ActivePrices.RegionCode = Core.RegionCode and ActivePrices.Fresh = 1
  inner join farm.Core0 c on c.Id = Core.Id
limit "
													 + states.Count);
				for (var i = 0; i < states.Count; i++)
					states[i].CoreID = Convert.ToUInt32(ids.Tables[0].Rows[i]["Id"]);

				var updateCommand = new MySqlCommand(@"
update Core set Cost = ?Cost where Id = ?Id;
update farm.Core0 set ProducerCost = ?ProducerCost, NDS = ?NDS where Id = ?Id;
", connection);
				updateCommand.Parameters.Add("?Id", MySqlDbType.UInt32);
				updateCommand.Parameters.Add("?Cost", MySqlDbType.Float);
				updateCommand.Parameters.Add("?ProducerCost", MySqlDbType.Float);
				updateCommand.Parameters.Add("?NDS", MySqlDbType.Float);

				states.ForEach(item =>
				{
				    updateCommand.Parameters["?Id"].Value = item.CoreID;
					updateCommand.Parameters["?Cost"].Value = item.Cost;
					updateCommand.Parameters["?ProducerCost"].Value = item.ProducerCost;
					updateCommand.Parameters["?NDS"].Value = item.NDS;
				    updateCommand.ExecuteNonQuery();
				});

				states.ForEach(item =>
				{
					var filledCore = MySqlHelper.ExecuteDataset(
						connection,
						helper.GetCoreCommand(false, true, false) + " and Core.Id = " + item.CoreID,
						new MySqlParameter("?Cumulative", 0));


					var rows = filledCore.Tables[0];
					if (rows.Rows.Count == 0)
						Assert.Fail("Не найдено предложение с Id = {0}", item.CoreID);
					else
						if (rows.Rows.Count > 1)
							Assert.Fail("Больше одного предложения с Id = {0}", item.CoreID);
						else
						{
							var calculatedSupplierPriceMarkup = Convert.IsDBNull(rows.Rows[0]["SupplierPriceMarkup"])
							                                    	? null
							                                    	: (float?) Convert.ToSingle(rows.Rows[0]["SupplierPriceMarkup"]);
							if (!item.SupplierPriceMarkup.HasValue)
								Assert.IsNull(calculatedSupplierPriceMarkup, 
									"Неправильно расчитана наценка поставщика для параметров: ProducerCost = {0}; Cost = {1}; NDS = {2}",
									item.ProducerCost,
									item.Cost,
									item.NDS);
							else
								if (item.SupplierPriceMarkup.HasValue && !calculatedSupplierPriceMarkup.HasValue)
									Assert.Fail(
										"Неправильно расчитана наценка поставщика для параметров (наценка = null): ProducerCost = {0}; Cost = {1}; NDS = {2}",
										item.ProducerCost,
										item.Cost,
										item.NDS);
								else
									Assert.IsTrue(Math.Abs(item.SupplierPriceMarkup.Value - calculatedSupplierPriceMarkup.Value) < 0.0001f,
										"Неправильно расчитана наценка поставщика для параметров: ProducerCost = {0}; Cost = {1}; NDS = {2}; calculated = {3}; needed = {4}",
										item.ProducerCost,
										item.Cost,
										item.NDS,
										calculatedSupplierPriceMarkup,
										item.SupplierPriceMarkup);
						}
				});
			}
		}


		private void CheckLocks(string firstLock, string secondLock, bool generateException, string exceptionMessage)
		{
			try
			{
				Counter.Clear();

				Counter.TryLock(_user.Id, firstLock);

				Counter.TryLock(_user.Id, secondLock);

				if (generateException)
					Assert.Fail("Ожидалось исключение для пары методов {0}-{1}: {2}", firstLock, secondLock, exceptionMessage);
			}
			catch (UpdateException exception)
			{
				if (!generateException || !exception.Message.Equals(exceptionMessage))
					Assert.Fail("Неожидаемое исключение для пары методов {0}-{1}: {2}", firstLock, secondLock, exception);
			}

			if (generateException)
			{
				Counter.ReleaseLock(_user.Id, firstLock);
				Counter.TryLock(_user.Id, secondLock);
			}
		}

		private void CheckUpdateLocks(string firstLock, string secondLock)
		{
			CheckLocks(firstLock, secondLock, true, "Обновление данных в настоящее время невозможно.");
		}

		private void CheckUnlockedLocks(string firstLock, string secondLock)
		{
			CheckLocks(firstLock, secondLock, false, null);
		}

		[Test]
		public void Check_PostOrder_lock()
		{
			CheckLocks("PostOrder", "PostOrder", true, "Отправка заказов в настоящее время невозможна.");
		}

		[Test]
		public void Check_SendClientLog_lock()
		{
			CheckUpdateLocks("SendClientLog", "SendClientLog");
		}

		[Test]
		public void Check_ReclameFileHandler_lock()
		{
			CheckUnlockedLocks("ReclameFileHandler", "ReclameFileHandler");
		}

		[Test]
		public void Check_GetUserData_lock()
		{
			var methods = new[] { "GetUserData", "MaxSynonymCode", "CommitExchange", "PostOrderBatch", "FileHandler" };
			for (var i = 0; i < methods.Length; i++)
				for (var j = 0; j < methods.Length; j++)
					CheckUpdateLocks(methods[i], methods[j]);
		}

		[Test]
		public void Check_History_locks()
		{
			var methods = new[] { "GetHistoryOrders", "HistoryFileHandler" };
			for (var i = 0; i < methods.Length; i++)
				for (var j = 0; j < methods.Length; j++)
					CheckLocks(methods[i], methods[j], true, "Загрузка истории заказов в настоящее время невозможна.");
		}

		[Test]
		public void Check_unlocked_locks()
		{
			var methods = new[] { "PostOrderBatch", "PostOrder", "SendClientLog", "ReclameFileHandler", "GetHistoryOrders" };
			for (var i = 0; i < methods.Length; i++)
				for (var j = 0; j < methods.Length; j++)
					if (i != j)
						CheckUnlockedLocks(methods[i], methods[j]);
		}

		[Test]
		public void Check_max_update_client_count()
		{
			Counter.Clear();
			var maxSessionCount = Convert.ToUInt32(ConfigurationManager.AppSettings["MaxGetUserDataSession"]);
			for (uint i = 0; i <= maxSessionCount; i++)
				Counter.TryLock(i, "PostOrderBatch");

			try
			{
				Counter.TryLock(maxSessionCount+1, "GetUserData");

				Assert.Fail("Ожидалось исключение при превышении максимального кол-ва пользователей: {0}", "Обновление данных в настоящее время невозможно.");
			}
			catch (UpdateException exception)
			{
				if (!exception.Message.Equals("Обновление данных в настоящее время невозможно."))
					Assert.Fail("Неожидаемое исключение при превышении максимального кол-ва пользователей: {0}", exception);
			}

			//Освободили предыдущую блокировку
			Counter.ReleaseLock(maxSessionCount, "PostOrderBatch");
			//Попытались наложить блокировку еще раз и она наложилась
			Counter.TryLock(maxSessionCount + 1, "GetUserData");
		}


	}
}
