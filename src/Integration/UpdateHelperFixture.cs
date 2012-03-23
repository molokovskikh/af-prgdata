using System;
using System.Collections.Generic;
using System.Linq;
using System.Configuration;
using System.Data;
using Castle.ActiveRecord;
using Common.Tools;
using NUnit.Framework;
using MySql.Data.MySqlClient;
using PrgData.Common;
using PrgData.Common.Counters;
using Test.Support;
using Test.Support.Suppliers;


namespace Integration
{
	[TestFixture]
	public class UpdateHelperFixture
	{
		TestClient _client;
		TestUser _user;

		[SetUp]
		public void SetUp()
		{
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

		private void ClearLocks()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				MySqlHelper.ExecuteNonQuery(connection, "delete from Logs.PrgDataLogs");
			}
		}

		[Test]
		public void Check_string_field_lengts_for_future_client()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);
				CheckFields(updateData, helper, connection);
			}
		}

		[Test(Description = "Проверка поля Clients.ShortName для клиентов из новой реальности для версий программы больше 1271 или обновляющихся на нее")]
		public void Check_Clients_field_lengts_for_future_client_with_version_greater_than_1271()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				updateData.BuildNumber = 1272;

				var firebirdSQL = helper.GetClientsCommand(true);
				var nonFirebirdSQL = helper.GetClientsCommand(false);

				Assert.That(firebirdSQL, Is.EqualTo(nonFirebirdSQL), "Два SQL-запроса по содержанию не равны");

				CheckFieldLength(
					connection,
					firebirdSQL,
					updateData,
					new KeyValuePair<string, int>[]
						{
							new KeyValuePair<string, int>("ShortName", 255)
						});

				CheckFieldLength(
					connection,
					nonFirebirdSQL,
					updateData,
					new KeyValuePair<string, int>[]
						{
							new KeyValuePair<string, int>("ShortName", 255)
						});

				updateData.BuildNumber = null;
				//Явно устанавливаем значение свойства NeedUpdateToNewClientsWithLegalEntity в true, чтобы проверить функциональность при обновлении версий
				typeof(UpdateData)
					.GetProperty("NeedUpdateToNewClientsWithLegalEntity")
					.SetValue(updateData, true, null);
				Assert.IsTrue(updateData.NeedUpdateToNewClientsWithLegalEntity, "Не получилось установить значение свойства NeedUpdateToNewClientsWithLegalEntity");

				var updateToNewClientsSQL = helper.GetClientsCommand(false);
				Assert.That(firebirdSQL, Is.EqualTo(updateToNewClientsSQL), "Два SQL-запроса по содержанию не равны");
				CheckFieldLength(
					connection,
					updateToNewClientsSQL,
					updateData,
					new KeyValuePair<string, int>[]
						{
							new KeyValuePair<string, int>("ShortName", 255)
						});
			}
		}


		[Test(Description = "Получаем данные для пользователя, которому не назначен ни один адрес доставки")]
		public void Get_UserInfo_without_addresses()
		{
			TestUser userWithoutAddresses;
			using (var transaction = new TransactionScope())
			{
				userWithoutAddresses = _client.CreateUser();

				userWithoutAddresses.SendRejects = true;
				userWithoutAddresses.SendWaybills = true;
				userWithoutAddresses.Update();
			}

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
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
			public ulong CoreID { get; set; }
			public float? ProducerCost { get; set; }
			public float Cost { get; set; }
			public float? NDS { get; set; }
			public float? SupplierPriceMarkup { get; set; }
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
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				helper.Cleanup();

				helper.SelectPrices();
				helper.SelectReplicationInfo();
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
					states[i].CoreID = Convert.ToUInt64(ids.Tables[0].Rows[i]["Id"]);

				var updateCommand = new MySqlCommand(@"
update Core set Cost = ?Cost where Id = ?Id;
update farm.Core0 set ProducerCost = ?ProducerCost, NDS = ?NDS where Id = ?Id;
", connection);
				updateCommand.Parameters.Add("?Id", MySqlDbType.UInt64);
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
						helper.GetCoreCommand(false, true, false, false) //+ " and Core.Id = " + item.CoreID
						,
						new MySqlParameter("?Cumulative", 0));

					filledCore.Tables[0].DefaultView.RowFilter = "CoreId = '" + item.CoreID.ToString().RightSlice(9) + "'";


					var rows = filledCore.Tables[0].DefaultView.ToTable();
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
			uint firstLockId = 0;
			try
			{
				ClearLocks();

				firstLockId = Counter.TryLock(_user.Id, firstLock);

				var secondLockId = Counter.TryLock(_user.Id, secondLock);

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
				Counter.ReleaseLock(_user.Id, firstLock, firstLockId);
				var errorLockId = Counter.TryLock(_user.Id, secondLock);
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
			var methods = new[] { "GetUserData", "MaxSynonymCode", "CommitExchange", "PostOrderBatch" };
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
			var methods = new[] { "PostOrderBatch", "PostOrder", "SendClientLog", "ReclameFileHandler", "GetHistoryOrders", "FileHandler" };
			for (var i = 0; i < methods.Length; i++)
				for (var j = 0; j < methods.Length; j++)
					if (i != j)
						CheckUnlockedLocks(methods[i], methods[j]);
		}

		[Test]
		public void Check_max_update_client_count()
		{
			ClearLocks();
			var maxSessionCount = Convert.ToUInt32(ConfigurationManager.AppSettings["MaxGetUserDataSession"]);
			uint lastPostOrderBatchLockId = 0;
			for (uint i = 0; i <= maxSessionCount; i++) {
				lastPostOrderBatchLockId = Counter.TryLock(i, "PostOrderBatch");
			}

			try
			{
				uint maxLockId = Counter.TryLock(maxSessionCount+1, "GetUserData");

				Assert.Fail("Ожидалось исключение при превышении максимального кол-ва пользователей: {0}", "Обновление данных в настоящее время невозможно.");
			}
			catch (UpdateException exception)
			{
				if (!exception.Message.Equals("Обновление данных в настоящее время невозможно."))
					Assert.Fail("Неожидаемое исключение при превышении максимального кол-ва пользователей: {0}", exception);
			}

			//Освободили предыдущую блокировку
			Counter.ReleaseLock(maxSessionCount, "PostOrderBatch", lastPostOrderBatchLockId);
			//Попытались наложить блокировку еще раз и она наложилась
			var lastLockId = Counter.TryLock(maxSessionCount + 1, "GetUserData");
			Assert.That(lastLockId, Is.GreaterThan(0));
		}

		[Test(Description = "Проверка значения поля Clients.ShortName для клиентов из новой реальности для версий программы больше 1271 с одним юридическим лицом")]
		public void Check_Clients_content_for_future_client_with_version_greater_than_1271_and_one_LegalEntity()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				updateData.BuildNumber = 1272;

				var dataAdapter = new MySqlDataAdapter(helper.GetClientsCommand(false), connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _user.Id);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?OffersRegionCode", updateData.OffersRegionCode);

				var clients = new DataTable();
				dataAdapter.Fill(clients);

				Assert.That(clients.Rows.Count, Is.EqualTo(_user.AvaliableAddresses.Count(item => item.Enabled)), "Не совпадает кол-во адресов доставки");
				var address =
					_user.AvaliableAddresses.FirstOrDefault(item => item.Id.ToString().Equals(clients.Rows[0]["FirmCode"].ToString()));
				Assert.That(address, Is.Not.Null, "Не нашли выгруженный адрес доставки");
				Assert.That(clients.Rows[0]["ShortName"].ToString(), Is.EqualTo(address.Value), "Не совпадает значение адреса");
			}
		}

		[Test(Description = "Проверка значения поля Clients.ShortName для клиентов из новой реальности для версий программы больше 1271 с несколькими юридическими лицами")]
		public void Check_Clients_content_for_future_client_with_version_greater_than_1271_and_same_LegalEntities()
		{
			TestAddress newAddress;
			TestLegalEntity newLegalEntity;

			using (var transaction = new TransactionScope(OnDispose.Rollback))
			{
				newLegalEntity = _client.CreateLegalEntity();

				newAddress = _client.CreateAddress();
				newAddress.LegalEntity = newLegalEntity;
				_user.JoinAddress(newAddress);

				_client.Update();

				transaction.VoteCommit();
			}

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				updateData.BuildNumber = 1272;

				var dataAdapter = new MySqlDataAdapter(helper.GetClientsCommand(false), connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _user.Id);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?OffersRegionCode", updateData.OffersRegionCode);

				var clients = new DataTable();
				dataAdapter.Fill(clients);
				
				Assert.That(clients.Rows.Count, Is.EqualTo(_user.AvaliableAddresses.Count(item => item.Enabled)), "Не совпадает кол-во адресов доставки");

				foreach (var enabledAddress in _user.AvaliableAddresses.Where(item => item.Enabled))
				{
					var rows = clients.Select("FirmCode = " + enabledAddress.Id);
					if (rows == null || rows.Length == 0)
						Assert.Fail("В списке клиентов не найден включенный адрес доставки: {0}", enabledAddress);
					var addressName = String.Format("{0}, {1}", enabledAddress.LegalEntity.Name, enabledAddress.Value);
					Assert.That(rows[0]["ShortName"].ToString(), Is.EqualTo(addressName), "Не совпадает значение адреса");
				}

				//проверка выгрузки поля, необходимого в "сетевой" версии AnalitF для разбора внешних заказов
				//Установили прайс поставщика Инфорум
				updateData.NetworkPriceId = 2647;
				dataAdapter.SelectCommand.CommandText = helper.GetClientsCommand(false);
				clients = new DataTable();
				dataAdapter.Fill(clients);
				DataRow row = clients.Rows[0];
				if (!String.IsNullOrEmpty(row["SelfClientId"].ToString()))
					Assert.That(row["FirmCode"].ToString(), Is.Not.EqualTo(row["SelfClientId"].ToString()));
			}
		}

		[Test(Description = "Проверка значения поля Clients.ShortName для клиентов из новой реальности для версий программы больше 1271 с несколькими юридическими лицами с уникальным набором адресов")]
		public void Check_Clients_content_for_future_client_with_version_greater_than_1271_and_same_LegalEntities_with_different_addresses()
		{
			TestAddress newAddress;
			TestLegalEntity newLegalEntity;

			using (var transaction = new TransactionScope(OnDispose.Rollback))
			{
				newLegalEntity = _client.CreateLegalEntity();

				newAddress = _client.CreateAddress();
				newAddress.LegalEntity = newLegalEntity;

				_client.Update();

				transaction.VoteCommit();
			}

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				updateData.BuildNumber = 1272;

				var dataAdapter = new MySqlDataAdapter(helper.GetClientsCommand(false), connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _user.Id);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?OffersRegionCode", updateData.OffersRegionCode);

				var clients = new DataTable();
				dataAdapter.Fill(clients);

				Assert.That(clients.Rows.Count, Is.EqualTo(_user.AvaliableAddresses.Count(item => item.Enabled)), "Не совпадает кол-во адресов доставки");

				foreach (var enabledAddress in _user.AvaliableAddresses.Where(item => item.Enabled))
				{
					var rows = clients.Select("FirmCode = " + enabledAddress.Id);
					if (rows == null || rows.Length == 0)
						Assert.Fail("В списке клиентов не найден включенный адрес доставки: {0}", enabledAddress);
					var addressName = String.Format("{0}, {1}", enabledAddress.LegalEntity.Name, enabledAddress.Value);
					Assert.That(rows[0]["ShortName"].ToString(), Is.EqualTo(addressName), "Не совпадает значение адреса");
				}

				//проверка выгрузки поля, необходимого в "сетевой" версии AnalitF для разбора внешних заказов
				//Установили прайс поставщика Инфорум
				updateData.NetworkPriceId = 2647;
				dataAdapter.SelectCommand.CommandText = helper.GetClientsCommand(false);
				clients = new DataTable();
				dataAdapter.Fill(clients);
				DataRow row = clients.Rows[0];
				if (!String.IsNullOrEmpty(row["SelfClientId"].ToString()))
					Assert.That(row["FirmCode"].ToString(), Is.Not.EqualTo(row["SelfClientId"].ToString()));
			}
		}

		[Test(Description = "Проверяем установку поля SelfClientId в зависимости от значений параметра NetworkPriceId")]
		public void Check_SelfClientId_by_NetworkPriceId()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);

				//Установили прайс поставщика Инфорум
				updateData.NetworkPriceId = 2647;

				var helper = new UpdateHelper(updateData, connection);

				var dataAdapter = new MySqlDataAdapter(helper.GetClientsCommand(false), connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _user.Id);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?OffersRegionCode", updateData.OffersRegionCode);

				var clients = new DataTable();
				dataAdapter.Fill(clients);

				var row = clients.Rows[0];
				if (!String.IsNullOrEmpty(row["SelfClientId"].ToString()))
					Assert.That(row["FirmCode"].ToString(), Is.Not.EqualTo(row["SelfClientId"].ToString()));

				//Установили параметр в null
				updateData.NetworkPriceId = null;
				dataAdapter.SelectCommand.CommandText = helper.GetClientsCommand(false);
				clients = new DataTable();
				dataAdapter.Fill(clients);
				Assert.That(clients.Columns.Contains("SelfClientId"), Is.EqualTo(false));
			}
		}

		[Test(Description = "Проверяем установку поля SelfClientId в зависимости от значений параметра NetworkPriceId для клиентов с несколькими юридическими лицами")]
		public void Check_SelfClientId_for_future_client_with_version_greater_than_1271_and_same_LegalEntities()
		{
			TestAddress newAddress;
			TestLegalEntity newLegalEntity;

			using (var transaction = new TransactionScope(OnDispose.Rollback))
			{
				newLegalEntity = _client.CreateLegalEntity();

				newAddress = _client.CreateAddress();
				newAddress.LegalEntity = newLegalEntity;
				_user.JoinAddress(newAddress);

				_client.Update();

				transaction.VoteCommit();
			}

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				updateData.BuildNumber = 1272;

				//проверка выгрузки поля, необходимого в "сетевой" версии AnalitF для разбора внешних заказов
				//Установили прайс поставщика Инфорум
				updateData.NetworkPriceId = 2647;

				var dataAdapter = new MySqlDataAdapter(helper.GetClientsCommand(false), connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _user.Id);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?OffersRegionCode", updateData.OffersRegionCode);

				var clients = new DataTable();
				dataAdapter.Fill(clients);

				var row = clients.Rows[0];
				if (!String.IsNullOrEmpty(row["SelfClientId"].ToString()))
					Assert.That(row["FirmCode"].ToString(), Is.Not.EqualTo(row["SelfClientId"].ToString()));

				//Установили параметр в null
				updateData.NetworkPriceId = null;
				dataAdapter.SelectCommand.CommandText = helper.GetClientsCommand(false);
				clients = new DataTable();
				dataAdapter.Fill(clients);
				Assert.That(clients.Columns.Contains("SelfClientId"), Is.EqualTo(false));
			}
		}

		[Test(Description = "Это тест для проверки чтение с помощью коннектора, надо перенести в другое место")]
		public void TestCallCount()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				var fillSql = helper.GetClientsCommand(true);

				var dataAdapter = CreateAdapter(connection, fillSql, updateData);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?UpdateTime", DateTime.Now);
				var table = new DataTable();
				dataAdapter.FillSchema(table, SchemaType.Source);

				//В предыдущих версиях коннектера при версии базы данных больше 5.5 после FillSchema запрос ExecuteScalar возвращал null,
				//что быть не должно

				var count = MySqlHelper.ExecuteScalar(connection, @"
	SELECT 
		count(distinct le.Id)
	FROM 
	Future.Users u
	  join future.Clients c on u.ClientId = c.Id
	  join Future.UserAddresses ua on ua.UserId = u.Id
	  join future.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
	  join billing.LegalEntities le on le.Id = a.LegalEntityId
	WHERE 
		u.Id = ?UserId
	and a.Enabled = 1",
				  new MySqlParameter("?UserId", _user.Id));

				Assert.That(count, Is.Not.Null);
			}
		}

		public static DataTable CompareTwoDataTable(DataTable dt1, DataTable dt2)
		{

			dt1.Merge(dt2);

			DataTable d3 = dt2.GetChanges();

			return d3;
		}

		[Test]
		public void Check_core_count_with_GroupBy()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				helper.Cleanup();

				helper.SelectPrices();
				helper.SelectReplicationInfo();
				helper.SelectActivePrices();

				helper.SelectOffers();

				var coreSql = helper.GetCoreCommand(false, true, false, false);
				var lastIndex = coreSql.LastIndexOf("group by", StringComparison.OrdinalIgnoreCase);
				var withoutGroupCoreSql = coreSql.Slice(lastIndex);

				MySqlHelper.ExecuteNonQuery(
					connection,
					@"
drop temporary table if exists usersettings.GroupByCore, usersettings.PureCore;");

				var startGroupBy = DateTime.Now;
				MySqlHelper.ExecuteNonQuery(
					connection,
					String.Format(
					@"
create temporary table usersettings.GroupByCore engine=memory as 
{0}
;
"
					,
					coreSql),
					new MySqlParameter("?Cumulative", 0));
				Console.WriteLine("fill group: {0}", DateTime.Now.Subtract(startGroupBy));

				var startPure = DateTime.Now;
				MySqlHelper.ExecuteNonQuery(
					connection,
					String.Format(
					@"
create temporary table usersettings.PureCore engine=memory as
select * from usersettings.GroupByCore limit 0;
insert into usersettings.PureCore
{0}
;
"
					,
					withoutGroupCoreSql),
					new MySqlParameter("?Cumulative", 0));
				Console.WriteLine("fill pure: {0}", DateTime.Now.Subtract(startPure));

				var withGroupBy = 
					MySqlHelper.ExecuteDataset(
						connection,
						"select * from usersettings.GroupByCore");
				var withGroupByCore = withGroupBy.Tables[0];

				var withoutGroupBy =
					MySqlHelper.ExecuteDataset(
						connection,
						"select * from usersettings.PureCore");
				var withoutGroupByCore = withoutGroupBy.Tables[0];

				Console.WriteLine("withGroupByCore : {0}", withGroupByCore.Rows.Count);
				Console.WriteLine("withoutGroupByCore : {0}", withoutGroupByCore.Rows.Count);

				var changes = CompareTwoDataTable(withGroupByCore, withoutGroupByCore);

				Assert.That(changes, Is.Null);

				MySqlHelper.ExecuteNonQuery(
					connection,
					@"
drop temporary table if exists usersettings.GroupByCore, usersettings.PureCore;");
			}
		}

		[Test(Description = "Все актуальные прайс-листы при первом подключении к клиенту должны быть свежими")]
		public void CheckActivePricesFreshAfterCreate()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				var SelProc = new MySqlCommand();
				SelProc.Connection = connection;

				helper.SetUpdateParameters(SelProc, false, DateTime.Now.AddHours(-1), DateTime.Now);

				helper.Cleanup();

				helper.SelectPrices();
				helper.PreparePricesData(SelProc);
				helper.SelectReplicationInfo();
				helper.SelectActivePrices();

				var notFreshCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					connection,
					@"
select 
 count(*)
from 
	Prices 
	left join ActivePrices on ActivePrices.PriceCode = Prices.PriceCode and ActivePrices.RegionCode = ActivePrices.RegionCode
where
	(ActivePrices.PriceCode is not null and Prices.actual = 1 and ActivePrices.Fresh = 0)"));

				Assert.That(notFreshCount, Is.EqualTo(0), "Все актуальные прайсы при первом подключении к клиенту должны быть свежими");

				var notActualCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					connection,
					@"
select 
 count(*)
from 
	Prices 
	left join ActivePrices on ActivePrices.PriceCode = Prices.PriceCode and ActivePrices.RegionCode = ActivePrices.RegionCode
where
	(ActivePrices.PriceCode is null and Prices.actual > 0)"));

				Assert.That(notActualCount, Is.EqualTo(0), "Неактуальный прайс-лист был добавлен в ActivePrices");

				SelProc.CommandText = helper.GetPricesDataCommand();
				var dataAdapter = new MySqlDataAdapter(SelProc);
				var prices = new DataTable();
				dataAdapter.Fill(prices);

				var freshRows = prices.Select("Fresh = 1");
				Assert.That(freshRows.Length, Is.EqualTo(prices.Rows.Count), "Все прайсы при первом подключении к клиенту должны быть свежими");
			}
		}

		[Test(Description = "Все прайс-листы должны быть несвежими после подтверждения обновления")]
		public void CheckActivePricesFreshAfterConfirm()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				MySqlHelper.ExecuteScalar(
					connection,
					@"
update
	AnalitFReplicationInfo
set
	ForceReplication = 0
where
	UserId = ?UserId
and ForceReplication > 0;",
					new MySqlParameter("?UserId", _user.Id));

				var SelProc = new MySqlCommand();
				SelProc.Connection = connection;

				helper.SetUpdateParameters(SelProc, false, DateTime.Now.AddHours(-1), DateTime.Now);

				helper.Cleanup();

				helper.SelectPrices();
				helper.PreparePricesData(SelProc);
				helper.SelectReplicationInfo();
				helper.SelectActivePrices();

				var freshCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					connection,
					@"
select 
 count(*)
from 
	Prices 
	left join ActivePrices on ActivePrices.PriceCode = Prices.PriceCode and ActivePrices.RegionCode = ActivePrices.RegionCode
where
	(ActivePrices.PriceCode is not null and ActivePrices.Fresh = 1)"));

				Assert.That(freshCount, Is.EqualTo(0), "Все актуальные прайсы после обнвовления должны быть несвежими");

				var nonActualPrices = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					connection,
					@"
select 
 count(*)
from 
	Prices 
where
	Prices.Actual = 0"));
				SelProc.CommandText = helper.GetPricesDataCommand();
				var dataAdapter = new MySqlDataAdapter(SelProc);
				var prices = new DataTable();
				dataAdapter.Fill(prices);

				var freshRows = prices.Select("Fresh = 1");
				Assert.That(freshRows.Length - nonActualPrices, Is.EqualTo(0), "Все актуальные прайсы после обновления должны быть несвежими");
			}
		}

		[Test(Description = "После отключения прайс-листа клиентом, он должен быть помечен как свежий")]
		public void CheckActivePricesFreshAfterDeletePrice()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				MySqlHelper.ExecuteScalar(
					connection,
					@"
update
	AnalitFReplicationInfo
set
	ForceReplication = 0
where
	UserId = ?UserId
and ForceReplication > 0;",
					new MySqlParameter("?UserId", _user.Id));

				helper.Cleanup();

				helper.SelectPrices();

				var disabledPrice = MySqlHelper.ExecuteDataset(
					connection,
					"select * from Prices where Actual = 1 and DisabledByClient = 0 limit 1").Tables[0].Rows[0];
				MySqlHelper.ExecuteScalar(
					connection,
					@"
delete from
	future.UserPrices
where
	PriceId = ?PriceId
and RegionId = ?RegionId
and UserId = ?UserId;",
					new MySqlParameter("?UserId", _user.Id),
					new MySqlParameter("?PriceId", disabledPrice["PriceCode"]),
					new MySqlParameter("?RegionId", disabledPrice["RegionCode"]));


				var SelProc = new MySqlCommand();
				SelProc.Connection = connection;

				helper.SetUpdateParameters(SelProc, false, DateTime.Now.AddHours(-1), DateTime.Now);

				helper.Cleanup();

				helper.SelectPrices();
				helper.PreparePricesData(SelProc);
				helper.SelectReplicationInfo();
				helper.SelectActivePrices();

				var nonActualPrices = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					connection,
					@"
select 
 count(*)
from 
	Prices 
where
	Prices.Actual = 0"));

				SelProc.CommandText = helper.GetPricesDataCommand();
				var dataAdapter = new MySqlDataAdapter(SelProc);
				var prices = new DataTable();
				dataAdapter.Fill(prices);

				var disabledPrices = prices.Select("PriceCode = {0}".Format(disabledPrice["PriceCode"]));
				Assert.That(disabledPrices.Length, Is.EqualTo(1), "Не найден отключенный прайс-лист");
				Assert.That(disabledPrices[0]["Fresh"], Is.EqualTo(1), "Отключенный прайс-лист должен быть помечен как свежий");

				var pricesBySupplier = prices.Select("FirmCode = {0}".Format(disabledPrice["FirmCode"]));

				var freshRows = prices.Select("Fresh = 1");
				Assert.That(freshRows.Length - nonActualPrices, Is.EqualTo(pricesBySupplier.Length), "Кроме неактуальных прайс-листов свежими должны быть помечены все прайс-листы поставщика, у которого отключили прайс-лист");
			}
		}

		[Test(Description = "После потери актуальности прайс-листа, он должен быть помечен как свежий")]
		public void CheckActivePricesFreshAfterUnactual()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				MySqlHelper.ExecuteScalar(
					connection,
					@"
update
	AnalitFReplicationInfo
set
	ForceReplication = 0
where
	UserId = ?UserId
and ForceReplication > 0;",
					new MySqlParameter("?UserId", _user.Id));

				helper.Cleanup();

				helper.SelectPrices();

				var nonActualPricesCount = Convert.ToInt32(MySqlHelper.ExecuteScalar(
					connection,
					@"
select 
 count(*)
from 
	Prices 
where
	Prices.Actual = 0"));

				var nonActualPrice = MySqlHelper.ExecuteDataset(
					connection,
					"select * from Prices where Actual = 1 and DisabledByClient = 0 Order by PriceDate asc limit 1").Tables[0].Rows[0];

				MySqlHelper.ExecuteScalar(
					connection,
					@"
update
  usersettings.PricesCosts,
  usersettings.PriceItems,
  farm.FormRules
set
  PriceItems.PriceDate = PriceItems.PriceDate - interval (FormRules.MaxOld + 1) day
where
	PricesCosts.PriceCode = ?PriceId
and PricesCosts.CostCode = ?CostId
and PriceItems.Id = PricesCosts.PriceItemId
and FormRules.Id = PriceItems.FormRuleId;
",
					new MySqlParameter("?PriceId", nonActualPrice["PriceCode"]),
					new MySqlParameter("?CostId", nonActualPrice["CostCode"]));


				var SelProc = new MySqlCommand();
				SelProc.Connection = connection;

				helper.SetUpdateParameters(SelProc, false, DateTime.Now.AddHours(-1), DateTime.Now);

				helper.Cleanup();

				helper.SelectPrices();
				helper.PreparePricesData(SelProc);
				helper.SelectReplicationInfo();
				helper.SelectActivePrices();

				SelProc.CommandText = helper.GetPricesDataCommand();
				var dataAdapter = new MySqlDataAdapter(SelProc);
				var prices = new DataTable();
				dataAdapter.Fill(prices);

				var nonActualPrices = prices.Select("PriceCode = {0}".Format(nonActualPrice["PriceCode"]));
				Assert.That(nonActualPrices.Length, Is.EqualTo(1), "Не найден отключенный прайс-лист");
				Assert.That(nonActualPrices[0]["Fresh"], Is.EqualTo(1), "Неактуальный прайс-лист должен быть помечен как свежий");

				var freshRows = prices.Select("Fresh = 1");
				Assert.That(freshRows.Length, Is.EqualTo(nonActualPricesCount + 1), "Кол-во свежих прайс-листов должно быть увеличено на один");

				MySqlHelper.ExecuteScalar(
					connection,
					@"
update
  usersettings.PricesCosts,
  usersettings.PriceItems,
  farm.FormRules
set
  PriceItems.PriceDate = ?PriceDate
where
	PricesCosts.PriceCode = ?PriceId
and PricesCosts.CostCode = ?CostId
and PriceItems.Id = PricesCosts.PriceItemId
and FormRules.Id = PriceItems.FormRuleId;
",
					new MySqlParameter("?PriceId", nonActualPrice["PriceCode"]),
					new MySqlParameter("?CostId", nonActualPrice["CostCode"]),
					new MySqlParameter("?PriceDate", nonActualPrice["PriceDate"]));
			}
		}

		[Test(Description = "При кумулятивном обновлении все прайс-листы должны быть свежими")]
		public void CheckActivePricesFreshOnCumulative()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				MySqlHelper.ExecuteScalar(
					connection,
					@"
update
	AnalitFReplicationInfo
set
	ForceReplication = 0
where
	UserId = ?UserId
and ForceReplication > 0;",
					new MySqlParameter("?UserId", _user.Id));

				var SelProc = new MySqlCommand();
				SelProc.Connection = connection;

				helper.SetUpdateParameters(SelProc, true, DateTime.Now.AddHours(-1), DateTime.Now);

				helper.Cleanup();

				helper.SelectPrices();
				helper.PreparePricesData(SelProc);
				helper.SelectReplicationInfo();
				helper.SelectActivePrices();

				SelProc.CommandText = helper.GetPricesDataCommand();
				var dataAdapter = new MySqlDataAdapter(SelProc);
				var prices = new DataTable();
				dataAdapter.Fill(prices);

				var freshRows = prices.Select("Fresh = 1");
				Assert.That(freshRows.Length, Is.EqualTo(prices.Rows.Count), "Все прайс-листы должны быть свежими");
			}
		}

		[Test(Description = "проверяем работу метода UpdateBuildNumber")]
		public void TestUpdateBuildNumber()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				MySqlHelper.ExecuteNonQuery(
					connection,
					"update usersettings.UserUpdateInfo set AFAppVersion = 0 where UserId = ?UserId",
					new MySqlParameter("?UserId", _user.Id));

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);

				Assert.That(updateData.KnownBuildNumber, Is.EqualTo(0));

				const int changedBuildNumber = 1300;
				updateData.BuildNumber = changedBuildNumber;
				UpdateHelper.UpdateBuildNumber(connection, updateData);

				var changedUpdateData = UpdateHelper.GetUpdateData(connection, _user.Login);

				Assert.That(changedUpdateData.KnownBuildNumber, Is.EqualTo(changedBuildNumber), "Не сохранился номер версии AnalitF");
			}
		}

		[Test(Description = "Проверяем доступность столбцов UseAdjustmentOrders, ShowSupplierCost")]
		public void CheckChangeUseAdjustmentOrders()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				MySqlHelper.ExecuteNonQuery(
					connection,
					"update Future.Users set UseAdjustmentOrders = 0, ShowSupplierCost = 0 where Id = ?UserId",
					new MySqlParameter("?UserId", _user.Id));

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);
				var dataAdapter = new MySqlDataAdapter(helper.GetUserCommand(), connection);
				var dataTable = new DataTable();
				dataAdapter.Fill(dataTable);
				Assert.That(dataTable.Rows.Count, Is.EqualTo(1), "Кол-во записей в UserInfo не равняется 1, хотя там всегда должна быть одна запись");
				Assert.That(dataTable.Rows[0]["RowId"], Is.EqualTo(_user.Id), "Столбец RowId не сопадает с Id пользователя");
				Assert.That(dataTable.Columns.Contains("UseAdjustmentOrders"), Is.EqualTo(true), "Не найден столбец UseAdjustmentOrders");
				Assert.That(Convert.ToBoolean(dataTable.Rows[0]["UseAdjustmentOrders"]), Is.EqualTo(false), "Свойство UseAdjustmentOrders не соответствует значению в базе");
				Assert.That(dataTable.Columns.Contains("ShowSupplierCost"), Is.EqualTo(true), "Не найден столбец ShowSupplierCost");
				Assert.That(Convert.ToBoolean(dataTable.Rows[0]["ShowSupplierCost"]), Is.EqualTo(false), "Свойство ShowSupplierCost не соответствует значению в базе");

				MySqlHelper.ExecuteNonQuery(
					connection,
					"update Future.Users set UseAdjustmentOrders = 1, ShowSupplierCost = 1 where Id = ?UserId",
					new MySqlParameter("?UserId", _user.Id));

				updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				helper = new UpdateHelper(updateData, connection);
				dataAdapter = new MySqlDataAdapter(helper.GetUserCommand(), connection);
				dataTable = new DataTable();
				dataAdapter.Fill(dataTable);
				Assert.That(dataTable.Rows.Count, Is.EqualTo(1), "Кол-во записей в UserInfo не равняется 1, хотя там всегда должна быть одна запись");
				Assert.That(dataTable.Rows[0]["RowId"], Is.EqualTo(_user.Id), "Столбец RowId не сопадает с Id пользователя");
				Assert.That(dataTable.Columns.Contains("UseAdjustmentOrders"), Is.EqualTo(true), "Не найден столбец UseAdjustmentOrders");
				Assert.That(Convert.ToBoolean(dataTable.Rows[0]["UseAdjustmentOrders"]), Is.EqualTo(true), "Свойство UseAdjustmentOrders не соответствует значению в базе");
				Assert.That(dataTable.Columns.Contains("ShowSupplierCost"), Is.EqualTo(true), "Не найден столбец ShowSupplierCost");
				Assert.That(Convert.ToBoolean(dataTable.Rows[0]["ShowSupplierCost"]), Is.EqualTo(true), "Свойство ShowSupplierCost не соответствует значению в базе");
			}
		}

		[Test]
		public void CheckCoreForRetailVitallyImportant()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				updateData.BuildNumber = 1405;
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				helper.Cleanup();

				helper.SelectPrices();
				helper.SelectReplicationInfo();
				helper.SelectActivePrices();

				helper.SelectOffers();

				var coreSql = helper.GetCoreCommand(false, true, false, false);

				var dataAdapter = new MySqlDataAdapter(coreSql + " limit 10", connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?Cumulative", 0);
				var coreTable = new DataTable();

				dataAdapter.Fill(coreTable);
				Assert.That(coreTable.Columns.Contains("RetailVitallyImportant"), Is.True);
				var index = coreTable.Columns.IndexOf("RetailVitallyImportant");
				Assert.That(index, Is.EqualTo(coreTable.Columns.Count-1));
			}
		}

		[Test]
		public void CheckCoreForBuyingMatrixType()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				updateData.BuyingMatrixPriceId = 4957;
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				helper.Cleanup();

				helper.SelectPrices();
				helper.SelectReplicationInfo();
				helper.SelectActivePrices();

				helper.SelectOffers();

				var coreSql = helper.GetCoreCommand(false, true, true, false);

				var dataAdapter = new MySqlDataAdapter(coreSql + " limit 10", connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?Cumulative", 0);
				var coreTable = new DataTable();

				dataAdapter.Fill(coreTable);
				Assert.That(coreTable.Columns.Contains("BuyingMatrixType"), Is.True);
				Assert.That(coreTable.Columns.Contains("RetailVitallyImportant"), Is.False);
				var index = coreTable.Columns.IndexOf("BuyingMatrixType");
				Assert.That(index, Is.EqualTo(coreTable.Columns.Count - 1));
			}
		}

		[Test]
		public void CheckCoreForBuyingMatrixTypeWithRetailVitallyImportant()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				updateData.BuyingMatrixPriceId = 4957;
				updateData.BuildNumber = 1405;
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				helper.Cleanup();

				helper.SelectPrices();
				helper.SelectReplicationInfo();
				helper.SelectActivePrices();

				helper.SelectOffers();

				var coreSql = helper.GetCoreCommand(false, true, true, false);

				var dataAdapter = new MySqlDataAdapter(coreSql + " limit 10", connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?Cumulative", 0);
				var coreTable = new DataTable();

				dataAdapter.Fill(coreTable);

				Assert.That(coreTable.Columns.Contains("RetailVitallyImportant"), Is.True);
				var indexRetail = coreTable.Columns.IndexOf("RetailVitallyImportant");

				Assert.That(coreTable.Columns.Contains("BuyingMatrixType"), Is.True);
				var indexBuying = coreTable.Columns.IndexOf("BuyingMatrixType");

				Assert.That(indexBuying, Is.EqualTo(coreTable.Columns.Count - 1));
				Assert.That(indexRetail, Is.EqualTo(indexBuying-1));
			}
		}

		[Test]
		public void CheckCoreForWhiteOfferMatrix()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				updateData.OfferMatrixPriceId = 4957;
				updateData.OfferMatrixType = 0;
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				helper.Cleanup();

				helper.SelectPrices();
				helper.SelectReplicationInfo();
				helper.SelectActivePrices();

				helper.SelectOffers();

				var existsProductId = MySqlHelper.ExecuteScalar(
					connection,
					"select ProductId from farm.BuyingMatrix where PriceId = ?PriceId limit 1",
					new MySqlParameter("?PriceId", 4957));

				var nonExistsProductId = MySqlHelper.ExecuteScalar(
					connection,
					@"
select 
	core.ProductId 
from 
	core 
	left join farm.BuyingMatrix bm on bm.ProductId = core.ProductId and bm.PriceId = ?PriceId
where 
	bm.Id is null
limit 1",
					new MySqlParameter("?PriceId", 4957));

				var coreSql = helper.GetCoreCommand(false, true, true, false);

				Assert.That(coreSql, Is.StringContaining("left join farm.BuyingMatrix offerlist on"));
				Assert.That(coreSql, Is.StringContaining("oms on oms.SupplierId = at.FirmCode and oms.ClientId ="));

				var dataAdapter = new MySqlDataAdapter(coreSql, connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?Cumulative", 0);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", _client.Id);
				var coreTable = new DataTable();

				dataAdapter.Fill(coreTable);

				Assert.That(coreTable.Columns.Contains("BuyingMatrixType"), Is.True);

				var existsOffers = coreTable.Select("ProductId = " + existsProductId);
				Assert.That(existsOffers.Length, Is.GreaterThan(0), "Все предложения по ProductId {0} должны быть в белом списке и присутствовать в доступных предложениях", existsProductId);

				var nonExistsOffers = coreTable.Select("ProductId = " + nonExistsProductId);
				Assert.That(nonExistsOffers.Length, Is.GreaterThan(0), "Предложения по ProductId {0} не существуют в белом списке и должны присутствовать в предложениях", nonExistsProductId);
				Assert.That(nonExistsOffers.All(o => o["BuyingMatrixType"].ToString() == "1"), Is.True, "Предложения по ProductId {0} должны быть недоступны к заказу", nonExistsProductId);
			}
		}

		[Test]
		public void CheckCoreForBlackOfferMatrix()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				updateData.OfferMatrixPriceId = 4957;
				updateData.OfferMatrixType = 1;
				var helper = new UpdateHelper(updateData, connection);

				helper.MaintainReplicationInfo();

				helper.Cleanup();

				helper.SelectPrices();
				helper.SelectReplicationInfo();
				helper.SelectActivePrices();

				helper.SelectOffers();

				var coreSql = helper.GetCoreCommand(false, true, true, false);

				Assert.That(coreSql, Is.StringContaining("left join farm.BuyingMatrix offerlist on"));
				Assert.That(coreSql, Is.StringContaining("oms on oms.SupplierId = at.FirmCode and oms.ClientId ="));

				var productId = MySqlHelper.ExecuteScalar(
					connection,
					"select ProductId from farm.BuyingMatrix where PriceId = ?PriceId limit 1",
					new MySqlParameter("?PriceId", 4957));

				var dataAdapter = new MySqlDataAdapter(coreSql, connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?Cumulative", 0);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", _client.Id);
				var coreTable = new DataTable();

				dataAdapter.Fill(coreTable);

				Assert.That(coreTable.Columns.Contains("BuyingMatrixType"), Is.True);

				var offers = coreTable.Select("ProductId = " + productId);
				Assert.That(offers.Length, Is.GreaterThan(0), "Предложения по ProductId {0} должны присутствовать в предложениях", productId);
				Assert.That(offers.All(o => o["BuyingMatrixType"].ToString() == "1"), Is.True, "Предложения по ProductId {0} должны быть в черном списке", productId);
			}
		}

		[Test(Description = "Настройка AllowDelayOfPayment должна экспортироваться относительно клиента")]
		public void GetAllowDelayOfPaymentByClient()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);
				var dataAdapter = new MySqlDataAdapter(helper.GetClientCommand(), connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _user.Id);

				var dataTable = new DataTable();
				dataAdapter.Fill(dataTable);
				Assert.That(dataTable.Rows.Count, Is.EqualTo(1), "Кол-во записей в Client не равняется 1, хотя там всегда должна быть одна запись");
				Assert.That(dataTable.Rows[0]["ClientId"], Is.EqualTo(_client.Id), "Столбец ClientId не сопадает с Id клиента");

				Assert.That(dataTable.Columns.Contains("AllowDelayOfPayment"), Is.False, "Столбец AllowDelayOfPayment должен экспортироваться с опеределенной версии");

				updateData.BuildNumber = 1490;
				dataAdapter.SelectCommand.CommandText = helper.GetClientCommand();

				dataTable = new DataTable();
				dataAdapter.Fill(dataTable);
				Assert.That(dataTable.Rows.Count, Is.EqualTo(1), "Кол-во записей в Client не равняется 1, хотя там всегда должна быть одна запись");
				Assert.That(dataTable.Rows[0]["ClientId"], Is.EqualTo(_client.Id), "Столбец ClientId не сопадает с Id клиента");

				Assert.That(dataTable.Columns.Contains("AllowDelayOfPayment"), Is.True, "Столбец AllowDelayOfPayment должен экспортироваться с опеределенной версии");

			}
		}

		[Test(Description = "проверка экспорта расписаний обновлений")]
		public void GetSchedulesCommand()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				MySqlHelper.ExecuteNonQuery(
					connection,
					@"
insert into UserSettings.AnalitFSchedules (ClientId, Enable, Hour, Minute) values (?ClientId, 1, 14, 3);
insert into UserSettings.AnalitFSchedules (ClientId, Enable, Hour, Minute) values (?ClientId, 0, 15, 40);
insert into UserSettings.AnalitFSchedules (ClientId, Enable, Hour, Minute) values (?ClientId, 1, 10, 50);
",
					new MySqlParameter("?clientId", _user.Client.Id));

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);
				var dataAdapter = new MySqlDataAdapter(helper.GetSchedulesCommand(), connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", _user.Client.Id);

				Assert.That(updateData.AllowAnalitFSchedule, Is.EqualTo(false));

				var dataTable = new DataTable();
				dataAdapter.Fill(dataTable);
				Assert.That(dataTable.Rows.Count, Is.EqualTo(0), "Расписаний быть не должно, т.к. механизм не включен");

				
				MySqlHelper.ExecuteNonQuery(
					connection,
					"update UserSettings.RetClientsSet set AllowAnalitFSchedule = 1 where ClientCode = ?clientId",
					new MySqlParameter("?clientId", _user.Client.Id));
				updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				helper = new UpdateHelper(updateData, connection);

				Assert.That(updateData.AllowAnalitFSchedule, Is.EqualTo(true));

				dataAdapter.SelectCommand.CommandText = helper.GetSchedulesCommand();
				dataTable = new DataTable();
				dataAdapter.Fill(dataTable);
				Assert.That(dataTable.Rows.Count, Is.EqualTo(2), "Расписаний должно быть два");
			}
		}

		[Test(Description = "проверка функции UserExists")]
		public void UserExistsTest()
		{
			var supplier = TestSupplier.Create();
			TestUser supplierUser;
			using (new SessionScope()) {
				supplierUser = supplier.Users.First();
			}

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var result = UpdateHelper.UserExists(connection, "ddsdsdsdsds");
				Assert.That(result, Is.False, "Нашли пользователя, котого не должно быть");

				result = UpdateHelper.UserExists(connection, _user.Login);
				Assert.That(result, Is.True, "Не найден пользователь {0}", _user.Login);

				result = UpdateHelper.UserExists(connection, supplierUser.Login);
				Assert.That(result, Is.True, "Не найден пользователь {0}", supplierUser.Login);
			}
		}

		[Test(Description = "проверка экспорта розничных наценок")]
		public void ExportRetailMargins()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				MySqlHelper.ExecuteNonQuery(
					connection,
					@"
insert into UserSettings.RetailMargins (ClientId, CatalogId, Markup, MaxMarkup) 
select 
	?ClientId, Id, 30, 30
from
	catalogs.Catalog
where
	hidden = 0
limit 3;
",
					new MySqlParameter("?clientId", _user.Client.Id));

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				//Проверяем каталог для предыдущей версии
				updateData.ParseBuildNumber("1.1.1.1755");
				var dataAdapter = new MySqlDataAdapter(helper.GetCatalogCommand(false, false), connection);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?UpdateTime", DateTime.Now);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?Cumulative", 0);
				dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", _user.Client.Id);

				var dataTable = new DataTable();
				dataAdapter.Fill(dataTable);
				Assert.That(dataTable.Columns.Count, Is.GreaterThan(0), "Нет колонок в таблице");
				Assert.That(dataTable.Columns.Contains("Markup"), Is.False, "Найден столбец Markup в таблице, хотя его там не должно быть");

				//Проверка каталога для версий с розничными наценками

				var updateTime = DateTime.Now;
				dataAdapter.SelectCommand.Parameters["?UpdateTime"].Value = updateTime;

				updateData.ParseBuildNumber("1.1.1.1766");
				dataAdapter.SelectCommand.CommandText = helper.GetCatalogCommand(false, true);

				dataTable = new DataTable();
				dataAdapter.Fill(dataTable);
				Assert.That(dataTable.Rows.Count, Is.GreaterThan(3), "Каталог должен быть выгружен весь");
				Assert.That(dataTable.Columns.Count, Is.GreaterThan(0), "Нет колонок в таблице");
				Assert.That(dataTable.Columns.Contains("Markup"), Is.True, "Не найден столбец Markup в таблице");
				

				MySqlHelper.ExecuteNonQuery(
					connection,
					@"
update UserSettings.RetailMargins
set
  Markup = 31
where
  ClientId = ?ClientId
limit 1;
",
					new MySqlParameter("?clientId", _user.Client.Id));

				dataAdapter.SelectCommand.CommandText = helper.GetCatalogCommand(false, false);

				dataTable = new DataTable();
				dataAdapter.Fill(dataTable);
				Assert.That(dataTable.Rows.Count, Is.EqualTo(1), "Запись должна быть одна, т.к. только одну запись мы изменили в таблице розничных наценок");
				Assert.That(dataTable.Columns.Count, Is.GreaterThan(0), "Нет колонок в таблице");
				Assert.That(dataTable.Columns.Contains("Markup"), Is.True, "Не найден столбец Markup в таблице");
			}
		}

	}
}
