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
using PrgData.Common.AnalitFVersions;
using PrgData.Common.Counters;
using Test.Support;
using Test.Support.Catalog;
using Test.Support.Suppliers;


namespace Integration
{
	[TestFixture]
	public class UpdateHelperFixture
	{
		TestClient _client;
		TestUser _user;

		Lazy<UpdateData> lazyUpdateData;
		UpdateData updateDataValue;
		UpdateData updateData
		{
			get { return updateDataValue ?? lazyUpdateData.Value; }
			set { updateDataValue = value; }
		}

		Lazy<UpdateHelper> lazyHelper;
		UpdateHelper helperValue;
		UpdateHelper helper
		{
			get { return helperValue ?? lazyHelper.Value; }
			set { helperValue = value; }
		}

		MySqlConnection connection;

		[SetUp]
		public void SetUp()
		{
			_client = TestClient.Create();
			using (new TransactionScope()) {
				_user = _client.Users[0];

				_client.Users.Each(u =>
				{
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				_user.Update();
			}

			connection = new MySqlConnection(Settings.ConnectionString());
			connection.Open();
			//тк тести может иметь инициализацию, нам нужно загружать данные только
			//после этой инициализации
			helperValue = null;
			lazyUpdateData = new Lazy<UpdateData>(() => {
				var data = UpdateHelper.GetUpdateData(connection, _user.Login);
				data.OldUpdateTime = DateTime.Now.AddHours(-1);
				return data;
			});
			updateDataValue= null;
			lazyHelper = new Lazy<UpdateHelper>(() => new UpdateHelper(updateData, connection));
		}

		[TearDown]
		public void TearDown()
		{
			connection.Clone();
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
			CheckFields(updateData, helper, connection);
		}

		[Test(Description = "Проверка поля Clients.ShortName для клиентов из новой реальности для версий программы больше 1271 или обновляющихся на нее")]
		public void Check_Clients_field_lengts_for_future_client_with_version_greater_than_1271()
		{

			updateData.BuildNumber = 1272;

			var firebirdSQL = helper.GetClientsCommand(true);
			var nonFirebirdSQL = helper.GetClientsCommand(false);

			Assert.That(firebirdSQL, Is.EqualTo(nonFirebirdSQL), "Два SQL-запроса по содержанию не равны");

			CheckFieldLength(
				connection,
				firebirdSQL,
				updateData,
				new[]
					{
						new KeyValuePair<string, int>("ShortName", 255)
					});

			CheckFieldLength(
				connection,
				nonFirebirdSQL,
				updateData,
				new[]
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
				new[]
					{
						new KeyValuePair<string, int>("ShortName", 255)
					});
		}


		[Test(Description = "Получаем данные для пользователя, которому не назначен ни один адрес доставки")]
		public void Get_UserInfo_without_addresses()
		{
			using (new TransactionScope()) {
				_user.AvaliableAddresses.Clear();
				_user.SendRejects = true;
				_user.SendWaybills = true;
				_user.Update();
			}

			var dataAdapter = new MySqlDataAdapter(helper.GetUserCommand(), connection);
			var dataTable = new DataTable();
			dataAdapter.Fill(dataTable);
			Assert.That(dataTable.Rows.Count, Is.EqualTo(1), "Кол-во записей в UserInfo не равняется 1, хотя там всегда должна быть одна запись");
			Assert.That(dataTable.Rows[0]["ClientCode"], Is.EqualTo(DBNull.Value), "Столбец ClientCode не содержит значение DBNull, хотя должен, т.к. адреса к пользователю не привязаны");
			Assert.That(dataTable.Rows[0]["RowId"], Is.EqualTo(_user.Id), "Столбец RowId не сопадает с Id пользователя");

			dataAdapter.SelectCommand.CommandText = helper.GetClientCommand();
			dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _user.Id);
			dataTable = new DataTable();
			dataAdapter.Fill(dataTable);
			Assert.That(dataTable.Rows.Count, Is.EqualTo(1), "Кол-во записей в Client не равняется 1, хотя там всегда должна быть одна запись");
			Assert.That(dataTable.Rows[0]["ClientId"], Is.EqualTo(_client.Id), "Столбец ClientId не сопадает с Id клиента");
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
			var states = new List<CheckedState> {
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
			if (!String.IsNullOrEmpty(row["SelfAddressId"].ToString()))
				Assert.That(row["FirmCode"].ToString(), Is.Not.EqualTo(row["SelfAddressId"].ToString()));
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
			if (!String.IsNullOrEmpty(row["SelfAddressId"].ToString()))
				Assert.That(row["FirmCode"].ToString(), Is.Not.EqualTo(row["SelfAddressId"].ToString()));
		}

		[Test(Description = "Проверяем установку поля SelfAddressId в зависимости от значений параметра NetworkPriceId")]
		public void Check_SelfAddressId_by_NetworkPriceId()
		{
			//Установили прайс поставщика Инфорум
			updateData.NetworkPriceId = 2647;

			var dataAdapter = new MySqlDataAdapter(helper.GetClientsCommand(false), connection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _user.Id);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?OffersRegionCode", updateData.OffersRegionCode);

			var clients = new DataTable();
			dataAdapter.Fill(clients);

			var row = clients.Rows[0];
			if (!String.IsNullOrEmpty(row["SelfAddressId"].ToString()))
				Assert.That(row["FirmCode"].ToString(), Is.Not.EqualTo(row["SelfAddressId"].ToString()));

			//Установили параметр в null
			updateData.NetworkPriceId = null;
			dataAdapter.SelectCommand.CommandText = helper.GetClientsCommand(false);
			clients = new DataTable();
			dataAdapter.Fill(clients);
			Assert.That(clients.Columns.Contains("SelfAddressId"), Is.True);
			row = clients.Rows[0];
			Assert.That(row["SelfAddressId"], Is.EqualTo(DBNull.Value));
		}

		[Test(Description = "Проверяем установку поля SelfAddressId в зависимости от значений параметра NetworkPriceId для клиентов с несколькими юридическими лицами")]
		public void Check_SelfAddressId_for_future_client_with_version_greater_than_1271_and_same_LegalEntities()
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
			if (!String.IsNullOrEmpty(row["SelfAddressId"].ToString()))
				Assert.That(row["FirmCode"].ToString(), Is.Not.EqualTo(row["SelfAddressId"].ToString()));

			//Установили параметр в null
			updateData.NetworkPriceId = null;
			dataAdapter.SelectCommand.CommandText = helper.GetClientsCommand(false);
			clients = new DataTable();
			dataAdapter.Fill(clients);
			Assert.That(clients.Columns.Contains("SelfAddressId"), Is.True);
			row = clients.Rows[0];
			Assert.That(row["SelfAddressId"], Is.EqualTo(DBNull.Value));
		}

		[Test(Description = "Это тест для проверки чтение с помощью коннектора, надо перенести в другое место")]
		public void TestCallCount()
		{
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
Customers.Users u
	join Customers.Clients c on u.ClientId = c.Id
	join Customers.UserAddresses ua on ua.UserId = u.Id
	join Customers.Addresses a on c.Id = a.ClientId and ua.AddressId = a.Id
	join billing.LegalEntities le on le.Id = a.LegalEntityId
WHERE 
	u.Id = ?UserId
and a.Enabled = 1",
				new MySqlParameter("?UserId", _user.Id));

			Assert.That(count, Is.Not.Null);
		}

		public static DataTable CompareTwoDataTable(DataTable dt1, DataTable dt2)
		{
			dt1.Merge(dt2);
			var d3 = dt2.GetChanges();
			return d3;
		}

		[Test]
		public void Check_core_count_with_GroupBy()
		{
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

		[Test(Description = "Все актуальные прайс-листы при первом подключении к клиенту должны быть свежими")]
		public void CheckActivePricesFreshAfterCreate()
		{

			helper.MaintainReplicationInfo();

			var SelProc = new MySqlCommand();
			SelProc.Connection = connection;

			helper.SetUpdateParameters(SelProc, DateTime.Now);

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

		[Test(Description = "Все прайс-листы должны быть несвежими после подтверждения обновления")]
		public void CheckActivePricesFreshAfterConfirm()
		{
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

			helper.SetUpdateParameters(SelProc, DateTime.Now);

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

		[Test(Description = "После отключения прайс-листа клиентом, он должен быть помечен как свежий")]
		public void CheckActivePricesFreshAfterDeletePrice()
		{
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
Customers.UserPrices
where
PriceId = ?PriceId
and RegionId = ?RegionId
and UserId = ?UserId;",
				new MySqlParameter("?UserId", _user.Id),
				new MySqlParameter("?PriceId", disabledPrice["PriceCode"]),
				new MySqlParameter("?RegionId", disabledPrice["RegionCode"]));


			var SelProc = new MySqlCommand();
			SelProc.Connection = connection;

			helper.SetUpdateParameters(SelProc, DateTime.Now);

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

		[Test(Description = "После потери актуальности прайс-листа, он должен быть помечен как свежий")]
		public void CheckActivePricesFreshAfterUnactual()
		{
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

			helper.SetUpdateParameters(SelProc, DateTime.Now);

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

		[Test(Description = "При кумулятивном обновлении все прайс-листы должны быть свежими")]
		public void CheckActivePricesFreshOnCumulative()
		{
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

			updateData.Cumulative = true;
			helper.SetUpdateParameters(SelProc, DateTime.Now);

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

		[Test(Description = "проверяем работу метода UpdateBuildNumber")]
		public void TestUpdateBuildNumber()
		{
			MySqlHelper.ExecuteNonQuery(
				connection,
				"update usersettings.UserUpdateInfo set AFAppVersion = 0 where UserId = ?UserId",
				new MySqlParameter("?UserId", _user.Id));


			Assert.That(updateData.KnownBuildNumber, Is.EqualTo(0));

			const int changedBuildNumber = 1300;
			updateData.BuildNumber = changedBuildNumber;
			UpdateHelper.UpdateBuildNumber(connection, updateData);

			var changedUpdateData = UpdateHelper.GetUpdateData(connection, _user.Login);

			Assert.That(changedUpdateData.KnownBuildNumber, Is.EqualTo(changedBuildNumber), "Не сохранился номер версии AnalitF");
		}

		[Test(Description = "Проверяем доступность столбцов UseAdjustmentOrders, ShowSupplierCost")]
		public void CheckChangeUseAdjustmentOrders()
		{
			MySqlHelper.ExecuteNonQuery(
				connection,
				"update Customers.Users set UseAdjustmentOrders = 0, ShowSupplierCost = 0 where Id = ?UserId",
				new MySqlParameter("?UserId", _user.Id));

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
				"update Customers.Users set UseAdjustmentOrders = 1, ShowSupplierCost = 1 where Id = ?UserId",
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

		[Test]
		public void CheckCoreForRetailVitallyImportant()
		{
			updateData.BuildNumber = 1405;
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

		[Test]
		public void CheckCoreForBuyingMatrixType()
		{
			updateData.BuyingMatrixPriceId = 4957;

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

		[Test]
		public void CheckCoreForBuyingMatrixTypeWithRetailVitallyImportant()
		{
			updateData.BuyingMatrixPriceId = 4957;
			updateData.BuildNumber = 1405;

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

		[Test]
		public void CheckCoreForWhiteOfferMatrix()
		{
			updateData.OfferMatrixPriceId = 4957;
			updateData.OfferMatrixType = 0;

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

		[Test]
		public void CheckCoreForBlackOfferMatrix()
		{
			updateData.OfferMatrixPriceId = 4957;
			updateData.OfferMatrixType = 1;

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

		[Test(Description = "Настройка AllowDelayOfPayment должна экспортироваться относительно клиента")]
		public void GetAllowDelayOfPaymentByClient()
		{
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

		[Test(Description = "проверка экспорта расписаний обновлений")]
		public void GetSchedulesCommand()
		{
			MySqlHelper.ExecuteNonQuery(
				connection,
				@"
insert into UserSettings.AnalitFSchedules (ClientId, Enable, Hour, Minute) values (?ClientId, 1, 14, 3);
insert into UserSettings.AnalitFSchedules (ClientId, Enable, Hour, Minute) values (?ClientId, 0, 15, 40);
insert into UserSettings.AnalitFSchedules (ClientId, Enable, Hour, Minute) values (?ClientId, 1, 10, 50);
",
				new MySqlParameter("?clientId", _user.Client.Id));

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

			//Проверяем каталог для предыдущей версии
			updateData.ParseBuildNumber("1.1.1.1755");
			Assert.That(updateData.NeedUpdateForRetailMargins(), Is.False);
			var dataAdapter = new MySqlDataAdapter(helper.GetCatalogCommand(false), connection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?UpdateTime", DateTime.Now);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?Cumulative", 0);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", _user.Client.Id);

			var dataTable = new DataTable();
			dataAdapter.Fill(dataTable);
			Assert.That(dataTable.Columns.Count, Is.GreaterThan(0), "Нет колонок в таблице");
			Assert.That(dataTable.Columns.Contains("Markup"), Is.False, "Найден столбец Markup в таблице, хотя его там не должно быть");


			//Проверяем экспорт каталога при обновлении версий
			updateData.UpdateExeVersionInfo = new VersionInfo(1791);
			Assert.That(updateData.NeedUpdateForRetailMargins(), Is.True);
			dataAdapter.SelectCommand.CommandText = helper.GetCatalogCommand(false);

			dataTable = new DataTable();
			dataAdapter.Fill(dataTable);
			Assert.That(dataTable.Rows.Count, Is.EqualTo(3), "Каталог должен быть выгружен весь");
			Assert.That(dataTable.Columns.Count, Is.GreaterThan(0), "Нет колонок в таблице");
			Assert.That(dataTable.Columns.Contains("Markup"), Is.True, "Не найден столбец Markup в таблице");


			//Проверка каталога для версий с розничными наценками

			var updateTime = DateTime.Now;
			dataAdapter.SelectCommand.Parameters["?UpdateTime"].Value = updateTime;

			updateData.ParseBuildNumber("1.1.1.1766");
			updateData.UpdateExeVersionInfo = null;
			updateData.Cumulative = true;
			dataAdapter.SelectCommand.CommandText = helper.GetCatalogCommand(false);

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

			updateTime = Convert.ToDateTime(MySqlHelper.ExecuteScalar(
				connection,
				"select max(UpdateTime) from UserSettings.RetailMargins where ClientId = ?ClientId",
				new MySqlParameter("?clientId", _user.Client.Id))).AddSeconds(-1);
			dataAdapter.SelectCommand.Parameters["?UpdateTime"].Value = updateTime;
			updateData.Cumulative = false;
			dataAdapter.SelectCommand.CommandText = helper.GetCatalogCommand(false);

			dataTable = new DataTable();
			dataAdapter.Fill(dataTable);
			Assert.That(dataTable.Rows.Count, Is.EqualTo(1), "Запись должна быть одна, т.к. только одну запись мы изменили в таблице розничных наценок");
			Assert.That(dataTable.Columns.Count, Is.GreaterThan(0), "Нет колонок в таблице");
			Assert.That(dataTable.Columns.Contains("Markup"), Is.True, "Не найден столбец Markup в таблице");
		}
		
		private void CheckDescriptionIdColumn(MySqlDataAdapter adapter, uint catalogId, uint? descritionId)
		{
			var dataTable = new DataTable();
			adapter.Fill(dataTable);
			Assert.That(dataTable.Columns.Count, Is.GreaterThan(0), "Нет колонок в таблице");
			Assert.That(dataTable.Columns.Contains("DescriptionId"), Is.True, "Не найден столбец DescriptionId в таблице, хотя он там должен быть");
			var rows = dataTable.Select("Id = " + catalogId);
			Assert.That(rows.Length, Is.EqualTo(1), "Не найдена строка с каталожным продуктом Id:{0}", catalogId);
			var row = rows[0];
			if (descritionId.HasValue)
				Assert.That(row["DescriptionId"], Is.EqualTo(descritionId), "Не совпадает description для продукта Id:{0}", catalogId);
			else
				Assert.That(row["DescriptionId"], Is.EqualTo(DBNull.Value), "Не совпадает description для продукта Id:{0}", catalogId);
		}

		private void ExportDescriptionIdBy(bool before1150, string version)
		{
			var updateTime = DateTime.Now.AddSeconds(-1);

			TestCatalogProduct catalogProduct;
			TestDescription description;

			using (new TransactionScope()) {
				var product = new TestProduct("тестовый продукт для пользователя " + _user.Id);
				product.Save();
				catalogProduct = product.CatalogProduct;

				description = new TestDescription("тестовое описание для пользователя " + _user.Id, "english description for user " + _user.Id);
				description.Save();
			}

			//Устанавливаем номер версии, если передан параметр
			if (!string.IsNullOrEmpty(version))
				updateData.ParseBuildNumber(version);

			//Проверяем обычный запрос данных при неустановленном описании
			var dataAdapter = new MySqlDataAdapter(helper.GetCatalogCommand(before1150), connection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?UpdateTime", updateTime);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?Cumulative", 0);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?ClientCode", _user.Client.Id);

			CheckDescriptionIdColumn(dataAdapter, catalogProduct.Id, null);
				

			//Проверяем кумулятивный запрос данных при неустановленном описании
			updateData.Cumulative = true;
			dataAdapter.SelectCommand.CommandText = helper.GetCatalogCommand(before1150);
			dataAdapter.SelectCommand.Parameters["?Cumulative"].Value = 1;
			CheckDescriptionIdColumn(dataAdapter, catalogProduct.Id, null);

			//добавляем описание к каталогу
			using (new TransactionScope()) {
				catalogProduct.CatalogName.Description = description;
				catalogProduct.Save();
			}

			updateTime = Convert.ToDateTime(MySqlHelper.ExecuteScalar(
				connection,
				"select UpdateTime from catalogs.CatalogNames where Id = ?Id",
				new MySqlParameter("?Id", catalogProduct.CatalogName.Id))).AddSeconds(-1);
			dataAdapter.SelectCommand.Parameters["?UpdateTime"].Value = updateTime;

			//Проверяем обычный запрос данных при установленном неопубликованном описании
			updateData.Cumulative = false;
			dataAdapter.SelectCommand.CommandText = helper.GetCatalogCommand(before1150);
			dataAdapter.SelectCommand.Parameters["?Cumulative"].Value = 0;
			CheckDescriptionIdColumn(dataAdapter, catalogProduct.Id, null);

			//Проверяем кумулятивный запрос данных при установленном неопубликованном описании
			updateData.Cumulative = true;
			dataAdapter.SelectCommand.CommandText = helper.GetCatalogCommand(before1150);
			dataAdapter.SelectCommand.Parameters["?Cumulative"].Value = 1;
			CheckDescriptionIdColumn(dataAdapter, catalogProduct.Id, null);

			//публикуем описание
			using (new TransactionScope()) {
				description.NeedCorrect = false;
				description.Save();
			}

			//updateTime = description.UpdateTime.AddSeconds(-1);
			updateTime = Convert.ToDateTime(MySqlHelper.ExecuteScalar(
				connection,
				"select UpdateTime from catalogs.Descriptions where Id = ?Id",
				new MySqlParameter("?Id", description.Id))).AddSeconds(-1);
			dataAdapter.SelectCommand.Parameters["?UpdateTime"].Value = updateTime;

			//Проверяем обычный запрос данных при установленном опубликованном описании
			updateData.Cumulative = false;
			dataAdapter.SelectCommand.CommandText = helper.GetCatalogCommand(before1150);
			dataAdapter.SelectCommand.Parameters["?Cumulative"].Value = 0;
			CheckDescriptionIdColumn(dataAdapter, catalogProduct.Id, description.Id);

			//Проверяем кумулятивный запрос данных при установленном опубликованном описании
			updateData.Cumulative = true;
			dataAdapter.SelectCommand.CommandText = helper.GetCatalogCommand(before1150);
			dataAdapter.SelectCommand.Parameters["?Cumulative"].Value = 1;
			CheckDescriptionIdColumn(dataAdapter, catalogProduct.Id, description.Id);
		}

		[Test(Description = "проверка экспорта ссылки на описание для версий раньше 1150")]
		public void ExportDescriptionIdByBefore1150()
		{
			 ExportDescriptionIdBy(true, null);
		}

		[Test(Description = "проверка экспорта ссылки на описание для версий до 1755")]
		public void ExportDescriptionIdBeforeRetailMargins()
		{
			 ExportDescriptionIdBy(false, "1.1.1.1755");
		}

		[Test(Description = "проверка экспорта ссылки на описание для версий после 1755")]
		public void ExportDescriptionIdWithRetailMargins()
		{
			 ExportDescriptionIdBy(false, "1.1.1.1766");
		}

		[Test(Description = "Проверяем установку поля ExcessAvgOrderTimes при экспорте для различных версий")]
		public void Check_ExcessAvgOrderTimes_for_client_with_version_greater_than_1791()
		{
			updateData.BuildNumber = 1272;

			var dataAdapter = new MySqlDataAdapter(helper.GetClientsCommand(false), connection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _user.Id);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?OffersRegionCode", updateData.OffersRegionCode);

			var clients = new DataTable();
			dataAdapter.Fill(clients);

			Assert.That(clients.Columns.Contains("ExcessAvgOrderTimes"), Is.False, "При обновлении старой версии столбец ExcessAvgOrderTimes не должен присутствовать");

			//установили версию больше, чем 1800
			updateData.BuildNumber = 1801;

			dataAdapter.SelectCommand.CommandText = helper.GetClientsCommand(false);
			clients = new DataTable();
			dataAdapter.Fill(clients);
			Assert.That(clients.Columns.Contains("ExcessAvgOrderTimes"), Is.True, "Отсутствует столбец ExcessAvgOrderTimes");

			var row = clients.Rows[0];
			Assert.That(row["ExcessAvgOrderTimes"], Is.EqualTo(5), "Неожидаемое значение по умолчанию для столбца ExcessAvgOrderTimes");
		}

		private void CheckContactInfo(MySqlDataAdapter adapter, string sql, TestSupplier supplier, TestSupplierRegionalData supplierRegionalData, string address, string contactInfo, object expectedValue)
		{
			using (new TransactionScope()) {
				supplierRegionalData.ContactInfo = contactInfo;
				supplier.Address = address;
				supplier.SaveAndFlush();
			}

			adapter.SelectCommand.CommandText = sql;
			var regionalData = new DataTable();
			adapter.Fill(regionalData);

			var infos = regionalData.Select("FirmCode = " + supplier.Id + " and RegionCode = " + supplier.HomeRegion.Id);
			Assert.That(infos.Length, Is.EqualTo(1), "Должна быть одна запись для поставщика {0}", supplier.Id);
			Assert.That(infos[0]["ContactInfo"], Is.EqualTo(expectedValue), "Неожидаемое значение поля ContactInfo для поставщика {0}", supplier.Id);
		}

		[Test(Description = "проверяем работу метода GetRegionalDataCommand")]
		public void CheckGetRegionDataCommand()
		{
			var supplier = TestSupplier.Create();

			var regionalData = supplier.RegionalData[0];

			helper.MaintainReplicationInfo();

			var dataAdapter = new MySqlDataAdapter("select now()", connection);

			var command = dataAdapter.SelectCommand;

			helper.SetUpdateParameters(command, DateTime.Now);

			helper.Cleanup();

			helper.SelectPrices();
			helper.PreparePricesData(command);
			helper.SelectReplicationInfo();
			helper.SelectActivePrices();


			CheckContactInfo(dataAdapter, helper.GetRegionalDataCommand(), supplier, regionalData, null, string.Empty, string.Empty);

			CheckContactInfo(dataAdapter, helper.GetRegionalDataCommand(), supplier, regionalData, string.Empty, string.Empty, string.Empty);

			CheckContactInfo(dataAdapter, helper.GetRegionalDataCommand(), supplier, regionalData, "test", string.Empty, "test\r\n");

			CheckContactInfo(dataAdapter, helper.GetRegionalDataCommand(), supplier, regionalData, "test", "contact test info", "test\r\ncontact test info");

			CheckContactInfo(dataAdapter, helper.GetRegionalDataCommand(), supplier, regionalData, null, "contact test info", "contact test info");

			CheckContactInfo(dataAdapter, helper.GetRegionalDataCommand(), supplier, regionalData, string.Empty, "contact test info", "contact test info");
		}

		[Test(Description = "проверка выгрузки контактной региональной информации относительно клиента")]
		public void GetRegionTechContactOnClient()
		{
			var dataAdapter = new MySqlDataAdapter(helper.GetClientCommand(), connection);
			dataAdapter.SelectCommand.Parameters.AddWithValue("?UserId", _user.Id);

			var dataTable = new DataTable();
			dataAdapter.Fill(dataTable);
			Assert.That(dataTable.Rows.Count, Is.EqualTo(1), "Кол-во записей в Client не равняется 1, хотя там всегда должна быть одна запись");
			Assert.That(dataTable.Rows[0]["ClientId"], Is.EqualTo(_client.Id), "Столбец ClientId не сопадает с Id клиента");

			Assert.That(dataTable.Columns.Contains("HomeRegion"), Is.False, "Столбец HomeRegion должен экспортироваться с опеределенной версии");
			Assert.That(dataTable.Columns.Contains("TechContact"), Is.False, "Столбец TechContact должен экспортироваться с опеределенной версии");
			Assert.That(dataTable.Columns.Contains("OperatingMode"), Is.False, "Столбец OperatingMode должен экспортироваться с опеределенной версии");

			updateData.BuildNumber = 1840;
			dataAdapter.SelectCommand.CommandText = helper.GetClientCommand();

			dataTable = new DataTable();
			dataAdapter.Fill(dataTable);
			Assert.That(dataTable.Rows.Count, Is.EqualTo(1), "Кол-во записей в Client не равняется 1, хотя там всегда должна быть одна запись");
			Assert.That(dataTable.Rows[0]["ClientId"], Is.EqualTo(_client.Id), "Столбец ClientId не сопадает с Id клиента");

			Assert.That(dataTable.Columns.Contains("HomeRegion"), Is.True, "Столбец HomeRegion должен экспортироваться с опеределенной версии");
			Assert.That(dataTable.Columns.Contains("TechContact"), Is.True, "Столбец TechContact должен экспортироваться с опеределенной версии");
			Assert.That(dataTable.Columns.Contains("OperatingMode"), Is.True, "Столбец OperatingMode должен экспортироваться с опеределенной версии");
		}
	}
}
