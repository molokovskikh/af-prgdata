using System;
using System.Collections.Generic;
using System.Data;
using Castle.ActiveRecord;
using Common.Models.Tests.Repositories;
using Common.Tools;
using NUnit.Framework;
using MySql.Data.MySqlClient;
using PrgData.Common;
using Test.Support;


namespace Integration
{
	[TestFixture]
	public class UpdateHelperFixture
	{
		TestClient _client;
		TestUser _user;

		TestOldClient _oldClient;

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

				var session = ActiveRecordMediator.GetSessionFactoryHolder().CreateSession(typeof(ActiveRecordBase));
				try
				{
					session.CreateSQLQuery(@"
insert into usersettings.AssignedPermissions (PermissionId, UserId) values (:permissionid, :userid)")
						.SetParameter("permissionid", permission.Id)
						.SetParameter("userid", _oldClient.Users[0].Id)
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
			updateData.ShowJunkOffers = true;
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

		[Test]
		public void Check_string_field_lengts_for_old_client()
		{
			using(var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, _oldClient.Users[0].OSUserName);
				var helper = new UpdateHelper(updateData, connection, connection);
				CheckFields(updateData, helper, connection);
			}
		}

		[Test]
		public void Check_string_field_lengts_for_future_client()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection, connection);
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
				var helper = new UpdateHelper(updateData, connection, connection);
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

	}
}
