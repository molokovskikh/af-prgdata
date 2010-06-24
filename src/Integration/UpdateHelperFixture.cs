using System;
using System.Collections.Generic;
using System.Data;
using NUnit.Framework;
using MySql.Data.MySqlClient;
using PrgData.Common;


namespace Integration
{
	[TestFixture]
	public class UpdateHelperFixture
	{
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
				var updateData = UpdateHelper.GetUpdateData(connection, "sergei");
				var helper = new UpdateHelper(updateData, connection, connection);
				CheckFields(updateData, helper, connection);
			}
		}

		[Test]
		public void Check_string_field_lengts_for_future_client()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				var updateData = UpdateHelper.GetUpdateData(connection, "10081");
				var helper = new UpdateHelper(updateData, connection, connection);
				CheckFields(updateData, helper, connection);
			}
		}


	}
}
