using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Castle.ActiveRecord;
using Common.MySql;
using Common.Tools;
using NUnit.Framework;
using MySql.Data.MySqlClient;
using PrgData.Common;
using System.Data;
using Test.Support;
using MySqlHelper = MySql.Data.MySqlClient.MySqlHelper;

namespace Integration
{
	[TestFixture]
	public class OrderHelperFixture
	{
		private TestClient _client;
		private TestUser _user;

		[SetUp]
		public void SetUp()
		{
			_client = TestClient.Create();

			using (var transaction = new TransactionScope()) {
				_user = _client.Users[0];

				_client.Users.Each(u => {
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				_user.Update();
			}
		}

		[Test(Description = "проверяем, что маска регионов загружается из Customers.Users")]
		public void check_OrderRegions_for_future_client()
		{
			var userName = _user.Login;

			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();
				var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted);
				try {
					var updateData = UpdateHelper.GetUpdateData(connection, userName);

					var command = new MySqlCommand(@"
update usersettings.RetClientsSet set OrderRegionMask = 2 where ClientCode = ?clientId ;
update Customers.Users set OrderRegionMask = 3 where Id = ?userId ;", connection, trans);
					command.Parameters.AddWithValue("?userId", updateData.UserId);
					command.Parameters.AddWithValue("?clientId", updateData.ClientId);
					command.ExecuteNonQuery();

					var updateHelper = new UpdateHelper(updateData, connection);

					var clients = MySqlHelper.ExecuteDataset(
						connection,
						updateHelper.GetClientsCommand(),
						new MySqlParameter("?OffersRegionCode", updateData.OffersRegionCode),
						new MySqlParameter("?UserId", updateData.UserId));

					Assert.AreEqual(2, clients.Tables[0].Rows[0]["OrderRegionMask"], "Не выбрали регион из Users");
				}
				finally {
					trans.Rollback();
				}
			}
		}
	}
}
