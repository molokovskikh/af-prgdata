using System;
using System.Data;
using System.Threading;
using Castle.ActiveRecord;
using Common.Tools;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using Test.Support;

namespace Integration
{
	[TestFixture]
	public class PromotionsFixture
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

		[Test(Description = "�������� ������ �����-����� ��� ������������ ����������")]
		public void GetAllPromotionsOnCumulative()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				var promotionCount = Convert.ToInt32(
					MySqlHelper.ExecuteScalar(
						connection,
						"select count(*) from usersettings.SupplierPromotions where Status = 1"));

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				var SelProc = new MySqlCommand();
				SelProc.Connection = connection;
				helper.SetUpdateParameters(SelProc, true, DateTime.Now.AddHours(-1), DateTime.Now);

				SelProc.CommandText = helper.GetPromotionsCommand();
				var dataAdapter = new MySqlDataAdapter(SelProc);
				var dataTable = new DataTable();
				dataAdapter.Fill(dataTable);

				Assert.That(dataTable.Rows.Count, Is.EqualTo(promotionCount));

				SelProc = new MySqlCommand();
				SelProc.Connection = connection;
				helper.SetUpdateParameters(SelProc, false, DateTime.Now.AddHours(-1), DateTime.Now);

				SelProc.CommandText = helper.GetPromotionsCommand();
				dataAdapter = new MySqlDataAdapter(SelProc);
				dataTable = new DataTable();
				dataAdapter.Fill(dataTable);

				Assert.That(dataTable.Rows.Count, Is.EqualTo(0));
			}
		}

		[Test(Description = "��� ���������� � �������� ����� ��� ������ ���������� �� ��������")]
		public void DeleteDisableActions()
		{
			var priceWithPromo = _user.GetActivePricesList()[0];

			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();

				//������� �����-�����
				var promoId = Convert.ToUInt32(
					MySqlHelper.ExecuteScalar(
						connection,
						@"
insert into usersettings.SupplierPromotions (Status, SupplierId, Name, Annotation, Begin, End) values (1, ?supplierId, 'test', 'test', curdate(), curdate());
select last_insert_id();",
						 new MySqlParameter("?supplierId", priceWithPromo.Price.Supplier.Id)));

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);
				var helper = new UpdateHelper(updateData, connection);

				var SelProc = new MySqlCommand();
				SelProc.Connection = connection;
				helper.SetUpdateParameters(SelProc, false, DateTime.Now.AddHours(-1), DateTime.Now);

				//��� �������������� ���������� ��� ����� ������ ���� � ������ �����
				SelProc.CommandText = helper.GetPromotionsCommand();
				var dataAdapter = new MySqlDataAdapter(SelProc);
				var dataTable = new DataTable();
				dataAdapter.Fill(dataTable);

				Assert.That(dataTable.Rows.Count, Is.GreaterThan(0));

				var promos = dataTable.Select("Id = " + promoId);
				Assert.That(promos.Length, Is.EqualTo(1), "�� ������� ������ ��� ��������� ����� {0}", promoId);
				Assert.That(promos[0]["Status"], Is.EqualTo(1), "������������ ������ ����� {0}", promoId);

				//����� ���������� ���� ���������� �� �� ������ ���� � ������ ����� �� ����������
				var updateDate = DateTime.Now;
				Thread.Sleep(1000);

				SelProc = new MySqlCommand();
				SelProc.Connection = connection;
				helper.SetUpdateParameters(SelProc, false, updateDate, DateTime.Now);

				SelProc.CommandText = helper.GetPromotionsCommand();
				dataAdapter = new MySqlDataAdapter(SelProc);
				dataTable = new DataTable();
				dataAdapter.Fill(dataTable);

				promos = dataTable.Select("Id = " + promoId);
				Assert.That(promos.Length, Is.EqualTo(0), "������� ����� {0}, ���� ��� �� ���� ��������", promoId);


				//����� ���������� ����� ��� ������ ���� � ������ ����� �� ����������
				MySqlHelper.ExecuteScalar(
					connection,
					@"update usersettings.SupplierPromotions set Status = 0 where Id = ?promoId;",
					new MySqlParameter("?promoId", promoId));

				SelProc = new MySqlCommand();
				SelProc.Connection = connection;
				helper.SetUpdateParameters(SelProc, false, updateDate, DateTime.Now);

				SelProc.CommandText = helper.GetPromotionsCommand();
				dataAdapter = new MySqlDataAdapter(SelProc);
				dataTable = new DataTable();
				dataAdapter.Fill(dataTable);

				promos = dataTable.Select("Id = " + promoId);
				Assert.That(promos.Length, Is.EqualTo(1), "�� ������� ����� {0}, ������� ���� �������� ��� �����������", promoId);
				Assert.That(promos[0]["Status"], Is.EqualTo(0), "������������ ������ ����� {0}", promoId);


				//����� ���������� ���� ���������� �� �� ������ ���� � ������ ����� �� ����������
				updateDate = DateTime.Now;
				Thread.Sleep(1000);

				SelProc = new MySqlCommand();
				SelProc.Connection = connection;
				helper.SetUpdateParameters(SelProc, false, updateDate, DateTime.Now);

				SelProc.CommandText = helper.GetPromotionsCommand();
				dataAdapter = new MySqlDataAdapter(SelProc);
				dataTable = new DataTable();
				dataAdapter.Fill(dataTable);

				promos = dataTable.Select("Id = " + promoId);
				Assert.That(promos.Length, Is.EqualTo(0), "������� ����� {0}, ���� ��� �� ���� ��������", promoId);

				//����� �������� ����� ��� ������ ���� � ������ ����� �� ����������
				MySqlHelper.ExecuteScalar(
					connection,
					@"delete from usersettings.SupplierPromotions where Id = ?promoId;",
					new MySqlParameter("?promoId", promoId));

				SelProc = new MySqlCommand();
				SelProc.Connection = connection;
				helper.SetUpdateParameters(SelProc, false, updateDate, DateTime.Now);

				SelProc.CommandText = helper.GetPromotionsCommand();
				dataAdapter = new MySqlDataAdapter(SelProc);
				dataTable = new DataTable();
				dataAdapter.Fill(dataTable);

				promos = dataTable.Select("Id = " + promoId);
				Assert.That(promos.Length, Is.EqualTo(1), "�� ������� ����� {0}, ������� ���� �������", promoId);
				Assert.That(promos[0]["Status"], Is.EqualTo(0), "������������ ������ ����� {0}", promoId);
			}
		}
	}
}