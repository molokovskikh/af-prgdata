using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using MySql.Data.MySqlClient;
using PrgData.Common;

namespace Integration
{
	[TestFixture]
	public class OrderHelperFixture
	{

		[Test(Description = "отправляем заказы под старым клиентом и проверяем время прайс-листа")]
		public void send_order_by_old_client()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				//Пользователь "sergei" - это клиент с кодом 1349, он должен быть старым
				var updateData = UpdateHelper.GetUpdateData(connection, "sergei");
				var helper = new OrderHelper(updateData, connection, connection);
				MySqlHelper.ExecuteNonQuery(connection, "drop temporary table if exists Usersettings.Prices");
				MySqlHelper.ExecuteNonQuery(connection, "drop temporary table if exists Usersettings.ActivePrices");
				MySqlHelper.ExecuteNonQuery(connection, "call usersettings.GetActivePrices(" + updateData.ClientId + ")");
				var dsPrice = MySqlHelper.ExecuteDataset(connection, "select * from usersettings.ActivePrices limit 1");
				var sendPrice = dsPrice.Tables[0].Rows[0];
				var orderid = helper.SaveOrder(
					updateData.ClientId, 
					Convert.ToUInt32(sendPrice["PriceCode"]), 
					Convert.ToUInt64(sendPrice["RegionCode"]), 
					Convert.ToDateTime(sendPrice["PriceDate"]).ToUniversalTime(), 
					1, 
					1, 
					null);
				Assert.That(orderid > 0, "не получилось сохранить заказ");
				try
				{
					var dsOrder = MySqlHelper.ExecuteDataset(connection, "select * from orders.OrdersHead where RowId = " + orderid);
					var drOrder = dsOrder.Tables[0].Rows[0];
					Assert.That(drOrder["PriceDate"], Is.EqualTo(Convert.ToDateTime(sendPrice["PriceDate"])), "не совпадает дата прайс-листа в заказе с датой прайс-листа");
				}
				finally
				{
					MySqlHelper.ExecuteNonQuery(connection, "delete from orders.OrdersHead where RowId = " + orderid);
				}
			}
		}

		[Test(Description = "отправляем заказы под клиентом из 'новой реальности' и проверяем время прайс-листа")]
		public void send_order_by_future_client()
		{
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				connection.Open();
				//Пользователь "10081" - это пользователь, привязанный к клиенту с кодом 10005, который должен быть клиентом из "Новой реальности"
				var updateData = UpdateHelper.GetUpdateData(connection, "10081");
				var helper = new OrderHelper(updateData, connection, connection);
				MySqlHelper.ExecuteNonQuery(connection, "drop temporary table if exists Usersettings.Prices");
				MySqlHelper.ExecuteNonQuery(connection, "drop temporary table if exists Usersettings.ActivePrices");
				MySqlHelper.ExecuteNonQuery(connection, "call future.GetActivePrices(" + updateData.ClientId + ")");
				var dsPrice = MySqlHelper.ExecuteDataset(connection, "select * from usersettings.ActivePrices limit 1");
				var sendPrice = dsPrice.Tables[0].Rows[0];
				var orderid = helper.SaveOrder(
					updateData.ClientId,
					Convert.ToUInt32(sendPrice["PriceCode"]),
					Convert.ToUInt64(sendPrice["RegionCode"]),
					Convert.ToDateTime(sendPrice["PriceDate"]).ToUniversalTime(),
					1,
					1,
					null);
				Assert.That(orderid > 0, "не получилось сохранить заказ");
				try
				{
					var dsOrder = MySqlHelper.ExecuteDataset(connection, "select * from orders.OrdersHead where RowId = " + orderid);
					var drOrder = dsOrder.Tables[0].Rows[0];
					Assert.That(drOrder["PriceDate"], Is.EqualTo(Convert.ToDateTime(sendPrice["PriceDate"])), "не совпадает дата прайс-листа в заказе с датой прайс-листа");
				}
				finally
				{
					MySqlHelper.ExecuteNonQuery(connection, "delete from orders.OrdersHead where RowId = " + orderid);
				}
			}
		}
	}
}
