using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Tests.Repositories;
using Inforoom.Common;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData;
using System.Data;
using PrgData.Common;
using SmartOrderFactory.Domain;
using SmartOrderFactory.Repositories;


namespace Integration
{
	[TestFixture]
	public class PostSomeOrdersFixture
	{
		[Test]
		public void Send_orders_without_SupplierPriceMarkup()		
		{
			Test.Support.Setup.Initialize();
			ContainerInitializer.InitializerContainerForTests(typeof(SmartOrderRule).Assembly);
			IoC.Container.Register(
				Component.For<ISmartOfferRepository>().ImplementedBy<SmartOfferRepository>()
				);

			ServiceContext.GetUserHost = () => "127.0.0.1";
			ServiceContext.GetUserName = () => "sergei";
			ConfigurationManager.AppSettings["WaybillPath"] = "FtpRoot\\";
			if (Directory.Exists("FtpRoot"))
				FileHelper.DeleteDir("FtpRoot");
			Directory.CreateDirectory("FtpRoot");
			CreateFolders("1349");


			using(var connection = new MySqlConnection(PrgData.Common.Settings.ConnectionString()))
			{
				connection.Open();

				var command = new MySqlCommand();
				command.Connection = connection;

				var UniqueId = MySqlHelper.ExecuteScalar(connection, "select AFCopyId from usersettings.UserUpdateInfo where UserId = " + 1289).ToString();

				command.CommandText = "call usersettings.GetOffers(1349, 0);";
				command.ExecuteNonQuery();

				command.CommandText = @"
select
  *
from
  ActivePrices ap,
  Core c,
  farm.Core0 fc
where
    c.PriceCode = ap.PriceCode
and c.RegionCode = ap.RegionCode
and fc.Id = c.Id
and fc.ProductId = c.ProductId
limit 10";

				var dataAdapter = new MySqlDataAdapter(command);
				var positions = new DataTable();
				dataAdapter.Fill(positions);
				var position = positions.Rows[0];

				string serverResponse;
				using (var prgData = new PrgDataEx())
				{
					serverResponse = prgData.PostSomeOrders(
						UniqueId,
						true,
						false,
						1349u,
						1,
						new ulong[] { 1L },
						new ulong[] { Convert.ToUInt64(position["PriceCode"]) },
						new ulong[] { Convert.ToUInt64(position["RegionCode"]) },
						new DateTime[] { Convert.ToDateTime(position["PriceDate"]) },
						new string[] { "" },
						new ushort[] { 1 },
						new ulong[] { 1 },
						new ulong[] { Convert.ToUInt64(position["Id"]) },
						new ulong[] { Convert.ToUInt64(position["ProductId"]) },
						new string[] { position["CodeFirmCr"].ToString() },
						new ulong[] { Convert.ToUInt64(position["SynonymCode"]) },
						new string[] { position["SynonymFirmCrCode"].ToString() },
						new string[] { position["Code"].ToString() },
						new string[] { position["Code"].ToString() },
						new bool[] { Convert.ToBoolean(position["Junk"]) },
						new bool[] { Convert.ToBoolean(position["Await"]) },
						new string[] { position["RequestRatio"].ToString() },
						new string[] { position["OrderCost"].ToString() },
						new string[] { position["MinOrderCount"].ToString() },
						new ushort[] { 1 },
						new decimal[] { Convert.ToDecimal(position["Cost"]) },
						new string[] { "" },
						new string[] { "" },
						new string[] { "" },
						new string[] { "" });
				}

				Assert.That(serverResponse, Is.Not.Null);
				Assert.That(serverResponse, Is.Not.Empty);
				var serverParams = serverResponse.Split(';');
				Assert.That(serverParams[0], Is.StringStarting("ClientOrderId=").IgnoreCase);
				Assert.That(serverParams[1], Is.StringStarting("PostResult=").IgnoreCase);
				Assert.That(serverParams[2], Is.StringStarting("ServerOrderId=").IgnoreCase);
				Assert.That(serverParams[3], Is.StringStarting("ErrorReason=").IgnoreCase);
				Assert.That(serverParams[4], Is.StringStarting("ServerMinReq=").IgnoreCase);

				var postResult = serverParams[1].Substring(serverParams[1].IndexOf('=') + 1);
				Assert.That(postResult, Is.EqualTo("0"));

				var serverOrderId = serverParams[2].Substring(serverParams[2].IndexOf('=') + 1);
				Assert.That(serverOrderId, Is.Not.Null);
				Assert.That(serverOrderId, Is.Not.Empty);

				try
				{
					var realOrderId = MySqlHelper.ExecuteScalar(connection, "select RowId from orders.ordershead where RowId = " + serverOrderId);
					Assert.That(realOrderId, Is.Not.Null);
					Assert.That(realOrderId.ToString(), Is.Not.Empty);

					var supplierPriceMarkup = MySqlHelper.ExecuteScalar(connection, "select SupplierPriceMarkup from orders.ordersList where OrderId = " + serverOrderId);
					Assert.That(supplierPriceMarkup, Is.EqualTo(DBNull.Value));
				}
				finally
				{
					MySqlHelper.ExecuteNonQuery(connection, "delete from orders.ordershead where RowId = " + serverOrderId);
				}
			}
		}

		private void CreateFolders(string folderName)
		{
			var fullName = Path.Combine("FtpRoot", folderName, "Waybills");
			Directory.CreateDirectory(fullName);
			fullName = Path.Combine("FtpRoot", folderName, "Rejects");
			Directory.CreateDirectory(fullName);
			fullName = Path.Combine("FtpRoot", folderName, "Docs");
			Directory.CreateDirectory(fullName);
		}

	}
}
