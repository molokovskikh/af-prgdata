﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
using System.ServiceModel;
using System.Text;
using MySql.Data.MySqlClient;
using System.IO;
using System.Configuration;
using Inforoom.Common;
using PrgData.Common.Waybills;
using log4net;

namespace PrgData.Common.Orders
{
	public class GenerateDocsHelper
	{
		enum DocumentType
		{
			Unknown = 0,
			Waybills = 1,
			Rejects = 2,
			Docs = 3
		}

		private static string[] fileNames = new string[] { "unknown", "waybill", "reject", "document"};
		private static string[] russianNames = new string[] { "неизвестный", "накладная", "отказ", "документ" };

		public static void GenerateDocs(MySqlConnection _readWriteConnection, UpdateData updateData, List<ClientOrderHeader> orders)
		{
			var random = new Random();
			var headerCommand = new MySqlCommand();
			headerCommand.Connection = _readWriteConnection;
			headerCommand.Parameters.Add("?PriceCode", MySqlDbType.UInt64);
			headerCommand.Parameters.Add("?FirmCode", MySqlDbType.UInt64);
			headerCommand.Parameters.Add("?ClientCode", MySqlDbType.UInt64);
			headerCommand.Parameters.Add("?OrderId", MySqlDbType.UInt64);
			headerCommand.Parameters.Add("?DocumentType", MySqlDbType.Int32);
			headerCommand.Parameters.Add("?FileName", MySqlDbType.String);

			orders.ForEach((item) =>
			{
				headerCommand.CommandText = "select FirmCode from usersettings.pricesdata pd where pd.PriceCode = ?PriceCode";

				headerCommand.Parameters["?PriceCode"].Value = item.PriceCode;
				var firmCode = Convert.ToUInt64(headerCommand.ExecuteScalar());

				//Кол-во генерируемых документов относительно данного заказа
				var documentCount = random.Next(3) + 1;

				if (documentCount >= 1)
					GenerateDoc(firmCode, headerCommand, updateData, item, DocumentType.Waybills);

				if (documentCount >= 2)
					GenerateDoc(firmCode, headerCommand, updateData, item, DocumentType.Rejects);

				if (documentCount >= 3)
					GenerateDoc(firmCode, headerCommand, updateData, item, DocumentType.Docs);
			});
		}

		private static void GenerateDoc(ulong firmCode, MySqlCommand headerCommand, UpdateData updateData, ClientOrderHeader order, DocumentType documentType)
		{
			headerCommand.CommandText = @"
insert into logs.document_logs 
  (FirmCode, ClientCode, DocumentType, FileName) 
values 
  (?FirmCode, ?ClientCode, ?DocumentType, ?FileName);
set @LastDownloadId = last_insert_id();
insert into documents.DocumentHeaders 
  (DownloadId, FirmCode, ClientCode, DocumentType, OrderId, ProviderDocumentId, DocumentDate)
values
  (@LastDownloadId, ?FirmCode, ?ClientCode, ?DocumentType, ?OrderId, concat(hex(?OrderId), '-', hex(@LastDownloadId)), curdate());
set @LastDocumentId = last_insert_id();
";

			headerCommand.Parameters["?FirmCode"].Value = firmCode;
			headerCommand.Parameters["?ClientCode"].Value = updateData.ClientId;
			headerCommand.Parameters["?OrderId"].Value = order.ServerOrderId;
			headerCommand.Parameters["?DocumentType"].Value = (int)documentType;
			headerCommand.Parameters["?FileName"].Value = fileNames[(int)documentType] + ".txt";
			headerCommand.ExecuteNonQuery();

			headerCommand.CommandText = "select @LastDownloadId";
			var lastDownloadId = Convert.ToUInt64(headerCommand.ExecuteScalar());

			headerCommand.CommandText = "select @LastDocumentId";
			var lastDocumentId = Convert.ToUInt64(headerCommand.ExecuteScalar());

			if (documentType != DocumentType.Docs)
				GenerateDocDetail(lastDocumentId, headerCommand.Connection, documentType, order);

			var createdFileName = 
				ConfigurationManager.AppSettings["DocumentsPath"] 
				+ updateData.ClientId.ToString().PadLeft(3, '0') 
				+ "\\" + documentType.ToString() + "\\" 
				+ lastDownloadId + "_" + fileNames[(int)documentType] + ".txt";
			using (var stream = new StreamWriter(createdFileName, false, Encoding.GetEncoding(1251)))
			{
				stream.WriteLine("Это {0} №{1}", russianNames[(int)documentType], lastDocumentId);
				stream.WriteLine("Ссылка на загруженный файл №{0}", lastDownloadId);
				stream.WriteLine("Ссылка на заказ №{0}", order.ServerOrderId);
			}
		}

		private static void GenerateDocDetail(ulong lastDocumentId, MySqlConnection connection, DocumentType documentType, ClientOrderHeader order)
		{
			var random = new Random();
			var detailCommand = new MySqlCommand();
			detailCommand.Connection = connection;
			detailCommand.Parameters.AddWithValue("?DocumentId", lastDocumentId);
			detailCommand.Parameters.Add("?Product", MySqlDbType.String);
			detailCommand.Parameters.Add("?Code", MySqlDbType.String);
			detailCommand.Parameters.Add("?Period", MySqlDbType.String);
			detailCommand.Parameters.Add("?Producer", MySqlDbType.String);
			detailCommand.Parameters.Add("?ProducerCost", MySqlDbType.Decimal);
			detailCommand.Parameters.Add("?RegistryCost", MySqlDbType.Decimal);
			detailCommand.Parameters.Add("?SupplierPriceMarkup", MySqlDbType.Decimal);
			detailCommand.Parameters.Add("?SupplierCostWithoutNDS", MySqlDbType.Decimal);
			detailCommand.Parameters.Add("?SupplierCost", MySqlDbType.Decimal);
			detailCommand.Parameters.Add("?Quantity", MySqlDbType.Int32);
			detailCommand.Parameters.Add("?VitallyImportant", MySqlDbType.Byte);
			detailCommand.Parameters.Add("?NDS", MySqlDbType.Int32);

			if (documentType == DocumentType.Waybills)
				detailCommand.CommandText = @"
insert into documents.DocumentBodies
  (DocumentId, Product, Code, Period, Producer, ProducerCost, RegistryCost, SupplierPriceMarkup, SupplierCostWithoutNDS, SupplierCost, Quantity, VitallyImportant, NDS)
values
  (?DocumentId, ?Product, ?Code, ?Period, ?Producer, ?ProducerCost, ?RegistryCost, ?SupplierPriceMarkup, ?SupplierCostWithoutNDS, ?SupplierCost, ?Quantity, ?VitallyImportant, ?NDS);";
			else
			  detailCommand.CommandText = @"
insert into documents.DocumentBodies
  (DocumentId, Product, Code, Period, Producer, SupplierCost, Quantity)
values
  (?DocumentId, ?Product, ?Code, ?Period, ?Producer, ?SupplierCost, ?Quantity);";

			order.Positions.ForEach((position) =>
			{
				var synonymName = Convert.ToString(MySqlHelper.ExecuteScalar(
					connection,
					"select Synonym from farm.Synonym where SynonymCode = ?SynonymCode",
					new MySqlParameter("?SynonymCode", position.SynonymCode)));
				var synonymFirmCrName = Convert.ToString(MySqlHelper.ExecuteScalar(
					connection,
					"select Synonym from farm.SynonymFirmCr where SynonymFirmCrCode = ?SynonymFirmCrCode",
					new MySqlParameter("?SynonymFirmCrCode", position.SynonymFirmCrCode)));

				detailCommand.Parameters["?Product"].Value = synonymName;
				detailCommand.Parameters["?Code"].Value = position.Code;
				detailCommand.Parameters["?Period"].Value = position.Period;
				detailCommand.Parameters["?Producer"].Value = synonymFirmCrName;
				detailCommand.Parameters["?Quantity"].Value = position.Quantity;
				detailCommand.Parameters["?SupplierCost"].Value = position.Cost;
				detailCommand.Parameters["?RegistryCost"].Value = position.RegistryCost;

				if (documentType == DocumentType.Waybills)
				{

					if (position.SupplierPriceMarkup.HasValue)
					{
						detailCommand.Parameters["?SupplierPriceMarkup"].Value = position.SupplierPriceMarkup;
						detailCommand.Parameters["?ProducerCost"].Value = position.ProducerCost;
						detailCommand.Parameters["?VitallyImportant"].Value = position.VitallyImportant;
						detailCommand.Parameters["?NDS"].Value = position.NDS;
						if (position.NDS.HasValue)
							detailCommand.Parameters["?SupplierCostWithoutNDS"].Value = position.Cost / (1 + position.NDS/100);
						else
							detailCommand.Parameters["?SupplierCostWithoutNDS"].Value = position.Cost / 1.10m;
					}
					else
					{
						detailCommand.Parameters["?SupplierPriceMarkup"].Value = 10m;
						detailCommand.Parameters["?ProducerCost"].Value = position.Cost / 1.25m;

						detailCommand.Parameters["?SupplierCostWithoutNDS"].Value = position.Cost / 1.18m;
						detailCommand.Parameters["?NDS"].Value = 18;

						switch (random.Next(3))
						{
							case 1:
								detailCommand.Parameters["?VitallyImportant"].Value = 0;
								break;
							case 2:
								detailCommand.Parameters["?VitallyImportant"].Value = 1;
								detailCommand.Parameters["?SupplierCostWithoutNDS"].Value = position.Cost / 1.1m;
								detailCommand.Parameters["?NDS"].Value = 10;
								break;
							default:
								detailCommand.Parameters["?VitallyImportant"].Value = null;
								break;
						}
					}
				}

				detailCommand.ExecuteNonQuery();
			});
		}

		public static bool ParseWaybils(MySqlConnection connection, UpdateData updateData, uint clientId, ulong[] providerIds, string[] fileNames, string waybillArchive)
		{
			var extractDir = Path.GetDirectoryName(waybillArchive) + "\\WaybillExtract";
			if (!Directory.Exists(extractDir))
				Directory.CreateDirectory(extractDir);

			ArchiveHelper.Extract(waybillArchive, "*.*", extractDir);
			var fileLength = new FileInfo(waybillArchive).Length;

			var ids = new List<uint>();

			global::Common.MySql.With.DeadlockWraper(() =>
			{
				var transaction = connection.BeginTransaction();
				try
				{
					var updateId = Convert.ToUInt64(MySqlHelper.ExecuteScalar(
						connection,
						@"
insert into logs.AnalitFUpdates 
  (RequestTime, UpdateType, UserId, ResultSize, Commit) 
values 
  (now(), ?UpdateType, ?UserId, ?Size, 1);
select last_insert_id()
"
						,
						new MySqlParameter("?UpdateType", (int)RequestType.SendWaybills),
						new MySqlParameter("?UserId", updateData.UserId),
						new MySqlParameter("?Size", fileLength)));

					ids.Clear();

					for (var i = 0; i < fileNames.Length; i++)
					{
						if (File.Exists(extractDir + "\\" + fileNames[i]))
							ids.Add(CopyWaybill(connection, updateData, clientId, providerIds[i], extractDir + "\\" + fileNames[i], updateId));
					}

					transaction.Commit();
				}
				catch
				{
					try
					{
						transaction.Rollback();
					}
					catch (Exception rollbackException)
					{
						ILog _logger = LogManager.GetLogger(typeof(GenerateDocsHelper));
						_logger.Error("Ошибка при rollback'е транзакции сохранения заказов", rollbackException);
					}
					throw;
				}
			});

			return ProcessWaybills(ids);
		}

		private static bool ProcessWaybills(List<uint> ids)
		{
			var binding = new NetTcpBinding();
			binding.Security.Mode = SecurityMode.None;

			var factory = new ChannelFactory<IWaybillService>(binding, ConfigurationManager.AppSettings["WaybillServiceUri"]);
			var channel = factory.CreateChannel();
			var communicationObject = (ICommunicationObject)channel;
			try
			{
				var parsedIds = channel.ParseWaybill(ids.ToArray());
				communicationObject.Close();
				return parsedIds.Length > 0;
			}
			catch(Exception)
			{
				if (communicationObject.State != CommunicationState.Closed)
					communicationObject.Abort();
				throw;
			}
		}

		private static string GetCuttedFileName(string fileName)
		{
			if (fileName.Length > 50)
				return fileName.Substring(fileName.Length - 50, 50);
			return fileName;
		}

		private static uint CopyWaybill(MySqlConnection connection, UpdateData updateData, uint addressId, ulong providerId, string waybillFileName, ulong updateId)
		{
			var resultFileName = Path.GetFileName(waybillFileName);
			var index = resultFileName.IndexOf('_');
			if (index >= 0)
				resultFileName = resultFileName.Substring(index + 1);
			resultFileName = GetCuttedFileName(resultFileName);

			var headerCommand = new MySqlCommand();
			headerCommand.Connection = connection;
			headerCommand.Parameters.Add("?PriceCode", MySqlDbType.UInt64);
			headerCommand.Parameters.Add("?FirmCode", MySqlDbType.UInt64);
			headerCommand.Parameters.Add("?ClientCode", MySqlDbType.UInt64);
			headerCommand.Parameters.Add("?AddressId", MySqlDbType.UInt64);
			headerCommand.Parameters.Add("?OrderId", MySqlDbType.UInt64);
			headerCommand.Parameters.Add("?DocumentType", MySqlDbType.Int32);
			headerCommand.Parameters.Add("?FileName", MySqlDbType.String);
			headerCommand.Parameters.Add("?SendUpdateId", MySqlDbType.UInt64);

			headerCommand.CommandText = @"
insert into logs.document_logs (FirmCode, ClientCode, DocumentType, FileName, AddressId, SendUpdateId) 
values (?FirmCode, ?ClientCode, ?DocumentType, ?FileName, ?AddressId, ?SendUpdateId);

set @LastDownloadId = last_insert_id();
";

			headerCommand.Parameters["?FirmCode"].Value = providerId;
			headerCommand.Parameters["?FileName"].Value = resultFileName;
			headerCommand.Parameters["?DocumentType"].Value = (int)DocumentType.Waybills;
			if (updateData.IsFutureClient)
			{
				headerCommand.Parameters["?ClientCode"].Value = updateData.ClientId;
				headerCommand.Parameters["?AddressId"].Value = addressId;
			}
			else
				headerCommand.Parameters["?ClientCode"].Value = addressId;
			headerCommand.Parameters["?SendUpdateId"].Value = updateId;
			headerCommand.ExecuteNonQuery();

			headerCommand.CommandText = "select @LastDownloadId";
			var lastDownloadId = Convert.ToUInt32(headerCommand.ExecuteScalar());

			headerCommand.CommandText = "select ShortName from usersettings.ClientsData where FirmCode = ?FirmCode;";
			var shortName = headerCommand.ExecuteScalar();

			resultFileName = 
				String.Format("{0}_{1}({2}){3}",
					lastDownloadId,
					shortName,
					Path.GetFileNameWithoutExtension(resultFileName),
					Path.GetExtension(resultFileName));

			File.Copy(
				waybillFileName,
				Path.Combine(
					Path.Combine(
						Path.Combine(
							ConfigurationManager.AppSettings["WaybillPath"],
						addressId.ToString().PadLeft(3, '0')),
						DocumentType.Waybills.ToString()),
					resultFileName
				)
			);

			return lastDownloadId;
		}
	}
}
