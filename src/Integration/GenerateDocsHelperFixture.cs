using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using Inforoom.Common;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.Orders;

namespace Integration
{
	[TestFixture]
	public class GenerateDocsHelperFixture
	{
		[Test]
		public void ProcessWaybills()
		{			
			using (var connection = new MySqlConnection(Settings.ConnectionString()))
			{
				ulong supplierId = 3;
				string sourceFilePath = @"..\..\Data\3687747_Протек-21_3687688_Протек-21_8993929-001__.sst";
				connection.Open();
				ArchiveHelper.SevenZipExePath = @".\7zip\7z.exe";
				var updateData = UpdateHelper.GetUpdateData(connection, "sergei");

				var waybillDirectory = Path.Combine(ConfigurationManager.AppSettings["DocumentsPath"], String.Format(@"{0}\Waybills", updateData.ClientId));
				if (Directory.Exists(waybillDirectory))
					Directory.Delete(waybillDirectory, true);                           
				Directory.CreateDirectory(waybillDirectory);

				try
				{
					GenerateDocsHelper.ParseWaybils(connection, updateData, updateData.ClientId,
                        new [] { supplierId },
                        new[] { Path.GetFileName(sourceFilePath) },
                        @"..\..\Data\3687747_Протек-21_3687688_Протек-21_8993929-001__.zip");
				}
				catch (Exception)
				{
					// Это делаем в catch потому что сервис не запущен и простого способа поднять сервис для разбора накладных не нашел.
					// Однако, проверить что файл скопирован в папку и сделана запись в document_logs можно.
					var files = Directory.GetFiles(waybillDirectory);
					Assert.That(files.Count(), Is.EqualTo(1));

					var command = new MySqlCommand();
					command.Connection = connection;
					command.CommandText = @"select RowId from logs.document_logs where logtime > curdate() order by logtime desc limit 1";
					var documentId = command.ExecuteScalar().ToString();
					
					command.CommandText = @"select FileName from logs.document_logs where RowId = ?RowId";
					command.Parameters.AddWithValue("?RowId", documentId);
					var filename = command.ExecuteScalar().ToString();

					command.CommandText = @"select ShortName from usersettings.ClientsData where FirmCode = ?FirmCode";
					command.Parameters.AddWithValue("?FirmCode", supplierId);
					var supplierName = command.ExecuteScalar().ToString();
					
					var name = String.Format("{0}_{1}({2}){3}",
						documentId,
						supplierName,
						Path.GetFileNameWithoutExtension(filename),
						Path.GetExtension(filename));
					// Проверяем, что в папке лежит файл с именем в формате ИД_Поставщик(ИмяФайлаВdocument_logs).расширение
					Assert.IsTrue(String.Equals(Path.GetFileName(files[0]), name, StringComparison.OrdinalIgnoreCase));
				}

				FileHelper.DeleteDir(ConfigurationManager.AppSettings["DocumentsPath"]);
				FileHelper.DeleteDir(@"..\..\Data\WaybillExtract");
			}
		}
	}
}
