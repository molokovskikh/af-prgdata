using System;
using System.IO;
using System.Threading;
using Common.MySql;
using Integration.BaseTests;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.AnalitFVersions;
using PrgData.Common.Models;
using OriginMySqlHelper = MySql.Data.MySqlClient.MySqlHelper;
using Common.Tools;

namespace Integration.Models
{
	[TestFixture]
	public class RejectsExportFixture : BaseExportFixture
	{
		[Test]
		public void Export_new_accumulative_rejects()
		{
			updateData.UpdateExeVersionInfo = new VersionInfo(2000);
			updateData.Cumulative = true;
			Export<RejectExport>();

			ReadExportContent("Rejects");
		}

		[Test]
		public void Export_new_cummulative_rejecs()
		{
			updateData.UpdateExeVersionInfo = new VersionInfo(2000);
			Export<RejectExport>();

			ReadExportContent("Rejects");
		}

		[Test]
		public void Export_old_rejects()
		{
			updateData.UpdateExeVersionInfo = new VersionInfo(1000);
			Export<RejectExport>();

			ReadExportContent("Rejects");
		}

		[Test(Description = "проверяем экспорт забраковки при установке только BuildNumber (нет файлов для автообновления)")]
		public void ExportRejectsWithoutUpdateExe()
		{
			updateData.BuildNumber = 1900;
			Export<RejectExport>();

			ReadExportContent("Rejects");
		}

		[Test(Description = "при обновлении на версию MatchWaybillsToOrders забраковка должна быть экспортирована полностью")]
		public void ExportOnUpdateByMatchWaybillsToOrders()
		{
			var updateRejectDate = DateTime.Now;
			With.Connection(c => {
				OriginMySqlHelper.ExecuteNonQuery(c, "insert into farm.Rejects (Product, Series) values ('testProduct', 'testSeries');");
				updateRejectDate = Convert.ToDateTime(OriginMySqlHelper.ExecuteScalar(c, "select max(UpdateTime) from farm.Rejects"));
			});

			//обновляемся на старой версии
			updateData.BuildNumber = 1800;
			updateData.OldUpdateTime = updateRejectDate.AddHours(1);

			Export<RejectExport>();
			var oldContent = ReadExportContent("Rejects");
			Assert.IsNullOrEmpty(oldContent, "Содержимое файла Rejects должно быть пустое, т.к. дата обновления больше максимальной даты обновления забраковки");

			//устанавливаем версию для автообновления, в этом случаем мы должны получить весь каталог забраковки
			updateData.UpdateExeVersionInfo = new VersionInfo(1840);

			Export<RejectExport>();
			var newContentOnUpdateExe = ReadExportContent("Rejects");
			Assert.IsNotNullOrEmpty(newContentOnUpdateExe, "В файле Rejects должен быть весь каталог забраковки");
		}

		[Test(Description = "накопительное обновление забраковки в новой версии")]
		public void Accumulation()
		{
			var updateRejectDate = DateTime.Now;
			uint lastRejectId = 0u;
			With.Connection(c => {
				OriginMySqlHelper.ExecuteNonQuery(c, "insert into farm.Rejects (Product, Series) values ('testProductExists', 'testSeriesExists');");
				updateRejectDate = Convert.ToDateTime(OriginMySqlHelper.ExecuteScalar(c, "select max(UpdateTime) from farm.Rejects"));
				Thread.Sleep(2000);
				OriginMySqlHelper.ExecuteNonQuery(c, "insert into farm.Rejects (Product, Series) values ('testProductNew', 'testSeriesNew');");
				lastRejectId = Convert.ToUInt32(OriginMySqlHelper.ExecuteScalar(c, "select last_insert_id()"));
			});

			updateData.BuildNumber = 1840;
			updateData.OldUpdateTime = updateRejectDate;

			Export<RejectExport>();
			var content = ReadExportContent("Rejects");
			Assert.IsNotNullOrEmpty(content, "Содержимое файла Rejects не должно быть пустым");

			var rejectList = content.Split('\n');
			Assert.That(rejectList.Length, Is.GreaterThanOrEqualTo(1));
			Assert.That(rejectList[0], Is.StringStarting("{0}\t".Format(lastRejectId)));
		}

		[Test]
		public void CheckAllowedArchiveRequest()
		{
			With.Connection(c => {
				var rejectExport = new RejectExport(updateData, c, files);

				foreach (RequestType request in Enum.GetValues(typeof(RequestType))) {
					Assert.That(rejectExport.AllowArchiveFiles(request), Is.False);
				}
			});
		}
	}
}