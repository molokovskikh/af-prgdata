using System.IO;
using Integration.BaseTests;
using NUnit.Framework;
using PrgData.Common.AnalitFVersions;
using PrgData.Common.Models;

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
	}
}