using System;
using System.IO;
using NUnit.Framework;
using PrgData.Common;

namespace Integration
{
	[TestFixture]
	public class ServiceContextFixture
	{
		[Test]
		public void CheckExpandExportPath()
		{
			Assert.That(ServiceContext.ExpandExportPath(@"C:\Temp"), Is.EqualTo(@"C:\Temp"));
			Assert.That(ServiceContext.ExpandExportPath(@"\\fms.adc.analit.net\AFFiles"), Is.EqualTo(@"\\fms.adc.analit.net\AFFiles"));

			var expectedDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "testDir");
			if (Directory.Exists(expectedDir))
				Directory.Delete(expectedDir, true);
			Assert.That(ServiceContext.ExpandExportPath("testDir"), Is.EqualTo(expectedDir));
			Assert.That(Directory.Exists(expectedDir), "В процессе работы метода ExpandExportPath не была создана директория {0}", expectedDir);
			Directory.Delete(expectedDir, true);
		}
	}
}