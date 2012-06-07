using System.Collections.Generic;
using System.IO;
using System.Text;
using Integration.BaseTests;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.AnalitFVersions;
using PrgData.Common.Models;
using Test.Support;
using Common.MySql;

namespace Integration.Models
{
	[TestFixture]
	public class NewsExportFixture : PrepareDataFixture
	{
		private TestUser user;
		private UpdateData updateData;
		private Queue<FileForArchive> files;

		[SetUp]
		public void Setup()
		{
			files = new Queue<FileForArchive>();

			user = CreateUser();

			With.Connection(c => {
				updateData = UpdateHelper.GetUpdateData(c, user.Login);
			});
			updateData.UpdateExeVersionInfo = new VersionInfo(1900);
		}

		[Test]
		public void Export_news()
		{
			With.Connection(c => {
				new MySqlCommand("delete from Usersettings.News", c).ExecuteNonQuery();
				new MySqlCommand("insert into Usersettings.News(PublicationDate, Header, Body) values(curdate(), 'Тестовая Новость', '<p>Тестовая Новость</p>')", c).ExecuteNonQuery();
			});
			Export();

			Assert.That(File.Exists("temp.7z"), Is.True,
				"не существует файл {0}",
				Path.GetFullPath("temp.7z"));

			var content = ReadExportContent("News");
			Assert.That(content, Is.StringContaining("Тестовая Новость"));
			Assert.That(files.Count, Is.EqualTo(1));
		}

		private string ReadExportContent(string prefix)
		{
			return File.ReadAllText(Path.Combine(ServiceContext.MySqlSharedExportPath(), prefix + user.Id + ".txt"),
				Encoding.GetEncoding(1251));
		}

		private void Export()
		{
			With.Connection(c => {
				var export = new NewsExport(updateData, c, files);
				export.Export();
				export.ArchiveFiles(Path.GetFullPath("temp.7z"));
			});
		}
	}
}