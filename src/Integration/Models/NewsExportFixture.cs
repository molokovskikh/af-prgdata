using System.IO;
using Integration.BaseTests;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common.AnalitFVersions;
using Common.MySql;
using PrgData.Common.Models;

namespace Integration.Models
{
	[TestFixture]
	public class NewsExportFixture : BaseExportFixture
	{
		[SetUp]
		public void Setup()
		{
			updateData.UpdateExeVersionInfo = new VersionInfo(1900);
		}

		[Test]
		public void Export_news()
		{
			With.Connection(c => {
				new MySqlCommand("delete from Usersettings.News", c).ExecuteNonQuery();
				new MySqlCommand("insert into Usersettings.News(PublicationDate, Header, Body) values(curdate(), 'Тестовая Новость Текущая', '<p>Тестовая Новость Текущая</p>')", c).ExecuteNonQuery();
				new MySqlCommand("insert into Usersettings.News(PublicationDate, Header, Body) values(curdate() + interval 1 day, 'Тестовая Новость Завтрашняя', '<p>Тестовая Новость Завтрашняя</p>')", c).ExecuteNonQuery();
				new MySqlCommand("insert into Usersettings.News(PublicationDate, Header, Body) values(curdate() - interval 1 day, 'Тестовая Новость Вчерашняя', '<p>Тестовая Новость Вчерашняя</p>')", c).ExecuteNonQuery();
			});
			Export<NewsExport>();

			Assert.That(File.Exists(archivefile), Is.True,
				"не существует файл {0}",
				Path.GetFullPath(archivefile));

			var content = ReadExportContent("News");
			Assert.That(content, Is.StringContaining("Тестовая Новость Текущая"));
			Assert.That(content, Is.StringContaining("Тестовая Новость Вчерашняя"));
			Assert.That(files.Count, Is.EqualTo(1));
		}
	}
}