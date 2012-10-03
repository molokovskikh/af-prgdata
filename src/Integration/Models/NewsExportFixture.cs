using System.IO;
using Integration.BaseTests;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
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
				new MySqlCommand("insert into Usersettings.News(PublicationDate, Header, Body, DestinationType) values(curdate(), 'Тестовая Новость Для поставщика', '<p>Тестовая Новость Для поставщика</p>', 0)", c).ExecuteNonQuery();
				new MySqlCommand("insert into Usersettings.News(PublicationDate, Header, Body, DestinationType) values(curdate(), 'Тестовая Новость Для всех', '<p>Тестовая Новость Для всех</p>', 2)", c).ExecuteNonQuery();
			});

			Export<NewsExport>();

			Assert.That(File.Exists(archivefile), Is.True,
				"не существует файл {0}",
				Path.GetFullPath(archivefile));

			var content = ReadExportContent("News");
			Assert.That(content, Is.StringContaining("Тестовая Новость Текущая"));
			Assert.That(content, Is.StringContaining("Тестовая Новость Вчерашняя"));
			Assert.That(content, Is.StringContaining("Тестовая Новость Для всех"));
			Assert.That(content, Is.Not.StringContaining("Тестовая Новость Завтрашняя"));
			Assert.That(content, Is.Not.StringContaining("Тестовая Новость Для поставщика"));
			Assert.That(files.Count, Is.EqualTo(1));
		}

		[Test]
		public void CheckAllowedArchiveRequest()
		{
			With.Connection(c => {
				var newsExport = new NewsExport(updateData, c, files);

				Assert.That(newsExport.AllowArchiveFiles(RequestType.GetData), Is.True);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.GetCumulative), Is.True);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.PostOrderBatch), Is.True);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.GetDataAsync), Is.True);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.GetCumulativeAsync), Is.True);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.GetLimitedCumulative), Is.True);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.GetLimitedCumulativeAsync), Is.True);

				Assert.That(newsExport.AllowArchiveFiles(RequestType.ResumeData), Is.False);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.SendOrder), Is.False);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.Forbidden), Is.False);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.Error), Is.False);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.CommitExchange), Is.False);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.GetDocs), Is.False);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.SendWaybills), Is.False);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.SendOrders), Is.False);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.PostPriceDataSettings), Is.False);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.GetHistoryOrders), Is.False);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.ConfirmUserMessage), Is.False);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.SendUserActions), Is.False);
				Assert.That(newsExport.AllowArchiveFiles(RequestType.RequestAttachments), Is.False);
			});
		}
	}
}