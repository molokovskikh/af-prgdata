using System;
using System.Collections.Generic;
using System.IO;
using Common.MySql;
using MySql.Data.MySqlClient;
using PrgData.Common.SevenZip;

namespace PrgData.Common.Models
{
	public class NewsExport : BaseExport
	{
		public NewsExport(UpdateData updateData, MySqlConnection connection, Queue<FileForArchive> files)
			: base(updateData, connection, files)
		{
		}

		public override int RequiredVersion
		{
			get { return 1833; }
		}

		public override void Export()
		{
			var sql = @"
select Id, PublicationDate, Header
from Usersettings.News
where PublicationDate < curdate() + interval 1 day
and Deleted = 0
and DestinationType in (1, 2)
order by PublicationDate desc
limit 30";
			Process("News", sql);
		}

		public override void ArchiveFiles(string archiveFile)
		{
			var sufix = "News";
			var template = "<html>"
				+ "<head>"
				+ "<meta charset=\"utf-8\">"
				+ "</head>"
				+ "<body>"
				+ "{0}"
				+ "<body>"
				+ "</html>";

			var tempPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
			var newsPath = Path.Combine(tempPath, sufix);

			try {
				if (!Directory.Exists(tempPath))
					Directory.CreateDirectory(tempPath);
				if (!Directory.Exists(newsPath))
					Directory.CreateDirectory(newsPath);

				var sql = @"select Id, Body
from Usersettings.News
where PublicationDate < curdate() + interval 1 day and Deleted = 0
and DestinationType in (1, 2)
order by PublicationDate desc
limit 30";
				var news = Db.Read(sql,
					r => Tuple.Create(r.GetUInt32("Id"), r["Body"].ToString()));

				foreach (var tuple in news) {
					var file = Path.Combine(newsPath, tuple.Item1 + ".html");
					File.WriteAllText(file, String.Format(template, tuple.Item2));
				}

				SevenZipHelper.ArchiveFilesWithNames(
					archiveFile,
					Path.Combine(sufix, "*.*"),
					tempPath);
			}
			finally {
				try {
					Directory.Delete(tempPath, true);
				}
				catch (Exception e) {
					log.Error(String.Format("Ошибка при удалении временной папки {0}", tempPath), e);
				}
			}
		}
	}
}