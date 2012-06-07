﻿using System;
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
		{}

		public override int RequiredVersion
		{
			get { return 1827; }
		}

		public override void Export()
		{
			var sql = @"
select Id, PublicationDate, Header
from Usersettings.News
where PublicationDate >= curdate()
and Deleted = 0
order by PublicationDate
limit 30";
			Process("News", sql);
		}

		public override void ArchiveFiles(string archiveFile)
		{
			var sufix = "News";

			var tempPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
			var newsPath = Path.Combine(tempPath, sufix);

			try {
				if (!Directory.Exists(tempPath))
					Directory.CreateDirectory(tempPath);
				if (!Directory.Exists(newsPath))
					Directory.CreateDirectory(newsPath);

				var sql = @"select Id, Body
from Usersettings.News
where PublicationDate >= curdate() and Deleted = 0
order by PublicationDate
limit 30";
				var news = Db.Read(sql,
					r => Tuple.Create(r.GetUInt32("Id"), r["Body"].ToString()));

				foreach (var tuple in news) {
					var file = Path.Combine(newsPath, tuple.Item1 + ".html");
					File.WriteAllText(file, tuple.Item2);
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