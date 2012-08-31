using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using Common.MySql;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.Models;
using Test.Support;

namespace Integration.BaseTests
{
	public class BaseExportFixture : PrepareDataFixture
	{
		protected TestUser user;
		protected UpdateData updateData;
		protected Queue<FileForArchive> files;
		protected string archivefile;

		[SetUp]
		public void BaseExportSetup()
		{
			archivefile = "temp.7z";
			files = new Queue<FileForArchive>();

			user = CreateUser();

			With.Connection(c => { updateData = UpdateHelper.GetUpdateData(c, user.Login); });

			if (File.Exists(Path.GetFullPath(archivefile)))
				File.Delete(Path.GetFullPath(archivefile));
		}

		protected string ReadExportContent(string prefix)
		{
			var fileName = ServiceContext.GetFileByShared(prefix + user.Id + ".txt");
			//ожидание необходимо на devsrv при экспортировании из тестовой базы на шару \\fms\AFFiles
			ShareFileHelper.WaitFile(fileName);
			return File.ReadAllText(fileName, Encoding.GetEncoding(1251));
		}

		protected void Export<T>()
		{
			With.Connection(c => {
				var export = (BaseExport)Activator.CreateInstance(typeof(T), updateData, c, files);
				Assert.IsTrue(updateData.CheckVersion(export.RequiredVersion), "Модель {0} не удовлетворяет требуемой версии {1}", typeof(T), export.RequiredVersion);
				export.Export();
				export.ArchiveFiles(Path.GetFullPath(archivefile));
			});
		}

		protected DataTable Export<T>(string name)
		{
			return With.Connection(c => {
				var export = (BaseExport)Activator.CreateInstance(typeof(T), updateData, c, files);
				export.UnderTest = true;
				Assert.IsTrue(updateData.CheckVersion(export.RequiredVersion), "Модель {0} не удовлетворяет требуемой версии {1}", typeof(T), export.RequiredVersion);
				export.Export();
				return export.data[name];
			});
		}
	}
}