using System;
using System.Collections.Generic;
using System.Linq;
using Common.Tools;
using MySql.Data.MySqlClient;

namespace PrgData.Common.Models
{
	public class ExportProcessor
	{
		private List<BaseExport> exporters;

		public ExportProcessor(UpdateData updateData, MySqlConnection connection, Queue<FileForArchive> files)
		{
			var rootType = typeof (BaseExport);
			var types = rootType.Assembly.GetTypes().Where(t =>
				t.Namespace == rootType.Namespace
				&& t.IsClass
				&& !t.IsAbstract
				&& rootType.IsAssignableFrom(t));
			exporters = types
				.Select(t => Activator.CreateInstance(t, updateData, connection, files))
				.Cast<BaseExport>()
				.Where(e => updateData.CheckVersion(e.RequiredVersion))
				.ToList();
		}

		public void Process()
		{
			exporters.Each(e => e.Export());
		}

		public void Archive(bool isDocumentsRequest, string file)
		{
			exporters
				.Where(e => e.SupportDocumentRequest == isDocumentsRequest)
				.Each(e => e.ArchiveFiles(file));
		}
	}
}