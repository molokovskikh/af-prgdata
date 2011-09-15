using System;
using System.IO;
using System.Linq;
using Common.Tools;
using NUnit.Framework;

namespace Integration
{
	public class FoldersHelper
	{
		public static void CheckTempFolders(Action action)
		{
			var dirsBefore = Directory.GetDirectories(Path.GetTempPath());
			var filesBefore = Directory.GetFiles(Path.GetTempPath());

			action();

			var dirsAfter = Directory.GetDirectories(Path.GetTempPath());
			var filesAfter = Directory.GetFiles(Path.GetTempPath());

			Assert.That(dirsAfter, Is.EquivalentTo(dirsBefore), "Список директорий не совпадает, новые: {0}, удаленные: {1}", dirsAfter.Except(dirsBefore).Implode(), dirsBefore.Except(dirsAfter).Implode());
			Assert.That(filesAfter, Is.EquivalentTo(filesBefore), "Список файлов не совпадает, новые: {0}, удаленные: {1}", filesAfter.Except(filesBefore).Implode(), filesBefore.Except(filesAfter).Implode());
		}
	}
}