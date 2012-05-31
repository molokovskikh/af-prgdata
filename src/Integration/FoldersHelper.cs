using System;
using System.IO;
using System.Linq;
using Common.Tools;
using NUnit.Framework;

namespace Integration
{
	public class FoldersHelper
	{
		//Имя переменной среды выполнения, в которой хранится ссылка на временную папку, возвращаемую функцией Path.GetTempPath()
		private static string _tempFolderVariableName = "TMP";

		public static void CheckTempFolders(Action action)
		{
			//сохраняем ссылку на оригинальный Temp
			var oldTemp = Environment.GetEnvironmentVariable(_tempFolderVariableName);

			//формируем новую временную директорию
			var newTemp = Path.Combine(oldTemp, Path.GetRandomFileName());

			try {

				Directory.CreateDirectory(newTemp);
				try {
					//подменяем переменную среды
					Environment.SetEnvironmentVariable(_tempFolderVariableName, newTemp);

					var dirsBefore = Directory.GetDirectories(Path.GetTempPath());
					var filesBefore = Directory.GetFiles(Path.GetTempPath());

					action();

					var dirsAfter = Directory.GetDirectories(Path.GetTempPath());
					var filesAfter = Directory.GetFiles(Path.GetTempPath());

					Assert.That(dirsAfter, Is.EquivalentTo(dirsBefore), "Список директорий не совпадает, новые: {0}, удаленные: {1}", dirsAfter.Except(dirsBefore).Implode(), dirsBefore.Except(dirsAfter).Implode());
					Assert.That(filesAfter, Is.EquivalentTo(filesBefore), "Список файлов не совпадает, новые: {0}, удаленные: {1}", filesAfter.Except(filesBefore).Implode(), filesBefore.Except(filesAfter).Implode());
				}
				finally {
					//удаляем все после использования
					FileHelper.DeleteDir(newTemp);
				}

			}
			finally {
				//восстанавливаем оригинальную переменную среды Temp
				Environment.SetEnvironmentVariable(_tempFolderVariableName, oldTemp);
			}
		}
	}
}