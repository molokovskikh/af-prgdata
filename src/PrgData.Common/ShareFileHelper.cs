using System;
using System.IO;
using System.Threading;

namespace PrgData.Common
{
	public class ShareFileHelper
	{
		public static void WaitFile(string fileName)
		{
			var i = 0;
			while (!File.Exists(fileName))			
			{ 
				i++;
				if (i > 50)
					throw new Exception(String.Format("Файл {0} не появился в папке после экспорта MySql-сервером после 25 секунд ожидания.", fileName));
				Thread.Sleep(500);
			}
		}

		public static void WaitDeleteFile(string fileName)
		{
			var i = 0;
			while (File.Exists(fileName))
			{
				i++;
				if (i > 50)
					throw new Exception(String.Format("Файл {0} не удалился из папки экспорта MySql-сервера после 25 секунд ожидания.", fileName));
				Thread.Sleep(500);
			}
		}
	}
}
