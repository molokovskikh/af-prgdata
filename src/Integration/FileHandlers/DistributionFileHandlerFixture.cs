using System;
using System.IO;
using System.Text;
using System.Web;
using Castle.ActiveRecord;
using Integration.BaseTests;
using NUnit.Framework;
using PrgData.Common;
using PrgData.FileHandlers;
using Test.Support;

namespace Integration
{
	[TestFixture]
	public class DistributionFileHandlerFixture : BaseFileHandlerFixture
	{
		private TestUser user;

		private string requestVersion;
		private string pathToDistrib;

		private string rollBackExeFileName;
		private string bigRackCardFrfFileName;

		[TestFixtureSetUp]
		public void FixtureSetup()
		{
			FileHanderAshxName = "GetDistributionFileHandler.ashx";

			requestVersion = "7.1.1.1553";
			pathToDistrib = Path.Combine(ServiceContext.DistributionPath(), requestVersion, "Exe", "AnalitF");
			if (!Directory.Exists(pathToDistrib))
				Directory.CreateDirectory(pathToDistrib);

			rollBackExeFileName = Path.Combine(pathToDistrib, "Rollback.exe");
			if (!File.Exists(rollBackExeFileName))
				File.WriteAllText(rollBackExeFileName, "Rollback.exe");

			var frfPath = Path.Combine(pathToDistrib, "Frf");
			if (!Directory.Exists(frfPath))
				Directory.CreateDirectory(frfPath);
			bigRackCardFrfFileName = Path.Combine(frfPath, "BigRackCard.frf");
			if (!File.Exists(bigRackCardFrfFileName))
				File.WriteAllText(bigRackCardFrfFileName, "BigRackCard.frf");
		}

		[SetUp]
		public void Setup()
		{
			user = CreateUser();

			SetCurrentUser(user.Login);
		}

		[Test(Description = "Пытаемся вызвать GetDistributionFileHandler для пустого списка параметров")]
		public void CheckNonExistsFile()
		{
			WithHttpContext(context => {
				var fileHandler = new GetDistributionFileHandler();
				fileHandler.ProcessRequest(context);

				Assert.That(context.Response.StatusCode, Is.EqualTo(404), "Не верный код ошибки от сервера");
			});
		}

		private void WithHttpContextAndParams(string version, string fileName, Action<HttpContext> action)
		{
			WithHttpContext(
				action,
				"Version=" + version + "&File=" + (fileName != null ? fileName.Replace("\\", "%5C") : null));
		}

		private void CheckFileName(string version, string fileName, string expectedFileName)
		{
			WithHttpContextAndParams(
				version,
				fileName,
				context => {
					var fileHandler = new GetDistributionFileHandler();

					var requestedFileName = fileHandler.GetDistributionFileName(context);

					Assert.That(requestedFileName, Is.EqualTo(expectedFileName).IgnoreCase);
				});
		}

		[Test(Description = "проверяем работу функции на различных параметрах")]
		public void CheckDistributionFileName()
		{
			CheckFileName("1", "3", Path.Combine(ServiceContext.DistributionPath(), "1", "Exe", "AnalitF", "3"));

			CheckFileName("1.3.4.1774", "Frf\\test.frf", Path.Combine(ServiceContext.DistributionPath(), "1.3.4.1774", "Exe", "AnalitF", "Frf\\test.frf"));

			CheckFileName(null, null, null);

			CheckFileName(String.Empty, String.Empty, null);

			CheckFileName("  ", "dsds", null);
		}

		private void ProcessFileRequest(string version, string fileName, string fullFileName)
		{
			WithHttpContextAndParams(
				version,
				fileName,
				context => {
					var fileHandler = new GetDistributionFileHandler();
					fileHandler.outputStream = new MemoryStream();

					var requestedFileName = fileHandler.GetDistributionFileName(context);

					Assert.That(requestedFileName, Is.EqualTo(fullFileName).IgnoreCase);

					fileHandler.ProcessRequest(context);

					Assert.That(context.Response.StatusCode, Is.EqualTo(200));

					Assert.That(fileHandler.outputStream.Length, Is.EqualTo(new FileInfo(fullFileName).Length), "Не совпадает размер отдаваемого файла");
				});
		}

		[Test(Description = "проверяем отдачу файлов")]
		public void CheckProcessRequest()
		{
			ProcessFileRequest(requestVersion, "Rollback.exe", rollBackExeFileName);

			ProcessFileRequest(requestVersion, "Frf\\BigRackCard.Frf", bigRackCardFrfFileName);
		}
	}
}