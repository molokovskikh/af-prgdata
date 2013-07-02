using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Tests.Repositories;
using Common.Tools;
using Inforoom.Common;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData.Common;
using PrgData.Common.Models;
using PrgData.Common.Repositories;
using SmartOrderFactory;
using SmartOrderFactory.Domain;
using SmartOrderFactory.Repositories;
using log4net.Config;

namespace Integration
{
	[SetUpFixture]
	public class FixtureSetup
	{
		[SetUp]
		public void Setup()
		{
			XmlConfigurator.Configure();

			ArchiveHelper.SevenZipExePath = @"7zip\7z.exe";
			Common.MySql.With.DefaultConnectionStringName = Common.MySql.ConnectionHelper.GetConnectionName();

			PrepareMySqlPaths();

			CreateResultsDir();

			CheckLocalDB();

			Test.Support.Setup.Initialize();

			ContainerInitializer.InitializerContainerForTests(new Assembly[] { typeof(SmartOrderRule).Assembly, typeof(AnalitFVersionRule).Assembly });

			IoC.Container.Register(
				Component.For<ISmartOrderFactoryRepository>().ImplementedBy<SmartOrderFactoryRepository>(),
				Component.For<ISmartOfferRepository>().ImplementedBy<SmartOfferRepository>(),
				Component.For<IOrderFactory>().ImplementedBy<SmartOrderFactory.SmartOrderFactory>(),
				Component.For<IVersionRuleRepository>().ImplementedBy<VersionRuleRepository>());
		}

		private void PrepareMySqlPaths()
		{
			ServiceContext.SetupMySqlPath();

			if (String.Equals(Environment.MachineName, "devsrv", StringComparison.OrdinalIgnoreCase)) {
				//Если мы запустились на компьютере devsrv, то должны подменить папки для экспорта и импрорта на шары,
				//чтобы этот путь был доступен базе данных с testsql.analit.net
				var shareExportImportPath = @"\\fms.adc.analit.net\AFFiles";
				ServiceContext.MySqlSharedExportPath = () => shareExportImportPath;
				ServiceContext.MySqlLocalImportPath = () => shareExportImportPath;
				CheckDBFiles();
			}
		}

		private void CheckLocalDB()
		{
			using (var connection = Settings.GetConnection()) {
				connection.Open();
				var coreCount = Convert.ToUInt32(MySqlHelper.ExecuteScalar(
					connection,
					"select count(*) from farm.Core0"));

				Assert.That(coreCount, Is.GreaterThan(30000), "Локальная база данных не готова к тестам. Выполните в корне проекта: bake PrepareLocalForPrgData");
			}
		}

		private void CreateResultsDir()
		{
			if (Directory.Exists("results"))
				FileHelper.DeleteDir("results");

			Directory.CreateDirectory("results");
		}

		private void CheckNetworkFile(string testFile)
		{
			if (File.Exists(testFile))
				File.Delete(testFile);

			File.WriteAllText(testFile, "this is file");

			if (File.Exists(testFile)) {
				var contentFile = File.ReadAllText(testFile);
				Assert.That(contentFile, Is.EqualTo("this is file"));
				File.Delete(testFile);
			}
			else
				throw new Exception("Не найден файл: " + testFile);
		}

		private void CheckDBFiles()
		{
			var dbFilesPath = ServiceContext.MySqlLocalImportPath();
			var testFile = ServiceContext.GetFileByLocal(DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt");

			var accessExists = false;
			try {
				CheckNetworkFile(testFile);

				accessExists = true;
			}
			catch (Exception) {
				ConnectToShare(dbFilesPath, "analit\\PrgDataTester", "newpassword");
			}

			if (!accessExists)
				CheckNetworkFile(testFile);
		}

		private void ConnectToShare(string share, string userName, string password)
		{
			//C:\Users\tester>net use \\fms.adc.analit.net\affiles newpassword /user:analit\PrgDataTester

			using (var process = new Process()) {
				var startInfo = new ProcessStartInfo("cmd.exe");
				startInfo.CreateNoWindow = true;
				startInfo.RedirectStandardOutput = true;
				startInfo.RedirectStandardError = true;
				startInfo.UseShellExecute = false;
				startInfo.StandardOutputEncoding = System.Text.Encoding.GetEncoding(866);
				startInfo.StandardErrorEncoding = System.Text.Encoding.GetEncoding(866);
				startInfo.Arguments = String
					.Format(
					"/c net use {0} {1} /user:{2}",
					share,
					password,
					userName);

				process.StartInfo = startInfo;

				process.Start();

				process.WaitForExit();

				if (process.ExitCode != 0) {
					throw new Exception(
						String.Format(
							"Команда подлючения завершилась в ошибкой: {0}\r\n{1}\r\n{2}",
							process.ExitCode,
							process.StandardOutput.ReadToEnd(),
							process.StandardError.ReadToEnd()));
				}
			}
		}
	}
}