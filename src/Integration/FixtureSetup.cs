using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using Castle.MicroKernel.Registration;
using Common.Models;
using Common.Models.Tests.Repositories;
using Common.Tools;
using Inforoom.Common;
using NUnit.Framework;
using PrgData.Common.Model;
using PrgData.Common.Repositories;
using SmartOrderFactory;
using SmartOrderFactory.Domain;
using SmartOrderFactory.Repositories;

namespace Integration
{
	[SetUpFixture]
	public class FixtureSetup
	{
		[SetUp]
		public void Setup()
		{
			CheckDBFiles();

			CreateResultsDir();

			Test.Support.Setup.Initialize();

			ContainerInitializer.InitializerContainerForTests(new Assembly[] { typeof(SmartOrderRule).Assembly, typeof(AnalitFVersionRule).Assembly });

			IoC.Container.Register(
				Component.For<ISmartOrderFactoryRepository>().ImplementedBy<SmartOrderFactoryRepository>(),
				Component.For<ISmartOfferRepository>().ImplementedBy<SmartOfferRepository>(),
				Component.For<IOrderFactory>().ImplementedBy<SmartOrderFactory.SmartOrderFactory>(),
				Component.For<IVersionRuleRepository>().ImplementedBy<VersionRuleRepository>()
				);
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

			if (File.Exists(testFile))
			{
				var contentFile = File.ReadAllText(testFile);
				Assert.That(contentFile, Is.EqualTo("this is file"));
				File.Delete(testFile);
			}
			else
				throw new Exception("Не найден файл: " + testFile);
		}

		private void CheckDBFiles()
		{
			var dbFilesPath = System.Configuration.ConfigurationManager.AppSettings["MySqlLocalFilePath"];
			if ((dbFilesPath.Length > 0) && (dbFilesPath[dbFilesPath.Length - 1] == Path.DirectorySeparatorChar))
				dbFilesPath = dbFilesPath.Slice(dbFilesPath.Length - 1);
			var testFile = Path.Combine(dbFilesPath, DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt");

			var accessExists = false;
			try
			{
				CheckNetworkFile(testFile);

				accessExists = true;

			}
			catch (Exception)
			{
				ConnectToShare(dbFilesPath, "analit\\PrgDataTester", "newpassword");
			}

			if (!accessExists)
				CheckNetworkFile(testFile);
		}

		private void ConnectToShare(string share, string userName, string password)
		{
			//C:\Users\tester>net use \\fms.adc.analit.net\affiles newpassword /user:analit\PrgDataTester

			using (var process = new Process())
			{
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

				if (process.ExitCode != 0)
				{
					throw new Exception(
						String.Format(
							"Команда подлючения завершилась в ошибкой: {0}\r\n{1}\r\n{2}",
							process.ExitCode,
							process.StandardOutput.ReadToEnd(),
							process.StandardError.ReadToEnd()
							));
				}
			}
		}
	}
}