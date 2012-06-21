using System;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Castle.ActiveRecord;
using Common.Tools;
using Inforoom.Common;
using Test.Support.Documents;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Filter;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData;
using PrgData.Common;
using Test.Support;

namespace Integration.BaseTests
{
	public class PrepareDataFixture : UserFixture
	{

		protected string UniqueId;
		protected MemoryAppender MemoryAppender;

		public virtual void FixtureSetup()
		{
			ArchiveHelper.SevenZipExePath = @".\7zip\7z.exe";
			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ServiceContext.GetResultPath = () => "results\\";
			ConfigurationManager.AppSettings["DocumentsPath"] = "FtpRoot\\";
		}

		public virtual void Setup()
		{
			UniqueId = "123";
			if (Directory.Exists("FtpRoot"))
				FileHelper.DeleteDir("FtpRoot");

			Directory.CreateDirectory("FtpRoot");
		}

		protected void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}

		protected string LoadData(bool getEtalonData, DateTime accessTime, string appVersion)
		{
			var service = new PrgDataEx();
			var responce = service.GetUserDataWithOrders(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", false, null, 1, 1, null);

			return responce;
		}

		protected string LoadDataAsync(bool getEtalonData, DateTime accessTime, string appVersion)
		{
			var service = new PrgDataEx();
			var responce = service.GetUserDataWithOrdersAsync(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", false, null, 1, 1, null);

			return responce;
		}

		protected string LoadDataAsyncDispose(bool getEtalonData, DateTime accessTime, string appVersion)
		{
			using (var service = new PrgDataEx()) {
				var responce = service.GetUserDataWithOrdersAsync(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", false, null, 1, 1, null);

				return responce;
			}
		}

		protected string LoadDataAsyncDocs(bool getEtalonData, DateTime accessTime, string appVersion, uint[] documentBodyIds)
		{
			var service = new PrgDataEx();
			var responce = service.GetUserDataWithOrdersAsyncCert(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", true, null, 1, 1, null, documentBodyIds);

			return responce;
		}

		protected string LoadDataAttachments(bool getEtalonData, DateTime accessTime, string appVersion, uint[] attachmentIds)
		{
			var service = new PrgDataEx();
			var responce = service.GetUserDataWithAttachments(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", false, null, 1, 1, null, null, attachmentIds);

			return responce;
		}

		protected string LoadDataAttachmentsAsync(bool getEtalonData, DateTime accessTime, string appVersion, uint[] attachmentIds)
		{
			var service = new PrgDataEx();
			var responce = service.GetUserDataWithAttachmentsAsync(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", false, null, 1, 1, null, null, attachmentIds);

			return responce;
		}

		protected string LoadDataAttachmentsOnly(bool getEtalonData, DateTime accessTime, string appVersion, uint[] attachmentIds)
		{
			var service = new PrgDataEx();
			var responce = service.GetUserDataWithRequestAttachments(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", false, null, 1, 1, null, null, attachmentIds);

			return responce;
		}

		protected uint ShouldBeSuccessfull(string responce)
		{
			Assert.That(responce, Is.StringStarting("URL=http://localhost/GetFileHandler.ashx?Id"));
			return ParseUpdateId(responce);
		}

		protected uint ParseUpdateId(string responce)
		{
			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
				return Convert.ToUInt32(match);

			Assert.Fail("Не найден номер UpdateId в ответе сервера: {0}", responce);
// ReSharper disable HeuristicUnreachableCode
			return 0;
// ReSharper restore HeuristicUnreachableCode
		}

		protected string CheckAsyncRequest(uint updateId)
		{
			var service = new PrgDataEx();
			return service.CheckAsyncRequest(updateId);
		}

		protected void WaitAsyncResponse(uint updateId)
		{
			var asyncResponse = String.Empty;
			var sleepCount = 0;
			do
			{
				asyncResponse = CheckAsyncRequest(updateId);
				if (asyncResponse == "Res=Wait")
				{
					sleepCount++;
					Thread.Sleep(1000);
				}

			} while (asyncResponse == "Res=Wait" && sleepCount < 5*60);

			Assert.That(asyncResponse, Is.EqualTo("Res=OK"), "Неожидаемый ответ от сервера при проверке асинхронного запроса, sleepCount: {0}", sleepCount);
		}

		protected DateTime CommitExchange(uint updateId, RequestType waitingRequestType)
		{
			var waybillsOnly = waitingRequestType == RequestType.GetDocs || waitingRequestType == RequestType.RequestAttachments;
			var service = new PrgDataEx();

			var updateTime = service.CommitExchange(updateId, waybillsOnly);
			
			//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
			Thread.Sleep(3000);

			var updateRow = MySqlHelper.ExecuteDataRow(
								Settings.ConnectionString(),
				@"
select 
  uui.UpdateDate,
  afu.UpdateType
from 
  logs.AnalitFUpdates afu
  join usersettings.UserUpdateInfo uui on uui.UserId = afu.UserId
where
  afu.UpdateId = ?UpdateId"
				,
				new MySqlParameter("?UpdateId", updateId));
			var updateType = Convert.ToInt32(updateRow["UpdateType"]);

			Assert.That(updateType, Is.EqualTo((int)waitingRequestType), "Не совпадает тип обновления");

			if (!waybillsOnly) {
				var dbUpdateTime = Convert.ToDateTime(updateRow["UpdateDate"]);
				Assert.That(updateTime, Is.EqualTo(dbUpdateTime.ToUniversalTime()), "Не совпадает дата обновления, выбранная из базы, для UpdateId: {0}", updateId);
			}

			return updateTime;
		}

		protected void ProcessWithLog(Action action)
		{
			ProcessWithLog(appender => action() );
		}

		protected void ProcessWithLog(Action<MemoryAppender> action, bool checkWarnLogs = true)
		{
			try
			{
				var memoryAppender = new MemoryAppender();
				memoryAppender.AddFilter(new LoggerMatchFilter { AcceptOnMatch = true, LoggerToMatch = "PrgData", Next = new DenyAllFilter() });
				BasicConfigurator.Configure(memoryAppender);
				
				try {
					action(memoryAppender);
				}
				catch
				{
					var logEvents = memoryAppender.GetEvents();
					Console.WriteLine("Протоколирование при подготовке данных:\r\n{0}", logEvents.Select(item =>
					{
						if (string.IsNullOrEmpty(item.GetExceptionString()))
							return item.RenderedMessage;
						else
							return item.RenderedMessage + Environment.NewLine + item.GetExceptionString();
					}).Implode("\r\n"));
					throw;
				}

				if (checkWarnLogs) {
					var events = memoryAppender.GetEvents();
					var errors = events.Where(item => item.Level >= Level.Warn);
					Assert.That(errors.Count(), Is.EqualTo(0), "При подготовке данных возникли ошибки:\r\n{0}", errors.Select(item => item.RenderedMessage).Implode("\r\n"));
				}

			}
			finally
			{
				LogManager.ResetConfiguration();
			}
		}

		protected string CheckArchive(TestUser user, ulong updateId, string mask = null)
		{
			var afterRequestDataFiles = Directory.GetFiles(ServiceContext.GetResultPath(), (mask ?? "{0}_*.zip").Format(user.Id));
			Assert.That(afterRequestDataFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterRequestDataFiles.Implode());
			if (mask == null)
				Assert.That(afterRequestDataFiles[0], Is.StringEnding("{0}_{1}.zip".Format(user.Id, updateId)));
			return afterRequestDataFiles[0];
		}

		protected string ExtractArchive(string archiveFileName)
		{
			var extractFolder = Path.Combine(Path.GetFullPath(ServiceContext.GetResultPath()), "ExtractZip");
			if (Directory.Exists(extractFolder))
				Directory.Delete(extractFolder, true);
			Directory.CreateDirectory(extractFolder);

			ArchiveHelper.Extract(archiveFileName, "*.*", extractFolder);

			return extractFolder;
		}

		protected void CheckForErrors()
		{
			LogManager.ResetConfiguration();

			var events = MemoryAppender.GetEvents();
			var errors = Enumerable.Where(events, item => item.Level >= Level.Warn);
			Assert.That(errors.Count(), Is.EqualTo(0),
				"При подготовке данных возникли ошибки:\r\n{0}",
				errors.Select(item => item.ExceptionObject).Implode("\r\n"));
		}

		protected void RegisterLogger()
		{
			MemoryAppender = new MemoryAppender();
			MemoryAppender.AddFilter(new LoggerMatchFilter
			{AcceptOnMatch = true, LoggerToMatch = "PrgData", Next = new DenyAllFilter()});
			BasicConfigurator.Configure((IAppender) MemoryAppender);
		}

		protected TestDocumentLog CreateDocument(TestUser user)
		{
			TestDocumentLog doc;
			var address = user.AvaliableAddresses[0];

			using (var transaction = new TransactionScope(OnDispose.Rollback))
			{
				var supplier = user.GetActivePrices()[0].Supplier;
				doc = new TestDocumentLog {
					LogTime = DateTime.Now,
					Supplier = supplier,
					DocumentType = DocumentType.Waybill,
					Client = user.Client,
					AddressId = address.Id,
					FileName = "test.data",
					Ready = true
				};
				doc.Save();
				new TestDocumentSendLog {
					ForUser = user,
					Document = doc
				}.Save();
				transaction.VoteCommit();
			}


			var waybills = Path.Combine("FtpRoot", address.Id.ToString(), "Waybills");
			if (!Directory.Exists(waybills))
				Directory.CreateDirectory(waybills);

			File.WriteAllText(Path.Combine(waybills, String.Format("{0}_test.data", doc.Id)), "");
			var waybillsPath = Path.Combine("FtpRoot", address.Id.ToString(), "Waybills");
			doc.LocalFile = Path.Combine(waybillsPath, String.Format("{0}_test.data", doc.Id));

			return doc;
		}


	}
}