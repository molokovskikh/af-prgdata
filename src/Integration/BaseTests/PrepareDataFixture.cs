using System;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Common.Tools;
using Inforoom.Common;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Filter;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData;
using PrgData.Common;

namespace Integration.BaseTests
{
	public class PrepareDataFixture : UserFixture
	{

		protected string UniqueId;

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

		protected string LoadDataAsyncDocs(bool getEtalonData, DateTime accessTime, string appVersion, uint[] documentBodyIds)
		{
			var service = new PrgDataEx();
			var responce = service.GetUserDataWithOrdersAsyncCert(accessTime, getEtalonData, appVersion, 50, UniqueId, "", "", true, null, 1, 1, null, documentBodyIds);

			return responce;
		}

		protected void ShouldBeSuccessfull(string responce)
		{
			Assert.That(responce, Is.StringStarting("URL=http://localhost//GetFileHandler.ashx?Id"));
		}

		protected ulong ParseUpdateId(string responce)
		{
			var match = Regex.Match(responce, @"\d+").Value;
			if (match.Length > 0)
				return Convert.ToUInt64(match);

			Assert.Fail("Не найден номер UpdateId в ответе сервера: {0}", responce);
// ReSharper disable HeuristicUnreachableCode
			return 0;
// ReSharper restore HeuristicUnreachableCode
		}

		protected string CheckAsyncRequest(ulong updateId)
		{
			var service = new PrgDataEx();
			return service.CheckAsyncRequest(updateId);
		}

		protected void WaitAsyncResponse(ulong updateId)
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
			var service = new PrgDataEx();

			var updateTime = service.CommitExchange(updateId, waitingRequestType == RequestType.GetDocs);
			
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
			var dbUpdateTime = Convert.ToDateTime(updateRow["UpdateDate"]);
			var updateType = Convert.ToInt32(updateRow["UpdateType"]);

			Assert.That(updateType, Is.EqualTo((int)waitingRequestType), "Не совпадает тип обновления");

			Assert.That(updateTime, Is.EqualTo(dbUpdateTime.ToUniversalTime()), "Не совпадает дата обновления, выбранная из базы, для UpdateId: {0}", updateId);

			return updateTime;
		}

		protected void ProcessWithLog(Action action)
		{
			try
			{
				var memoryAppender = new MemoryAppender();
				memoryAppender.AddFilter(new LoggerMatchFilter { AcceptOnMatch = true, LoggerToMatch = "PrgData", Next = new DenyAllFilter() });
				BasicConfigurator.Configure(memoryAppender);
				
				try {
					action();
				}
				catch
				{
					var logEvents = memoryAppender.GetEvents();
					Console.WriteLine("Ошибки при подготовке данных:\r\n{0}", logEvents.Select(item =>
					{
						if (string.IsNullOrEmpty(item.GetExceptionString()))
							return item.RenderedMessage;
						else
							return item.RenderedMessage + Environment.NewLine + item.GetExceptionString();
					}).Implode("\r\n"));
					throw;
				}

				var events = memoryAppender.GetEvents();
				var errors = events.Where(item => item.Level >= Level.Warn);
				Assert.That(errors.Count(), Is.EqualTo(0), "При подготовке данных возникли ошибки:\r\n{0}", errors.Select(item => item.RenderedMessage).Implode("\r\n"));
			}
			finally
			{
				LogManager.ResetConfiguration();
			}
		}

	}
}