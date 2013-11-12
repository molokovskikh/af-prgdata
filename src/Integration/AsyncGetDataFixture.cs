using System;
using System.IO;
using System.Linq;
using System.Threading;
using Castle.ActiveRecord;
using Common.MySql;
using Common.Tools;
using Integration.BaseTests;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using PrgData;
using PrgData.Common;
using Test.Support;
using Test.Support.Logs;
using log4net.Core;
using MySqlHelper = MySql.Data.MySqlClient.MySqlHelper;

namespace Integration
{
	[TestFixture(Description = "тесты для асинхронного запроса данных")]
	public class AsyncGetDataFixture : PrepareDataFixture
	{
		private TestUser _user;

		private string _afAppVersion;
		private DateTime _lastUpdateTime;

		[TestFixtureSetUp]
		public void FixtureSetUp()
		{
			_afAppVersion = "1.1.1.1413";
		}

		[SetUp]
		public void SetUp()
		{
			_user = CreateUser();

			_lastUpdateTime = GetLastUpdateTime(_user);

			SetCurrentUser(_user.Login);

			RegisterLogger();
		}

		[TearDown]
		public void TearDown()
		{
			CheckForErrors();
		}

		[Test(Description = "Простой асинхронный запрос данных")]
		public void SimpleAsyncGetData()
		{
			var firstAsyncResponse = CheckAsyncRequest(1);
			Assert.That(firstAsyncResponse, Is.StringStarting("Error=При выполнении Вашего запроса произошла ошибка."));
			//Надо очистить список событий, т.к. после CheckAsyncRequest(1) будет запись с ошибкой о неожидаемом типе обновления
			MemoryAppender.Clear();

			var responce = LoadDataAsync(false, _lastUpdateTime.ToUniversalTime(), _afAppVersion);
			var simpleUpdateId = ShouldBeSuccessfull(responce);

			var afterAsyncFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_{1}.zip".Format(_user.Id, simpleUpdateId));
			Assert.That(afterAsyncFiles.Length, Is.EqualTo(0), "Файлов быть не должно, т.к. это асинхронный запрос: {0}", afterAsyncFiles.Implode());

			var log = TestAnalitFUpdateLog.Find(Convert.ToUInt32(simpleUpdateId));
			Assert.That(log.Commit, Is.False, "Запрос не должен быть подтвержден");
			Assert.That(log.UserId, Is.EqualTo(_user.Id));
			Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.GetDataAsync)).Or.EqualTo(Convert.ToUInt32(RequestType.GetCumulativeAsync)), "Не совпадает тип обновления");

			WaitAsyncResponse(simpleUpdateId);

			log.Refresh();
			Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.GetData)).Or.EqualTo(Convert.ToUInt32(RequestType.GetCumulative)), "Не совпадает тип обновления");

			var afterAsyncRequestFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_{1}.zip".Format(_user.Id, simpleUpdateId));
			Assert.That(afterAsyncRequestFiles.Length, Is.EqualTo(1), "Неожидаемый список файлов после подготовки обновления: {0}", afterAsyncRequestFiles.Implode());

			var service = new PrgDataEx();
			var updateTime = service.CommitExchange(simpleUpdateId, false);

			//Нужно поспать, т.к. не успевает отрабатывать нитка подтверждения обновления
			Thread.Sleep(3000);

			log.Refresh();
			Assert.That(log.Commit, Is.True, "Запрос не подтвержден");
			Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.GetData)).Or.EqualTo(Convert.ToUInt32(RequestType.GetCumulative)), "Не совпадает тип обновления");
		}

		[Test(Description = "Попытка воспроизвести ошибку по требованию Ошибка #8627 Обновление на подготовке данных висит неограниченное время (версия 1723)")]
		public void SimpleAsyncGetDataError()
		{
			var firstAsyncResponse = CheckAsyncRequest(1);
			Assert.That(firstAsyncResponse, Is.StringStarting("Error=При выполнении Вашего запроса произошла ошибка."));
			//Надо очистить список событий, т.к. после CheckAsyncRequest(1) будет запись с ошибкой о неожидаемом типе обновления
			MemoryAppender.Clear();

			var responce = LoadDataAsyncDispose(false, _lastUpdateTime.ToUniversalTime(), _afAppVersion);
			var simpleUpdateId = ShouldBeSuccessfull(responce);

			var afterAsyncFiles = Directory.GetFiles(ServiceContext.GetResultPath(), "{0}_{1}.zip".Format(_user.Id, simpleUpdateId));
			Assert.That(afterAsyncFiles.Length, Is.EqualTo(0), "Файлов быть не должно, т.к. это асинхронный запрос: {0}", afterAsyncFiles.Implode());

			var log = TestAnalitFUpdateLog.Find(Convert.ToUInt32(simpleUpdateId));
			Assert.That(log.Commit, Is.False, "Запрос не должен быть подтвержден");
			Assert.That(log.UserId, Is.EqualTo(_user.Id));
			Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.GetDataAsync)).Or.EqualTo(Convert.ToUInt32(RequestType.GetCumulativeAsync)), "Не совпадает тип обновления");

			WaitAsyncResponse(simpleUpdateId);

			log.Refresh();
			Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(RequestType.GetData)).Or.EqualTo(Convert.ToUInt32(RequestType.GetCumulative)), "Не совпадает тип обновления");

			CommitExchange(simpleUpdateId, RequestType.GetData);

			log.Refresh();
			Assert.That(log.Commit, Is.True, "Запрос должен быть подтвержден");
			Assert.IsNotNullOrEmpty(log.Addition);
			Assert.That(log.ResultSize, Is.Not.Null);
			Assert.That(log.ResultSize, Is.GreaterThan(1));
		}

		private void CheckUpdateRequestType(RequestType asyncRequest, RequestType finalRequest)
		{
			TestAnalitFUpdateLog log;
			using (new TransactionScope()) {
				log = new TestAnalitFUpdateLog {
					RequestTime = DateTime.Now,
					Commit = false,
					UserId = _user.Id,
					UpdateType = Convert.ToUInt32(asyncRequest)
				};
				log.Save();
			}

			using (var connection = new MySqlConnection(ConnectionHelper.GetConnectionString())) {
				connection.Open();

				var updateData = UpdateHelper.GetUpdateData(connection, _user.Login);

				UpdateHelper.UpdateRequestType(connection, updateData, log.Id, "", 1);
			}

			using (new SessionScope()) {
				log.Refresh();
				Assert.That(log.UpdateType, Is.EqualTo(Convert.ToUInt32(finalRequest)));
				Assert.That(log.ResultSize, Is.EqualTo(1));
			}
		}

		[Test(Description = "проверка обновления для типа обновления Кумулятивное")]
		public void CheckUpdateRequestTypeCumulative()
		{
			CheckUpdateRequestType(RequestType.GetCumulativeAsync, RequestType.GetCumulative);
		}

		[Test(Description = "проверка обновления для типа обновления Накопитильное")]
		public void CheckUpdateRequestTypeGetData()
		{
			CheckUpdateRequestType(RequestType.GetDataAsync, RequestType.GetData);
		}

		[Test(Description = "проверка обновления для типа обновления Частичное кумулятивное")]
		public void CheckUpdateRequestTypeLimitedCumulative()
		{
			CheckUpdateRequestType(RequestType.GetLimitedCumulativeAsync, RequestType.GetLimitedCumulative);
		}

		[Test(Description = "производим накопительное обновление после успешного кумулятивного")]
		public void ProcessGetDataAsyncAfterCumulative()
		{
			ProcessWithLog(() => {
				var cumulativeResponse = LoadDataAttachmentsAsync(true, DateTime.Now, "1.1.1.1413", null);

				var cumulativeUpdateId = ShouldBeSuccessfull(cumulativeResponse);

				WaitAsyncResponse(cumulativeUpdateId);

				TestAnalitFUpdateLog log;
				using (new SessionScope()) {
					log = TestAnalitFUpdateLog.Find(Convert.ToUInt32(cumulativeUpdateId));
					Assert.That(log.Commit, Is.False);
					Assert.IsNullOrEmpty(log.Log);
				}

				var lastUpdate = CommitExchange(cumulativeUpdateId, RequestType.GetCumulative);

				using (new SessionScope()) {
					log.Refresh();
					Assert.That(log.Commit, Is.True);
					Assert.IsNullOrEmpty(log.Log);
				}

				var response = LoadDataAttachmentsAsync(false, lastUpdate, "1.1.1.1413", null);
				var simpleUpdateId = ShouldBeSuccessfull(response);
				WaitAsyncResponse(simpleUpdateId);
				CommitExchange(simpleUpdateId, RequestType.GetData);
			});
		}

		[Test(Description = "производим проверку докачки файла при асинхоронном запросе")]
		public void ProcessGetDataAsyncResume()
		{
			ProcessWithLog(() => {
				var cumulativeResponse = LoadDataAttachmentsAsync(true, DateTime.Now, "1.1.1.1413", null);
				var cumulativeUpdateId = ShouldBeSuccessfull(cumulativeResponse);
				WaitAsyncResponse(cumulativeUpdateId);

				var nextCumulativeResponse = LoadDataAttachmentsAsync(true, DateTime.Now, "1.1.1.1413", null);

				var nextCumulativeUpdateId = ShouldBeSuccessfull(nextCumulativeResponse);

				Assert.That(nextCumulativeUpdateId, Is.EqualTo(cumulativeUpdateId));

				WaitAsyncResponse(nextCumulativeUpdateId);

				TestAnalitFUpdateLog log;
				using (new SessionScope()) {
					log = TestAnalitFUpdateLog.Find(Convert.ToUInt32(nextCumulativeUpdateId));
					Assert.That(log.Commit, Is.False);
					Assert.IsNullOrEmpty(log.Log);
				}

				var lastUpdate = CommitExchange(nextCumulativeUpdateId, RequestType.GetCumulative);

				using (new SessionScope()) {
					log.Refresh();
					Assert.That(log.Commit, Is.True);
					Assert.IsNullOrEmpty(log.Log);
				}
			});
		}

		[Test(Description = "обрабатываем получение ошибки при экспортировании данных при асинхронном запросе")]
		public void GetErrorOnAsync()
		{
			//сохраняем предыдущее значение
			var oldSharedExportPath = ServiceContext.MySqlSharedExportPath();
			try {
				var cumulativeResponse = LoadDataAttachmentsAsync(true, DateTime.Now, "1.1.1.1413", null);

				var cumulativeUpdateId = ShouldBeSuccessfull(cumulativeResponse);

				//Ломаем экспорт при подготовке данных, указывая несуществующую папку
				ServiceContext.MySqlSharedExportPath = () => "errorShared";

				WaitAsyncResponse(cumulativeUpdateId, "Error=При выполнении Вашего запроса произошла ошибка.;Desc=Пожалуйста, повторите попытку через несколько минут.");

				//TestAnalitFUpdateLog log;
				using (new SessionScope()) {
					var log = TestAnalitFUpdateLog.Find(Convert.ToUInt32(cumulativeUpdateId));
					Assert.That(log.Commit, Is.False);
					Assert.IsNullOrEmpty(log.Log);
					Assert.That(log.UpdateType, Is.EqualTo((int)RequestType.Error));
				}

				//Удаляем события, чтобы не возникало ошибки при завершении теста в TearDown()
				MemoryAppender.Clear();
			}
			finally {
				ServiceContext.MySqlSharedExportPath = () => oldSharedExportPath;
			}
		}

		private void checkConnectionList()
		{
			var processSql = "show full processlist";
			var connections = MySqlHelper.ExecuteDataset(ConnectionHelper.GetConnectionString(), processSql);

			var dump = DebugReplicationHelper.TableToString(connections, connections.Tables[0].TableName);
			Assert.That(connections.Tables[0].Rows.Count == 1 && connections.Tables[0].Rows[0]["Info"].ToString().Trim() == processSql, "В списке процессов содержаться неожидаемые соединения:\r\n{0}", dump);
		}

		[Test(Description = "пытаемся воспроизвести ошибки, когда connection не закрывается после подготовки данных: при возникновении ошибки доступа"), Ignore("Это тест имеет смысл запускать только вручную")]
		public void CheckConnectionCountAfterDisableUser()
		{
			checkConnectionList();

			//выключаем пользователя, чтобы получить ошибку доступа
			using (new TransactionScope()) {
				_user.Enabled = false;
				_user.Save();
			}

			var cumulativeResponse = LoadDataAttachmentsAsync(true, DateTime.Now, "1.1.1.1413", null);

			Assert.That(cumulativeResponse, Is.StringStarting("Error="));

			//после ответа надо немного подождать, т.к. освобожденный connection не сразу возвращается в пул
			Thread.Sleep(100);

			//Удаляем события, чтобы не возникало ошибки при завершении теста в TearDown()
			MemoryAppender.Clear();

			//очищаем все пулы и соединений там быть не должно
			MySqlConnection.ClearAllPools();

			checkConnectionList();
		}

		[Test(Description = "пытаемся воспроизвести ошибки, когда connection не закрывается после подготовки данных: при докачке"), Ignore("Это тест имеет смысл запускать только вручную")]
		public void CheckConnectionCountOnResume()
		{
			checkConnectionList();
			var updateDate = DateTime.Now;

			//Производим подготовку данных КО
			var cumulativeResponse = LoadDataAttachmentsAsync(true, updateDate, "1.1.1.1413", null);

			var cumulativeUpdateId = ShouldBeSuccessfull(cumulativeResponse);

			WaitAsyncResponse(cumulativeUpdateId);

			//после ответа надо немного подождать, т.к. освобожденный connection не сразу возвращается в пул
			Thread.Sleep(100);

			//очищаем все пулы и соединений там быть не должно
			MySqlConnection.ClearAllPools();

			checkConnectionList();

			var nextCumulativeResponse = LoadDataAttachmentsAsync(true, updateDate, "1.1.1.1413", null);

			var nextCumulativeUpdateId = ShouldBeSuccessfull(nextCumulativeResponse);

			WaitAsyncResponse(nextCumulativeUpdateId);

			Assert.That(nextCumulativeUpdateId, Is.EqualTo(cumulativeUpdateId), "При повторном запросе должен быть отдан подготовленное обновление");

			//после ответа надо немного подождать, т.к. освобожденный connection не сразу возвращается в пул
			Thread.Sleep(100);

			//очищаем все пулы и соединений там быть не должно
			MySqlConnection.ClearAllPools();

			checkConnectionList();
		}
	}
}