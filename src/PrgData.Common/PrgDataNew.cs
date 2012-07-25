using System;
using System.Configuration;
using System.Threading;
using System.Web.Services;
using MySql.Data.MySqlClient;
using PrgData.Common;
using PrgData.Common.Models;
using log4net;
using Common.Tools;

namespace PrgData
{
	public class PrgDataNew : WebService
	{
		protected RequestType UpdateType;
		protected string Addition;
		protected bool ErrorFlag;
		protected string UserHost;
		protected ILog Log;
		protected UpdateData UpdateData;
		protected uint? GUpdateId = 0;
		protected string UserName;
		protected uint CCode;
		protected uint UserId;
		protected bool SpyHostsFile;
		protected bool SpyAccount;
		protected DateTime UncDT;

		protected MySqlConnection readWriteConnection;

		protected MySqlCommand Cm;

		protected Thread ProtocolUpdatesThread;

		protected bool DBConnect()
		{
			UserHost = ServiceContext.GetUserHost();
			try {
				readWriteConnection = Settings.GetConnection();
				readWriteConnection.Open();

				return true;
			}
			catch  {
				DBDisconnect();
				throw;
			}
		}

		protected void DBDisconnect()
		{
			try {
				if (AsyncPrgDatas.Contains(this))
					Log.Debug("Попытка удалить из списка AsyncPrgDatas.DeleteFromList");
				AsyncPrgDatas.DeleteFromList(this);
				if (readWriteConnection != null)
					readWriteConnection.Dispose();
			}
			catch (Exception e) {
				Log.Error("Ошибка при закрытии соединения", e);
				throw;
			}
		}

		protected void GetClientCode()
		{
			UserName = ServiceContext.GetShortUserName();
			ThreadContext.Properties["user"] = ServiceContext.GetUserName();
			this.UpdateData = UpdateHelper.GetUpdateData(readWriteConnection, UserName);

			if (UpdateData == null || UpdateData.Disabled()) {
				if (UpdateData == null) {
			        throw new UpdateException(
			            "Доступ закрыт.",
			            "Пожалуйста, обратитесь в АК \"Инфорум\".[1]",
						"Для логина " + UserName + " услуга не предоставляется; ",
			            RequestType.Forbidden);
				}
				else {
					if (UpdateData.BillingDisabled()) {
			            throw new UpdateException(
			                "В связи с неоплатой услуг доступ закрыт.",
			                "Пожалуйста, обратитесь в бухгалтерию АК \"Инфорум\".[1]",
							"Для логина " + UserName + " услуга не предоставляется: " + UpdateData.DisabledMessage() + "; ",
			                RequestType.Forbidden);
					}
					else {
			            throw new UpdateException(
			                "Доступ закрыт.",
			                "Пожалуйста, обратитесь в АК \"Инфорум\".[1]",
							"Для логина " + UserName + " услуга не предоставляется: " + UpdateData.DisabledMessage() + "; ",
			                RequestType.Forbidden);
					}
				}
			}

			UpdateData.ResultPath = ServiceContext.GetResultPath();
			UpdateData.ClientHost = UserHost;
			CCode = UpdateData.ClientId;
			UserId = UpdateData.UserId;
			UncDT = UpdateData.UncommitedUpdateTime;
			SpyHostsFile = UpdateData.Spy;
			SpyAccount = UpdateData.SpyAccount;
			ThreadContext.Properties["user"] = UpdateData.UserName;

			Cm.Parameters.AddWithValue("?UserName", UserName);
			Cm.Parameters.AddWithValue("?ClientCode", CCode);

			Cm.Connection = readWriteConnection;
			Cm.Transaction = null;
			Cm.CommandText = @"
UPDATE Logs.AuthorizationDates A
SET     AFTime    = now()
WHERE   UserId=" + UserId;
			var authorizationDatesCounter = Cm.ExecuteNonQuery();

			if (authorizationDatesCounter != 1)
				Addition += "Нет записи в AuthorizationDates (" + UserId + "); ";
		}

		protected string ProcessUpdateException(UpdateException updateException, bool wait)
		{
			UpdateType = updateException.UpdateType;
			Addition += updateException.Addition + "; IP:" + UserHost + "; ";
			ErrorFlag = true;

			if (UpdateData != null) {
				Log.Warn(updateException);
				ProtocolUpdatesThread.Start();

				if (wait) {
					var waitCount = 0;
					while (GUpdateId == 0 && waitCount < 30) {
						waitCount += 1;
						Thread.Sleep(500);
					}
				}

			}
			else {
				if (UpdateHelper.UserExists(readWriteConnection, UserName) ) {
					Log.Warn(updateException);
					Common.MailHelper.Mail(
						"Хост: " + Environment.MachineName + Environment.NewLine +
						"Пользователь: " + UserName + Environment.NewLine +
						updateException.ToString(),
						updateException.Message,
						null,
						null, ConfigurationManager.AppSettings["SupportMail"]);
				}
				else
					Log.Error(updateException);
			}
			return updateException.GetAnalitFMessage();
		}

		protected string ProcessUpdateException(UpdateException updateException)
		{
			return ProcessUpdateException(updateException, false);
		}

		[WebMethod]
		public string CheckAsyncRequest(uint UpdateId)
		{
			try {
				DBConnect();
				GetClientCode();

				var rawUpdateType = AnalitFUpdate.GetUpdateTypeByUpdateId(readWriteConnection, UpdateId);

				if (!rawUpdateType.HasValue)
					throw new Exception("Получили null для updateId = {0}".Format(UpdateId));

				RequestType requestType;
				if (RequestType.TryParse(rawUpdateType.ToString(), out requestType)) {

					if (requestType == RequestType.GetDataAsync || requestType == RequestType.GetCumulativeAsync || requestType == RequestType.GetLimitedCumulativeAsync)
						return "Res=Wait";
					else
						if (requestType == RequestType.GetData || requestType == RequestType.GetCumulative || requestType == RequestType.GetLimitedCumulative)
							return "Res=OK";
						else
							throw new Exception("Получили неожидаемый тип обновления {0} для updateId = {1}".Format(requestType, UpdateId));
				}
				else
					throw new Exception("Получили неизвестный тип обновления {0} для updateId = {1}".Format(rawUpdateType, UpdateId));
			}
			catch (UpdateException updateException) {
				return ProcessUpdateException(updateException);
			}
			catch (Exception ex) {
				LogRequestHelper.MailWithRequest(Log, "Ошибка при проверке статуста асинхронного обновления", ex);
				return "Error=При выполнении Вашего запроса произошла ошибка.;Desc=Пожалуйста, повторите попытку через несколько минут.";
			}
			finally {
				DBDisconnect();
			}
		}

	}
}