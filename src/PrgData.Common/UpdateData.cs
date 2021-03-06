﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using Common.Models;
using Common.Tools;
using Common.Tools.Calendar;
using Common.Tools.Helpers;
using ICSharpCode.SharpZipLib.Zip;
using PrgData.Common.AnalitFVersions;
using PrgData.Common.Orders;

namespace PrgData.Common
{
	public class UncommittedRequest
	{
		public uint? UpdateId;
		public RequestType RequestType;
		public DateTime RequestTime;
		public bool Commit;
		public string Addition;
	}

	public class CertificateRequest
	{
		public uint DocumentBodyId;
		public uint? CertificateId;
		public List<uint> CertificateFiles = new List<uint>();
		//сертификаты которые на самом деле были отправлены клиенту
		//тк какие либо файлы мы можем не найти
		//нужно что бы сформировать лог об отправке
		public List<string> SendedFiles = new List<string>();
	}

	public class AttachmentRequest
	{
		public uint AttachmentId;
		public bool Success;
	}

	public class ExportedMiniMail
	{
		public uint MiniMailId;
		public bool ForceExportAttachments;
	}

	public class UpdateData
	{
		private static int _versionBeforeConfirmUserMessage = 1299;
		public static int _versionBeforeSupplierPromotions = 1363;
		//версия AnalitF до поддержки отсрочек платежа с разделением на ЖНВЛС и прочий ассортимент
		private static int _versionBeforeDelayWithVitallyImportant = 1385;
		//версия AnalitF до поддержки отсрочек платежа по прайс-листам
		private static int _versionBeforeDelayByPrice = 1403;
		//версия AnalitF до поддержки загрузки неподтвержденных заказов
		private static int _versionBeforeDownloadUnconfirmedOrders = 1411;
		//версия AnalitF до поддержки экспорта счет-фактуры
		private static int _versionBeforeInvoiceHeaders = 1461;
		//версия AnalitF до поддержки настройки "Показывать цену поставщика при отсрочках платежа"
		private static int _versionBeforeShowSupplierCost = 1489;
		//версия AnalitF до поддержки настройки "Расписание обновлений AnalitF"
		private static int _versionBeforeAnalitFSchedule = 1505;
		//версия AnalitF до поддержки экспортрования поля "SendDate" при экспорте неподтвержденных заказов
		private static int _versionBeforeExportSendDate = 1540;
		//версия AnalitF до поддержки экспорта сертификатов
		private static int _versionBeforeCertificates = 1580;
		//версия AnalitF до поддержки розничных наценок по каждому препарату (от 1755)
		private static int _versionBeforeRetailMargins = 1765;
		//версия AnalitF до поддержки превышения по среднему заказанному количеству (от 1791)
		private static int _versionBeforeExcessAvgOrderTimes = 1800;
		//версия AnalitF до поддержки принудительной загрузки истории документов (от 1809)
		private static int _versionBeforeHistoryDocs = 1820;
		//версия AnalitF до поддержки сопоставления накладных заказам (от 1833)
		private static int _versionBeforeMatchWaybillsToOrders = 1833;
		//версия AnalitF до коррективки поля TechContact у региона (от 1869)
		private static int _versionBeforeCorrectTechContact = 1869;
		//версия AnalitF до экспорта новых полей в core: EAN13, CodeOKP, Series (от 1877)
		private static int _versionBeforeEAN13 = 1877;
		//версия AnalitF до экспорта поле Адрес отдельным столбцом (от 1883)
		private static int _version1883 = 1883;
		//версия AnalitF до поддержки минимального дозаказа (от 1927)
		private static int _versionBeforeMinReordering = 1927;
		//версия AnalitF до поддержки экспорта поля Core0.Exp (от 1935)
		private static int _versionBeforeExportExp = 1935;

		public string ShortName;
		public uint ClientId;
		public uint UserId;
		public string UserName;
		public bool CheckCopyId;
		public string Message;
		public DateTime OldUpdateTime;
		public DateTime UncommitedUpdateTime;
		public DateTime? CatalogUpdateTime;

		public DateTime? BuyingMatrixUpdateTime;
		public DateTime? OfferMatrixUpdateTime;

		public uint? OffersClientCode;
		public ulong? OffersRegionCode;
		public bool EnableImpersonalPrice;
		public uint ImpersonalPriceId = 2647;

		public bool Spy;
		public bool SpyAccount;

		public bool ClientEnabled;
		public bool UserEnabled;
		public bool AFPermissionExists;

		public bool SaveAFDataFiles;

		public uint? BuildNumber;
		public uint? KnownBuildNumber;
		public uint? TargetVersion;

		public string UniqueID;
		public string KnownUniqueID;

		public bool NeedUpdateToBuyingMatrix { get; private set; }

		public bool NeedUpdateToNewMNN { get; private set; }

		public string ClientHost;

		public string CostSessionKey;

		public bool NeedUpdateToCryptCost { get; private set; }

		public bool NeedUpdateToNewClientsWithLegalEntity { get; private set; }

		public bool NeedUpdateToSupplierPromotions { get; private set; }

		public VersionInfo UpdateExeVersionInfo { get; set; }

		public uint? NetworkPriceId;

		public bool ShowAdvertising;

		public ulong RegionMask;

		public UncommittedRequest PreviousRequest;

		public bool AllowDownloadUnconfirmedOrders;

		public bool AllowAnalitFSchedule;

		public string ResultPath;
		private string _currentTempFileName;

		public uint MaxOrderId;
		public uint MaxOrderListId;

		public List<SupplierPromotion> SupplierPromotions = new List<SupplierPromotion>();

		public List<UnconfirmedOrderInfo> UnconfirmedOrders = new List<UnconfirmedOrderInfo>();

		public bool AsyncRequest;
		public bool Cumulative;

		public List<CertificateRequest> CertificateRequests = new List<CertificateRequest>();

		public bool SendWaybills;
		public bool SendRejects;

		public uint LastLockId;

		public List<AttachmentRequest> AttachmentRequests = new List<AttachmentRequest>();
		public List<ExportedMiniMail> ExportMails = new List<ExportedMiniMail>();

		public List<uint> MissingProductIds = new List<uint>();

		public OrderRules Settings;
		public ConcurrentQueue<string> FilesForArchive = new ConcurrentQueue<string>();
		public DateTime CurrentUpdateTime;
		public bool ImpersonalPriceFresh;
		public bool InstallNet;

		//для тестов
		public UpdateData()
		{
		}

		public UpdateData(DataSet data, OrderRules settings)
		{
			_currentTempFileName = DateTime.Now.ToString("yyyyMMddHHmmssfff");
			CurrentUpdateTime = DateTime.Now;

			PreviousRequest = new UncommittedRequest();
			if (data.Tables.Count < 2)
				throw new Exception("Не выбрана таблица с предыдущими обновлениями");
			if (data.Tables[1].Rows.Count > 0) {
				var previousRequest = data.Tables[1].Rows[0];
				PreviousRequest.UpdateId = Convert.ToUInt32(previousRequest["UpdateId"]);
				PreviousRequest.RequestTime = Convert.ToDateTime(previousRequest["RequestTime"]);
				PreviousRequest.RequestType = (RequestType)Convert.ToInt32(previousRequest["UpdateType"]);
				PreviousRequest.Commit = Convert.ToBoolean(previousRequest["Commit"]);
				PreviousRequest.Addition = previousRequest["Addition"].ToString();
			}

			var row = data.Tables[0].Rows[0];
			RegionMask = Convert.ToUInt64(row["RegionMask"]);
			ClientId = Convert.ToUInt32(row["ClientId"]);
			UserId = Convert.ToUInt32(row["UserId"]);
			Message = Convert.ToString(row["Message"]).Trim();
			CheckCopyId = Convert.ToBoolean(row["CheckCopyId"]);
			if (!(row["UpdateDate"] is DBNull))
				OldUpdateTime = Convert.ToDateTime(row["UpdateDate"]);
			if (!(row["UncommitedUpdateDate"] is DBNull))
				UncommitedUpdateTime = Convert.ToDateTime(row["UncommitedUpdateDate"]);
			if (!(row["BuyingMatrixUpdateTime"] is DBNull))
				BuyingMatrixUpdateTime = Convert.ToDateTime(row["BuyingMatrixUpdateTime"]);
			if (!(row["OfferMatrixUpdateTime"] is DBNull))
				OfferMatrixUpdateTime = Convert.ToDateTime(row["OfferMatrixUpdateTime"]);
			ShortName = Convert.ToString(row["ShortName"]);
			Spy = Convert.ToBoolean(row["Spy"]);
			SpyAccount = Convert.ToBoolean(row["SpyAccount"]);
			ClientEnabled = Convert.ToBoolean(row["ClientEnabled"]);
			UserEnabled = Convert.ToBoolean(row["UserEnabled"]);
			AFPermissionExists = Convert.ToBoolean(row["AFPermissionExists"]);
			EnableImpersonalPrice = Convert.ToBoolean(row["EnableImpersonalPrice"]);
			KnownUniqueID = row["KnownUniqueID"].ToString();
			KnownBuildNumber = Convert.IsDBNull(row["KnownBuildNumber"]) ? null : (uint?)Convert.ToUInt32(row["KnownBuildNumber"]);
			TargetVersion = Convert.IsDBNull(row["TargetVersion"]) ? null : (uint?)Convert.ToUInt32(row["TargetVersion"]);
			NetworkPriceId = Convert.IsDBNull(row["NetworkPriceId"])
				? null
				: (uint?)Convert.ToUInt32(row["NetworkPriceId"]);
			SaveAFDataFiles = Convert.ToBoolean(row["SaveAFDataFiles"]);
			ShowAdvertising = Convert.ToBoolean(row["ShowAdvertising"]);
			AllowDownloadUnconfirmedOrders = Convert.ToBoolean(row["AllowDownloadUnconfirmedOrders"]);
			AllowAnalitFSchedule = Convert.ToBoolean(row["AllowAnalitFSchedule"]);

			SendWaybills = Convert.ToBoolean(row["SendWaybills"]);
			SendRejects = Convert.ToBoolean(row["SendRejects"]);
			InstallNet = Convert.ToBoolean(row["InstallNet"]);
			Settings = settings;
		}

		public bool IsUpdateToNet(RequestType type)
		{
			if (!(type == RequestType.GetCumulative || type == RequestType.GetCumulativeAsync
				|| type == RequestType.GetData || type == RequestType.GetDataAsync
				|| type == RequestType.GetLimitedCumulative || type == RequestType.GetLimitedCumulativeAsync))
				return false;

			if (!EnableUpdate())
				return false;

			return UpdateExeVersionInfo.IsNet;
		}

		public bool Disabled()
		{
			return !ClientEnabled || !UserEnabled || !AFPermissionExists;
		}

		public string DisabledMessage()
		{
			if (!AFPermissionExists)
				return "пользователю не разрешено обновлять AnalitF";
			if (!UserEnabled)
				return "пользователь отключен";
			if (!ClientEnabled)
				return "клиент отключен";
			return null;
		}

		public bool BillingDisabled()
		{
			return !UserEnabled || !ClientEnabled;
		}

		public string[] GetUpdateFiles()
		{
			if (EnableUpdate())
				return GetUpdateFiles(UpdateExeVersionInfo.ExeFolder(), String.Empty);

			return new string[] { };
		}

		private string[] GetUpdateFiles(string path, string sufix)
		{
			path += sufix;
			if (Directory.Exists(path))
				return Directory.GetFiles(path);
			return new string[0];
		}

		public void ParseBuildNumber(string exeVersion)
		{
			var numbers = exeVersion.Split('.');
			if (numbers.Length > 0 && !String.IsNullOrEmpty(numbers[numbers.Length - 1])) {
				uint buildNumber;
				if (uint.TryParse(numbers[numbers.Length - 1], out buildNumber)) {
					BuildNumber = buildNumber;
					if (!NetworkPriceId.HasValue)
						CheckBuildNumber();
					UpdateExeVersionInfo = GetUpdateVersionInfo();
					NeedUpdateToBuyingMatrix = CheckNeedUpdateToBuyingMatrix();
					NeedUpdateToNewMNN = CheckNeedUpdateToNewMNN();
					NeedUpdateToNewClientsWithLegalEntity = CheckNeedUpdateToNewClientsWithLegalEntity();
					NeedUpdateToSupplierPromotions = CheckNeedUpdateToSupplierPromotions();
					return;
				}
			}

			throw new UpdateException(
				"Пожалуйста, повторите попытку через несколько минут.",
				"При выполнении Вашего запроса произошла ошибка.",
				"Ошибка при разборе номера версии '" + exeVersion + "'; ",
				RequestType.Error);
		}

		private void CheckBuildNumber()
		{
			if (KnownBuildNumber.HasValue && BuildNumber < KnownBuildNumber)
				throw new UpdateException("Доступ закрыт.",
					"Используемая версия программы не актуальна, необходимо обновление до версии №" + KnownBuildNumber + ".[5]",
					"Попытка обновить устаревшую версию; ",
					RequestType.Forbidden);
		}

		public bool NeedUpdateTo945()
		{
			return (BuildNumber == 945 || (BuildNumber >= 829 && BuildNumber <= 837)) && EnableUpdate();
		}

		private bool CheckNeedUpdateToBuyingMatrix()
		{
			if ((BuyingMatrixUpdateTime != null && BuyingMatrixUpdateTime.Value > OldUpdateTime) ||
				(OfferMatrixUpdateTime != null && OfferMatrixUpdateTime.Value > OldUpdateTime))
				return true;

			if (UpdateExeVersionInfo != null && BuildNumber >= 1183 && BuildNumber <= 1229)
				return UpdateExeVersionInfo.VersionNumber >= 1249;

			return false;
		}

		private bool CheckNeedUpdateToNewMNN()
		{
			if (UpdateExeVersionInfo != null && BuildNumber >= 1183 && BuildNumber <= 1263)
				return UpdateExeVersionInfo.VersionNumber > 1263;

			return false;
		}

		private bool CheckNeedUpdateToNewClientsWithLegalEntity()
		{
			if (UpdateExeVersionInfo != null && BuildNumber <= 1271)
				return UpdateExeVersionInfo.VersionNumber > 1271;

			return false;
		}

		private bool BuildNumberGreaterThen(int version)
		{
			//Версия удовлетворяем, если разобранная версия BuildNumber больше необходимой или разобранная версия BuildNumber
			//не установлена и предыдущая версия KnownBuildNumber (версия программы, с которой пользователь работал в предыдущий раз) больше необходимой
			return BuildNumber > version || ((!BuildNumber.HasValue || BuildNumber == 0) && KnownBuildNumber > version);
		}

		public bool IsConfirmUserMessage()
		{
			return BuildNumberGreaterThen(_versionBeforeConfirmUserMessage);
		}

		public bool AllowSupplierPromotions()
		{
			return BuildNumberGreaterThen(_versionBeforeSupplierPromotions);
		}

		private bool CheckNeedUpdateToSupplierPromotions()
		{
			if (UpdateExeVersionInfo != null && BuildNumber <= _versionBeforeSupplierPromotions)
				return UpdateExeVersionInfo.VersionNumber > _versionBeforeSupplierPromotions;

			return false;
		}

		public bool AllowDelayWithVitallyImportant()
		{
			return BuildNumberGreaterThen(_versionBeforeDelayWithVitallyImportant)
				|| (UpdateExeVersionInfo != null
					&& UpdateExeVersionInfo.VersionNumber > _versionBeforeDelayWithVitallyImportant
					&& UpdateExeVersionInfo.VersionNumber <= _versionBeforeDelayByPrice);
		}

		public bool AllowDelayByPrice()
		{
			return BuildNumberGreaterThen(_versionBeforeDelayByPrice)
				|| (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeDelayByPrice);
		}

		public bool NeedUpdateForRetailVitallyImportant()
		{
			return (BuildNumber <= _versionBeforeDelayByPrice)
				&& (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeDelayByPrice);
		}

		private VersionInfo GetUpdateVersionInfo()
		{
			if (!AllowUpdate())
				return null;

			return VersionUpdaterFactory.GetUpdater().GetVersionInfo(BuildNumber.Value, TargetVersion);
		}

		private void CheckResultPath()
		{
			if (String.IsNullOrEmpty(ResultPath))
				throw new Exception("Не установлено свойство ResultPath");
		}

		public string GetReclameFile()
		{
			CheckResultPath();
			return String.Format("{0}r{1}.zip", ResultPath, UserId);
		}

		public string GetOrdersFile()
		{
			CheckResultPath();
			return String.Format("{0}Orders{1}.zip", ResultPath, UserId);
		}

		public string GetPreviousFile()
		{
			CheckResultPath();
			if (PreviousRequest.UpdateId == null)
				throw new Exception("Отсутствует предыдущее неподтвержденное обновление");
			return String.Format("{0}{1}_{2}.zip", ResultPath, UserId, PreviousRequest.UpdateId);
		}

		public string GetCurrentTempFile()
		{
			CheckResultPath();
			return String.Format("{0}{1}_{2}.zip", ResultPath, UserId, _currentTempFileName);
		}

		public string GetCurrentFile(uint updateId)
		{
			CheckResultPath();
			return String.Format("{0}{1}_{2}.zip", ResultPath, UserId, updateId);
		}

		public string GetOldFileMask()
		{
			return String.Format("{0}_*.zip", UserId);
		}

		private bool AllowUpdate()
		{
			return BuildNumber.HasValue && (!TargetVersion.HasValue || BuildNumber < TargetVersion);
		}

		public bool EnableUpdate()
		{
			return AllowUpdate() && UpdateExeVersionInfo != null && (!TargetVersion.HasValue || UpdateExeVersionInfo.VersionNumber <= TargetVersion);
		}

		public bool SupportDownloadUnconfirmedOrders
		{
			get { return BuildNumberGreaterThen(_versionBeforeDownloadUnconfirmedOrders); }
		}

		public bool AllowDeleteUnconfirmedOrders
		{
			get { return AllowDownloadUnconfirmedOrders && SupportDownloadUnconfirmedOrders; }
		}

		public bool NeedDownloadUnconfirmedOrders
		{
			get { return AllowDeleteUnconfirmedOrders && MaxOrderId > 0 && MaxOrderListId > 0; }
		}

		public void FillDocumentBodyIds(uint[] documentBodyIds)
		{
			if (documentBodyIds.Length > 50)
				throw new UpdateException(
					"Количество запрашиваемых сертификатов превышает 50 штук.",
					"Пожалуйста, измените список запрашиваемых сертификатов.",
					"Количество запрашиваемых сертификатов превышает 50 штук; ", RequestType.Forbidden);

			foreach (var documentBodyId in documentBodyIds) {
				var request = new CertificateRequest {
					DocumentBodyId = documentBodyId
				};
				CertificateRequests.Add(request);
			}
		}

		public bool NeedExportCertificates
		{
			get { return CertificateRequests.Count > 0; }
		}

		public string GetCertificatesResult()
		{
			var builder = new StringBuilder();

			foreach (var certificateRequest in CertificateRequests) {
				builder.AppendFormat(
					"{0}\t{1}\n",
					certificateRequest.DocumentBodyId,
					certificateRequest.CertificateId.HasValue && certificateRequest.CertificateFiles.Count > 0
						? certificateRequest.CertificateId.ToString()
						: "\\N");
			}

			return builder.ToString();
		}

		public bool AllowInvoiceHeaders
		{
			get { return CheckVersion(_versionBeforeInvoiceHeaders); }
		}

		public bool AllowShowSupplierCost
		{
			get { return CheckVersion(_versionBeforeShowSupplierCost); }
		}

		public bool SupportAnalitFSchedule
		{
			get { return CheckVersion(_versionBeforeAnalitFSchedule); }
		}

		public bool AllowExportSendDate
		{
			get { return CheckVersion(_versionBeforeExportSendDate); }
		}

		public bool AllowCertificates
		{
			get { return CheckVersion(_versionBeforeCertificates); }
		}

		public bool AllowRetailMargins
		{
			get { return CheckVersion(_versionBeforeRetailMargins); }
		}

		public bool AllowExcessAvgOrderTimes
		{
			get { return CheckVersion(_versionBeforeExcessAvgOrderTimes); }
		}

		public bool AllowHistoryDocs
		{
			get { return CheckVersion(_versionBeforeHistoryDocs); }
		}

		public bool CheckVersion(int requiredVersion)
		{
			//requiredVersion = -1 - выдают модели экспорта если у них нет требований к версии
			return requiredVersion < 0
				|| BuildNumberGreaterThen(requiredVersion)
				|| (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > requiredVersion);
		}

		public bool NeedUpdateForHistoryDocs()
		{
			return (BuildNumber <= _versionBeforeHistoryDocs)
				&& (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeHistoryDocs);
		}

		public bool AllowMatchWaybillsToOrders()
		{
			return BuildNumberGreaterThen(_versionBeforeMatchWaybillsToOrders)
				|| (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeMatchWaybillsToOrders);
		}

		public bool NeedUpdateForMatchWaybillsToOrders()
		{
			return (BuildNumber <= _versionBeforeMatchWaybillsToOrders)
				&& (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeMatchWaybillsToOrders);
		}

		public bool AllowCorrectTechContact()
		{
			return BuildNumberGreaterThen(_versionBeforeCorrectTechContact)
				|| (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeCorrectTechContact);
		}

		public bool AllowEAN13()
		{
			return BuildNumberGreaterThen(_versionBeforeEAN13)
				|| (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeEAN13);
		}

		public bool NeedUpdateForEAN13()
		{
			return (BuildNumber <= _versionBeforeEAN13)
				&& (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeEAN13);
		}

		public bool AllowAfter1883()
		{
			return BuildNumberGreaterThen(_version1883)
				|| (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _version1883);
		}

		public bool NeedUpdateForRetailMargins()
		{
			return (BuildNumber <= _versionBeforeRetailMargins)
				&& (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeRetailMargins);
		}

		public bool SupportedMinReordering()
		{
			return BuildNumberGreaterThen(_versionBeforeMinReordering)
				|| (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeMinReordering);
		}

		public bool SupportExportExp()
		{
			return BuildNumberGreaterThen(_versionBeforeExportExp)
				|| (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeExportExp);
		}

		public bool AllowDocumentType(int documentType)
		{
			if ((GenerateDocsHelper.DocumentType)documentType == GenerateDocsHelper.DocumentType.Waybills && !SendWaybills)
				return false;
			if ((GenerateDocsHelper.DocumentType)documentType == GenerateDocsHelper.DocumentType.Rejects && !SendRejects)
				return false;
			return true;
		}

		public bool NeedExportAttachments()
		{
			return AttachmentRequests.Count > 0;
		}

		public void FillAttachmentIds(uint[] attachmentIds)
		{
			if (attachmentIds == null)
				return;

			foreach (var attachmentId in attachmentIds.Where(id => id != 0)) {
				var request = new AttachmentRequest {
					AttachmentId = attachmentId
				};
				AttachmentRequests.Add(request);
			}
		}

		public bool SuccesAttachmentsExists()
		{
			return AttachmentRequests.Exists(r => r.Success);
		}

		public List<uint> SuccesAttachmentIds()
		{
			return AttachmentRequests.Where(r => r.Success).Select(r => r.AttachmentId).ToList();
		}

		public string AttachmentFailMessage()
		{
			var ids = AttachmentRequests.Where(a => !a.Success).Implode(a => a.AttachmentId);
			return String.Format("Запрошенные вложения не найдены {0}", ids);
		}

		public string GetAttachmentsResult()
		{
			return SuccesAttachmentIds().Implode("\n");
		}

		public void ParseMissingProductIds(uint[] missingProductIds)
		{
			MissingProductIds = (missingProductIds ?? new uint[0]).Distinct().Where(i => i > 0).ToList();
		}

		public string LoadNetUpdate(string username, string password)
		{
			var url = ConfigurationManager.AppSettings["NetUpdateUrl"];
			using (var cleaner = new FileCleaner())
			using (var handler = new HttpClientHandler())
			using (var client = new HttpClient(handler))
			{
				handler.PreAuthenticate = true;
				handler.Credentials = new NetworkCredential(username, password);
				var response = client.GetAsync(url).Result;
				while (response.StatusCode == HttpStatusCode.Accepted)
				{
					Thread.Sleep(3.Second());
					response = client.GetAsync(url).Result;
				}

				if (response.StatusCode != HttpStatusCode.OK)
				{
					throw new Exception(String.Format("Не удалось получить обновление {0}", response));
				}

				var stream = response.Content.ReadAsStreamAsync().Result;
				var file = cleaner.TmpFile();
				using (var tmpStream = File.OpenWrite(file))
					stream.CopyTo(tmpStream);

				var dir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName(), "update");
				Directory.CreateDirectory(dir);
				cleaner.WatchDir(dir);
				new FastZip().ExtractZip(file, dir, null);

				var binDir = Path.GetDirectoryName(GetUpdateFiles()[0]);
				var tmpArchive = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
				ProcessHelper.CmdDir("\"" + @"C:\Program Files\7-Zip\7z.exe" + "\"" + " a \"" + tmpArchive + "\"  \"" + binDir + "\" " + " -mx7 -bd -slp -mmt=6 -w" + Path.GetTempPath(),
					timeout: TimeSpan.FromMinutes(15));
				ProcessHelper.CmdDir("\"" + @"C:\Program Files\7-Zip\7z.exe" + "\"" + " a \"" + tmpArchive + "\"  \"" + dir + "\" " + " -mx7 -bd -slp -mmt=6 -w" + Path.GetTempPath(),
					timeout: TimeSpan.FromMinutes(15));
				return tmpArchive;
			}
		}
	}
}
