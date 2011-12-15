using System;
using System.Collections.Generic;
using System.IO;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Common.Tools;
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
	}

	public class AttachmentRequest
	{
		public uint AttachmentId;
		public bool Success;
	}

	public class UpdateData
	{
		private static int _versionBeforeConfirmUserMessage = 1299;
		private static int _versionBeforeSupplierPromotions = 1363;
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

		public string ShortName;
		public uint ClientId;
		public uint UserId;
		public string UserName;
		public bool CheckCopyId;
		public string Message;
		public DateTime OldUpdateTime;
		public DateTime UncommitedUpdateTime;

		public uint? OffersClientCode;
		public ulong? OffersRegionCode;
		public bool EnableImpersonalPrice;
		public uint ImpersonalPriceId = 2647;

		public bool IsFutureClient;

		public bool Spy;
		public bool SpyAccount;

		public bool ClientEnabled;
		public bool UserEnabled;
		public bool AFPermissionExists;

		public uint? BuyingMatrixPriceId;
		public int BuyingMatrixType;
		public bool WarningOnBuyingMatrix;

		public uint? OfferMatrixPriceId;
		public int OfferMatrixType;

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

		public UncommittedRequest PreviousRequest;

		public bool AllowDownloadUnconfirmedOrders;

		public bool AllowAnalitFSchedule;

		public string ResultPath;
		private string _currentTempFileName;

		public uint MaxOrderId;
		public uint MaxOrderListId;

		public List<SupplierPromotion> SupplierPromotions = new List<SupplierPromotion>();

		public List<uint> UnconfirmedOrders = new List<uint>();

		public bool AsyncRequest;

		public List<CertificateRequest> CertificateRequests = new List<CertificateRequest>();

		public bool SendWaybills;
		public bool SendRejects;

		public uint LastLockId;

		public List<AttachmentRequest> AttachmentRequests = new List<AttachmentRequest>();
		public List<uint> ExportMails = new List<uint>();

		public UpdateData(DataSet data)
		{
			_currentTempFileName = DateTime.Now.ToString("yyyyMMddHHmmssfff");

			PreviousRequest = new UncommittedRequest();
			if (data.Tables.Count < 2)
				throw new Exception("Не выбрана таблица с предыдущими обновлениями");
			if (data.Tables[1].Rows.Count > 0)
			{
				var previousRequest = data.Tables[1].Rows[0];
				PreviousRequest.UpdateId = Convert.ToUInt32(previousRequest["UpdateId"]);
				PreviousRequest.RequestTime = Convert.ToDateTime(previousRequest["RequestTime"]);
				PreviousRequest.RequestType = (RequestType)Convert.ToInt32(previousRequest["UpdateType"]);
				PreviousRequest.Commit = Convert.ToBoolean(previousRequest["Commit"]);
				PreviousRequest.Addition = previousRequest["Addition"].ToString();
			}

			var row = data.Tables[0].Rows[0];
			ClientId = Convert.ToUInt32(row["ClientId"]);
			UserId = Convert.ToUInt32(row["UserId"]);
			Message = Convert.ToString(row["Message"]).Trim();
			CheckCopyId = Convert.ToBoolean(row["CheckCopyId"]);
			if (!(row["UpdateDate"] is DBNull))
				OldUpdateTime = Convert.ToDateTime(row["UpdateDate"]);
			if (!(row["UncommitedUpdateDate"] is DBNull))
				UncommitedUpdateTime = Convert.ToDateTime(row["UncommitedUpdateDate"]);
			if (data.Tables[0].Columns.Contains("Future"))
				IsFutureClient = true;
			ShortName = Convert.ToString(row["ShortName"]);
			Spy = Convert.ToBoolean(row["Spy"]);
			SpyAccount = Convert.ToBoolean(row["SpyAccount"]);
			ClientEnabled = Convert.ToBoolean(row["ClientEnabled"]);
			UserEnabled = Convert.ToBoolean(row["UserEnabled"]);
			AFPermissionExists = Convert.ToBoolean(row["AFPermissionExists"]);
			BuyingMatrixPriceId = Convert.IsDBNull(row["BuyingMatrixPriceId"])
									? null
									: (uint?)Convert.ToUInt32(row["BuyingMatrixPriceId"]);
			BuyingMatrixType = Convert.ToInt32(row["BuyingMatrixType"]);
			WarningOnBuyingMatrix = Convert.ToBoolean(row["WarningOnBuyingMatrix"]);
			EnableImpersonalPrice = Convert.ToBoolean(row["EnableImpersonalPrice"]);
			KnownUniqueID = row["KnownUniqueID"].ToString();
			KnownBuildNumber = Convert.IsDBNull(row["KnownBuildNumber"]) ? null : (uint?)Convert.ToUInt32(row["KnownBuildNumber"]);
			TargetVersion = Convert.IsDBNull(row["TargetVersion"]) ? null : (uint?)Convert.ToUInt32(row["TargetVersion"]);
			NetworkPriceId = Convert.IsDBNull(row["NetworkPriceId"])
			                    	? null
			                    	: (uint?) Convert.ToUInt32(row["NetworkPriceId"]);
			SaveAFDataFiles = Convert.ToBoolean(row["SaveAFDataFiles"]);
			ShowAdvertising = Convert.ToBoolean(row["ShowAdvertising"]);
			OfferMatrixPriceId = Convert.IsDBNull(row["OfferMatrixPriceId"])
									? null
									: (uint?)Convert.ToUInt32(row["OfferMatrixPriceId"]);
			OfferMatrixType = Convert.ToInt32(row["OfferMatrixType"]);
			AllowDownloadUnconfirmedOrders = Convert.ToBoolean(row["AllowDownloadUnconfirmedOrders"]);
			AllowAnalitFSchedule = Convert.ToBoolean(row["AllowAnalitFSchedule"]);

			SendWaybills = Convert.ToBoolean(row["SendWaybills"]);
			SendRejects = Convert.ToBoolean(row["SendRejects"]);
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

			return new string[]{};
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
			if (numbers.Length > 0 && !String.IsNullOrEmpty(numbers[numbers.Length-1]))
			{
				uint buildNumber;
				if (uint.TryParse(numbers[numbers.Length - 1], out buildNumber))
				{
					BuildNumber = buildNumber;
					if (!NetworkPriceId.HasValue)
						CheckBuildNumber();
					UpdateExeVersionInfo = GetUpdateVersionInfo();
					NeedUpdateToBuyingMatrix = CheckNeedUpdateToBuyingMatrix();
					NeedUpdateToNewMNN = CheckNeedUpdateToNewMNN();
					//NeedUpdateToCryptCost = CheckNeedUpdateToCryptCost();
					NeedUpdateToNewClientsWithLegalEntity = CheckNeedUpdateToNewClientsWithLegalEntity();
					NeedUpdateToSupplierPromotions = CheckNeedUpdateToSupplierPromotions();
				}
			}
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

		private bool CheckNeedUpdateToCryptCost()
		{
			throw new NotImplementedException();
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
			get
			{
				return BuildNumberGreaterThen(_versionBeforeDownloadUnconfirmedOrders);
			}
		}

		public bool AllowDeleteUnconfirmedOrders
		{
			get
			{
				return AllowDownloadUnconfirmedOrders && SupportDownloadUnconfirmedOrders;
			}
		}

		public bool NeedDownloadUnconfirmedOrders
		{
			get
			{
				return AllowDeleteUnconfirmedOrders && MaxOrderId > 0 && MaxOrderListId > 0;
			}
		}

		public bool AllowInvoiceHeaders()
		{
			return BuildNumberGreaterThen(_versionBeforeInvoiceHeaders)
				|| (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeInvoiceHeaders);
		}

		public bool AllowShowSupplierCost()
		{
			return BuildNumberGreaterThen(_versionBeforeShowSupplierCost)
				|| (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeShowSupplierCost);
		}

		public bool SupportAnalitFSchedule
		{
			get
			{
				return BuildNumberGreaterThen(_versionBeforeAnalitFSchedule)
					   || (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeAnalitFSchedule);
			}
		}

		public bool AllowExportSendDate
		{
			get
			{
				return BuildNumberGreaterThen(_versionBeforeExportSendDate)
					   || (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeExportSendDate);
			}
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

		public bool NeedExportCertificates{ 
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

		public bool AllowCertificates()
		{
			return BuildNumberGreaterThen(_versionBeforeCertificates)
				|| (UpdateExeVersionInfo != null && UpdateExeVersionInfo.VersionNumber > _versionBeforeCertificates);
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
			//if (attachmentIds.Length > 50)
			//    throw new UpdateException(
			//        "Количество запрашиваемых сертификатов превышает 50 штук.",
			//        "Пожалуйста, измените список запрашиваемых сертификатов.", 
			//        "Количество запрашиваемых сертификатов превышает 50 штук; ", RequestType.Forbidden);

			foreach (var attachmentId in attachmentIds) {
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

		public string GetAttachmentsResult()
		{
			return SuccesAttachmentIds().Implode("\n");
		}

	}
}