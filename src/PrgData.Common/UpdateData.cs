using System;
using System.IO;
using System.Data;
using System.Diagnostics;

namespace PrgData.Common
{
	public class UpdateData
	{
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

		public bool EnableUpdate;
		private readonly bool _updateToTestBuild;

		public bool ClientEnabled;
		public bool UserEnabled;

		public uint? BuyingMatrixPriceId;
		public int BuyingMatrixType;
		public bool WarningOnBuyingMatrix;

		public int? BuildNumber;
		public int? KnownBuildNumber;

		public string UniqueID;
		public string KnownUniqueID;

		public bool NeedUpdateToBuyingMatrix { get; private set; }

		public bool NeedUpdateToNewMNN { get; private set; }

		public string ClientHost;

		public string CostSessionKey;

		public bool NeedUpdateToCryptCost { get; private set; }

		public bool NeedUpdateToNewClientsWithLegalEntity { get; private set; }

		public FileVersionInfo UpdateExeVersionInfo { get; private set; }

		public uint? NetworkSupplierId;

		public UpdateData(DataSet data)
		{
			var row = data.Tables[0].Rows[0];
			ClientId = Convert.ToUInt32(row["ClientId"]);
			UserId = Convert.ToUInt32(row["UserId"]);
			Message = Convert.ToString(row["Message"]);
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
			EnableUpdate = Convert.ToBoolean(row["EnableUpdate"]);
			_updateToTestBuild = Convert.ToBoolean(row["UpdateToTestBuild"]);
			ClientEnabled = Convert.ToBoolean(row["ClientEnabled"]);
			UserEnabled = Convert.ToBoolean(row["UserEnabled"]);
			BuyingMatrixPriceId = Convert.IsDBNull(row["BuyingMatrixPriceId"])
									? null
									: (uint?)Convert.ToUInt32(row["BuyingMatrixPriceId"]);
			BuyingMatrixType = Convert.ToInt32(row["BuyingMatrixType"]);
			WarningOnBuyingMatrix = Convert.ToBoolean(row["WarningOnBuyingMatrix"]);
			EnableImpersonalPrice = Convert.ToBoolean(row["EnableImpersonalPrice"]);
			KnownUniqueID = row["KnownUniqueID"].ToString();
			KnownBuildNumber = Convert.IsDBNull(row["KnownBuildNumber"]) ? null : (int?)Convert.ToInt32(row["KnownBuildNumber"]);
			NetworkSupplierId = Convert.IsDBNull(row["NetworkSupplierId"])
			                    	? null
			                    	: (uint?) Convert.ToUInt32(row["NetworkSupplierId"]);
		}

		public bool Disabled()
		{
			return !ClientEnabled || !UserEnabled;
		}

		public string[] GetUpdateFiles(string result)
		{
			return GetUpdateFiles(GetUpdateFilesPath(result), "exe");
		}

		public string[] GetFrfUpdateFiles(string result)
		{
			return GetUpdateFiles(GetUpdateFilesPath(result), "frf");
		}

		private string GetUpdateFilesPath(string result)
		{
			if (EnableUpdate && _updateToTestBuild && BuildNumber.HasValue)
			{
				var testPath = result + @"Updates\test_" + BuildNumber + @"\";
				if (Directory.Exists(testPath))
					return testPath;
			}
			return result + @"Updates\Future_" + (BuildNumber.HasValue ? BuildNumber.ToString() : "null") + @"\";
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
				int buildNumber;
				if (int.TryParse(numbers[numbers.Length - 1], out buildNumber))
				{
					BuildNumber = buildNumber;
					CheckBuildNumber();
					UpdateExeVersionInfo = GetUpdateVersionInfo();
					NeedUpdateToBuyingMatrix = CheckNeedUpdateToBuyingMatrix();
					NeedUpdateToNewMNN = CheckNeedUpdateToNewMNN();
					//NeedUpdateToCryptCost = CheckNeedUpdateToCryptCost();
					NeedUpdateToNewClientsWithLegalEntity = CheckNeedUpdateToNewClientsWithLegalEntity();
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
			return (BuildNumber == 945 || (BuildNumber >= 829 && BuildNumber <= 837)) && EnableUpdate;
		}

		private bool CheckNeedUpdateToBuyingMatrix()
		{
			if (UpdateExeVersionInfo != null && BuildNumber >= 1183 && BuildNumber <= 1229)
				return UpdateExeVersionInfo.FilePrivatePart >= 1249;

			return false;
		}

		private bool CheckNeedUpdateToNewMNN()
		{
			if (UpdateExeVersionInfo != null && BuildNumber >= 1183 && BuildNumber <= 1263)
				return UpdateExeVersionInfo.FilePrivatePart > 1263;

			return false;
		}

		private bool CheckNeedUpdateToCryptCost()
		{
			throw new NotImplementedException();
		}

		private bool CheckNeedUpdateToNewClientsWithLegalEntity()
		{
			if (UpdateExeVersionInfo != null && BuildNumber <= 1271)
				return UpdateExeVersionInfo.FilePrivatePart > 1271;

			return false;
		}

		private FileVersionInfo GetUpdateVersionInfo()
		{
			if (!EnableUpdate)
				return null;

			try
			{
				var exeName = Array.Find(GetUpdateFiles(ServiceContext.GetResultPath()), item => item.EndsWith("AnalitF.exe", StringComparison.OrdinalIgnoreCase));
				return FileVersionInfo.GetVersionInfo(exeName);
			}
			catch
			{
				return null;
			}
		}

	}
}