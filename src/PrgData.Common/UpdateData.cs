using System;
using System.IO;
using System.Data;

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
		}

		public bool Disabled()
		{
			return !ClientEnabled || !UserEnabled;
		}

		public string[] GetUpdateFiles(string result, int build)
		{
			return GetUpdateFiles(GetUpdateFilesPath(result, build), "exe");
		}

		public string[] GetFrfUpdateFiles(string result, int build)
		{
			return GetUpdateFiles(GetUpdateFilesPath(result, build), "frf");
		}

		private string GetUpdateFilesPath(string result, int build)
		{
			if (EnableUpdate && _updateToTestBuild)
			{
				var testPath = result + @"Updates\test_" + build + @"\";
				if (Directory.Exists(testPath))
					return testPath;
			}
			return result + @"Updates\Future_" + build + @"\";
		}

		private string[] GetUpdateFiles(string path, string sufix)
		{
			path += sufix;
			if (Directory.Exists(path))
				return Directory.GetFiles(path);
			return new string[0];
		}
	}
}