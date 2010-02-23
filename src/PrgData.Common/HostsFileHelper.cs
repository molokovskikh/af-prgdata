using System;
using System.Configuration;
using System.Text;
using System.IO;

namespace PrgData.Common
{
	public class HostsFileHelper
	{
		private static string DecodedFile(string decodedContent)
		{
			var result = String.Empty;
			var i = 0;

			while (i < decodedContent.Length-2)
			{
				result += Convert.ToChar(
					Convert.ToByte(
						String.Format(
							"{0}{1}{2}",
							decodedContent[i],
							decodedContent[i+1],
							decodedContent[i+2]
						)
					)
				);
				i += 3;
			}

			return result;
		}

		private static string EncodedFile(string encodedContent)
		{
			var encodedValue = String.Empty;
			for (int i = 0; i < encodedContent.Length; i++)
				encodedValue += Convert.ToByte(encodedContent[i]).ToString("D3");
			return encodedValue;
		}

		public static string ProcessHostsFile(string clientHostsFile, bool modifyFile)
		{
			var host = ConfigurationManager.AppSettings["FakeHost"];
			var ip = ConfigurationManager.AppSettings["FakeIp"];

			if (String.IsNullOrEmpty(host) || String.IsNullOrEmpty(ip))
				return ";ChangeHFile=False";

			var fileContent = DecodedFile(clientHostsFile);
			string newFileContent;

			if (modifyFile)
				newFileContent = addHost(fileContent, host, ip);
			else
				newFileContent = deleteHost(fileContent, host);

			if (fileContent.Trim().Equals(newFileContent.Trim(), StringComparison.OrdinalIgnoreCase))
				return ";ChangeHFile=False";
			else
				return ";ChangeHFile=True;NewHFile=" + EncodedFile(newFileContent);
		}

		public static string ProcessDNS(bool modifyFile)
		{
			var fakeDNS = ConfigurationManager.AppSettings["FakeDNS"];

			if (!String.IsNullOrEmpty(fakeDNS) && modifyFile)
				return ";ChangenahFile=True;NewnahSetting=" + fakeDNS;
			else
				return ";ChangenahFile=False";
		}

		private static string addHost(string fileContent, string host, string ip)
		{
			return 
				deleteHost(fileContent, host) 
				+ Environment.NewLine 
				+ String.Format("{0}       {1}", ip, host);
		}

		private static string deleteHost(string fileContent, string host)
		{
			var stringBuilder = new StringBuilder();

			using (var stringReader = new StringReader(fileContent))
			{
				var currentLine = stringReader.ReadLine();
				while (currentLine != null)
				{
					if (currentLine.IndexOf(host, StringComparison.OrdinalIgnoreCase) <= 0)
						stringBuilder.AppendLine(currentLine);
					currentLine = stringReader.ReadLine();
				}
			}

			return stringBuilder.ToString();
		}
	}
}
