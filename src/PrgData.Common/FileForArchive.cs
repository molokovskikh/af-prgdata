namespace PrgData.Common
{
	public class FileForArchive
	{
		public string FileName;
		public bool FileType;

		public FileForArchive(string fileName, bool fileType)
		{
			FileName = fileName;
			FileType = fileType;
		}
	}
}