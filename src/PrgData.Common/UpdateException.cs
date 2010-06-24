using System;

namespace PrgData.Common
{
	public class UpdateException : Exception
	{
		public UpdateException(string description, string error, string addition, RequestType requestType)
			: base(description)
		{
			Description = description;
			Error = error;
			UpdateType = requestType;
			Addition = addition;
		}

		public UpdateException(string description, string error, RequestType requestType)
			: this(description, error, "", requestType)
		{}

		public RequestType UpdateType { get; private set; }
		public string Error { get; private set; }
		public string Description { get; private set; }
		public string Addition { get; private set; }

		public string GetAnalitFMessage()
		{
			return String.Format("Error={0};Desc={1}", Error, Description);
		}
	}
}