using System;

namespace PrgData.Common
{
	public class UpdateException : Exception
	{
		public UpdateException(string description, string error, string addition, RequestType requestType)
			: base(description)
		{
			Error = error;
			UpdateType = requestType;
			Addition = addition;
		}

		public UpdateException(string description, string error, RequestType requestType)
			: this(description, error, description, requestType)
		{
		}

		public RequestType UpdateType { get; private set; }
		public string Error { get; private set; }
		public string Addition { get; private set; }

		public string GetAnalitFMessage()
		{
			return String.Format("Error={0};Desc={1}", Error, Message);
		}

		public override string ToString()
		{
			return
				String.Format(
					"Error = {0}\r\nUpdateType = {1}\r\nAddition = {2}\r\n{3}",
					Error,
					UpdateType,
					Addition,
					base.ToString());
		}
	}
}