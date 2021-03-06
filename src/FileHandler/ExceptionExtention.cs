﻿using System.Runtime.InteropServices;
using System.Web;

namespace PrgData.FileHandlers
{
	public static class ExceptionExtention
	{
		public static bool IsWellKnownException(this ExternalException exception)
		{
			//-2147024775 Message: Удаленный хост разорвал соединение. Код ошибки: 0x80070079
			if (exception.ErrorCode == -2147024775)
				return true;
			//-2147024832 Message: Удаленный хост разорвал соединение. Код ошибки: 0x80070040.
			if (exception.ErrorCode == -2147024832)
				return true;
			if (exception.ErrorCode == -2147014842)
				return true;
			//-2147023901 - (0x800703E3): Удаленный хост разорвал соединение.
			if (exception.ErrorCode == -2147023901)
				return true;
			if (exception.ErrorCode == -2147467259)
				return true;
			return false;
		}
	}
}