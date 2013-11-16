using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.IO;
using Castle.ActiveRecord;
using Common.Tools;
using Inforoom.Common;
using NUnit.Framework;
using PrgData.Common;
using PrgData.FileHandlers;
using Test.Support;
using PrgData;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;
using System.Data;
using NHibernate.Criterion;
using System.Web;

namespace Integration
{
	[TestFixture]
	public class FileHandlerFixture : BaseFileHandlerFixture
	{
		private TestUser user;

		[SetUp]
		public void Setup()
		{
			FileHanderAshxName = "GetFileHandler.ashx";

			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ServiceContext.GetResultPath = () => "results\\";

			user = CreateUser();
		}


		private void CheckProcessRequest(string login, string errorMessage)
		{
			SetCurrentUser(login);

			WithHttpContext(context => {
				var fileHandler = new GetFileHandler();
				fileHandler.ProcessRequest(context);

				Assert.That(context.Response.StatusCode, Is.EqualTo(500), "Не верный код ошибки от сервера");

				Assert.That(context.Error, Is.Not.Null);
				Assert.That(context.Error.GetType(), Is.EqualTo(typeof(Exception)));
				Assert.That(context.Error.Message, Is.StringStarting(errorMessage).IgnoreCase);
			});
		}

		[Test(Description = "Пытаемся вызвать GetFileHandler для несуществующего клиента, должны получить исключение")]
		public void Check_nonExists_user()
		{
			CheckProcessRequest("dsdsdsdsds", "Не удалось идентифицировать клиента.");
		}

		[Test(Description = "Пытаемся вызвать GetFileHandler для клиента с неподготовленным файлом данных, должны получить исключение")]
		public void Check_nonExists_file()
		{
			CheckProcessRequest(user.Login, "При вызове GetFileHandler не найден файл с подготовленными данными:");
		}
	}
}