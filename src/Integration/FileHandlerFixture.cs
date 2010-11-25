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
using Test.Support;
using PrgData;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;
using System.Data;
using NHibernate.Criterion;
using FileHandler;
using System.Web;

namespace Integration
{
	[TestFixture]
	public class FileHandlerFixture
	{
		private TestClient client;
		private TestUser user;

		[SetUp]
		public void Setup()
		{
			Test.Support.Setup.Initialize();
			ServiceContext.GetUserHost = () => "127.0.0.1";
			UpdateHelper.GetDownloadUrl = () => "http://localhost/";
			ServiceContext.GetResultPath = () => "results\\";

			using (var transaction = new TransactionScope())
			{

				var permission = TestUserPermission.ByShortcut("AF");

				client = TestClient.CreateSimple();
				user = client.Users[0];

				client.Users.Each(u =>
				{
					u.AssignedPermissions.Add(permission);
					u.SendRejects = true;
					u.SendWaybills = true;
				});
				user.Update();
			}
		}

		private void SetCurrentUser(string login)
		{
			ServiceContext.GetUserName = () => login;
		}

		/*
		 * Пример отладки IHttpHandler:
		 * Заголовок: HttpHandler Unit Testing
		 * источник: http://codebetter.com/blogs/karlseguin/archive/2007/01/25/HttpHandler-Unit-Testing.aspx
protected string RawRequest(string fileName, string queryString)
{
   StringBuilder output = new StringBuilder();
   using (StringWriter sw = new StringWriter(output))
   {
      HttpResponse response = new HttpResponse(sw);
      HttpRequest request = new HttpRequest(fileName, "http://fueltest.net/" + fileName, queryString);
      HttpContext context = new HttpContext(request, response);
      new RequestHandler().ProcessRequest(context);
   }
   return output.ToString();
}		 
		 *  
		 * 
		 * Еще один способ отладки:
		 * Заголовок: Как тестировать логику модулей и хендлеров ASP.NET?
		 * источник: http://www.codehelper.ru/questions/251/new/как-тестировать-логику-модулей-и-хендлеров-aspnet
		 */

		private void CheckProcessRequest(string login, string errorMessage)
		{
			SetCurrentUser(login);

			var fileName = "GetFileHandler.asxh";

			var output = new StringBuilder();
			using (var sw = new StringWriter(output))
			{
				var response = new HttpResponse(sw);
				var request = new HttpRequest(fileName, "http://127.0.0.1/" + fileName, String.Empty);
				var context = new HttpContext(request, response);

				var fileHandler = new GetFileHandler();
				fileHandler.ProcessRequest(context);

				Assert.That(response.StatusCode, Is.EqualTo(500), "Не верный код ошибки от сервера");

				Assert.That(context.Error, Is.Not.Null);
				Assert.That(context.Error.GetType(), Is.EqualTo(typeof(Exception)));
				Assert.That(context.Error.Message, Is.StringStarting(errorMessage).IgnoreCase);
			}
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
