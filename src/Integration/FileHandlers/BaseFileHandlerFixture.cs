using System;
using System.IO;
using System.Text;
using System.Web;
using Integration.BaseTests;
using NUnit.Framework;

namespace Integration
{
	public class BaseFileHandlerFixture : PrepareDataFixture
	{
		protected string FileHanderAshxName;

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

		protected void WithHttpContext(Action<HttpContext> action, string queryParams = null)
		{
			Assert.IsNotNullOrEmpty(FileHanderAshxName, "Для теста не установлено свойство FileHanderAshxName");

			var output = new StringBuilder();
			using (var sw = new StringWriter(output)) {
				var response = new HttpResponse(sw);
				var request = new HttpRequest(FileHanderAshxName, "http://127.0.0.1/" + FileHanderAshxName, queryParams);
				var context = new HttpContext(request, response);

				action(context);
			}
		}
	}
}