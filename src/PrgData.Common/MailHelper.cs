using System;
using System.Net.Mail;
using System.Configuration;
using System.Text;


namespace PrgData.Common
{
	public class MailHelper
	{
		public static void Mail(string messageText, string subject)
		{
			try
			{
				var MailAddress = new MailAddress("service@analit.net", "Сервис AF", Encoding.UTF8);
				var message = new MailMessage("service@analit.net", ConfigurationManager.AppSettings["ErrorMail"]);
				var SC = new SmtpClient("box.analit.net");
				message.From = MailAddress;
				message.Subject = subject;
				message.SubjectEncoding = Encoding.UTF8;
				message.BodyEncoding = Encoding.UTF8;
				message.Body = messageText;
				SC.Send(message);
			}
			catch
			{
			}
		}

		public static void MailErr(uint ClientId, string ErrSource, string ErrDesc)
		{
			Mail(
				"Клиент: " + ClientId + Environment.NewLine + "Процесс: " + ErrSource + Environment.NewLine + "Описание: " + ErrDesc,
				"Ошибка в сервисе подготовки данных");
		}
	}
}