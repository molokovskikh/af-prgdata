using System;
using System.IO;
using System.Text;
using MySql.Data.MySqlClient;
using NUnit.Framework;
using log4net;

namespace Integration
{
	[TestFixture(Description = "проверка протоколирования log4net в базу MySql")]
	public class log4netMySqlAdoNetAdapter
	{
		private string _connectionString = "Database=analit;Data Source=bdstat.analit.net;Port=3306;User Id=AFdev;Password=password;pooling=true;default command timeout=200;Allow user variables=true;convert zero datetime=yes;";

		[Test(Description = "Создаем таблицу для протоколирования"), Ignore("Это не тест, а метод для создания таблицы протоколирования в production-сервере")]
		public void CreateLogTable()
		{
			var createTableSql = @"
CREATE TABLE analit.Log (
	Id          INT UNSIGNED NOT NULL AUTO_INCREMENT,
	Date        DATETIME NOT NULL,
	Level       VARCHAR(50) NOT NULL,
	Logger      VARCHAR(255) NOT NULL,
	Host        VARCHAR(255) DEFAULT NULL,
	User        VARCHAR(255) DEFAULT NULL,
	Message     TEXT DEFAULT NULL,
	Exception   TEXT DEFAULT NULL,
	PRIMARY KEY (Id),
	key(Date)
)
ENGINE = MYISAM	
#CHARACTER SET cp1251
#COLLATE cp1251_general_ci
;";

			using (var connection = new MySqlConnection(_connectionString)) {
				connection.Open();

				MySqlHelper.ExecuteNonQuery(connection, createTableSql);
			}
		}

		[Test(Description = "Проверяем протоколирование"), Ignore("Это не тест, а метод для проверки протоколирования в production-сервер")]
		public void LogToTable()
		{
			log4net.Config.XmlConfigurator.Configure(new FileInfo("TestData\\log4netMySql.config"));
			try {
				var log = LogManager.GetLogger(typeof(log4netMySqlAdoNetAdapter));

				log.Warn("Тест предупреждения");
				log.Error("Тест ошибки");
				log.ErrorFormat("Тест ошибки {0}", "с параметром");
				log.Error("Тест ошибки с исключением", new Exception("это тестовое исключение"));

				ThreadContext.Properties["user"] = "тестовый пользователь";
				try {
					log.Error("Тест ошибки с указанием контекста пользователя");
					log.Error("Тест ошибки с указанием контекста пользователя и исключением", new Exception("еще одно тестовое исключение"));
				}
				finally {
					ThreadContext.Properties.Clear();
				}
			}
			finally {
				LogManager.ResetConfiguration();
			}
		}
	}
}