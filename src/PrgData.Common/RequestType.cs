﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PrgData.Common
{
	public enum RequestType
	{
		GetData = 1, //Обычное обновление данных
		GetCumulative = 2, //Кумулятивное обновление
		ResumeData = 3, //Докачка обновления
		SendOrder = 4, //Отправка заказа
		Forbidden = 5, //Запрет на обращения к сервису
		Error = 6, //Ошибка во время запроса к сервису
		CommitExchange = 7, //Подтверждение получения обновления данных
		GetDocs = 8, //Запрос новых документов
		SendWaybills = 9, //Отправка накладных для разбора
		PostOrderBatch = 10, //Отправка дефектуры для разбора
		SendOrders = 11, //Отправка заказов
		PostPriceDataSettings = 12, //Отправка настроек прайс-листов
		GetHistoryOrders = 13, //Получить список архивных заказов
		ConfirmUserMessage = 14, //Подтвердить прочтение сообщения для пользователя
		SendUserActions = 15, //Сохрание статистики пользователя в базе
		GetDataAsync = 16, //Асинхронный запрос для обычного обновления данных
		GetCumulativeAsync = 17, //Асинхронный запрос для кумулятивного обновления данных
		GetLimitedCumulative = 18, //Частичное кумулятивное обновление
		GetLimitedCumulativeAsync = 19, //Асинхронный запрос для частичного кумулятивного обновление
		RequestAttachments = 20, //Запрос вложений мини-почты
	}
}