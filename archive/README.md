###Описание файлов:


<br>main.py</br> - запуск основной программы, подключение к БД и создание 
таблиц, также запуск ассинхронного выполнения команд в двух режимах. Для нескольких 
пользователей (извлечение данных с API, отличающихся для каждого seller'а) 
и для условно одного пользователя (извлечение общих данных)

<br>db_client.py</br> - файл работы с базой данных. Создание таблиц и логика 
взаимодействия с ней. Прописан универсальный загрузчик данных в БД, однако 
в нем важно совпадение порядка полей на входе и порядок полей в БД

<br>update_token.py</br> - скрипт для обновления основного токена для удаленного 
входа в кабинет продавца (рекомендуется запуск раз 8-24 ч)

<br>wb_delivery_parsing.py</br> - основной класс для подключения к API WB, получение 
токенов для поддоменов и ответа в виде "сырого" json 

<br>data_extractor.py</br> - файл-обработчик получаемых данных, переименовывание и 
обработка полей, приходящих после запроса json'ов

<br>daily_data_extractor_script.py</br> - скрипт для выгрузки данных за длительный 
промежуток времени, загружает данные от начальной даты за каждый день до 
текущей