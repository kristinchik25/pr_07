# pr_07
## Практическая работа 7. Импорт и экспорт данных в SQL. Работа с внешними источниками данных
## Цель:
Разработка и реализация информационной системы для базы данных больницы. Включает в себя создание базы данных, таблиц, выполнение SQL-запросов и визуализацию данных.

## Задачи:
1. Создание ERD диаграммы для базы данных.
2. Разработка SQL-скриптов для создания базы данных и таблиц.
3. Реализация заданий в Jupyter Notebook с подключением к базе данных, вставкой и обновлением данных, а также визуализацией информации.

## Индивидуальные задания, вариант 12:
1.	Создайте таблицу "Hospital" с полями "ID", "NAME", "TYPE".
2.	Получите информацию о докторе с ID=109
3.	Обновите стаж врача с ID=110 на 2 года.
4.	Удалите запись из таблицы "Doctor" с ID=115.
5.	Постройте график для анализа количества врачей по специальности.

## Выполнение практической работы
# Подключение к базе данных
Следующую работу будем осуществлять в «Visual Studio Code». Для начала необходимо установить psycopg2(популярная библиотека Python для работы с базами данных PostgreSQL. Она предоставляет интерфейс для подключения к базе данных, выполнения SQL-запросов и управления данными), для этого используем следующую команду:
````
%pip install psycopg2 
````
Для перехвата и обработки ошибок, возникающих при работе с базой данных PostgreSQL используем код:
````
import psycopg2
from psycopg2 import Error
````
Для подключение к базе данных можно использовать следующий шаблон:
````
try:
    # Подключение к существующей базе данных
    connection = psycopg2.connect(user="ваш логин",
                                  password="ваш пароль",
                                  host="ваш хост",
                                  port="5432",
                                  database="имя ")

    # Создание курсора для выполнения операций с базой данных
    cursor = connection.cursor()
    # Вывод информации о сервере PostgreSQL
    print("Информация о сервере PostgreSQL")
    print(connection.get_dsn_parameters(), "\n")
    # Выполнение SQL-запроса
    cursor.execute("SELECT version();")
    # Получение результата
    record = cursor.fetchone()
    print("Вы подключены к - ", record, "\n")

except (Exception, Error) as error:
    print("Ошибка при подключении к PostgreSQL:", error)
finally:
    if (connection):
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")
````
## Задание 1.	Создайте таблицу "Hospital" с полями "ID", "NAME", "TYPE".
В «Visual Studio Code» возьмем в качестве шаблона документ с лекции student_lecture7.ipynb, в процессе работы с ним на занятии уже была создана таблица «doctor», создадим «hospital». Важно отметить, что всегда необходимо закрывать соединение с PostgreSQL, чтобы избежать замедления работы приложения, ошибки "too many connections" и переизбытка хранящейся информации.
````
import psycopg2

def get_connection(database_name):
    # Функция для получения подключения к базе данных
    connection = psycopg2.connect(user="ваш логин",
                                  password="ваш пароль",
                                  host="ваш хост",
                                  port="5432",
                                  database=database_name)
    return connection

def close_connection(connection):
    # Функция для закрытия подключения к базе данных
    if connection:
        connection.close()
        print("Соединение с PostgreSQL закрыто")

try:
    # Создание подключения к базе данных sql_case_bi_mgpu (база, с которой можно создавать другие базы)
    connection = psycopg2.connect(user="ваш логин",
                                  password="ваш пароль",
                                  host="ваш хост",
                                  port="5432",
                                  database="medical_db")
    connection.autocommit = True  # Отключаем транзакцию для команды CREATE DATABASE
    cursor = connection.cursor()

    # Создание базы данных
    #cursor.execute("CREATE DATABASE medical_db;")
    #print("База данных 'medical_db' успешно создана")

    # Закрытие текущего соединения для подключения к новой базе данных
    close_connection(connection)

    # Подключение к новой базе данных 'medical_db'
    connection = get_connection("medical_db")
    cursor = connection.cursor()

    # Создание таблицы Hospital
    create_table_query = '''
    CREATE TABLE Hospital (
        Hospital_Id serial NOT NULL PRIMARY KEY,
        Hospital_Name VARCHAR (100) NOT NULL,
        Hospital_Type VARCHAR (10)
    );
    '''
    cursor.execute(create_table_query)
    connection.commit()
    print("Таблица 'Hospital' успешно создана")

    # Вставка данных в таблицу Hospital
    insert_query = '''
    INSERT INTO Hospital (Hospital_Id, Hospital_Name, Hospital_Type)
    VALUES
    (1, 'Mayo Clinic', private),
    (2, 'Cleveland Clinic', state),
    (3, 'Johns Hopkins', state),
    (4, 'UCLA Medical Center', state);
    '''
    cursor.execute(insert_query)
    connection.commit()
    print("Данные успешно вставлены в таблицу 'Hospital'")

except (Exception, psycopg2.Error) as error:
    print("Ошибка при подключении или работе с PostgreSQL:", error)

finally:
    # Закрытие подключения к базе данных
    if connection:
        close_connection(connection)
````
# Результат выполнения кода:
![image](https://github.com/user-attachments/assets/bf815ab7-2853-4d97-8552-ecc21a20d61d)

# Проверим в pgAdmin
![image](https://github.com/user-attachments/assets/a175d65b-f8c9-4f31-80b0-b75c1b6b1013)

![image](https://github.com/user-attachments/assets/46972dec-5a67-463c-9fa2-5884f7d831a0)

Таблица создана верно. 

## Задание 2.Получите информацию о докторе с ID=109.
Составляем запрос, который выдаст нам необходимую информацию:
````
def get_connection(database_name):
    # Функция для получения подключения к базе данных
    connection = psycopg2.connect(user="ваш логин",
                                  password="ваш пароль",
                                  host="ваш хост",
                                  port="5432",
                                  database=database_name)
    return connection

def close_connection(connection):
    # Функция для закрытия подключения к базе данных
    if connection:
        connection.close()
        print("Соединение с PostgreSQL закрыто")



def get_doctor_detail(doctor_id):
    try:
        # Подключаемся к базе данных medical_db
        database_name = 'medical_db'
        connection = get_connection(database_name)
        cursor = connection.cursor()

        # Запрос для получения информации о докторе
        select_query = """SELECT * FROM Doctor WHERE Doctor_Id = %s"""
        cursor.execute(select_query, (doctor_id,))
        records = cursor.fetchall()

        # Вывод информации о докторе
        print("Печать записи о докторе:")
        for row in records:
            print("Doctor Id:", row[0])
            print("Doctor Name:", row[1])
            print("Hospital Id:", row[2])
            print("Joining Date:", row[3])
            print("Specialty:", row[4])
            print("Salary:", row[5])
            print("Experience:", row[6])

        # Закрытие подключения
        close_connection(connection)
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при получении данных:", error)

get_doctor_detail(109)
````
# Результат выполнения кода
![image](https://github.com/user-attachments/assets/d51bdf18-346c-40f4-9a23-83aa6b3b08a9)

Необходимые данные получены.

## Задание 3.Обновите стаж врача с ID=110 на 2 года.
Создаем новую строку Code и составляем код по заданию:
````
import psycopg2

def get_connection(database_name):
    # Функция для получения подключения к базе данных
    connection = psycopg2.connect(user="ваш логин",
                                  password="ваш пароль",
                                  host="ваш хость",
                                  port="5432",
                                  database=database_name)
    return connection

def close_connection(connection):
    # Функция для закрытия подключения к базе данных
    if connection:
        connection.close()
        print("Соединение с PostgreSQL закрыто")

def update_experience(doctor_id, new_experience):
    try:
        # Подключаемся к базе данных
        database_name = 'medical_db'
        connection = get_connection(database_name)
        cursor = connection.cursor()

        # Обновляем стаж доктора с указанным ID
        update_query = """UPDATE Doctor SET Experience = %s WHERE Doctor_Id = %s"""
        cursor.execute(update_query, (new_experience, doctor_id))
        connection.commit()

        print(f"Стаж врача с ID {doctor_id} успешно обновлен на {new_experience} года")

        # Печать данных о докторе после обновления
        select_query = """SELECT Doctor_Id, Doctor_Name, Hospital_Id, Joining_Date, Speciality, Salary, Experience
                          FROM Doctor WHERE Doctor_Id = %s"""
        cursor.execute(select_query, (doctor_id,))
        doctor_record = cursor.fetchone()

        if doctor_record:
            print("\nИнформация о докторе после обновления:")
            print(f"Doctor Id: {doctor_record[0]}")
            print(f"Doctor Name: {doctor_record[1]}")
            print(f"Hospital Id: {doctor_record[2]}")
            print(f"Joining Date: {doctor_record[3]}")
            print(f"Speciality: {doctor_record[4]}")
            print(f"Salary: {doctor_record[5]}")
            print(f"Experience: {doctor_record[6]}")

        # Закрытие подключения
        close_connection(connection)

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при обновлении данных:", error)

# Обновим стаж врача с ID 101 на 3 года
print("Задание: Обновить стаж врачу с ID 110 на 2 года\n")
update_experience(110, 2)
````
# Результат обновления данных:
![image](https://github.com/user-attachments/assets/e23cbf09-4957-4e24-b181-a5ecd9a6ec51)

## Задание 4. Удалите запись из таблицы "Doctor" с ID=115.
Пишем запрос, позволяющий удалить необходимую информацию
````
import psycopg2

def get_connection(database_name):
    # Функция для получения подключения к базе данных
    connection = psycopg2.connect(user="ваш логин",
                                  password="ваш пароль",
                                  host="ваш хост",
                                  port="5432",
                                  database=database_name)
    return connection

def close_connection(connection):
    # Функция для закрытия подключения к базе данных
    if connection:
        connection.close()
        print("Соединение с PostgreSQL закрыто")

def delete_doctor(doctor_id):
    try:
        # Подключаемся к базе данных
        database_name = 'medical_db'
        connection = get_connection(database_name)
        cursor = connection.cursor()

        # Удаляем запись доктора с указанным ID
        delete_query = """DELETE FROM Doctor WHERE Doctor_Id = %s"""
        cursor.execute(delete_query, (doctor_id,))
        connection.commit()

        # Проверяем, была ли запись удалена
        if cursor.rowcount > 0:
            print(f"Запись врача с ID {doctor_id} успешно удалена")
        else:
            print(f"Запись врача с ID {doctor_id} не найдена")

        # Закрытие подключения
        close_connection(connection)

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при удалении данных:", error)

# Удалим запись врача с ID 115
print("Задание: Удалить запись врача с ID 115\n")
delete_doctor(115)
````

# Результат удаления данных
![image](https://github.com/user-attachments/assets/c2aaa5d8-e178-4fc9-9055-1369299b16b8)

## Задание 5. Постройте график для анализа количества врачей по специальности.
Это задание выполняется при помощи библиотек pandas и matplotlib , загрузим их

````
%pip install pandas
````

````
%pip install matplotlib
````
Выполним запрос
````
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

# Подключение к базе данных
conn = psycopg2.connect(
    host="localhost",
    database="medical_db",
    user="postgres",
    password="1",
    port="5432"
)
# SQL-запрос
query = """
SELECT speciality, COUNT(*) AS count
FROM Doctor
GROUP BY speciality;
"""
df = pd.read_sql(query, conn)
conn.close()
df_sorted = df.sort_values(by='count', ascending=False).reset_index(drop=True)
plt.figure(figsize=(12, 6))
plt.plot(df_sorted['speciality'], df_sorted['count'], 
         marker='o', linestyle='-', color='teal', markersize=8)
plt.title('Количество врачей по специальностям')
plt.xlabel('Специальность')
plt.ylabel('Количество врачей')
plt.xticks(rotation=45, ha='right')  
plt.tight_layout()
plt.grid(True, linestyle='--', alpha=0.6)
plt.show()
````
![image](https://github.com/user-attachments/assets/5ea80969-283a-40cf-9124-9e003ec709ef)

Альтернативный, на мой взгляд, более наглядный вариант
````
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

conn = psycopg2.connect(
    host="localhost",
    database="medical_db",
    user="postgres",
    password="1",
    port="5432"
)

query = """
SELECT speciality, COUNT(*) AS count
FROM Doctor
GROUP BY speciality;
"""

df = pd.read_sql(query, conn)

conn.close()

df_sorted = df.sort_values(by='count', ascending=False)

plt.figure(figsize=(12, 6))
plt.bar(df_sorted['speciality'], df_sorted['count'], color='pink')
plt.title('Количество врачей по специальностям')
plt.xlabel('Специальность')
plt.ylabel('Количество врачей')
plt.xticks(rotation=45)
plt.tight_layout()
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()
````

![image](https://github.com/user-attachments/assets/ae49933c-9bde-43ca-bdf8-f003f0151b3b)


# Выводы
В результате выполнения практической работы были успешно освоены методы импорта и экспорта данных в базу данных SQL, а также получены навыки работы с внешними источниками данных. Эти знания и умения являются фундаментальными для работы с базами данных, что значительно расширяет возможности для решения практических задач.
Все индивидуальные задания успешно выполнены
## Структура репозитория:
- `erd_diagram.png` — ERD диаграмма схемы базы данных.
- `DAMDINOVA_SQL.sql` — SQL скрипт для создания базы данных и таблиц.
- `Damdinova_Kristina_Takhirovna` — Jupyter Notebook с выполнением всех заданий.

## Как запустить:
1. Установите PostgreSQL и настройте доступ к базе данных.
2. Выполните SQL-скрипт `create_db_and_tables.sql` для создания базы данных и таблиц.
3. Откройте и выполните Jupyter Notebook для проверки выполнения заданий.
