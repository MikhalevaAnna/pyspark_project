# Обработать данные с использованием движка PySpark

## Часть 1. Генерация информации
Используется библиотека **Faker** для генерации логов веб-сервера. Логи содержат следующую информацию:

- IP-адрес клиента
- Временная метка запроса
- HTTP-метод (GET, POST, etc.)
- URL запроса
- Код ответа (200, 404, etc.)
 
Размер ответа в байтах
Генерируется 100,000 записей логов и сохраняется в **CSV**-файл. Обязательно сделать **!pip install faker**

```
import csv
from faker import Faker
import random

fake = Faker()

num_records = 100000

http_methods = ['GET', 'POST', 'PUT', 'DELETE']
response_codes = [200, 301, 404, 500]

file_path = "web_server_logs.csv"

with open(file_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['ip', 'timestamp', 'method', 'url', 'response_code', 'response_size'])
    
    for _ in range(num_records):
        ip = fake.ipv4()
        timestamp = fake.date_time_this_year().isoformat()
        method = random.choice(http_methods)
        url = fake.uri_path()
        response_code = random.choice(response_codes)
        response_size = random.randint(100, 10000)
        
        writer.writerow([ip, timestamp, method, url, response_code, response_size])

print(f"Сгенерировано {num_records} записей и сохранено в {file_path}")
```

## Часть 2. Анализ информации
1. Сгруппировать данные по IP и посчитать количество запросов для каждого IP, вывести 10 самых активных IP.
2. Сгруппировать данные по HTTP-методу и посчитать количество запросов для каждого метода.
3. Профильтровать и посчитать количество запросов с кодом ответа 404.
4. Сгруппировать данные по дате и просуммировать размер ответов, сортировать по дате.
