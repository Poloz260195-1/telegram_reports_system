# Инструкции по развертыванию

## 🚀 Быстрый старт

### 1. Клонирование репозитория
```bash
git clone https://github.com/your-username/telegram-reports-system.git
cd telegram-reports-system
```

### 2. Установка зависимостей
```bash
pip install -r requirements.txt
```

### 3. Настройка переменных окружения
```bash
# Скопируйте пример файла
cp env.example .env

# Отредактируйте .env файл с вашими данными
nano .env
```

### 4. Тестирование
```bash
python telegram_reports_system.py
```

## ⚙️ Подробная настройка

### Создание Telegram бота

1. **Найдите @BotFather в Telegram**
2. **Отправьте команду**: `/newbot`
3. **Следуйте инструкциям**:
   - Введите имя бота
   - Введите username (должен заканчиваться на 'bot')
4. **Скопируйте токен** и добавьте в `.env` файл

### Получение ID чата

1. **Добавьте бота в нужный чат**
2. **Отправьте любое сообщение в чат**
3. **Выполните запрос**:
```bash
curl "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates"
```
4. **Найдите chat_id** в ответе и добавьте в `.env` файл

### Настройка ClickHouse

Если у вас есть доступ к ClickHouse:
```bash
# В .env файле укажите ваши данные
CLICKHOUSE_HOST=your_clickhouse_host
CLICKHOUSE_DATABASE=your_database
CLICKHOUSE_USER=your_username
CLICKHOUSE_PASSWORD=your_password
```

## 🔄 Настройка Airflow

### 1. Установка Airflow
```bash
pip install apache-airflow
```

### 2. Инициализация базы данных
```bash
airflow db init
```

### 3. Создание пользователя
```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 4. Копирование DAG
```bash
# Скопируйте файл в папку dags
cp telegram_reports_system.py $AIRFLOW_HOME/dags/
```

### 5. Запуск Airflow
```bash
# Запустите webserver
airflow webserver --port 8080

# В другом терминале запустите scheduler
airflow scheduler
```

## 📊 Мониторинг

### Проверка логов
```bash
# Логи Airflow
tail -f $AIRFLOW_HOME/logs/dag_id/task_id/execution_date/attempt_number.log

# Логи приложения
tail -f telegram_reports.log
```

### Проверка статуса DAG
1. Откройте веб-интерфейс Airflow: http://localhost:8080
2. Найдите DAG `aleksej_polozov_bel8894_full_report`
3. Проверьте статус выполнения

## 🛠️ Устранение проблем

### Ошибка подключения к Telegram
- ✅ Проверьте токен бота
- ✅ Убедитесь, что бот добавлен в чат
- ✅ Проверьте права бота на отправку сообщений

### Ошибка подключения к ClickHouse
- ✅ Проверьте настройки подключения
- ✅ Убедитесь, что база данных доступна
- ✅ Проверьте права пользователя

### Ошибки в DAG
- ✅ Проверьте логи Airflow
- ✅ Убедитесь, что все зависимости установлены
- ✅ Проверьте переменные окружения

## 🔒 Безопасность

### Важные моменты:
- ✅ Никогда не коммитьте `.env` файл
- ✅ Используйте переменные окружения для конфиденциальных данных
- ✅ Регулярно обновляйте токены
- ✅ Ограничьте права бота минимально необходимыми

## 📈 Масштабирование

### Для продакшена:
1. **Используйте Docker** для контейнеризации
2. **Настройте мониторинг** (Prometheus, Grafana)
3. **Добавьте алерты** при ошибках
4. **Настройте бэкапы** данных
5. **Используйте CI/CD** для автоматического деплоя

## 📞 Поддержка

При возникновении проблем:
1. Проверьте логи
2. Убедитесь в корректности настроек
3. Протестируйте подключения отдельно
4. Создайте issue в репозитории

---

**Автор**: Алексей Полозов  
**Версия**: 1.0  
**Дата**: 2025 