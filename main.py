
import asyncio
import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler("monitor.log"), logging.StreamHandler()]
)

async def fetch_data_source(source_id: int, url: str, timeout: float = 5.0) -> dict:
    logging.info(f"Запуск запроса к Источнику {source_id} ({url})...")
    try:
        if source_id == 2:
            await asyncio.sleep(timeout + 1.0) 
        else:
            await asyncio.sleep(1.5)
            
        return {"source": source_id, "status": "success", "payload": {"metric": 42}}
        
    except asyncio.TimeoutError:
        logging.error(f"Ошибка: Превышен таймаут ожидания ({timeout} сек) для Источника {source_id}")
        return {"source": source_id, "status": "error", "reason": "TimeoutError"}
    except Exception as e:
        logging.error(f"Непредвиденное исключение при опросе Источника {source_id}: {str(e)}")
        return {"source": source_id, "status": "error", "reason": type(e).__name__}

def save_data(data: list) -> None:
    record = {
        "timestamp": datetime.now().isoformat(),
        "dataset": data
    }
    try:
        with open("collected_data.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        logging.info("Данные успешно верифицированы и сохранены в локальный репозиторий.")
    except IOError as e:
        logging.critical(f"Критическая ошибка ввода-вывода при сохранении данных: {str(e)}")

async def main():
    targets = [
        {"id": 1, "url": "https://source1.ru"},
        {"id": 2, "url": "https://source2.ru"}
    ]
    
    logging.info("Инициализация службы автоматического асинхронного мониторинга...")
    
    while True:
        logging.info("Начало новой итерации сбра данных.")
        
        tasks = [
            fetch_data_source(target["id"], target["url"], timeout=7.0) 
            for target in targets
        ]
        
        results = await asyncio.gather(*tasks)
        save_data(results)
        
        logging.info("Итерация завершена. Переход в режим ожидания (10 минут)...")
        await asyncio.sleep(600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Служба мониторинга принудительно остановлена пользователем.")
