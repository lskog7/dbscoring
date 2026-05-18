# Report Requirements

## Назначение файла

Файл фиксирует требования к будущему итоговому отчёту по лабораторной работе, чтобы они не потерялись на фоне
текущей реализации notebook и тестового контура.

## Актуальный статус

Требования отчёта не входят в runtime-контур Spark/Polars хранилища, но должны учитываться при финальной сдаче.

## Обязательные разделы отчёта

Отчёт должен включать:

- титульный лист;
- физическую модель данных;
- описание физической модели в формулировке, требуемой заданием;
- блок-схему кода.

## Откуда брать материалы для отчёта

- физическую модель данных — из [schemas/schema_v3.drawio](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/schemas/schema_v3.drawio) и [schemas/schema_v3.png](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/schemas/schema_v3.png);
- описание логики загрузки и структуры модулей — из [README.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/README.md), [notebooks/README.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/notebooks/README.md) и docstring внутри notebook;
- сведения о тестовом контуре и воспроизводимости — из [tests/README.md](/Users/avtereshchenko/Desktop/Магистратура/Айрапетян_БД_2_сем/dbscoring/tests/README.md).

## Что не включать в отчёт

- исходный код целиком;
- служебные кэши и локальные виртуальные окружения;
- промежуточные debug-артефакты warehouse.
