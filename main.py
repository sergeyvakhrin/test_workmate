import argparse
import csv
from pathlib import Path

from tabulate import tabulate
import operator

from src.classes import CSVLoader, DataProcessor, WhereCommand, AggregateCommand, CommandError
from src.utils import print_data


def main():
    """ Обработка csv файла в командной строке """
    parser = argparse.ArgumentParser(description="Запуска с ключами")
    parser.add_argument('--file', '-f', type=str, help="Путь к входному файлу")
    parser.add_argument('--where', '-w', type=str, help="Фильтрация по столбцу")
    parser.add_argument('--aggregate', '-a', type=str, help="Агрегация по столбцу")

    args = parser.parse_args()

    # Получаем исходные данные
    file = Path(args.file)
    where = args.where
    aggregate = args.aggregate
    print(f"Входной файл: {file}")
    print(f"Фильтрация по столбцу: {where}")
    print(f"Агрегация по столбцу: {aggregate}")

    # Загружаем данные из файла
    try:
        data = CSVLoader.load(file)
    except FileNotFoundError as e:
        print(f"\nОшибка: {e}\n")
        return

    processor = DataProcessor()

    try:
        if where:
            processor.add_command(WhereCommand(where))
        if aggregate:
            processor.add_command(AggregateCommand(aggregate))
    except CommandError as e:
        print(f"\nОшибка обработки: {e}\n")
        return

    result = processor.process(data)
    print_data(result)


if __name__ == '__main__':
    main()
