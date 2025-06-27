from abc import ABC, abstractmethod
import operator
from pathlib import Path
import csv


class CommandError(Exception):
    """ Базовое исключение для ошибок команд """

class InvalidConditionError(CommandError):
    """ Некорректное условие фильтрации """

class InvalidAggregationError(CommandError):
    """ Некорректная агрегация """

class FileNotFoundError(FileNotFoundError, CommandError):
    """ Файл не найден """


class Command(ABC):
    """ Абстрактный базовый класс для команд обработки данных """

    @abstractmethod
    def execute(self, data):
        pass


class WhereCommand(Command):
    """ Команда фильтрации данных """
    # Состояние
    def __init__(self, condition):
        try:
            self.key, self.op_func, self.value = self._parse_condition(condition)
        except ValueError as e:
            raise InvalidConditionError(f'Не верное условие фильтрации: {condition}!') from e

    @staticmethod
    def _parse_condition(condition):
        ops = {
            '>=': operator.ge,
            '<=': operator.le,
            '!=': operator.ne,
            '>': operator.gt,
            '<': operator.lt,
            '=': operator.eq,
        }
        condition = condition.replace(' ', '')
        for op_str in ops:
            parts = condition.split(op_str, 1)
            if len(parts) == 2:
                return parts[0].strip(), ops[op_str], parts[1].strip()
        raise ValueError(f"Не верный оператор сравнения: {condition}!")

    def execute(self, data):
        if not data:
            return []

        where_data: list[dict] = []
        for row in data:
            row_val = row.get(self.key)
            if row_val is None:
                continue

            try:
                if self.op_func(float(row_val), float(self.value)):
                    where_data.append(row)
            except ValueError:
                if self.op_func(str(row_val), self.value):
                    where_data.append(row)
        return where_data


class AggregateCommand(Command):
    """ Команда агрегации данных """
    # выражение
    def __init__(self, expression):
        try:
            self.key, self.agg_func_name = self._parse_expression(expression)
        except ValueError as e:
            raise InvalidAggregationError(f'Не верная агрегация: {expression}!') from e

        self.agg_funcs = {
            'avg': self._avg,
            'min': self._min,
            'max': self._max
        }
        if self.agg_func_name not in self.agg_funcs:
            raise InvalidAggregationError(f'Не верная функция агрегации: {self.agg_func_name}')

    @staticmethod
    def _parse_expression(expression):
        if expression.count('=') != 1:
            raise ValueError('Агрегатное выражение должно содержать "="!')
        key, agg_func = expression.split('=', 1)
        key = key.strip()
        agg_func = agg_func.strip()

        if not key or not agg_func:
            raise ValueError("Нет данных для агрегации!")

        return key, agg_func

    def execute(self, data):
        if not data:
            return [{
                "result": "Нет данных для агрегации!"
            }]

        agg_func = self.agg_funcs[self.agg_func_name]
        result = agg_func(data)
        return [result]

    def _avg(self, data):
        values = []
        for row in data:
            val = row.get(self.key)
            if val is None or val == '':
                continue
            try:
                values.append(float(val))
            except (ValueError, TypeError):
                continue

        if not values:
            return {"avg": None}
        return {"avg": sum(values) / len(values)}

    def _min(self, data):
        values = [row.get(self.key) for row in data if row.get(self.key) not in (None, '')]
        if not values:
            return {"min": None}
        try:
            return {"min": min(map(float, values))}
        except ValueError:
            return {"min": min(values)}

    def _max(self, data):
        values = [row.get(self.key) for row in data if row.get(self.key) not in (None, '')]
        if not values:
            return {"max": None}
        try:
            return {"max": max(map(float, values))}
        except ValueError:
            return {"max": max(values)}


class DataProcessor:
    """ Обработчик данных с цепочкой команд """
    def __init__(self):
        self.commands = []

    def add_command(self, command: Command):
        self.commands.append(command)

    def process(self, data):
        for command in self.commands:
            data = command.execute(data)
        return data


class CSVLoader:
    """ Загрузчик данных из CSV файла """

    @staticmethod
    def load(path: Path):
        if not path.exists():
            raise FileNotFoundError(f"Файл {path} не найден!")

        with open(path, 'r', encoding='utf-8') as file:
            return list(csv.DictReader(file))
