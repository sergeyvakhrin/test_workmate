import argparse
import csv
from tabulate import tabulate
import operator


def main():
    parser = argparse.ArgumentParser(description="Запуска с ключами")
    parser.add_argument('--file', '-f', type=str, help="Путь к входному файлу")
    parser.add_argument('--where', '-w', type=str, help="Фильтрация по столбцу")
    parser.add_argument('--aggregate', '-a', type=str, help="Агрегация по столбцу")
    args = parser.parse_args()

    file = args.file
    where = args.where
    aggregate = args.aggregate
    print(f"Входной файл: {file}")
    print(f"Фильтрация по столбцу: {where}")
    print(f"Агрегация по столбцу: {aggregate}")

    read_csv = load_file(file) if file else None

    if read_csv:
        if where:
            key, op_func, value = get_operator(where)
            read_csv = get_where_data(read_csv, key, op_func, value)
        if aggregate:
            key, op_func, value = get_operator(aggregate)
            read_csv = get_aggregate_data(read_csv, key, op_func, value)
        out_data(read_csv)


def load_file(path):
    """ Загрузка данных из файла csv """
    with open(path, 'r', encoding='utf-8') as file:
        reader_dict = csv.DictReader(file, delimiter=',')
        obj_list = []
        for obj in reader_dict:
            obj_list.append(obj)
    return obj_list


def get_operator(where):
    """ Получает оператор сравнения для фильтрации """
    ops = {
        '>=': operator.ge,
        '<=': operator.le,
        '!=': operator.ne,
        '>': operator.gt,
        '<': operator.lt,
        '=': operator.eq,
    }
    for op_str, op_func in ops.items():
        if op_str in where:
            key, value = where.split(op_str)
            return key.strip(), op_func, value.strip()
    raise ValueError('Не верное условие фильтрации!!!')


def get_where_data(read_csv, key, op_func, value):
    """ Фильтрация данных """ #TODO: проверить на фильтрацию строк
    where_data: list[dict] = []
    for data in read_csv:
        if op_func(data.get(key), value):
            where_data.append(data)
    return where_data


def get_aggregate_data(read_csv, key, op_func, value):
    """ Агрегация данных """
    aggr_data = 0
    if value == 'avg':
        values = [float(data.get(key)) for data in read_csv]
        aggr_data = sum(values) / len(values)
        read_csv = []
        read_csv.append({'key': value})

    elif value == 'min':
        values = [float(data.get(key)) for data in read_csv]
        aggr_data = min(values)
        read_csv = []
        read_csv.append({'key': value})
    elif value == 'max':
        values = [float(data.get(key)) for data in read_csv]
        aggr_data = max(values)
        read_csv = []
        read_csv.append({'key': value})

    read_csv.append({key: aggr_data})
    return read_csv


def out_data(data):
    """ Вывод данных в консоль """
    print(tabulate(data, headers="keys", tablefmt="psql"))


if __name__ == '__main__':
    main()
