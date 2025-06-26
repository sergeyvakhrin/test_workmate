from tabulate import tabulate


def print_data(data):
    """ Вывод данных в табличном формате """
    if not data:
        print("\nНет данных для вывода!\n")
        return

    headers = data[0].keys()
    rows = [list(item.values()) for item in data]
    print(tabulate(rows, headers=headers, tablefmt="psql"))