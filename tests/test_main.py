import pytest
import operator
import sys

from main import get_operator, get_where_data, get_aggregate_data, out_data, load_file, main

dict_1 = {'name': 'aaaa', 'brand': 'bbbb', 'price': 1111, 'rating': 2222}
dict_2 = {'name': 'cccc', 'brand': 'dddd', 'price': 3333, 'rating': 4444}

read_csv = [dict_1, dict_2]


def test_load_file(tmp_path):
    # Создаём временный CSV-файл
    csv_content = "name,brand,price,rating\naaaa,bbbb,1111,2222\ncccc,dddd,3333,4444"
    file = tmp_path / "test.csv"
    file.write_text(csv_content, encoding="utf-8")

    loaded_data = load_file(file)

    assert isinstance(loaded_data, list)
    assert len(loaded_data) == 2
    assert loaded_data[0]['name'] == 'aaaa'
    assert loaded_data[1]['price'] == '3333'


def test_get_operator(capsys):
    assert get_operator('qwe>=qwe') == ('qwe', operator.ge, 'qwe')
    assert get_operator('qwe<=qwe') == ('qwe', operator.le, 'qwe')
    assert get_operator('qwe!=qwe') == ('qwe', operator.ne, 'qwe')
    assert get_operator('qwe>qwe') == ('qwe', operator.gt, 'qwe')
    assert get_operator('qwe<qwe') == ('qwe', operator.lt, 'qwe')
    assert get_operator('qwe=qwe') == ('qwe', operator.eq, 'qwe')
    assert get_operator('qweqwe') == (None, None, None)
    key, op, value = get_operator('')
    cap = capsys.readouterr()
    assert 'не верное условие фильтрации' in cap.out.lower()


def test_get_where_data():
    assert get_where_data(read_csv, 'name', operator.ge, 'bbbb') == [dict_2]
    assert get_where_data(read_csv, 'name', operator.le, 'bbbb') == [dict_1]
    assert get_where_data(read_csv, 'price', operator.le, '2222') == [dict_1]


def test_get_aggregate_data():
    assert get_aggregate_data(read_csv, 'price', operator.ge, '') == []
    assert get_aggregate_data(read_csv, 'price', operator.eq, '') == []
    assert get_aggregate_data(read_csv, 'price', operator.eq, '500') == []
    assert get_aggregate_data(read_csv, 'price', operator.eq, 'min') == [{'min': 1111.0}]
    assert get_aggregate_data(read_csv, 'price', operator.eq, 'max') == [{'max': 3333.0}]
    assert get_aggregate_data(read_csv, 'price', operator.eq, 'avg') == [{'avg': 2222.0}]
    assert get_aggregate_data(read_csv, 'name', operator.eq, 'min') == [{'min': 'aaaa'}]
    assert get_aggregate_data(read_csv, 'name', operator.eq, 'max') == [{'max': 'cccc'}]
    assert get_aggregate_data(read_csv, 'name', operator.eq, 'avg') == []


def test_out_data(capsys):
    out_data(read_csv)
    cap = capsys.readouterr()
    assert "aaaa" in cap.out
    assert "cccc" in cap.out


def test_main(capsys):
    sys.argv = ['main.py', '--file', '1111.csv']
    main()
    cap = capsys.readouterr()
    assert 'не найден' in cap.out.lower()




