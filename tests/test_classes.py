import pytest
from pathlib import Path

from src.classes import CSVLoader, WhereCommand, InvalidConditionError, AggregateCommand, InvalidAggregationError, \
    DataProcessor


@pytest.fixture
def sample_data(tmp_path):
    csv_content = """id,name,age,salary\n
1,Alice,30,50000\n
2,Bob,25,45000\n
3,Charlie,35,55000\n
4,Diana,40,60000
"""
    file_path = tmp_path / "test.csv"
    file_path.write_text(csv_content)
    return file_path


@pytest.fixture
def loaded_data(sample_data):
    return CSVLoader.load(sample_data)


@pytest.mark.parametrize("condition,expected_count", [
    ("age>30", 2),
    ("salary<=50000", 2),
    ("name=Alice", 1),
    ("id!=2", 3),
    ("age<100", 4),  # Все записи
])
def test_where_command(loaded_data, condition, expected_count):
    command = WhereCommand(condition)
    result = command.execute(loaded_data)
    assert len(result) == expected_count


@pytest.mark.parametrize("invalid_condition", [
    "invalid_column>10",
    "age??100",
    "name",
    "",
])
def test_invalid_where_condition(invalid_condition):
    with pytest.raises(InvalidConditionError):
        WhereCommand(invalid_condition)


@pytest.mark.parametrize("expression,expected", [
    ("age=avg", (30 + 25 + 35 + 40) / 4),
    ("salary=min", 45000),
    ("age=max", 40),
    ("salary=max", 60000),
])
def test_aggregate_command(loaded_data, expression, expected):
    command = AggregateCommand(expression)
    result = command.execute(loaded_data)[0]
    assert list(result.values())[0] == pytest.approx(expected, 0.001)


@pytest.mark.parametrize("invalid_aggregation", [
    "age=invalid_func",
    "unknown_column=avg",
    "age",
    "=avg",
    "age==",
])
def test_invalid_aggregation(invalid_aggregation):
    with pytest.raises(InvalidAggregationError):
        AggregateCommand(invalid_aggregation)


def test_empty_data_aggregation(loaded_data):
    # Фильтруем так, чтобы не осталось данных
    where_command = WhereCommand("age>100")
    filtered_data = where_command.execute(loaded_data)

    agg_command = AggregateCommand("salary=avg")
    result = agg_command.execute(filtered_data)

    assert "result" in result[0]  # Проверяем сообщение о пустых данных


def test_command(loaded_data):
    processor = DataProcessor()
    processor.add_command(WhereCommand("salary>50000"))
    processor.add_command(AggregateCommand("age=avg"))

    result = processor.process(loaded_data)
    avg_age = result[0]["avg"]

    expected_avg = (40 + 35) / 2
    assert avg_age == pytest.approx(expected_avg, 0.01)


def test_file_not_found():
    with pytest.raises(FileNotFoundError):
        CSVLoader.load(Path("non_existent_file.csv"))
