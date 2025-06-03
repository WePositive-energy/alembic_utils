from alembic_utils.statement import (
    coerce_to_quoted,
    coerce_to_unquoted,
    format_storage_parameters_clause,
    parse_storage_parameters,
)


def test_coerce_to_quoted() -> None:
    assert coerce_to_quoted('"public"') == '"public"'
    assert coerce_to_quoted("public") == '"public"'
    assert coerce_to_quoted("public.table") == '"public"."table"'
    assert coerce_to_quoted('"public".table') == '"public"."table"'
    assert coerce_to_quoted('public."table"') == '"public"."table"'


def test_coerce_to_unquoted() -> None:
    assert coerce_to_unquoted('"public"') == "public"
    assert coerce_to_unquoted("public") == "public"
    assert coerce_to_unquoted("public.table") == "public.table"
    assert coerce_to_unquoted('"public".table') == "public.table"


def test_format_storage_parameters_clause() -> None:
    assert (
        format_storage_parameters_clause(["param1", ("param2", 80), ("param3", "'test'")])
        == " WITH (param1, param2=80, param3='test')"
    )
    assert (
        format_storage_parameters_clause(["timescaledb.continuous"])
        == " WITH (timescaledb.continuous)"
    )

    assert format_storage_parameters_clause([]) == ""
    assert format_storage_parameters_clause(None) == ""


def test_parse_storage_parameters_clause() -> None:
    assert parse_storage_parameters("param1, param2=80,param3='test',param4=80.5") == [
        "param1",
        ("param2", 80),
        ("param3", "'test'"),
        ("param4", 80.5),
    ]
    assert parse_storage_parameters("param1,param2") == [
        "param1",
        "param2",
    ]
