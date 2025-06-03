from typing import TypeAlias, Union
from uuid import uuid4

StorageParameter: TypeAlias = Union[str, tuple[str, Union[str, int, float]]]


def normalize_whitespace(text, base_whitespace: str = " ") -> str:
    """Convert all whitespace to *base_whitespace*"""
    return base_whitespace.join(text.split()).strip()


def strip_terminating_semicolon(sql: str) -> str:
    """Removes terminating semicolon on a SQL statement if it exists"""
    return sql.strip().rstrip(";").strip()


def strip_double_quotes(sql: str) -> str:
    """Removes starting and ending double quotes"""
    sql = sql.strip().rstrip('"')
    return sql.strip().lstrip('"').strip()


def escape_colon_for_sql(sql: str) -> str:
    """Escapes colons for use in sqlalchemy.text"""
    holder = str(uuid4())
    sql = sql.replace("::", holder)
    sql = sql.replace(":", r"\:")
    sql = sql.replace(holder, "::")
    return sql


def escape_colon_for_plpgsql(sql: str) -> str:
    """Escapes colons for plpgsql for use in sqlalchemy.text"""
    holder1 = str(uuid4())
    holder2 = str(uuid4())
    holder3 = str(uuid4())
    sql = sql.replace("::", holder1)
    sql = sql.replace(":=", holder2)
    sql = sql.replace(r"\:", holder3)

    sql = sql.replace(":", r"\:")

    sql = sql.replace(holder3, r"\:")
    sql = sql.replace(holder2, ":=")
    sql = sql.replace(holder1, "::")
    return sql


def format_storage_parameters_clause(
    storage_parameters: list[StorageParameter] | None,
) -> str:
    """Generates a WITH clause with storage parameters.

    Examples:
        format_storage_parameters_clause([("param1",70),"param2", ("param3","'test'")]) => " WITH (param1=70, param2, param3='test')"
        format_storage_parameters_clause([]) => ""
        format_storage_parameters_clause(None) => ""
    """
    if storage_parameters is None or len(storage_parameters) == 0:
        return ""
    params = [
        param if isinstance(param, str) else f"{param[0]}={param[1]}"
        for param in storage_parameters
    ]
    return f" WITH ({', '.join(params)})"


def parse_storage_parameters(storage_parameters: str) -> list[StorageParameter]:
    """Parse a string of storage parameters.

    Examples:
        parse_storage_parameters("param1,param2=80, param3='test'") => ["param1",("param2",80),("param3","'test'")]
    """
    params: list[StorageParameter] = []
    for part in storage_parameters.split(","):
        new_part: StorageParameter = part.strip()
        if "=" in part:
            split_part = part.split("=", 1)
            # doing it this way so mypy doesn't complain
            new_part = (split_part[0].strip(), split_part[1].strip())
            try:
                new_part = (part[0], int(part[1]))
            except ValueError:
                try:
                    new_part = (part[0], float(part[1]))
                except ValueError:
                    pass
        params.append(new_part)
    return params


def coerce_to_quoted(text: str) -> str:
    """Coerces schema and entity names to double quoted one

    Examples:
        coerce_to_quoted('"public"') => '"public"'
        coerce_to_quoted('public') => '"public"'
        coerce_to_quoted('public.table') => '"public"."table"'
        coerce_to_quoted('"public".table') => '"public"."table"'
        coerce_to_quoted('public."table"') => '"public"."table"'
    """
    if "." in text:
        schema, _, name = text.partition(".")
        schema = f'"{strip_double_quotes(schema)}"'
        name = f'"{strip_double_quotes(name)}"'
        return f"{schema}.{name}"

    text = strip_double_quotes(text)
    return f'"{text}"'


def coerce_to_unquoted(text: str) -> str:
    """Coerces schema and entity names to unquoted

    Examples:
        coerce_to_unquoted('"public"') => 'public'
        coerce_to_unquoted('public') => 'public'
        coerce_to_unquoted('public.table') => 'public.table'
        coerce_to_unquoted('"public".table') => 'public.table'
    """
    return "".join(text.split('"'))
