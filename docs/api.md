## API Reference

::: alembic_utils.replaceable_entity.register_entities
    :docstring:

```python
# migrations/env.py

from alembic_utils.replaceable_entity import register_entities
from app.functions import my_function
from app.views import my_view

register_entities(entities=[my_function, my_view], exclude_schema=['audit'])
```

::: alembic_utils.pg_function.PGFunction
    :docstring:

```python
from alembic_utils.pg_function import PGFunction

to_lower = PGFunction(
    schema="public",
    signature="to_lower(some_text text)",
    definition="returns text as $$ lower(some_text) $$ language sql"
)
```


::: alembic_utils.pg_view.PGView
    :docstring:


```python
from alembic_utils.pg_view import PGView

scifi_books = PGView(
    schema="public",
    signature="scifi_books",
    definition="select * from books where genre='scifi'"
)
```

::: alembic_utils.pg_materialized_view.PGMaterializedView
    :docstring:


```python
from alembic_utils.pg_materialized_view import PGMaterializedView

scifi_books = PGMaterializedView(
    schema="public",
    signature="scifi_books",
    definition="select * from books where genre='scifi'",
    with_data=True
)

temperature_15_minute = PGMaterializedView(
    schema="public",
    signature="temperature_15_minute",
    definition="SELECT device, time_bucket(INTERVAL '15 minutes', time) AS bucket, AVG(temperature) as avg_temperature FROM metrics GROUP BY device, bucket;",
    with_data=True,
    storage_parameters=["timescaledb.continuous"]
)
```


::: alembic_utils.pg_trigger.PGTrigger
    :docstring:


```python
from alembic_utils.pg_trigger import PGTrigger

trigger = PGTrigger(
    schema="public",
    signature="lower_account_email",
    on_entity="public.account",
    definition="""
        BEFORE INSERT ON public.account
        FOR EACH ROW EXECUTE FUNCTION public.downcase_email()
    """,
)
```

::: alembic_utils.pg_extension.PGExtension
    :docstring:


```python
from alembic_utils.pg_extension import PGExtension

extension = PGExtension(
    schema="public",
    signature="uuid-ossp",
)
```


::: alembic_utils.pg_policy.PGPolicy
    :docstring:


```python
from alembic_utils.pg_policy import PGPolicy

policy = PGPolicy(
    schema="public",
    signature="allow_read",
    on_entity="public.account",
    definition="""
        AS PERMISSIVE
        FOR SELECT
        TO api_user
        USING (id = current_setting('api_current_user', true)::int)
    """,
)
```


::: alembic_utils.pg_grant_table.PGGrantTable
    :docstring:


```python
from alembic_utils.pg_grant_table import PGGrantTable

grant = PGGrantTable(
    schema="public",
    table="account",
    columns=["id", "email"],
    role="anon_user",
    grant='SELECT',
    with_grant_option=False,
)
```
