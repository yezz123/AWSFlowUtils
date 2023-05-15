import os
from typing import Dict, Union

import pg8000


def redshift_get_conn(env_var: str) -> pg8000.Connection:
    """Creates a Redshift connection object"""
    _env_var_validator(env_var=env_var)
    cred_str = os.environ[env_var]
    creds_dict = _create_creds_dict(cred_str)
    return pg8000.connect(ssl_context=True, **creds_dict)


def read_sql(sql_filename: str) -> str:
    """Ingests a SQL file and returns a str containing the contents of the file"""
    if not isinstance(sql_filename, str):
        raise TypeError("sql_filename must be of str type")
    with open(sql_filename) as f:
        sql_str = " ".join(f.readlines())
    return sql_str


def redshift_execute_sql(
    sql: str,
    env_var: str,
    return_data: bool = False,
    return_dict: bool = False,
):
    """Ingests a SQL query as a string and executes it returing Data"""
    _redshift_execute_sql_arg_validator(
        sql=sql, env_var=env_var, return_data=return_data, return_dict=return_dict
    )
    with redshift_get_conn(env_var=env_var) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        if return_data:
            columns = [desc[0] for desc in cursor.description]
            data = list(cursor)
            conn.commit()
            return (
                {"data": data, "columns": columns} if return_dict else (data, columns)
            )
        else:
            conn.commit()
            return


def _create_creds_dict(creds_str: str) -> Dict[str, Union[str, int]]:
    """Takes the credentials str and converts it to a dict"""
    creds_dict = {}
    for param in creds_str.split(" "):
        split_param = param.split("=")
        if split_param[0] == "port":
            split_param[1] = int(split_param[1])
        creds_dict[split_param[0]] = split_param[1]
    return creds_dict


def _env_var_validator(env_var: str) -> None:
    """Validates that the user is providing the an environment variable name, rather than the credentials string itself"""
    creds_str_keys = ["host", "database", "user", "password", "port"]
    if all(key in env_var for key in creds_str_keys):
        raise ValueError(
            "This field should contain the name of an env variable, not the credentials string."
        )
    elif env_var not in os.environ:
        raise KeyError("Redshift credentials env variable not found.")
    return


def _redshift_execute_sql_arg_validator(
    sql: str,
    env_var: str,
    return_data: bool,
    return_dict: bool,
) -> None:
    """Validates the redshift_execute_sql arguments and raises clear errors"""
    for arg in [sql, env_var]:
        if not isinstance(arg, str):
            raise TypeError("sql and env_var must be of str type")
    for arg in [return_data, return_dict]:
        if not isinstance(arg, bool):
            raise TypeError("return_data and return_dict must be of bool type")
    return
