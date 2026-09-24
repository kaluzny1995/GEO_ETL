from unittest.mock import MagicMock


def mock_F_upload():
    f_mock = MagicMock()
    f_mock.lit.return_value = MagicMock()
    return f_mock


def mock_F_raw2plain(extra_col_methods=None, extra_f_methods=None):
    f_mock = MagicMock()

    def col_side_effect(name):
        col_mock = MagicMock()
        col_mock.__gt__ = MagicMock(return_value=MagicMock())
        if extra_col_methods:
            for method_name in extra_col_methods:
                setattr(col_mock, method_name, MagicMock(return_value=col_mock))
        return col_mock

    f_mock.col.side_effect = col_side_effect
    f_mock.lit.return_value = MagicMock()
    if extra_f_methods:
        for method_name in extra_f_methods:
            setattr(f_mock, method_name, MagicMock(return_value=f_mock))
    return f_mock


def mock_app_config_upload():
    cfg = MagicMock()
    cfg.data.directory = "../GEO_Download/data"
    cfg.logging.directory = "logs"
    cfg.logging.level_int = 20
    cfg.database.db_schema = "geo"
    cfg.database.jdbc_driver = "org.postgresql.Driver"
    return cfg


def mock_app_config_raw2plain():
    cfg = MagicMock()
    cfg.database.db_schema = "geo"
    cfg.database.jdbc_driver = "org.postgresql.Driver"
    cfg.logging.directory = "logs"
    cfg.logging.level_int = 20
    return cfg


def mock_db_config():
    cfg = MagicMock()
    cfg.get_jdbc_url.return_value = "jdbc:postgresql://db.example.com:5432/postgres"
    cfg.get_jdbc_properties.return_value = {
        "user": "user",
        "password": "pass",
        "driver": "org.postgresql.Driver",
    }
    return cfg


def mock_spark_upload(max_file_date=None, count=10):
    spark = MagicMock()
    spark.sparkContext.master = "spark://localhost:7078"

    df = MagicMock()
    df.count.return_value = count
    df.withColumn.return_value = df
    df.unionByName.return_value = df

    jdbc_df = MagicMock()
    jdbc_df.first.return_value = MagicMock(max_file_date=max_file_date)

    spark.read.csv.return_value = df
    spark.read.jdbc.return_value = jdbc_df
    return spark, df


def mock_spark_raw2plain(max_file_date=None, count=5):
    spark = MagicMock()
    spark.sparkContext.master = "spark://localhost:7078"

    raw_df = MagicMock()
    raw_df.count.return_value = count
    raw_df.filter.return_value = raw_df

    plain_df = MagicMock()
    plain_df.count.return_value = count
    raw_df.select.return_value = plain_df

    jdbc_df = MagicMock()
    jdbc_df.first.return_value = MagicMock(max_file_date=max_file_date)

    def read_side_effect(*args, **kwargs):
        table = kwargs.get("table", args[1] if len(args) > 1 else "")
        if "MAX" in str(table):
            return jdbc_df
        return raw_df

    spark.read.jdbc.side_effect = read_side_effect
    return spark, raw_df, plain_df
