from __future__ import annotations

from pyspark.sql import SparkSession


def _filesystem(spark: SparkSession, path: str):
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    return jvm_path, jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm_path, fs = _filesystem(spark, path)
    return fs.exists(jvm_path)


def delete_path(spark: SparkSession, path: str) -> None:
    jvm_path, fs = _filesystem(spark, path)
    fs.delete(jvm_path, True)


def write_text(spark: SparkSession, path: str, text: str) -> None:
    jvm_path, fs = _filesystem(spark, path)
    stream = fs.create(jvm_path, True)
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()


def read_text(spark: SparkSession, path: str) -> str | None:
    if not path_exists(spark, path):
        return None
    return spark.read.text(path, wholetext=True).first()[0]
