import os

import pandas as pd

import json

from ingestor.tools import Log

_logger = Log()


def _generate_catalog_path_from_table_name(table_name: str, extension=".csv"):
    """Generates file from catalog directory based on table name"""

    dataset = table_name.split("_")[0]
    csv_root_path = "catalog"
    path = os.path.join(csv_root_path, dataset, table_name + extension)

    return path


def _get_all_column_names_of_table_from_catalog(table_name: str):
    """Reads a csv file containing schema for the given table and returns the list of columns for this table."""

    cols = ["column_name"]
    csv_file_path = _generate_catalog_path_from_table_name(table_name)
    csv_file = pd.read_csv(csv_file_path, sep="|", header=0, index_col=False, usecols=cols)

    return csv_file["column_name"].to_list()


def _get_data_quality_column_names_from_catalog(table_name: str):
    """
    Reads catalog file and get a list of all columns to include in data quality computation
    :param table_name: name of table
    :return: list of kpi columns to be calculated
    """

    cols = ["column_name", "data_quality"]
    csv_file_path = _generate_catalog_path_from_table_name(table_name)
    csv_file = pd.read_csv(csv_file_path, sep="|", header=0, index_col=False, usecols=cols)

    # keep only columns to use for data quality computation, if data_qality_column exist
    try:
        csv_file = csv_file[csv_file["data_quality"] == 1]
        return csv_file["column_name"].to_list()
    except Exception as e:
        _logger.info("data_qality column does not exist!")
        return csv_file["column_name"].to_list()


def _load_json_data(json_string):
    """
    function that take a json param string and return all the column's kpi data to be calculate
    :param json_string: json string
    :return: completness_cols, unicity_cols, integrity_cols, validity_cols
    """

    completness_cols = []
    unicity_key_cols = []
    integrity_cols = []
    validity_cols = []

    # deserializes into dict
    json_data = json.loads(json_string)
    # json_data = json.loads(json_string)["data_quality"]
    try:
        completness_cols = json_data["tasks"]["completeness"]
    except Exception as e:
        _logger.info("parametre tasks.completeness absent!")
    try:
        unicity_key_cols = json_data["tasks"]["unicity"]
    except Exception as e:
        _logger.info("parametre tasks.unicity_key_cols absent!")
    try:
        integrity_cols = json_data["tasks"]["integrity"]
    except Exception as e:
        _logger.info("parametre tasks.integrity absent!")
    try:
        validity_cols = json_data["tasks"]["validity"]
        column_names = ["column_name", "validity_type"]
        # validity_cols = pd.DataFrame.from_dict(validity_cols, orient="index")
        validity_cols = pd.DataFrame(validity_cols.items(), columns=column_names)
    except Exception as e:
        _logger.info("parametre tasks.validity absent!")

    return completness_cols, unicity_key_cols, integrity_cols, validity_cols


def _get_data_quality_conf_from_json(table_name):
    """
    function that take a table_name, and generate the corresponding json conf file
    then get each quality kpi configuration
    :param table_name:
    :return: completness_cols, unicity_key_cols, integrity_cols, validity_cols
    """

    json_conf_file_path = _generate_catalog_path_from_table_name(table_name, extension=".json")
    with open(json_conf_file_path, "r") as json_content:
        json_string = json_content.read()
    completness_cols, unicity_key_cols, integrity_cols, validity_cols = _load_json_data(json_string)
    # check if all columns completeness are set to be calculated
    if completness_cols == ["all"]:
        completness_cols = _get_data_quality_column_names_from_catalog(table_name).to_list()
    return completness_cols, unicity_key_cols, integrity_cols, validity_cols
