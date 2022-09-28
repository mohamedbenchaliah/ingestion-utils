import json
import os
import io

from typing import Dict

from google.cloud import bigquery
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud.bigquery.enums import StandardSqlDataTypes, SqlTypeNames
from ingestor.tools import Log

_logger = Log()
client = bigquery.Client()


class SchemaUpdate:
    def __init__(
        self,
        target_project: str,
        target_dataset: str,
        target_table: str,
        expiration_time: str,
    ):
        """Class responsible for updating metadata of Bigquery tables."""

        self._target_project: str = target_project if target_project else ""
        self._target_dataset: str = target_dataset
        self._target_table: str = target_table
        self.expiration_time: str = expiration_time

    @staticmethod
    def _get_table_file(table_name: str, extension=".csv"):
        """function build path file to get the csv file with the name given in parameter."""

        dataset = table_name
        csv_root_path = "./catalog"
        path = os.path.join(csv_root_path, dataset, table_name + extension)
        print("here path")
        print(path)

        return path

    @staticmethod
    def _get_mode(name: str) -> str:
        """Get mode value for a specific column."""

        if name == "list":
            return "REPEATED"
        else:
            return "NULLABLE"

    @staticmethod
    def _load_in_json(t: str) -> Dict[str, str]:
        """Replace strings."""

        # todo(developer): replace by a regex expression
        return json.loads(
            t.replace("STRUCT<", '{"')
            .replace("ARRAY<", '{"')
            .replace(">", '"}')
            .replace(", ", '","')
            .replace(" ", '"' ':"')
            .replace('"{', "{")
            .replace('}"', "}")
            .replace("}}}}", "}}}")
            .replace("{{", "{")
        )

    @classmethod
    def _transform_into_schema(cls, dict_column, mode, description):
        """Transform a column into BQ schemaField."""

        request = []
        for column_name, data_type in dict_column.items():
            if type(data_type) != dict:
                request.append(bigquery.SchemaField(column_name, data_type, mode, description))
            else:
                if column_name.lower() == "list":
                    request.append(
                        bigquery.SchemaField(
                            column_name,
                            "RECORD",
                            "REPEATED",
                            description,
                            fields=cls._transform_into_schema(data_type, "NULLABLE", description),
                        )
                    )
                else:
                    request.append(
                        bigquery.SchemaField(
                            column_name,
                            "RECORD",
                            mode,
                            description,
                            fields=cls._transform_into_schema(data_type, mode, description),
                        )
                    )
        return request

    def _transform_schema(self, col_dict: Dict[str, str]) -> bigquery.SchemaField:
        """Transform a schema request into BQ tableSchema."""

        supported_data_types = [type.name for type in StandardSqlDataTypes]
        supported_data_types += [type.name for type in SqlTypeNames]
        data_type = col_dict["data_type"].upper()

        if data_type in supported_data_types:
            return bigquery.SchemaField(
                col_dict["column_name"], data_type, self._get_mode(col_dict["column_name"]), col_dict["description"]
            )

        elif data_type.startswith("STRUCT<"):
            return bigquery.SchemaField(
                col_dict["column_name"],
                "RECORD",
                self._get_mode(col_dict["column_name"]),
                col_dict["description"],
                fields=self._transform_into_schema(
                    self._load_in_json(data_type), self._get_mode(col_dict["column_name"]), col_dict["description"]
                ),
            )

        raise BaseException(
            "Unsupported type found: {}, for column: {}. Supported values: {}".format(
                data_type, col_dict["column_name"], supported_data_types
            )
        )

    def get_schema_from_csv(self, table_name: str) -> str:
        """get_schema_from_csv function allow us to get schema
        from csv file and transform it into a spark schema,
        the schema must have 3 columns (column_name, data_type and description)."""

        with open(self._get_table_file(table_name), "r") as file:
            raw_schema = file.readlines()
            extracted_schema = [line.strip().split("|") for line in raw_schema if line][1:]

            schemas = [
                {
                    "column_name": column_schema[0],
                    "data_type": column_schema[1],
                    "mode": column_schema[3],
                    "isPii": False,
                    "description": column_schema[2],
                }
                for column_schema in extracted_schema
                if column_schema != [""]
            ]

        return schemas

    def _update_schema(self) -> None:
        """Main function responsible for updating metadata of Bigquery table."""

        try:
            # get schema from csv catalog file
            control_schema = self.get_schema_from_csv(self._target_table)

            d = []
            for col in control_schema:
                d.append(
                    {
                        "column_name": col["column_name"],
                        "data_type": col["data_type"],
                        "mode": col["mode"],
                        "description": col["description"],
                    }
                )

            bq_schema = [self._transform_schema(c) for c in d]

            table_id = "{}.{}.{}".format(self._target_project, self._target_dataset, self._target_table)
            table = client.get_table(table_id)
            table.schema = bq_schema

            # upload metadata from json catalog
            metadata = json.load(io.open(self._get_table_file(self._target_table, ".json"), "r", encoding="utf-8-sig"))
            table.labels = metadata["labels"]
            table.description = metadata["description"]
            try:
                if metadata["expiration_timestamp"]:
                    table.expiration_timestamp = metadata["expiration_timestamp"].format(self.expiration_time)
                    client.update_table(table, ["schema", "description", "labels", "expires"])
            except KeyError:
                client.update_table(table, ["schema", "description", "labels"])
            _logger.info("Successfully updated the schema of the table `{}`. ".format(self._target_table))

        except NotFound as e:
            _logger.error("Table `{}.{}` not found. Please correct ! ".format(self._target_project, self._target_table))
            raise e

        except Forbidden as e:
            _logger.error(
                "Unexpected error while updating the table `{}.{}`. ".format(self._target_project, self._target_table)
            )
            raise e
