from google.cloud import bigquery
from google.api_core.exceptions import Forbidden, NotFound

from ingestor.tools import Log


_logger = Log()
_BQ_CLIENT = bigquery.Client()


class Bootstrapper:
    @classmethod
    def bootstrap(
        cls, file: str, target_project: str, expiration_time: str = "2999-01-01 00:00:00 UTC", table_prefix: str = ""
    ) -> None:
        """Main function responsible for bootstrapping Bigquery.

        Parameters
        ----------
        file : str
            sql file path
        target_project : str
            target project ID
        expiration_time : str
            table expiration time
        table_prefix : str
            BQ table prefix, e.g. '' | 'tmp_'

        Returns
        -------
        None

        Examples
        --------
        >>> from ingestor.loader._create import Bootstrapper
        >>> Bootstrapper(
        ...    file='../sql/ddl.sql',
        ...    target_project='target_project_id',
        ...    expiration_time='2999-01-01 00:00:00 UTC',
        ...    table_prefix=''
        ...    )
        """

        sql_path = file
        with open(sql_path, "r") as sql:
            query_text = sql.read().format(
                project_id=target_project,
                expiration=expiration_time,
                table_prefix=table_prefix
            )

        try:
            query_job = _BQ_CLIENT.query(query_text)  # Make an API request.
            query_job.result()  # Wait for the job to complete.

            _logger.info("Successfully executed the DDL `{}`. ".format(file))

        except NotFound as e:
            _logger.error("Unexpected error while initializing the DDL `{}`. ".format(file))
            _logger.error("Exception: {}. ".format(str(e)))

        except Forbidden as e:
            _logger.error("Unexpected error while initializing the DDL `{}`. ".format(file))
            _logger.error("Exception: {}. ".format(str(e)))
