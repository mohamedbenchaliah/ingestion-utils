class BigQueryTablesNotFound(Exception):
    """Wrap errors returned by BigQuery table exists requests in an Exception."""

    @staticmethod
    def raise_if_present(errors):
        """Raise this exception if errors is truthy."""
        if errors:
            raise BigQueryTablesNotFound(errors)


class BigQueryInsertError(Exception):
    """Wrap errors returned by BigQuery insertAll requests in an Exception."""

    @staticmethod
    def raise_if_present(errors):
        """Raise this exception if errors is truthy."""
        if errors:
            raise BigQueryInsertError(errors)


class BigQueryUpdateError(Exception):
    """Wrap errors returned by BigQuery Update Table requests in an Exception."""

    @staticmethod
    def raise_if_present(errors):
        """Raise this exception if errors is truthy."""
        if errors:
            raise BigQueryUpdateError(errors)


class BigQueryCreateError(Exception):
    """Wrap errors returned by BigQuery Create Table requests in an Exception."""

    @staticmethod
    def raise_if_present(errors):
        """Raise this exception if errors is truthy."""
        if errors:
            raise BigQueryCreateError(errors)


class QueryException(Exception):
    pass


class GroupingException(Exception):
    pass


class CaseException(Exception):
    pass


class JoinException(Exception):
    pass


class SetOperationException(Exception):
    pass


class RollupException(Exception):
    pass


class DialectNotSupported(Exception):
    pass


class FunctionException(Exception):
    pass
