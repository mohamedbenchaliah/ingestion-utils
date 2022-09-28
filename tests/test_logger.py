# import pytest
# from datetime import datetime as dt
#
# from smh_engine.tools import Log
#
#
# @pytest.mark.e2e
# class TestBigQueryClient:
#
#     @pytest.fixture(scope="module")
#     def logger(self):
#         return Log()
#
#     @pytest.fixture(scope="module")
#     def app_name(self):
#         return 'test_app_name'
#
#     @pytest.fixture(scope="module")
#     def message(self):
#         return 'test_message'
#
#     @pytest.fixture(scope="function")
#     def truncate_io(self, logger):
#         logger.log_string_io.seek(0)
#         logger.log_string_io.truncate(0)
#
#     def returned_output(self, logger):
#         output = logger.log_string_io.getvalue().strip()
#         output = ' '.join(output.split())
#         return output
#
#     def expected_output(self, level):
#         datetime = dt.now().strftime("%Y-%m-%d %H:%M:%S")
#         return f'[LOGGER] {datetime} {level} smh_engine.tools._logger test_message'
#
#     def test_critical_terminates(self, truncate_io, logger, app_name, message):
#         with pytest.raises(SystemExit):
#             logger.critical(message)
#
#     def test_error(self, truncate_io, logger, app_name, message):
#         logger.error(message)
#         assert self.returned_output(logger) == self.expected_output('ERROR')
#
#     def test_warning(self, truncate_io, logger, app_name, message):
#         logger.warning(message)
#         assert self.returned_output(logger) == self.expected_output('WARNING')
#
#     def test_info(self, truncate_io, logger, app_name, message):
#         logger.info(message)
#         assert self.returned_output(logger) == self.expected_output('INFO')
#
#     def test_debug(self, truncate_io, logger, app_name, message):
#         logger.debug(message)
#         assert self.returned_output(logger) == ''
#
#     def test_debug_using_debug_level(self, truncate_io, logger, app_name, message):
#         logger.set_level('DEBUG')
#         logger.debug(message)
#         assert self.returned_output(logger) == self.expected_output('DEBUG')
#
#     def test_set_level(self,truncate_io, logger, app_name):
#         logger.set_level('DEBUG')
#         assert logger.logger.level == 10
#         logger.set_level('INFO')
#
#     def test_set_app_name(self,truncate_io, logger, app_name):
#         logger.set_app_name(app_name)
#         assert logger.logger.name == app_name
#
#
# if __name__ == "__main__":
#     raise SystemExit(pytest.main([__file__]))
