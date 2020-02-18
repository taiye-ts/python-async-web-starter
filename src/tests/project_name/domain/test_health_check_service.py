from unittest import TestCase

from project_name.domain.health_check.status import HealthCheckStatus
from project_name.domain.health_check_service import HealthCheckService


class HealthCheckServiceTestCase(TestCase):

    def setUp(self) -> None:
        self.service = HealthCheckService()

    def test_should_return_correct_result(self) -> None:
        result = self.service.get_status()
        self.assertIsInstance(result, HealthCheckStatus)
        self.assertTrue(result.api)
        self.assertTrue(result.db)
