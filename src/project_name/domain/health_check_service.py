from project_name.domain.health_check.status import HealthCheckStatus


class HealthCheckService:

    def get_status(self) -> HealthCheckStatus:
        return HealthCheckStatus(
            api=True,
            db=True,
        )
