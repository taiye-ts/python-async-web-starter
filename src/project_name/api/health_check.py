from aiohttp.web import View, json_response
from aiohttp.web_request import Request
from aiohttp.web_response import Response

from project_name.domain.health_check_service import HealthCheckService


class HealthCheckResource(View):

    def __init__(self, request: Request) -> None:
        super().__init__(request)
        self.service = HealthCheckService()

    async def get(self) -> Response:
        status = self.service.get_status()
        return json_response(status.to_dict())
