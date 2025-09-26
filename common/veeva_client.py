import requests
from .constants import *
from .exceptions import VeevaApiFailed
import logging
import asyncio
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VeevaClient:
    def __init__(self, veeva_domain: str, veeva_api_version: str, session_id: str, app_id: str):
        self.veeva_domain = veeva_domain
        self.veeva_api_version = veeva_api_version
        self.session_id = session_id
        self.app_id = app_id

    def _get_header(self, content_type='application/json'):
        return {
            "Authorization": self.session_id,
            "Accept": "application/json",
            "Content-Type": content_type,
            "X-VaultAPI-DescribeQuery": "false",
            "X-VaultAPI-ClientID": self.app_id
        }

    async def query(self, query_string, log: str | None =None) -> list[dict]:
        if log is not None:
            logger.info(f"{log}:Query Execution")
        url = f"{self.veeva_domain}/api/{self.veeva_api_version}/query"
        headers = self._get_header(content_type="application/x-www-form-urlencoded")
        payload = f"q={query_string}"
        data_list = []
        try:
            async with aiohttp.ClientSession() as session:
                while True:
                    async with session.post(url, headers=headers, data=payload) as resp:
                        value = await resp.json()
                    status = value.get("responseStatus", "").lower()
                    if status not in {SUCCESS, WARNING}:
                        raise VeevaApiFailed(value)
                    data_list.extend(value.get("data", []))
                    details = value.get("responseDetails", {})
                    if "next_page" not in details:
                        break
                    url = f"{self.veeva_domain}{details['next_page']}" 
        except Exception as ex:
            logger.error(ex)
        return data_list