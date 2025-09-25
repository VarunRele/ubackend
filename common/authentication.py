from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed
from django.contrib.auth.models import AnonymousUser
import requests
from constance import config
from .constants import *
import json


class VeevaSessionAuthentication(BaseAuthentication):
    def authenticate(self, request):
        session_id = request.headers.get('Authorization')
        if not session_id:
            return None
        app_id = config.VEEVA_APP_ID
        veeva_domain = config.VEEVA_DOMAIN
        veeva_api_version = config.VEEVA_API_VERSION
        url = f"{veeva_domain}/api/{veeva_api_version}/objects/users/me"
        headers = {
            "Authorization": session_id,
            "Accept": "application/json",
            "X-VaultAPI-ClientID": app_id
        }
        response = requests.get(url, headers=headers)
        session_details = response.json()
        if session_details.get('responseStatus', '').lower() != SUCCESS:
            raise AuthenticationFailed("Invalid session ID. Please login again")
        request.veeva_user_id = session_details['users'][0]['user']['id']
        user = AnonymousUser()
        return (user, None)



