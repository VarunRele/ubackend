from django.shortcuts import render
from django.http import HttpResponse
from rest_framework import views
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework import status
from common.serializers import DataSerializer
from common.permissions import HasValidVeevaSession
from common.constants import *
from common.veeva_client import VeevaClient
from constance import config
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi


class LandingPage(views.APIView):
    """
    When report is open from project.
    """
    permission_classes = [HasValidVeevaSession]
    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter(
                'report_name',
                openapi.IN_PATH,
                description="Name of the report",
                type=openapi.TYPE_STRING,
                enum=REPORT_NAMES
            )
        ],
        query_serializer=DataSerializer,
    )
    def get(self, request: Request, report_name):
        if report_name not in REPORT_NAMES:
            return Response("Invalid report name.", status=status.HTTP_400_BAD_REQUEST)
        serializer = DataSerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        valid_data = serializer.validated_data
        session_id = request.headers.get('Authorization')
        project_id = valid_data['project_id']
        client = VeevaClient(config.VEEVA_DOMAIN, config.VEEVA_API_VERSION, session_id, config.VEEVA_APP_ID)
        payload = f"""SELECT id,name__v FROM project__v WHERE id = '{project_id}'"""
        response = client.query(payload)
        return Response(response, status=status.HTTP_200_OK)
