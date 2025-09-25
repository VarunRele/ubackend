from django.shortcuts import render
from rest_framework import views
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework import status
from common.permissions import HasValidVeevaSession
from common.constants import *
from common.veeva_client import VeevaClient
from common.utils import *
from .serializers import RunReportSerializer
from constance import config
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from .payload import payloads
from .downloads import VeevaMasterReport
import pandas as pd
import numpy as np
import asyncio


class DownloadReport(views.APIView):
    """
    Download project details.
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
        query_serializer=RunReportSerializer,
    )
    def get(self, request: Request, report_name):
        if report_name not in REPORT_NAMES:
            return Response("Invalid report name.", status=status.HTTP_400_BAD_REQUEST)
        serializer = RunReportSerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        valid_data = serializer.validated_data
        session_id = request.headers.get('Authorization')
        assert isinstance(valid_data, dict)
        project_id = valid_data['project_id']
        client = VeevaMasterReport(config.VEEVA_DOMAIN, config.VEEVA_API_VERSION, session_id, config.VEEVA_APP_ID, project_id)
        return_value = asyncio.run(client.run())
        return Response(return_value, status.HTTP_200_OK)