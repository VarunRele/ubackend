from common.serializers import DataSerializer
from rest_framework import serializers


class RunReportSerializer(DataSerializer):
    country_id = serializers.CharField(required=False, help_text="Commo-separated list of countries.")

    def validate_country_id(self, value):
        return value.split(",")