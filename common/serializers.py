from rest_framework import serializers


class DataSerializer(serializers.Serializer):
    project_id = serializers.CharField(help_text="Project ID")