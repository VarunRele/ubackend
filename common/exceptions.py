from rest_framework.exceptions import APIException
from rest_framework import status

class VeevaApiFailed(APIException):
    status_code = status.HTTP_400_BAD_REQUEST
    default_detail = "Error in getting details from Veeva"
    default_code = "failed_veeva_api"