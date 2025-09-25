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
        project_id = valid_data['project_id']
        client = VeevaMasterReport(config.VEEVA_DOMAIN, config.VEEVA_API_VERSION, session_id, config.VEEVA_APP_ID, project_id)
        return_value = client.run()
        # client = VeevaClient(config.VEEVA_DOMAIN, config.VEEVA_API_VERSION, session_id, config.VEEVA_APP_ID)
        # return_value = {}
        # return_value['project_details'] = client.query(payloads['project_details'].format(project_id=project_id), f'{report_name}:project_details')
        # return_value['project_geography_details'] = client.query(payloads['project_geography_details'].format(project_id=project_id), f'{report_name}:project_geography_details')
        # return_value['project_team_details'] = client.query(payloads['project_team_details'].format(project_id=project_id), f'{report_name}:project_team_details')
        # return_value['project_product_details'] = client.query(payloads['project_product_details'].format(project_id=project_id), f'{report_name}:project_project_details')

        # project_product_details_df = pd.DataFrame(return_value['project_product_details'])
        # products_to_query_project = get_query_params(project_product_details_df, 'product__v')
        # return_value['product_details'] = client.query(payloads['product_details'].format(products_to_query=products_to_query_project), f'{report_name}:product_details')

        # product_details_df = pd.DataFrame(return_value['product_details'])
        # products_to_query_by_tech_id = get_query_params(product_details_df, 'technology_id__c') + ',' + get_query_params(product_details_df, 'technology_id_1__c')
        # return_value['technology_product_details'] = client.query(payloads['technology_product_details'].format(products_to_query=products_to_query_by_tech_id), f'{report_name}:technology_product_details')

        # return_value['product_child_composition_cuc'] = client.query(payloads['product_child_composition_cuc'].format(products_to_query=products_to_query_project ), f'{report_name}:product_child_composition_cuc')
        # return_value['project_claims_details'] = client.query(payloads['project_claims_details'].format(project_id=project_id), f'{report_name}:project_claims_details')

        # project_claims_details_df = pd.DataFrame(return_value['project_claims_details'])
        # claims_to_query = get_query_params(project_claims_details_df, 'claim__v')
        # claims_list = client.query(payloads['claims_details'].format(claims_to_query=claims_to_query, products_to_query=products_to_query_project ), f'{report_name}:claims_details')
        # claims_list += client.query(payloads['claims_details_tech_products'].format(products_to_query=products_to_query_by_tech_id), f'{report_name}:claims_details_tech_products')
        # claims_list_df = pd.DataFrame(claims_list)
        # if not claims_list_df.empty:
        #     claims_list_df = claims_list_df.drop_duplicates(subset=['id'])
        #     claims_list_df['claim_order__c'] = claims_list_df['claim_order__c'].astype("Int64")
        #     claims_list_df = claims_list_df.sort_values(by='claim_order__c', ascending=True, na_position='last')
        # return_value['claims_details'] = claims_list_df.to_dict(orient='records')
        
        # claims_details_df = pd.DataFrame(return_value['claims_details'])
        # claims_to_query = get_query_params(claims_details_df, 'id')
        # return_value['claim_substantiation_join_details'] = client.query(payloads['claim_substantiation_join_details'].format(claims_to_query=claims_to_query), f'{report_name}:claim_substantiation_join_details')

        # claim_substantiation_join_details_df = pd.DataFrame(return_value['claim_substantiation_join_details'])
        # subtantiations_to_query = get_query_params(claim_substantiation_join_details_df, 'substantiation__v')
        # return_value['substantiation_claims_details'] = client.query(payloads['substantiation_claims_details'].format(subtantiations_to_query=subtantiations_to_query), f'{report_name}:substantiation_claims_details')
        # return_value['claim_risk_assessment_details'] = client.query(payloads['claim_risk_assessment_details'].format(claims_to_query=claims_to_query), f'{report_name}:claim_risk_assessment_details')

        # claim_risk_assessment_details_df = pd.DataFrame(return_value['claim_risk_assessment_details'])
        # risks_to_query = get_query_params(claim_risk_assessment_details_df, 'risk_assessment__c')
        # return_value['risk_assessment_details_claim'] = client.query(payloads['risk_assessment_details_claim'].format(risks_to_query=risks_to_query), f'{report_name}:risk_assessment_details_claim')
        # return_value['project_local_adaptation_details'] = client.query(payloads['project_local_adaptation_details'].format(project_id=project_id), f'{report_name}:project_local_adaptation_details')

        # project_local_adaptation_details_df = pd.DataFrame(return_value['project_local_adaptation_details'])
        # las_to_query = get_query_params(project_local_adaptation_details_df, 'local_adaptation__v')
        # las_list = client.query(payloads['local_adaptation_details'].format(las_to_query=las_to_query, products_to_query=products_to_query_project), f'{report_name}:local_adaptation_details')
        # las_list += client.query(payloads['local_adaptation_details_tech_products'].format(products_to_query=products_to_query_by_tech_id), f'{report_name}:local_adaptation_details_tech_products')
        # las_list_df = pd.DataFrame(las_list)
        # if not las_list_df.empty:
        #     las_list_df = las_list_df.drop_duplicates(subset=['id'])
        #     las_list_df['claim_order__c'] = las_list_df['claim_order__c'].astype('Int64')
        #     las_list_df = las_list_df.sort_values(by='claim_order__c', ascending=True, na_position='last')
        # return_value['local_adaptation_details'] = las_list_df.to_dict(orient='records')

        # local_adaptation_details_df = pd.DataFrame(return_value['local_adaptation_details'])
        # las_to_query = get_query_params(local_adaptation_details_df, 'id')
        # return_value['local_adaptation_substantiation_join_details'] = client.query(payloads['local_adaptation_substantiation_join_details'].format(las_to_query=las_to_query), f'{report_name}:local_adaptation_substantiation_join_details')

        # local_adaptation_substantiation_join_details_df = pd.DataFrame(return_value['local_adaptation_substantiation_join_details'])
        # substantiations_to_query_la = get_query_params(local_adaptation_substantiation_join_details_df, 'substantiation__c')
        # return_value['substantiation_local_adaptation_details'] = client.query(payloads['substantiation_local_adaptation_details'].format(substantiations_to_query=substantiations_to_query_la), f'{report_name}:substantiation_local_adaptation_details')
        # return_value['local_adaptation_risk_assessment_details'] = client.query(payloads['local_adaptation_risk_assessment_details'].format(las_to_query=las_to_query), f'{report_name}:local_adaptation_risk_assement_details')

        # local_adaptation_risk_assessment_details_df = pd.DataFrame(return_value['local_adaptation_risk_assessment_details'])
        # risks_to_query_la = get_query_params(local_adaptation_risk_assessment_details_df, 'risk_assessment__c')
        # return_value['risk_assessment_details_la'] = client.query(payloads['risk_assessment_details_la'].format(risks_to_query=risks_to_query_la), f'{report_name}:risk_assessment_details_la')
        # return_value['project_assets'] = client.query(payloads['project_assets'].format(project_id=project_id), f'{report_name}:project_assets')

        # project_details_df = pd.DataFrame(return_value['project_details'])
        # risks_assessment_details_claim_df = pd.DataFrame(return_value['risk_assessment_details_claim'])
        # risks_assessment_details_la_df = pd.DataFrame(return_value['risk_assessment_details_la'])
        # project_assets_df = pd.DataFrame(return_value['project_assets'])
        # user_ids = get_query_params(project_details_df, 'created_by__v') 
        # user_ids += ',' + get_query_params(risks_assessment_details_claim_df, 'created_by__v')
        # user_ids += ',' + get_query_params(risks_assessment_details_la_df, 'created_by__v')
        # user_ids += ',' + get_query_params(project_assets_df, 'created_by__v')
        # return_value['user_details_by_id'] = client.query(payloads['user_details_by_id'].format(user_id_string=user_ids), f'{report_name}:user_details_by_id')

        # product_child_composition_cuc_df = pd.DataFrame(return_value['product_child_composition_cuc'])        
        # geographies_to_query = get_query_params(product_child_composition_cuc_df, 'child__vr.geography__c')
        # return_value['cuc_geography_details'] = client.query(payloads['cuc_geography_details'].format(geographies_to_query=geographies_to_query), f'{report_name}:cuc_geography_details')

        # formulation_doc_details_list = []
        # product_child_composition_cuc_df = product_child_composition_cuc_df.dropna(subset=['child__vr.formulation_document__c'])
        # for idx, row in product_child_composition_cuc_df.iterrows():
        #     doc_id_to_query = row['child__vr.formulation_document__c'].split("_")[0]
        #     major_verison_no = row['child__vr.formulation_document__c'].split("_")[1]
        #     minor_verison_no = row['child__vr.formulation_document__c'].split("_")[2]
        #     formulation_doc_details_list += client.query(payloads['formulation_doc_details'].format(doc_id_to_query=doc_id_to_query, major_verison_no=major_verison_no, minor_verison_no=minor_verison_no), f'{report_name}:formulation_doc_details')
        # return_value['formulation_doc_details'] = formulation_doc_details_list

        # country_id_string = get_query_params_list_element(project_assets_df, 'country__v')
        # return_value['assets_country_details_by_id'] = client.query(payloads['assets_country_details_by_id'].format(country_id_string=country_id_string), f'{report_name}:assets_country_details_by_id')

        # claim_id_string = get_query_params_list_element(project_assets_df, 'related_claims__c')
        # return_value['assets_claim_details_by_id'] = client.query(payloads['assets_claim_details_by_id'].format(claim_id_string=claim_id_string), f'{report_name}:assets_claim_details_by_id')

        # la_id_string = get_query_params_list_element(project_assets_df, 'related_local_adaptations__c')
        # return_value['assets_la_details_by_id'] = client.query(payloads['assets_la_details_by_id'].format(la_id_string=la_id_string), f'{report_name}:assets_la_details_by_id')

        return Response(return_value, status.HTTP_200_OK)