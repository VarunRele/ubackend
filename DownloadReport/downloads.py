from common.veeva_client import VeevaClient
from common.utils import *
from .payload import payloads
import asyncio
from .constants import *


class DownloadReport(VeevaClient):
    def __init__(self, veeva_domain, veeva_api_version, session_id, app_id, project_id):
        self.project_id = project_id
        self.queries = {}
        self.return_value = {}
        super().__init__(veeva_domain, veeva_api_version, session_id, app_id)

    def get_details(self, key: str):
        """
        Project - project_details\n
        Project Geography - project_geography_details\n
        Project Team - project_team_details\n
        Project Product - project_product_details\n
        Project Claims - project_claims_details
        """
        return self.query(payloads[key].format(project_id=self.project_id), f'{self.report_name}:{key}')

    def get_query_val(self, value):
        if value in self.queries:
            return self.queries[value]
        if value == PROJECT_PRODUCT_DETAILS:
            df = pd.DataFrame(self.return_value[PROJECT_PRODUCT_DETAILS])
            result = get_query_params(df, PRODUCT__V)
        elif value == PRODUCT_DETAILS_TECH:
            df = pd.DataFrame(self.return_value[PRODUCT_DETAILS])
            result = get_query_params(df, TECHNOLOGY_ID_C) + ',' + get_query_params(df, TECHNOLOGY_ID_1_C)
        elif value == PROJECT_CLAIMS_DETAILS:
            df = pd.DataFrame(self.return_value[PROJECT_CLAIMS_DETAILS])
            result = get_query_params(df, CLAIM_V)
        self.queries[value] = result
        return result

    def get_product_details(self):
        products_to_query_project = self.get_query_val(PROJECT_PRODUCT_DETAILS)
        return self.query(payloads[PRODUCT_DETAILS].format(products_to_query=products_to_query_project), f'{self.report_name}:product_details')

    def get_technology_product_details(self):
        products_to_query_by_tech_id = self.get_query_val(PRODUCT_DETAILS_TECH)
        return self.query(payloads[TECHNOLOGY_PRODUCT_DETAILS].format(products_to_query=products_to_query_by_tech_id), f'{self.report_name}:technology_product_details')

    def get_product_child_composition_cuc(self):
        products_to_query_project = self.get_query_val(PROJECT_PRODUCT_DETAILS)
        return self.query(payloads[PRODUCT_CHILD_COMPOSITION_CUC].format(products_to_query=products_to_query_project ), f'{self.report_name}:product_child_composition_cuc')

    def get_claims_details(self):
        claims_to_query = self.get_query_val(PROJECT_CLAIMS_DETAILS)
        products_to_query_project = self.get_query_val(PROJECT_PRODUCT_DETAILS)
        products_to_query_by_tech_id = self.get_query_val(PRODUCT_DETAILS_TECH)
        claims_list = self.query(payloads[CLAIMS_DETAILS].format(claims_to_query=claims_to_query, products_to_query=products_to_query_project ), f'{self.report_name}:claims_details')
        claims_list += self.query(payloads['claims_details_tech_products'].format(products_to_query=products_to_query_by_tech_id), f'{self.report_name}:claims_details_tech_products')
        claims_list_df = pd.DataFrame(claims_list)
        if not claims_list_df.empty:
            claims_list_df = claims_list_df.drop_duplicates(subset=['id'])
            claims_list_df['claim_order__c'] = claims_list_df['claim_order__c'].astype("Int64")
            claims_list_df = claims_list_df.sort_values(by='claim_order__c', ascending=True, na_position='last')
        return claims_list_df.to_dict(orient='records')
    
class VeevaMasterReport(DownloadReport):
    report_name = "veeva_master"
    def run(self):
        self.return_value[PROJECT_DETAILS] = self.get_details(PROJECT_DETAILS)
        self.return_value[PROJECT_GEOGRAPHY_DETAILS] = self.get_details(PROJECT_GEOGRAPHY_DETAILS)
        self.return_value[PROJECT_TEAM_DETAILS] = self.get_details(PROJECT_TEAM_DETAILS)
        self.return_value[PROJECT_PRODUCT_DETAILS] = self.get_details(PROJECT_PRODUCT_DETAILS)
        self.return_value[PRODUCT_DETAILS] = self.get_product_details()
        self.return_value[TECHNOLOGY_PRODUCT_DETAILS] = self.get_technology_product_details()
        self.return_value[PRODUCT_CHILD_COMPOSITION_CUC] = self.get_product_child_composition_cuc()
        self.return_value[PROJECT_CLAIMS_DETAILS] = self.get_details(PROJECT_CLAIMS_DETAILS)
        self.return_value[CLAIMS_DETAILS] = self.get_claims_details()
        return self.return_value