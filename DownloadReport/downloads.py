from common.veeva_client import VeevaClient
from common.utils import *
from .payload import payloads
import asyncio
from .constants import *
from typing import Callable, Awaitable
from collections import defaultdict


class DownloadReport(VeevaClient):
    def __init__(self, veeva_domain, veeva_api_version, session_id, app_id, project_id):
        self.project_id = project_id
        self.dataframes: dict[str, pd.DataFrame] = {}
        self.return_value = {}
        self.tasks = {}
        self.report_name = "DownloadReport"
        super().__init__(veeva_domain, veeva_api_version, session_id, app_id)

    async def get_details(self, key: str) -> list[dict]:
        """
        Project - project_details\n
        Project Geography - project_geography_details\n
        Project Team - project_team_details\n
        Project Product - project_product_details\n
        Project Claims - project_claims_details
        """
        return await self.query(payloads[key].format(project_id=self.project_id), f'{self.report_name}:{key}')

    def get_df(self, table_name: str) -> pd.DataFrame:
        if table_name in self.dataframes:
            df = self.dataframes[table_name]
        else:
            df = pd.DataFrame(self.return_value[table_name])
            self.dataframes[table_name] = df
        return df

    def get_query_val(self, table_name: str, columns: tuple) -> str:
        """
        Value - get query params. Dependency
        """
        if COUNTRY__V in columns or RELATED_CLAIMS__C in columns or RELATED_LOCAL_ADAPTATIONS__C in columns:
            query_fn = get_query_params_list_element
        else:
            query_fn = get_query_params
        df = self.get_df(table_name)
        result = []
        for column in columns:
            result.append(query_fn(df, column))
        result = ','.join(result)
        return result

    async def get_product_details(self) -> list[dict]:
        products_to_query_project = self.get_query_val(PROJECT_PRODUCT_DETAILS, (PRODUCT__V,))
        return await self.query(payloads[PRODUCT_DETAILS].format(products_to_query=products_to_query_project), f'{self.report_name}:{PRODUCT_DETAILS}')

    async def get_technology_product_details(self) -> list[dict]:
        products_to_query_by_tech_id = self.get_query_val(PRODUCT_DETAILS, (TECHNOLOGY_ID_C, TECHNOLOGY_ID_1_C))
        return await self.query(payloads[TECHNOLOGY_PRODUCT_DETAILS].format(products_to_query=products_to_query_by_tech_id), f'{self.report_name}:{TECHNOLOGY_PRODUCT_DETAILS}')

    async def get_product_child_composition_cuc(self) -> list[dict]:
        products_to_query_project = self.get_query_val(PROJECT_PRODUCT_DETAILS, (PRODUCT__V,))
        return await self.query(payloads[PRODUCT_CHILD_COMPOSITION_CUC].format(products_to_query=products_to_query_project ), f'{self.report_name}:{PRODUCT_CHILD_COMPOSITION_CUC}')

    async def get_claims_details(self) -> list[dict]:
        claims_to_query = self.get_query_val(PROJECT_CLAIMS_DETAILS, (CLAIM_V,))
        products_to_query_project = self.get_query_val(PROJECT_PRODUCT_DETAILS, (PRODUCT__V,))
        products_to_query_by_tech_id = self.get_query_val(PRODUCT_DETAILS, (TECHNOLOGY_ID_C, TECHNOLOGY_ID_1_C))
        claims_list = await self.query(payloads[CLAIMS_DETAILS].format(claims_to_query=claims_to_query, products_to_query=products_to_query_project ), f'{self.report_name}:{CLAIMS_DETAILS}')
        claims_list += await self.query(payloads[CLAIMS_DETAILS_TECH_PRODUCT].format(products_to_query=products_to_query_by_tech_id), f'{self.report_name}:{CLAIMS_DETAILS_TECH_PRODUCT}')
        claims_list_df = pd.DataFrame(claims_list)
        if not claims_list_df.empty:
            claims_list_df = claims_list_df.drop_duplicates(subset=[ID])
            claims_list_df[CLAIM_ORDER_C] = claims_list_df[CLAIM_ORDER_C].astype("Int64")
            claims_list_df = claims_list_df.sort_values(by=CLAIM_ORDER_C, ascending=True, na_position='last')
        return claims_list_df.to_dict(orient='records')

    async def get_claim_substantiation_join_details(self) -> list[dict]:
        claims_to_query = self.get_query_val(CLAIMS_DETAILS, (ID,))
        return await self.query(payloads[CLAIM_SUBSTANTIATION_JOIN_DETAILS].format(claims_to_query=claims_to_query), f'{self.report_name}:{CLAIM_SUBSTANTIATION_JOIN_DETAILS}')

    async def get_substantiation_claims_details(self) -> list[dict]:
        subtantiations_to_query = self.get_query_val(CLAIM_SUBSTANTIATION_JOIN_DETAILS, (SUBSTANTIATION__V,))
        return await self.query(payloads[SUBSTANTIATION_CLAIMS_DETAILS].format(subtantiations_to_query=subtantiations_to_query), f'{self.report_name}:{SUBSTANTIATION_CLAIMS_DETAILS}')

    async def get_claim_risk_assessment_details(self) -> list[dict]:
        claims_to_query = self.get_query_val(CLAIMS_DETAILS, (ID,))
        return await self.query(payloads[CLAIM_RISK_ASSESSMENT_DETAILS].format(claims_to_query=claims_to_query), f'{self.report_name}:{CLAIM_RISK_ASSESSMENT_DETAILS}')

    async def get_risk_assessment_details_claim(self) -> list[dict]:
        risks_to_query = self.get_query_val(CLAIM_RISK_ASSESSMENT_DETAILS, (RISK_ASSESSMENT__C,))
        return await self.query(payloads[RISK_ASSESSMENT_DETAILS_CLAIM].format(risks_to_query=risks_to_query), f'{self.report_name}:{RISK_ASSESSMENT_DETAILS_CLAIM}')

    async def get_local_adaptation_details(self) -> list[dict]:
        products_to_query_project = self.get_query_val(PROJECT_PRODUCT_DETAILS, (PRODUCT__V,))
        products_to_query_by_tech_id = self.get_query_val(PRODUCT_DETAILS, (TECHNOLOGY_ID_C, TECHNOLOGY_ID_1_C))
        las_to_query = self.get_query_val(PROJECT_LOCAL_ADAPTATION_DETAILS, (LOCAL_ADAPTATION__V,))
        las_list = await self.query(payloads[LOCAL_ADAPTATION_DETAILS].format(las_to_query=las_to_query, products_to_query=products_to_query_project), f'{self.report_name}:{LOCAL_ADAPTATION_DETAILS}')
        las_list += await self.query(payloads[LOCAL_ADAPTATION_DETAILS_TECH_PRODUCTS].format(products_to_query=products_to_query_by_tech_id), f'{self.report_name}:{LOCAL_ADAPTATION_DETAILS_TECH_PRODUCTS}')
        las_list_df = pd.DataFrame(las_list)
        if not las_list_df.empty:
            las_list_df = las_list_df.drop_duplicates(subset=[ID])
            las_list_df[CLAIM_ORDER_C] = las_list_df[CLAIM_ORDER_C].astype('Int64')
            las_list_df = las_list_df.sort_values(by=CLAIM_ORDER_C, ascending=True, na_position='last')
        return las_list_df.to_dict(orient='records')

    async def get_local_adaptation_substantiation_join_details(self) -> list[dict]:
        las_to_query = self.get_query_val(LOCAL_ADAPTATION_DETAILS, (ID,))
        return await self.query(payloads[LOCAL_ADAPTATION_SUBSTANTIATION_JOIN_DETAILS].format(las_to_query=las_to_query), f'{self.report_name}:{LOCAL_ADAPTATION_SUBSTANTIATION_JOIN_DETAILS}')

    async def get_substantiation_local_adaptation_details(self) -> list[dict]:
        substantiations_to_query_la = self.get_query_val(LOCAL_ADAPTATION_SUBSTANTIATION_JOIN_DETAILS, (SUBSTANTIATION__C,))
        return await self.query(payloads[SUBSTANTIATION_LOCAL_ADAPTATION_DETAILS].format(substantiations_to_query=substantiations_to_query_la), f'{self.report_name}:{SUBSTANTIATION_LOCAL_ADAPTATION_DETAILS}')
    
    async def get_local_adaptation_risk_assessment_details(self) -> list[dict]:
        las_to_query = self.get_query_val(LOCAL_ADAPTATION_DETAILS, (ID,))
        return await self.query(payloads[LOCAL_ADAPTATION_RISK_ASSESSMENT_DETAILS].format(las_to_query=las_to_query), f'{self.report_name}:{LOCAL_ADAPTATION_RISK_ASSESSMENT_DETAILS}')

    async def get_risk_assessment_details_la(self) -> list[dict]:
        risks_to_query_la = self.get_query_val(LOCAL_ADAPTATION_RISK_ASSESSMENT_DETAILS, (RISK_ASSESSMENT__C,))
        return await self.query(payloads[RISK_ASSESSMENT_DETAILS_LA].format(risks_to_query=risks_to_query_la), f'{self.report_name}:{RISK_ASSESSMENT_DETAILS_LA}')

    async def get_user_details_by_id(self) -> list[dict]:
        user_ids = self.get_query_val(PROJECT_DETAILS, (CREATED_BY__V,)) 
        user_ids += ',' + self.get_query_val(RISK_ASSESSMENT_DETAILS_CLAIM, (CREATED_BY__V,))
        user_ids += ',' + self.get_query_val(RISK_ASSESSMENT_DETAILS_LA, (CREATED_BY__V,))
        user_ids += ',' + self.get_query_val(PROJECT_ASSETS, (CREATED_BY__V,))
        return await self.query(payloads[USER_DETAILS_BY_ID].format(user_id_string=user_ids), f'{self.report_name}:{USER_DETAILS_BY_ID}')

    async def get_cuc_geography_details(self) -> list[dict]:
        geographies_to_query = self.get_query_val(PRODUCT_CHILD_COMPOSITION_CUC, (CHILD__VR_GEOGRAPHY__C,))
        return await self.query(payloads[CUC_GEOGRAPHY_DETAILS].format(geographies_to_query=geographies_to_query), f'{self.report_name}:{CUC_GEOGRAPHY_DETAILS}')

    async def get_formulation_doc_details(self) -> list[dict]:
        formulation_doc_details_list = []
        product_child_composition_cuc_df = pd.DataFrame(self.return_value[PRODUCT_CHILD_COMPOSITION_CUC])        
        product_child_composition_cuc_df = product_child_composition_cuc_df.dropna(subset=[CHILD__VR_FORMULATION_DOCUMENT__C])
        for idx, row in product_child_composition_cuc_df.iterrows():
            doc_id_to_query = row[CHILD__VR_FORMULATION_DOCUMENT__C].split("_")[0]
            major_verison_no = row[CHILD__VR_FORMULATION_DOCUMENT__C].split("_")[1]
            minor_verison_no = row[CHILD__VR_FORMULATION_DOCUMENT__C].split("_")[2]
            formulation_doc_details_list += await self.query(payloads[FORMULATION_DOC_DETAILS].format(doc_id_to_query=doc_id_to_query, major_verison_no=major_verison_no, minor_verison_no=minor_verison_no), f'{self.report_name}:{FORMULATION_DOC_DETAILS}')
        return formulation_doc_details_list
    
    async def get_assets_country_details_by_id(self) -> list[dict]:
        country_id_string = self.get_query_val(PROJECT_ASSETS, (COUNTRY__V,))
        return await self.query(payloads[ASSETS_COUNTRY_DETAILS_BY_ID].format(country_id_string=country_id_string), f'{self.report_name}:{ASSETS_COUNTRY_DETAILS_BY_ID}')

    async def get_assets_claim_details_by_id(self) -> list[dict]:
        claim_id_string = self.get_query_val(PROJECT_ASSETS, (RELATED_CLAIMS__C,))
        return await self.query(payloads[ASSETS_CLAIM_DETAILS_BY_ID].format(claim_id_string=claim_id_string), f'{self.report_name}:{ASSETS_CLAIM_DETAILS_BY_ID}')

    async def get_assets_la_details_by_id(self) -> list[dict]:
        la_id_string = self.get_query_val(PROJECT_ASSETS, (RELATED_LOCAL_ADAPTATIONS__C,))
        return await self.query(payloads[ASSETS_LA_DETAILS_BY_ID].format(la_id_string=la_id_string), f'{self.report_name}:{ASSETS_LA_DETAILS_BY_ID}')

    def combine_project(self):
        project_df = self.get_df(PROJECT_DETAILS)
        user_df = self.get_df(USER_DETAILS_BY_ID)
        if project_df.empty:
            return
        project_df[CREATED_BY_NAME] = project_df[CREATED_BY__V].apply(lambda x: get_details_by_id(x, user_df).get(NAME__V))
        self.return_value[PROJECT_DETAILS] = project_df.to_dict(orient='records')

    def combine_technology_product_data(self):
        technology_df = self.get_df(TECHNOLOGY_PRODUCT_DETAILS)
        if technology_df.empty:
            return 
        technology_df[FORMULATION_DOCUMENT__C_FORMULATION__LINK] = technology_df[FORMULATION_DOCUMENT__C]\
            .apply(lambda x: f"{self.veeva_domain}{get_technology_formulation_link(x)}" if x is not None else None)
        technology_df[FORMULATION_DOCUMENT__CR_ID] = technology_df[FORMULATION_DOCUMENT__CR_ID].astype('Int64')
        self.return_value[TECHNOLOGY_PRODUCT_DETAILS] = technology_df.to_dict(orient='records')

    def combine_product_child_composition(self):
        child_cuc_df = self.get_df(PRODUCT_CHILD_COMPOSITION_CUC)
        cuc_geo_df = self.get_df(CUC_GEOGRAPHY_DETAILS)
        formulation_doc_details_df = self.get_df(FORMULATION_DOC_DETAILS)
        if child_cuc_df.empty:
            return
        child_cuc_df[CHILD__VR_GEOGRAPHY_NAME] = child_cuc_df[CHILD__VR_GEOGRAPHY__C]\
            .apply(lambda x: get_details_by_id(x, cuc_geo_df)[NAME__V] if x is not None else None)
        child_cuc_df[[
            CHILD__VR_FORMULATION_DOCUMENT_ID, 
            CHILD__VR_FORMULATION_DOCUMENT_NAME, 
            CHILD__VR_FORMULATION_DOCUMENT_FILENAME,
            FORMULATION_DOCUMENT_MIME_TYPE,
            CHILD__VR_FORMULATION_MINOR_VERSION_NUMBER,
            CHILD__VR_FORMULATION_MAJOR_VERSION_NUMBER,
            CHILD__VR_FORMULATION_LINK
        ]] = \
            child_cuc_df[CHILD__VR_FORMULATION_DOCUMENT__C].apply(lambda x:get_formulation_doc_details_by_id(x, formulation_doc_details_df))
        child_cuc_df[[CHILD__VR_FORMULATION_DOCUMENT_ID, CHILD__VR_FORMULATION_MAJOR_VERSION_NUMBER, CHILD__VR_FORMULATION_MINOR_VERSION_NUMBER]] = child_cuc_df[[
            CHILD__VR_FORMULATION_DOCUMENT_ID, CHILD__VR_FORMULATION_MAJOR_VERSION_NUMBER, CHILD__VR_FORMULATION_MINOR_VERSION_NUMBER
            ]].astype('Int64')
        child_cuc_df[CHILD__VR_FORMULATION_LINK] = child_cuc_df[CHILD__VR_FORMULATION_LINK].apply(lambda x: f"{self.veeva_domain}{x}" if x else None)
        self.return_value[PRODUCT_CHILD_COMPOSITION_CUC] = child_cuc_df.to_dict(orient='records')

    def combine_claim(self):
        claim_df = self.get_df(CLAIMS_DETAILS)
        product_df = self.get_df(PRODUCT_DETAILS)
        claim_substantiation_df = self.get_df(CLAIM_SUBSTANTIATION_JOIN_DETAILS)
        claim_risk_df = self.get_df(CLAIM_RISK_ASSESSMENT_DETAILS)
        if claim_df.empty:
            return
        claim_df = calculate_row_count_per_claim_df(claim_df, claim_substantiation_df, claim_risk_df)
        product_lookup = product_df.set_index(ID)[SUBRANGE1__C].to_dict()
        tech_to_product = defaultdict(list)
        for _, row in product_df.iterrows():
            if row[TECHNOLOGY_ID_C]:
                tech_to_product[row[TECHNOLOGY_ID_C]].append(row[ID])
            if row[TECHNOLOGY_ID_1_C]:
                tech_to_product[row[TECHNOLOGY_ID_1_C]].append(row[ID])
        claim_df[SUBRANGE] = claim_df[PRODUCT__V].map(product_lookup)
        claim_df[SUBRANGE] = claim_df[SUBRANGE].astype(bool)
        claim_df[TECHNOLOGY] = claim_df[PRODUCT__V].apply(lambda p: tech_to_product.get(p, []))
        claim_df[CLAIM_ORDER_C] = claim_df[CLAIM_ORDER_C].astype('Int64')
        self.return_value[CLAIMS_DETAILS] = claim_df.to_dict(orient='records')

    async def run(self):
        pending = set(self.tasks.keys())
        while pending:
            runnable = [key for key in pending if all(dep in self.return_value for dep in self.tasks[key][0])]
            if not runnable:
                raise RuntimeError(f"Circular or missing dependencies in {pending}")
            results = await asyncio.gather(*[self.tasks[key][1]() for key in runnable])
            for key, result in zip(runnable, results):
                self.return_value[key] = result
                pending.remove(key)
    
class VeevaMasterReport(DownloadReport):
    report_name = "veeva_master"
    def __init__(self, veeva_domain, veeva_api_version, session_id, app_id, project_id):
        super().__init__(veeva_domain, veeva_api_version, session_id, app_id, project_id)
        self.tasks: dict[str, tuple[list[str], Callable[[], Awaitable]]] = {
            PROJECT_DETAILS: ([], lambda: self.get_details(PROJECT_DETAILS)), # dep - Dependency. ([list of dep], function)
            PROJECT_GEOGRAPHY_DETAILS: ([], lambda: self.get_details(PROJECT_GEOGRAPHY_DETAILS)),
            PROJECT_TEAM_DETAILS: ([], lambda: self.get_details(PROJECT_TEAM_DETAILS)),
            PROJECT_PRODUCT_DETAILS: ([], lambda: self.get_details(PROJECT_PRODUCT_DETAILS)),
            PRODUCT_DETAILS: ([PROJECT_PRODUCT_DETAILS], self.get_product_details),
            TECHNOLOGY_PRODUCT_DETAILS: ([PRODUCT_DETAILS], self.get_technology_product_details),
            PRODUCT_CHILD_COMPOSITION_CUC: ([PROJECT_PRODUCT_DETAILS], self.get_product_child_composition_cuc),
            PROJECT_CLAIMS_DETAILS: ([], lambda: self.get_details(PROJECT_CLAIMS_DETAILS)),
            CLAIMS_DETAILS: ([PROJECT_CLAIMS_DETAILS, PROJECT_PRODUCT_DETAILS, PRODUCT_DETAILS], self.get_claims_details),
            CLAIM_SUBSTANTIATION_JOIN_DETAILS: ([CLAIMS_DETAILS], self.get_claim_substantiation_join_details),
            SUBSTANTIATION_CLAIMS_DETAILS: ([CLAIM_SUBSTANTIATION_JOIN_DETAILS], self.get_substantiation_claims_details),
            CLAIM_RISK_ASSESSMENT_DETAILS: ([CLAIMS_DETAILS], self.get_claim_risk_assessment_details),
            RISK_ASSESSMENT_DETAILS_CLAIM: ([CLAIM_RISK_ASSESSMENT_DETAILS], self.get_risk_assessment_details_claim),
            PROJECT_LOCAL_ADAPTATION_DETAILS: ([], lambda: self.get_details(PROJECT_LOCAL_ADAPTATION_DETAILS)),
            LOCAL_ADAPTATION_DETAILS: ([PROJECT_PRODUCT_DETAILS, PRODUCT_DETAILS, PROJECT_LOCAL_ADAPTATION_DETAILS], self.get_local_adaptation_details),
            LOCAL_ADAPTATION_SUBSTANTIATION_JOIN_DETAILS: ([LOCAL_ADAPTATION_DETAILS], self.get_local_adaptation_substantiation_join_details),
            SUBSTANTIATION_LOCAL_ADAPTATION_DETAILS: ([LOCAL_ADAPTATION_SUBSTANTIATION_JOIN_DETAILS], self.get_substantiation_local_adaptation_details),
            LOCAL_ADAPTATION_RISK_ASSESSMENT_DETAILS: ([LOCAL_ADAPTATION_DETAILS], self.get_local_adaptation_risk_assessment_details),
            RISK_ASSESSMENT_DETAILS_LA: ([LOCAL_ADAPTATION_RISK_ASSESSMENT_DETAILS], self.get_risk_assessment_details_la),
            PROJECT_ASSETS: ([], lambda: self.get_details(PROJECT_ASSETS)),
            USER_DETAILS_BY_ID: ([PROJECT_DETAILS, RISK_ASSESSMENT_DETAILS_CLAIM, RISK_ASSESSMENT_DETAILS_LA, PROJECT_ASSETS], self.get_user_details_by_id),
            CUC_GEOGRAPHY_DETAILS: ([PRODUCT_CHILD_COMPOSITION_CUC], self.get_cuc_geography_details),
            FORMULATION_DOC_DETAILS: ([PRODUCT_CHILD_COMPOSITION_CUC], self.get_formulation_doc_details),
            ASSETS_COUNTRY_DETAILS_BY_ID: ([PROJECT_ASSETS], self.get_assets_country_details_by_id),
            ASSETS_CLAIM_DETAILS_BY_ID: ([PROJECT_ASSETS], self.get_assets_claim_details_by_id),
            ASSETS_LA_DETAILS_BY_ID: ([PROJECT_ASSETS], self.get_assets_la_details_by_id)
        }

    async def run(self):
        await super().run()
        self.combine_project()
        self.combine_technology_product_data()
        self.combine_product_child_composition()
        self.combine_claim()
        return self.return_value