import pandas as pd
from .constants import *
from DownloadReport.constants import *
import mimetypes
import pathlib


def get_query_params(df: pd.DataFrame, column: str) -> str:
    """
    Returns comma-seperated strings for query
    """
    return ','.join(df[column].dropna().apply(lambda x: f"'{x}'").tolist()) if not df.empty else "'NA'"

def get_query_params_list_element(df: pd.DataFrame, column: str) -> str:
    return (
        ','.join(df[column].explode().dropna().apply(lambda x: f"'{x}'").unique().tolist())
        if not df.empty else "'NA'"
    )


def calculate_row_count_per_claim_df(claim_df: pd.DataFrame, substantiation_df: pd.DataFrame, risk_df: pd.DataFrame) -> pd.DataFrame:
    claim_df[SUBRANGE] = False
    claim_df[TECHNOLOGY] = [[] for _ in range(len(claim_df))]
    sub_counts = substantiation_df['claim__v'].value_counts() if not substantiation_df.empty else pd.Series(dtype=int)
    risk_counts = risk_df['claim__c'].value_counts() if not risk_df.empty else pd.Series(dtype=int)

    if claim_df.empty:
        claim_df['max_row_count'] = pd.Series(dtype=int)
        return claim_df
    
    def compute_max_row_count(claim_id):
        sub_count = sub_counts.get(claim_id, 0)
        risk_count = risk_counts.get(claim_id, 0)
        max_rows = max(sub_count, risk_count)
        return max(max_rows, MIN_ROW_PER_CLAIM)

    claim_df['max_row_count'] = claim_df['id'].apply(compute_max_row_count)
    return claim_df


def calculate_row_count_per_la_df(la_df: pd.DataFrame, substantiation_df: pd.DataFrame, risk_df: pd.DataFrame) -> pd.DataFrame:
    la_df[SUBRANGE] = False
    la_df[TECHNOLOGY] = [[] for _ in range(len(la_df))]
    sub_counts = substantiation_df['local_adaptation__c'].value_counts() if not substantiation_df.empty else pd.Series(dtype=int)
    risk_counts = risk_df['local_adaptation__c'].value_counts() if not risk_df.empty else pd.Series(dtype=int)

    if la_df.empty:
        la_df['max_row_count'] = pd.Series(dtype=int)
        return la_df

    def compute_max_row_count(la_id):
        sub_count = sub_counts.get(la_id, 0)
        risk_count = risk_counts.get(la_id, 0)
        max_rows = max(sub_count, risk_count)
        return max(max_rows, MIN_ROW_PER_LA)

    la_df['max_row_count'] = la_df['id'].apply(compute_max_row_count)
    return la_df


def check_claim_la_for_product_df(product_df: pd.DataFrame, claim_df: pd.DataFrame, la_df: pd.DataFrame) -> pd.DataFrame:
    claim_product_ids = set(claim_df['product__v'].dropna()) if not claim_df.empty else set()
    la_product_ids = set(la_df['product__c'].dropna()) if not la_df.empty else set()
    relevant_product_ids = claim_product_ids.union(la_product_ids)
    filtered_product_df = product_df[product_df['id'].isin(relevant_product_ids)].copy()
    return filtered_product_df

def get_details_by_id(id: int | str, df: pd.DataFrame) -> dict:
    """
    User - user_details_by_id\n
    Cuc Geography - cuc_geography_details
    """
    if df.empty:
        return {}
    matched_user = df[df['id'] == str(id)]
    return matched_user.iloc[0].to_dict() if not matched_user.empty else {}

def get_formulation_doc_details_by_id(val: str, formulation_doc_details_df: pd.DataFrame) -> pd.Series:
    default = pd.Series([None, None, None, None, None, None, None])
    if formulation_doc_details_df.empty or val is None:
        return default
    doc_id, major_version, minor_version = val.split("_")
    matched_formulation = formulation_doc_details_df[
            (formulation_doc_details_df['id'] == int(doc_id)) &
            (formulation_doc_details_df['minor_version_number__v'] == int(minor_version)) &
            (formulation_doc_details_df['major_version_number__v'] == int(major_version))
        ]
    if matched_formulation.empty:
        return default
    match_dict = matched_formulation.iloc[0].to_dict()
    mime = mimetypes.guess_type(str(match_dict[FILENAME__V]), strict=False)[0]
    return pd.Series([
        match_dict[ID],
        match_dict[NAME__V],
        match_dict[FILENAME__V],
        mime if mime else pathlib.Path(str(match_dict[FILENAME__V])).suffix,
        match_dict[MINOR_VERSION_NUMBER__V],
        match_dict[MAJOR_VERSION_NUMBER__V],
        f"/ui/#doc_info/{match_dict[ID]}/{match_dict[MAJOR_VERSION_NUMBER__V]}/{match_dict[MINOR_VERSION_NUMBER__V]}"
    ])

def get_assets_details_by_id(geography_id: int | str, df: pd.DataFrame) -> str:
    """
    Geography - assets_country_details_by_id\n
    Claim - assets_claim_details_by_id\n
    La - assets_la_details_by_id
    """
    if df.empty:
        return ""
    matched_assets_geo = df[df['id'] == str(geography_id)]
    return matched_assets_geo.iloc[0]['name__v'] if not matched_assets_geo.empty else ""

def get_technology_formulation_link(value: str) -> str:
    if value is None or not value:
        return ""
    id, major, minor = value.split("_")
    return f"/ui/#doc_info/{id}/{major}/{minor}"