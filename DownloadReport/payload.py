payloads = {
    "project_details": """SELECT id,name__v, TOLABEL(division__c), TOLABEL(business_unit_project__c), TOLABEL(category__c), 
                ipm_project_name__c, TOLABEL(project_type__v), LONGTEXT(description__v), TOLABEL(project_scope__c), LONGTEXT(project_background__c), 
                created_by__v, created_date__v, target_launch_date__c FROM project__v WHERE id = '{project_id}'""",
    "project_geography_details": """SELECT id, project__vr.name__v, country__vr.name__v FROM project_country_join__v 
                    WHERE project__v = '{project_id}'""",
    "project_team_details": """SELECT project__cr.name__v, user__cr.name__v, role__cr.name__v, geography__cr.name__v 
                    FROM project_team1__c WHERE project__c = '{project_id}'""",
    "project_product_details": """SELECT product__v, product__vr.name__v FROM project_product_join__v WHERE project__v = '{project_id}'""",
    "product_details": """SELECT id,name__v,title__v,TOLABEL(division__c),TOLABEL(category__c),brand__c,
        brand__cr.name__v,brand_name__c,format__c,format__cr.name__v,format_title__c,variant_name__c,
        object_type__vr.api_name__v, subrange1__c, technology_id__c, technology_name__c , technology_id_1__c, technology_name_1__c, range__c, range_name__c FROM product__v WHERE id CONTAINS ({products_to_query}) 
        ORDER BY variant_name__c""",
    "technology_product_details": """SELECT id,name__v,title__v,TOLABEL(division__c),TOLABEL(category__c),brand__c,
        brand__cr.name__v,brand_name__c,format__c,format__cr.name__v,format_title__c,variant_name__c,
        object_type__vr.api_name__v, subrange1__c, technology_id__c, technology_id_1__c,
        formulation_document__c, formulation_document__cr.id,formulation_document__cr.name__v, 
        formulation_document__cr.filename__v FROM product__v WHERE id CONTAINS ({products_to_query}) 
        ORDER BY variant_name__c""",
    "product_child_composition_cuc": """SELECT id,name__v, parent__v,parent_product__c, child__v, child_product__c, child__vr.cuc_code__c, 
                        child__vr.geography__c, child__vr.formulation_document__c, child__vr.source_plmeln__c,
                        child__vr.elnplm_link__c FROM product_composition__v 
                        WHERE parent__v CONTAINS ({products_to_query})""",
    "project_claims_details": """SELECT project__v, claim__v, claim__vr.name__v 
                    FROM claim_project_join__v WHERE project__v = '{project_id}'""",
    "claims_details": """SELECT id,name__v,claim_order__c,product__v,product__vr.name__v,product_name__c,
            brand__c,product_brand_name__c,TOLABEL(marketing_channels__c),select_statement__v,
            select_statement__vr.name__v,statement__c,statement_image__c,LONGTEXT(qualifier__c),
            TOLABEL(state__v),TOLABEL(category__c),LONGTEXT(support_strategy_rich__c),TOLABEL(final_risk_level__c),
            TOLABEL(initial_risk_level__c),legal_summary__c,nutrition_summary__c,rd_summary__c,regulatory_summary__c,claims_forum_summary__c,marketing_feedback__c, related_projects__c FROM claim__v 
            WHERE id CONTAINS ({claims_to_query}) AND product__v CONTAINS ({products_to_query}) AND state__v != 'cancelled_state__c' ORDER BY claim_order__c""",
    "claims_details_tech_products": """SELECT id,name__v,claim_order__c,product__v,product__vr.name__v,product_name__c,
            brand__c,product_brand_name__c,TOLABEL(marketing_channels__c),select_statement__v,
            select_statement__vr.name__v,statement__c,statement_image__c,LONGTEXT(qualifier__c),
            TOLABEL(state__v),TOLABEL(category__c),LONGTEXT(support_strategy_rich__c),TOLABEL(final_risk_level__c),
            TOLABEL(initial_risk_level__c),legal_summary__c,nutrition_summary__c,rd_summary__c,regulatory_summary__c,claims_forum_summary__c,marketing_feedback__c, related_projects__c FROM claim__v 
            WHERE product__v CONTAINS ({products_to_query}) AND state__v != 'cancelled_state__c'  ORDER BY claim_order__c""",
    "claim_substantiation_join_details": """SELECT claim__v,claim__vr.name__v,substantiation__v,substantiation__vr.name__v,
                        substantiation__vr.description__v FROM claim_substantiation__v 
                        WHERE claim__v CONTAINS ({claims_to_query})""",
    "substantiation_claims_details": """SELECT id,name__v,description__v,country__c,country__cr.name__v,external_document_links__c,
                    LONGTEXT(substantiation_details__c), related_reference__v  FROM substantiation__v  
                    WHERE id CONTAINS ({subtantiations_to_query})""",
    "claim_risk_assessment_details": """SELECT claim__c,risk_assessment__c FROM claim_and_risk_assessment_join__c 
                    WHERE claim__c CONTAINS ({claims_to_query})""",
    "risk_assessment_details_claim": """SELECT id,name__v,created_by__v,created_date__v,object_type__vr.name__v,LONGTEXT(comments__c),
                    TOLABEL(overall_risk_level__c),risk_level_calculated_formula__c FROM risk_assessment__c 
                    WHERE id CONTAINS ({risks_to_query})""",
    "project_local_adaptation_details": """SELECT project__v, local_adaptation__v,local_adaptation__vr.name__v 
                        FROM project_local_adaptation_join__v WHERE project__v = '{project_id}'""",
    "local_adaptation_details": """SELECT id,name__v,country__vr.name__v,claim_order__c,product__c,product__cr.name__v,
        product_name11__c,brand_id__c,brand__c,TOLABEL(marketing_channels__v),statement_ref__v,
        statement_ref__vr.name__v,statement_text1__c,statement_image__c,LONGTEXT(qualifier__c),
        TOLABEL(state__v),TOLABEL(claim_category__c),LONGTEXT(support_strategy_rich__c),ready_for_use__c,
        TOLABEL(final_risk_level__c), TOLABEL(initial_risk_level__c),legal_summary__c,nutrition_summary__c,
        rd_summary__c,regulatory_affairs_summary__c,claims_forum_summary__c,marketing_feedback__c,related_projects__c 
        FROM local_adaptation__v WHERE id CONTAINS ({las_to_query}) AND product__c 
        CONTAINS ({products_to_query}) AND state__v != 'cancelled_state__c'
        ORDER BY geography_lookup__c, claim_order__c""",
    "local_adaptation_details_tech_products": """SELECT id,name__v,country__vr.name__v,claim_order__c,product__c,product__cr.name__v,
        product_name11__c,brand_id__c,brand__c,TOLABEL(marketing_channels__v),statement_ref__v,
        statement_ref__vr.name__v,statement_text1__c,statement_image__c,LONGTEXT(qualifier__c),
        TOLABEL(state__v),TOLABEL(claim_category__c),LONGTEXT(support_strategy_rich__c),ready_for_use__c,
        TOLABEL(final_risk_level__c), TOLABEL(initial_risk_level__c),legal_summary__c,nutrition_summary__c,
        rd_summary__c,regulatory_affairs_summary__c,claims_forum_summary__c,marketing_feedback__c,related_projects__c 
        FROM local_adaptation__v WHERE product__c 
        CONTAINS ({products_to_query}) AND state__v != 'cancelled_state__c'
        ORDER BY geography_lookup__c, claim_order__c""",
    "local_adaptation_substantiation_join_details": """SELECT local_adaptation__c,local_adaptation__cr.name__v,substantiation__c,substantiation__cr.name__v,
                    substantiation__cr.description__v FROM local_adaptation_substantiation_join__c 
                    WHERE local_adaptation__c CONTAINS ({las_to_query})""",
    "substantiation_local_adaptation_details": """SELECT id,name__v,description__v,country__c,country__cr.name__v,external_document_links__c,
                    LONGTEXT(substantiation_details__c), related_reference__v  FROM substantiation__v  
                    WHERE id CONTAINS ({substantiations_to_query})""",
    "local_adaptation_risk_assessment_details": """SELECT local_adaptation__c,risk_assessment__c FROM local_adaptation_risk_assessment_join__c 
                        WHERE local_adaptation__c CONTAINS ({las_to_query})""",
    "risk_assessment_details_la": """SELECT id,name__v,created_by__v,created_date__v,object_type__vr.name__v,LONGTEXT(comments__c),
                    TOLABEL(overall_risk_level__c),risk_level_calculated_formula__c FROM risk_assessment__c 
                    WHERE id CONTAINS ({risks_to_query})""",
    "project_assets": """SELECT id, name__v, filename__v, document_number__v, title__v, type__v, subtype__v, md5checksum__v, 
                        minor_version_number__v, major_version_number__v,created_by__v, document_creation_date__v,
                        related_projects__c, product__v, country__v, status__v, related_claims__c, size__v, 
                        related_local_adaptations__c, final_risk_level__c, file_meta_comments__v FROM documents 
                        WHERE related_projects__c = '{project_id}' and type__v = 'Asset Type'""",
    "user_details_by_id": """SELECT id, name__v, first_name__sys, last_name__sys, username__sys, email__sys FROM user__sys 
                        WHERE id CONTAINS ({user_id_string})""",
    "cuc_geography_details": """SELECT id, name__v, region__c, levels__c, object_type__v, code__sys, abbreviation__c, 
                        artwork_cluster__c FROM country__v WHERE id CONTAINS ({geographies_to_query})""",
    "formulation_doc_details": """SELECT id, name__v, filename__v, document_number__v, title__v, type__v, subtype__v, md5checksum__v, 
                                    minor_version_number__v, major_version_number__v, size__v FROM documents 
                                    WHERE id CONTAINS ({doc_id_to_query}) and major_version_number__v = {major_verison_no} 
                                    AND minor_version_number__v = {minor_verison_no} AND type__v = 'Formulation Documents'""",
    "assets_country_details_by_id": """SELECT id, name__v FROM country__v WHERE id CONTAINS ({country_id_string})""",
    "assets_claim_details_by_id": """SELECT id,name__v FROM claim__v WHERE id CONTAINS ({claim_id_string})""",
    "assets_la_details_by_id": """SELECT id,name__v FROM local_adaptation__v WHERE id CONTAINS ({la_id_string})"""
}


payloads_to_run = {
    'veeva_master': [
        "project_details",
        "project_geography_details",
        "project_team_details",
        "project_product_details",
        "product_details",
        "technology_product_details",
        "product_child_composition_cuc",
        "project_claims_details",
        "claims_details",
        "claims_details_tech_products",
        "claims_substantiation_join_details",
        "substantiation_claims_details",
        "claim_risk_assesment_details",
        "risk_assessment_detail_claims",
        "project_local_adaptation_details",
        "local_adaptation_details",
        "local_adaptation_details_tech_products",
        "local_adaptation_substantiation_join_details",
        "substantiation_local_adaptation_details",
        "local_adaptation_risk_assessment_details",
        "risk_assessment_details_la",
        "project_assets",
        "user_details_by_id",
        "cuc_geography_details",
        "formulation_doc_details",
        "assets_country_details_by_id",
        "assets_claim_details_by_id",
        "assets_la_details_by_id"
    ]
}