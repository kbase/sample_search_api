from utils.re_utils import execute_query

SAMPLE_NODE_COLLECTION = "samples_nodes"
SAMPLE_SAMPLE_COLLECTION = "samples_sample"

META_AQL_TEMPLATE = f"""
        let version_ids = (for sample_id in @sample_ids
            let doc = DOCUMENT({SAMPLE_SAMPLE_COLLECTION}, sample_id.id)
            RETURN {{
                'id': doc.id,
                'version_id': doc.vers[sample_id.version - 1],
                'version': sample_id.version
            }}
        )
        let node_metas = (for version_id in version_ids
            for node in {SAMPLE_NODE_COLLECTION}
                FILTER node.id == version_id.id AND node.uuidver == version_id.version_id
                LIMIT @num_sample_ids
                let meta_objs_keys = (FOR meta in node.cmeta
                    RETURN meta.ok
                )
            RETURN meta_objs_keys
        )
        RETURN UNIQUE(FLATTEN(node_metas))
        """

class MetadataManager:

    def __init__(cls, re_admin_token, re_api_url):
        cls.re_api_url = re_api_url
        cls.re_admin_token = re_admin_token

    def get_sampleset_meta(self, sample_ids, user_token):
        # use the user token if an admin token is not provided
        query_params = {"sample_ids": sample_ids, 'num_sample_ids': len(sample_ids)}
        run_token = self.re_admin_token if self.re_admin_token else user_token
        results = execute_query(
            META_AQL_TEMPLATE,
            self.re_api_url,
            run_token,
            query_params
        )

        return {'results': results['results'][0]}