# Primary file for filtering samples workflows
import time

from utils.re_utils import execute_query
from utils.parsing_and_formatting import (
    parse_input,
    parse_field,
    parse_value,
    parse_operator,
    parse_join,
    field_value_formatting
)

SAMPLE_NODE_COLLECTION = "samples_nodes"
SAMPLE_SAMPLE_COLLECTION = "samples_sample"

AQL_query_template = f"""
let version_ids = (for sample_id in @sample_ids
    let doc = DOCUMENT({SAMPLE_SAMPLE_COLLECTION}, sample_id.id)
    RETURN {{'id': doc.id, 'version_id': doc.vers[sample_id.version - 1], 'version': sample_id.version}}
)

let node_metas = (for version_id in version_ids
    for node in {SAMPLE_NODE_COLLECTION}
        FILTER node.id == version_id.id and node.uuidver == version_id.version_id
        LIMIT @num_sample_ids
        let meta_objs_values = (for meta in node.cmeta
            RETURN {{ [ meta.ok ]: {{ [ meta.k ]: meta.v }} }}
        )
        return {{"id": node.id, "version": node.ver, "meta": APPLY("MERGE_RECURSIVE", meta_objs_values)}}
)
for node in node_metas
    FILTER """

class SampleFilterer():
    '''
    '''
    def __init__(cls, ctx, re_api_url, sample_service):
        cls.re_api_url = re_api_url
        cls.sample_service = sample_service
        cls.re_admin_token = ctx.get('token')

    def filter_samples(self, params):
        samples = parse_input(params)
        # AQL = Arango Query Languages
        # double curly braces "{{}}" are used to create string literal curly braces "{}"
        AQL_query = AQL_query_template
        query_params = {"sample_ids": samples, 'num_sample_ids': len(samples)}
        filter_conditions = params.get('filter_conditions', [])
        num_filters = len(filter_conditions)
        parsed_filters = [{
            'field': parse_field(fc.get('metadata_field'), idx),
            'value': parse_value(fc.get('metadata_value'), idx),
            'operator': parse_operator(fc.get('operator'), idx),
            'join': parse_join(fc.get('join_condition'), idx, num_filters)
        } for idx, fc in enumerate(filter_conditions)]
        formatted_filters = self._validate_filters(parsed_filters)
        for idx, formatted_filter in enumerate(formatted_filters):
            query_constraint, filter_params = self._construct_filter(
                formatted_filter, idx
            )
            join = formatted_filter.get('join')
            query_params.update(filter_params)
            if idx+1 < num_filters:
                AQL_query += query_constraint + f" {join} "
            else:
                # the final join statement is ignored
                AQL_query += query_constraint

        AQL_query += """
        RETURN {"id": node.id, "version": node.version, "meta": node.meta}
        """
        results = execute_query(
            AQL_query,
            self.re_api_url,
            self.re_admin_token,
            query_params
        )
        return {
            'sample_ids': results['results'],
            'query': AQL_query
        }

    def _construct_filter(self, formatted_filter, idx):
        '''
        formatted_filter must contain the following parameters:
            'field', 'operator',
            'value', 'join_condition'
        '''
        field = formatted_filter.get('field')
        operator = formatted_filter.get('operator')
        AQL_query = f"node.meta.{field}.value {operator} @value{idx}"
        filter_params = {
            f"value{idx}": formatted_filter.get('value')
        }
        return AQL_query, filter_params

    def _validate_filters(self, parsed_filters):
        '''The SampleService will error here if the metadata field is not found as an accepted controlled metadata field'''
        try:
            static_metadata = self.sample_service.get_metadata_key_static_metadata({
                'prefix': 0,  # expects these not to be prefix validated.
                'keys': set([pf['field'] for pf in parsed_filters])
            })['static_metadata']
        except error:
            # may require a better message later.
            raise error
        formatted_filters = []
        for idx, parsed_filter in enumerate(parsed_filters):
            stat_meta = static_metadata.get(parsed_filter.get('field'))
            formatted_filters.append(
                field_value_formatting(parsed_filter, stat_meta, idx)
            )
        return formatted_filters
