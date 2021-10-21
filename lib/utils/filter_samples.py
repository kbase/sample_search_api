# Primary file for filtering samples workflows
from utils.re_utils import execute_query
from utils.parsing_and_formatting import (
    parse_input,
    parse_field,
    parse_values,
    parse_comparison_operator,
    parse_logical_operator,
    field_value_formatting
)

SAMPLE_NODE_COLLECTION = "samples_nodes"
SAMPLE_SAMPLE_COLLECTION = "samples_sample"

# AQL = Arango Query Languages
# double curly braces "{{}}" are used to create string literal curly braces "{}"
AQL_query_template = f"""
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
        FILTER node.id == version_id.id and node.uuidver == version_id.version_id
        LIMIT @num_sample_ids
        let meta_objs_values = (for meta in node.cmeta
            RETURN {{ [ meta.ok ]: {{ [ meta.k ]: meta.v }} }}
        )
        RETURN {{
            "id": node.id,
            "version": node.ver,
            "meta": APPLY("MERGE_RECURSIVE", meta_objs_values)
        }}
)
for node in node_metas
    FILTER """


class SampleFilterer():
    '''
    '''
    def __init__(cls, re_admin_token, re_api_url, sample_service):
        cls.re_api_url = re_api_url
        cls.sample_service = sample_service
        cls.re_admin_token = re_admin_token

    def filter_samples(self, params, user_token):
        samples, filter_conditions = parse_input(params)
        # AQL = Arango Query Languages
        AQL_query = AQL_query_template
        query_params = {"sample_ids": samples, 'num_sample_ids': len(samples)}
        num_filters = len(filter_conditions)
        parsed_filters = [{
            'field': parse_field(fc.get('metadata_field'), idx),
            'values': parse_values(fc.get('metadata_values'), idx),
            'comp_op': parse_comparison_operator(fc.get('comparison_operator'), idx),
            'logic_op': parse_logical_operator(fc.get('logical_operator'), idx, num_filters)
        } for idx, fc in enumerate(filter_conditions)]
        formatted_filters = self._format_and_validate_filters(parsed_filters)
        for idx, formatted_filter in enumerate(formatted_filters):
            query_constraint, filter_params = self._construct_filter(
                formatted_filter, idx
            )
            logic_op = formatted_filter.get('logic_op')
            query_params.update(filter_params)
            if idx + 1 < num_filters:
                AQL_query += query_constraint + f" {logic_op} "
            else:
                # the final logical operator statement is ignored
                AQL_query += query_constraint

        AQL_query += """
        RETURN {"id": node.id, "version": node.version}
        """
        # use the user token if an admin token is not provided
        run_token = self.re_admin_token if self.re_admin_token else user_token
        results = execute_query(
            AQL_query,
            self.re_api_url,
            run_token,
            query_params
        )
        return {
            'sample_ids': results['results'],
            'query': AQL_query
        }

    def _construct_filter(self, formatted_filter, idx):
        '''
        formatted_filter must contain the following parameters:
            'field', 'comparison_operator', 'value', 'logical_operator'
        '''
        field = formatted_filter.get('field')
        comp_op = formatted_filter.get('comp_op')
        values = formatted_filter.get('values')
        AQL_query = f"node.meta.{field}.value {comp_op} @value{idx}"
        # if there is one value in the list of values, flatten to just the value.
        if len(values) == 1 and comp_op not in ["IN", "NOT IN"]:
            values = values[0]
        filter_params = {
            f"value{idx}": values
        }
        return AQL_query, filter_params

    def _format_and_validate_filters(self, parsed_filters):
        '''
        The SampleService will error here if the metadata field
        is not found as an accepted controlled metadata field
        '''
        try:
            static_metadata = self.sample_service.get_metadata_key_static_metadata({
                'prefix': 0,  # expects these not to be prefix validated.
                'keys': set([pf['field'] for pf in parsed_filters])
            })['static_metadata']
        except Exception as error:
            err_message = error.message
            err_keys = err_message.split(':')[-1]
            try:
                static_metadata = self.sample_service.get_metadata_key_static_metadata({
                    'prefix': 1,  # assume the ones that failed are prefix validated
                    'keys': set([pf['field'] for pf in parsed_filters])
                })['static_metadata']
            except Exception as error:
                err_message = error.message
                prefix_err_keys = err_message.split(':')[-1]
                key_set = set([k.strip() for k in err_keys.strip().split(',')]).union(
                          set([k.strip() for k in prefix_err_keys.strip().split(',')]))
                message = "Unable to resolve metadata fields or prefix metadata fields: " + \
                          ", ".join(list(key_set))
                raise ValueError(message)
        formatted_filters = []
        for idx, parsed_filter in enumerate(parsed_filters):
            stat_meta = static_metadata.get(parsed_filter.get('field'))
            formatted_filters.append(
                field_value_formatting(parsed_filter, stat_meta, idx)
            )
        return formatted_filters
