# file for any parsing and formatting functions
import re

# AQL = Arango Query Languages
ACCEPTED_TYPE_OPERATOR_PAIRINGS = {
    'number': ["==", "!=", "<", ">", ">=", "<=", "IN", "NOT IN"],
    'string': ["==", "!=", "IN", "NOT IN"],
    'noop': ["==", "!=", "IN", "NOT IN"],
    'enum': ["==", "!=", "IN", "NOT IN"],
    'ontology': ["==", "!=","IN", "NOT IN"]
}
AQL_multi_value_comparison_operators = {"IN", "NOT IN"}
AQL_single_value_comparison_operators = {"==", "!=", "<", ">", ">=", "<="}
AQL_comparison_operators = {
    *AQL_single_value_comparison_operators, *AQL_multi_value_comparison_operators
}
AQL_join_terms = {"AND", "OR"}


def _has_whitespace(s):
    return bool(re.search(r'\s+', s))

def field_value_formatting(parsed_filter, stat_meta, idx):
    '''This function handles all the gross nitty gritty formatting stuff
    parsed_filter 'must' have the following keys: 'values', 'operator', 'field'
    '''
    if 'values' not in parsed_filter or \
       'operator' not in parsed_filter or \
       'field' not in parsed_filter:
        raise ValueError('Missing required field in parsed_filter argument to field_value_formatting.')

    values = parsed_filter.get('values')
    operator = parsed_filter.get('operator')
    field = parsed_filter.get('field')
    validated_values = []
    for pos_idx, value in enumerate(values):
        if stat_meta.get('type') == 'number':
            try:
                value = float(value)
            except:
                raise ValueError(f"provided value '{value}' at position {pos_idx} is not a valid input "
                                 f"for field '{field}', numerical value expected.")
            if stat_meta.get('maximum'):
                if value > float(stat_meta['maximum']):
                    raise ValueError(f"provided value '{value}' at position {pos_idx} is greater than maximum "
                                     f"value '{stat_meta['maximum']}' for field '{field}'.")
            if stat_meta.get('minimum'):
                if value < float(stat_meta['minimum']):
                    raise ValueError(f"provided value '{value}' at position {pos_idx} is less than minimum "
                                     f"value '{stat_meta['minimum']}' for field '{field}'.")

        elif stat_meta.get('type') == 'string':
            # no need to format more here.
            value = str(value)
            if stat_meta.get('max-len'):
                if len(value) > stat_meta['max-len']:
                    raise ValueError(f"provided value '{value}' at position {pos_idx} is longer than "
                                     f"allowable length of {stat_meta['max-len']} for field '{field}'.")

        elif stat_meta.get('type') == 'enum':
            if stat_meta.get('enum'):
                if value not in stat_meta['enum']:
                    raise ValueError(f"{value} at position {pos_idx} is not a valid input for filter condition on metadat field '{field}' "
                                     f"at position {idx}, only the following fields are permitted: {stat_meta['enum']}")
        
        elif stat_meta.get('type') == 'ontology':
            # no need to format more here..... jk there is
            value = str(value)
        elif stat_meta.get('type') == 'noop':
            # no need to format more here.
            value = str(value)
        else:
            raise ValueError(f"Unsupported validator type '{stat_meta.get('type')}' for "
                             f"filter condition on metadata field '{field}' at position {idx}.")
        validated_values.append(value)
    if operator not in ACCEPTED_TYPE_OPERATOR_PAIRINGS.get(stat_meta['type']):
        raise ValueError(f"provided operator {operator} not accepted for filter using for filter "
                         f"condition on metadata field '{field}' at position {idx}.")

    # now we make sure that there are more broadly consistent operator/value pairings
    if operator in AQL_multi_value_comparison_operators:
        # any number of values accepted here
        pass
    if operator in AQL_single_value_comparison_operators:
        if len(validated_values) > 1:
            raise ValueError(f"Provided comparison operator expects 1 value, {len(validated_values)} values were provided.")

    parsed_filter.update({'values': validated_values})
    return parsed_filter


def parse_input(params):
    # no filter_conditions is valid input.. albeit useless...
    if not params.get('filter_conditions'):
        raise ValueError("Must provide at least one filter condition in 'filter_conditions' as input.")
    if not params.get('sample_ids'):
        raise ValueError(f"Must provide 'sample_ids' as input")
    samples = params.get('sample_ids', [])
    filter_conditions = params.get('filter_conditions', [])
    if len(samples) < 2:
        # must provide at least 2 samples
        raise ValueError("Must provide at least two samples in 'sample_ids'")
    if len(filter_conditions) < 1:
        # must provide at least 1 filter condition
        raise ValueError("Must provide at least one filter condition in 'filter_conditions' as input.")
    return samples, filter_conditions


def parse_field(field, idx):
    '''field cannot have any white space in it'''
    if not field:
        raise ValueError(f"please provide a metadata field input for filter condition "
                         f"on metadata field '{field}' at position {idx}")
    # remove any trailing and leading whitespace
    field = field.strip()
    # if contains any whitespace, error
    if _has_whitespace(field):
        raise ValueError(f"metadata field input cannot contain any spaces"
                         f", field '{field}' at position {idx} contains whitespace.")
    return field


def parse_operator(operator, idx):
    if not operator:
        raise ValueError(f"please provide an operator input for filter condition at position {idx}")
    # remove any trailing and leading whitespace
    operator = str(operator).strip()
    if operator.upper() not in AQL_comparison_operators:
        raise ValueError(f"Input Operator in filter condition {idx} must be one of: " + ", ".join(["\'" + str(term) + "\'" for term in AQL_comparison_operators]))
    return operator.upper()


def parse_values(values, idx):
    '''value cannot have any white space in it'''
    if not values:
        raise ValueError(f"please provide metadata values input for filter condition at position {idx}")
    parsed = []
    for pos_idx, value in enumerate(values):
        if not value:
            raise ValueError(f"please provide a valid metadata value input at at position {pos_idx} for "
                             f"filter condition at position {idx}")
        # remove any trailing and leading whitespace
        value = value.strip()
        # NOTE: this may be overkill...
        if _has_whitespace(value):
            raise ValueError(f"metadata value input cannot contain any spaces, value '{value}' "
                             f"at position {pos_idx} in filter condition position {idx} contains whitespace.")
        parsed.append(value)
    return parsed


def parse_join(join, idx, num_filters):
    if idx+1 >= num_filters:
        return None
    if not join:
        raise ValueError(f"please provide a join condition input for filter condition at position {idx}")
    # remove any trailing and leading whitespace
    join = str(join).strip()
    if join.upper() not in AQL_join_terms:
        raise ValueError(f"Input join condition in filter condition {idx} must be one of: " + \
                          ", ".join(["\'" + str(term) + "\'" for term in AQL_join_terms]))
    return join.upper()
