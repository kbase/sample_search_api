import re

ACCEPTED_TYPE_OPERATOR_PAIRINGS = {
    'number': ["==", "!=", "<", ">", ">=", "<=", "IN", "NOT IN"],
    'string': ["==", "!=", "IN", "NOT IN"],
    'noop': ["==", "!=", "IN", "NOT IN"],
    'enum': ["==", "!=", "IN", "NOT IN"],
    'ontology': ["==", "!=","IN", "NOT IN"]
}
AQL_operator_terms = {
    "==", "!=", "<", ">", ">=", "<=", "IN", "NOT IN"
}
AQL_join_terms = {"AND", "OR"}


def _has_whitespace(s):
    return bool(re.search(r'\s+', s))

def field_value_formatting(parsed_filter, stat_meta, idx):
    '''This function handles all the gross nitty gritty formatting stuff'''
    value = parsed_filter.get('value')
    operator = parsed_filter.get('operator')
    field = parsed_filter.get('field')
    if stat_meta.get('type') == 'number':
        try:
            value = float(value)
        except:
            raise ValueError(f"provided value '{value}' is not a valid input "
                             f"for field '{field}', numerical value expected.")
        if stat_meta.get('maximum'):
            if value > float(stat_meta['maximum']):
                raise ValueError(f"provided value '{value}' is greater than maximum "
                                 f"value '{stat_meta['maximum']}' for field '{field}'.")
        if stat_meta.get('minimum'):
            if value < float(stat_meta['minimum']):
                raise ValueError(f"provided value '{value}' is less than minimum "
                                 f"value '{stat_meta['minimum']}' for field '{field}'.")

    elif stat_meta.get('type') == 'string':
        # no need to format more here.
        value = str(value)
        if stat_meta.get('max-len'):
            if len(value) > stat_meta['max-len']:
                raise ValueError(f"provided value '{value}' is longer than "
                                 f"allowable length of {stat_meta['max-len']} for field '{field}'.")

    elif stat_meta.get('type') == 'enum':
        if stat_meta.get('enum'):
            if value not in stat_meta['enum']:
                raise ValueError(f"{value} is not a valid input for filter condition on metadat field '{field}' "
                                 f"at position {idx}, only the following fields are permitted: {stat_meta['enum']}")
    
    elif stat_meta.get('type') == 'ontology':
        # no need to format more here..... jk there is
        value = str(value)
    elif stat_meta.get('type') == 'noop':
        # no need to format more here.
        value = str(value)
    else:
        raise ValueError(f"Unsupported validator type '{stat_meta.get('type')}' for "
                         f"filter constraint on metadata field '{field}' at position {idx}.")

    if operator not in ACCEPTED_TYPE_OPERATOR_PAIRINGS.get(stat_meta['type']):
        raise ValueError(f"provided operator {operator} not accepted for filter using for filter "
                         f"constraint on metadata field '{field}' at position {idx}.")
    parsed_filter.update({'value': value})
    return parsed_filter


def parse_field(field, idx):
    '''field cannot have any white space in it'''
    if not field:
        raise ValueError(f"please provide a metadata field input for filter constraint "
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
        raise ValueError(f"please provide an operator input for filter constraint at position {idx}")
    if operator not in AQL_operator_terms:
        raise ValueError(f"Input Operator in filter condition {idx} must be one of: " + ", ".join(["\'" + str(term) + "\'" for term in AQL_operator_terms]))
    return operator


def parse_value(value, idx):
    '''value cannot have any white space in it'''
    if not value:
        raise ValueError(f"please provide a metadata value input for filter constraint at position {idx}")
    # remove any trailing and leading whitespace
    value = value.strip()
    if _has_whitespace(value):
        raise ValueError(f"metadata value input cannot contain any spaces, value '{value}' at position {idx} contains whitespace.")
    try:
        value = float(value)
    except:
        pass
    return value


def parse_join(join, idx, num_filters):
    if idx+1 >= num_filters:
        return None
    if not join:
        raise ValueError(f"please provide a join condition input for filter constraint at position {idx}")
    join = str(join)
    if join.upper() not in AQL_join_terms:
        raise ValueError(f"Input join condition in filter condition {idx} must be one of: " + ", ".join(["\'" + str(term) + "\'" for term in AQL_join_terms]))
    return join.lower()


def parse_input(params):
    # no filter_conditions is valid input.. albeit useless...
    # if not params.get('filter_conditions'):
    #     raise ValueError(f"")
    if not params.get('sample_ids'):
        raise ValueError(f"Must provide 'sample_ids' as input")
    samples = params.get('sample_ids', [])
    if len(samples) < 2:
        # must provide at least 2 samples
        raise ValueError("Must provide at least two samples in 'sample_ids'")
    return samples
