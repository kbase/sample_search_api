# file for any parsing and formatting functions
import re

# AQL = Arango Query Languages
ACCEPTED_FIELD_TYPE_OPERATOR_PAIRINGS = {
    'number': ["==", "!=", "<", ">", ">=", "<=", "IN", "NOT IN"],
    'string': ["==", "!=", "IN", "NOT IN"],
    'noop': ["==", "!=", "IN", "NOT IN"],
    'enum': ["==", "!=", "IN", "NOT IN"],
    'ontology': ["==", "!=", "IN", "NOT IN"],
    'any': ["==", "!=", "<", ">", ">=", "<=", "IN", "NOT IN"] # for uncontrolled, unvalidated fields
}
AQL_one_to_many_value_comparison_operators = {"IN", "NOT IN"}
AQL_single_value_comparison_operators = {"==", "!=", "<", ">", ">=", "<="}
AQL_comparison_operators = {
    *AQL_single_value_comparison_operators,
    *AQL_one_to_many_value_comparison_operators
}
AQL_logical_operators = {"AND", "OR"}


def _has_whitespace(s):
    return bool(re.search(r'\s+', s))


def field_value_formatting(parsed_filter, stat_meta, idx):
    '''This function handles all the gross nitty gritty formatting stuff
    parsed_filter 'must' have the following keys: 'values', 'operator', 'field'
    '''
    # for now we store certain validation errors as unhandled warnings
    format_warnings = []
    if 'values' not in parsed_filter or \
       'comp_op' not in parsed_filter or \
       'field' not in parsed_filter:
        raise ValueError('Missing required field in parsed_filter '
                         'argument to field_value_formatting.')

    values = parsed_filter.get('values')
    comp_op = parsed_filter.get('comp_op')
    field = parsed_filter.get('field')
    validated_values = []
    for pos_idx, value in enumerate(values):
        if stat_meta.get('type') == 'number':
            try:
                try:
                    value = int(value)
                except ValueError:
                    value = float(value)
            except ValueError:
                raise ValueError(f"provided value '{value}' at position {pos_idx} is not a "
                                 f"valid input for field '{field}', numerical value expected.")
            if stat_meta.get('maximum'):
                if value > float(stat_meta['maximum']):
                    format_warnings.append(f"provided value '{value}' at position {pos_idx} "
                                           "is greater than maximum value "
                                           f"'{stat_meta['maximum']}' for field '{field}'.")
            if stat_meta.get('minimum'):
                if value < float(stat_meta['minimum']):
                    format_warnings.append(f"provided value '{value}' at position {pos_idx}"
                                           " is less than minimum value "
                                           f"'{stat_meta['minimum']}' for field '{field}'.")

        elif stat_meta.get('type') == 'string':
            # no need to format more here.
            value = str(value)
            if stat_meta.get('max-len'):
                if len(value) > stat_meta['max-len']:
                    format_warnings.append(f"provided value '{value}' at "
                                           f"position {pos_idx} is longer than allowable length "
                                           f"of {stat_meta['max-len']} for field '{field}'.")

        elif stat_meta.get('type') == 'enum':
            if stat_meta.get('enum'):
                if value not in stat_meta['enum']:
                    format_warnings.append(f"{value} at position {pos_idx} is not a valid input "
                                           f"for filter condition on metadata field '{field}' at "
                                           f"position {idx}, only the following fields are "
                                           f"permitted: {stat_meta['enum']}")

        elif stat_meta.get('type') == 'any':
            # treat any types as numbers if the value can be cast as a number, otherwise str
            try:
                value = int(value)
            except ValueError:
                value = float(value)
            except ValueError:
                value = str(value)
        elif stat_meta.get('type') in ['ontology', 'noop']:
            # no need to format more here
            value = str(value)
        else:
            raise ValueError(f"Unsupported validator type '{stat_meta.get('type')}' for "
                             f"filter condition on metadata field '{field}' at position {idx}.")
        validated_values.append(value)
    if comp_op not in ACCEPTED_FIELD_TYPE_OPERATOR_PAIRINGS.get(stat_meta['type']):
        raise ValueError(f"provided operator '{comp_op}' not accepted for metadata "
                         f"field '{field}' in filter condition at position {idx}.")

    # now we make sure that there are more broadly consistent operator/value pairings
    if comp_op in AQL_one_to_many_value_comparison_operators:
        # any number of values accepted here
        pass
    if comp_op in AQL_single_value_comparison_operators:
        if len(validated_values) > 1:
            raise ValueError(f"Provided comparison operator '{comp_op}' expects 1 value, "
                             f"{len(validated_values)} values were provided.")

    # for now we just print out the format warnings...
    print('\n'.join(format_warnings))

    parsed_filter.update({'values': validated_values})
    return parsed_filter


def parse_input(params):
    # no filter_conditions is valid input.. albeit useless...
    if not params.get('filter_conditions'):
        raise ValueError("Must provide at least one filter condition in "
                         "'filter_conditions' as input.")
    if not params.get('sample_ids'):
        raise ValueError("Must provide 'sample_ids' as input")
    samples = params.get('sample_ids', [])
    filter_conditions = params.get('filter_conditions', [])
    if len(samples) < 2:
        # must provide at least 2 samples
        raise ValueError("Must provide at least two samples in 'sample_ids'")
    if len(filter_conditions) < 1:
        # must provide at least 1 filter condition
        raise ValueError("Must provide at least one filter condition in "
                         "'filter_conditions' as input.")
    return samples, filter_conditions


def parse_field(field, idx):
    '''field cannot have any white space in it'''
    if not field:
        raise ValueError("please provide a metadata field input for filter condition "
                         f"on metadata field '{field}' at position {idx}")
    # remove any trailing and leading whitespace
    field = field.strip()
    # if contains any whitespace, error
    if _has_whitespace(field):
        raise ValueError("metadata field input cannot contain any spaces"
                         f", field '{field}' at position {idx} contains whitespace.")
    return field


def parse_comparison_operator(comp_op, idx):
    if not comp_op:
        raise ValueError("please provide a comparison operator "
                         f"input for filter condition at position {idx}")
    # remove any trailing and leading whitespace
    comp_op = str(comp_op).strip().upper()
    if comp_op not in AQL_comparison_operators:
        raise ValueError(f"Input comparison operator in filter condition {idx} must be one of: "
                         ", ".join(["'" + str(term) + "'" for term in AQL_comparison_operators]))
    return comp_op


def parse_values(values, idx):
    '''value cannot have any white space in it'''
    if not values:
        raise ValueError(f"please provide metadata values input for filter "
                         f"condition at position {idx}")
    parsed = []
    for pos_idx, value in enumerate(values):
        if not value:
            raise ValueError(f"please provide a valid metadata value input at at position "
                             f"{pos_idx} for filter condition at position {idx}")
        # remove any trailing and leading whitespace
        value = value.strip()
        parsed.append(value)
    return parsed


def parse_logical_operator(logic_op, idx, num_filters):
    if idx + 1 >= num_filters:
        return None
    if not logic_op:
        raise ValueError("please provide a logical operator input for "
                         f"filter condition at position {idx}")
    # remove any trailing and leading whitespace
    logic_op = str(logic_op).strip()
    if logic_op.upper() not in AQL_logical_operators:
        raise ValueError(f"Input logical operator in filter condition {idx} must be one of: "
                         ", ".join(["\'" + str(term) + "\'" for term in AQL_logical_operators]))
    return logic_op.upper()

def partition_controlled_parsed_filters(parsed_filters):
    # separates out controlled parsed_filters from uncontrolled for validation
    uc_filters = [pf for pf in parsed_filters if pf['field'].startswith('custom:')]
    c_filters = [cf for cf in parsed_filters if cf not in uc_filters]

    return c_filters, uc_filters
