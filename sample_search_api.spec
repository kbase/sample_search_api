/*
A KBase module: sample_search_api
*/

/*

TODO:
    Ontology type queries
    more complex lexicographical queries (nested or parenthesis)

*/


module sample_search_api {


    /* A Sample ID. Must be globally unique. Always assigned by the Sample service. */
    typedef string sample_id;

    typedef structure{
        sample_id id;
        int version;
    } SampleAddress;

    /*
    Args:
        metadata_field - should only be a controlled_metadata field, if not will error.
        comparison_operator - suppported values for the operators are
            "==", "!=", "<", ">", ">=", "<=", "in", "not in"
        metadata_values - list of values on which to constrain metadata_field with the input operator.
        logical_operator - accepted values for the operators are:
            "and", "or"

    potential future args:
        paren_position -
            None/0 - no operation
            n - add "n" open parenthesis to the beginning of the statement
            -n - add "n" closed paranthesis to the end of statement
    */

    typedef structure{
        string metadata_field;
        string comparison_operator;
        list<string> metadata_values;
        string logical_operator;
    } filter_condition;

    typedef structure{
        list<SampleAddress> sample_ids;
        list<filter_condition> filter_conditions;
    } FilterSamplesParams;

    typedef structure {
        list<SampleAddress> sample_ids;
    } FilterSamplesResults;

    /*
    General sample filtering query
    */
    funcdef filter_samples(FilterSamplesParams params) returns (FilterSamplesResults results) authentication required;

    typedef structure {
        list<string> sample_set_refs;
    } GetSamplesetMetaParams;

    /*
    Gets all metadata fields present in a given list of sampleset refs. If samples with different custom fields are
    included, it will return both different fields in an OR style operation. This is intended for use in the 
    filter_samplesets dynamic dropdown.
    */
    funcdef get_sampleset_meta(GetSamplesetMetaParams params) returns (list<string> results) authentication required;
};
