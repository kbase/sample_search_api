/*
A KBase module: sample_search_api
*/

/*

TODO:
    Ontology type queries
    more complex lexicographical queries (nested or paranthesis)

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
        operator - suppported values for the operators are
            "==", "!=", "<", ">", ">=", "<=", "in", "not in"
        metadata_values - list of values on which to constrain metadata_field with the input operator.
        join_condition - accepted values for the operators are:
            "and", "or"

    potential future args:
        paren_position -
            None - no operation
            1 - 
            2 - add two open paranthesis
            -1 - 
            -2 - closed paranthesis
    */

    typedef structure{
        string metadata_field;
        string operator;
        list<string> metadata_values;
        string join_condition;
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

};
