/*
A KBase module: sample_search_api
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
            "=", "==", "equals", "equal", "is"
            "!=", "not equals", "does not equal", "is not"
            "<", "less than"
            ">", "greater than"
            ">=", "greater than or equal to"
            "<=", "less than or equal to"
            "in", "is in"
            "not in", "is not in"
            note: each of the values in the above lines evaluates to the same python operator (which corresponds to the first value).
        metadata_value - value on which to constrain metadata_field with the input operator.
        join_condition - accepted values for the operators are:
        "and", "&", "&&"
        "or", "|", "||"
    */

    typedef structure{
        string metadata_field;
        string operator;
        string metadata_value;
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
