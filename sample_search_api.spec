/*
A KBase module: sample_search_api
*/

module sample_search_api {
    typedef structure {
        string report_name;
        string report_ref;
    } ReportResults;

    /*
        This example function accepts any number of parameters and returns results in a KBaseReport
    */
    funcdef run_sample_search_api(mapping<string,UnspecifiedObject> params) returns (ReportResults output) authentication required;

};
