# sample_search_api release notes
=========================================

0.1.0
-----
* Adding get_sampleset_meta method to get sample field names from given list of sample ids
* Adding support for filtering by custom user-defined fields
* Adding "any" type for user-defined fields that will attempt to parse field to correct type without
validation schema needed

0.0.2
-----
* Changing filter generation to support terms with colons (":") in them. i.e. `sesar:term`

0.0.1
-----
* Adding filter_samples function for filtering on a list of given samples
* RE_ADMIN_TOKEN is a required catalog defined variable for full functionality

0.0.0
-----
* Module created by kb-sdk init
