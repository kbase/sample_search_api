# -*- coding: utf-8 -*-
#BEGIN_HEADER
import logging
import os

from installed_clients.KBaseReportClient import KBaseReport
from installed_clients.SampleServiceClient import SampleService
from utils.filter_samples import SampleFilterer
from utils.meta_manager import MetadataManager
#END_HEADER


class sample_search_api:
    '''
    Module Name:
    sample_search_api

    Module Description:
    TODO:
Ontology type queries
more complex lexicographical queries (nested or parenthesis)
    '''

    ######## WARNING FOR GEVENT USERS ####### noqa
    # Since asynchronous IO can lead to methods - even the same method -
    # interrupting each other, you must be *very* careful when using global
    # state. A method could easily clobber the state set by another while
    # the latter method is running.
    ######################################### noqa
    VERSION = "0.1.0"
    GIT_URL = "https://github.com/charleshtrenholm/sample_search_api.git"
    GIT_COMMIT_HASH = "6e628e4c48facab106c772c6341c3404d13f272c"

    #BEGIN_CLASS_HEADER
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        re_api_url = config.get('re-api-url', config.get('kbase-endpoint') +
                                '/relation_engine_api')
        self.sample_url = config.get('kbase-endpoint') + '/sampleservice'
        self.shared_folder = config['scratch']
        self.sample_service = SampleService(self.sample_url)
        self.meta_manager = MetadataManager(re_api_url,
                                            re_admin_token=config.get('re-admin-token'))
        self.sample_filter = SampleFilterer(config.get('re-admin-token'), re_api_url,
                                            self.sample_service)
        logging.basicConfig(format='%(created)s %(levelname)s: %(message)s',
                            level=logging.INFO)
        #END_CONSTRUCTOR
        pass


    def filter_samples(self, ctx, params):
        """
        General sample filtering query
        :param params: instance of type "FilterSamplesParams" -> structure:
           parameter "sample_ids" of list of type "SampleAddress" ->
           structure: parameter "id" of type "sample_id" (A Sample ID. Must
           be globally unique. Always assigned by the Sample service.),
           parameter "version" of Long, parameter "filter_conditions" of list
           of type "filter_condition" (Args: metadata_field - should only be
           a controlled_metadata field, if not will error.
           comparison_operator - suppported values for the operators are
           "==", "!=", "<", ">", ">=", "<=", "in", "not in" metadata_values -
           list of values on which to constrain metadata_field with the input
           operator. logical_operator - accepted values for the operators
           are: "and", "or" potential future args: paren_position - None/0 -
           no operation n - add "n" open parenthesis to the beginning of the
           statement -n - add "n" closed paranthesis to the end of statement)
           -> structure: parameter "metadata_field" of String, parameter
           "comparison_operator" of String, parameter "metadata_values" of
           list of String, parameter "logical_operator" of String
        :returns: instance of type "FilterSamplesResults" -> structure:
           parameter "sample_ids" of list of type "SampleAddress" ->
           structure: parameter "id" of type "sample_id" (A Sample ID. Must
           be globally unique. Always assigned by the Sample service.),
           parameter "version" of Long
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN filter_samples
        results = self.sample_filter.filter_samples(params, ctx.get('token'))
        #END filter_samples

        # At some point might do deeper type checking...
        if not isinstance(results, dict):
            raise ValueError('Method filter_samples return value ' +
                             'results is not type dict as required.')
        # return the results
        return [results]

    def get_sampleset_meta(self, ctx, params):
        """
        Gets all metadata fields present in a given list of samples. If samples with different
        custom fields are included, it will return both different fields in an OR style operation.
        This is intended for use in the filter_samplesets dynamic dropdown.
        :param params: instance of type "GetSamplesetMetaParams" ->
           structure: parameter "sample_ids" of list of type "SampleAddress"
           -> structure: parameter "id" of type "sample_id" (A Sample ID.
           Must be globally unique. Always assigned by the Sample service.),
           parameter "version" of Long
        :returns: instance of type "GetSamplesetMetaResults" -> structure:
           parameter "results" of list of String
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN get_sampleset_meta
        results = self.meta_manager.get_sampleset_meta(params['sample_ids'], ctx.get('token'))
        #END get_sampleset_meta
        # At some point might do deeper type checking...
        if not isinstance(results, dict):
            raise ValueError('Method get_sampleset_meta return value ' +
                             'results is not type dict as required.')
        # return the results
        return [results]
    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK",
                     'message': "",
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH}
        #END_STATUS
        return [returnVal]
