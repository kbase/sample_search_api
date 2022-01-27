# -*- coding: utf-8 -*-
import os
import time
import unittest
from configparser import ConfigParser

from sample_search_api.sample_search_apiImpl import sample_search_api
from sample_search_api.sample_search_apiServer import MethodContext
from sample_search_api.authclient import KBaseAuth as _KBaseAuth
from utils.filter_samples import SampleFilterer

from installed_clients.WorkspaceClient import Workspace
from installed_clients.SampleServiceClient import SampleService


class sample_search_apiTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        token = os.environ.get('KB_AUTH_TOKEN', None)
        config_file = os.environ.get('KB_DEPLOYMENT_CONFIG', None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('sample_search_api'):
            cls.cfg[nameval[0]] = nameval[1]
        # Getting username from Auth profile for token
        authServiceUrl = cls.cfg['auth-service-url']
        auth_client = _KBaseAuth(authServiceUrl)
        user_id = auth_client.get_user(token)
        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        cls.ctx = MethodContext(None)
        cls.ctx.update({'token': token,
                        'user_id': user_id,
                        'provenance': [
                            {'service': 'sample_search_api',
                             'method': 'please_never_use_it_in_production',
                             'method_params': []
                             }],
                        'authenticated': 1})
        cls.wsURL = cls.cfg['workspace-url']
        cls.wsClient = Workspace(cls.wsURL)
        cls.serviceImpl = sample_search_api(cls.cfg)
        cls.re_api_url = cls.cfg['kbase-endpoint'] + '/relation_engine_api'
        cls.sample_service = SampleService(cls.cfg['kbase-endpoint'] + '/sampleservice')
        cls.scratch = cls.cfg['scratch']
        cls.callback_url = os.environ['SDK_CALLBACK_URL']
        suffix = int(time.time() * 1000)
        cls.wsName = "test_ContigFilter_" + str(suffix)
        ret = cls.wsClient.create_workspace({'workspace': cls.wsName})  # noqa
        cls.valid_sample_ids = [
            {'id': 'c9daec72-348e-426b-bef6-04bcdd0e01fa', 'version': 1},
            {'id': 'efffc90e-64bb-48fb-97c9-c2db3f37f7fc', 'version': 1},
            {'id': '1cd59e35-9057-427b-9b88-17077a5810e4', 'version': 1},
            {'id': '6fc28cbd-39b3-4da6-a928-e56a680111b7', 'version': 1},
            {'id': '3f272ea3-6e52-4e09-b5c5-525217ac5a49', 'version': 1},
            {'id': '3d108e8a-d583-4aa4-a2b8-0ae592abf066', 'version': 1},
            {'id': 'b969c622-ea18-4dda-9943-bf1692e526dd', 'version': 1}
        ]

        # corresponding sample set object ref for the above samples
        cls.sampleset_object_ref = '64822/2/1'

        # for testing get_sampleset_meta
        cls.valid_enigma_sample_ids = [
            {"id": "cb77625e-e6af-4a1e-846a-71788c66904b", 'version': 1},
            {"id": "70dc08fa-3331-4090-a1ad-79fb872a8082", "version": 1},
            {"id": "f44b4278-1fc8-4607-aa3e-28652309e290", "version": 1},
            {"id": "26a758a6-2779-425f-a8d0-e9c6276d3b9f", "version": 1},
            {"id": "5c587fb6-6510-4e55-8798-5f9504019dfd", "version": 1},
            {"id": "30c54e60-e598-4d9c-8038-0a0c9c82bb63", "version": 1},
            {"id": "ed967127-5ba5-4ae6-a062-d566973aa9c3", "version": 1},
            {"id": "f02a03a7-0e5f-4517-b859-d6061956784f", "version": 1}
        ]

        # corresponding sampleset object ref for the above enigma samples
        cls.enigma_object_ref = '65799/3/2'

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

    # @unittest.skip('x')
    def test_filter_with_one_condition_and_value_has_whitespace(self):
        # NOTE: this filter should not filter any samples out
        params = {
            'workspace_name': self.wsName,
            'sample_ids': self.valid_sample_ids,
            'filter_conditions': [{
                'metadata_field': "name",
                'comparison_operator': "!=",
                'metadata_values': ["    this has spaces and thats okay!  "]
            }]
        }
        start = time.time()
        ret = self.serviceImpl.filter_samples(
            self.ctx,
            params
        )[0]
        end = time.time()
        self.assertEqual(len(ret['sample_ids']), 7)
        self.assertEqual(ret['sample_ids'], self.valid_sample_ids)
        print('filter samples test_filter_with_one_condition_and_value_has_whitespace takes '
              f'{end - start} seconds to run')

    # @unittest.skip('x')
    def test_multi_condition_filter_from_same_sample_set(self):
        # retrieve a list of samples
        params = {
            'workspace_name': self.wsName,
            'sample_ids': self.valid_sample_ids,
            'filter_conditions': [
                {
                    'metadata_field': "latitude",
                    'comparison_operator': ">",
                    'metadata_values': ["0.0"],
                    'logical_operator': "AND"
                },
                {
                    'metadata_field': "longitude",
                    'comparison_operator': ">",
                    'metadata_values': ["0.0"],
                    'logical_operator': "OR"
                },
                {
                    'metadata_field': "state_province",
                    'comparison_operator': "==",
                    'metadata_values': ["Georgia"]
                }
            ]
        }
        start = time.time()
        ret = self.serviceImpl.filter_samples(
            self.ctx,
            params
        )[0]
        end = time.time()
        self.assertEqual(len(ret['sample_ids']), 4)
        self.assertEqual([{"id": s['id'], "version": s['version']} for s in ret['sample_ids']], [
            {'id': 'c9daec72-348e-426b-bef6-04bcdd0e01fa', 'version': 1},
            {'id': 'efffc90e-64bb-48fb-97c9-c2db3f37f7fc', 'version': 1},
            {'id': '3d108e8a-d583-4aa4-a2b8-0ae592abf066', 'version': 1},
            {'id': 'b969c622-ea18-4dda-9943-bf1692e526dd', 'version': 1}
        ])
        print('filter samples test_multi_condition_filter_from_same_sample_set '
              f'takes {end - start} seconds to run')

    # @unittest.skip('x')
    def test_multi_value_filter_condition(self):
        # retrieve a list of samples
        params = {
            'workspace_name': self.wsName,
            'sample_ids': self.valid_sample_ids,
            'filter_conditions': [
                {
                    'metadata_field': "state_province",
                    'comparison_operator': "in",
                    'metadata_values': ["Georgia", "Washington", "Tennessee"],
                }
            ]
        }
        start = time.time()
        ret = self.serviceImpl.filter_samples(
            self.ctx,
            params
        )[0]
        end = time.time()
        self.assertEqual(len(ret['sample_ids']), 5)
        self.assertEqual([{"id": s['id'], "version": s['version']} for s in ret['sample_ids']], [
            {'id': 'c9daec72-348e-426b-bef6-04bcdd0e01fa', 'version': 1},
            {'id': 'efffc90e-64bb-48fb-97c9-c2db3f37f7fc', 'version': 1},
            {'id': '1cd59e35-9057-427b-9b88-17077a5810e4', 'version': 1},
            {'id': '6fc28cbd-39b3-4da6-a928-e56a680111b7', 'version': 1},
            {'id': '3f272ea3-6e52-4e09-b5c5-525217ac5a49', 'version': 1}
        ])
        print('filter test_multi_value_filter_condition samples '
              f'takes {end - start} seconds to run')

    # @unittest.skip('x')
    def test_not_enough_samples(self):
        params = {
            'workspace_name': self.wsName,
            'sample_ids': self.valid_sample_ids[:1],
            'filter_conditions': [
                {
                    'metadata_field': "latitude",
                    'comparison_operator': ">",
                    'metadata_values': ["0.0"],
                    'logical_operator': "AND"
                }
            ]
        }
        with self.assertRaises(ValueError) as context:
            _ = self.serviceImpl.filter_samples(
                self.ctx,
                params
            )
        self.assertEqual(
            "Must provide at least two samples in 'sample_ids'",
            str(context.exception)
        )

    # @unittest.skip('x')
    def test_no_filters(self):
        params = {
            'workspace_name': self.wsName,
            'sample_ids': self.valid_sample_ids,
            'filter_conditions': []
        }
        with self.assertRaises(ValueError) as context:
            _ = self.serviceImpl.filter_samples(
                self.ctx,
                params
            )
        self.assertEqual(
            "Must provide at least one filter condition in 'filter_conditions' as input.",
            str(context.exception)
        )

    # @unittest.skip('x')
    def test_non_metadata_field(self):
        fake_field = "kachow!"
        params = {
            'workspace_name': self.wsName,
            'sample_ids': self.valid_sample_ids,
            'filter_conditions': [{
                'metadata_field': fake_field,
                'comparison_operator': "==",
                'metadata_values': ["value"],
                'logical_operator': "AND"
            }]
        }
        with self.assertRaises(Exception) as context:
            _ = self.serviceImpl.filter_samples(
                self.ctx,
                params
            )
        self.assertEqual(
            f"Unable to resolve metadata fields or prefix metadata fields: {fake_field}",
            str(context.exception)
        )

    # @unittest.skip('x')
    def test_too_many_values_for_comparison_operator(self):
        params = {
            'workspace_name': self.wsName,
            'sample_ids': self.valid_sample_ids,
            'filter_conditions': [{
                'metadata_field': "latitude",
                'comparison_operator': "==",
                'metadata_values': ["30.023", "45.03"],
                'logical_operator': "AND"
            }]
        }
        with self.assertRaises(ValueError) as context:
            _ = self.serviceImpl.filter_samples(
                self.ctx,
                params
            )
        self.assertEqual(
            "Provided comparison operator '==' expects 1 value, 2 values were provided.",
            str(context.exception)
        )

    # @unittest.skip('x')
    def test_non_accepted_comparison_operator_for_field_type(self):
        params = {
            'workspace_name': self.wsName,
            'sample_ids': self.valid_sample_ids,
            'filter_conditions': [{
                'metadata_field': "state_province",
                'comparison_operator': ">",
                'metadata_values': ["Georgia"],
                'logical_operator': "AND"
            }]
        }
        with self.assertRaises(ValueError) as context:
            _ = self.serviceImpl.filter_samples(
                self.ctx,
                params
            )
        self.assertEqual(
            "provided operator '>' not accepted for metadata "
            "field 'state_province' in filter condition at position 0.",
            str(context.exception)
        )

    @unittest.skip('not implemented')
    def test_catch_injection(self):
        # Should build out framework for this more effectively.
        # Perhaps we don't have to worry because RE is read_only
        # and 'value' field is provided as @value for AQL query.
        pass

    # @unittest.skip('x')
    def test_static_metadata_validation(self):
        sf = SampleFilterer(self.ctx, self.re_api_url, self.sample_service)
        # here we test the validation functionality
        ret = sf._format_and_validate_filters([
            {'field': 'name',  # string validator
                'comp_op': '==', 'values': ['foo'], 'logic_op': 'and'},
            {'field': 'latitude',  # number (float) validator
                'comp_op': ">=", 'values': ["45.046"], 'logic_op': "or"},
            {'field': 'longitude',  # number (int) validator
                'comp_op': ">=", 'values': ["35"], 'logic_op': "or"},
            {'field': 'sesar:material',  # enum validator
                'comp_op': "!=", 'values': ['Sediment'], 'logic_op': 'or'},
            {'field': 'biome',  # ontology_has_ancestor validator
                'comp_op': "==", 'values': ['ENVO:00000001'], 'logic_op': 'and'},
            {'field': 'city_township',  # noop validator
                'comp_op': "IN", 'values': ["Barcelona", "Madrid", "Sevilla", "Granada"]}
        ], [], self.ctx['token'])
        self.assertEqual(len(ret), 6)
        # we only expect the number validtor to be transformed
        expected_ret = [
            {'field': 'name', 'comp_op': '==', 'values': ['foo'], 'logic_op': 'and'},
            {'field': 'latitude', 'comp_op': '>=', 'values': [45.046], 'logic_op': 'or'},
            {'field': 'longitude', 'comp_op': '>=', 'values': [35], 'logic_op': 'or'},
            {'field': 'sesar:material', 'comp_op': '!=', 'values': ['Sediment'], 'logic_op': 'or'},
            {'field': 'biome', 'comp_op': '==', 'values': ['ENVO:00000001'], 'logic_op': 'and'},
            {'field': 'city_township', 'comp_op': 'IN',
                'values': ["Barcelona", "Madrid", "Sevilla", "Granada"]}
        ]
        self.assertEqual(ret, expected_ret)

    # @unittest.skip('x')
    def test_get_sampleset_meta(self):
        params = {
            'sample_set_refs': [self.sampleset_object_ref]
        }
        results = self.serviceImpl.get_sampleset_meta(self.ctx, params)[0]
        # get list of unique items to compare length
        result_fields = {f['field'] for f in results}

        self.assertIsInstance(results, list)
        # check that the results are dicts of strings
        self.assertIsInstance(results[0], dict)
        self.assertIsInstance(results[0]['field'], str)
        # check that all meta field values are unique
        self.assertEqual(len(result_fields), len(results))

        self.assertIn({'field': 'sesar:igsn'}, results)
        self.assertIn({'field': 'purpose'}, results)

    # @unittest.skip('x')
    def test_get_sampleset_meta_uncontrolled_fields(self):

        """
        Gets uncontrolled meta fields and prepends with "custom:"
        """

        params = {
            'sample_set_refs': [self.enigma_object_ref]
        }

        ret = self.serviceImpl.get_sampleset_meta(self.ctx, params)[0]

        custom_fields = [r['field'] for r in ret if r['field'].startswith('custom:')]

        self.assertEqual(len(ret), 51)
        self.assertEqual(len(custom_fields), 8)
        self.assertIn('custom:adams_nitrate_um', custom_fields)
        self.assertIn('custom:hazen_n2_mm', custom_fields)

    # @unittest.skip('x')
    def test_get_sampleset_meta_with_different_sample_columns(self):

        '''
        test that all metadata columns come back exactly once, regardless of whether
        they exist in all of the samples queried for
        '''

        params = {
            'sample_set_refs': [self.sampleset_object_ref, self.enigma_object_ref]
        }

        results = self.serviceImpl.get_sampleset_meta(self.ctx, params)[0]
        # convert to set
        result_fields = {r['field'] for r in results}

        self.assertEqual(len(result_fields), len(results))
        # ensure that there are unconrolled meta keys included
        # (even when not included in all samplesets)
        self.assertIn({'field': 'custom:hazen_uranium_mg_l'}, results)
        self.assertEqual(len(results), 69)

    def test_filter_samples_with_uncontrolled_fields(self):
        params = {
            'sample_ids': self.valid_enigma_sample_ids,
            'filter_conditions': [{
                'metadata_field': "custom:hazen_n2_mm",
                'comparison_operator': ">",
                'metadata_values': ["1"],
                'logical_operator': "OR"
            }]
        }
        ret = self.serviceImpl.filter_samples(self.ctx, params)
        self.assertEqual(len(ret[0]['sample_ids']), 3)
        self.assertEqual(ret[0]['sample_ids'], [
            {'id': '26a758a6-2779-425f-a8d0-e9c6276d3b9f', 'version': 1},
            {'id': 'ed967127-5ba5-4ae6-a062-d566973aa9c3', 'version': 1},
            {'id': 'f02a03a7-0e5f-4517-b859-d6061956784f', 'version': 1}
        ])

    # @unittest.skip('x')
    def test_validate_filters_uncontrolled_fields(self):

        """
        Fails when uncontrolled field prefixed with "custom:" is not resolved
        """

        sf = SampleFilterer(self.ctx, self.re_api_url, self.sample_service)

        filters = [
            # controlled field shouldn't throw
            {
                'field': 'ph',
                'comp_op': '>=',
                'values': [7],
                'logic_op': 'or'
            },
            # uncontrolled field that exists so shouldn't throw
            {
                'field': 'custom:hazen_uranium_mg_l',
                'comp_op': '>',
                'values': ['0'],
                'logic_op': 'or'
            },
            # field that does not exist on any sampleset should throw
            {
                'field': 'custom:AAAAAAA',
                'comp_op': '>',
                'values': ['0'],
                'logic_op': 'or'
            }
        ]

        # test 2 different types of samplesets for uncontrolled fields
        # (they should work if theres at least one)
        test_samples = self.valid_enigma_sample_ids + self.valid_sample_ids

        with self.assertRaises(ValueError) as context:
            sf._format_and_validate_filters(filters, test_samples, self.ctx['token'])

        self.assertIn('Unable to resolve uncontrolled custom metadata fields:',
                      str(context.exception))
        self.assertIn('custom:AAAAAAA', str(context.exception))
        self.assertNotIn('custom:hazen_uranium_mg_l', str(context.exception))
        self.assertNotIn('ph', str(context.exception))
