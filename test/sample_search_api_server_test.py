# -*- coding: utf-8 -*-
import os
import time
import json
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

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

    # @unittest.skip('x')
    def test_multi_condition_filter_from_same_sample_set(self):
        # retrieve a list of samples
        params = {
            'workspace_name': self.wsName,
            'sample_ids': self.valid_sample_ids,
            'filter_conditions': [
                {
                    'metadata_field': "latitude",
                    'operator': ">",
                    'metadata_values': ["0.0"],
                    'join_condition': "AND"
                },
                {
                    'metadata_field': "longitude",
                    'operator': ">",
                    'metadata_values': ["0.0"],
                    'join_condition': "OR"
                },
                {
                    'metadata_field': "state_province",
                    'operator': "==",
                    'metadata_values': ["Georgia"],
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
        self.assertEqual([{"id": s['id'], "version": s['version']} for s in ret['sample_ids']],[
            {'id': 'c9daec72-348e-426b-bef6-04bcdd0e01fa', 'version': 1},
            {'id': 'efffc90e-64bb-48fb-97c9-c2db3f37f7fc', 'version': 1},
            {'id': '3d108e8a-d583-4aa4-a2b8-0ae592abf066', 'version': 1},
            {'id': 'b969c622-ea18-4dda-9943-bf1692e526dd', 'version': 1}
        ])
        print(f'filter samples test_multi_condition_filter_from_same_sample_set takes {end - start} seconds to run')

    # @unittest.skip('x')
    def test_multi_value_filter_condition(self):
        # retrieve a list of samples
        params = {
            'workspace_name': self.wsName,
            'sample_ids': self.valid_sample_ids,
            'filter_conditions': [
                {
                    'metadata_field': "state_province",
                    'operator': "in",
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
        self.assertEqual([{"id": s['id'], "version": s['version']} for s in ret['sample_ids']],[
            {'id': 'c9daec72-348e-426b-bef6-04bcdd0e01fa', 'version': 1},
            {'id': 'efffc90e-64bb-48fb-97c9-c2db3f37f7fc', 'version': 1},
            {'id': '1cd59e35-9057-427b-9b88-17077a5810e4', 'version': 1},
            {'id': '6fc28cbd-39b3-4da6-a928-e56a680111b7', 'version': 1},
            {'id': '3f272ea3-6e52-4e09-b5c5-525217ac5a49', 'version': 1}
        ])
        print(f'filter test_multi_value_filter_condition samples takes {end - start} seconds to run')

    # @unittest.skip('x')
    def test_not_enough_samples(self):
        params = {
            'workspace_name': self.wsName,
            'sample_ids': self.valid_sample_ids[:1],
            'filter_conditions': [
                {
                    'metadata_field': "latitude",
                    'operator': ">",
                    'metadata_values': ["0.0"],
                    'join_condition': "AND"
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
        pass

    # @unittest.skip('x')
    def test_too_many_values_for_operator(self):
        pass

    # @unittest.skip('x')
    def test_non_accepted_operator_for_field_type(self):
        pass

    # @unittest.skip('x')
    def test_(self):
        pass

    # @unittest.skip('x')
    def test_value_has_whitespace(self):
        pass

    # @unittest.skip('x')
    def test_catch_injection(self):
        # Should build out framework for this more effectively.
        # Perhaps we don't have to worry because RE is read_only
        pass

    # @unittest.skip('x')
    def test_static_metadata_validation(self):
        sf = SampleFilterer(self.ctx, self.re_api_url, self.sample_service)
        # here we test the validation functionality
        ret = sf._validate_filters([
            {'field': 'name',  # string validator
                'operator': '==','values': ['foo'],'join': 'and'},
            {'field': 'latitude',  # number validator
                'operator': ">=",'values': ["45.046"],'join': "or"},
            {'field': 'sesar:material',  # enum validator
                'operator': "!=",'values': ['Sediment'],'join': 'or'},
            {'field': 'biome',  # ontology_has_ancestor validator
                'operator': "==",'values': ['ENVO:00000001'],'join': 'and'},
            {'field': 'city_township',  # noop validator
                'operator': "IN",'values': ["Barcelona", "Madrid", "Sevilla", "Granada"]}
        ])
        self.assertEqual(len(ret), 5)
        # we only expect the number validtor to be transformed
        expected_ret = [
            {'field': 'name', 'operator': '==', 'values': ['foo'], 'join': 'and'},
            {'field': 'latitude', 'operator': '>=', 'values': [45.046], 'join': 'or'},
            {'field': 'sesar:material', 'operator': '!=', 'values': ['Sediment'], 'join': 'or'},
            {'field': 'biome', 'operator': '==', 'values': ['ENVO:00000001'], 'join': 'and'},
            {'field': 'city_township', 'operator': 'IN', 'values': ["Barcelona", "Madrid", "Sevilla", "Granada"]}
        ]
        self.assertEqual(ret, expected_ret)

