import unittest
from .context import sacctpy
from datetime import datetime



class GetAllJobsTestCase(unittest.TestCase):

    @unittest.skip("pawsey0001 not available")
    def test_total_number_of_jobs(self):
        results = sacctpy.query_sacct(start_time = datetime(2019, 3, 5))
        self.assertGreater(len(results), 0)
        all_accounts = set(x.Account for x in results)
        self.assertTrue('pawsey0001' in all_accounts)
        j = results[0]
        for x in j._fields:
            print(x, getattr(j, x))


class ParseDumpFileTestCase(unittest.TestCase):

    def test_parse_dump_file(self):
        results = sacctpy.query_sacct(start_time = datetime(2019, 3, 5))
        for x in results:
            print(x)