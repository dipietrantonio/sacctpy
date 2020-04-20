import unittest
from .context import * 
from datetime import datetime
import time



class GetJobsTestCase(unittest.TestCase):


    @unittest.skipUnless(CLUSTER  == 'Topaz', "sacct is only available on clusters.") 
    def test_retrieve_small_number_of_jobs(self):
        start_date = datetime(2020, 3, 1, 5, 0, 0)
        end_date = datetime(2020, 3, 1, 23, 0, 0)
        output = sacctpy.sacct(start_time=start_date, end_time=end_date, header=['Account', 'Start'])
        results = list(sacctpy.parse(output)) 
        self.assertTrue(any(x.Account == 'director2113' and x.Start == datetime(2020, 3, 1, 15, 6, 21) for x in results))



    @unittest.skipUnless(CLUSTER  == 'Topaz', "sacct is only available on clusters.") 
    def test_retrieve_two_weeks_of_jobs(self):
        start_date = datetime(2020, 3, 1, 5, 0, 0)
        end_date = datetime(2020, 3, 17, 23, 0, 0)
        output = sacctpy.sacct(start_time=start_date, end_time=end_date, header=['Account', 'Start']) 
        with open('file.txt', 'w') as fp:
            fp.write(output)
         
        results = list(sacctpy.parse(output)) 
        self.assertTrue(any(x.Account == 'director2113' and x.Start == datetime(2020, 3, 1, 15, 6, 21) for x in results))


if __name__ == "__main__":
    unittest.main()
