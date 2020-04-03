import unittest
from .context import * 
from datetime import datetime
import time



class GetAllJobsTestCase(unittest.TestCase):

    @unittest.skipUnless(ON_CLUSTER, "sacct is only available on clusters.") 
    def test_retrieve_small_number_of_jobs(self):
        yesterday = datetime.fromtimestamp(time.time() - 60 * 60 * 24) 
        output = sacctpy.sacct(start_time = yesterday)
        results = list(sacctpy.parse(output)) 
        self.assertGreater(len(results), 0)
  


    @unittest.skipUnless(ON_CLUSTER, "sacct is only available on clusters.") 
    def test_retrieve_large_number_of_jobs(self):
        one_year_ago = datetime.fromtimestamp(time.time() - 60 * 60 * 24 * 365) 
        output = sacctpy.sacct(start_time = one_year_ago)
        results = sacctpy.parse(output) 
        first_result = next(results)
        print(first_result)



class GetDataFromTopazTestCase(unittest.TestCase):


    @unittest.skipUnless(ON_CLUSTER, "sacct is only available on clusters.") 
    def test_this_year_data_one_week_at_time(self):
        beginning_of_the_year = datetime(day = 1, month=1, year=2020)
        start = beginning_of_the_year
        stop = datetime.fromtimestamp(start.timestamp() +  60 * 60 * 24 * 7)
        now = datetime.now()
        of = open('zeus.log', 'w')
        header_written = False
        while stop <= now:
            print(start)
            output = sacctpy.sacct(start_time = start, end_time=stop)
            if not header_written:
                header_written = True
            else:
                output = output[output.find('\n')+1:]
            of.write(output)
            start = stop
            stop = datetime.fromtimestamp(start.timestamp() +  60 * 60 * 24 * 7)
            time.sleep(2)
        of.close()



class ParseDumpFileTestCase(unittest.TestCase):

    @unittest.skip('too much output')
    def test_parse_dump_file(self):
        with open(f"{DATA}/zeus.log") as fp: 
            results = sacctpy.parse(fp)
            for x in results:
                print(x)



if __name__ == '__main__':
    unittest.main()
