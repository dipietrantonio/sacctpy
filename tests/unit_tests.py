import unittest
from .context import * 


class SacctLauncherUnitTest(unittest.TestCase):
   
    @unittest.skipUnless(CLUSTER in ['Topaz', 'Magnus', 'Zeus'], "sacct is only available on clusters.") 
    def test_sacct_launch(self):
        """
        Tests if sacct is invoked with success.
        """
        test_output = sacctpy.sacct()
        important_headers = {'JobID', 'JobName', 'Partition', 'Account',
            'AllocCPUS', 'State', 'ExitCode'}
        first_line = test_output.splitlines()[0]
        headers_returned = set(first_line.split('|'))
        self.assertEqual(important_headers, headers_returned.intersection(important_headers))



if __name__ == "__main__":
    unittest.main()
