import unittest
from .context import sacctpy


class SacctLauncherUnitTest(unittest.TestCase):

    @unittest.skip("not available")
    def test_sacct_launch(self):
        test_output = sacctpy.sacct_exec()
        self.assertEqual(['JobID', 'JobName', 'Partition', 'Account',
            'AllocCPUS', 'State', 'ExitCode'], test_output.splitlines()[0].split())
