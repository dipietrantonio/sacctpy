import sys
import os
sys.path.append('..')
import sacctpy
CLUSTER = os.environ.get("CLUSTER", None) 
MODULE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA = os.path.join(MODULE_ROOT, 'tests', 'data')
