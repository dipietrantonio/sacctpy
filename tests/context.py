import sys
import os
sys.path.append('..')
import sacctpy
ON_CLUSTER = True if os.environ.get("ON_CLUSTER", False) == "True" else False 
MODULE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA = os.path.join(MODULE_ROOT, 'tests', 'data')
