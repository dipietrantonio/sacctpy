import sys
import os
import sacctpy
import logging

CLUSTER = os.environ.get("CLUSTER", None) 
sys.path.append('..')
MODULE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA = os.path.join(MODULE_ROOT, 'tests', 'data')
logging.basicConfig(level=logging.INFO)
