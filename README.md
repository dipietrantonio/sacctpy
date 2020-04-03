# sacctpy

A Python wrapper around Slurm's ``sacct`` utility.

## Introduction

The package developed in this repository provides a convenient wrapper around ``sacct``
so to query programmatically the Slurm database from the Python interpreter.

The interface is specifically tailored for the Slurm installation at Pawsey Supercomputing
Centre. The output of ``sacctpy`` is parsed into suitable Python objects assuming a certain
format for things like timestamps and timedeltas. 
