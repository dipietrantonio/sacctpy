"""
Implements the Python interface to sacct.
"""


import subprocess
from numbers import Integral
from collections import namedtuple
from datetime import timedelta, datetime
import calendar
from functools import partial

month_to_num = {name: num for num, name in enumerate(calendar.month_abbr) if num}
letter_to_exp = {x : e for e, x in enumerate(['K', 'M', 'G', 'T', 'P'], 1)}



def number(x, base = 1024):
    if x == '': return None
    exp = 0
    if x[-1] in letter_to_exp:
        x, exp = x[:-1], letter_to_exp[x[-1]]  
    try:
        value = int(x)
    except ValueError:
        try:
            value = float(x)
        except ValueError:
            return x
    return value * base ** exp



number10 = partial(number, base=1000)



def date(x):
    return x
    day, month, year = x.split()
    day = int(day)
    month = month_to_num[month]
    year = int(year) if len(year) == 4 else int('20'+year)
    # TODO: check the format


def to_elapsed(t, seconds_first = False):
    """
    Parses the '[DD-[HH:]]MM:SS' SLURM time format into a Python `datetime.timedelta` object.

    Parameters
    ----------

    """
    time_labels = ['seconds', 'minutes', 'hours', 'days']
    if t == 'INVALID': return t
    elif t == '': return None
    order = (lambda x : x) if seconds_first else reversed
    tmp = t.split(':')
    if len(tmp) == 3 and '-' in tmp[0]:
        tmp = tmp[0].split('-') + tmp[1:]
    # process milliseconds if present
    if '.' in tmp[-1]:
        s, ms = (int(x) for x in tmp[-1].split('.'))
        tmp[-1] = s
        tmp.append(ms)
        time_labels.insert(0, 'milliseconds')

    return timedelta(**dict(zip(order(time_labels), (int(x) for x in tmp))))



def ecode(x):
    if x == '': return None
    return tuple(int(c) for c in x.split(':'))



def asdict(x, sep = '='):
    if len(x) == 0: return {}
    return dict(tuple(number(i) for i in pair.split(sep)) for pair in x.split(','))



ident = lambda x : x 


"""
All the information sacct displays about a job.
"""
SACCT_OUTPUT_INFO = {
    "Account"             : ident,                    "AdminComment"        : ident,      "AllocCPUS"          : number10,
    "AllocGRES"           : partial(asdict, sep=':'), "AllocNodes"          : number10,   "AllocTRES"          : asdict,
    "AssocID"             : ident,                    "AveCPU"              : to_elapsed, "AveCPUFreq"         : number10,
    "AveDiskRead"         : number,                   "AveDiskWrite"        : number,     "AvePages"           :  number10,
    "AveRSS"              : number,                   "AveVMSize"           : number,     "BlockID"            : ident,
    "Cluster"             : ident,                    "Comment"             : ident,      "Constraints"        : ident,
    "ConsumedEnergy"      : ident,                    "ConsumedEnergyRaw"   : ident,      "CPUTime"            : to_elapsed,
    "CPUTimeRAW"          : number,                   "DerivedExitCode"     : ecode,      "Elapsed"            : to_elapsed,
    "ElapsedRaw"          : number,                   "Eligible"            : date,       "End"                : date,
    "ExitCode"            : ecode,                    "Flags"               : ident,      "GID"                : ident,
    "Group"               : ident,                    "JobID"               : ident,      "JobIDRaw"           : ident,
    "JobName"             : ident,                    "Layout"              : ident,      "MaxDiskRead"        : number,
    "MaxDiskReadNode"     : ident,                    "MaxDiskReadTask"     : ident,      "MaxDiskWrite"       : number,
    "MaxDiskWriteNode"    : ident,                    "MaxDiskWriteTask"    : ident,      "MaxPages"           : number10,
    "MaxPagesNode"        : ident,                    "MaxPagesTask"        : ident,      "MaxRSS"             : number,
    "MaxRSSNode"          : ident,                    "MaxRSSTask"          : ident,      "MaxVMSize"          : number,
    "MaxVMSizeNode"       : ident,                    "MaxVMSizeTask"       : ident,      "McsLabel"           : ident,
    "MinCPU"              : to_elapsed,               "MinCPUNode"          : ident,      "MinCPUTask"         : ident,
    "NCPUS"               : number10,                 "NNodes"              : number10,   "NodeList"           : ident,
    "NTasks"              : number10,                 "Priority"            : number10,   "Partition"          : ident,
    "QOS"                 : ident,                    "QOSRAW"              : ident,      "Reason"             : ident,
    "ReqCPUFreq"          : number10,                 "ReqCPUFreqMin"       : number10,   "ReqCPUFreqMax"      : number10,
    "ReqCPUFreqGov"       : ident,                    "ReqCPUS"             : number10,   "ReqGRES"            : partial(asdict, sep=':'),
    "ReqMem"              : number,                   "ReqNodes"            : number10,   "ReqTRES"            : asdict,
    "Reservation"         : ident,                    "ReservationId"       : ident,      "Reserved"           : to_elapsed,
    "ResvCPU"             : to_elapsed,               "ResvCPURAW"          : number10,   "Start"              : date,
    "State"               : ident,                    "Submit"              : date,       "Suspended"          : to_elapsed,
    "SystemCPU"           : to_elapsed,               "SystemComment"       : ident,      "Timelimit"          : to_elapsed,
    "TimelimitRaw"        : number10,                 "TotalCPU"            : to_elapsed, "TRESUsageInAve"     : asdict,
    "TRESUsageInMax"      : asdict,                   "TRESUsageInMaxNode"  : asdict,     "TRESUsageInMaxTask" : asdict,
    "TRESUsageInMin"      : asdict,                   "TRESUsageInMinNode"  : asdict,     "TRESUsageInMinTask" : asdict,     
    "TRESUsageInTot"      : asdict,                   "TRESUsageOutAve"     : asdict,     "TRESUsageOutMax"    : asdict,
    "TRESUsageOutMaxNode" : asdict,                   "TRESUsageOutMaxTask" : asdict,     "TRESUsageOutMin"    : asdict,   
    "TRESUsageOutMinNode" : asdict,                   "TRESUsageOutMinTask" : asdict,     "TRESUsageOutTot"    : asdict,        
    "UID"                 : ident,                    "User"                : ident,      "UserCPU"            : to_elapsed,  
    "WCKey"               : ident,                    "WCKeyID"             : ident,      "WorkDir"            : ident,   
}



def __format_datetime(d):
    o = lambda x : "{}{}".format('0' if x < 10 else "", x)
    return f'{o(d.year)}-{o(d.month)}-{o(d.day)}{o(d.hour)}:{o(d.minute)}:{o(d.second)}'



def exec_sacct(**kwargs):
    """
    Queries the Slurm job accounting log or Slurm database to retrieve information
    about jobs marching the specified search criteria.

    Parameters
    ----------
    The function takes only keywords-only arguments. They are:

    user : sequence of str, optional
        Returns jobs submitted by a certain user (default: returns all users jobs).
    
    accounts : sequence of str, optional
        Returns jobs belonging to one of the accounts specified in the list
        (default: returns jobs from all accounts).
    
    end_time : datetime.datetime, optional
        Returns jobs whose end time is less than or equal to `end_time` (default: now).
    
    start_time : datetime.datetime, optional
        Returns jobs whose start time is greater than or equal to `start_time` (default: time 00:00:00 of current day).

    nnodes : int or tuple, optional
        Returns jobs that used at list `nnodes` nodes if an integer is specified; if
        `nnodes` is a tuple, returns jobs that used a number of nodes that is within
        the range specified by `nnodes` (default: any number of nodes).
    
    jobs : list of Integral, optional
        Returns information about the jobs whose id is in `jobs`.
    
    Returns
    -------
    A list of named tuples, each representing a job.
    """
    # run sacct without header info being displayed in the output.
    # Also, the output must be parsable.
    args = ['sacct', '-P']
    sep = '|'
    if 'user' in kwargs:
        args.append('--user={}'.format(','.join(kwargs['user'])))
    else:
        args.append('-a')
    if 'accounts' in kwargs:
        args.extend(('-A', ','.join(kwargs['accounts'])))
    if 'end_time' in kwargs:
        args.extend(('-E', __format_datetime(kwargs['end_time'])))
    if 'start_time' in kwargs:
        args.extend(('-S', __format_datetime(kwargs['start_time'])))
    if 'nnodes' in kwargs:
        i = kwargs['nnodes']
        args.extend(('-i', str(i) if isinstance(i, Integral) else f"{i[0]}-{i[1]}"))
    if 'jobs' in kwargs:
        args.append('--jobs={}'.format(','.join(kwargs['jobs'])))
    
    header = sorted(SACCT_OUTPUT_INFO.keys())
    args.append('--format={}'.format(','.join(header)))
    proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    proc_out = proc.stdout.decode('utf8')
    return proc_out



def _line_iterator(input_str):
    buff = list()
    for x in input_str:
        if x == '\n':
            if len(buff) > 0:
                yield ''.join(buff)
                buff.clear()
        else:
            buff.append(x)
    if len(buff) > 0:
        yield ''.join(buff)



def parse(sacct_out):
    if isinstance(sacct_out, str):
        sacct_out = _line_iterator(sacct_out)
    header = next(sacct_out).split('|')
    print(header)
    for line in sacct_out:        
    SlurmJob = namedtuple('SlurmJob', header)
    jobs = [SlurmJob(*(SACCT_OUTPUT_INFO[header[i]](x) for i, x in enumerate(line.split(sep)))) for line in proc_out.splitlines() if len(line) > 0]
    return jobs

