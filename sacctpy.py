"""
Implements a Python interface to sacct.
"""
import subprocess
import os
from numbers import Integral
from collections import namedtuple
from datetime import timedelta, datetime, date
import calendar
from functools import partial
import logging
import time


month_to_num = {name: num for num, name in enumerate(calendar.month_abbr) if num}
letter_to_exp = {x : e for e, x in enumerate(['K', 'M', 'G', 'T', 'P'], 1)}

# set a SLURM environment variable to return dates in standard format
os.environ['SLURM_TIME_FORMAT'] = 'standard'



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



def to_datetime(x):
    if x == 'Unknown':
        return None
    (year, month, day), (hour, minute, second) = (int(v) for v in x[:10].split('-')), (int(v) for v in x[11:].split(':')) 
    return datetime(year, month, day, hour, minute, second)



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
    "Cluster"             : ident,                    "Comment"             : ident,      # "Constraints"        : ident,
    "ConsumedEnergy"      : ident,                    "ConsumedEnergyRaw"   : ident,      "CPUTime"            : to_elapsed,
    "CPUTimeRAW"          : number,                   "DerivedExitCode"     : ecode,      "Elapsed"            : to_elapsed,
    "ElapsedRaw"          : number,                   "Eligible"            : to_datetime,       "End"                : to_datetime,
    "ExitCode"            : ecode,                    "GID"                : ident,       # "Flags"               : ident, 
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
    "QOS"                 : ident,                    "QOSRAW"              : ident,      # "Reason"             : ident,
    "ReqCPUFreq"          : number10,                 "ReqCPUFreqMin"       : number10,   "ReqCPUFreqMax"      : number10,
    "ReqCPUFreqGov"       : ident,                    "ReqCPUS"             : number10,   "ReqGRES"            : partial(asdict, sep=':'),
    "ReqMem"              : number,                   "ReqNodes"            : number10,   "ReqTRES"            : asdict,
    "Reservation"         : ident,                    "ReservationId"       : ident,      "Reserved"           : to_elapsed,
    "ResvCPU"             : to_elapsed,               "ResvCPURAW"          : number10,   "Start"              : to_datetime,
    "State"               : ident,                    "Submit"              : to_datetime,       "Suspended"          : to_elapsed,
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



def __exec_sacct(kwargs : 'dict'):
    """
    Supports the execution of `sacct` building the command line arguments and executing
    the `sacct` utility.
    """
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
    header = kwargs.get('header', sorted(SACCT_OUTPUT_INFO.keys()))
    args.append('--format={}'.format(','.join(header)))
    proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=os.environ)
    proc_out = proc.stdout.decode('utf8')
    if proc_out.startswith('sacct: error'):
        raise Exception(proc_out) 
    return proc_out



def sacct(**kwargs):
    """
    Queries the Slurm job accounting log or Slurm database to retrieve information
    about jobs marching the specified search criteria.

    Description
    -----------
    In case the data requested span over a time period greater than a week, the query
    will be splitted in sub queries such that sacct will be called for 

    Parameters
    ----------
    The function takes only keywords-only arguments. They are:

    header : list of str, optional
        Returns data associated to specified columns.
    
    pause : float, optional
        In case the query must be splitted in subqueries, the time to wait before
        one query and the next.

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
    jobs : list
        A list of named tuples, each representing a job.
    """
    # First check. If the data required spans over one week time period,
    # break the query in multiple sub queries, of one week length each.
    today = date.today()
    beginning_of_day = datetime(today.year, today.month, today.day)
    start_time, end_time = kwargs.get('start_time', beginning_of_day), kwargs.get('end_time', datetime.now())
    time_period = end_time - start_time
    week_period = timedelta(days=7)
    if time_period > week_period:
        time_pause = kwargs.get('pause', 1)
        final_output = str()
        week_period_start = start_time
        while week_period_start < end_time:
            week_period_end = week_period_start + week_period
            week_period_end = week_period_end if week_period_end < end_time else end_time 
            logging.info(f"Querying time period {week_period_start} - {week_period_end}") 
            kwargs['start_time'] = week_period_start
            kwargs['end_time'] = week_period_end
            output = __exec_sacct(kwargs)
            if len(final_output) > 0:
                output = output[output.find('\n')+1:]
            final_output += output
            week_period_start = week_period_end
            time.sleep(time_pause)
        return final_output
    else:
        return __exec_sacct(kwargs)



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



def parse(sacct_out, sep='|'):
    if isinstance(sacct_out, str):
        sacct_out = _line_iterator(sacct_out)
    header = next(sacct_out).strip().split(sep)
    SlurmJob = namedtuple('SlurmJob', header)
    for line in sacct_out:        
        if len(line) > 0:
            yield SlurmJob(*(SACCT_OUTPUT_INFO[header[i]](x) for i, x in enumerate(line.strip().split(sep))))

