#!/usr/bin/env python
import argparse
import ConfigParser
import exceptions
import os
import socket
import StringIO
import sys
import xmlrpclib


def read_config(config_file):

    parser=ConfigParser.SafeConfigParser()
    parser.read([config_file])

    for section in parser.sections():
        
        for option in parser.options(section):
            
            try:
                value=parser.getint(section,option)
            except ValueError:
                value=parser.get(section,option)

    return parser 



def main(args):

    config = read_config('/opt/storage/python/ratking/etc/ratkingd.conf')

    server_url = 'http://'+config.get('xmlrpc', 'host')+':'+config.get('xmlrpc', 'port')+config.get('xmlrpc', 'url')
    s = xmlrpclib.ServerProxy(server_url)

    realuser = os.getlogin()
    user = os.environ['USER']

    test = True

    try:

        # check if user running the client is allowed...
        returncode,output = s.check_auth(user)

        # no passing go broseph... 
        if returncode is False:
            print output
            sys.exit(1)

        if args.listjobs:
            out = s.show_jobs()

            # length of 2 means that only the header + divider line were received, aka no jobs
            if len(out) == 2:
                returncode, output = False, "No jobs."

            else:
                form = StringIO.StringIO()
            
                for line in out:
                    form.write("%s\n" % line)

                returncode, output = [True, form.getvalue()] 

        elif args.addjob:
            returncode, output = s.add_job(args.addjob, user, realuser)

        elif args.disablejob:
            returncode, output = s.disable_job(args.disablejob, user, realuser)

        elif args.enablejob:
            returncode, output = s.enable_job(args.enablejob, user, realuser)

        elif args.forcerun:
            returncode, output = s.force_run_job(args.forcerun, user, realuser)

        elif args.removejob:
            returncode, output = s.remove_job(args.removejob, user, realuser)

        elif args.status:
            returncode, output = s.check_sched()

        elif args.startsched:
            returncode, output = s.start_sched(user)

        elif args.stopsched:
            returncode, output = s.stop_sched(user)

        if returncode is False:
            print "ERROR: %s" % output

        else:
            print output

    except xmlrpclib.Fault as fault:

        print "ERROR: %s" % fault
        sys.exit(1)

    except socket.error as se:
        print "ERROR: Socket error, XMLRPC server not running..."                            
        sys.exit(1)

if __name__ == '__main__':

    
    parser = argparse.ArgumentParser()
    parser.add_argument('--add_job',
                        default=False,
                        dest='addjob',
                        required=False,
                        help='add a new job. takes a filename as an argument')
    parser.add_argument('--disable_job',
                        default=False,
                        dest='disablejob',
                        required=False,
                        help='disables job JOBNAME')
    parser.add_argument('--enable_job',
                        default=False,
                        dest='enablejob',
                        required=False,
                        help='enables job JOBNAME')
    parser.add_argument('--force_run_job',
                        default=False,
                        dest='forcerun',
                        required=False,
                        help='run a job. takes a jobname as an argument')
    parser.add_argument('--list_jobs',
                        action='store_true',
                        default=False,
                        dest='listjobs',
                        required=False,
                        help='list configured jobs')
    parser.add_argument('--remove_job',
                        default=False,
                        dest='removejob',
                        required=False,
                        help='remove a job. takes a jobname as an argument')
    parser.add_argument('--stop_scheduling',
                        action='store_true',
                        default=False,
                        dest='stopsched',
                        required=False,
                        help='stop job scheduling. no more jobs will run until scheduling started again')
    parser.add_argument('--start_scheduling',
                        action='store_true',
                        default=False,
                        dest='startsched',
                        required=False,
                        help='start job scheduling.')
    parser.add_argument('--status',
                        action='store_true',
                        default=False,
                        dest='status',
                        required=False,
                        help='check the status of the job scheduler')
    args = parser.parse_args()

    if len(vars(args)) == 0:
        parser.print_usage()
        sys.exit(0)

    else: 
        main(args)




