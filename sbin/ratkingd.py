#!/usr/bin/env python
import argparse
import ConfigParser
import datetime
import daemon
import exceptions
import logging
import pwd
import re
import select
import sys, os, time, atexit
from apscheduler.scheduler import Scheduler
from pwd import getpwnam
from signal import SIGTERM 


class Ratkingd(object):
    """
    Ratkingd class for daemonization, or standalone mode...
    """

    def __init__(self, pidfile, args, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):

        self.args = args
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile

        # this will create a self.config object
        self.initialize_env()

        # setup the logging object
        setup_logging(args, self.config)

    
    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced 
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """

        try: 
            pid = os.fork() 
            if pid > 0:
                # exit first parent
                sys.exit(0) 

        except OSError, e: 
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
    
        # decouple from parent environment
        os.chdir("/") 
        os.setsid() 
        os.umask(0) 
    
        # do second fork
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit from second parent
                sys.exit(0) 

        except OSError, e: 
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1) 
    
        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
    
        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'w+').write("%s\n" % pid)
    

    def delpid(self):
        os.remove(self.pidfile)


    def initialize_env(self):
        """Reads configuration file, etc"""

        # do something with arguments
        config_file = self.args.configfile

        # read configuration files
        self.config = read_config(config_file)


    def restart(self):
        """
        Restart the daemon
        """

        self.stop()
        self.start()


    def run(self):
        """
        You should override this method when you subclass Daemon. It will be called after the process has been
        daemonized by start() or restart().
        """

        # import some modules and then go to the strip club
        sys.path.append(self.config.get('main', 'lib_dir'))
        from ratking.engine import SchedCtl
        from ratking.rpchandler import RpcCtl

        # who am i?
        realuser = os.getlogin()
        user = os.environ['USER']

        # scheduler setup
        sched_options = {}

        for opt in self.config.options('main'):

            m = re.compile(r'^apscheduler.')

            if not bool(m.search(opt)):
                pass

            else:
                conf = '.'.join(opt.split('.')[1:])

                # if the value is an int, we need to pass it to sched.configure properly. handle that here
                isint = re.compile(r'^[0-9]')

                if bool(isint.search(self.config.get('main', opt))):
                    value = int(self.config.get('main', opt))

                else:
                    value = self.config.get('main', opt)

                sched_options[conf] = value
                 

        logging.debug("APScheduler options: %s", sched_options)

        self.sched = Scheduler(**sched_options)

        # we'll control the scheduler through the RatkingCtl class
        try:
            ratking = SchedCtl(self.sched, self.config, logging)
            ratking.initialize()
            ratking.import_jobs()
            ratking.start_sched(user)

        except select.error as e:

            print "Found you, error: %s" % e
            pass

   
        
        logging.info("Starting xmlrpc instance...")
        ratrpc = RpcCtl(self.sched, self.config, logging)
        ratrpc.start_instance()

    '''
    def setup_logging(self):
        """Sets up the proper logging to handle standalone or daemon mode"""

        logger = logging.getLogger('ratkingd')
        logfile = self.config.get('main', 'log_file')

        if args.standalone:
            self.stdout = sys.__stdout__
            self.stderr = sys.__stout__
            logger.StreamHandler( sys.__stdout__ )

        else:
            self.stdout = logfile
            self.stderr = logfile
            logging.StreamHandler( logfile )

        if self.args.debug:
            logging.basicConfig(
                        datefmt='%b %d %H:%M:%S',
                        format='%(asctime)s %(name)s %(levelname)s: %(message)s', 
                        level=logging.DEBUG,
                        tz='UTC')
            #logging.debug('Enabled debug mode, logging set to DEBUG.')

        else:
            logging.basicConfig(
                        datefmt='%b %d %H:%M:%S',
                        format='%(asctime)s ratkingd %(levelname)s: %(message)s', 
                        level=logging.INFO,
                        tz='UTC')
            #logging.info('Normal mode, logging set to INFO.')

        return logger
    '''


    def start(self):
        """
        Start the daemon
        """

        #self.initialize_env()

        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()

        except IOError:
            pid = None
    
        if pid:
            message = "pidfile %s already exist. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)

        logging.getLogger('ratkingd')
        logging.info("Starting up ratkingd...")

        if args.standalone:
            self.run()

        else:
            # Start the daemon
            self.daemonize()
            self.run()


    def stop(self):
        """
        Stop the daemon
        """

        logging.getLogger('ratking')
        logging.info("Stopping ratkingd...")

        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()

        except IOError:
            pid = None
    
        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        # Try killing the daemon process    
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)

        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)




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

def setup_logging(args, config):
    """Sets up the proper logging to handle standalone or daemon mode"""

    logging.getLogger('ratkingd')
    #config = read_config(config_file)
    logfile = config.get('main', 'log_file')

    if args.standalone:
        #self.stdout = sys.__stdout__
        #self.stderr = sys.__stout__
        logging.StreamHandler( sys.__stdout__ )

    else:
        #self.stdout = logfile
        #self.stderr = logfile
        logging.StreamHandler( logfile )

    if args.debug:
        logging.basicConfig(
                    datefmt='%b %d %H:%M:%S',
                    format='%(asctime)s %(name)s %(levelname)s: %(message)s', 
                    level=logging.DEBUG,
                    tz='UTC')
        #logging.debug('Enabled debug mode, logging set to DEBUG.')

    else:
        logging.basicConfig(
                    datefmt='%b %d %H:%M:%S',
                    format='%(asctime)s %(name)s %(levelname)s: %(message)s', 
                    level=logging.INFO,
                    tz='UTC')



if __name__ == '__main__':

    
    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--configfile',
                        dest='configfile',
                        required=True,
                        help='ratking config file')
    parser.add_argument('-d','--daemon',
                        action='store_true',
                        default=False,
                        dest='daemon',
                        required=False,
                        help='run ratking in daemon mode')
    parser.add_argument('--debug',
                        action='store_true',
                        dest='debug',
                        required=False,
                        help='enable verbose/debug mode. useful for standalone mode')
    parser.add_argument('--restart',
                        action='store_true',
                        default=False,
                        dest='restart',
                        required=False,
                        help='Restart daemon.')
    parser.add_argument('--standalone',
                        action='store_true',
                        default=False,
                        dest='standalone',
                        required=False,
                        help='standalone mode (non-daemon).')
    parser.add_argument('--start',
                        action='store_true',
                        default=False,
                        dest='start',
                        required=False,
                        help='Start in daemon mode.')
    parser.add_argument('--stop',
                        action='store_true',
                        default=False,
                        dest='stop',
                        required=False,
                        help='Stop daemon.')
    args = parser.parse_args()

    if os.environ['USER'] != 'root':
        print "Please run ratkingd as root."
        sys.exit(1)

    rat = Ratkingd('/var/run/ratkingd.pid', args)

    if args.standalone:
        #rat.initialize_env()
        rat.start()

    else:
        if args.start:
            #rat.initialize_env()
            rat.start()

        elif args.stop:
            rat.stop()

        elif args.restart:
            rat.stop()
            #rat.initialize_env()
            rat.start()

        else:
            print "Need --start, --stop, --restart, or --standalone broseph..."
            sys.exit(1)


