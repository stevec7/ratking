import ast
import ConfigParser
import glob
import grp
import importlib
import multiprocessing
import os
import sys
from drop_privileges import drop_privileges
from jobhandler import JobCtl
from pwd import getpwnam 

class SchedCtl(object):

    def __init__(self, sched, config, logging):

        self.sched = sched
        self.config = config
        self.logging = logging

        # create an object to link to the job control class. only really used by this class to import
        #   jobs in the $RATKINGROOT/etc/jobs.d directory
        self.job_control_instance = JobCtl(self.sched, self.config, self.logging)

    def check_sched(self):
        """Checks to see if scheduler is running"""

        if self.sched.running is True:
            return True, "Scheduler is running."

        else:
            return False, "Scheduler is stopped."

    def import_jobs(self):
        """read jobs from persistent directory, specified in the config file, under option job_dir"""

        for infile in glob.glob( os.path.join(self.config.get('main', 'job_dir'), '*.conf') ):
            self.logging.info("Trying to import jobfile: %s", infile)
            
            try:
                self.job_control_instance.add_job(infile, 'initial_startup', 'initial_startup')
    
            except RatkingException as error:
                print "RatkingException: Error adding job, jobfile: %s. " % infile
                pass

            except ConfigParser.ParsingError as error:
                self.logging.error("ConfigParser.ParsingError: %s. ", error)
                pass

    def initialize(self):
        """Starts the scheduler for the first time. Only to be used in ratkingd daemon"""
        
        self.sched.start()

        return True


    def start_sched(self, user):
        """Start the AP Scheduler. Return 'True' if success."""

        if user != 'root':
            return False, "Only root can stop/start scheduling."

        if self.sched.running is True:
            return False, "Scheduler already running."

        else:

            try:
                self.sched.start()

            except exceptions.AttributeError as e:
                raise RatkingException("Error starting scheduling: %s" % e)

            return True, "Scheduler started."    
        

    def stop_sched(self, user):
        """Stop the AP Scheduler. Return 'True' if success."""

        if user != 'root':
            return False, "Only root can stop/start scheduling."

        if self.sched.running is False:
            return False, "Scheduler is not running."

        else:

            try:
                self.sched.shutdown()


            except exceptions.AttributeError as e:
                raise RatkingException("Error stopping scheduling: %s" % e)

        self.sched.shutdown()
        return True, "Ratkingd job scheduling has been stopped."


class RatkingException(Exception):

    def __init__(self, message):

        self.message = message

    def __str__(self):
       
        return repr(self.message)

