import ast
import ConfigParser
import datetime
import exceptions
import glob
import grp
import importlib
import inspect
import multiprocessing
import os
import sys
from drop_privileges import drop_privileges
from pwd import getpwnam 


class JobCtl(object):

    def __init__(self, sched, config, logging):

        self.sched = sched
        self.config = config  
        self.logging = logging


    def add_job(self, filename, username, realuser):
        """adds a job to jobstore if it passes read_jobfile, check_job functions"""

        # can't read a jobfile that doesn't exist
        if not os.path.isfile(filename):
            return False, "Jobfile does not exist."

        if os.path.basename(filename) not in os.listdir(self.config.get('main', 'job_dir')):
            return False, "Job file must be placed under: %s" % self.config.get('main', 'job_dir')

        returncode, output = self.read_jobfile(filename)

        if returncode is False:
            self.logging.error("Adding job failed: %s", output)
            return False, output

        else:
            # output is a dictionary, return from read_jobfile
            jobdict = output.itervalues().next() 

        try:
            jobname = jobdict['__name__']
            owner = jobdict['owner']

            # don't let someone submit a job with a name that already exists
            if self._check_if_job_exists(jobname):
                return False, "Job: %s already exists." % jobname

            returncode, output = self._check_job(jobname, owner, username)

            if returncode is False:
            
                return False, output

        except KeyError as ke:
            return False, "Jobcheck subroutine could not complete, exception: '%s'" % ke 

        except TypeError as te:
            return False, "Improper format for in jobfile: '%s', error: '%s'" % (filename, te)


        try:
            
            # takes string "* * * * *" and turns it into a list 
            schedule = [ f for f in jobdict['schedule'].split() ]

            # converts a string to dict, to be passed to a function as **kwargs
            jobargs = ast.literal_eval(jobdict['kwargs'])

            # silently add the owner to the kwargs. 
            jobargs['owner'] = jobdict['owner']

            # add the job to the jobstore
            self.sched.add_cron_job(
                self.run_job,
                minute=schedule[0],
                hour=schedule[1],
                month=schedule[3],
                day_of_week=schedule[4],
                name=jobdict['__name__'],
                kwargs=jobargs,
                max_instances=1
                )

        except ValueError as ve:
            
            self.logging.error("Error adding job: '%s', Jobfile: '%s', Error: '%s'", 
                    jobdict['__name__'], filename, ve)
            return False, "Error adding job, job file parse error: '%s'" % ve

        # extend the apscheduler.job schema by adding some attributes to a job object
        job = self._get_job_obj(jobname)

        job.jobfile = filename
        job.owner = jobdict['owner']
        job.type = jobdict['type']

        # if the job is disabled by default (enabled=false), set job as disabled
        #   so that during the add_job phase, it's disabled...
        if jobdict['enabled'].lower() != 'true':
            job.status = 'Null' # set the attribute for now. as soon as disable_job() runs, it will be set to 'Disabled'
            self.disable_job(jobname, 'initial_import', 'initial_import')

        else:
            job.status = 'Enabled'

        self.logging.info("Adding job: '%s', Submitted by user: '%s(%s)'", 
                jobdict['__name__'], username, realuser)
        return True, "Successfully added job: '%s'" % jobdict['__name__'] 


    def _check_if_job_exists(self, jobname):
        """returns True/False based on whether or not job exists"""

        joblist = self.sched._jobstores.get('default').jobs

        for job in joblist:

            if jobname == job.name:
                self.logging.debug("check_if_job_exists = True, for job: '%s'" % jobname)
                return True

            else:
                pass

        self.logging.debug("check_if_job_exists = False, for job: '%s'" % jobname)
        return False


    def _check_job(self, jobname, owner, submituser):
        """
        Checks if if submitting user submits a job to be run by someone else. 
        
        Returns False if job submission is invalid for any number of reasons
        """


        # cant submit a job that runs as someone else, nonexistant userid
        try: 
            getpwnam(owner)

        except KeyError as ke:
            
            self.logging.debug("checkjob: '%s', job_owner: '%s' does not exist on system", 
                    jobname, owner)
            return False, "User: '%s' in jobfile does not exist on system" % owner 

        # anyone as root or initial_startup gets a free pass; security hole!
        if submituser == 'root' or submituser == 'initial_startup':
            pass

        else:

            if submituser != owner:
                self.logging.debug("Job: '%s', JobOwner: '%s', JobSubmitter: '%s'" 
                        % ( jobname, owner, submituser))
                return False, "Job is set to run as user: '%s', You cannot add jobs that run as a different user." % owner 

        return True, "Job passed all checks"


    def disable_job(self, jobname, user, realuser):
        """
        This disables a job by setting its rundate.year > 2100, 
        and job.status to 'Disabled'.
        """

        if not self._check_if_job_exists(jobname):
            return False, "Job does not exist."

        job = self._get_job_obj(jobname)

        # sorry, can't disable someone else's job unless you are root
        if job.owner != user and user not in ['root', 'initial_import']:
            self.logging.error("User '%s' tried to disable job: '%s', owned by: '%s'." 
                                % (user, job.name, job.owner) )
            return False, "Cannot disable job: '%s', owned by: '%s'" % (job.name, job.owner)
        
        # no job.status attribute. The only way possible is during job import
        #
        # this was ordered above the job.status line below because of Attribute.errors that 
        #   I couldn't figure out how to catch
        elif not job.status:
            # this shouldn't return anything, since an xmlrpc request to disable
            #   a non existent job should fail long before this
            self.logging.debug("Job: '%s' has no status, must be an import with \
                    'enabled=false'. Disabling job.", jobname)
            
        # can't disable a job that's already in a 'Disabled' state
        elif job.status == 'Disabled':
            return False, "Job: '%s' is already disabled." % job.name

            
        # set the year for the job to run to be > 2100. ghetto disable
        #
        # idea from: http://stackoverflow.com/questions/5871168/how-can-i-subtract-or-add-100-years-to-a-datetime-field-in-the-database-in-djang
        next_run_time = job.next_run_time
        disabled_run_time = datetime.datetime(
                next_run_time.year + 200, *next_run_time.timetuple()[1:-2])
        job.next_run_time = disabled_run_time
        job.status = 'Disabled'

        self.logging.info("Job: '%s' has been disabled by user: '%s'.", 
                job.name, user)
        self.logging.info("Disabled job: '%s', New Schedule: '%s'", 
                job.name, job)

        return True, "Job: '%s' has been disabled." % job.name 

            
    def enable_job(self, jobname, user, realuser):
        """Re-enables a job that was disabled via the rpc client"""

        if not self._check_if_job_exists(jobname):
            return False, "Job does not exist."
        
        job = self._get_job_obj(jobname)
    
        # sorry, can't enable someone else's job unless you are root
        if job.owner != user and user != 'root':
            self.logging.error("User '%s' tried to re-enable job: '%s', owned by: '%s'." 
                                % (user, job.name, job.owner) )
            return False, "Cannot re-enable job: '%s', owned by: '%s'" % (job.name, job.owner)

        elif job.status == 'Enabled':
            return False, "Job: '%s' is already enabled." % job.name

        # job.compute_next_run_time is an internal apscheduler function that 
        #   uses the initial job scedule submission parameters to determine the 
        #   next run time. since we want to re-enable the job, this will 
        #   reschedule the job to run at the next valid time
        new_next_run_time = job.compute_next_run_time(datetime.datetime.now())
        job.next_run_time = new_next_run_time
        job.status = 'Enabled'

        self.logging.info("Job: '%s' has been re-enabled by user: '%s'.", 
                job.name, user)
        self.logging.info("Re-enabled job: '%s', New Schedule: '%s'", job.name, job)
        return True, "Job: '%s' has been re-enabled" % job.name


    def force_run_job(self, jobname, user, realuser):
        """Run a job in the jobstore at this very moment. Does not spawn another thread."""

        if not self._check_if_job_exists(jobname):
            return False, "Job does not exist."

        job = self._get_job_obj(jobname)
        
        # don't let any joe schmoe force run a job they don't own
        if user != job.owner and user != 'root':
            self.logging.error("User '%s', tried to run job: '%s', owned by '%s'", 
                    user, job.name, job.owner)
            return False, "User: '%s', cannot force run job: '%s', owned by '%s'" % (user, job.name, job.owner)

        self.logging.info("User: '%s(%s)', force running job: '%s'", user, realuser, jobname)
        self.run_job(**job.kwargs)

        return True, "Successfully force ran job: '%s'" % job.name


    def _get_job_obj(self, jobname):
        """returns a job object"""

        # check if job checks
        if not self._check_if_job_exists(jobname):
            self.logging.debug("Function: get_job, Job: '%s' does not exist." % jobname)
            return False, "Job: '%s' does not exist." % jobname

        jobs = self.sched.get_jobs()

        for job in jobs:

            if job.name == jobname:
                return job
            else:
                pass

        # this is an internal error. if we make it all the way through the for loop
        #   and don't find the job, it doesn't exist. 
        raise RatkingException("Function: ratking.jobcontrol._get_job_obj() went plaid.")


    def read_jobfile(self, filename):
        """reads a file, parses with ConfigParser, and returns a dictionary of config options"""


        # first off, check and see if the jobfile is in $RATKING_ROOT/etc/jobs.d
        #   We do not want jobfiles spread randomly everywhere

        parser=ConfigParser.SafeConfigParser()
        try:
            parser.read([filename])
        
        except ConfigParser.MissingSectionHeaderError as error:
            return False, "Config file: '%s' is not in correct format" % filename

        except MissingSectionHeaderError as error:
            return False, "Config file: '%s' is not in correct format" % filename

        for section in parser.sections():
            
            for option in parser.options(section):
                
                try:
                    value=parser.getint(section,option)
                except ValueError:
                    value=parser.get(section,option)


        # check the config file, and make sure a number of things exist
        if len(parser.sections()) != 1:
            self.logging.error("Reading jobfile: Cannot have more (or less) than one [section] in job config file.")
            return False, "Cannot have more (or less) than one [section] in job config file."

        jobname = parser.sections()[0]  # we already ensured that this array is length 1

        # make sure the jobname matches the filename (minus the .conf)
        if jobname != filename.split(os.path.sep)[-1].split('.')[0]:
            self.logging.error("Filename: %s (minus .conf) must match header name: [%s]", filename, jobname)
            return False, "Filename: %s (minus .conf) must match header name: [%s]" % (filename, jobname)

        required_options = ['type', 'schedule', 'owner', 'plugin_name', 'kwargs', 'enabled', 'autostart']

        # make sure if the job is a plugin job, the plugin is in $RATKING_ROOT/var/lib/ratkingd/plugins.d

        for req in required_options:
            
            if not req in parser._sections[jobname]:

                self.logging.error("Missing required job config options: '%s'" % req)
                return False, "Missing one or more attributes '[%s]' in job config file." % req

        # get the calling function, so that if its import_jobs, we don't load if autostart=false
        calling_function = inspect.stack()[2][3]

        if calling_function == 'import_jobs' and parser._sections[jobname]['autostart'].lower() == 'false':
            self.logging.info("During import: job will not be added, autostart=false in config file.")
            return False, 'During import: job will not be added, autostart=false in config file.'

        # this checks to make sure that the kwargs= section in the jobfile can be properly converted to
        #   a dictionary later on in the process
        try:
            ast.literal_eval(parser._sections[jobname]['kwargs'])

        except ValueError as ve:

            self.logging.debug("Error importing jobfile: '%s'. kwargs must have apostrophies around all key value " +
                                "pairs: { 'keyname' : 'value' }, or be True|False: { 'mail' : True }")
            return False, 'Job import failed, kwargs key/value parsing error'
        
        return True, parser._sections

    
    def remove_job(self, jobname, user, realuser):
        """Removes a job from the schedule completely"""

        if not self._check_if_job_exists(jobname):
            return False, "Job does not exist."

        job = self._get_job_obj(jobname)

        # first check if the user removing the job owns that job
        if user != 'root' and user != job.owner:

            self.logging.error("Job: '%s', cannot be removed by user: '%s(%s)'", job.name, user, realuser)
            return False, 'Cannot remove a job you do not own.'

        try:
            self.sched.unschedule_job(job)
            self.logging.info("Job: '%s', removed by user: '%s(%s)'", job.name, user, realuser)
            return True, "Successfully removed job: '%s'" % job.name

        except KeyError as ke:
            return False, "Removing job: '%s' failed, Error: '%s'" % (job.name, ke)      


    def run_job(self, **kwargs):
        """sets up multiprocessor to run the actual exec function """

        # test mode enabled. No subprocesses will spawn, and job won't execute it's function
        if self.config.get('main', 'test_mode') == '1':
            self.logging.info("Test mode enabled, job '%s' finishing.", kwargs['job_name'])
            return True

        # use multiprocess to fork a new process for each job being run
        p = multiprocessing.Process(target=self._run_job_exec, kwargs=kwargs)
        p.daemon = True
        p.start()

        # this will wait for the process to finish. since apscheduler runs jobs via a separate thread,
        #   only the job running thread will be blocking, so the rest of the main daemon will be fine
        p.join()    
    
        return True


    def _run_job_exec(self, **kwargs):
        """Loads appropriate module, changes uid/gid to owner/group, and runs the job """
       
        plugin_dir = self.config.get('main', 'plugin_dir')
        plugin_path = "%s%s%s" % (plugin_dir, os.path.sep, kwargs['plugin_name'])
        sys.path.append(plugin_dir)

        # this loads the plugin
        try:

            # first, check if the plugin even exists
            if os.path.isfile(plugin_path):
                pass
            else:
                #self.logging.error("Job (%s) run error, plugin does not exist: '%s'" 
                #        %  (kwargs['job_name'], plugin_path) )
                raise RatkingException("Job (%s) run error, plugin does not exist: '%s'" 
                    %  (kwargs['job_name'], plugin_path) )
            

            if kwargs['plugin_name'] not in sys.modules.keys():
                lib = importlib.import_module(kwargs['plugin_name'])

            else:
                #lib = importlib.import_module(kwargs['plugin_name'])
                # we reload so that each time the script is run, any updates to the plugin
                #   will be in effect. usecases would be if you disable/re-enable a job due to error.
                #   Python won't 'unload' the module when you disable it. remove_job and then add_job
                #   probably won't do any garbage collection either.
                reload(kwargs['plugin_name'])

        except ImportError as ie:
            
            self.logging.error("Module import error for job: '%s'" % kwargs['job_name'])    
            raise RatkingException("Module import error for job: '%s, error: %s'" \
                                % (kwargs['job_name'], ie) )

        try:     

            # get uid/gid of job owner:
            uid = getpwnam(kwargs['owner'])[2]

            # get primary group gid of user
            gid = getpwnam(kwargs['owner'])[3]  
            group_name = grp.getgrgid(gid)[0]

            self.logging.debug("Running job: '%s' as user: '%s', group: '%s'", 
                            kwargs['job_name'], kwargs['owner'], group_name)
            
            # use drop_privileges module to set the uid/gid of the subprocess
            drop_privileges(uid_name=kwargs['owner'], gid_name=group_name)

            # ALL plugin's main() function should accept **kwargs:
            #   EX: def main(**kwargs):
            #
            # run the actual module
            lib.main(**kwargs)

            return True
    
        except OSError as oe:

            self.logging.error("Something went wrong here: '%s'" % oe )
            raise RatkingException("Job '%s' did not execute successfully, error: '%s'" % (kwargs['job_name'], oe) )

        return True

 
    def show_jobs(self):
        """Returns a list object of all active jobs"""

        output = []
        jobs = self.sched.get_jobs()

        output.append('{0: <25} {1: <15} {2: <15} {3: <10} {4}' \
                .format('Jobname', 'Jobowner', 'JobType', 'Status', 'Next Run Time'))
        output.append('='*110)

        for job in jobs:

            status = job.status
            next_run_time = job.next_run_time

            # dirty hack to display disabled jobs in a meaningful way
            if job.next_run_time.year > 2100 or job.status == 'Disabled':
                next_run_time = 'Never'
                status = 'Disabled'

            line = '{0: <25} {1: <15} {2: <15} {3: <10} {4}' \
                .format(job.name, job.owner, job.type, status, next_run_time)
            output.append(line)



        return output


class RatkingException(Exception):

    def __init__(self, message):

        self.message = message

    def __str__(self):
       
        return repr(self.message)
