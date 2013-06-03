#!/usr/bin/env python
import exceptions
import sys
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
from ratking.engine import JobCtl, SchedCtl


class RpcCtl:

    def __init__(self, sched, config, logging):
        """Initializes the RpcCtl class. Should only be called from ratkingd daemon."""

        self.sched = sched
        self.config = config
        self.logging = logging

        # creates a mapping to the scheduling and job control object references from main daemon startup
        self.rpcreq = JobCtl(self.sched, self.config, self.logging)
        self.schedreq = SchedCtl(self.sched, self.config, self.logging)


    # statically define all of the JobCtl methods we want access to, versus allowing access to all functions
    #
    # these all interact with the JobCtl/Schedctl objects
    def add_job(self, jobfile, user, realuser):
        return self.rpcreq.add_job(jobfile, user, realuser)

    def check_auth(self, username):
        """Checks if user is allowed to issue xmlrpc queries."""

        if username not in self.config.get('main', 'valid_users').split(','):
            self.logging.error("XMLRPC security: User: '%s' is not allowed access to the ratking.", username)
            return False, "Dear %s, PERMISSION DENIED: (http://download.garyshood.com/root/trautman.jpg)." % username

        else:
            return True, "User: %s is allowed." % username        

    def check_sched(self):
        return self.schedreq.check_sched()

    def disable_job(self, jobname, user, realuser):
        return self.rpcreq.disable_job(jobname, user, realuser)

    def enable_job(self, jobname, user, realuser):
        return self.rpcreq.enable_job(jobname, user, realuser)

    def force_run_job(self, jobname, user, realuser):
        return self.rpcreq.force_run_job(jobname, user, realuser)

    def remove_job(self, jobname, user, realuser):
        return self.rpcreq.remove_job(jobname, user, realuser)

    def show_jobs(self):
        return self.rpcreq.show_jobs()

    def start_sched(self, user):
        return self.schedreq.start_sched(user)

    def stop_sched(self, user):
        return self.schedreq.stop_sched(user)


    def start_instance(self):
        """Starts an XMLRPC server, and registers its own functions"""

        try:
            self.server = SimpleXMLRPCServer((self.config.get('xmlrpc', 'host'), int(self.config.get('xmlrpc', 'port'))))
            self.server.allow_none=False
            self.server.logRequests=False
            self.server.RequestHandlerClass.rpc_paths = self.config.get('xmlrpc', 'url')
            self.server.register_introspection_functions()
            self.server.register_instance(self)
            self.server.serve_forever()

            return True, "Successfully started instance."
        
        except Exception as e:
            return False, "Error starting instance: %s" % e

        


    def stop_instance(self):

        # stop xmlrpc server instance
        pass
