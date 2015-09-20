"""
## CODE OWNERS: Shea Parkes

### OBJECTIVE:
  Provide tooling to spin up a local Spark cluster.

### DEVELOPER NOTES:
  Will vomit all over your system environment variables.
  Produces a much more realistic (and capable) cluster than master=local[n]
    - `master=local[n]` clusters do all processing in the puny driver executor
        (and can hide parallelization glitches)
"""

import os
import sys
import tempfile
import subprocess
import time
import socket
import itertools
from pathlib import Path

import indyspark.setup.environment


# Spark is *very* particular about the IP used to access the master
#  - it *must* match what is displayed in the master WebUI when connecting workers
LOCAL_HOSTNAME = socket.gethostname()
LOCAL_IP = socket.gethostbyname(LOCAL_HOSTNAME)

# Directly munging object attributes below, so pylint cannot follow them
# pylint: disable=no-member

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================


class SparkCluster(object):
    """Wrapper to control setting up, launching, and tearing down a local Spark cluster"""

    _default_attributes = {
        'path_spark': indyspark.setup.environment.PATH_SPARK,
        'path_hadoop': indyspark.setup.environment.PATH_HADOOP_FAKE,
        'local_ip': LOCAL_IP,

        # Worker related parameters
        'n_workers': 2,
        'spark_worker_memory': '2G',
        'spark_worker_cores': '1',
        }

    def __init__(self, **kwargs):
        """Initialize attributes, but trigger no side-effects"""

        # Use **kwargs magic to place all parameters into attributes
        _gathered_attributes = SparkCluster._default_attributes.copy() # Copy in the defaults
        _gathered_attributes.update(kwargs) # Replace defaults with any input parameters
        self.__dict__.update(_gathered_attributes) # Place final attributes into their home

        # Setup a bucket to direct all filesystem artifact
        self.path_spark_local_dirs = Path(tempfile.mkdtemp(prefix='spark_local_dir'))

        # Redirect config file searching out of (shared) Spark_Home
        self.path_spark_conf_dir = self.path_spark_local_dirs / 'spark_conf_dir'

        self.subprocess_master = None
        self.url_master = 'spark://{}:7077'.format(self.local_ip)
        self._next_worker_webui_port = itertools.count(8081)
        self.workers = []

    def _mangle_environment(self):
        """Actually mangle environment to prepare for cluster"""

        # This function also makes pyspark importable
        #   - Not mandatory at this point, but nice to then use the cluster
        indyspark.setup.environment.setup_spark_env(
            # Re-pass parameters since the (shared) defaults may have been overridden
            path_spark=self.path_spark,
            path_hadoop=self.path_hadoop,
            )

        os.environ['SPARK_LOCAL_DIRS'] = str(self.path_spark_local_dirs)

        self.path_spark_conf_dir.mkdir()
        os.environ['SPARK_CONF_DIR'] = str(self.path_spark_conf_dir)

    def start_cluster(self):
        """Start the full cluster"""
        self._mangle_environment()

        self._start_master()
        time.sleep(8.4)

        for _ in range(self.n_workers):
            self.workers.append(SparkWorker(self))

        for worker in self.workers:
            worker.start_worker()
            time.sleep(2.1)

    def stop_cluster(self):
        """Stop the full cluster"""
        for worker in self.workers:
            worker.stop_worker()
        self._stop_master()

    def _start_master(self):
        """Start the master node"""
        assert self.subprocess_master is None, 'Master has already been started'

        with (self.path_spark_local_dirs / 'master_stdout_stderr.txt').open('w') as fh_log:
            self.subprocess_master = subprocess.Popen(
                [
                    str(self.path_spark / 'bin' / 'spark-class.cmd'),
                    'org.apache.spark.deploy.master.Master',
                    ],
                stdout=fh_log,
                stderr=subprocess.STDOUT,
                )

    def _stop_master(self):
        """Stop the master node"""
        assert self.subprocess_master is not None, 'Master has not been started'
        assert self.subprocess_master.returncode is None, 'Master has already stopped'

        self.subprocess_master.kill()

    @property
    def next_worker_webui_port(self):
        """Return the next Worker WebUI Port (and increment the count)"""
        return next(self._next_worker_webui_port)


class SparkWorker(object):
    """Wrapper to control setup/teardown of single worker"""

    def __init__(self, master):
        """Initialize attributes, but trigger no side-effects"""
        self.master = master

        self.worker_webui_port = self.master.next_worker_webui_port
        self.path_spark_worker_dir = self.master.path_spark_local_dirs /\
            'worker-{}'.format(self.worker_webui_port)

        self.subprocess = None

    def _mangle_environment(self):
        """Actually mangle environment to prepare for worker"""
        self.path_spark_worker_dir.mkdir(parents=True)

        os.environ['SPARK_WORKER_DIR'] = str(self.path_spark_worker_dir)
        os.environ['WORKER_WEBUI_PORT'] = str(self.worker_webui_port)
        os.environ['SPARK_WORKER_MEMORY'] = str(self.master.spark_worker_memory)
        os.environ['SPARK_WORKER_CORES'] = str(self.master.spark_worker_cores)

    def start_worker(self):
        """Start this worker node"""
        assert self.subprocess is None, 'Worker has already been started'

        self._mangle_environment()

        with (self.path_spark_worker_dir / 'stdout_stderr.txt').open('w') as fh_log:
            self.subprocess = subprocess.Popen(
                [
                    str(self.master.path_spark / 'bin' / 'spark-class.cmd'),
                    'org.apache.spark.deploy.worker.Worker',
                    self.master.url_master,
                    ],
                stdout=fh_log,
                stderr=subprocess.STDOUT,
                )

    def stop_worker(self):
        """Stop this worker node"""
        assert self.subprocess is not None, 'Worker has not been started'
        assert self.subprocess.returncode is None, 'Worker has already stopped'

        self.subprocess.terminate()


if __name__ == '__main__':

    superman = SparkCluster(n_workers=3)
    print(superman.path_spark_local_dirs)
    superman.start_cluster()

    import pyspark
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SQLContext
    import pyspark.sql.types as types

    conf = SparkConf().setAppName('playground').setMaster(superman.url_master)
    conf = conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    print('Spark Cluster, Context, and SQL Context successfuly created.')
    superman.stop_cluster()
    sys.exit(0)
