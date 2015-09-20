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
from pathlib import Path


LOCAL_HOSTNAME = socket.gethostname()
LOCAL_IP = socket.gethostbyname(LOCAL_HOSTNAME)

PATH_SPARK = Path(r'S:\ZQL\Software\Hotware\spark-1.5.0-bin-hadoop2.6')
PATH_HADOOP_FAKE = Path(r'S:\ZQL\Software\Hotware\fake-hadoop-for-spark')

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================


class SparkCluster(object):
    """Wrapper to control setting up, launching, and tearing down a local Spark cluster"""

    def __init__(self, path_spark=PATH_SPARK, path_hadoop=PATH_HADOOP_FAKE, local_ip=LOCAL_IP):
        """Initialize attributes, but trigger no side-effects"""

        # Absolute basic Spark_Home
        self.path_spark = path_spark

        # Point Spark to a mostly fake Hadoop install to supress a couple warnings
        self.path_hadoop = path_hadoop

        self.local_ip = local_ip

        # Setup a bucket to direct all filesystem artifact
        self.path_spark_local_dirs = Path(tempfile.mkdtemp(prefix='spark_local_dir'))

        # Redirect config file searching out of (shared) Spark_Home
        self.path_spark_conf_dir = self.path_spark_local_dirs / 'spark_conf_dir'

        self.subprocess_master = None
        self.url_master = 'spark://{}:7077'.format(self.local_ip)
        self.next_worker_webui_port = 8081
        self.workers = []

    def _mangle_environment(self):
        """Actually mangle environment to prepare for cluster"""

        os.environ['SPARK_HOME'] = str(self.path_spark)
        os.environ['HADOOP_HOME'] = str(self.path_hadoop)
        os.environ['SPARK_LOCAL_DIRS'] = str(self.path_spark_local_dirs)

        self.path_spark_conf_dir.mkdir()
        os.environ['SPARK_CONF_DIR'] = str(self.path_spark_conf_dir)

        # Make PySpark importable into this python instance
        #   - not necessarily the most logical place for this
        #   - this is not needed to launch a Spark cluster via subprocess
        #   - but it is nice to be able to then connect to it
        sys.path.append(str(self.path_spark / 'python'))
        for path_py4j in (self.path_spark / 'python' / 'lib').glob('py4j*.zip'):
            sys.path.append(str(path_py4j))

        # Inform Spark of current Python environment
        os.environ['PYSPARK_PYTHON'] = sys.executable

    def start_cluster(self, n_workers=4):
        """Start the full local cluster"""

        self._mangle_environment()

        self._start_master()
        time.sleep(8.4)

        for _ in range(n_workers):
            self.workers.append(SparkWorker(
                self.path_spark,
                self.next_worker_webui_port,
                self.path_spark_local_dirs,
                self.url_master,
                ))
            self.next_worker_webui_port += 1

        for worker in self.workers:
            worker.start_worker()
            time.sleep(2.1)

    def stop_cluster(self):
        """Stop the full cluster"""
        for worker in self.workers:
            worker.stop_worker()
        self._start_master()

    def _start_master(self):
        """Start the master node"""
        assert self.subprocess_master is None, 'Master has already been launched'

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
        self.subprocess_master.kill()



class SparkWorker(object):
    """Wrapper to control setup/teardown of single worker"""

    def __init__(self, path_spark, worker_webui_port, path_spark_worker_dir_root, url_master):
        """Setup everything except actually launching (delay environment munging)"""
        self.path_spark = path_spark
        self.worker_webui_port = worker_webui_port
        self.path_spark_worker_dir_root = path_spark_worker_dir_root
        self.path_spark_worker_dir = self.path_spark_worker_dir_root /\
            'worker-{}'.format(self.worker_webui_port)

        self.path_spark_worker_dir.mkdir(parents=True)

        self.url_master = url_master

        self.subprocess = None


    def start_worker(self):
        """Actually launch the worker in a subprocess"""

        assert self.subprocess is None, 'Worker has already been launched'

        os.environ['SPARK_WORKER_DIR'] = str(self.path_spark_worker_dir)
        os.environ['WORKER_WEBUI_PORT'] = str(self.worker_webui_port)
        os.environ['SPARK_WORKER_MEMORY'] = '2G'
        os.environ['SPARK_WORKER_CORES'] = '1'

        with (self.path_spark_worker_dir / 'stdout_stderr.txt').open('w') as fh_log:
            self.subprocess = subprocess.Popen(
                [
                    str(self.path_spark / 'bin' / 'spark-class.cmd'),
                    'org.apache.spark.deploy.worker.Worker',
                    self.url_master,
                    ],
                stdout=fh_log,
                stderr=subprocess.STDOUT,
                )

    def stop_worker(self):
        """Stop the worker"""
        assert self.subprocess is not None, 'Worker has not been launched'
        assert self.subprocess.returncode is None, 'Worker has already stopped'

        self.subprocess.terminate()


if __name__ == '__main__':

    superman = SparkCluster()
    print(superman.path_spark_local_dirs)
    superman.start_cluster(2)

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
