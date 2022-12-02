import ast
from os import path

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.hdfs_hook import HdfsHook


class HdfsMkdirsFileOperator(BaseOperator):

    template_fields = ("directory", "hdfs_conn_id", "file_names")
    ui_color = "#fcdb03"

    @apply_defaults
    def __init__(self, directory, file_names, hdfs_conn_id, *args, **kwargs):
        """
        :param directory: directory, which should be created
        :type directory: string
        :param hdfs_conn_id: airflow connection id of HDFS connection to be used
        :type hdfs_conn_id: string
        """

        super(HdfsMkdirsFileOperator, self).__init__(*args, **kwargs)
        self.directory = directory
        self.file_names = file_names
        self.hdfs_conn_id = hdfs_conn_id

    def execute(self, context):

        self.log.info("HdfsMkdirsFileOperator execution started.")

        file_names = ast.literal_eval(self.file_names[0])
        hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)

        for file in file_names:
            mkdir_path = path.join(self.directory, file)
            self.log.info("Mkdir HDFS directory'" + mkdir_path + "'.")
            hh.mkdir(mkdir_path)

        self.log.info("HdfsMkdirFileOperator done.")


class HdfsPutFilesOperator(BaseOperator):

    template_fields = ("local_path", "remote_path", "file_names", "hdfs_conn_id")
    ui_color = "#fcdb03"

    @apply_defaults
    def __init__(self, local_path, remote_path, file_names, hdfs_conn_id, *args, **kwargs):
        """
        :param local_file: which file to upload to HDFS
        :type local_file: string
        :param remote_file: where on HDFS upload file to
        :type remote_file: string
        :param hdfs_conn_id: airflow connection id of HDFS connection to be used
        :type hdfs_conn_id: string
        """

        super(HdfsPutFilesOperator, self).__init__(*args, **kwargs)
        self.files = []
        self.local_path = local_path
        self.remote_path = remote_path
        self.file_names = file_names
        self.hdfs_conn_id = hdfs_conn_id

    def execute(self, context):

        self.log.info("HdfsPutFileOperator execution started.")

        file_names = ast.literal_eval(self.file_names[0])
        self.files = [
            [
                path.join(self.local_path, "{}-hubway-tripdata.csv".format(file)),
                path.join(self.remote_path, file, "{}-hubway-tripdata.csv".format(file)),
            ]
            for file in file_names
        ]
        hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)

        for index, file in enumerate(self.files):
            local_file = file[0]
            remote_file = file[1]

            self.log.info(str(index) + ": Upload file '" + local_file + "' to HDFS '" + remote_file + "'.")

            hh.putFile(local_file, remote_file)

        self.log.info("HdfsPutFileOperator done.")
