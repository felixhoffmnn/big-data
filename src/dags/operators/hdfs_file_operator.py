import ast
from os import path
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.hdfs_hook import HdfsHook


class HdfsMkdirFileOperator(BaseOperator):

    template_fields = ("directory", "hdfs_conn_id")
    ui_color = "#fcdb03"

    @apply_defaults
    def __init__(self, directory, hdfs_conn_id, *args, **kwargs):
        """
        :param directory: directory, which should be created
        :type directory: string
        :param hdfs_conn_id: airflow connection id of HDFS connection to be used
        :type hdfs_conn_id: string
        """

        super(HdfsMkdirFileOperator, self).__init__(*args, **kwargs)
        self.directory = directory
        self.hdfs_conn_id = hdfs_conn_id

    def execute(self, context):

        self.log.info("HdfsMkdirFileOperator execution started.")

        self.log.info("Mkdir HDFS directory'" + self.directory + "'.")

        hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)
        hh.mkdir(self.directory)

        self.log.info("HdfsMkdirFileOperator done.")


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
    def __init__(
        self, local_path, remote_path, file_names, hdfs_conn_id, *args, **kwargs
    ):
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
            # local_file = path.join(self.local_path, file[0], "-hubway-tripdata.csv")
            # remote_file = path.join(self.remote_path, file[1], file[1], "-hubway-tripdata.csv")
            local_file = file[0]
            remote_file = file[1]

            self.log.info(
                str(index)
                + ": Upload file '"
                + local_file
                + "' to HDFS '"
                + remote_file
                + "'."
            )

            hh.putFile(local_file, remote_file)

        self.log.info("HdfsPutFileOperator done.")


class HdfsGetFileOperator(BaseOperator):

    template_fields = ("remote_file", "local_file", "hdfs_conn_id")
    ui_color = "#fcdb03"

    @apply_defaults
    def __init__(self, remote_file, local_file, hdfs_conn_id, *args, **kwargs):
        """
        :param remote_file: file to download from HDFS
        :type remote_file: string
        :param local_file: where to store HDFS file locally
        :type local_file: string
        :param hdfs_conn_id: airflow connection id of HDFS connection to be used
        :type hdfs_conn_id: string
        """

        super(HdfsGetFileOperator, self).__init__(*args, **kwargs)
        self.remote_file = remote_file
        self.local_file = local_file
        self.hdfs_conn_id = hdfs_conn_id

    def execute(self, context):

        self.log.info("HdfsGetFileOperator execution started.")

        self.log.info(
            "Download file '"
            + self.remote_file
            + "' to local filesystem '"
            + self.local_file
            + "'."
        )

        hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)
        hh.getFile(self.remote_file, self.local_file)

        self.log.info("HdfsGetFileOperator done.")
