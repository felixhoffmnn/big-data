import gzip
import os
import shutil

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class UnzipFileOperator(BaseOperator):

    template_fields = ("zip_file", "extract_to")
    ui_color = "#b05b27"

    @apply_defaults
    def __init__(self, zip_file, extract_to, *args, **kwargs):
        """
        :param zip_file: file to unzip (including path to file)
        :type zip_file: string
        :param extract_to: where to extract zip file to
        :type extract_to: string
        """

        super(UnzipFileOperator, self).__init__(*args, **kwargs)
        self.zip_file = zip_file
        self.extract_to = extract_to

    def execute(self, context):

        self.log.info("UnzipFileOperator execution started.")

        self.log.info("Unzipping '" + self.zip_file + "' to '" + self.extract_to + "'.")

        with gzip.open(self.zip_file, "r") as f_in, open(
            self.extract_to, "wb"
        ) as f_out:
            shutil.copyfileobj(f_in, f_out)

        self.log.info("UnzipFileOperator done.")


class CopyFileOperator(BaseOperator):

    template_fields = ("source", "dest", "overwrite")
    ui_color = "#427bf5"

    @apply_defaults
    def __init__(self, source, dest, overwrite, *args, **kwargs):
        """
        :param source: source file
        :type source: string
        :param dest: destination file
        :type dest: string
        :param overwrite: overwrite file
        :type: overwrite: boolean
        """

        super(CopyFileOperator, self).__init__(*args, **kwargs)
        self.source = source
        self.dest = dest
        self.overwrite = overwrite

    def execute(self, context):

        self.log.info("CopyFileOperator execution started.")
        self.log.info("Overwrite: " + str(self.overwrite))

        if os.path.exists(self.dest) and self.overwrite == False:
            raise AirflowException("File '" + self.dest + "' already exists.")

        self.log.info("Copying file '" + self.source + "' to '" + self.dest + "'")

        dest = shutil.copyfile(self.source, self.dest)

        self.log.info("CopyFileOperator done.")
