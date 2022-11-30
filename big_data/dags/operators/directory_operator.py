import glob
import os

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateDirectoryOperator(BaseOperator):

    template_fields = ("path", "directory")
    ui_color = "#427bf5"

    @apply_defaults
    def __init__(self, path, directory, *args, **kwargs):
        """
        :param path: path in which to create directory
        :type path: string
        :param directory: name of directory to create
        :type directory: string
        """

        super(CreateDirectoryOperator, self).__init__(*args, **kwargs)
        self.path = path
        self.directory = directory

    def execute(self, context):

        self.log.info("CreateDirectoryOperator execution started.")

        if not os.path.exists(self.path + "/" + self.directory):
            if os.path.isdir(self.path):
                try:
                    os.mkdir(self.path + "/" + self.directory)
                except OSError:
                    raise AirflowException(
                        "Creation of directory '"
                        + self.path
                        + "/"
                        + self.directory
                        + "' failed."
                    )
                else:
                    self.log.info(
                        "Successfully created directory '"
                        + self.path
                        + "/"
                        + self.directory
                        + "'."
                    )
            else:
                raise AirflowException("Path '" + self.path + "' is not a directory.")
        else:
            self.log.info(
                "Directory '" + self.path + "/" + self.directory + "' already exists."
            )

        self.log.info("CreateDirectoryOperator done.")


class ClearDirectoryOperator(BaseOperator):

    template_fields = ("directory", "pattern")
    ui_color = "#427bf5"

    @apply_defaults
    def __init__(self, directory, pattern, *args, **kwargs):
        """
        :param directory: name of directory to clear files and directories within
        :type directory: string
        """

        super(ClearDirectoryOperator, self).__init__(*args, **kwargs)
        self.directory = directory
        self.pattern = pattern

    def execute(self, context):

        self.log.info("ClearDirectoryOperator execution started.")

        if os.path.exists(self.directory):
            if os.path.isdir(self.directory):
                if self.pattern == "":
                    raise AirflowException("Failure, file pattern is empty.")
                fileList = glob.glob(
                    self.directory + "/" + self.pattern, recursive=True
                )
                for filePath in fileList:
                    try:
                        os.remove(filePath)
                        self.log.info("Deleted file '" + filePath + "'.")
                    except OSError:
                        raise AirflowException(
                            "Failure, couldn't delete file '" + filePath + "'."
                        )
                if len(fileList) == 0:
                    self.log.info(
                        "No files to delete matching pattern '"
                        + self.pattern
                        + "' found in directory '"
                        + self.directory
                        + "'."
                    )
            else:
                raise AirflowException(
                    "Directory '" + self.directory + "' is not a directory."
                )
        else:
            raise AirflowException("Directory '" + self.directory + "' does not exist.")

        self.log.info("ClearDirectoryOperator done.")
