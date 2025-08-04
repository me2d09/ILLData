import pysftp
from urllib.parse import urlparse
import os


class IllSftp:

    def has_proposal(func):
        def inner(self, *args, **kwargs):
            if self._proposal == "":
                raise Exception("There is no opened proposal, use openproposal method.")
            return func(self, *args, **kwargs)
        return inner


    def __init__(self, hostname, username, password, port=22):
        """Constructor Method"""
        # Set connection object to None (initial value)
        self.connection = None
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self._proposal = ""

    def connect(self):
        """Connects to the sftp server and returns the sftp connection object"""

        try:
            # Get the sftp connection object
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None   
            self.connection = pysftp.Connection(
                host=self.hostname,
                username=self.username,
                password=self.password,
                port=self.port,
                cnopts = cnopts
            )
        except Exception as err:
            raise Exception(err)
        finally:
            self._home = self.connection.readlink('MyData')
            print(f"Connected to {self.hostname} as {self.username}, home directory is {self._home}.")


    def disconnect(self):
        """Closes the sftp connection"""
        self.connection.close()
        print(f"Disconnected from host {self.hostname}")

    @has_proposal
    def listdir(self, remote_path, attr = False):
        """lists all the files and directories in the specified path and returns them"""
        if attr:
            fc = self.connection.listdir_attr
        else:
            fc = self.connection.listdir
        return fc(self._propdir + "/" + remote_path)

    def proposals(self):
        """lists all the files and directories in the specified path and returns them"""
        remote_path = self._home + "/byProposal"
        for obj in self.connection.listdir(remote_path):
            yield obj[4:] if obj[:4] == "exp_" else obj
    
    @property
    def proposal(self):
        return self._proposal

    def openproposal(self, value):
        self._proposal = value
        self.connection.chdir(self._home + "/byProposal")
        self._propdir = self.connection.readlink("exp_" + value)    

    def listdir_attr(self, remote_path):
        """lists all the files and directories (with their attributes) in the specified path and returns them"""
        return self.listdir(remote_path, attr = True)

    @has_proposal
    def download(self, remote_path, target_local_path):
        """
        Downloads the file from remote sftp server to local.
        Also, by default extracts the file to the specified target_local_path
        """

        try:
            print(
                f"downloading from {self.hostname} as {self.username} [(remote path : {remote_path});(local path: {target_local_path})]"
            )

            # Create the target directory if it does not exist
            path, _ = os.path.split(target_local_path)
            if not os.path.isdir(path):
                try:
                    os.makedirs(path)
                except Exception as err:
                    raise Exception(err)

            # Download from remote sftp server to local
            self.connection.get(self._propdir + "/" + remote_path, target_local_path)
            print("download completed")

        except Exception as err:
            raise Exception(err)

    @has_proposal
    def upload(self, source_local_path, remote_path):
        """
        Uploads the source files from local to the sftp server.
        """

        try:
            print(
                f"uploading to {self.hostname} as {self.username} [(remote path: {remote_path});(source local path: {source_local_path})]"
            )

            # Download file from SFTP
            self.connection.put(source_local_path, self._propdir + "/" + remote_path)
            print("upload completed")

        except Exception as err:
            raise Exception(err)


