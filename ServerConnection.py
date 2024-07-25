#!/usr/bin/env python3.11
"""
To store functions related to the ias server connection.
"""

# IMPORTS
import os
import time
import shutil
import tempfile
import subprocess
import multiprocessing as mp



# PUBLIC API
__all__ = ['SSHMirroredFilesystem']



class MirroredFilesystemHelper:
    """
    To help manage the filesystem for the SSHMirroredFilesystem class. It is therefore a private class.
    """

    def __init__(self):

        # CONTEXT CHECK
        self.is_multiprocessing = (os.getpid() != os.getppid())
        self.is_threading = threading.current_thread().name != "MainThread"

        # ATTRIBUTE SETUP
        self.multi_lock = mp.Lock() if self.is_multiprocessing else None
        self.thread_lock = threading.Lock() if self.is_threading else None

        



#TODO: important to later update this class. Problem being that if the class is used twice while only one cleanup is needed at a given time, then it cleanup everything by default...
class SSHMirroredFilesystem:
    """
    To create a temporary filesystem that partially mirrors the server archive for the desired files. For Windows OS users, you need to have WSL installed for these
    methods to work.

     - The remote directory creation is set at class level, i.e. it is created as this python file is imported.
     - The deletion of the filesystem has to be set manually using SSHMirroredFilesystem.cleanup().

     - If a single server connection is needed (i.e. cases where only one file or a single list of files need to be fetched), then using the staticmethod 
        .remote_to_local() is best.
     - If the file fetching cannot be done at once, you can open an ssh connection to the server by creating an instance of the class. You can then use .mirror()
        to mirror some files to the temporary filesystem and .close() to close the open ssh connection. Again, .cleanup() is used to remove the temporary filesystem.
    """

    # OS
    os_name = os.name  # to see if the user is on Windows. If so WSL is used for the bash commands

    # DIRECTORIES
    directory_path = tempfile.mkdtemp()  # the path to the main directory for the temporary filesystem
    directory_list: list[str] = []  # list of the subdirectory names that were created. To be able to decide what to cleanup
    # Locks for the list 
    mp_lock = mp.Lock()

    def __init__(self, host_shortcut: str = 'sol', compression: str = 'z', connection_timeout: int | float = 20, verbose: int = 0, flush: bool = False) -> None:
        """
        It opens an ssh connection to the server by saving a control socket file in the OS specific temporary directory.
        WSL needs to be set up if the user is on Windows OS.

        Args:
            host_shortcut (str, optional): the shortcut for the host connection (as configured in the ~/.ssh/config file on Unix). Defaults to 'sol'.
            compression (str, optional): the compression method used by tar. Choices are 'z'(gzip), 'j'(bzip2), 'J'(xz), ''(None). Defaults to 'z'.
            connection_timeout (int | float, optional): the time in seconds before a TimeoutError is raised if the connection hasn't yet been done. Defaults to 20.
            verbose (int, optional): sets the level of the prints. The higher the value, the more prints there are. Defaults to 0.
            flush (bool, optional): sets the internal buffer to immediately write the output to it's destination, i.e. it decides to force the prints or not. 
                Has a negative effect on the running efficiency as you are forcing the buffer but makes sure that the print is outputted exactly when it is 
                called (usually not the case when multiprocessing). Defaults to False.
        """
        
        # Arguments
        self.host_shortcut = host_shortcut
        self.compression = compression
        self.timeout = connection_timeout
        self.verbose = verbose
        self.flush = flush

        # Constants
        self.ctrl_socket_filepath = os.path.join(tempfile.gettempdir(), 'server_control_socket_file')

        # Ssh connection
        self._activate()

    def _activate(self) -> None:
        """
        Does master connection to the server by creating a control socket file. The connection can be closed using the .close() method.

        Raises:
            Exception: if the ssh connection fails.
        """

        # Command to run
        bash_command = f'ssh -M -S {self.ctrl_socket_filepath} -fN {self.host_shortcut}'

        # OS check
        if SSHMirroredFilesystem.os_name == 'nt': bash_command = 'wsl' + bash_command

        # Bash run
        if self.verbose > 1: print(f'\033[37mConnecting to {self.host_shortcut} ...\033[0m', flush=self.flush)
        process = subprocess.Popen(bash_command, shell=True, stderr=subprocess.PIPE) 

        # Connection check
        if self._check_connection(process):
            if self.verbose > 1: print(f'\033[37mConnection successful.\033[0m', flush=self.flush)
        else:
            process.terminate()
            raise TimeoutError(f'\033[1;31mServer SSH connection timeout after {self.timeout} seconds.\033[0m')
        
    @classmethod
    def _append(cls, folder_path: str) -> None:
        """
        To append to the class level attribute list containing the temporary subfolders created. It does a preliminary
        check to see if you are multiprocessing to decide if to use a lock. While initially, this also took into account if you are multithreading, now it doesn't as, after
        further thinking, I don't see why anyone would use this code in a thread as the fetching is not really I/O bound but more dependent on the maximum Wi-Fi connection speed.
        Furthermore, as the default is to to use compression when transferring files, it will be CPU bound before becoming I/O bound.

        Args:
            folder_path (str): path to the new created temporary sub folder.
        """

        is_multiprocessing = (os.getpid() != os.getppid())
        if is_multiprocessing:
            with cls.mp_lock: cls.directory_list.append(folder_path)
        else:
            cls.directory_list.append(folder_path)

    @staticmethod
    def _sub_creation() -> str:

        # Get ID
        ID = os.getpid()

        # Getting the folder creation time
        current_time = time.time()
        time_str = SSHMirroredFilesystem._to_date(current_time)

        # Filesystem check
        SSHMirroredFilesystem._recreate_filesystem()

        # New folder path
        folder_path = os.path.join(SSHMirroredFilesystem.directory_path, f"{ID}_{time_str}") 
        os.mkdir(folder_path)
        
        # Append the new name to the directory list
        SSHMirroredFilesystem._append(folder_path)
        return folder_path

    def _check_connection(self, process: subprocess.Popen) -> bool:
        """
        Checks if the ssh master connection was created (i.e. if the control socket file exists). If not, waits 100ms before checking again. Also looks if the 
        process finished and catches the corresponding error.

        Args:
            process (subprocess.Popen): the bash process for creating the SSH master connection.

        Raises:
            Exception: if the process is finished and an error output was caught.

        Returns:
            bool: False if the control socket file still doesn't exist before reaching the timeout time. True otherwise.
        """

        start_time = time.time()

        while time.time() - start_time < self.timeout:
            if os.path.exists(self.ctrl_socket_filepath): return True  # connection established.

            # Bash errors
            if process.poll() is not None:  # i.e. process finished
                error_message = process.stderr.read().decode()
                if error_message: raise Exception(f'\033[1;31mSSH connection failed. Error: {error_message.strip()}\033[0m')
            time.sleep(0.1)
        return False

    def mirror(self, remote_filepaths: str | list[str], strip_level: int = 2) -> str | list[str]:
        """
        Given server filepath(s), it returns the corresponding filepath(s) to the file(s) now in the local temporary folder. The filename order remains the same than
        the inputted one.

        This method is based on running an ssh and tar bash command on the shell.

        Args:
            remote_filepaths (str | list[str]): the filepath(s) to the remote files.
            strip_level (int, optional): set the --strip-components tar command option. Hence, it sets how many parent folder for each filepaths are removed in the 
                temporary local mirrored filesystem. This is why the mirroring is only partial. If the value is higher than the number of parent directories, then
                only the filename itself is kept. Defaults to 2.

        Raises:
            Exception: shell error.

        Returns:
            str | list[str]: the created local filepaths.
        """

        # Setup        
        if isinstance(remote_filepaths, str): remote_filepaths = [remote_filepaths]
        remote_filepaths_str = ' '.join(remote_filepaths)

        # Creating a temporary sub directory
        folder_path = self._sub_creation()

        # Bash command
        tar_creation_command = f'tar c{self.compression}f - --absolute-names {remote_filepaths_str}'
        tar_extraction_command = f"tar x{self.compression}f - -C {folder_path} --absolute-names --strip-components={strip_level}"
        bash_command = f"ssh -S {self.ctrl_socket_filepath} {self.host_shortcut} '{tar_creation_command}' | {tar_extraction_command}"  

        # OS check
        if SSHMirroredFilesystem.os_name == 'nt': bash_command = 'wsl ' + bash_command

        # Bash run
        process = subprocess.run(bash_command, shell=True, stderr=subprocess.PIPE)
    
        # Errors
        if process.stderr: raise Exception(f"\033[1;31mFunction 'mirror' didn't get the file(s). Error: {process.stderr.decode().strip()}\033[0m")
        
        # Local filepath setup 
        length = len(remote_filepaths)
        local_filepaths = [None] * length
        for i in range(length): local_filepaths[i] = os.path.join(folder_path, self._strip(remote_filepaths[i], strip_level))

        if length == 1: return local_filepaths[0]
        return local_filepaths

    def close(self) -> None:
        """
        Closes the ssh connection to the server.
        """

        # Checking if the control socket still exists
        if not os.path.exists(self.ctrl_socket_filepath): return

        # Command
        bash_command = f'ssh -S {self.ctrl_socket_filepath} -O exit sol',
    
        # OS check
        if SSHMirroredFilesystem.os_name == 'nt': bash_command = 'wsl ' + bash_command

        # Bash run
        process = subprocess.run(bash_command, shell=True, stderr=subprocess.PIPE)

        # Bash errors
        if process.stderr and self.verbose > 0:
            error_message = process.stderr.decode().strip()
            if error_message != 'Exit request sent.':
                print(f'\033[1;31mFailed to disconnect from {self.host_shortcut}. Error: {error_message}\033[0m', flush=self.flush)

    @staticmethod
    def remote_to_local(remote_filepaths: str | list[str], host_shortcut: str = 'sol', compression: str = 'z', strip_level: int = 2) -> str | list[str]:
        """
        Given server filepath(s), it returns the created corresponding local filepath(s). Partially mirrors the server directory path(s) inside a temporary folder.

        This method is based on running an ssh and tar bash command on the shell. It uses WSL if the OS is Windows and, as such, a Windows user needs to have 
        set up WSL through the Microsoft store.

        Args:
            remote_filepaths (str | list[str]): the filepath(s) to the remote files.
            host_shortcut (str, optional): the short cut to the server set in the ~/.shh/config file. A local private key and remote public key needs to
                already set up. Defaults to 'sol'.
            compression (str, optional): the compression method used by tar. Choices are 'z'(gzip), 'j'(bzip2), 'J'(xz), ''(None). Defaults to 'z'.
            strip_level (int, optional): set the --strip-components tar command option. Hence, it sets how many parent folder to each files are removed in the 
                temporary local mirrored filesystem. This is why the mirroring is only partial. Defaults to 2.
        Raises:
            Exception: corresponding shell error.

        Returns:
            str | list[str]: the created local filepaths. 
        """

        # Setup        
        if isinstance(remote_filepaths, str): remote_filepaths = [remote_filepaths]
        remote_filepaths_str = ' '.join(remote_filepaths)

        # Temporary sub folder creation
        folder_path = SSHMirroredFilesystem._sub_creation()

        # Bash commands
        tar_creation_command = f'tar c{compression}f - --absolute-names {remote_filepaths_str}'
        tar_extraction_command = f"tar x{compression}f - --strip-components={strip_level} --absolute-names -C {folder_path}"
        bash_command = f"ssh {host_shortcut} '{tar_creation_command}' | {tar_extraction_command}"

        # OS check
        if SSHMirroredFilesystem.os_name == 'nt': bash_command = 'wsl ' + bash_command

        # Bash run
        result = subprocess.run(bash_command, shell=True, stderr=subprocess.PIPE)
    
        # Errors
        if result.stderr: raise Exception(f"\033[1;31mFunction 'remote_to_local' didn't get the file(s). Error: {result.stderr.decode().strip()}\033[0m")
        
        # Local filepath setup 
        length = len(remote_filepaths)
        local_filepaths = [None] * length
        for i in range(length): local_filepaths[i] = os.path.join(folder_path, SSHMirroredFilesystem._strip(remote_filepaths[i], 1))

        if len(local_filepaths) == 1: return local_filepaths[0]
        return local_filepaths    
    
    @staticmethod
    def _to_timestamp(date_str : str):

        date_part, micro_part = date_str.rsplit(sep='_', maxsplit=1)
        timestamp = time.mktime(time.strptime(date_part, '%Y%m%d_%H%M%S'))
        microseconds = int(micro_part)
        return timestamp + microseconds / 1e6

    @staticmethod
    def _to_date(timestamp: float) -> str:

        date_part = time.strftime('%Y%m%d_%H%M%S', time.localtime(int(timestamp)))
        microseconds = int((timestamp % 1) * 1e6)
        return f"{date_part}_{microseconds:06d}"
    
    @staticmethod
    def _cleanup_choices(which: str):

        directory_path = SSHMirroredFilesystem.directory_path
        directories = os.listdir(directory_path)
        dates = [name.split(sep='_', maxsplit=1)[1] for name in directories]


        if 'ID' in which:
            # Getting all the directories with the same ID
            ID = str(os.getpid())  # TODO: maybe I need to also set a threading.get_ident(). But I might take away the 
            #threads as there is no need to put this code inside a thread. 
            rm_dir = [name for name in directories if ID in name]

            if 'Closest' in which:
                # Taking the time into account
                current_time = time.time()

                # Closest date before current_time
                timestamps = [SSHMirroredFilesystem._to_timestamp(name.split(sep='_', maxsplit=1)[1]) for name in rm_dir]
                closest = max([ts for ts in timestamps if ts < current_time])
                closest_index = timestamps.index(closest)

                # Corresponding directory
                rm_dir = directories[closest_index]

                    



    @staticmethod
    def cleanup(which: str = 'all', verbose: int = 0) -> None:
        """
        Used to clean up the temporary filesystem.

        Args:
            verbose (int, optional): if > 0 gives out a print when the temporary folder doesn't exist. Defaults to 0.
        """

        options = ['all', 'latest', 'oldest', 'sameIDClosest', 'sameIDAll']

        if which not in options: raise ValueError(f"\033[1;31mArgument 'which' cannot be '{which}'. Possible values are: {', '.join(options)}")

        # If the directory exists
        if os.path.exists(SSHMirroredFilesystem.directory_path):
            
            # Setup
            if 'ID' in which: ID = os.getpid()
            directory_path = SSHMirroredFilesystem.directory_path
            directories = os.listdir(directory_path)

            directories_info = [name.split(sep='_', maxsplit=1) for name in directories]






        if os.path.exists(SSHMirroredFilesystem.directory_path):
            shutil.rmtree(SSHMirroredFilesystem.directory_path)
            if verbose > 0: print(f"\033[37mcleanup: temporary filesystem {SSHMirroredFilesystem.directory_path} removed.\033[0m")
        elif verbose > 0:
            print(f"\033[37mcleanup: temporary filesystem {SSHMirroredFilesystem.directory_path} already removed.\033[0m")

    @staticmethod
    def _strip(fullpath: str, strip_level: int) -> str:
        """
        Strips the leading directories of a filepath just like the --strip-components command used with tar in bash. If the strip_level is too high, only the 
        filename is kept.

        Args:
            fullpath (str): the filepath.
            strip_level (int): number of leading directories to be stripped.

        Returns:
            str: the filepath after being stripped.
        """

        # Path separation
        parts = fullpath.strip(os.sep).split(os.sep)

        # Strip
        path_stripped = parts[strip_level:] if len(parts) > strip_level else [parts[-1]]
        return os.path.join(*path_stripped)
    
    @classmethod
    def _recreate_filesystem(cls) -> None:
        """
        To recreate the main temporary folder if it was already removed.
        """

        if not os.path.exists(cls.directory_path): 
            cls.directory_list = []
            cls.directory_path = tempfile.mkdtemp()

