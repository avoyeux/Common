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

from io import BytesIO
from paramiko import SSHClient, AutoAddPolicy



class ServerUtils:
    """
    Stores functions that are related to the server connection. 
    """
    
    @staticmethod
    def slow_remote_access(remote_filepaths: str | list[str], server_username: str = 'avoyeux', private_keypath: str = '/home/avoyeux/.ssh/id_rsa', ssh_port: int = 22,
                      first_hostname: str = 'ias-ssh.ias.u-psud.fr', second_hostname: str = 'sol-calcul1.ias.u-psud.fr', raise_error: bool = True, 
                      verbose: int = 1, flush: bool = False, bufsize: int = 10 * 1024 * 1024, window_size: int = 3 * 1024 * 1024, packet_size: int = 32 * 1024) -> list[BytesIO]:
        """
        Functions that connects to the sol-calcul1.ias.u-psud.fr server and saves the files referenced by their fullpath in the RAM. The connection is done 
        through a private key stored locally as there should already be the equivalent public key on the server.  
        While there are more choices for setting up the remote access using this method, it is super super slow and weirdly highly dependent on the CPU clock speed
        (no clue why as the decoding and encoding shouldn't be that demanding). 

        Args:
            remote_filepaths (str | list[str]): the server fullpath for the files to be accessed.
            server_username (str, optional): your username on the server. Defaults to 'avoyeux'.
            private_keypath (str, optional): the local fullpath to the private key (with the corresponding public key already set up on the server).
                Defaults to '/home/avoyeux/.ssh/id_rsa'.
            port (int, optional): the ssh port value. While 22 is the usual value, it can depend on how the server administrators set it up. Defaults to 22.
            first_hostname (str, optional): the hostname for the first part of the ssh connection. Defaults to 'ias-ssh.ias.u-psud.fr'.
            second_hostname (str, optional): the hostname for the second part of the ssh connection. Defaults to 'sol-calcul1.ias.u-psud.fr'.
            raise_error (bool, optional): deciding to raise the error if an error occurred when trying to read and convert a remote file. Defaults to True.
            verbose (int, optional): set the verbosity. If > 0 the error print is given. If > 1 then all the prints are shown. Defaults to 1.
            flush (bool, optional): sets the internal buffer to immediately write the output to it's destination, i.e. it decides to
                force the prints or not. Has a negative effect on the running efficiency as you are forcing the buffer but makes sure that the print is outputted
                exactly when it is called (usually not the case when multiprocessing). Defaults to False.

        Returns:
            list[BytesIO]: the file data that were access through the server.
        """
        
        # Filepath setup
        remote_filepaths = remote_filepaths if isinstance(remote_filepaths, list) else [remote_filepaths]

        # First connection
        first_client = SSHClient()
        first_client.set_missing_host_key_policy(AutoAddPolicy())
        first_client.connect(hostname=first_hostname, port=ssh_port, username=server_username, key_filename=private_keypath)

        # Set up the forward the connection
        first_transport = first_client.get_transport()
        first_transport.window_size = window_size
        first_transport.packetizer.REKEY_BYTES = pow(2, 40)
        first_transport.packetizer.REKEY_PACKETS = pow(2, 40)
        local_bind_address = ('127.0.0.1', 0)

        # Direct-tcpip channel from the first to the second server
        remote_bind_address = (second_hostname, ssh_port)
        channel = first_transport.open_channel("direct-tcpip", remote_bind_address, local_bind_address, max_packet_size=packet_size)

        # Connect to the second server using the second transport
        second_client = SSHClient()
        second_client.set_missing_host_key_policy(AutoAddPolicy())
        second_client.connect(hostname=second_hostname, sock=channel, username=server_username, key_filename=private_keypath)

        # Open the SFTP session
        sftp = second_client.open_sftp()
        remote_files = [None] * len(remote_filepaths)
        for i, filepath in enumerate(remote_filepaths):
            try:
                remote_file = sftp.file(filepath, mode='r', bufsize=bufsize)
                remote_file_size = remote_file.stat().st_size
                remote_file.prefetch(remote_file_size)
                remote_file.set_pipelined()
                remote_files[i] = BytesIO(remote_file.read(remote_file_size))
                remote_file.close()
            except Exception as e:
                if verbose > 0: print(f"\033[1;31mCouldn't read or convert file {os.path.basename(filepath)} on the server. Given error is: {e}\033[0m", flush=flush)
                if raise_error: raise

        # Closing the tunnel
        sftp.close()
        second_client.close()
        first_client.close()

        if verbose > 1: 
            print(f"\033[37mFrom sol-calcul1, {len(remote_filepaths)} files were accessed and saved in RAM.\033[0m", flush=flush)
            if not raise_error: print("\033[37mThe above number might be wrong as some files might have raise an exception. Look at the read prints.\033[0m", flush=flush)
        
        if len(remote_files) == 1: return remote_files[0]
        return remote_files
    
    @staticmethod
    def ssh_connect(filepaths: str | list[str], raise_error: bool = True, verbose: int = 0, flush: bool = False) -> BytesIO | list[BytesIO] | None:
        """
        To access the sol-calcul1.ias.u-psud server and save the data (reference by the filepaths argument) in the RAM for local use.
        Quick but slower than using the SSHMirroredFilesystem class because this method converts the output of a bash cat function to bytes to save the 
        corresponding file data. The ssh connection is only done once regardless of the number of filepaths given so it is still fairly quick.

        Args:
            filepaths (str | list[str]): the full server filepaths to the files to be used.
            raise_error (bool, optional): deciding to raise an error if there is a problem when trying to access the files. Defaults to True.
            verbose (int, optional): decides how much is being printed. Defaults to 0 (i.e. no prints).
            flush (bool, optional): sets the internal buffer to immediately write the output to it's destination, i.e. it decides to
                force the prints or not. Has a negative effect on the running efficiency as you are forcing the buffer but makes sure that the print is outputted
                exactly when it is called (usually not the case when multiprocessing). Defaults to False.

        Returns:
            BytesIO | list[BytesIO] | None: the buffer references to the files given by the filepaths argument. Keep in mind that the output are
                buffers, i.e. .close() would close the buffer and hence you would loose the data. The functions returns None if there is an error when trying to 
                access the files and raise_error is set to False.  
        """

        if isinstance(filepaths, str): filepaths = [filepaths]
        
        # ssh bash command to get the data using cat and add a delimiter to later separate the data
        delimiter = "dajlkdjlksjdlasljdlkjlfgdfeafeageaeyiokdhfgDSTSY"
        cat_commands = '; '.join([f"cat {path} && echo -n '{delimiter}'" for path in filepaths])
        ssh_command = f"ssh sol \"{cat_commands}\""
        
        if verbose > 1: 
            print(f"\033[37mFunction ssh_connect accessing the server to get file(s): {[os.path.basename(filepath) for filepath in filepaths]}\033[0m", flush=flush)

        # Running the ssh command
        result = subprocess.run(ssh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Checking for errors
        if result.stderr:
            if raise_error: raise Exception(f"\033[1;31mFunction ssh_connect unable to get the files from the server. Error: {result.stderr.decode()}\033[0m")
            return None

        # Split the output into separate files based on the delimiter
        files_data = result.stdout.split(bytes(delimiter, encoding='utf-8'))
        files_in_memory = [BytesIO(data.strip()) for data in files_data if data.strip()]

        if verbose > 1: print("\033[37mFunction ssh_connect successfully accessed and saved the file(s) in RAM.", flush=flush)

        if len(files_in_memory) == 1: return files_in_memory[0]
        return files_in_memory
    

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

    directory_path = tempfile.mkdtemp()  # the path to the main directory for the temporary filesystem
    os_name = os.name  # the OS name to see if the user is on Windows. If so WSL is used for the bash commands

    def __init__(self, host_shortcut: str = 'sol', compression: str = 'z', connection_timeout: int | float = 20, verbose: int = 0, flush: bool = False) -> None:
        """
        Initialising the class. It opens an ssh connection to the server by saving a control socket file in the OS specific temporary directory.
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
    
    def _check_connection(self, process: subprocess.Popen) -> bool:
        """
        Checks if the ssh master connection was created (i.e. if the control socket file exists). If not, waits 100ms before checking again. Also looks if the process
        finished and catches the corresponding error.

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

        # Temporary folder check
        self._recreate_filesystem()

        # Bash commands
        tar_creation_command = f'tar c{self.compression}f - --absolute-names {remote_filepaths_str}'
        tar_extraction_command = f"tar x{self.compression}f - -C {SSHMirroredFilesystem.directory_path} --absolute-names --strip-components={strip_level}"
        bash_command = f"ssh -S {self.ctrl_socket_filepath} {self.host_shortcut} '{tar_creation_command}' | {tar_extraction_command}"  

        # OS check
        if SSHMirroredFilesystem.os_name == 'nt': bash_command = 'wsl ' + bash_command

        # Bash run
        result = subprocess.run(bash_command, shell=True, stderr=subprocess.PIPE)
    
        # Errors
        if result.stderr: raise Exception(f"\033[1;31mFunction 'mirror' unable to get the files from the server. Error: {result.stderr.decode().strip()}\033[0m")
        
        # Local filepath setup 
        length = len(remote_filepaths)
        local_filepaths = [None] * length
        for i in range(length): local_filepaths[i] = os.path.join(SSHMirroredFilesystem.directory_path, self._strip(remote_filepaths[i], strip_level))

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
        result = subprocess.run(bash_command, shell=True, stderr=subprocess.PIPE)

        # Bash errors
        if result.stderr and self.verbose > 0:
            error_message = result.stderr.decode().strip()
            if error_message != 'Exit request sent.':
                print(f'\033[1;31mFailed to disconnect from {self.host_shortcut}. Error: {error_message}\033[0m', flush=self.flush)

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

        # Temporary folder check
        SSHMirroredFilesystem._recreate_filesystem()

        # Bash commands
        tar_creation_command = f'tar c{compression}f - --absolute-names {remote_filepaths_str}'
        tar_extraction_command = f"tar x{compression}f - --strip-components={strip_level} --absolute-names -C {SSHMirroredFilesystem.directory_path}"
        bash_command = f"ssh {host_shortcut} '{tar_creation_command}' | {tar_extraction_command}"

        # OS check
        if SSHMirroredFilesystem.os_name == 'nt': bash_command = 'wsl ' + bash_command

        # Bash run
        result = subprocess.run(bash_command, shell=True, stderr=subprocess.PIPE)
    
        # Errors
        if result.stderr: raise Exception(f"\033[1;31mFunction fetch_tar unable to get the files from the server. Error: {result.stderr.decode()}\033[0m")
        
        # Local filepath setup 
        length = len(remote_filepaths)
        local_filepaths = [None] * length
        for i in range(length): local_filepaths[i] = os.path.join(SSHMirroredFilesystem.directory_path, SSHMirroredFilesystem._strip(remote_filepaths[i], 1))

        if len(local_filepaths) == 1: return local_filepaths[0]
        return local_filepaths
    
    @classmethod
    def _recreate_filesystem(cls) -> None:
        """
        To recreate the main temporary folder if it was already removed.
        """

        if not os.path.exists(cls.directory_path): cls.directory_path = tempfile.mkdtemp()

    @staticmethod
    def cleanup(verbose: int = 0) -> None:
        """
        Used to clean up the temporary filesystem.

        Args:
            verbose (int, optional): if > 0 gives out a print when the temporary folder doesn't exist. Defaults to 0.
        """

        if os.path.exists(SSHMirroredFilesystem.directory_path):
            shutil.rmtree(SSHMirroredFilesystem.directory_path)
        elif verbose > 0:
            print(f"\033[37mcleanup: temporary filesystem {SSHMirroredFilesystem.directory_path} already removed.\033[0m")


