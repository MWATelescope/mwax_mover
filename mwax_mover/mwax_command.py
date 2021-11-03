import subprocess
import shlex

# def run_command(logger, command: str, timeout: int = 60) -> bool:
#     # Example: ["dada_diskdb", "-k 1234", "-f 1216447872_02_256_201.sub -s"]
#     try:
#         logger.debug(f"Executing {command}...")
#
#         # Parse the command into executable and args
#         args = shlex.split(command)
#
#         # Execute the command
#         completed_process = subprocess.run(args, shell=False, check=True, timeout=timeout)
#
#         return_code = completed_process.returncode
#         stdout = completed_process.stdout
#         stderror = completed_process.stderr
#
#         if return_code != 0:
#             logger.error(f"Error executing {command}. Return code: {return_code} StdErr: {stderror} StdOut: {stdout}")
#             return False
#         else:
#             return True
#
#     except subprocess.CalledProcessError as cpe:
#         logger.error(f"Error executing {command}: {str(cpe)}")
#         return False
#
#     except Exception as command_exception:
#         logger.error(f"Error executing {command}: {str(command_exception)}")
#         return False


#def run_shell_command(logger, command: str, timeout: int = 60) -> bool:
#    # Example: "dada_diskdb -k 1234 -f 1216447872_02_256_201.sub -s"
#    try:
#        logger.debug(f"Executing {command}...")
#        # Execute the command
#        completed_process = subprocess.run(command, shell=True, check=True, timeout=timeout)
#
#        return_code = completed_process.returncode
#        stderror = completed_process.stderr
#
#        if return_code != 0:
#            logger.error(f"Error executing {command}. Return code: {return_code} StdErr: {stderror}")
#            return False
#        else:
#            return True

#    except subprocess.CalledProcessError as cpe:
#        logger.error(f"Error executing {command} {str(cpe)}")
#        return False
#
#    except Exception as command_exception:
#        logger.error(f"Error executing {command}: {str(command_exception)}")
#        return False
#

# This will return true/false plus the output from stdout
# use shell should be used when you are using wildcards and other shell features
def run_command_ext(logger, command: str, numa_node: int, timeout: int = 60, use_shell: bool = False) -> (bool, str):
    # Example: ["dada_diskdb", "-k 1234", "-f 1216447872_02_256_201.sub -s"]
    if numa_node > 0:
        cmdline = f"numactl --cpunodebind={str(numa_node)} --membind={str(numa_node)} " \
                  f"{command}"
    else:
        cmdline = f"{command}"

    try:
        logger.debug(f"Executing {cmdline}...")

        # Parse the command into executable and args
        args = shlex.split(cmdline)

        # Execute the command
        completed_process = subprocess.run(args, shell=use_shell, check=True,
                                           timeout=timeout, capture_output=True, text=True, )

        return_code = completed_process.returncode
        stdout = completed_process.stdout
        stderror = completed_process.stderr

        if return_code != 0:
            logger.error(f"Error executing {cmdline}. Return code: {return_code} StdErr: {stderror} StdOut: {stdout}")
            return False, ""
        else:
            return True, stdout

    except subprocess.CalledProcessError as cpe:
        logger.error(f"CalledProcessError executing {cmdline}: {str(cpe)} {cpe.stderr}")
        return False, ""

    except Exception as command_exception:
        logger.error(f"Exception executing {cmdline}: {str(command_exception)}")
        return False, ""