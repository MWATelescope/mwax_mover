"""Module to execute arbitrary commands"""

import os
import subprocess
import shlex
import typing
from typing import Optional


# This will return true/false plus the output from stdout
# use shell should be used when you are using wildcards and other shell
# features
def run_command_ext(
    logger,
    command: str,
    numa_node: typing.Optional[int],
    timeout: int = 60,
    use_shell: bool = False,
    copy_user_env: bool = False,
) -> typing.Tuple[bool, str]:
    """Runs a command and returns success or failure and stdout"""
    myenv: Optional[dict[str, str]] = None

    if copy_user_env:
        # Should we copy the user's environment for the subprocess? Default is no
        myenv = os.environ.copy()

    # Example: ["dada_diskdb", "-k 1234", "-f 1216447872_02_256_201.sub -s"]
    if numa_node is None:
        cmdline = f"{command}"
    else:
        if int(numa_node) >= 0:
            cmdline = "numactl" f" --cpunodebind={str(numa_node)} --membind={str(numa_node)} " f"{command}"
        else:
            cmdline = f"{command}"

    try:
        logger.debug(f"Executing {cmdline}...")

        # Parse the command into executable and args

        if use_shell:
            #
            # NOTE: using shell=true in subprocess.run requires a string.
            # Passing a list won't work!
            #
            args = cmdline
        else:
            args = shlex.split(cmdline)

        # Execute the command
        completed_process = subprocess.run(
            args, shell=use_shell, check=False, timeout=timeout, capture_output=True, text=True, env=myenv
        )

        return_code = completed_process.returncode
        stdout = completed_process.stdout
        stderror = completed_process.stderr

        if return_code != 0:
            # Remove \n from outputs to make the log message nicer
            stderror_log = ""
            if stderror:
                stderror_log = stderror.replace("\n", " ")
            else:
                # if it is None, change it to empty string
                stderror = ""

            stdout_log = ""
            if stdout:
                stdout_log = stdout.replace("\n", " ")
            else:
                # if it is None, change it to empty string
                stdout = ""

            logger.error(
                f"Error executing {cmdline}. Return code: {return_code} "
                f"StdErr: {stderror_log} "
                f"StdOut: {stdout_log}"
            )
            return False, f"{stdout} {stderror}"
        else:
            return True, f"{stdout} {stderror}"

    except Exception as command_exception:  # pylint: disable=broad-except
        error = f"Exception executing {cmdline}: {str(command_exception)}"
        logger.exception(f"Exception executing {cmdline}:")
        return False, error


# This will return a popen process object which can be polled for exit
# use shell should be used when you are using wildcards and other shell
# features
def run_command_popen(
    logger,
    command: str,
    numa_node: int,
    use_shell: bool = False,
    copy_user_env: bool = False,
):
    """Runs a command and returns success or failure and stdout"""
    myenv: Optional[dict[str, str]] = None

    if copy_user_env:
        # Should we copy the user's environment for the subprocess? Default is no
        myenv = os.environ.copy()

    # Example: ["dada_diskdb", "-k 1234", "-f 1216447872_02_256_201.sub -s"]
    if numa_node is None:
        cmdline = f"{command}"
    else:
        if int(numa_node) > 0:
            cmdline = "numactl" f" --cpunodebind={str(numa_node)} --membind={str(numa_node)} " f"{command}"
        else:
            cmdline = f"{command}"

    logger.debug(f"Executing {cmdline}...")

    # Parse the command into executable and args

    if use_shell:
        #
        # NOTE: using shell=true in subprocess.run requires a string.
        # Passing a list won't work!
        #
        args = cmdline
    else:
        args = shlex.split(cmdline)

    # Execute the command
    popen_process = subprocess.Popen(
        args, shell=use_shell, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=myenv
    )
    return popen_process


def check_popen_finished(logger, popen_process, timeout: int = 60) -> typing.Tuple[int, str, str]:
    """Given a running popen_process object
    wait for it to finish and return the exit code
    and output stdout & stderr"""
    stderror = ""
    stdout = ""
    exit_code = -1

    try:
        stdout, stderror = popen_process.communicate(timeout=timeout)
        exit_code = popen_process.returncode

        if exit_code != 0:
            logger.error(
                f"Error executing {popen_process.args}. Return code:"
                f" {exit_code} StdErr: {stderror} StdOut: {stdout}"
            )

    except subprocess.TimeoutExpired as timeout_expired:
        logger.error(f"Timeout expired executing {timeout_expired.cmd}")
        stderror += "\nTimeout expired"

    except subprocess.CalledProcessError as cpe:
        logger.error(f"CalledProcessError executing {popen_process.args}:" f" {str(cpe)} {cpe.stderr}")

    except Exception as command_exception:  # pylint: disable=broad-except
        logger.error(f"Exception executing {popen_process.args}:" f" {str(command_exception)}")

    return (exit_code, stdout, stderror)
