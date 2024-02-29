"""Module to execute arbitrary commands"""

import subprocess
import shlex
import typing


# This will return true/false plus the output from stdout
# use shell should be used when you are using wildcards and other shell
# features
def run_command_ext(
    logger,
    command: str,
    numa_node: int,
    timeout: int = 60,
    use_shell: bool = False,
) -> typing.Tuple[bool, str]:
    """Runs a command and returns success or failure and stdout"""
    # Example: ["dada_diskdb", "-k 1234", "-f 1216447872_02_256_201.sub -s"]
    if numa_node is None:
        cmdline = f"{command}"
    else:
        if int(numa_node) > 0:
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
            args,
            shell=use_shell,
            check=True,
            timeout=timeout,
            capture_output=True,
            text=True,
        )

        return_code = completed_process.returncode
        stdout = completed_process.stdout
        stderror = completed_process.stderr

        if return_code != 0:
            logger.error(
                f"Error executing {cmdline}. Return code:" f" {return_code} StdErr: {stderror} StdOut: {stdout}"
            )
            return False, ""
        else:
            return True, stdout

    except subprocess.CalledProcessError as cpe:
        logger.error(f"CalledProcessError executing {cmdline}: {str(cpe)} {cpe.stderr}")
        return False, ""

    except Exception as command_exception:  # pylint: disable=broad-except
        logger.error(f"Exception executing {cmdline}: {str(command_exception)}")
        return False, ""
