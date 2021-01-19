import subprocess

def run_command(logger, executable: str, args: list, timeout: int = 60) -> bool:
    # Example: ["dada_diskdb", "-k 1234", "-f 1216447872_02_256_201.sub -s"]
    try:
        logger.debug(f"Executing {executable} {' '.join(args)}...")
        # Execute the command
        completed_process = subprocess.run(executable=executable, args=args, shell=False, check=True, timeout=timeout)

        return_code = completed_process.returncode
        stdout = completed_process.stdout
        stderror = completed_process.stderr

        if return_code != 0:
            logger.error(f"Error executing {executable} {' '.join(args)}. Return code: {return_code} StdErr: {stderror} StdOut: {stdout}")
            return False
        else:
            return True

    except subprocess.CalledProcessError as cpe:
        logger.error(f"Error executing {executable} {' '.join(args)} {str(cpe)}")
        return False

    except Exception as command_exception:
        logger.error(f"Error executing {executable} {' '.join(args)}: {str(command_exception)}")
        return False


def run_shell_command(logger, command: str, timeout: int = 60) -> bool:
    # Example: "dada_diskdb -k 1234 -f 1216447872_02_256_201.sub -s"
    try:
        logger.debug(f"Executing {command}...")
        # Execute the command
        completed_process = subprocess.run(command, shell=True, check=True, timeout=timeout)

        return_code = completed_process.returncode
        stderror = completed_process.stderr

        if return_code != 0:
            logger.error(f"Error executing {command}. Return code: {return_code} StdErr: {stderror}")
            return False
        else:
            return True

    except subprocess.CalledProcessError as cpe:
        logger.error(f"Error executing {command} {str(cpe)}")
        return False

    except Exception as command_exception:
        logger.error(f"Error executing {command}: {str(command_exception)}")
        return False
