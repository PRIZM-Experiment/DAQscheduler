import argparse
import subprocess
import datetime
try:
    import oyaml as yaml
except ImportError:
    import yaml
import yaml
import time
import logging
import threading
import os

# ----------------------------------------------------------------------------------------------------------------------------
def recursive_replace(old_dict, new_dict):
    for key, value in old_dict.items():
        if isinstance(value, dict):
            recursive_replace(old_dict[key], new_dict[key])
        else:
            new_dict[key] = value    
# ----------------------------------------------------------------------------------------------------------------------------                
def timestamp(dt):
    """ Return the total number of seconds since the UNIX epoch, i.e 1970-01-01 """
    return (dt-datetime.datetime(1970, 1, 1, 0, 0, 0)).total_seconds()
# ----------------------------------------------------------------------------------------------------------------------------
def run_process(cmd, end_timestamp):
    process = subprocess.Popen(cmd.split())

    logger.info("Command '{}' started with pid {}".format(cmd, process.pid))
    
    while process.poll() == None:
        if not(time.time() <= end_timestamp):
            process.terminate()
        time.sleep(0.1)

    if process.returncode == 0:
        logger.info("Command '{}' with pid {} ended with return code 0".format(cmd, process.pid))
    elif process.returncode == -15:
        logger.warning("Command '{}' with pid {} was sent SIGTERM signal".format(cmd, process.pid))
    elif process.returncode == -9:
        logger.warning("Command '{}' with pid {} was sent SIGKILL signal".format(cmd, process.pid))
    else:
        logger.error("Command '{}' with pid {} failed with return code {}".format(cmd, process.pid, process.returncode))
# ----------------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument("configfile", help="yaml file containing albaboss configuration parameters")
    args=parser.parse_args()

    # Read parameters from configuaration file
    parameters=None
    with open(args.configfile, 'r') as cf:
        parameters=yaml.load(cf.read())

    # Setup logging
    logger = logging.getLogger()
    logger.setLevel(parameters["logging"]["level"])
    log_formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s", "%d-%m-%Y %H:%M:%S")
    if parameters["logging"]["directory"] == None:
        log_handler = logging.StreamHandler()
    else:
        if not os.path.isdir(parameters["logging"]["directory"]):
            os.makedirs(parameters["logging"]["directory"])
        log_handler = logging.FileHandler(parameters["logging"]["directory"]+"/"+datetime.datetime.now().strftime("%d%m%Y_%H%M%S")+".log")
    log_handler.setFormatter(log_formatter)
    log_handler.setLevel(parameters["logging"]["level"])
    logger.addHandler(log_handler)

    logger.info("------------------------------------------------------")
    logger.info("-------------------- DAQ autorun ---------------------")
    logger.info("------------------------------------------------------")

    runs = parameters["runs"].keys()
    runs.sort(key=lambda val: int(val[val.find("-")+1:]))

    # Loop through each run
    for run in runs:
        start_time = None
        end_time = None
        runtime = None

        # Check if the 'time' key in a run is a dict or not. If it is, look for
        # start and end times. Else, the 'time' is the runtime of the 'cmd'
        if isinstance(parameters["runs"][run]["time"], dict):
            start_time = datetime.datetime.strptime(parameters["runs"][run]["time"]["start"], "%d-%m-%Y %H:%M:%S")
            end_time = datetime.datetime.strptime(parameters["runs"][run]["time"]["end"], "%d-%m-%Y %H:%M:%S")
        else:
            runtime = parameters["runs"][run]["time"]

        # Check for the "new-parameters" key. If found, replace the old
        # parameters with the new parameters then store it in a new configuration
        # file for the run. Else, use the original configuration file.
        if "new-parameters" in parameters["runs"][run].keys():
            config_parameters = None
            with open(parameters["configuration-file"], 'r') as cf:
                config_parameters = yaml.load(cf.read())
            recursive_replace(parameters["runs"][run]["new-parameters"], config_parameters)
            temp_config_file = "temp_config_"+run+".yaml"
            with open(temp_config_file, "w") as temp:
                yaml.dump(config_parameters, temp, default_flow_style=False)
        else:
            temp_config_file = parameters["configuration-file"]

        updated_cmd = parameters["cmd"].replace("{configuration-file}", temp_config_file)

        time_now = time.time()
        if runtime == None:
            if time_now < timestamp(start_time):
                sleep_time = timestamp(start_time)-time_now
                logger.info("Current time is '{}' which is less than the start time '{}' for {}".format(datetime.datetime.now(), start_time, run))
                logger.info("Sleeping for {} seconds".format(sleep_time))
                time.sleep(sleep_time)
                logger.info("Starting {}".format(run))
                run_process(updated_cmd, timestamp(end_time))
            else:
                logger.info("The time to begin {} has passed. Skipping".format(run))
        else:
            logger.info("Starting {}".format(run))
            run_process(updated_cmd, time_now+runtime)
