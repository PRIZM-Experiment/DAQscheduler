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
    return (dt-datetime.datetime(1970, 1, 1, 0, 0, 0)).total_seconds()
# ----------------------------------------------------------------------------------------------------------------------------
def run_process(cmd, start_time, end_time):
    start_timestamp = timestamp(start_time)
    end_timestamp = timestamp(end_time)

    now = time.time()
    
    process = subprocess.Popen(cmd.split())

    logger.info("Command '{}' started with pid {}".format(cmd, process.pid))
    
    while process.poll() == None:
        if not(time.time()-start_timestamp <= end_timestamp-now):
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

    parameters=None

    with open(args.configfile, 'r') as cf:
        parameters=yaml.load(cf.read())

    logger = logging.getLogger()
    logger.setLevel(parameters["logging"]["level"])
    log_formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s", "%d-%m-%Y %H:%M:%S")
    if not os.path.isdir(parameters["logging"]["directory"]):
        os.makedirs(parameters["logging"]["directory"])
    log_handler = logging.FileHandler(parameters["logging"]["directory"]+"/scheduler/"+datetime.datetime.now().strftime("%d%m%Y_%H%M%S")+".log")
    log_handler.setFormatter(log_formatter)
    log_handler.setLevel(parameters["logging"]["level"])
    logger.addHandler(log_handler)

    logger.info("------------------------------------------------------")
    logger.info("-------------------- DAQ autorun ---------------------")
    logger.info("------------------------------------------------------")

    config_parameters = None
    with open(parameters["configuration-file"], 'r') as cf:
        config_parameters = yaml.load(cf.read())

    runs = parameters["runs"].keys()
    runs.sort(key=lambda val: int(val[val.find("-")+1:]))

    for run in runs:
        start_time = datetime.datetime.strptime(parameters["runs"][run]["time"]["start"], "%d-%m-%Y %H:%M:%S")
        end_time = datetime.datetime.strptime(parameters["runs"][run]["time"]["end"], "%d-%m-%Y %H:%M:%S")

        temp_parameters = config_parameters.copy()
        recursive_replace(parameters["runs"][run]["new-parameters"], temp_parameters)
        temp_config_file = "temp_config_"+run+".yaml"

        with open(temp_config_file, "w") as temp:
            yaml.dump(temp_parameters, temp, default_flow_style=False)

        updated_cmd = parameters["cmd"].replace("{configuration-file}", temp_config_file)

        if time.time() < timestamp(start_time):
            logger.info("Current time is '{}' which is less than the start time '{}' for {}".format(datetime.datetime.now(), start_time, run))
            logger.info("Sleeping for {} seconds".format(timestamp(start_time)-time.time()))
            time.sleep(timestamp(start_time)-time.time())
            logger.info("Starting {}".format(run))
            run_process(updated_cmd, start_time, end_time)
        elif time.time() > timestamp(end_time):
            logger.info("The time to begin {} has passed. Skipping".format(run))
        else:
            logger.info("Starting {}".format(run))
            run_process(updated_cmd, start_time, end_time)
