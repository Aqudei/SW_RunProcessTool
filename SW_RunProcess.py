from os import system
import os
import requests
import logging
import logging.handlers
import csv
import json
import argparse
from pprint import pprint
from datetime import datetime
import time
from requests.api import head

logger = None

USAGE = 'Command Usage: python omid.py --sched-procname <scheduler process name> --check-triggers <list of trigger files>\nor\npython omid.py --sched-procname <scheduler process name>'
CONFIG_FILE = './SW_RunProcessConfig.json'
# Subclass Builtin ArgumentParser so we can print error to console


class MyArgumentParser(argparse.ArgumentParser):
    """
    docstring
    """

    def error(self, message: str):
        logger.error(message)
        super().error(message)


# Parse Config from ./config.json file
def read_config():
    with open(CONFIG_FILE) as fp:
        return json.loads(fp.read())

# Logger setup. Add Streamhandler and FileHandler


def setup_logger():
    """
    docstring
    """
    print("Setting up logger...")
    config = read_config()

    logfile = config['logfile']

    # Setup of Logger
    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(logging_format)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    # logging.basicConfig(level=logging.DEBUG, handlers=(fh, ch))
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


logger = setup_logger()


def check_global_running():
    """
    docstring
    """
    logger.info("Checking if Global Action is Running...")
    config = read_config()
    sleep_time = config.get('sleep_seconds', 5)
    wait_time = config['wait_time_global_action'] * 60
    headers = config['api_headers']
    url = config['base_url'] + 'globalactionstatus'

    loop_start = datetime.now()

    while (datetime.now() - loop_start).total_seconds() < wait_time:
        time.sleep(sleep_time)

        response = requests.get(url, headers=headers)
        logger.debug(f"Request Url: <{url}>")
        logger.debug(f"Response Status Code: <{response.status_code}>")

        if not response.status_code == 200:
            logger.error(f"Response Status Text: <{response.text}>")
            raise Exception("Unable to check global action status")

        if 'true' in response.text.lower():
            continue

        return False

    return True


def get_process(process_name):
    """
    docstring
    """
    logger.info(f"Fetching <process id> of <{process_name}>...")
    config = read_config()
    headers = config['api_headers']
    base_url = config['base_url']
    url = base_url + 'scheduleitem/'
    response = requests.get(url=url, headers=headers)
    logger.info(f"Request Url: <{url}>")
    logger.info(f"Response Status Code: <{response.status_code}>")
    if response.status_code != 200:
        logger.error(f"Response Status Text: <{response.text}>")
        return

    response_json = response.json()

    lookup = dict({item['name'].upper(): item for item in response_json})
    process = lookup.get(process_name.upper())
    return process


def check_triggers(triggers):
    """
    docstring
    """
    logger.debug(
        f"Checking if {len(triggers)} <Trigger Files> exists... Files: <{', '.join(triggers).strip()}>")
    found = set()
    config = read_config()
    base_url = config['base_url']
    sleep_time = config.get('sleep_seconds', 5)
    loop_seconds = config['wait_time_trigger_file'] * 60

    url = base_url + 'serverfilenames?filter=FileType%3DData%3BIsComplete%3Dtrue'
    loop_start = datetime.now()
    headers = config['api_headers']

    while (datetime.now()-loop_start).total_seconds() < loop_seconds:
        time.sleep(sleep_time)

        response = requests.get(url, headers=headers)
        logger.debug(f"Request Url: <{url}>")
        logger.debug(f"Response Status Code: <{response.status_code}>")

        if response.status_code != 200:
            logger.error(
                f"Error on Check Trigger Files with Response Text: <{response.text}>")
            break

        response_json = response.json()
        for item in triggers:
            if item in response_json:
                logger.debug(f"Found Trigger File: {item}")
                found.add(item)

        if len(found) == len(triggers):
            break

    return len(found) == len(triggers)


def check_if_running(activity_id):
    """
    docstring
    """
    config = read_config()
    headers = config['api_headers']
    url = config['base_url'] + f'liveactivities/{activity_id}'
    response = requests.get(url=url, headers=headers)
    logger.debug(f"Request Url: <{url}>")
    logger.debug(f"Response Status Code: <{response.status_code}>")
    logger.debug(f"Response Status Text: <{response.text}>")
    if response.status_code == 200:
        return True
    if response.status_code == 404:
        return False

    raise Exception(
        f"Cannot determine if process is running <instance_id:{activity_id}>.!")


def execute_scheduled_process(process_id):
    """
    docstring
    """
    logger.info(f"Executing scheduled process <{process_id}>")
    config = read_config()
    headers = config['api_headers']
    wait_time = config['wait_time_process_execution'] * 60
    sleep_time = config.get('sleep_seconds', 5)
    url = config['base_url'] + f'rpc/scheduleitem/{process_id}/run'
    response = requests.post(url=url, headers=headers)
    logger.debug(f"Request Url: <{url}>")
    logger.debug(f"Response Status Code: <{response.status_code}>")

    response_json = response.json()
    if not response_json.get('liveactivities'):
        logger.error(f"Failed to run process with id:<{process_id}>")
        return False

    instance_id = response_json['liveactivities'].split("/")[-1]
    logger.info(f"Process ran with instance_id:<{instance_id}>")

    logger.info("Waiting for process completion...")
    loop_start = datetime.now()
    status_code = 0

    while (datetime.now()-loop_start).total_seconds() < wait_time and not status_code == 404:
        url = config['base_url'] + f'liveactivities/{instance_id}'
        response = requests.get(url=url, headers=headers)
        logger.debug(f"Request Url: <{url}>")
        logger.debug(f"Response Status Code: <{response.status_code}>")
        status_code = response.status_code
        time.sleep(sleep_time)

    return status_code == 404


def delete_triggers(triggers):
    """
    docstring
    """
    config = read_config()
    headers = config['api_headers']
    logger.info(f"Deleting Trigger files: <{', '.join(triggers).strip()}>")
    for trigger in triggers:
        url = config['base_urlv2'] + f'serverfiles/{trigger}'
        response = requests.delete(url=url, headers=headers)
        logger.debug(f"Request Url: <{url}>")
        logger.debug(f"Response Status Code: <{response.status_code}>")
        if not response.status_code == 204:
            logger.warn(f"Failed to delete trigger file: {trigger}")

        logger.info(f"Trigger file deleted! <{trigger}>")

# retrieve arguments from commandline giver by user


def retrieve_arguments():
    """
    docstring
    """
    parser = MyArgumentParser()
    parser.add_argument("--sched-procname")
    parser.add_argument("--check-triggers", type=str, nargs='+')

    try:
        options = parser.parse_args()
        return options
    except argparse.ArgumentError as e:
        logger.error(e)

# validate arguments/parameters logic


def validate_options(options):
    """
    docstring
    """
    logger.info("Validating arguments...")
    if not options.sched_procname:
        logger.error("Parameter <sched-procname> is required!")
        print(USAGE)
        exit(1)


def final_check(process_id):
    """
    docstring
    """
    config = read_config()
    url = config['base_url'] + f'scheduleitem/{process_id}'
    headers = config['api_headers']
    response = requests.get(url=url, headers=headers)
    logger.debug(f"Request Url: <{url}>")
    logger.debug(f"Response Status Code: <{response.status_code}>")
    if not response.status_code == 200:
        logger.error(response.text)
        return False

    response_json = response.json()
    message1 = f"LastRun:{response_json.get('lastRun')}, LastRunStatus:{response_json.get('lastRunStatus')}"
    logger.debug(message1)

    for child in response_json.get('childScheduleItems'):
        if child.get('activation', 'activation').lower() != 'enabled':
            continue

        message = f"Name:{child['name']}, ScheduledItemType:{child['scheduleItemType']}, LastRun:{child['lastRun']}, LastRunStatus:{child['lastRunStatus']}"
        logger.debug(message)

    return response_json.get('lastRunStatus') and not response_json.get('lastRunStatus').lower() == 'error'


if __name__ == "__main__":
    logger.info("***")
    logger.info("***")
    logger.info("***")
    logger.info("*** Started process to Execute Scheduler Tasks’")

    os.makedirs('./logs', exist_ok=True)

    options = retrieve_arguments()
    validate_options(options)

    process = get_process(options.sched_procname)
    if not process:
        logger.error(
            f"Process Terminated. Process: <{options.sched_procname}> cannot be found !")
        exit(1)

    process_id = process['id']
    if process.get('activation', 'activation').lower() != 'enabled':
        logger.error(
            f"Process Terminated. Process: <{options.sched_procname}> is disabled!")
        exit(1)

    if options.check_triggers:
        if not check_triggers(options.check_triggers):
            logger.error("Process Terminated. Trigger Files Not Found!")
            exit(1)

    if check_global_running():
        logger.error("Process Terminated. Global Action ongoing!")
        exit(1)

    success = execute_scheduled_process(process_id)
    if success:
        success = final_check(process_id)

    if success and options.check_triggers:
        delete_triggers(options.check_triggers)

    exit(0 if success else 1)
