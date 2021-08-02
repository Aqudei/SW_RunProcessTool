from os import system
import requests
import logging
import logging.handlers
import csv
import json
import argparse
from pprint import pprint
from datetime import datetime

from requests.api import head

logger = None

USAGE = 'Command Usage: python omid.py --sched-procname <scheduler process name> --check-triggers <list of trigger files>\nor\npython omid.py --sched-procname <scheduler process name>'

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
    with open('./config.json') as fp:
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

    logging.basicConfig(level=logging.DEBUG, handlers=(fh, ch))

    return logging.getLogger()

def check_global_running():
    """
    docstring
    """
    logger.info("Checking if Global Action is Running...")
    config = read_config()
    wait_time = config['waiting_minutes'] * 60
    headers = config['api_headers']
    url = config['base_url'] + 'globalactionstatus'

    loop_start = datetime.now()

    while (datetime.now() - loop_start).total_seconds < wait_time:
        response = requests.get(url, headers=headers)
        logger.debug(f"Request Url: <{url}>")
        logger.debug(f"Response Status Code: <{response.status_code}>")

        if not response.status_code == 200:
            logger.error(f"Response Status Text: <{response.text}>")
            raise Exception("Unable to check global action status")

        if 'True' in response.text:
            continue

        return False

    return True

def get_process_id(process_name):
    """
    docstring
    """
    logger.info(f"Fetching <process id> of <{process_name}>...")
    config = read_config()
    headers = config['api_headers']
    base_url = config['base_url']
    url = base_url + 'scheduleitem/'
    response = requests.get(url=url, headers=headers)
    logger.debug(f"Request Url: <{url}>")
    logger.debug(f"Response Status Code: <{response.status_code}>")
    logger.info(f"Status Code: {response.status_code}")
    if response.status_code != 200:
        logger.debug(f"Response Status Text: <{response.text}>")
        return

    response_json = response.json()

    lookup = dict({item['name'].upper(): item['id'] for item in response_json})
    return lookup.get(process_name.upper())

def check_triggers(triggers):
    """
    docstring
    """
    found = 0
    logger.info(
        f"Checking if Trigger Files exists... Files: <{', '.join(triggers).strip()}>")
    config = read_config()
    base_url = config['base_url']

    loop_seconds = config['waiting_minutes'] * 60

    url = base_url + 'serverfilenames?filter=FileType%3DData%3BIsComplete%3Dtrue'
    loop_start = datetime.now()
    headers = config['api_headers']

    while True:
        response = requests.get(url, headers=headers)
        logger.debug(f"Request Url: <{url}>")
        logger.debug(f"Response Status Code: <{response.status_code}>")

        if response.status_code != 200:
            logger.error(f"Response Status Text: <{response.text}>")
            break

        response_json = response.json()
        for item in triggers:
            if item in response_json:
                found = found + 1

        if found == len(triggers):
            break

        if ((datetime.now()-loop_start).total_seconds) > loop_seconds:
            break

    return found == len(triggers)

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
    wait_time = config['waiting_minutes'] * 60

    url = config['base_url'] + f'rpc/scheduleitem/{process_id}/run'
    response = requests.post(url=url, headers=headers)
    logger.debug(f"Request Url: <{url}>")
    logger.debug(f"Response Status Code: <{response.status_code}>")

    response_json = response.json()
    if not response_json.get('liveactivities'):
        raise Exception(f"Failed to run process with id:<{process_id}>")
    instance_id = response_json['liveactivities'].split("/")[-1]
    logger.info(f"Process ran with instance_id:<{instance_id}>")
    logger.info("Waiting for process completion...")
    loop_start = datetime.now()
    while (datetime.now()-loop_start).total_seconds < wait_time:
        url = config['base_url'] + f'liveactivities/{instance_id}'
        response = requests.get(url=url, headers=headers)
        logger.debug(f"Request Url: <{url}>")
        logger.debug(f"Response Status Code: <{response.status_code}>")
        if response.status_code == 200:
            continue

        logger.error(response.text)
        break


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


if __name__ == "__main__":
    logger = setup_logger()
    options = retrieve_arguments()
    validate_options(options)

    process_id = get_process_id(options.sched_procname)
    if not process_id:
        logger.error(
            f"Process Id cannot be found for Process Name: {options.sched_procname}!")
        exit(1)

    if not check_triggers(options.check_triggers):
        logger.error("Trigger Files Not Found!")
        exit(1)

    if check_global_running():
        logger.error("Global Action ongoing!")
        exit(1)

    instance_id = execute_scheduled_process(process_id)
