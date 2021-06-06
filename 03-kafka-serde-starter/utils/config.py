import logging
import sys

import yaml


def get_yaml(in_path):
    """
    Read YAML config
    :param in_path: yaml file location
    :return: config in dict format
    """
    with open(in_path, 'r') as yml_file:
        cfg = yaml.load(yml_file, Loader=yaml.FullLoader)

    return cfg


CONFIGS = get_yaml("../resources/configs.yaml")
KAFKA_CONFIGS = CONFIGS['kafka']
APP_CONFIGS = CONFIGS['application']

LOG = logging.getLogger('learning_journal')
LOG.setLevel(APP_CONFIGS['log-level'])
BASE_FMT = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                             datefmt='%Y-%m-%d %H:%M:%S')

console_out = logging.StreamHandler(sys.stdout)
console_out.setFormatter(BASE_FMT)
LOG.addHandler(console_out)
