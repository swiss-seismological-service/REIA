import configparser

from reia.io import CalculationBranchSettings
from reia.tests.test_api import test_api
from settings import get_config
from settings.config import Config


class Calculation():
    def __init__(self,
                 config: Config,
                 branches: list[CalculationBranchSettings]):

        self.config = config
        self.branches = branches
        self.files = []


def main():
    # test_api()
    job_file = configparser.ConfigParser()

    branches = [CalculationBranchSettings(1, job_file)]
    Calculation(get_config(), branches)


if __name__ == '__main__':
    main()
