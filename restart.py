import argparse
import datetime
import glob
import json
import logging
import pathlib
import random
import tempfile

import boto3


logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    level=logging.INFO
)


class Restart:
    """Class to restart exeuction of Step Function after failure."""

    MODULES_JSON = {
        "input": "reaches.json",
        "prediagnostics": "reaches.json",
        "hivdi": "hivdisets.json",
        "metroman": "metrosets.json",
        "momma": "reaches.json",
        "neobam": "reaches.json",
        "sad": "reaches.json",
        "sic4dvar": "sicsets.json",
        "moi": "basin.json",
        "offline": "reaches.json",
        "validation": "reaches.json",
        "random_fail": "reaches.json"
    }
    JSON = [ "basin.json", "reaches.json", "hivdisets.json", "metrosets.json", "neosets.json", "sicsets.json" ]
    S3 = boto3.client("s3")
    SFN = boto3.client("stepfunctions")
    REACHES_OF_INTEREST = "reaches_of_interest.json"

    def __init__(self, input_dir, prefix, expanded, subset):
        """
        Parameters:
        input_dir (str): Full path to input directory
        prefix (str): Indicates the AWS environment venue
        expanded (bool): Indicates whether attempting to run on expanded reach idenitifier list
        subset (str): Reach subset JSON file name
        """

        self.input_dir = pathlib.Path(input_dir)
        self.random_int = random.randint(100000, 999999)
        self.save_failures = {}
        self.s3_config = f"{prefix}-config"
        self.s3_json = f"{prefix}-json"
        self.s3_map = f"{prefix}-map-state"
        if expanded:
            self.MODULES_JSON["input"] = f"expanded_{subset}"
            self.JSON[1] = f"expanded_{subset}"

    def locate_failures(self):
        """Locate state task failures by parsing S3 bucket for map results."""

        # Search for FAILED files
        failed_files = self.search_files("FAILED")

        # Open FAILED files and locate info
        exe_arns = []
        module_dict = {}
        for failure in failed_files:
            module_name = failure.split("/")[0]
            filename = failure.split("/")[-1]
            module_dict[module_name] = []
            with tempfile.TemporaryDirectory() as s3_temp_dir:
                s3_temp_file = pathlib.Path(s3_temp_dir).joinpath(filename)
                self.S3.download_file(self.s3_map, failure, s3_temp_file)
                module_dict[module_name].extend(self.parse_failures(s3_temp_file))

        return module_dict

    def search_files(self, term=""):
        """Search and return files with term in filename.

        Parameters:
        term (str): String term to search in filename

        Returns:
        list: files
        """

        paginator = self.S3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=self.s3_map
        )
        items = [key["Key"] for page in page_iterator for key in page["Contents"]]
        files = [item for item in items if term in item]
        return files

    @staticmethod
    def parse_failures(failure_file):
        """Parse JSON failure file for index of failed task.

        Parameters:
        failure_file (pathlib.Path): Path to JSON failure file
        
        Returns:
        list: indexes
        """

        with open(failure_file) as jf:
            failure_json = json.load(jf)

        indexes = []
        for failure in failure_json:
            if failure["Status"] == "FAILED":
                indexes.append(json.loads(failure["Input"])["context_index"])

        return indexes

    def find_failed_identifiers(self, module_dict):
        """Find failed identifiers from appropriate files.

        Parameters:
        module_dict (dict): Dictionary of modules and indexes to remove
        """

        for module, indexes in module_dict.items():
            if len(indexes) == 0: continue
            reach_ids = self.search_reaches(self.input_dir.joinpath(self.MODULES_JSON[module]), indexes)
            self.save_failures[module] = {
                "json_file": self.MODULES_JSON[module],
                "indexes": indexes,
                "reach_ids": reach_ids
            }

    @staticmethod        
    def search_reaches(json_file, indexes):
        """Search JSON file for reach_ids at indexes.

        Parameters:
        json_file (pathlib.Path): Path to JSON file
        indexes (list): List of indexes

        Returns:
        (list): reach_ids
        """

        with open(json_file) as jf:
            json_data = json.load(jf)

        if "basin" in str(json_file):
            reach_ids = [reach_id for i in indexes for reach_id in json_data[i]["reach_id"] ]
        if "reaches" in str(json_file):
            reach_ids = [ json_data[i]["reach_id"] for i in indexes ]
        if "sets" in str(json_file):
            reach_ids = [reach["reach_id"] for i in indexes for reach in json_data[i]]

        reach_ids = list(set(reach_ids))
        return reach_ids

    def save_failures_json(self):
        """Save failures as JSON to file.

        Returns:
        (pathlib.Path) json_file
        """

        json_file = self.input_dir.joinpath("failures.json")
        with open(json_file, "w") as jf:
            json.dump(self.save_failures, jf, indent=2)
        return json_file

    def upload_json(self, json_file, s3_type, date_prefix=None):
        """Upload JSON files to S3 JSON bucket.

        Parameters:
        json_files (pathlib.Path): JSON file to upload
        """

        if date_prefix:
            s3_file = f"{date_prefix}/{json_file.name}"
        else:
            s3_file = json_file.name

        if s3_type == "json":
            s3_bucket = self.s3_json
        else:
            s3_bucket = self.s3_config

        self.S3.upload_file(str(json_file),
                            s3_bucket,
                            s3_file,
                            ExtraArgs={"ServerSideEncryption": "aws:kms"})
        return f"{s3_bucket}/{s3_file}"

    def remove_reaches(self, json_file):
        """Remove reach identifiers from JSON file.

        Parameters:
        json_file (pathlib.Path): Path to JSON file to remove reaches from
        """

        failed_reach_ids = [str(reach_id) for value in self.save_failures.values() for reach_id in value["reach_ids"]]

        with open(json_file) as jf:
            json_data = json.load(jf)
        removed_json_data = [identifier for identifier in json_data if identifier not in failed_reach_ids]

        removed_json_file = json_file.parent.joinpath(f"{json_file.name.replace('.json', '')}_{self.random_int}.json")
        with open(removed_json_file, "w") as jf:
            json.dump(removed_json_data, jf, indent=2)
        return removed_json_file

    def create_reach_subset_file(self):
        """Create reach subset file without failed reaches."""

        failed_reach_ids = [reach_id for value in self.save_failures.values() for reach_id in value["reach_ids"]]

        reach_file = self.input_dir.joinpath(self.MODULES_JSON["input"])
        with open(reach_file) as jf:
            reach_data = json.load(jf)
        reach_ids = [reach["reach_id"] for reach in reach_data]

        removed_json_data = [identifier for identifier in reach_ids if identifier not in failed_reach_ids]
        json_file = self.input_dir.joinpath(f"{self.REACHES_OF_INTEREST.replace('.json', '')}_{self.random_int}.json")
        with open(json_file, "w") as jf:
            json.dump(removed_json_data, jf, indent=2)
        return json_file

    def delete_map(self):
        """Delete all objects in S3 map bucket."""

        map_files = self.search_files()
        delete_files = {"Objects": [ { "Key": file } for file in map_files ]}
        self.S3.delete_objects(Bucket=self.s3_map, Delete=delete_files)
        for map_file in map_files: logging.info("Deleted %s bucket object: %s", self.s3_map, map_file)

    def restart_execution(self, prefix, version, run_type, tolerated, subset):
        """Restart Step Function execution to skip failures and try again.

        Parameters:
        version (str): 4-digit version number for current run
        run_type (str): Indicates 'constrained' or 'unconstrained' run
        tolerated (int): Indicates tolerated percentage of failures
        subset (pathlib.Path): Reach subset JSON file
        """

        exe_arn = self.locate_exe_arn()
        logging.info("Located execution ARN: %s", exe_arn)

        is_empty = self.check_is_json_empty(subset)
        if is_empty:
            logging.error(f"There are not enough reach data to continue execution.")
            logging.info(f"Check 'failures.json' file stored at s3://{self.s3_json} for info on failed reaches.")
            raise Exception("All reaches were removed from reaches JSON file.")

        input = {"version": version, "run_type": run_type, "reach_subset_file": subset.name, "tolerated_failure_percentage": tolerated}
        logging.info("State machine input: %s", input)

        state_machine_arn = ":".join(exe_arn.split(":")[:-1]).replace("execution", "stateMachine")
        name = f"{state_machine_arn.split(':')[-1]}-{self.random_int}"
        response = self.SFN.start_execution(
            stateMachineArn=f"arn:aws:states:us-west-2:475312736312:stateMachine:{prefix}-workflow",
            name=name,
            input=json.dumps(input)
        )
        return name, response["startDate"]

    def locate_exe_arn(self):
        """Locate execution ARN for failed Step Function.

        Raises Exception when more than one EXE ARN is detected.
        """

        # Locate "MANIFEST" files
        mainfest_files = self.search_files("manifest")

        # Open manifest files and locate failed map ARN
        map_arns = []
        for manifest in mainfest_files:
            filename = manifest.split("/")[-1]
            with tempfile.TemporaryDirectory() as s3_temp_dir:
                s3_temp_file = pathlib.Path(s3_temp_dir).joinpath(filename)
                self.S3.download_file(self.s3_map, manifest, s3_temp_file)
                map_arns.append(self.parse_manifest(s3_temp_file))

        # Get top-level exe ARN
        exe_arns = []
        for map_arn in map_arns:
            response = self.SFN.describe_map_run(
                mapRunArn=map_arn
            )
            exe_arns.append(response["executionArn"])

        # There should only be one exe ARN that initiated all tasks
        exe_arn = list(set(exe_arns))
        if len(exe_arn) > 1:
            logging.error("Error located more than one exe ARN: %s", exe_arn)
            raise Exception(f"More than one execution ARN has been detected please make sure all previous run files were deleted from S3, {self.s3_map}")
        else:
            return exe_arn[0]

    @staticmethod
    def parse_manifest(manifest_file):
        """Parse JSON manfiest file for execution ARN.

        Parameters:
        manifest_file (pathlib.Path): Path to JSON manifest file

        Returns:
        str: map_arn
        """

        with open(manifest_file) as jf:
            manifest_json = json.load(jf)

        map_arn = manifest_json["MapRunArn"]
        return map_arn

    @staticmethod
    def check_is_json_empty(json_file):
        """Check if JSON file does not contain data.

        Parameters:
        json_file (pathlib.Path): JSON file to check if empty
        Returns:
        (bool): is_empty
        """

        with open(json_file) as jf:
            data = json.load(jf)

        if len(data) == 0:
            return True
        else:
            return False


def create_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Redrive Confluence Step Function failures")
    arg_parser.add_argument("-d",
                            "--inputdir",
                            type=str,
                            default="/mnt/data/input",
                            help="Name of input directory (EFS).")
    arg_parser.add_argument("-p",
                            "--prefix",
                            type=str,
                            default="confluence-dev1",
                            help="Prefix that indicates AWS environment venue.")
    arg_parser.add_argument("-v",
                            "--version",
                            type=str,
                            help="4-digit version number, ie: 0001.")
    arg_parser.add_argument("-r",
                            "--runtype",
                            type=str,
                            choices=["constrained", "unconstrained"],
                            help="Run type, 'constrained' or 'unconstrained'.")
    arg_parser.add_argument("-e",
                            "--expanded",
                            action="store_true",
                            help="Indicates expanded reach run.")
    arg_parser.add_argument("-o",
                            "--report",
                            action="store_true",
                            help="Indicates to report failures and not restart execution.")
    arg_parser.add_argument("-t",
                            "--tolerated",
                            type=int,
                            help="Tolerated failure percentage.")
    arg_parser.add_argument("-u",
                            "--subset",
                            type=str,
                            default="",
                            help="Name of reach subset JSON file.")
    return arg_parser

def run_redrive():

    start = datetime.datetime.now()

    arg_parser = create_args()
    args = arg_parser.parse_args()

    input_dir = args.inputdir
    prefix = args.prefix
    version = args.version
    run_type = args.runtype
    expanded = args.expanded
    report = args.report
    tolerated = args.tolerated
    subset = args.subset

    logging.info("Input directory: %s", input_dir)
    logging.info("Prefix: %s", prefix)
    logging.info("Version: %s", version)
    logging.info("Run type: %s", run_type)
    logging.info("Expanded: %s", expanded)
    logging.info("Report only: %s", report)
    logging.info("Tolerated failure percentage: %s", tolerated)
    logging.info("Reach subset file: %s", subset)

    restart = Restart(input_dir, prefix, expanded, subset)
    logging.info("Unique identifier: %s", restart.random_int)

    module_dict = restart.locate_failures()
    count = len([value for values in module_dict.values() for value in values])
    logging.info("Located %s failures.", count)

    restart.find_failed_identifiers(module_dict)
    for module, data in restart.save_failures.items(): 
        logging.info("Located %s failed reach identifiers: %s.", module.upper(), data["reach_ids"])

    failure_json = restart.save_failures_json()
    logging.info("Saved failures to file.")

    upload_file = restart.upload_json(
        failure_json,
        "json",
        date_prefix=f"{datetime.datetime.now().strftime('%Y%m%dT%H%M%S')}_{restart.random_int}_redrive"
    )
    logging.info("Uploaded: %s", upload_file)

    if not report:
        if subset:
            subset_file = pathlib.Path(input_dir).joinpath(subset)
            updated_subset = restart.remove_reaches(subset_file)
            logging.info("Created new reaches of interest from existing: %s.", updated_subset)
        else:
            updated_subset = restart.create_reach_subset_file()
            logging.info("Created new reaches of interest: %s.", updated_subset)
        upload_file = restart.upload_json(updated_subset, "config")
        logging.info("Uploaded: %s", upload_file)
        exe_name, exe_time = restart.restart_execution(prefix, version, run_type, tolerated, updated_subset)
        logging.info("%s was restarted at %s UTC.", exe_name, exe_time.strftime("%Y-%m-%dT%H:%M%S"))
        restart.delete_map()

    end = datetime.datetime.now()
    logging.info("Elapsed time: %s", end - start)


if __name__ == "__main__":
    run_redrive()
