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

    def __init__(self, input_dir, prefix, expanded):
        """
        Parameters:
        input_dir (str): Full path to input directory
        prefix (str): Indicates the AWS environment venue
        expanded (bool): Indicates whether attempting to run on expanded reach idenitifier list
        """

        self.input_dir = pathlib.Path(input_dir)
        self.save_failures = {}
        self.s3_json = f"{prefix}-json"
        self.s3_map = f"{prefix}-map-state"
        if expanded:
            self.MODULES_JSON["input"] = "expanded_reaches_of_interest.json"
            self.JSON[1] = "expanded_reaches_of_interest.json"

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

    def remove_failures(self, module_dict, remove):
        """Remove failed identifiers from appropriate files.

        Parameters:
        module_dict (dict): Dictionary of modules and indexes to remove
        remove (bool): Indicates if failures should be removed from EFS
        """

        for module, indexes in module_dict.items():
            if len(indexes) == 0: continue
            reach_ids = self.search_reaches(self.input_dir.joinpath(self.MODULES_JSON[module]), indexes)
            self.save_failures[module] = {
                "json_file": self.MODULES_JSON[module],
                "indexes": indexes,
                "reach_ids": reach_ids
            }
            if remove:
                for json_file in self.JSON:
                    removed_json_data = self.remove_reaches(self.input_dir.joinpath(json_file), reach_ids)
                    self.write_json(self.input_dir.joinpath(json_file), removed_json_data)
    
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

    @staticmethod
    def remove_reaches(json_file, reach_ids):
        """Remove reach identifiers from JSON file.

        Parameters:
        json_file (pathlib.Path): Path to JSON file to remove reaches from
        reach_ids (list): List of integer reach identifiers

        Returns:
        (list): removed_json_data
        """

        with open(json_file) as jf:
            json_data = json.load(jf)

        removed_json_data = []    
        for data in json_data:

            if "basin" in str(json_file):
                replacement_reach_ids = [reach_id for reach_id in data["reach_id"] if reach_id not in reach_ids]
                data["reach_id"] = replacement_reach_ids
                removed_json_data.append(data)

            if "reaches" in str(json_file):
                if data["reach_id"] not in reach_ids:
                    removed_json_data.append(data)

            if "sets" in str(json_file):
                found = False
                for reach_id in data:
                    if reach_id["reach_id"] in reach_ids:
                        found = True
                        break
                if not found:
                    removed_json_data.append(data)

        return removed_json_data

    def write_json(self, json_file, json_data):
        """Write JSON data to JSON file.

        Parameters:
        json_file (pathlib.Path): Path to JSON file to write to
        json_data (list): JSON data to write to file
        """

        with open(json_file, "w") as jf:
            json.dump(json_data, jf, indent=2)

    def save_failures_json(self):
        """Save failures as JSON to file."""

        with open(self.input_dir.joinpath("failures.json"), "w") as jf:
            json.dump(self.save_failures, jf, indent=2)

    def upload_json(self, remove):
        """Upload JSON files to S3 JSON bucket.

        Parameters:
        remove (bool): Indicates if failures should be removed from EFS
        """

        if remove:
            json_files = [self.input_dir.joinpath(json_file) for json_file in self.JSON] \
                + [self.input_dir.joinpath("failures.json")] \
                + [self.input_dir.joinpath("continent.json")] \
                + [self.input_dir.joinpath("continent-setfinder.json")]
        else:
            json_files = [self.input_dir.joinpath("failures.json")]

        date_prefix = f"{datetime.datetime.now().strftime('%Y%m%dT%H%M%S')}_redrive"
        for json_file in json_files:
            # Upload under date prefix
            self.S3.upload_file(str(json_file),
                        self.s3_json,
                        f"{date_prefix}/{json_file.name}",
                        ExtraArgs={"ServerSideEncryption": "aws:kms"})
            logging.info(f"Uploaded {self.s3_json}/{json_file.name}.")

            # Upload to root of bucket
            if json_file.name != "failures.json":
                self.S3.upload_file(str(json_file),
                                    self.s3_json,
                                    json_file.name,
                                    ExtraArgs={"ServerSideEncryption": "aws:kms"})
            logging.info(f"Uploaded {self.s3_json}/{date_prefix}/{json_file.name}.")

    def restart_execution(self, version, run_type, tolerated, expanded):
        """Restart Step Function execution to skip failures and try again.

        Parameters:
        version (str): 4-digit version number for current run
        run_type (str): Indicates 'constrained' or 'unconstrained' run
        tolerated (int): Indicates tolerated percentage of failures
        expanded (bool): Indicates whether attempting to run on expanded reach idenitifier list
        """

        exe_arn = self.locate_exe_arn()
        logging.info("Located execution ARN: %s", exe_arn)

        is_empty = []
        for json_file in (self.input_dir.joinpath(self.MODULES_JSON["input"]), self.input_dir.joinpath(self.MODULES_JSON["moi"])):
            is_empty.append(self.check_is_json_empty(json_file))

        if any(is_empty):
            logging.error(f"There are not enough reach data to continue execution.")
            logging.info(f"Check 'failures.json' file stored at s3://{self.s3_json} for info on failed reaches.")
            raise Exception("All reaches were removed from basin or reaches JSON files.")
        
        if expanded:
            input = {"version": version, "run_type": run_type, "reach_subset_file": "reaches_of_interest.json", "tolerated_failure_percentage": tolerated}
        else:
            input = {"version": version, "run_type": run_type, "tolerated_failure_percentage": tolerated}

        state_machine_arn = ":".join(exe_arn.split(":")[:-1]).replace("execution", "stateMachine")
        name = f"{state_machine_arn.split(':')[-1]}-{random.randint(100000, 999999)}"
        response = self.SFN.start_execution(
            stateMachineArn=f"arn:aws:states:us-west-2:475312736312:stateMachine:confluence-dev1-workflow",
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

        is_empty = []
        if "basin" in json_file.name:
            for item in data:
                if len(item["reach_id"]) == 0:
                    is_empty.append(True)
                else:
                    is_empty.append(False)
        if "reaches" in json_file.name:
            if len(data) == 0:
                is_empty.append(True)
            else:
                is_empty.append(False)

        return all(is_empty)

    def delete_map(self):
        """Delete all objects in S3 map bucket."""

        map_files = self.search_files()
        delete_files = {"Objects": [ { "Key": file } for file in map_files ]}
        self.S3.delete_objects(Bucket=self.s3_map, Delete=delete_files)
        for map_file in map_files: logging.info("Deleted %s bucket object: %s", self.s3_map, map_file)


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
    arg_parser.add_argument("-s",
                            "--startexe",
                            action="store_true",
                            help="Indicates whether to start another execution of the workflow.")
    arg_parser.add_argument("-f",
                            "--remove",
                            action="store_true",
                            help="Indicates whether to remove failures from EFS.")
    arg_parser.add_argument("-t",
                            "--tolerated",
                            type=int,
                            help="Tolerated failure percentage.")
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
    start_exe = args.startexe
    remove = args.remove
    tolerated = args.tolerated

    logging.info("Input directory: %s", input_dir)
    logging.info("Prefix: %s", prefix)
    logging.info("Version: %s", version)
    logging.info("Run type: %s", run_type)
    logging.info("Expanded: %s", expanded)
    logging.info("Start execution: %s", start_exe)
    logging.info("Remove failures: %s", remove)
    logging.info("Tolerated failure percentage: %s", tolerated)

    restart = Restart(input_dir, prefix, expanded)

    module_dict = restart.locate_failures()
    count = len([value for values in module_dict.values() for value in values])
    logging.info("Located %s failures.", count)

    restart.remove_failures(module_dict, remove)
    for module, data in restart.save_failures.items(): 
        logging.info("Located %s failed reach identifiers: %s.", module.upper(), data["reach_ids"])

    restart.save_failures_json()
    logging.info("Saved failures to file.")

    restart.upload_json(remove)

    if start_exe:
        exe_name, exe_time = restart.restart_execution(version, run_type, tolerated, expanded)
        logging.info("%s was restarted at %s UTC.", exe_name, exe_time.strftime("%Y-%m-%dT%H:%M%S"))
        restart.delete_map()

    end = datetime.datetime.now()
    logging.info("Elapsed time: %s", end - start)


if __name__ == "__main__":
    run_redrive()
