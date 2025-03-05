import csv
import requests
import os
from io import BytesIO
import zipfile
from typing import List, Dict, Any, Optional
from pathlib import Path

RESPONSE_ID_FIELD_NAME = "ResponseId"
QUESTION_ID_FIELD_NAME = "qid"

DATASET_SUBDIR = "data/stack_overflow_developer_survey"
SO_DEVELOPER_SURVEY_URL = "https://cdn.sanity.io/files/jo7n4k8s/production/262f04c41d99fea692e0125c342e446782233fe4.zip/stack-overflow-developer-survey-2024.zip"
class SurveyDataReader:
    """
    A class to read and process Stack Overflow Developer Survey data.
    """

    def __init__(self, schema_file: str, data_file: str):
        if not (os.path.exists(DATASET_SUBDIR) and len([f for f in os.listdir(DATASET_SUBDIR) if f.endswith(".csv")]) == 2):
            self._download_datasets()
        self.schema = self._parse_schema(schema_file)
        self.data = self._parse_data(data_file)

    def _download_datasets(self):
        response = requests.get(SO_DEVELOPER_SURVEY_URL)

        if response.status_code == 200:
            zip_file = BytesIO(response.content)

            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                os.makedirs(DATASET_SUBDIR, exist_ok=True)

                zip_ref.extractall(DATASET_SUBDIR)
        else:
            print(f"Failed to download datasets: Response {response.text}")

    def _parse_schema(self, schema_file: str) -> List[Dict[str, str]]:
        schema = []
        schema_path = Path(schema_file).resolve()
        with open(schema_path, mode="r") as file:
            reader = csv.DictReader(file)
            schema = [row for row in reader]
        return schema

    def _parse_data(self, data_file: str) -> List[Dict[str, Any]]:
        data = []
        data_path = Path(data_file).resolve()
        with open(data_path, mode="r") as file:
            reader = csv.DictReader(file)
            data = [row for row in reader]
        return data

    def get_schema(self) -> List[Dict[str, str]]:
        return self.schema

    def get_data(self) -> List[Dict[str, Any]]:
        return self.data

    def get_question_by_id(self, qid: str) -> Optional[Dict[str, str]]:
        for question in self.schema:
            if question[QUESTION_ID_FIELD_NAME] == qid:
                return question
        return None

    def get_responses_for_question(self, qname: str) -> List[Any]:
        return [response[qname] for response in self.data if qname in response]

    def get_response_by_id(self, response_id: str | int) -> Optional[Dict[str, Any]]:
        response_id_str = str(response_id)
        for response in self.data:
            if response[RESPONSE_ID_FIELD_NAME] == response_id_str:
                return response
        return None