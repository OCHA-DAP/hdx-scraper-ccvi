#!/usr/bin/python
"""Ccvi scraper"""

import logging
from os.path import splitext
from typing import Optional
from zipfile import ZipFile

import pyarrow.compute as compute
import pyarrow.parquet as parquet
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.dateparse import get_quarter_end, get_quarter_start
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

    def generate_dataset(self) -> Optional[Dataset]:
        dataset_name = "climate-conflict-vulnerability-index"
        dataset_title = "Climate Conflict Vulnerability Index"
        dataset_tags = [
            "climate hazards",
            "climate-weather",
            "conflict-violence",
            "geodata",
            "hazards and risk",
            "natural disasters",
        ]

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )
        dataset.add_tags(dataset_tags)
        dataset.set_subnational(True)
        dataset.add_other_location("world")

        resources = []
        qtr_path = self._retriever.download_file(
            self._configuration["qtr_url"], filename="latest_data.zip"
        )
        readme_resource = None
        with ZipFile(qtr_path, "r") as zipfile:
            for fileinfo in sorted(
                zipfile.infolist(), key=lambda x: x.file_size, reverse=True
            ):
                filename = fileinfo.filename
                inputpath = zipfile.extract(filename, path=self._tempdir)
                if filename == "README.md":
                    readme_resource = Resource(
                        {
                            "name": f"LAST_QTR_{filename}",
                            "description": "File structures of last quarter dataset.",
                        }
                    )
                    readme_resource.set_format("txt")
                    readme_resource.set_file_to_upload(inputpath)
                else:
                    resource = Resource(
                        {
                            "name": f"last_qtr_{filename}",
                            "description": "Dataset covers only the latest quarter. See LAST_QTR_README.md for file structure.",
                        }
                    )
                    file_format = splitext(filename)[1]
                    resource.set_format(file_format[1:])
                    resource.set_file_to_upload(inputpath)
                    resources.append(resource)
        resources.append(readme_resource)
        full_path = self._retriever.download_file(
            self._configuration["full_url"], filename="latest_data_full.zip"
        )
        with ZipFile(full_path, "r") as zipfile:
            parquets = sorted(
                [
                    x
                    for x in zipfile.infolist()
                    if splitext(x.filename)[1] == ".parquet"
                ],
                key=lambda x: x.file_size,
                reverse=True,
            )
            for fileinfo in parquets:
                filename = fileinfo.filename
                inputpath = zipfile.extract(filename, path=self._tempdir)
                if "exposure" in filename:
                    date_columns = parquet.read_table(
                        inputpath, columns=["year", "quarter"]
                    )
                    year_column = date_columns["year"]
                    quarter_column = date_columns["quarter"]
                    year_minmax = compute.min_max(year_column)
                    year_min = int(year_minmax[0])
                    year_max = int(year_minmax[1])
                    ind_min = compute.equal(year_column, year_min)
                    ind_max = compute.equal(year_column, year_max)
                    qtrs_year_min = compute.filter(quarter_column, ind_min)
                    qtrs_year_max = compute.filter(quarter_column, ind_max)
                    quarter_min = int(compute.min(qtrs_year_min))
                    quarter_max = int(compute.max(qtrs_year_max))
                    date_start = get_quarter_start(year_min, quarter_min)
                    date_end = get_quarter_end(year_max, quarter_max)
                    dataset.set_time_period(date_start, date_end)
                resource = Resource(
                    {
                        "name": filename,
                        "description": "Full dataset including historical time series and reference data. See FULL_README.md for file structure.",
                    }
                )
                file_format = splitext(filename)[1]
                resource.set_format(file_format[1:])
                resource.set_file_to_upload(inputpath)
                resources.append(resource)
            for fileinfo in sorted(
                zipfile.infolist(), key=lambda x: x.file_size, reverse=True
            ):
                filename = fileinfo.filename
                file_format = splitext(filename)[1]
                if file_format == "parquet":
                    continue
                inputpath = zipfile.extract(filename, path=self._tempdir)
                if filename == "README.md":
                    readme_resource = Resource(
                        {
                            "name": f"FULL_{filename}",
                            "description": "File structures of full dataset.",
                        }
                    )
                    readme_resource.set_format("txt")
                    readme_resource.set_file_to_upload(inputpath)
                else:
                    resource = Resource(
                        {
                            "name": filename,
                            "description": "Full dataset including historical time series and reference data. See FULL_README.md for file structure.",
                        }
                    )
                    resource.set_format(file_format[1:])
                    resource.set_file_to_upload(inputpath)
                    resources.append(resource)
        resources.append(readme_resource)
        dataset.add_update_resources(resources)
        return dataset
