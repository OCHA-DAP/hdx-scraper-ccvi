#!/usr/bin/python
"""Ccvi scraper"""

import logging
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

        full_path = self._retriever.download_file(self._configuration["full_url"])
        quarter_max = None
        year_max = None
        with ZipFile(full_path, "r") as zipfile:
            for filename in zipfile.namelist():
                if "exposure" in filename:
                    inputpath = zipfile.extract(filename, path=self._tempdir)
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

        dataset.add_tags(dataset_tags)
        # Only if needed
        dataset.set_subnational(True)
        dataset.add_other_location("world")

        qtr_path = self._retriever.download_file(self._configuration["qtr_url"])
        resource = Resource(
            {
                "name": f"CCVI Q{quarter_max} {year_max}",
                "description": "Contains only the data from the latest quarter in tsv format.",
            }
        )
        resource.set_format("tsv")
        resource.set_file_to_upload(qtr_path)
        dataset.add_update_resource(resource)

        resource = Resource(
            {
                "name": "CCVI Full Dataset",
                "description": "Full dataset including historical time series and reference data in parquet format.",
            }
        )
        resource.set_format("parquet")
        resource.set_file_to_upload(full_path)
        dataset.add_update_resource(resource)
        return dataset
