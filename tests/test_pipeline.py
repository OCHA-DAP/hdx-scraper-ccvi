from os.path import join

from hdx.utilities.downloader import Download
from hdx.utilities.path import script_dir_plus_file, temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.ccvi.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir):
        with temp_dir(
            "TestCcvi",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)
                dataset = pipeline.generate_dataset()
                dataset.update_from_yaml(
                    script_dir_plus_file(
                        join("config", "hdx_dataset_static.yaml"), Pipeline
                    )
                )
                assert dataset == {
                    "caveats": "Please refer to the [CCVI Technical "
                    "Documentation](https://climate-conflict.org/www/latest-data/docs/) "
                    "for the full methodology, including limitations.",
                    "data_update_frequency": 90,
                    "dataset_date": "[2015-01-01T00:00:00 TO 2025-06-30T23:59:59]",
                    "dataset_source": "University of the Bundeswehr Munich; Potsdam Institute for "
                    "Climate Impact Research",
                    "groups": [{"name": "world"}],
                    "license_id": "cc-by",
                    "maintainer": "45dc2728-4dfc-4e6a-afa8-2b1a8ed7623b",
                    "methodology": "Direct Observational Data/Anecdotal Data",
                    "name": "climate-conflict-vulnerability-index",
                    "notes": "The Climate—Conflict—Vulnerability Index (CCVI) maps current global "
                    "risks by integrating climate and conflict hazards with local "
                    "vulnerabilities. The index comprises a harmonized set of data "
                    "layers and a transparent scoring methodology to make regions "
                    "globally comparable. The data is updated quarterly and gridded to "
                    "0.5 degrees (ca. 55km by 55km at the equator).\n\n"
                    "The CCVI metrics are organized hierarchically in three pillars — "
                    "climate, conflict and vulnerability. Each pillar is based on "
                    "indicators from publicly available sources, which are further "
                    "grouped into dimensions. Following the IPCC definition, risk "
                    "metrics are computed as a function of hazards, exposure and "
                    "vulnerability.\n\n"
                    "There are two sets of resources. One smaller set contains only the "
                    "data from the latest quarter in tsv format, while the full dataset "
                    "includes historical time series and reference data in parquet "
                    "format.\n",
                    "owner_org": "91b12bfd-0e04-4b43-a33d-9f13bd42d3a8",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "climate hazards",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "climate-weather",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "conflict-violence",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "hazards and risk",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "natural disasters",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Climate Conflict Vulnerability Index",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "Dataset covers only the latest quarter. See "
                        "LAST_QTR_README.md for file structure.",
                        "format": "tsv",
                        "name": "last_qtr_ccvi-structure.tsv",
                    },
                    {
                        "description": "Dataset covers only the latest quarter. See "
                        "LAST_QTR_README.md for file structure.",
                        "format": "tsv",
                        "name": "last_qtr_ccvi-latest.tsv",
                    },
                    {
                        "description": "Dataset covers only the latest quarter. See "
                        "LAST_QTR_README.md for file structure.",
                        "format": "tsv",
                        "name": "last_qtr_ccvi-data-sources.tsv",
                    },
                    {
                        "description": "Dataset covers only the latest quarter. See "
                        "LAST_QTR_README.md for file structure.",
                        "format": "tsv",
                        "name": "last_qtr_ccvi-data-recency.tsv",
                    },
                    {
                        "description": "File structures of last quarter dataset.",
                        "format": "txt",
                        "name": "LAST_QTR_README.md",
                    },
                    {
                        "description": "Full dataset including historical time series and reference "
                        "data. See FULL_README.md for file structure.",
                        "format": "parquet",
                        "name": "exposure_layers.parquet",
                    },
                    {
                        "description": "Full dataset including historical time series and reference "
                        "data. See FULL_README.md for file structure.",
                        "format": "parquet",
                        "name": "base_grid.parquet",
                    },
                    {
                        "description": "Full dataset including historical time series and reference "
                        "data. See FULL_README.md for file structure.",
                        "format": "parquet",
                        "name": "vul_country_raw.parquet",
                    },
                    {
                        "description": "Full dataset including historical time series and reference "
                        "data. See FULL_README.md for file structure.",
                        "format": "parquet",
                        "name": "data_recency.parquet",
                    },
                    {
                        "description": "Full dataset including historical time series and reference "
                        "data. See FULL_README.md for file structure.",
                        "format": "parquet",
                        "name": "index-full.parquet",
                    },
                    {
                        "description": "Full dataset including historical time series and reference "
                        "data. See FULL_README.md for file structure.",
                        "format": "parquet",
                        "name": "ccvi_scores.parquet",
                    },
                    {
                        "description": "Full dataset including historical time series and reference "
                        "data. See FULL_README.md for file structure.",
                        "format": "tsv",
                        "name": "ccvi-structure.tsv",
                    },
                    {
                        "description": "Full dataset including historical time series and reference "
                        "data. See FULL_README.md for file structure.",
                        "format": "tsv",
                        "name": "ccvi-data-sources.tsv",
                    },
                    {
                        "description": "File structures of full dataset.",
                        "format": "txt",
                        "name": "FULL_README.md",
                    },
                ]
