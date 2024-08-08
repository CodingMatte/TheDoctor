from spark_fast_etl.config.costants import Constants
from spark_fast_etl.config.engine_config import EngineConfig


class EngineConfigReader:

    def __init__(
        self, config_content: dict, aws_key: str = None, aws_secret: str = None
    ):
        self.config_content = config_content
        self.aws_key = aws_key
        self.aws_secret = aws_secret

    def initialize_engine_content(self) -> EngineConfig:
        return EngineConfig(
            self.config_content[Constants.ETL_CONFIG][Constants.CONFIGS][Constants.ENV],
            self.config_content[Constants.ETL_CONFIG][Constants.INPUT][
                Constants.SOURCE_FORMAT
            ],
            self.config_content[Constants.ETL_CONFIG][Constants.OUTPUT][
                Constants.DESTINATION_FORMAT
            ],
            self.config_content[Constants.ETL_CONFIG][Constants.OPERATIONS][
                Constants.MERGE_TYPE
            ],
            self.config_content[Constants.ETL_CONFIG][Constants.OPERATIONS][
                Constants.TRANSFORMATION
            ],
            self.aws_key if self.aws_key else None,
            self.aws_secret if self.aws_secret else None,
            (
                self.config_content[Constants.ETL_CONFIG][Constants.CONFIGS][
                    Constants.IAM_ROLE
                ]
                if Constants.IAM_ROLE
                in self.config_content[Constants.ETL_CONFIG][Constants.CONFIGS].keys()
                else None
            ),
            (
                self.config_content[Constants.ETL_CONFIG][Constants.INPUT][
                    Constants.S3_SOURCE
                ]
                if Constants.S3_SOURCE
                in self.config_content[Constants.ETL_CONFIG][Constants.INPUT].keys()
                else None
            ),
            (
                self.config_content[Constants.ETL_CONFIG][Constants.INPUT][
                    Constants.S3_SOURCE_PATH
                ]
                if Constants.S3_SOURCE_PATH
                in self.config_content[Constants.ETL_CONFIG][Constants.INPUT].keys()
                else None
            ),
            (
                self.config_content[Constants.ETL_CONFIG][Constants.INPUT][
                    Constants.LOCAL_SOURCE
                ]
                if Constants.LOCAL_SOURCE
                in self.config_content[Constants.ETL_CONFIG][Constants.INPUT].keys()
                else None
            ),
            (
                self.config_content[Constants.ETL_CONFIG][Constants.OUTPUT][
                    Constants.S3_DESTINATION
                ]
                if Constants.S3_DESTINATION
                in self.config_content[Constants.ETL_CONFIG][Constants.OUTPUT].keys()
                else None
            ),
            (
                self.config_content[Constants.ETL_CONFIG][Constants.OUTPUT][
                    Constants.S3_DESTINATION_PREFIX
                ]
                if Constants.S3_DESTINATION_PREFIX
                in self.config_content[Constants.ETL_CONFIG][Constants.OUTPUT].keys()
                else None
            ),
            (
                self.config_content[Constants.ETL_CONFIG][Constants.OUTPUT][
                    Constants.LOCAL_DESTINATION
                ]
                if Constants.LOCAL_DESTINATION
                in self.config_content[Constants.ETL_CONFIG][Constants.OUTPUT].keys()
                else None
            ),
        )


# TODO: Add checks on dict before initialize?
