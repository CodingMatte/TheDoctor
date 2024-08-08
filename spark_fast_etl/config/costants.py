class Constants:
    # Configs
    ETL_CONFIG = "etl_config"
    CONFIGS = "configs"
    ENV = "env"
    IAM_ROLE = "aws_iam_role"

    # Input
    INPUT = "input"
    S3_SOURCE = "s3_source_bucket"
    S3_SOURCE_PATH = "s3_source_path"
    SOURCE_FORMAT = "source_format"
    LOCAL_SOURCE = "local_source"

    # Output
    OUTPUT = "output"
    S3_DESTINATION = "s3_destination_bucket"
    S3_DESTINATION_PREFIX = "s3_destination_prefix"
    DESTINATION_FORMAT = "destination_format"
    LOCAL_DESTINATION = "local_destination"

    # Operations
    OPERATIONS = "operations"
    MERGE_TYPE = "merge_type"
    TRANSFORMATION = "transformation_name"
