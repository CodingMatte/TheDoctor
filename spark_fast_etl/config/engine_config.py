class EngineConfig:
    def __init__(
        self,
        env: str,
        source_format: str,
        destination_format: str,
        merge_type: str,
        transformation_name: str,
        aws_key: str = None,
        aws_secret: str = None,
        aws_iam_role: str = None,
        s3_source_bucket: str = None,
        s3_source_path: str = None,
        local_source: str = None,
        s3_destination_bucket: str = None,
        s3_destination_prefix: str = None,
        local_destination: str = None,
    ):
        if local_source is None and (
            s3_source_bucket is None
            or s3_source_path is None
            or aws_iam_role is None
            or aws_secret is None
            or aws_key is None
        ):
            raise ValueError(
                "At least one of 'local_source' or aws s3 configs "
                "(s3_source_bucket, s3_source_path, aws_iam_role, aws_secret, aws_key) "
                "must be provided."
            )

        if local_destination is None and (
            s3_destination_bucket is None
            or s3_destination_prefix is None
            or aws_iam_role is None
            or aws_secret is None
            or aws_key is None
        ):
            raise ValueError(
                "At least one of 'local_destination' or aws s3 configs "
                "(s3_destination_bucket, s3_destination_prefix, aws_iam_role, aws_secret, aws_key) "
                "must be provided."
            )

        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.env = env
        self.aws_iam_role = aws_iam_role
        self.s3_source_bucket = s3_source_bucket
        self.s3_source_path = s3_source_path
        self.source_format = source_format
        self.local_source = local_source
        self.s3_destination_bucket = s3_destination_bucket
        self.s3_destination_prefix = s3_destination_prefix
        self.destination_format = destination_format
        self.local_destination = local_destination
        self.merge_type = merge_type
        self.transformation_name = transformation_name
