import yaml

class SparkFastEtl:

    def import_etl_config(self, path_to_config: str):
        with open(path_to_config, 'r') as file:
            etl_config_content = yaml.safe_load(file)

        print(etl_config_content)
        return etl_config_content



if __name__ == '__main__':
    spark_fast_etl = SparkFastEtl()
