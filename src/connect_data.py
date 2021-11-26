import json
from abc import ABC, abstractmethod

import pandas as pd

from errors import ValidationError


def read_json_file(file_path: str) -> pd.DataFrame:
    """read a json file as DataFrame

    :param file_path: path of file
    """
    with open(file_path, mode="r") as f:
        data = json.load(f)
        data = json.loads(data[0])
        return pd.DataFrame.from_dict(data)


def read_csv_file(file_path: str) -> pd.DataFrame:
    """read a csv file as DataFrame

    :param file_path: path of file
    """
    return pd.read_csv(file_path)


class DataSchema:
    def __init__(self, schema_path: str, source_name: str) -> None:
        self.schema_path = schema_path
        self.source_name = source_name
        self._schema = None

    @property
    def schema(self) -> dict:
        if self._schema is None:
            self._schema = self._get_schema(self.source_name)
        return self._schema

    def _get_schema(self) -> dict:
        with open(self.schema_path) as f:
            schema = json.load(f)
            return schema.get(self.source_name)


class BaseValidator(ABC):
    def __init__(
        self,
        schema: dict,
    ) -> None:
        self.schema = schema

    @abstractmethod
    def _transform_type(self, data: pd.Series) -> pd.Series:
        pass

    def _map_value(self, data: pd.Series) -> pd.Series:
        return data

    def validate(self, data: pd.Series) -> pd.Series:
        """transform and validate data by schema"""
        data = self._transform_type(data)
        return self._map_value(data)


class IntegerValidator(BaseValidator):
    def _transform_type(self, data: pd.Series) -> pd.Series:
        """transform data type by schema"""
        try:
            data = data.astype(self.schema["type"])
        except ValueError:
            raise ValidationError(
                "Data %s is not valid type: %s" % data, self.schema["type"]
            )
        return data

    def _get_mapping(self, value_mapping: dict) -> dict:
        """get mapping from current value to target value

        :param value_mapping: mapping from target value to list of current value
        """
        mapping = {}
        for key, values in value_mapping.items():
            mapping.update({value: key for value in values})
            mapping.update({key: key})
        return mapping

    def _map_value(self, data: pd.Series) -> pd.Series:
        """map current value to target value based on schema"""
        value_mapping = self.schema.get("value_mapping")
        if value_mapping:
            mapping = self._get_mapping(value_mapping)
            data = data.map(mapping)
        return data


StringValidator = IntegerValidator


class DateValidator(BaseValidator):
    def _transform_type(self, data: pd.Series) -> pd.Series:
        """transform data type by schema"""
        try:
            data = pd.to_datetime(data)
        except ValueError:
            raise ValidationError(
                "Data %s is not valid type: %s" % data, self.schema["type"]
            )
        return data


class ChoicesValidator(StringValidator):
    def validate(self, data: pd.Series) -> pd.Series:
        """validate data values are subset of choices"""
        data = super().validate(data)
        diff = set(data).difference(self.schema["choices"])
        if diff:
            raise ValidationError(f"Value is not subset of choices: {diff}")
        return data


class ValidatorFactory:
    def get_validator(self, schema: dict):
        """provide validator based on schema type information

        :param schema: data schema
        """
        if schema.get("choices"):
            return ChoicesValidator(schema)
        if schema["type"] in (int, float, str):
            return IntegerValidator(schema)
        elif schema["type"] == "date":
            return DateValidator(schema)


class DataFrameValidator:
    def __init__(self) -> None:
        self.factory = ValidatorFactory()

    def validate(self, schema: dict, data: pd.DataFrame) -> pd.DataFrame:
        """transform and validate dataframe by schema

        :param schema: data schema
        :param data: data to be validated
        """
        for key, value in schema.items():
            validator = self.factory.get_validator(value)
            data.loc[:, key] = validator.validate(data.loc[:, key])
        return data
