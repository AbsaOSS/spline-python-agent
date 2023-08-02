#  Copyright 2023 ABSA Group Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

class LineageTrackingContextNotInitializedError(Exception):
    """
    Lineage harvesting context was not properly initialized
    """

    def __init__(self, message: str):
        super().__init__(message)


class LineageTrackingContextIncompleteError(Exception):
    """
    Required property is missing from the lineage harvesting context
    """

    def __init__(self, property_name: str):
        self.property_name = property_name
        super().__init__(f"Required property '{property_name}' wasn't specified")


class ConfigurationError(Exception):
    """
    Represents some problem in the configuration
    """

    def __init__(self, message: str):
        super().__init__(message)
