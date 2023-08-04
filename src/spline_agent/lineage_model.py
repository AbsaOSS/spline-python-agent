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

from dataclasses import dataclass
from typing import Optional, Any, Mapping
from uuid import UUID

# Timestamp since the Unix epoch in milliseconds
Timestamp = int

# Duration of the event in nanoseconds
DurationNs = int

# Represents an execution plan operation ID as string, e.g. 'op-123'
OperationId = str


@dataclass
class NameAndVersion:
    name: str
    version: str


@dataclass
class WriteOperation:
    id: OperationId
    childIds: tuple[OperationId, ...]
    name: str
    outputSource: str
    append: bool


@dataclass
class ReadOperation:
    id: OperationId
    name: str
    inputSources: tuple[str, ...]


@dataclass
class DataOperation:
    id: OperationId
    childIds: tuple[OperationId, ...]
    name: str
    extra: Mapping[str, Any]


@dataclass
class Operations:
    write: WriteOperation
    reads: tuple[ReadOperation, ...]
    other: tuple[DataOperation, ...]


@dataclass
class ExecutionPlan:
    id: Optional[UUID]
    name: Optional[str]
    operations: Operations
    agentInfo: NameAndVersion
    systemInfo: NameAndVersion
    extraInfo: Mapping[str, Any]


@dataclass
class ExecutionEvent:
    planId: UUID
    timestamp: Timestamp
    durationNs: Optional[DurationNs]
    error: Optional[Any]
    extra: Mapping[str, Any]


@dataclass
class Lineage:
    plan: ExecutionPlan
    event: ExecutionEvent
