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

import inspect
import platform
import uuid
from typing import Callable

from spline_agent.commons.utils import current_time
from spline_agent.constants import AGENT_INFO, EXECUTION_PLAN_NAMESPACE
from spline_agent.context import LineageTrackingContext, WriteMode
from spline_agent.exceptions import LineageTrackingContextIncompleteError
from spline_agent.json_serde import to_compact_json_str
from spline_agent.lineage_model import *


def harvest_lineage(
        ctx: LineageTrackingContext,
        entry_func: Callable,
        duration_ns: Optional[DurationNs],
        error: Optional[Any]) -> Lineage:
    if ctx.output is None:
        raise LineageTrackingContextIncompleteError('output')
    if ctx.write_mode is None:
        raise LineageTrackingContextIncompleteError('write_mode')
    if ctx.system_info is None:
        raise LineageTrackingContextIncompleteError('system_info')

    cur_time = current_time()

    write_operation = WriteOperation(
        id='op-0',
        childIds=('op-1',),
        name='Write',  # todo: put something more meaningful here, maybe 'write to {ds.type}' (issue #15)
        outputSource=ctx.output.url,
        append=ctx.write_mode == WriteMode.APPEND,
    )

    read_operations = tuple(
        ReadOperation(
            id=f'op-{i + 2}',
            inputSources=(inp.url,),
            name='Read',  # todo: put something more meaningful here, maybe 'read from {ds.type}' (issue #15)
        ) for i, inp in zip(range(len(ctx.inputs)), ctx.inputs))

    data_operations = _process_func(ctx, entry_func)

    operations = Operations(
        write=write_operation,
        reads=read_operations,
        other=data_operations,
    )

    plan = ExecutionPlan(
        id=None,  # the value assigned below as it needs to be calculated from the object hash
        name=ctx.name,
        operations=operations,
        systemInfo=ctx.system_info,
        agentInfo=AGENT_INFO,
        extraInfo={}
    )

    plan.id = uuid.uuid5(EXECUTION_PLAN_NAMESPACE, to_compact_json_str(plan))

    event = ExecutionEvent(
        planId=plan.id,
        timestamp=cur_time,
        durationNs=duration_ns,
        error=error,
        extra={
            'python_implementation': platform.python_implementation(),
            'python_version': platform.python_version(),
            'platform': platform.platform(),
            'system': platform.system(),
            'release': platform.release(),
            'machine': platform.machine(),
        }
    )

    lineage = Lineage(plan, event)
    return lineage


def _process_func(ctx: LineageTrackingContext, func: Callable) -> tuple[DataOperation, ...]:
    # todo: parse and process the imported modules/functions recursively (issue #4)
    func_source_code = inspect.getsource(func)

    func_name = func.__name__
    # noinspection PyUnresolvedReferences
    module_name = func.__module__
    file_name = inspect.getfile(func)

    operation = DataOperation(
        id='op-1',
        childIds=tuple(f'op-{i + 2}' for i in range(len(ctx.inputs))),
        name='Python script',
        extra={
            'function_name': func_name,
            'module_name': module_name,
            'source_file': file_name,
            'source_code': func_source_code,
        }
    )
    return (operation,)
