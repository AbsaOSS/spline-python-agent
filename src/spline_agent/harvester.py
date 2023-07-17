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
import sys
import uuid
from typing import Callable

from spline_agent.constants import AGENT_INFO
from spline_agent.context import LineageTrackingContext, WriteMode
from spline_agent.exceptions import LineageContextIncompleteError
from spline_agent.lineage_model import *
from spline_agent.utils import current_time


def harvest_lineage(ctx: LineageTrackingContext, entry_func: Callable) -> Lineage:
    if ctx.output is None:
        raise LineageContextIncompleteError('output')
    if ctx.write_mode is None:
        raise LineageContextIncompleteError('write_mode')
    if ctx.system_info is None:
        raise LineageContextIncompleteError('system_info')

    cur_time = current_time()

    write_operation = WriteOperation(
        id='op-0',
        childIds=('op-1',),
        name='Write',  # todo: put something more meaningful there, maybe 'write to {ds.type}'
        outputSource=ctx.output.url,
        append=ctx.write_mode == WriteMode.APPEND,
    )

    read_operations = tuple(
        ReadOperation(
            id=f'op-{i + 2}',
            inputSources=(inp.url,),
            name='Read',  # todo: put something more meaningful there, maybe 'read from {ds.type}'
        ) for i, inp in zip(range(len(ctx.inputs)), ctx.inputs))

    data_operations = _process_func(ctx, entry_func)

    operations = Operations(
        write=write_operation,
        reads=read_operations,
        other=data_operations,
    )

    plan = ExecutionPlan(
        id=uuid.uuid4(),
        name=ctx.name,
        operations=operations,
        systemInfo=ctx.system_info,
        agentInfo=AGENT_INFO
    )
    event = ExecutionEvent(
        plan_id=plan.id,
        timestamp=cur_time,
    )
    lineage = Lineage(plan, event)
    return lineage


def _process_func(ctx: LineageTrackingContext, func: Callable) -> tuple[DataOperation, ...]:
    # todo: parse and process the imported modules/functions recursively (issue #4)
    func_source_code = inspect.getsource(func)

    operation = DataOperation(
        id='op-1',
        childIds=tuple(f'op-{i + 2}' for i in range(len(ctx.inputs))),
        name='Python script',
        extra={
            'python_version': sys.version,
            'function_name': func.__name__,
            'source_code': func_source_code,
        }
    )
    return (operation,)