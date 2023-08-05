# Spline agent for Python scripts [PoC]

See: https://github.com/AbsaOSS/spline

The goal is to create a module that would act as a wrapper around a user Python code,
that would execute it in a lineage trackable manner. The main idea is that the user would create a function,
e.g. `my_awesome_function`, that accepts input and output data source definitions as function arguments,
and it would execute some logic reading the data from input sources, transforming it in some way and writing
the result into the output. Then, in order to track lineage, the user would decorate that function with the 
Spline Python agent decorators and execute the function as usual.
The Agent would then intercept the call, inspect the given function and the input/output definitions,
execute the function, take some stats, create a lineage metadata and send it to the Spline server in a similar way
the [Spark Agent](https://github.com/AbsaOSS/spline-spark-agent) does it.
So that the lineage tracking process would be as seamless and non-intrusive to the user code as possible in Python.

### Example

```python
import spline_agent
from spline_agent.enums import WriteMode


# Decorate a function you want to track with as follows
@spline_agent.track_lineage()
@spline_agent.inputs('{input_source_1}', '{input_source_2}')
@spline_agent.output('{output_source}', WriteMode.APPEND)
def my_awesome_function(output_source, input_source_1, input_source_2):
    print("read from input source")
    print("do some business logic")
    print("write into the output")


# Execute the target function as normal
my_awesome_function("some_url_1", "some_url_2", "some_url_3")
```

The `inputs()` and `output()` decorators could be places on any other
function that is called (also transitively) from the `my_awesome_function`.
The data source can be represented as a URL string t the source,
or the string containing the Spline Expression (SpEL) - currently ir only
supports the format `"{func_parameter_name}"` (e.r. `"{abc}"` would mean -
take the value from the `abc` parameter of the decorated function).

# Building

### TL;DR

```shell
make build install
```

### Makefile targets

- _Default_ - Runs tests and linters.
- `clean` - Remove output directory (`dist`)
- `build` - Full clean build with everything excepts for installing.
- `install` - Install WHEEL file produced by the `build` target

---

    Copyright 2023 ABSA Group Limited

    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
