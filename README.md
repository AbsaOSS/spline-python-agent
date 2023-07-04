# Spline agent for Python scripts [PoC]

See: https://github.com/AbsaOSS/spline

The goal is to create a module that would act as a wrapper around a user Python code,
that would execute it in a lineage trackable manner. The main idea is that the user would create a function,
e.g. `my_data_transform`, that would accept input and output data source definitions as function arguments,
and it would execute some logic reading the data from input sources, transforming it in some way and writing
the result into the output. Then instead of executing that function directly, the user would call Spline Python agent
and pass the function as a callback parameter along with the concrete input and output data source definitions.
The Agent would then inspect the given function and the input/output definitions, execute the function,
take some stats, create a lineage metadata and send it to the Spline server in a similar way
the [Spark Agent](https://github.com/AbsaOSS/spline-spark-agent) does it.
So that the lineage tracking process would be as seamless and non-intrusive to the user code as possible in Python.

Example:

```python
# Define a user function
def my_data_transform(output_source, input_source_1, input_source_2):
    print("read from input source")
    print("do some business logic")
    print("write into the output")
  
# Define data sources
input_source_1 = S3DataSource(...)
input_source_2 = FileDataSource(...)
output_source = HDFSDataSource("my.parquet")

# Use the SplineAgent to execute the user function
agent = SplineAgent()
agent.execute(my_data_transform, output_source, input_source_1, input_source_2)

# later in another Python program that consumes data from the "my.parquet"... 

input_source = HDFSDataSource("my.parquet")
agent.execute(my_another_function, my_another_output_source, input_source)

# The lineage would be recorded and the dependency between both runs would be inferred automatically.
```



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
