from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction

class UpperCaseMapFunction(MapFunction):
    def map(self, value):
        return (value[0], value[1].upper())

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(4)
ds = env.from_collection(
    [(1, "a"), (2, "b"), (3, "c")], type_info=Types.TUPLE([Types.INT(), Types.STRING()])
)
ds_upper = ds.map(UpperCaseMapFunction(), output_type=Types.TUPLE([Types.INT(), Types.STRING()]))
ds_upper.print()
env.execute("Flink Setup Test")
