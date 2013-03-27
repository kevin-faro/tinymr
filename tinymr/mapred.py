class MapRed:
    def identity_reducer(key, values, context):
        """just emits the key/value pairs"""

        for value in values:
            context.emit(key, value)
    #end identity_reducer

    def default_output(key, value):
        """print tab delimited key and value"""

        if key is None:
            print(str(value))
        elif value is None:
            print(str(key))
        else:
            print(str(key) + '\t' + str(value))

    @staticmethod 
    def run(mapper, reducer=identity_reducer, output=default_output):
        """runs a mapred job"""

        import sys
        from .context import MContext
        from .context import RContext
        import string
        #input
        lines = sys.stdin.readlines()
        #map
        map_context = MContext()
        line_num = 0
        for line in lines:
            mapper(line_num, line.strip(), map_context)
            line_num += 1
        #shuffle (NOOP)
        #sort
        context_sorted_keys = sorted(map_context.keys())
        #reduce
        reduce_context = RContext()
        for key in context_sorted_keys:
            values = map_context.get(key)
            reducer(key, values, reduce_context)
        #output
        for entry in reduce_context.data():
            output(entry[0], entry[1])
    #end run
#end mapred
if __name__ == "__main__":
    usage="""submit a job to the run method.
    input_path: is the file to read in line by line
    mapper: is the map function to apply.  Should fit the convetion of (key, value, context)
    reducer: (optional) is the reducer function to apply.  Should fit the convention of (key, value, context)
    """

    print(usage)
