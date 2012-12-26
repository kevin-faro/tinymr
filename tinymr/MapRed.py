class MapRed:
    

    def identity_reducer(key, values, context):
        """just emits the key/value pairs"""

        for value in values:
            context.emit(key, value)
    #end identity_reducer
           
    @staticmethod 
    def run(mapper, reducer=identity_reducer):
        """runs a mapred job"""

        import sys
        from .Context import MContext
        from .Context import RContext
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
            if entry[0] is None:
                print(str(entry[1]))
	    elif entry[1] is None:
		print(str(entry[0]))
            else:
                print(str(entry[0]) + '\t' + str(entry[1]))
    #end run
#end mapred
if __name__ == "__main__":
    usage="""submit a job to the run method.
    input_path: is the file to read in line by line
    mapper: is the map function to apply.  Should fit the convetion of (key, value, context)
    reducer: (optional) is the reducer function to apply.  Should fit the convention of (key, value, context)
    """

    print(usage)
