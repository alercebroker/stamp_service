from .extensions import ralidator

def filter_atlas_data(filter_name, arg_key):
    def wrapper_function(arg_function):
        def decorator_function(*args, **kwargs):
            """
            The decorator function works because we assume that the
            survey id exists.
            """
            if filter_name in ralidator.ralidator.user_filters and kwargs[arg_key] == "atlas":
                return None
            else:
                print(f" args = \n {args} \n kwargs = \n {kwargs}")
                return arg_function(*args, **kwargs)
        return decorator_function
    return wrapper_function